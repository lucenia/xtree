/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Comprehensive Checkpoint Performance Benchmarks
 * Tests checkpoint write/load performance, size efficiency, and recovery
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <iomanip>
#include <filesystem>
#include <string>
#include <random>
#include <thread>
#include <atomic>
#include "../../src/persistence/ot_checkpoint.h"
#include "../../src/persistence/object_table_sharded.hpp"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class CheckpointBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    
    void SetUp() override {
        test_dir_ = "/tmp/checkpoint_bench_" + std::to_string(getpid());
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        try {
            fs::remove_all(test_dir_);
        } catch (...) {}
    }
    
    void populateObjectTable(ObjectTableSharded* ot, size_t num_entries, bool add_retires = false) {
        for (size_t i = 0; i < num_entries; ++i) {
            OTAddr addr{};
            addr.file_id = i / 10000;
            addr.segment_id = (i / 100) % 100;
            addr.offset = (i % 100) * 4096;
            addr.length = 4096 << (i % 4);  // 4K to 32K
            
            NodeKind kind = (i % 3 == 0) ? NodeKind::Leaf : NodeKind::Internal;
            uint8_t class_id = i % 7;
            
            auto id = ot->allocate(kind, class_id, addr, i + 1);
            
            // Retire some entries to test mixed state
            if (add_retires && i % 10 == 5) {
                ot->retire(id, i + 100);
            }
        }
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
};

TEST_F(CheckpointBenchmark, WritePerformance) {
    printSeparator("Checkpoint Write Performance");
    
    const size_t ENTRY_COUNTS[] = {1000, 10000, 50000, 100000, 500000};
    
    std::cout << "\nMeasuring checkpoint write speed for various entry counts:\n\n";
    std::cout << "Entries  | Write Time | Throughput    | File Size | MB/s    | Status\n";
    std::cout << "---------|------------|---------------|-----------|---------|--------\n";
    
    double prev_throughput = 0;
    
    for (size_t num_entries : ENTRY_COUNTS) {
        auto ot = std::make_unique<ObjectTableSharded>();
        populateObjectTable(ot.get(), num_entries, true);
        
        OTCheckpoint checkpoint(test_dir_);
        
        auto start = high_resolution_clock::now();
        bool success = checkpoint.write(ot.get(), 1000);
        auto end = high_resolution_clock::now();
        
        ASSERT_TRUE(success);
        
        auto duration = duration_cast<milliseconds>(end - start);
        double throughput = (num_entries * 1000.0) / std::max(duration.count(), 1LL);
        
        // Get file size
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        size_t file_size = fs::file_size(checkpoint_path);
        double size_mb = file_size / (1024.0 * 1024.0);
        double mb_per_sec = size_mb / (duration.count() / 1000.0);
        
        // Check for regression (throughput shouldn't drop by more than 20%)
        bool regression = (prev_throughput > 0 && throughput < prev_throughput * 0.8);
        const char* status = regression ? "⚠ REGR" : "✓ OK";
        
        std::cout << std::setw(8) << num_entries << " | "
                  << std::setw(10) << duration.count() << " ms | "
                  << std::fixed << std::setprecision(0) 
                  << std::setw(10) << throughput << "/s | "
                  << std::setprecision(1) << std::setw(8) << size_mb << " MB | "
                  << std::setw(6) << mb_per_sec << " MB/s | "
                  << status << "\n";
        
        prev_throughput = throughput;
        fs::remove(checkpoint_path);
    }
    
    std::cout << "\n💡 Target: >100K entries/sec write throughput\n";
}

TEST_F(CheckpointBenchmark, LoadPerformance) {
    printSeparator("Checkpoint Load Performance");
    
    const size_t ENTRY_COUNTS[] = {1000, 10000, 50000, 100000, 500000};
    
    std::cout << "\nMeasuring checkpoint load/recovery speed:\n\n";
    std::cout << "Entries  | Load Time | Throughput    | Memory | Status\n";
    std::cout << "---------|-----------|---------------|--------|--------\n";
    
    for (size_t num_entries : ENTRY_COUNTS) {
        // First create a checkpoint
        auto ot_write = std::make_unique<ObjectTableSharded>();
        populateObjectTable(ot_write.get(), num_entries);
        
        OTCheckpoint checkpoint_write(test_dir_);
        ASSERT_TRUE(checkpoint_write.write(ot_write.get(), 1000));
        
        // Now measure load time
        auto ot_load = std::make_unique<ObjectTableSharded>();
        ot_load->begin_recovery();
        
        OTCheckpoint checkpoint_load(test_dir_);
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        
        auto start = high_resolution_clock::now();
        
        bool success = checkpoint_load.map_for_read(checkpoint_path, 
                                                    &epoch, &entry_count, &entries);
        ASSERT_TRUE(success);
        
        // Restore entries to OT
        for (size_t i = 0; i < entry_count; ++i) {
            ot_load->restore_handle(entries[i].handle_idx, entries[i]);
        }
        
        auto end = high_resolution_clock::now();
        
        ot_load->end_recovery();
        
        auto duration = duration_cast<milliseconds>(end - start);
        double throughput = (num_entries * 1000.0) / std::max(duration.count(), 1LL);
        
        // Estimate memory usage
        size_t memory_kb = (entry_count * sizeof(OTCheckpoint::PersistentEntry)) / 1024;
        
        // Check target: <100ms for 100K entries
        bool meets_target = (num_entries != 100000) || (duration.count() < 100);
        const char* status = meets_target ? "✓ OK" : "✗ SLOW";
        
        std::cout << std::setw(8) << num_entries << " | "
                  << std::setw(9) << duration.count() << " ms | "
                  << std::fixed << std::setprecision(0) 
                  << std::setw(10) << throughput << "/s | "
                  << std::setw(5) << memory_kb << " KB | "
                  << status << "\n";
        
        fs::remove(checkpoint_path);
    }
    
    std::cout << "\n💡 Target: <100ms load time for 100K entries (<2s for 1M)\n";
}

TEST_F(CheckpointBenchmark, SizeEfficiency) {
    printSeparator("Checkpoint Size Efficiency");
    
    const size_t NUM_ENTRIES = 100000;
    
    std::cout << "\nTesting storage efficiency with different data patterns (100K entries):\n\n";
    std::cout << "Pattern         | File Size | Bytes/Entry | Compression | Overhead\n";
    std::cout << "----------------|-----------|-------------|-------------|----------\n";
    
    struct Pattern {
        const char* name;
        std::function<void(ObjectTableSharded*)> populate;
    };
    
    size_t raw_entry_size = sizeof(OTCheckpoint::PersistentEntry);
    size_t baseline_size = 0;
    
    Pattern patterns[] = {
        {"Sequential", [this](ObjectTableSharded* ot) {
            for (size_t i = 0; i < NUM_ENTRIES; ++i) {
                OTAddr addr{};
                addr.file_id = 0;
                addr.segment_id = i / 1000;
                addr.offset = i * 4096;
                addr.length = 4096;
                ot->allocate(NodeKind::Internal, 1, addr, i + 1);
            }
        }},
        {"Random Files", [this](ObjectTableSharded* ot) {
            std::mt19937 rng(42);
            for (size_t i = 0; i < NUM_ENTRIES; ++i) {
                OTAddr addr{};
                addr.file_id = rng() % 10;
                addr.segment_id = rng() % 100;
                addr.offset = (i * 4096) % 1000000;
                addr.length = 4096 << (rng() % 4);
                ot->allocate(NodeKind::Leaf, rng() % 7, addr, i + 1);
            }
        }},
        {"With Retires", [this](ObjectTableSharded* ot) {
            for (size_t i = 0; i < NUM_ENTRIES; ++i) {
                OTAddr addr{};
                addr.file_id = i / 10000;
                addr.segment_id = i / 100;
                addr.offset = i * 4096;
                addr.length = 4096;
                auto id = ot->allocate(NodeKind::Internal, 1, addr, i + 1);
                if (i % 3 == 0) {
                    ot->retire(id, i + 100);
                }
            }
        }},
        {"Fragmented", [this](ObjectTableSharded* ot) {
            // Allocate and retire to create fragmentation
            for (size_t i = 0; i < NUM_ENTRIES * 2; ++i) {
                OTAddr addr{};
                addr.file_id = i / 10000;
                addr.segment_id = (i / 100) % 100;
                addr.offset = (i % 100) * 4096;
                addr.length = 4096;
                auto id = ot->allocate(NodeKind::Internal, 1, addr, i + 1);
                // Retire every other entry
                if (i % 2 == 0) {
                    ot->retire(id, i + 2);
                }
            }
        }}
    };
    
    for (size_t idx = 0; idx < sizeof(patterns)/sizeof(patterns[0]); ++idx) {
        const auto& pattern = patterns[idx];
        auto ot = std::make_unique<ObjectTableSharded>();
        pattern.populate(ot.get());
        
        OTCheckpoint checkpoint(test_dir_);
        ASSERT_TRUE(checkpoint.write(ot.get(), 1000));
        
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        size_t file_size = fs::file_size(checkpoint_path);
        
        if (idx == 0) baseline_size = file_size;
        
        double bytes_per_entry = file_size / double(NUM_ENTRIES);
        double compression_ratio = baseline_size / double(file_size);
        double overhead = ((file_size - (NUM_ENTRIES * raw_entry_size)) / 
                          double(NUM_ENTRIES * raw_entry_size)) * 100;
        
        std::cout << std::setw(15) << pattern.name << " | "
                  << std::fixed << std::setprecision(2) 
                  << std::setw(8) << file_size / (1024.0 * 1024.0) << " MB | "
                  << std::setw(11) << bytes_per_entry << " | "
                  << std::setprecision(2) << std::setw(11) << compression_ratio << "x | "
                  << std::setprecision(1) << std::setw(8) << overhead << "%\n";
        
        fs::remove(checkpoint_path);
    }
    
    std::cout << "\n💡 Raw entry size: " << raw_entry_size << " bytes\n";
    std::cout << "💡 Target overhead: <10% over raw size\n";
}

TEST_F(CheckpointBenchmark, IncrementalPerformance) {
    printSeparator("Incremental Checkpoint Performance");
    
    std::cout << "\nMeasuring checkpoint time as object table grows:\n\n";
    std::cout << "Total Entries | Checkpoint Time | Throughput  | Delta\n";
    std::cout << "--------------|-----------------|-------------|--------\n";
    
    auto ot = std::make_unique<ObjectTableSharded>();
    const size_t INCREMENT = 10000;
    const size_t MAX_ENTRIES = 100000;
    
    double prev_time = 0;
    
    for (size_t total = INCREMENT; total <= MAX_ENTRIES; total += INCREMENT) {
        // Add more entries
        size_t start_idx = total - INCREMENT;
        for (size_t i = start_idx; i < total; ++i) {
            OTAddr addr{};
            addr.file_id = i / 10000;
            addr.segment_id = (i / 100) % 100;
            addr.offset = (i % 100) * 4096;
            addr.length = 4096;
            ot->allocate(NodeKind::Internal, 1, addr, i + 1);
        }
        
        OTCheckpoint checkpoint(test_dir_);
        
        auto start = high_resolution_clock::now();
        bool success = checkpoint.write(ot.get(), 1000 + total);
        auto end = high_resolution_clock::now();
        
        ASSERT_TRUE(success);
        
        auto duration = duration_cast<milliseconds>(end - start);
        double throughput = (total * 1000.0) / std::max(duration.count(), 1LL);
        double delta = duration.count() - prev_time;
        
        std::cout << std::setw(13) << total << " | "
                  << std::setw(15) << duration.count() << " ms | "
                  << std::fixed << std::setprecision(0) 
                  << std::setw(10) << throughput << "/s | ";
        
        if (prev_time > 0) {
            std::cout << std::showpos << std::setw(5) << delta << " ms" << std::noshowpos;
        } else {
            std::cout << "    --";
        }
        std::cout << "\n";
        
        prev_time = duration.count();
        
        // Clean up checkpoint
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        fs::remove(checkpoint_path);
    }
    
    std::cout << "\n💡 Checkpoint time should scale linearly with entry count\n";
}

TEST_F(CheckpointBenchmark, ConcurrentReadPerformance) {
    printSeparator("Concurrent Checkpoint Read Performance");
    
    const size_t NUM_ENTRIES = 100000;
    const int READER_COUNTS[] = {1, 2, 4, 8};
    
    // Create a checkpoint
    auto ot = std::make_unique<ObjectTableSharded>();
    populateObjectTable(ot.get(), NUM_ENTRIES);
    
    OTCheckpoint checkpoint(test_dir_);
    ASSERT_TRUE(checkpoint.write(ot.get(), 1000));
    
    std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    
    std::cout << "\nMeasuring concurrent read performance (100K entries):\n\n";
    std::cout << "Readers | Total Time | Throughput/Reader | Scalability\n";
    std::cout << "--------|------------|-------------------|-------------\n";
    
    double single_thread_throughput = 0;
    
    for (int num_readers : READER_COUNTS) {
        auto start = high_resolution_clock::now();
        
        std::vector<std::thread> readers;
        std::atomic<size_t> total_loaded(0);
        
        for (int i = 0; i < num_readers; ++i) {
            readers.emplace_back([&]() {
                OTCheckpoint reader_checkpoint(test_dir_);
                uint64_t epoch;
                size_t entry_count;
                const OTCheckpoint::PersistentEntry* entries;
                
                if (reader_checkpoint.map_for_read(checkpoint_path, 
                                                   &epoch, &entry_count, &entries)) {
                    // Simulate processing
                    size_t sum = 0;
                    for (size_t j = 0; j < entry_count; ++j) {
                        sum += entries[j].handle_idx;
                    }
                    total_loaded += entry_count;
                }
            });
        }
        
        for (auto& t : readers) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        double total_throughput = (total_loaded * 1000.0) / std::max(duration.count(), 1LL);
        double per_reader_throughput = total_throughput / num_readers;
        
        if (num_readers == 1) {
            single_thread_throughput = per_reader_throughput;
        }
        
        double scalability = (single_thread_throughput > 0) ? 
            (per_reader_throughput / single_thread_throughput * 100) : 100;
        
        std::cout << std::setw(7) << num_readers << " | "
                  << std::setw(10) << duration.count() << " ms | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(15) << per_reader_throughput << "/s | "
                  << std::setprecision(1) << std::setw(10) << scalability << "%\n";
    }
    
    fs::remove(checkpoint_path);
    
    std::cout << "\n💡 Memory-mapped checkpoints should scale well for reads\n";
}

TEST_F(CheckpointBenchmark, Summary) {
    printSeparator("Checkpoint Performance Summary");
    
    std::cout << "\n📊 Running comprehensive checkpoint benchmark suite...\n\n";
    
    const size_t TEST_SIZE = 100000;  // 100K entries for summary
    
    // Test 1: Write Performance
    {
        auto ot = std::make_unique<ObjectTableSharded>();
        populateObjectTable(ot.get(), TEST_SIZE);
        
        OTCheckpoint checkpoint(test_dir_);
        auto start = high_resolution_clock::now();
        bool success = checkpoint.write(ot.get(), 1000);
        auto end = high_resolution_clock::now();
        
        ASSERT_TRUE(success);
        auto duration = duration_cast<milliseconds>(end - start);
        double throughput = (TEST_SIZE * 1000.0) / std::max(duration.count(), 1LL);
        
        std::cout << "Write Performance:\n";
        std::cout << "  • " << TEST_SIZE << " entries in " << duration.count() << " ms\n";
        std::cout << "  • Throughput: " << std::fixed << std::setprecision(0) 
                  << throughput << " entries/sec "
                  << (throughput > 100000 ? "✓" : "✗") << "\n";
    }
    
    // Test 2: Load Performance
    {
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        
        auto ot_load = std::make_unique<ObjectTableSharded>();
        ot_load->begin_recovery();
        
        OTCheckpoint checkpoint_load(test_dir_);
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        
        auto start = high_resolution_clock::now();
        checkpoint_load.map_for_read(checkpoint_path, &epoch, &entry_count, &entries);
        for (size_t i = 0; i < entry_count; ++i) {
            ot_load->restore_handle(entries[i].handle_idx, entries[i]);
        }
        auto end = high_resolution_clock::now();
        ot_load->end_recovery();
        
        auto duration = duration_cast<milliseconds>(end - start);
        
        std::cout << "\nLoad Performance:\n";
        std::cout << "  • " << entry_count << " entries in " << duration.count() << " ms\n";
        std::cout << "  • Target <100ms: " << (duration.count() < 100 ? "✓ PASS" : "✗ FAIL") << "\n";
        
        // Size efficiency
        size_t file_size = fs::file_size(checkpoint_path);
        double bytes_per_entry = file_size / double(entry_count);
        
        std::cout << "\nSize Efficiency:\n";
        std::cout << "  • File size: " << std::fixed << std::setprecision(2) 
                  << file_size / (1024.0 * 1024.0) << " MB\n";
        std::cout << "  • Bytes per entry: " << bytes_per_entry << "\n";
        std::cout << "  • Overhead: " << std::setprecision(1)
                  << ((bytes_per_entry / sizeof(OTCheckpoint::PersistentEntry) - 1) * 100) 
                  << "%\n";
        
        fs::remove(checkpoint_path);
    }
    
    std::cout << "\n🎯 Performance Targets:\n";
    std::cout << "  ✓ Write: >100K entries/sec\n";
    std::cout << "  ✓ Load: <100ms for 100K entries\n";
    std::cout << "  ✓ Size: <10% overhead\n";
    std::cout << "  ✓ Scale: Linear with entry count\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}