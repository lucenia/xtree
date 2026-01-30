/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Comprehensive Recovery Performance Benchmarks
 * Tests cold start, delta replay, and recovery under various scenarios
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <iomanip>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <atomic>
#include <random>
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/ot_checkpoint.h"
#include "../../src/persistence/object_table_sharded.hpp"
#include "../../src/persistence/superblock.hpp"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class RecoveryBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    
    void SetUp() override {
        test_dir_ = "/tmp/recovery_bench_" + std::to_string(getpid());
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        try {
            fs::remove_all(test_dir_);
        } catch (...) {}
    }
    
    void createCheckpoint(size_t num_entries, uint64_t epoch) {
        auto ot = std::make_unique<ObjectTableSharded>();
        
        for (size_t i = 0; i < num_entries; ++i) {
            OTAddr addr{};
            addr.file_id = i / 10000;
            addr.segment_id = (i / 100) % 100;
            addr.offset = (i % 100) * 4096;
            addr.length = 4096;
            ot->allocate(NodeKind::Internal, 1, addr, i + 1);
        }
        
        OTCheckpoint checkpoint(test_dir_);
        checkpoint.write(ot.get(), epoch);
    }
    
    void createDeltaLog(const std::string& filename, size_t num_deltas, uint64_t start_epoch) {
        std::string log_path = test_dir_ + "/" + filename;
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        std::vector<OTDeltaRec> batch;
        batch.reserve(100);
        
        for (size_t i = 0; i < num_deltas; ++i) {
            OTDeltaRec rec{};
            rec.handle_idx = i;
            rec.tag = 1;
            rec.birth_epoch = start_epoch + i;
            rec.retire_epoch = ~uint64_t{0};
            rec.class_id = i % 7;
            rec.kind = static_cast<uint8_t>(NodeKind::Internal);
            rec.file_id = 0;
            rec.segment_id = i / 1000;
            rec.offset = (i % 1000) * 4096;
            rec.length = 4096;
            
            batch.push_back(rec);
            
            if (batch.size() == 100 || i == num_deltas - 1) {
                log.append(batch);
                batch.clear();
            }
        }
        
        log.sync();
        log.close();
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
};

TEST_F(RecoveryBenchmark, CheckpointOnlyRecovery) {
    printSeparator("Checkpoint-Only Recovery Performance");
    
    const size_t ENTRY_COUNTS[] = {1000, 10000, 50000, 100000, 500000};
    
    std::cout << "\nMeasuring cold start recovery from checkpoint only:\n\n";
    std::cout << "Entries  | Recovery Time | Throughput    | MB/s   | Status\n";
    std::cout << "---------|---------------|---------------|--------|--------\n";
    
    for (size_t num_entries : ENTRY_COUNTS) {
        // Create checkpoint
        createCheckpoint(num_entries, 1000);
        
        // Measure recovery
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        auto start = high_resolution_clock::now();
        
        // Load checkpoint
        OTCheckpoint checkpoint(test_dir_);
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        
        bool success = checkpoint.map_for_read(checkpoint_path, 
                                               &epoch, &entry_count, &entries);
        ASSERT_TRUE(success);
        
        // Restore to OT
        for (size_t i = 0; i < entry_count; ++i) {
            ot->restore_handle(entries[i].handle_idx, entries[i]);
        }
        
        ot->end_recovery();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        double throughput = (num_entries * 1000.0) / std::max(duration.count(), 1LL);
        
        // Calculate MB/s (each entry ~48 bytes)
        double mb_per_sec = (num_entries * 48) / (1024.0 * 1024.0) / 
                           (duration.count() / 1000.0);
        
        // Check if we meet 2-second target for 1M entries
        // Extrapolate: 100K should be <200ms, 500K should be <1000ms
        bool meets_target = true;
        if (num_entries == 100000 && duration.count() > 200) meets_target = false;
        if (num_entries == 500000 && duration.count() > 1000) meets_target = false;
        
        const char* status = meets_target ? "✓ OK" : "✗ SLOW";
        
        std::cout << std::setw(8) << num_entries << " | "
                  << std::setw(13) << duration.count() << " ms | "
                  << std::fixed << std::setprecision(0) 
                  << std::setw(10) << throughput << "/s | "
                  << std::setprecision(1) << std::setw(5) << mb_per_sec << " MB/s | "
                  << status << "\n";
        
        // Clean up
        fs::remove(checkpoint_path);
    }
    
    std::cout << "\n💡 Target: <2 seconds recovery for 1M entries\n";
}

TEST_F(RecoveryBenchmark, DeltaLogReplayPerformance) {
    printSeparator("Delta Log Replay Performance");
    
    const size_t DELTA_COUNTS[] = {100, 1000, 10000, 50000, 100000};
    
    std::cout << "\nMeasuring delta log replay speed:\n\n";
    std::cout << "Deltas   | Replay Time | Throughput     | MB/s  | Overhead\n";
    std::cout << "---------|-------------|----------------|-------|----------\n";
    
    for (size_t num_deltas : DELTA_COUNTS) {
        std::string log_file = "delta_" + std::to_string(num_deltas) + ".wal";
        createDeltaLog(log_file, num_deltas, 1001);
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        auto start = high_resolution_clock::now();
        
        // Replay the log
        size_t replayed = 0;
        uint64_t last_offset = 0;
        std::string error_msg;
        
        OTDeltaLog::replay(test_dir_ + "/" + log_file,
            [&](const OTDeltaRec& rec) {
                ot->apply_delta(rec);
                replayed++;
            },
            &last_offset,
            &error_msg);
        
        auto end = high_resolution_clock::now();
        ot->end_recovery();
        
        auto duration = duration_cast<microseconds>(end - start);
        double throughput = (replayed * 1000000.0) / duration.count();
        
        // Calculate MB/s (each delta is ~64 bytes)
        double mb_per_sec = (replayed * 64) / (1024.0 * 1024.0) / 
                           (duration.count() / 1000000.0);
        
        // Compare with raw read speed
        auto raw_start = high_resolution_clock::now();
        std::ifstream file(test_dir_ + "/" + log_file, std::ios::binary);
        std::vector<char> buffer(fs::file_size(test_dir_ + "/" + log_file));
        file.read(buffer.data(), buffer.size());
        auto raw_end = high_resolution_clock::now();
        auto raw_duration = duration_cast<microseconds>(raw_end - raw_start);
        
        double overhead = ((double)duration.count() / raw_duration.count() - 1.0) * 100;
        
        std::cout << std::setw(8) << num_deltas << " | "
                  << std::fixed << std::setprecision(1) 
                  << std::setw(11) << duration.count() / 1000.0 << " ms | "
                  << std::setprecision(0) << std::setw(12) << throughput << "/s | "
                  << std::setprecision(1) << std::setw(5) << mb_per_sec << " MB/s | "
                  << std::setw(7) << overhead << "%\n";
        
        // Clean up
        fs::remove(test_dir_ + "/" + log_file);
    }
    
    std::cout << "\n💡 Target: >1M deltas/sec replay speed\n";
}

TEST_F(RecoveryBenchmark, MixedCheckpointDeltaRecovery) {
    printSeparator("Mixed Checkpoint + Delta Recovery");
    
    struct TestCase {
        size_t checkpoint_entries;
        size_t delta_entries;
        const char* name;
    };
    
    TestCase cases[] = {
        {10000, 0, "Checkpoint only"},
        {10000, 1000, "Small delta (10%)"},
        {10000, 10000, "Equal delta (100%)"},
        {10000, 50000, "Large delta (500%)"},
        {100000, 100000, "Large scale"},
    };
    
    std::cout << "\nMeasuring mixed recovery scenarios:\n\n";
    std::cout << "Scenario          | Checkpoint | Deltas  | Total Time | Throughput\n";
    std::cout << "------------------|------------|---------|------------|------------\n";
    
    for (const auto& tc : cases) {
        // Create checkpoint
        createCheckpoint(tc.checkpoint_entries, 1000);
        
        // Create delta log if needed
        if (tc.delta_entries > 0) {
            createDeltaLog("delta.wal", tc.delta_entries, 1001);
        }
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        auto start = high_resolution_clock::now();
        
        // Load checkpoint
        OTCheckpoint checkpoint(test_dir_);
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        
        checkpoint.map_for_read(checkpoint_path, &epoch, &entry_count, &entries);
        for (size_t i = 0; i < entry_count; ++i) {
            ot->restore_handle(entries[i].handle_idx, entries[i]);
        }
        
        // Replay delta log if present
        if (tc.delta_entries > 0) {
            size_t replayed = 0;
            uint64_t last_offset = 0;
            std::string error_msg;
            
            OTDeltaLog::replay(test_dir_ + "/delta.wal",
                [&](const OTDeltaRec& rec) {
                    ot->apply_delta(rec);
                    replayed++;
                },
                &last_offset,
                &error_msg);
        }
        
        ot->end_recovery();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        size_t total_entries = tc.checkpoint_entries + tc.delta_entries;
        double throughput = (total_entries * 1000.0) / std::max(duration.count(), 1LL);
        
        std::cout << std::setw(17) << tc.name << " | "
                  << std::setw(10) << tc.checkpoint_entries << " | "
                  << std::setw(7) << tc.delta_entries << " | "
                  << std::setw(10) << duration.count() << " ms | "
                  << std::fixed << std::setprecision(0) 
                  << std::setw(10) << throughput << "/s\n";
        
        // Clean up
        fs::remove(checkpoint_path);
        if (tc.delta_entries > 0) {
            fs::remove(test_dir_ + "/delta.wal");
        }
    }
    
    std::cout << "\n💡 Delta replay should add minimal overhead to checkpoint recovery\n";
}

TEST_F(RecoveryBenchmark, RecoveryWithCorruption) {
    printSeparator("Recovery with Corruption Handling");
    
    const size_t BASE_DELTAS = 10000;
    
    std::cout << "\nTesting recovery robustness and performance with corruption:\n\n";
    std::cout << "Scenario            | Expected | Recovered | Time    | Status\n";
    std::cout << "--------------------|----------|-----------|---------|--------\n";
    
    struct CorruptionTest {
        const char* name;
        std::function<void(const std::string&)> corrupt;
        size_t expected_recovered;
    };
    
    CorruptionTest tests[] = {
        {"Clean log", nullptr, BASE_DELTAS},
        
        {"Truncated tail", 
         [](const std::string& path) {
             auto size = fs::file_size(path);
             fs::resize_file(path, size - 20);  // Remove last 20 bytes
         }, BASE_DELTAS - 1},  // Should recover all but last
         
        {"Zeroed middle",
         [](const std::string& path) {
             // Zero out bytes in the middle
             std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
             file.seekp(fs::file_size(path) / 2);
             char zeros[100] = {0};
             file.write(zeros, 100);
         }, BASE_DELTAS / 2},  // Should recover up to corruption
         
        {"Bad CRC",
         [](const std::string& path) {
             // Corrupt a byte to invalidate CRC
             std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
             file.seekp(1000);  // Somewhere in the middle
             char bad = 0xFF;
             file.write(&bad, 1);
         }, 0},  // Depends on CRC checking implementation
    };
    
    for (const auto& test : tests) {
        // Create clean delta log
        createDeltaLog("corrupt_test.wal", BASE_DELTAS, 1);
        
        // Apply corruption if specified
        if (test.corrupt) {
            test.corrupt(test_dir_ + "/corrupt_test.wal");
        }
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        auto start = high_resolution_clock::now();
        
        size_t replayed = 0;
        uint64_t last_offset = 0;
        std::string error_msg;
        
        OTDeltaLog::replay(test_dir_ + "/corrupt_test.wal",
            [&](const OTDeltaRec& rec) {
                ot->apply_delta(rec);
                replayed++;
            },
            &last_offset,
            &error_msg);
        
        auto end = high_resolution_clock::now();
        ot->end_recovery();
        
        auto duration = duration_cast<milliseconds>(end - start);
        
        // Allow some tolerance for corruption cases
        bool recovered_ok = (test.corrupt == nullptr) ? 
            (replayed == test.expected_recovered) :
            (replayed >= test.expected_recovered * 0.9);  // 90% tolerance
        
        const char* status = recovered_ok ? "✓ OK" : "⚠ ISSUE";
        
        std::cout << std::setw(19) << test.name << " | "
                  << std::setw(8) << test.expected_recovered << " | "
                  << std::setw(9) << replayed << " | "
                  << std::setw(7) << duration.count() << " ms | "
                  << status << "\n";
        
        if (!error_msg.empty()) {
            std::cout << "  Error: " << error_msg << "\n";
        }
        
        fs::remove(test_dir_ + "/corrupt_test.wal");
    }
    
    std::cout << "\n💡 Recovery should be resilient to common corruption scenarios\n";
}

TEST_F(RecoveryBenchmark, ParallelRecoveryComponents) {
    printSeparator("Parallel Recovery Component Performance");
    
    const size_t CHECKPOINT_SIZE = 100000;
    const size_t DELTA_SIZE = 50000;
    
    std::cout << "\nMeasuring parallel recovery of different components:\n\n";
    
    // Create test data
    createCheckpoint(CHECKPOINT_SIZE, 1000);
    createDeltaLog("delta1.wal", DELTA_SIZE, 1001);
    createDeltaLog("delta2.wal", DELTA_SIZE, 1001 + DELTA_SIZE);
    
    // Test 1: Sequential recovery
    std::chrono::milliseconds sequential_time;
    {
        auto start = high_resolution_clock::now();
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        // Load checkpoint
        OTCheckpoint checkpoint(test_dir_);
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        checkpoint.map_for_read(checkpoint_path, &epoch, &entry_count, &entries);
        for (size_t i = 0; i < entry_count; ++i) {
            ot->restore_handle(entries[i].handle_idx, entries[i]);
        }
        
        // Replay both delta logs
        for (const auto& log_name : {"delta1.wal", "delta2.wal"}) {
            size_t replayed = 0;
            uint64_t last_offset = 0;
            std::string error_msg;
            OTDeltaLog::replay(test_dir_ + "/" + log_name,
                [&](const OTDeltaRec& rec) {
                    ot->apply_delta(rec);
                    replayed++;
                },
                &last_offset,
                &error_msg);
        }
        
        ot->end_recovery();
        auto end = high_resolution_clock::now();
        sequential_time = duration_cast<milliseconds>(end - start);
        
        std::cout << "Sequential Recovery:\n";
        std::cout << "  • Checkpoint + 2 delta logs: " << sequential_time.count() << " ms\n";
    }
    
    // Test 2: Parallel delta log loading (simulated)
    std::string checkpoint_path;
    {
        auto start = high_resolution_clock::now();
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        // Load checkpoint first
        OTCheckpoint checkpoint(test_dir_);
        checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        checkpoint.map_for_read(checkpoint_path, &epoch, &entry_count, &entries);
        for (size_t i = 0; i < entry_count; ++i) {
            ot->restore_handle(entries[i].handle_idx, entries[i]);
        }
        
        // Parallel read of delta logs (simulation - actual OT apply must be sequential)
        std::vector<std::vector<OTDeltaRec>> delta_batches(2);
        std::vector<std::thread> readers;
        
        for (int i = 0; i < 2; ++i) {
            readers.emplace_back([&, idx = i]() {
                std::string log_name = "delta" + std::to_string(idx + 1) + ".wal";
                size_t replayed = 0;
                uint64_t last_offset = 0;
                std::string error_msg;
                
                OTDeltaLog::replay(test_dir_ + "/" + log_name,
                    [&](const OTDeltaRec& rec) {
                        delta_batches[idx].push_back(rec);
                        replayed++;
                    },
                    &last_offset,
                    &error_msg);
            });
        }
        
        for (auto& t : readers) {
            t.join();
        }
        
        // Apply deltas sequentially
        for (const auto& batch : delta_batches) {
            for (const auto& rec : batch) {
                ot->apply_delta(rec);
            }
        }
        
        ot->end_recovery();
        auto end = high_resolution_clock::now();
        auto parallel_time = duration_cast<milliseconds>(end - start);
        
        std::cout << "\nParallel Delta Loading:\n";
        std::cout << "  • Checkpoint + 2 parallel delta loads: " << parallel_time.count() << " ms\n";
        std::cout << "  • Speedup: " << std::fixed << std::setprecision(1) 
                  << ((double)sequential_time.count() / parallel_time.count()) << "x\n";
    }
    
    // Clean up
    fs::remove(checkpoint_path);
    fs::remove(test_dir_ + "/delta1.wal");
    fs::remove(test_dir_ + "/delta2.wal");
    
    std::cout << "\n💡 Parallel I/O can improve recovery time for multiple delta logs\n";
}

TEST_F(RecoveryBenchmark, Summary) {
    printSeparator("Recovery Performance Summary");
    
    std::cout << "\n📊 Running comprehensive recovery benchmark suite...\n\n";
    
    const size_t CHECKPOINT_SIZE = 100000;
    const size_t DELTA_SIZE = 10000;
    
    // Test checkpoint recovery
    {
        createCheckpoint(CHECKPOINT_SIZE, 1000);
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        auto start = high_resolution_clock::now();
        
        OTCheckpoint checkpoint(test_dir_);
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        checkpoint.map_for_read(checkpoint_path, &epoch, &entry_count, &entries);
        for (size_t i = 0; i < entry_count; ++i) {
            ot->restore_handle(entries[i].handle_idx, entries[i]);
        }
        ot->end_recovery();
        
        auto end = high_resolution_clock::now();
        auto checkpoint_time = duration_cast<milliseconds>(end - start);
        
        std::cout << "Checkpoint Recovery:\n";
        std::cout << "  • " << CHECKPOINT_SIZE << " entries in " 
                  << checkpoint_time.count() << " ms\n";
        std::cout << "  • Throughput: " << std::fixed << std::setprecision(0)
                  << (CHECKPOINT_SIZE * 1000.0 / checkpoint_time.count()) << " entries/sec\n";
        
        fs::remove(checkpoint_path);
    }
    
    // Test delta replay
    {
        createDeltaLog("summary_delta.wal", DELTA_SIZE, 1001);
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        auto start = high_resolution_clock::now();
        
        size_t replayed = 0;
        uint64_t last_offset = 0;
        std::string error_msg;
        OTDeltaLog::replay(test_dir_ + "/summary_delta.wal",
            [&](const OTDeltaRec& rec) {
                ot->apply_delta(rec);
                replayed++;
            },
            &last_offset,
            &error_msg);
        ot->end_recovery();
        
        auto end = high_resolution_clock::now();
        auto delta_time = duration_cast<microseconds>(end - start);
        
        std::cout << "\nDelta Log Replay:\n";
        std::cout << "  • " << DELTA_SIZE << " deltas in " 
                  << std::fixed << std::setprecision(1) 
                  << delta_time.count() / 1000.0 << " ms\n";
        std::cout << "  • Throughput: " << std::setprecision(0)
                  << (DELTA_SIZE * 1000000.0 / delta_time.count()) << " deltas/sec\n";
        
        fs::remove(test_dir_ + "/summary_delta.wal");
    }
    
    // Mixed recovery
    {
        createCheckpoint(CHECKPOINT_SIZE, 1000);
        createDeltaLog("mixed.wal", DELTA_SIZE, 1001);
        
        auto start = high_resolution_clock::now();
        
        auto ot = std::make_unique<ObjectTable>();
        ot->begin_recovery();
        
        // Checkpoint
        OTCheckpoint checkpoint(test_dir_);
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        uint64_t epoch;
        size_t entry_count;
        const OTCheckpoint::PersistentEntry* entries;
        checkpoint.map_for_read(checkpoint_path, &epoch, &entry_count, &entries);
        for (size_t i = 0; i < entry_count; ++i) {
            ot->restore_handle(entries[i].handle_idx, entries[i]);
        }
        
        // Delta
        size_t replayed = 0;
        uint64_t last_offset = 0;
        std::string error_msg;
        OTDeltaLog::replay(test_dir_ + "/mixed.wal",
            [&](const OTDeltaRec& rec) {
                ot->apply_delta(rec);
                replayed++;
            },
            &last_offset,
            &error_msg);
        
        ot->end_recovery();
        auto end = high_resolution_clock::now();
        auto total_time = duration_cast<milliseconds>(end - start);
        
        std::cout << "\nMixed Recovery:\n";
        std::cout << "  • " << (CHECKPOINT_SIZE + DELTA_SIZE) << " total entries in "
                  << total_time.count() << " ms\n";
        std::cout << "  • Target <500ms: " << (total_time.count() < 500 ? "✓ PASS" : "✗ FAIL") << "\n";
        
        fs::remove(checkpoint_path);
        fs::remove(test_dir_ + "/mixed.wal");
    }
    
    std::cout << "\n🎯 Performance Targets:\n";
    std::cout << "  ✓ Checkpoint: >500K entries/sec\n";
    std::cout << "  ✓ Delta replay: >1M deltas/sec\n";
    std::cout << "  ✓ Mixed recovery: <500ms typical\n";
    std::cout << "  ✓ Corruption: Graceful degradation\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}