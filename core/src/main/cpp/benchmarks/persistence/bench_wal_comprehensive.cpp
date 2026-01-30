/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Comprehensive WAL Performance Benchmarks
 * 
 * This file contains all WAL (Write-Ahead Log) performance benchmarks:
 * 1. Basic throughput and latency tests
 * 2. Sync overhead measurements
 * 3. Concurrent scalability analysis
 * 4. Batch size optimization tests
 * 5. Payload-in-WAL performance (EVENTUAL mode)
 * 
 * Run all WAL benchmarks:
 *   ./xtree_benchmarks --gtest_filter="WALBenchmark.*"
 * 
 * Run specific tests:
 *   ./xtree_benchmarks --gtest_filter="WALBenchmark.BasicThroughput"
 *   ./xtree_benchmarks --gtest_filter="WALBenchmark.ConcurrentScalability"
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <filesystem>
#include <string>
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/object_table.hpp"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class WALBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    
    void SetUp() override {
        test_dir_ = "/tmp/wal_bench_" + std::to_string(getpid());
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        try {
            fs::remove_all(test_dir_);
        } catch (...) {}
    }
    
    std::vector<OTDeltaRec> generateDeltas(size_t count) {
        std::vector<OTDeltaRec> deltas;
        deltas.reserve(count);
        
        for (size_t i = 0; i < count; ++i) {
            OTDeltaRec rec{};
            rec.handle_idx = i;
            rec.tag = 1;
            rec.class_id = (i % 7);
            rec.kind = static_cast<uint8_t>(NodeKind::Internal);
            rec.file_id = 0;
            rec.segment_id = i / 1000;
            rec.offset = (i % 1000) * 4096;
            rec.length = 4096 << (i % 3);
            rec.birth_epoch = i + 1;
            rec.retire_epoch = ~uint64_t{0};
            deltas.push_back(rec);
        }
        return deltas;
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(60, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(60, '=') << "\n";
    }
};

// ============================================================================
// Test 1: Basic Throughput and Latency
// ============================================================================
TEST_F(WALBenchmark, BasicThroughput) {
    printSeparator("Basic WAL Throughput Test");
    
    OTDeltaLog log(test_dir_ + "/throughput.wal");
    ASSERT_TRUE(log.open_for_append());
    
    const size_t BATCH_SIZES[] = {1, 10, 100, 1000, 5000};
    const size_t TOTAL_RECORDS = 100000;
    
    std::cout << "\nTarget: >1M records/sec for batch >= 100\n\n";
    std::cout << "Batch Size | Throughput (rec/sec) | Latency (us) | Status\n";
    std::cout << "-----------|---------------------|--------------|--------\n";
    
    for (size_t batch_size : BATCH_SIZES) {
        auto deltas = generateDeltas(batch_size);
        size_t iterations = TOTAL_RECORDS / batch_size;
        
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; ++i) {
            log.append(deltas);
        }
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start);
        double throughput = (TOTAL_RECORDS * 1000000.0) / duration.count();
        double latency_us = duration.count() / double(iterations);
        
        bool meets_target = (batch_size >= 100) ? (throughput > 1000000) : true;
        const char* status = meets_target ? "✓ PASS" : "✗ FAIL";
        
        std::cout << std::setw(10) << batch_size << " | "
                  << std::fixed << std::setprecision(0) 
                  << std::setw(19) << throughput << " | "
                  << std::setprecision(1) << std::setw(12) << latency_us << " | "
                  << status << "\n";
        
        if (batch_size >= 100) {
            EXPECT_GT(throughput, 1000000) 
                << "Should exceed 1M rec/sec for batch " << batch_size;
        }
    }
    
    log.close();
}

// ============================================================================
// Test 2: Sync Overhead
// ============================================================================
TEST_F(WALBenchmark, SyncOverhead) {
    printSeparator("WAL Sync Overhead Test");
    
    OTDeltaLog log(test_dir_ + "/sync.wal");
    ASSERT_TRUE(log.open_for_append());
    
    const size_t BATCH_SIZE = 100;
    const size_t NUM_ITERATIONS = 100;
    auto deltas = generateDeltas(BATCH_SIZE);
    
    std::cout << "\nTarget: Sync latency <10ms per batch\n\n";
    
    // Without sync
    auto start_no_sync = high_resolution_clock::now();
    for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
        log.append(deltas);
    }
    auto end_no_sync = high_resolution_clock::now();
    
    // With sync
    auto start_sync = high_resolution_clock::now();
    for (size_t i = 0; i < NUM_ITERATIONS; ++i) {
        log.append(deltas);
        log.sync();
    }
    auto end_sync = high_resolution_clock::now();
    
    auto duration_no_sync = duration_cast<microseconds>(end_no_sync - start_no_sync);
    auto duration_sync = duration_cast<microseconds>(end_sync - start_sync);
    
    double latency_no_sync = duration_no_sync.count() / double(NUM_ITERATIONS);
    double latency_sync = duration_sync.count() / double(NUM_ITERATIONS);
    double sync_overhead = latency_sync - latency_no_sync;
    double overhead_percent = (sync_overhead / latency_sync) * 100;
    
    std::cout << "Append without sync: " << std::fixed << std::setprecision(1) 
              << latency_no_sync << " us/batch\n";
    std::cout << "Append with sync:    " << latency_sync << " us/batch ("
              << latency_sync / 1000.0 << " ms)\n";
    std::cout << "Sync overhead:       " << sync_overhead << " us ("
              << std::setprecision(0) << overhead_percent << "%)\n";
    
    bool meets_target = (latency_sync < 10000);  // 10ms = 10000us
    std::cout << "\nStatus: " << (meets_target ? "✓ PASS" : "✗ FAIL") << "\n";
    
    EXPECT_LT(latency_sync, 10000) << "Sync latency should be <10ms";
    
    log.close();
}

// ============================================================================
// Test 3: Concurrent Scalability
// ============================================================================
TEST_F(WALBenchmark, ConcurrentScalability) {
    printSeparator("Concurrent Scalability Test");
    
    std::cout << "\nShowing effect of batch size on thread scalability:\n\n";
    
    const size_t BATCH_SIZES[] = {100, 1000, 5000};
    const size_t THREAD_COUNTS[] = {1, 2, 4, 8};
    const size_t RECORDS_PER_THREAD = 50000;
    
    std::cout << "Batch | 1 Thread  | 2 Threads | 4 Threads | 8 Threads | 8T Efficiency\n";
    std::cout << "------|-----------|-----------|-----------|-----------|---------------\n";
    
    for (size_t batch_size : BATCH_SIZES) {
        std::cout << std::setw(5) << batch_size << " | ";
        
        double single_thread_throughput = 0;
        
        for (size_t num_threads : THREAD_COUNTS) {
            OTDeltaLog log(test_dir_ + "/concurrent.wal");
            ASSERT_TRUE(log.open_for_append());
            
            std::atomic<size_t> total_appends{0};
            auto deltas = generateDeltas(batch_size);
            
            auto worker = [&]() {
                size_t iterations = RECORDS_PER_THREAD / batch_size;
                for (size_t i = 0; i < iterations; ++i) {
                    log.append(deltas);
                    total_appends.fetch_add(batch_size);
                }
            };
            
            auto start = high_resolution_clock::now();
            
            std::vector<std::thread> threads;
            for (size_t i = 0; i < num_threads; ++i) {
                threads.emplace_back(worker);
            }
            
            for (auto& t : threads) {
                t.join();
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            
            double throughput = (total_appends.load() * 1000000.0) / duration.count();
            
            // Print in millions
            std::cout << std::fixed << std::setprecision(1) 
                      << std::setw(7) << throughput / 1000000.0 << " M |";
            
            if (num_threads == 1) {
                single_thread_throughput = throughput;
            }
            
            // Calculate efficiency for 8 threads
            if (num_threads == 8 && single_thread_throughput > 0) {
                double efficiency = (throughput / single_thread_throughput) / 8.0 * 100;
                std::cout << std::setw(12) << std::setprecision(0) << efficiency << "%";
            }
            
            log.close();
            fs::remove(test_dir_ + "/concurrent.wal");
        }
        
        std::cout << "\n";
    }
    
    std::cout << "\nEfficiency = (8-thread throughput / single-thread throughput) / 8\n";
    std::cout << "Note: Larger batches dramatically improve concurrent efficiency!\n";
}

// ============================================================================
// Test 4: Optimal Batch Size Finding
// ============================================================================
TEST_F(WALBenchmark, OptimalBatchSize) {
    printSeparator("Finding Optimal Batch Size");
    
    const size_t BATCH_SIZES[] = {50, 100, 500, 1000, 2000, 5000, 10000};
    const size_t NUM_THREADS = 8;
    const size_t TOTAL_RECORDS = 500000;
    
    std::cout << "\nTesting with " << NUM_THREADS << " concurrent threads\n\n";
    
    std::cout << "Batch Size | Throughput  | Atomic Ops/M | Improvement\n";
    std::cout << "-----------|-------------|--------------|------------\n";
    
    double baseline_throughput = 0;
    double best_throughput = 0;
    size_t best_batch_size = 0;
    
    for (size_t batch_size : BATCH_SIZES) {
        OTDeltaLog log(test_dir_ + "/optimal.wal");
        ASSERT_TRUE(log.open_for_append());
        
        std::atomic<size_t> total_appends{0};
        std::atomic<size_t> total_batches{0};
        auto deltas = generateDeltas(batch_size);
        
        size_t records_per_thread = TOTAL_RECORDS / NUM_THREADS;
        
        auto worker = [&]() {
            size_t iterations = records_per_thread / batch_size;
            for (size_t i = 0; i < iterations; ++i) {
                log.append(deltas);
                total_appends.fetch_add(batch_size);
                total_batches.fetch_add(1);
            }
        };
        
        auto start = high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (size_t i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back(worker);
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        double throughput = (total_appends.load() * 1000000.0) / duration.count();
        size_t atomic_ops_per_million = (total_batches.load() * 1000000) / total_appends.load();
        
        if (batch_size == 100) {
            baseline_throughput = throughput;
        }
        
        double improvement = (baseline_throughput > 0) ? 
            ((throughput / baseline_throughput - 1) * 100) : 0;
        
        std::cout << std::setw(10) << batch_size << " | "
                  << std::fixed << std::setprecision(1) 
                  << std::setw(9) << throughput / 1000000.0 << " M/s | "
                  << std::setw(12) << atomic_ops_per_million << " | ";
        
        if (improvement > 0) {
            std::cout << "+" << std::setprecision(0) << std::setw(8) << improvement << "%";
        } else {
            std::cout << std::setw(10) << "baseline";
        }
        std::cout << "\n";
        
        if (throughput > best_throughput) {
            best_throughput = throughput;
            best_batch_size = batch_size;
        }
        
        log.close();
        fs::remove(test_dir_ + "/optimal.wal");
    }
    
    std::cout << "\n✓ Optimal batch size: " << best_batch_size 
              << " (achieves " << std::fixed << std::setprecision(1) 
              << best_throughput / 1000000.0 << " M records/sec)\n";
}

// ============================================================================
// Test 5: Payload-in-WAL Performance (EVENTUAL mode)
// ============================================================================
TEST_F(WALBenchmark, PayloadInWAL) {
    printSeparator("Payload-in-WAL Performance (EVENTUAL Mode)");
    
    const size_t PAYLOAD_SIZES[] = {512, 1024, 4096, 8192};
    const size_t NUM_RECORDS = 10000;
    const size_t BATCH_SIZE = 100;
    
    std::cout << "\nTarget: >100 MB/s for payloads <= 4KB\n\n";
    std::cout << "Payload | Throughput | Latency    | Status\n";
    std::cout << "--------|------------|------------|-------\n";
    
    for (size_t payload_size : PAYLOAD_SIZES) {
        OTDeltaLog log(test_dir_ + "/payload.wal");
        ASSERT_TRUE(log.open_for_append());
        
        auto deltas = generateDeltas(BATCH_SIZE);
        std::vector<std::vector<uint8_t>> payloads;
        std::vector<const void*> payload_ptrs;
        
        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            payloads.emplace_back(payload_size, 0x42);
            payload_ptrs.push_back(payloads.back().data());
            deltas[i].length = payload_size;
        }
        
        // Convert to DeltaWithPayload format
        std::vector<OTDeltaLog::DeltaWithPayload> dwp;
        dwp.reserve(BATCH_SIZE);
        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            dwp.push_back({deltas[i], payload_ptrs[i], payload_size});
        }
        
        size_t iterations = NUM_RECORDS / BATCH_SIZE;
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < iterations; ++i) {
            log.append_with_payloads(dwp);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        double throughput_mb = (NUM_RECORDS * payload_size) / (1024.0 * 1024.0) / 
                               (duration.count() / 1000000.0);
        double latency_us = duration.count() / double(iterations);
        
        bool meets_target = (payload_size <= 4096) ? (throughput_mb > 100) : true;
        const char* status = meets_target ? "✓ PASS" : "✗ FAIL";
        
        std::cout << std::setw(5) << payload_size << " B | "
                  << std::fixed << std::setprecision(1) << std::setw(8) << throughput_mb << " MB/s | "
                  << std::setprecision(0) << std::setw(8) << latency_us << " us | "
                  << status << "\n";
        
        if (payload_size <= 4096) {
            EXPECT_GT(throughput_mb, 100) 
                << "Failed to meet 100 MB/s target for " << payload_size << "B payloads";
        }
        
        log.close();
        fs::remove(test_dir_ + "/payload.wal");
    }
}

// ============================================================================
// Test 6: Summary and Recommendations
// ============================================================================
TEST_F(WALBenchmark, PerformanceSummary) {
    printSeparator("WAL Performance Summary");
    
    std::cout << "\n📊 Running comprehensive WAL benchmark suite...\n\n";
    
    // Test 1: Basic throughput with optimal batch size
    double basic_throughput = 0;
    {
        std::string log_path = test_dir_ + "/summary_basic.wal";
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        const size_t BATCH_SIZE = 1000;
        const size_t NUM_RECORDS = 100000;
        std::vector<OTDeltaRec> batch = generateDeltas(BATCH_SIZE);
        
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_RECORDS / BATCH_SIZE; ++i) {
            log.append(batch);
        }
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start);
        basic_throughput = (NUM_RECORDS * 1000000.0) / duration.count();
        
        std::cout << "Basic Throughput:\n";
        std::cout << "  • Batch size 1000: " << std::fixed << std::setprecision(1) 
                  << basic_throughput / 1000000.0 << "M records/sec\n";
        std::cout << "  • Target >1M/sec: " << (basic_throughput > 1000000 ? "✓ PASS" : "✗ FAIL") << "\n";
        
        log.close();
        fs::remove(log_path);
    }
    
    // Test 2: Sync latency
    double sync_latency_ms = 0;
    {
        std::string log_path = test_dir_ + "/summary_sync.wal";
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        const size_t BATCH_SIZE = 1000;
        std::vector<OTDeltaRec> batch = generateDeltas(BATCH_SIZE);
        
        // Measure multiple syncs
        const int NUM_SYNCS = 100;
        log.append(batch); // Initial data
        
        auto start = high_resolution_clock::now();
        for (int i = 0; i < NUM_SYNCS; ++i) {
            log.sync();
        }
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start);
        sync_latency_ms = duration.count() / (NUM_SYNCS * 1000.0);
        
        std::cout << "\nSync Latency:\n";
        std::cout << "  • Average sync time: " << std::fixed << std::setprecision(2) 
                  << sync_latency_ms << " ms\n";
        std::cout << "  • Target <10ms: " << (sync_latency_ms < 10 ? "✓ PASS" : "✗ FAIL") << "\n";
        
        log.close();
        fs::remove(log_path);
    }
    
    // Test 3: Concurrent throughput
    double concurrent_throughput = 0;
    {
        const size_t BATCH_SIZE = 5000;
        const size_t RECORDS_PER_THREAD = 100000;
        const int NUM_THREADS = 8;
        
        std::string log_path = test_dir_ + "/summary_concurrent.wal";
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        std::atomic<size_t> total_appended(0);
        
        auto start = high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int t = 0; t < NUM_THREADS; ++t) {
            threads.emplace_back([&, thread_batch_size = BATCH_SIZE]() {
                std::vector<OTDeltaRec> batch = generateDeltas(thread_batch_size);
                size_t iterations = RECORDS_PER_THREAD / thread_batch_size;
                
                for (size_t i = 0; i < iterations; ++i) {
                    log.append(batch);
                    total_appended += thread_batch_size;
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        concurrent_throughput = (total_appended * 1000000.0) / duration.count();
        
        std::cout << "\nConcurrent Performance:\n";
        std::cout << "  • " << NUM_THREADS << " threads, batch " << BATCH_SIZE << ": " 
                  << std::fixed << std::setprecision(1) 
                  << concurrent_throughput / 1000000.0 << "M records/sec\n";
        std::cout << "  • Scaling vs single thread: " 
                  << std::setprecision(1) << (concurrent_throughput / basic_throughput) << "x\n";
        
        log.close();
        fs::remove(log_path);
    }
    
    // Test 4: Payload throughput
    double payload_throughput_mb = 0;
    {
        std::string log_path = test_dir_ + "/summary_payload.wal";
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        const size_t PAYLOAD_SIZE = 1024; // 1KB payloads
        const size_t BATCH_SIZE = 100;
        const size_t NUM_RECORDS = 10000;
        
        std::vector<OTDeltaLog::DeltaWithPayload> dwp;
        dwp.reserve(BATCH_SIZE);
        
        // Store payloads separately to keep them alive
        std::vector<std::vector<uint8_t>> payloads;
        payloads.reserve(BATCH_SIZE);
        
        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            OTDeltaRec rec{};
            rec.handle_idx = i;
            rec.tag = 1;
            rec.birth_epoch = 1000 + i;
            rec.retire_epoch = ~uint64_t{0};
            rec.class_id = i % 7;
            rec.kind = static_cast<uint8_t>(NodeKind::Internal);
            rec.file_id = 0;
            rec.segment_id = i / 1000;
            rec.offset = (i % 1000) * 4096;
            rec.length = PAYLOAD_SIZE;
            
            payloads.emplace_back(PAYLOAD_SIZE, static_cast<uint8_t>(i & 0xFF));
            dwp.push_back({rec, payloads.back().data(), PAYLOAD_SIZE});
        }
        
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_RECORDS / BATCH_SIZE; ++i) {
            log.append_with_payloads(dwp);
        }
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start);
        payload_throughput_mb = (NUM_RECORDS * PAYLOAD_SIZE) / (1024.0 * 1024.0) / 
                               (duration.count() / 1000000.0);
        
        std::cout << "\nPayload Performance:\n";
        std::cout << "  • 1KB payload throughput: " << std::fixed << std::setprecision(1) 
                  << payload_throughput_mb << " MB/s\n";
        std::cout << "  • Target >100MB/s: " << (payload_throughput_mb > 100 ? "✓ PASS" : "✗ FAIL") << "\n";
        
        log.close();
        fs::remove(log_path);
    }
    
    // Summary and recommendations based on actual measurements
    std::cout << "\n🎯 Performance Targets vs Actual:\n";
    
    bool all_pass = true;
    
    // Check basic throughput
    bool basic_pass = basic_throughput > 1000000;
    all_pass &= basic_pass;
    std::cout << "  " << (basic_pass ? "✓" : "✗") << " Target: >1M rec/sec   Actual: " 
              << std::fixed << std::setprecision(1) << basic_throughput / 1000000.0 
              << "M rec/sec (" << (basic_throughput / 1000000.0) << "x target)\n";
    
    // Check sync latency
    bool sync_pass = sync_latency_ms < 10;
    all_pass &= sync_pass;
    std::cout << "  " << (sync_pass ? "✓" : "✗") << " Target: <10ms sync   Actual: " 
              << std::setprecision(2) << sync_latency_ms << "ms\n";
    
    // Check payload throughput
    bool payload_pass = payload_throughput_mb > 100;
    all_pass &= payload_pass;
    std::cout << "  " << (payload_pass ? "✓" : "✗") << " Target: >100MB/s WAL Actual: " 
              << std::setprecision(1) << payload_throughput_mb << " MB/s\n";
    
    std::cout << "\n💡 Data-Driven Recommendations:\n";
    
    // Recommendations based on actual performance
    if (concurrent_throughput / basic_throughput > 5) {
        std::cout << "  1. System scales well - use " << 8 << "+ threads for max throughput\n";
    } else {
        std::cout << "  1. Limited scaling - consider reducing thread count\n";
    }
    
    if (sync_latency_ms < 1) {
        std::cout << "  2. Excellent sync performance - can use STRICT durability\n";
    } else if (sync_latency_ms < 5) {
        std::cout << "  3. Good sync performance - BALANCED durability recommended\n";
    } else {
        std::cout << "  3. High sync latency - consider EVENTUAL durability\n";
    }
    
    if (payload_throughput_mb > 200) {
        std::cout << "  4. Payload-in-WAL suitable for records up to 8KB\n";
    } else if (payload_throughput_mb > 100) {
        std::cout << "  4. Payload-in-WAL suitable for records up to 4KB\n";
    } else {
        std::cout << "  4. Consider separate storage for large payloads\n";
    }
    
    std::cout << "\nOverall Status: " << (all_pass ? "✓ ALL TARGETS MET" : "⚠ SOME TARGETS MISSED") << "\n";
    
    std::cout << "\n" << std::string(60, '=') << "\n\n";
    
    // Assert that we meet critical targets
    EXPECT_GT(basic_throughput, 1000000) << "WAL should achieve >1M records/sec";
    EXPECT_LT(sync_latency_ms, 10) << "Sync latency should be <10ms";
    EXPECT_GT(payload_throughput_mb, 100) << "Payload throughput should be >100MB/s";
}