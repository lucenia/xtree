/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Segment Allocator Performance Benchmarks
 * Tests critical allocation and deallocation performance
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <filesystem>
#include <atomic>
#include <thread>
#include "../../src/persistence/segment_allocator.h"
#include "../../src/persistence/segment_classes.hpp"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class SegmentAllocatorPerformanceBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    std::unique_ptr<SegmentAllocator> allocator_;
    
    void SetUp() override {
        test_dir_ = "/tmp/segment_bench_" + std::to_string(getpid());
        fs::create_directories(test_dir_);
        allocator_ = std::make_unique<SegmentAllocator>(test_dir_);
    }
    
    void TearDown() override {
        allocator_.reset();
        fs::remove_all(test_dir_);
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
};

TEST_F(SegmentAllocatorPerformanceBenchmark, AllocationThroughput) {
    printSeparator("Segment Allocation Hot Path");
    
    const size_t ALLOCATION_SIZES[] = {512, 1024, 4096, 8192, 16384, 32768};
    const size_t NUM_ALLOCATIONS = 100000;
    
    std::cout << "\nMeasuring allocation throughput (hot path):\n\n";
    std::cout << "Size    | Allocations/sec | MB/s    | ns/alloc | Status\n";
    std::cout << "--------|-----------------|---------|----------|--------\n";
    
    for (size_t alloc_size : ALLOCATION_SIZES) {
        std::vector<SegmentAllocator::Allocation> results;
        results.reserve(NUM_ALLOCATIONS);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_ALLOCATIONS; ++i) {
            auto alloc = allocator_->allocate(alloc_size);
            results.push_back(std::move(alloc));
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_ALLOCATIONS * 1e9) / duration.count();
        double mb_per_sec = (NUM_ALLOCATIONS * alloc_size) / (1024.0 * 1024.0) / 
                           (duration.count() / 1e9);
        double ns_per_alloc = duration.count() / double(NUM_ALLOCATIONS);
        
        // Verify all allocations succeeded
        size_t valid_count = 0;
        for (const auto& alloc : results) {
            if (alloc.is_valid()) valid_count++;
        }
        
        // Target: <100ns per allocation for small sizes
        bool meets_target = (alloc_size <= 4096) ? (ns_per_alloc < 100) : (ns_per_alloc < 200);
        const char* status = meets_target ? "✓ FAST" : "⚠ SLOW";
        
        std::cout << std::setw(7) << alloc_size << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(15) << throughput << " | "
                  << std::setprecision(1) << std::setw(7) << mb_per_sec << " | "
                  << std::setprecision(0) << std::setw(8) << ns_per_alloc << " | "
                  << status << "\n";
        
        EXPECT_EQ(valid_count, NUM_ALLOCATIONS) << "Some allocations failed";
    }
    
    std::cout << "\n💡 Target: <100ns per allocation for sizes ≤4KB\n";
}

TEST_F(SegmentAllocatorPerformanceBenchmark, SizeClassPerformance) {
    printSeparator("Size Class Allocation Performance");
    
    std::cout << "\nMeasuring allocation by size class:\n\n";
    std::cout << "Class | Size    | Allocs/sec   | ns/alloc | Fill Rate | Status\n";
    std::cout << "------|---------|--------------|----------|-----------|--------\n";
    
    for (uint8_t cls = 0; cls <= 6; ++cls) {
        size_t class_size = class_to_size(cls);
        const size_t NUM_ALLOCS = 50000;
        
        std::vector<SegmentAllocator::Allocation> results;
        results.reserve(NUM_ALLOCS);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_ALLOCS; ++i) {
            // Allocate exactly at class boundary for optimal packing
            auto alloc = allocator_->allocate(class_size);
            results.push_back(std::move(alloc));
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_ALLOCS * 1e9) / duration.count();
        double ns_per_alloc = duration.count() / double(NUM_ALLOCS);
        
        // Calculate segment fill rate
        auto stats = allocator_->get_stats(0);  // Get stats for first class
        double fill_rate = (NUM_ALLOCS * class_size) / 
                          double(stats.active_segments * SegmentAllocator::DEFAULT_SEGMENT_SIZE);
        
        bool efficient = fill_rate > 0.8;  // >80% fill rate
        const char* status = efficient ? "✓ EFF" : "⚠ FRAG";
        
        std::cout << std::setw(5) << (int)cls << " | "
                  << std::setw(7) << class_size << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(12) << throughput << " | "
                  << std::setw(8) << ns_per_alloc << " | "
                  << std::setprecision(2) << std::setw(9) << fill_rate << " | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 Each size class should achieve >80% fill rate\n";
}

TEST_F(SegmentAllocatorPerformanceBenchmark, ConcurrentAllocationScaling) {
    printSeparator("Concurrent Allocation Scaling");
    
    const int THREAD_COUNTS[] = {1, 2, 4, 8, 16};
    const size_t ALLOCS_PER_THREAD = 10000;
    const size_t ALLOC_SIZE = 4096;
    
    std::cout << "\nMeasuring concurrent allocation scaling:\n\n";
    std::cout << "Threads | Total Throughput | Per-Thread   | Scaling | Status\n";
    std::cout << "--------|------------------|--------------|---------|--------\n";
    
    double single_thread_throughput = 0;
    
    for (int num_threads : THREAD_COUNTS) {
        std::atomic<size_t> total_allocated(0);
        std::atomic<size_t> total_time_ns(0);
        
        auto worker = [&]() {
            auto thread_start = high_resolution_clock::now();
            
            for (size_t i = 0; i < ALLOCS_PER_THREAD; ++i) {
                auto alloc = allocator_->allocate(ALLOC_SIZE);
                if (alloc.is_valid()) {
                    total_allocated++;
                }
            }
            
            auto thread_end = high_resolution_clock::now();
            auto thread_duration = duration_cast<nanoseconds>(thread_end - thread_start);
            total_time_ns += thread_duration.count();
        };
        
        auto start = high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back(worker);
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto wall_duration = duration_cast<nanoseconds>(end - start);
        
        double total_throughput = (total_allocated * 1e9) / wall_duration.count();
        double per_thread_throughput = total_throughput / num_threads;
        
        if (num_threads == 1) {
            single_thread_throughput = total_throughput;
        }
        
        double scaling = (single_thread_throughput > 0) ? 
            (total_throughput / single_thread_throughput) : 1.0;
        
        // Good scaling: >0.7x per thread up to 8 threads
        bool good_scaling = (num_threads <= 8) ? 
            (scaling >= num_threads * 0.7) : (scaling >= 5.6);
        const char* status = good_scaling ? "✓ GOOD" : "⚠ CONT";
        
        std::cout << std::setw(7) << num_threads << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(16) << total_throughput << " | "
                  << std::setw(12) << per_thread_throughput << " | "
                  << std::setprecision(2) << std::setw(7) << scaling << "x | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 Should maintain >70% scaling efficiency up to 8 threads\n";
}

TEST_F(SegmentAllocatorPerformanceBenchmark, AllocationDeallocationChurn) {
    printSeparator("Allocation/Deallocation Churn");
    
    std::cout << "\nMeasuring allocation/free patterns:\n\n";
    std::cout << "Pattern            | Ops/sec      | Fragmentation | Memory | Status\n";
    std::cout << "-------------------|--------------|---------------|--------|--------\n";
    
    struct ChurnPattern {
        const char* name;
        std::function<void(SegmentAllocator*, std::vector<SegmentAllocator::Allocation>&)> execute;
    };
    
    ChurnPattern patterns[] = {
        {"FIFO (queue-like)", [](SegmentAllocator* alloc, std::vector<SegmentAllocator::Allocation>& results) {
            const size_t WINDOW = 1000;
            const size_t ITERATIONS = 100000;
            
            // Pre-fill
            for (size_t i = 0; i < WINDOW; ++i) {
                results.push_back(std::move(alloc->allocate(4096)));
            }
            
            // Churn: free oldest, allocate new
            for (size_t i = 0; i < ITERATIONS; ++i) {
                if (!results.empty()) {
                    alloc->free(results.front());
                    results.erase(results.begin());
                }
                results.push_back(std::move(alloc->allocate(4096)));
            }
        }},
        
        {"LIFO (stack-like)", [](SegmentAllocator* alloc, std::vector<SegmentAllocator::Allocation>& results) {
            const size_t ITERATIONS = 100000;
            
            for (size_t i = 0; i < ITERATIONS; ++i) {
                if (i % 2 == 0) {
                    results.push_back(std::move(alloc->allocate(4096)));
                } else if (!results.empty()) {
                    alloc->free(results.back());
                    results.pop_back();
                }
            }
        }},
        
        {"Random", [](SegmentAllocator* alloc, std::vector<SegmentAllocator::Allocation>& results) {
            std::mt19937 rng(42);
            const size_t ITERATIONS = 100000;
            
            for (size_t i = 0; i < ITERATIONS; ++i) {
                if (results.empty() || (rng() % 2 == 0 && results.size() < 5000)) {
                    results.push_back(std::move(alloc->allocate(4096)));
                } else {
                    std::uniform_int_distribution<size_t> dist(0, results.size() - 1);
                    size_t idx = dist(rng);
                    alloc->free(results[idx]);
                    results.erase(results.begin() + idx);
                }
            }
        }}
    };
    
    for (const auto& pattern : patterns) {
        // Reset allocator
        allocator_ = std::make_unique<SegmentAllocator>(test_dir_ + "/" + pattern.name);
        std::vector<SegmentAllocator::Allocation> results;
        
        auto start = high_resolution_clock::now();
        pattern.execute(allocator_.get(), results);
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start);
        double ops_per_sec = (100000.0 * 1e6) / duration.count();
        
        auto stats = allocator_->get_stats(0);  // Get stats for first class
        double fragmentation = 1.0 - ((double)stats.live_bytes / 
                                      (stats.active_segments * SegmentAllocator::DEFAULT_SEGMENT_SIZE));
        double memory_mb = (stats.active_segments * SegmentAllocator::DEFAULT_SEGMENT_SIZE) / (1024.0 * 1024.0);
        
        bool efficient = fragmentation < 0.3;  // <30% fragmentation
        const char* status = efficient ? "✓ OK" : "⚠ FRAG";
        
        std::cout << std::setw(18) << pattern.name << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(12) << ops_per_sec << " | "
                  << std::setprecision(1) << std::setw(12) << fragmentation * 100 << "% | "
                  << std::setw(5) << memory_mb << "MB | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 Fragmentation should stay below 30% under churn\n";
}

TEST_F(SegmentAllocatorPerformanceBenchmark, Summary) {
    printSeparator("Segment Allocator Performance Summary");
    
    std::cout << "\n📊 Validating critical hot path performance...\n\n";
    
    const size_t NUM_WARMUP = 10000;
    const size_t NUM_MEASURE = 100000;
    
    // Warmup
    for (size_t i = 0; i < NUM_WARMUP; ++i) {
        allocator_->allocate(4096);
    }
    
    // Measure allocation hot path
    auto alloc_start = high_resolution_clock::now();
    std::vector<SegmentAllocator::Allocation> results;
    for (size_t i = 0; i < NUM_MEASURE; ++i) {
        results.push_back(std::move(allocator_->allocate(4096)));
    }
    auto alloc_end = high_resolution_clock::now();
    
    auto alloc_duration = duration_cast<nanoseconds>(alloc_end - alloc_start);
    double ns_per_alloc = alloc_duration.count() / double(NUM_MEASURE);
    double alloc_throughput = (NUM_MEASURE * 1e9) / alloc_duration.count();
    
    // Measure deallocation hot path
    auto free_start = high_resolution_clock::now();
    for (auto& alloc : results) {  // Non-const to allow moving
        allocator_->free(alloc);
    }
    auto free_end = high_resolution_clock::now();
    
    auto free_duration = duration_cast<nanoseconds>(free_end - free_start);
    double ns_per_free = free_duration.count() / double(NUM_MEASURE);
    double free_throughput = (NUM_MEASURE * 1e9) / free_duration.count();
    
    std::cout << "Allocation Hot Path:\n";
    std::cout << "  • " << std::fixed << std::setprecision(0)
              << ns_per_alloc << " ns/allocation\n";
    std::cout << "  • " << alloc_throughput / 1e6 << "M allocations/sec\n";
    std::cout << "  • Target <100ns: " 
              << (ns_per_alloc < 100 ? "✓ PASS" : "✗ FAIL") << "\n";
    
    std::cout << "\nDeallocation Hot Path:\n";
    std::cout << "  • " << ns_per_free << " ns/deallocation\n";
    std::cout << "  • " << free_throughput / 1e6 << "M deallocations/sec\n";
    std::cout << "  • Target <50ns: "
              << (ns_per_free < 50 ? "✓ PASS" : "✗ FAIL") << "\n";
    
    std::cout << "\nThroughput:\n";
    std::cout << "  • Combined: " << (alloc_throughput + free_throughput) / 2e6 << "M ops/sec\n";
    std::cout << "  • Target >10M ops/sec: "
              << (alloc_throughput > 10e6 ? "✓ PASS" : "✗ FAIL") << "\n";
    
    auto stats = allocator_->get_stats(0);  // Get stats for first class
    double fill_rate = (stats.live_bytes * 100.0 / (stats.active_segments * SegmentAllocator::DEFAULT_SEGMENT_SIZE));
    
    std::cout << "\nMemory Efficiency:\n";
    std::cout << "  • Fill rate: " << std::setprecision(1) << fill_rate << "%\n";
    std::cout << "  • Memory used: " 
              << std::setprecision(0)
              << (stats.active_segments * SegmentAllocator::DEFAULT_SEGMENT_SIZE) / (1024.0 * 1024.0) << " MB\n";
    std::cout << "  • Target >80% fill: "
              << (fill_rate > 80 ? "✓ PASS" : "✗ FAIL") << "\n";
    
    std::cout << "\n🎯 Performance Targets:\n";
    std::cout << "  ✓ Allocation: <100ns per operation\n";
    std::cout << "  ✓ Deallocation: <50ns per operation\n";
    std::cout << "  ✓ Throughput: >10M ops/sec\n";
    std::cout << "  ✓ Fill Rate: >80% memory utilization\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}