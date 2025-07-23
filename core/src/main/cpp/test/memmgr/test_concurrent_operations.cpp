/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test concurrent search and indexing operations
 */

#include <gtest/gtest.h>
#include "../src/memmgr/concurrent_compact_allocator.hpp"
#include "../src/memmgr/compact_allocator.hpp"
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <iostream>

using namespace xtree;
using namespace std::chrono;

class ConcurrentOperationsTest : public ::testing::Test {
protected:
    void SetUp() override {
        allocator = std::make_unique<ConcurrentCompactAllocator>(64 * 1024 * 1024);
    }
    
    std::unique_ptr<ConcurrentCompactAllocator> allocator;
};

TEST_F(ConcurrentOperationsTest, ConcurrentAllocation) {
    const int NUM_THREADS = 8;
    const int ALLOCATIONS_PER_THREAD = 1000;
    
    std::vector<std::thread> threads;
    std::vector<std::vector<ConcurrentCompactAllocator::offset_t>> thread_offsets(NUM_THREADS);
    
    auto start = high_resolution_clock::now();
    
    // Launch threads to perform concurrent allocations
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([this, t, &thread_offsets]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> size_dist(64, 1024);
            
            for (int i = 0; i < ALLOCATIONS_PER_THREAD; i++) {
                size_t size = size_dist(gen);
                auto offset = allocator->allocate(size);
                EXPECT_NE(offset, ConcurrentCompactAllocator::INVALID_OFFSET);
                thread_offsets[t].push_back(offset);
                
                // Write thread ID and index
                int* ptr = allocator->get_ptr_write<int>(offset);
                ptr[0] = t;
                ptr[1] = i;
            }
        });
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    
    std::cout << "\nConcurrent Allocation Test:\n";
    std::cout << "  Threads: " << NUM_THREADS << "\n";
    std::cout << "  Total allocations: " << (NUM_THREADS * ALLOCATIONS_PER_THREAD) << "\n";
    std::cout << "  Time: " << duration << " ms\n";
    std::cout << "  Throughput: " << (NUM_THREADS * ALLOCATIONS_PER_THREAD * 1000.0 / duration) 
              << " allocations/sec\n";
    
    // Verify all allocations
    for (int t = 0; t < NUM_THREADS; t++) {
        for (size_t i = 0; i < thread_offsets[t].size(); i++) {
            const int* ptr = allocator->get_ptr_read<int>(thread_offsets[t][i]);
            EXPECT_EQ(ptr[0], t);
            EXPECT_EQ(ptr[1], static_cast<int>(i));
        }
    }
}

TEST_F(ConcurrentOperationsTest, ConcurrentReadWrite) {
    const int NUM_WRITERS = 2;
    const int NUM_READERS = 6;
    const int OPERATIONS = 10000;
    
    // Pre-allocate some data
    std::vector<ConcurrentCompactAllocator::offset_t> data_offsets;
    for (int i = 0; i < 1000; i++) {
        auto offset = allocator->allocate(sizeof(int));
        data_offsets.push_back(offset);
        *allocator->get_ptr_write<int>(offset) = i;
    }
    
    std::atomic<int> write_count{0};
    std::atomic<int> read_count{0};
    std::atomic<bool> stop{false};
    
    std::vector<std::thread> threads;
    
    // Writer threads
    for (int w = 0; w < NUM_WRITERS; w++) {
        threads.emplace_back([this, &data_offsets, &write_count, &stop]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> idx_dist(0, data_offsets.size() - 1);
            
            while (!stop.load()) {
                int idx = idx_dist(gen);
                int* ptr = allocator->get_ptr_write<int>(data_offsets[idx]);
                (*ptr)++;
                write_count.fetch_add(1);
            }
        });
    }
    
    // Reader threads
    for (int r = 0; r < NUM_READERS; r++) {
        threads.emplace_back([this, &data_offsets, &read_count, &stop]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> idx_dist(0, data_offsets.size() - 1);
            
            int sum = 0;
            while (!stop.load()) {
                int idx = idx_dist(gen);
                const int* ptr = allocator->get_ptr_read<int>(data_offsets[idx]);
                sum += *ptr;
                read_count.fetch_add(1);
                
                // Prevent optimization
                if (sum == INT_MAX) std::cout << sum;
            }
        });
    }
    
    // Run for a fixed time
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true);
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "\nConcurrent Read/Write Test:\n";
    std::cout << "  Writers: " << NUM_WRITERS << "\n";
    std::cout << "  Readers: " << NUM_READERS << "\n";
    std::cout << "  Write operations: " << write_count.load() << "\n";
    std::cout << "  Read operations: " << read_count.load() << "\n";
    std::cout << "  Read/Write ratio: " << (double)read_count.load() / write_count.load() << ":1\n";
}

TEST_F(ConcurrentOperationsTest, SegmentedSearchSimulation) {
    // Simulate searching across segments
    const size_t RECORDS_PER_SEGMENT = 100000;
    const size_t NUM_SEGMENTS = 3;
    
    std::cout << "\nSegmented Search Simulation:\n";
    
    // Allocate data across multiple segments
    std::vector<ConcurrentCompactAllocator::offset_t> record_offsets;
    
    // Force allocation across segments by allocating large chunks
    for (size_t seg = 0; seg < NUM_SEGMENTS; seg++) {
        std::cout << "  Allocating segment " << seg << "...\n";
        
        for (size_t i = 0; i < RECORDS_PER_SEGMENT; i++) {
            auto offset = allocator->allocate(100); // 100 bytes per record
            record_offsets.push_back(offset);
            
            // Store record ID
            uint32_t* ptr = allocator->get_ptr_write<uint32_t>(offset);
            *ptr = seg * RECORDS_PER_SEGMENT + i;
        }
    }
    
    std::cout << "  Total records: " << record_offsets.size() << "\n";
    std::cout << "  Memory used: " << (allocator->get_used_size() / (1024.0 * 1024.0)) << " MB\n";
    
    // Concurrent search simulation
    const int NUM_SEARCHERS = 4;
    std::atomic<int> matches{0};
    std::vector<std::thread> searchers;
    
    auto search_start = high_resolution_clock::now();
    
    for (int s = 0; s < NUM_SEARCHERS; s++) {
        searchers.emplace_back([this, &record_offsets, &matches, s]() {
            // Each searcher looks for records matching a pattern
            uint32_t target = s * 1000;
            
            for (const auto& offset : record_offsets) {
                const uint32_t* ptr = allocator->get_ptr_read<uint32_t>(offset);
                if (*ptr % 1000 == target % 1000) {
                    matches.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : searchers) {
        thread.join();
    }
    
    auto search_end = high_resolution_clock::now();
    auto search_time = duration_cast<milliseconds>(search_end - search_start).count();
    
    std::cout << "  Concurrent searchers: " << NUM_SEARCHERS << "\n";
    std::cout << "  Search time: " << search_time << " ms\n";
    std::cout << "  Records scanned per second: " 
              << (record_offsets.size() * NUM_SEARCHERS * 1000.0 / search_time) << "\n";
    std::cout << "  Matches found: " << matches.load() << "\n";
}

TEST_F(ConcurrentOperationsTest, StressTestMixedOperations) {
    // Stress test with mixed read/write operations
    const int NUM_THREADS = 10;
    const int DURATION_SECONDS = 5;
    
    std::atomic<bool> stop{false};
    std::atomic<int> total_ops{0};
    std::atomic<int> allocation_failures{0};
    
    std::vector<std::thread> threads;
    std::vector<ConcurrentCompactAllocator::offset_t> shared_data;
    std::mutex shared_data_mutex;
    
    // Pre-populate some data
    for (int i = 0; i < 1000; i++) {
        auto offset = allocator->allocate(256);
        shared_data.push_back(offset);
    }
    
    std::cout << "\nStress Test - Mixed Operations:\n";
    
    // Launch mixed operation threads
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([this, &stop, &total_ops, &allocation_failures, 
                             &shared_data, &shared_data_mutex, t]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> op_dist(0, 99);
            std::uniform_int_distribution<> size_dist(64, 1024);
            
            while (!stop.load()) {
                int op = op_dist(gen);
                
                if (op < 20) {  // 20% allocations
                    size_t size = size_dist(gen);
                    auto offset = allocator->allocate(size);
                    
                    if (offset != ConcurrentCompactAllocator::INVALID_OFFSET) {
                        std::lock_guard<std::mutex> lock(shared_data_mutex);
                        shared_data.push_back(offset);
                    } else {
                        allocation_failures.fetch_add(1);
                    }
                    
                } else if (op < 30) {  // 10% writes
                    std::lock_guard<std::mutex> lock(shared_data_mutex);
                    if (!shared_data.empty()) {
                        std::uniform_int_distribution<> idx_dist(0, shared_data.size() - 1);
                        int idx = idx_dist(gen);
                        
                        int* ptr = allocator->get_ptr_write<int>(shared_data[idx]);
                        if (ptr) {
                            *ptr = t * 1000 + op;
                        }
                    }
                    
                } else {  // 70% reads
                    std::lock_guard<std::mutex> lock(shared_data_mutex);
                    if (!shared_data.empty()) {
                        std::uniform_int_distribution<> idx_dist(0, shared_data.size() - 1);
                        int idx = idx_dist(gen);
                        
                        const int* ptr = allocator->get_ptr_read<int>(shared_data[idx]);
                        if (ptr) {
                            volatile int value = *ptr;  // Prevent optimization
                            (void)value;
                        }
                    }
                }
                
                total_ops.fetch_add(1);
            }
        });
    }
    
    // Run for fixed duration
    std::this_thread::sleep_for(std::chrono::seconds(DURATION_SECONDS));
    stop.store(true);
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "  Threads: " << NUM_THREADS << "\n";
    std::cout << "  Duration: " << DURATION_SECONDS << " seconds\n";
    std::cout << "  Total operations: " << total_ops.load() << "\n";
    std::cout << "  Operations/sec: " << (total_ops.load() / DURATION_SECONDS) << "\n";
    std::cout << "  Final data items: " << shared_data.size() << "\n";
    std::cout << "  Allocation failures: " << allocation_failures.load() << "\n";
    std::cout << "  Memory used: " << (allocator->get_used_size() / (1024.0 * 1024.0)) << " MB\n";
}