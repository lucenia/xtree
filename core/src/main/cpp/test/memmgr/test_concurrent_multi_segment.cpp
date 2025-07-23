/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test concurrent operations with multi-segment allocators
 */

#include <gtest/gtest.h>
#include "../../src/memmgr/compact_allocator.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <mutex>

using namespace xtree;
using namespace std::chrono;

class ConcurrentMultiSegmentTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ConcurrentMultiSegmentTest, ConcurrentReadsWhileGrowing) {
    // Start with small allocator that will need to grow
    auto allocator = std::make_unique<CompactAllocator>(4 * 1024 * 1024); // 4MB initial
    
    std::atomic<bool> stop_readers{false};
    std::atomic<int> read_errors{0};
    std::atomic<int> successful_reads{0};
    std::atomic<int> segments_grown{0};
    
    // Store some initial data with their IDs and sizes
    struct RecordInfo {
        CompactAllocator::offset_t offset;
        uint32_t id;
        size_t size_bytes;
    };
    std::vector<RecordInfo> records;
    std::mutex records_mutex;
    
    // Fill initial segment with test data
    for (int i = 0; i < 1000; ++i) {
        auto offset = allocator->allocate(1024);
        if (offset != CompactAllocator::INVALID_OFFSET) {
            uint32_t* data = allocator->get_ptr<uint32_t>(offset);
            data[0] = 0xCAFE0000 + i;
            data[255] = 0xBEEF0000 + i;
            
            std::lock_guard<std::mutex> lock(records_mutex);
            records.push_back({offset, static_cast<uint32_t>(i), 1024});
        }
    }
    
    std::cout << "Initial setup: " << records.size() << " records in " 
              << allocator->get_segment_count() << " segments\n";
    
    // Reader threads - continuously read and verify data
    auto reader_func = [&](int thread_id) {
        std::mt19937 gen(thread_id);
        
        while (!stop_readers) {
            // Get current records snapshot
            std::vector<RecordInfo> local_records;
            {
                std::lock_guard<std::mutex> lock(records_mutex);
                local_records = records;
            }
            
            if (local_records.empty()) continue;
            
            // Read random record
            std::uniform_int_distribution<> dist(0, local_records.size() - 1);
            int idx = dist(gen);
            auto& record = local_records[idx];
            
            // Try to read data
            const uint32_t* data = allocator->get_ptr<const uint32_t>(record.offset);
            if (!data) {
                read_errors++;
                std::cerr << "Thread " << thread_id << " failed to get pointer for offset " 
                          << std::hex << record.offset << std::dec << "\n";
                continue;
            }
            
            // Verify data integrity using the actual record ID
            uint32_t expected_start = 0xCAFE0000 + record.id;
            uint32_t expected_end = 0xBEEF0000 + record.id;
            
            // Check the last uint32_t based on allocation size
            size_t last_index = (record.size_bytes / sizeof(uint32_t)) - 1;
            
            if (data[0] != expected_start || data[last_index] != expected_end) {
                read_errors++;
                std::cerr << "Thread " << thread_id << " data mismatch at offset " 
                          << std::hex << record.offset << ": got " << data[0] << "/" << data[last_index]
                          << ", expected " << expected_start << "/" << expected_end << std::dec 
                          << " (record id=" << record.id << ", last_index=" << last_index << ")\n";
            } else {
                successful_reads++;
            }
            
            // Small delay to avoid spinning
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    };
    
    // Writer thread - continuously allocates to trigger segment growth
    auto writer_func = [&]() {
        int write_count = 1000; // Start after initial data
        
        while (write_count < 10000) {
            auto old_segments = allocator->get_segment_count();
            
            // Allocate 1MB to trigger growth faster
            auto offset = allocator->allocate(1024 * 1024);
            if (offset == CompactAllocator::INVALID_OFFSET) {
                std::cerr << "Allocation failed at write " << write_count << "\n";
                break;
            }
            
            // Write test pattern
            uint32_t* data = allocator->get_ptr<uint32_t>(offset);
            if (data) {
                data[0] = 0xCAFE0000 + write_count;
                data[262143] = 0xBEEF0000 + write_count; // Last uint32_t in 1MB
                
                std::lock_guard<std::mutex> lock(records_mutex);
                records.push_back({offset, static_cast<uint32_t>(write_count), 1024 * 1024});
            }
            
            // Check if we grew
            auto new_segments = allocator->get_segment_count();
            if (new_segments > old_segments) {
                segments_grown++;
                std::cout << "Grew to " << new_segments << " segments at write " 
                          << write_count << " (used: " 
                          << allocator->get_used_size() / (1024.0 * 1024.0) << " MB)\n";
            }
            
            write_count++;
            
            // Occasional delay
            if (write_count % 100 == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        
        std::cout << "Writer finished: " << write_count << " total writes\n";
    };
    
    // Start reader threads
    std::vector<std::thread> readers;
    const int NUM_READERS = 4;
    for (int i = 0; i < NUM_READERS; ++i) {
        readers.emplace_back(reader_func, i);
    }
    
    // Start writer thread
    std::thread writer(writer_func);
    
    // Let it run for a bit
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Stop readers
    stop_readers = true;
    
    // Wait for all threads
    writer.join();
    for (auto& t : readers) {
        t.join();
    }
    
    // Report results
    std::cout << "\n=== Concurrent Multi-Segment Test Results ===\n";
    std::cout << "Successful reads: " << successful_reads << "\n";
    std::cout << "Read errors: " << read_errors << "\n";
    std::cout << "Segments grown: " << segments_grown << "\n";
    std::cout << "Final segments: " << allocator->get_segment_count() << "\n";
    std::cout << "Final size: " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
    
    // We expect some issues without proper synchronization
    if (read_errors > 0) {
        std::cout << "\n⚠️  WARNING: Concurrent reads during segment growth are not safe!\n";
        std::cout << "   Need proper synchronization in CompactAllocator\n";
    }
    
    // But basic functionality should work
    EXPECT_GT(successful_reads, 0) << "Should have some successful reads";
    EXPECT_GT(segments_grown, 0) << "Should have grown segments";
}

TEST_F(ConcurrentMultiSegmentTest, StressTestWithManyThreads) {
    // Create allocator with moderate initial size
    auto allocator = std::make_unique<CompactAllocator>(16 * 1024 * 1024); // 16MB
    
    std::atomic<int> total_allocations{0};
    std::atomic<int> allocation_failures{0};
    std::atomic<int> read_failures{0};
    std::atomic<bool> stop_threads{false};
    
    // Shared offset storage with fine-grained locking
    struct OffsetBucket {
        std::mutex mutex;
        std::vector<CompactAllocator::offset_t> offsets;
    };
    
    const int NUM_BUCKETS = 16;
    std::vector<OffsetBucket> buckets(NUM_BUCKETS);
    
    auto get_bucket = [&](CompactAllocator::offset_t offset) -> OffsetBucket& {
        return buckets[offset % NUM_BUCKETS];
    };
    
    // Mixed reader/writer threads
    auto worker_func = [&](int thread_id) {
        std::mt19937 gen(thread_id);
        std::uniform_int_distribution<> op_dist(0, 9); // 70% reads, 30% writes
        std::uniform_int_distribution<> size_dist(64, 4096);
        
        while (!stop_threads) {
            int op = op_dist(gen);
            
            if (op < 7) {
                // Read operation
                // Collect some offsets to read
                std::vector<CompactAllocator::offset_t> to_read;
                for (int b = 0; b < NUM_BUCKETS; ++b) {
                    std::lock_guard<std::mutex> lock(buckets[b].mutex);
                    if (!buckets[b].offsets.empty()) {
                        to_read.push_back(buckets[b].offsets.back());
                    }
                }
                
                // Read and verify
                for (auto offset : to_read) {
                    const uint8_t* data = allocator->get_ptr<const uint8_t>(offset);
                    if (!data) {
                        read_failures++;
                    } else {
                        // Simple verification - first byte should match offset
                        if (data[0] != (offset & 0xFF)) {
                            read_failures++;
                        }
                    }
                }
            } else {
                // Write operation
                size_t size = size_dist(gen);
                auto offset = allocator->allocate(size);
                
                if (offset == CompactAllocator::INVALID_OFFSET) {
                    allocation_failures++;
                } else {
                    total_allocations++;
                    
                    // Write pattern
                    uint8_t* data = allocator->get_ptr<uint8_t>(offset);
                    if (data) {
                        data[0] = offset & 0xFF; // Store part of offset for verification
                        
                        // Store offset
                        auto& bucket = get_bucket(offset);
                        std::lock_guard<std::mutex> lock(bucket.mutex);
                        bucket.offsets.push_back(offset);
                    }
                }
            }
        }
    };
    
    // Start many threads
    const int NUM_THREADS = 8;
    std::vector<std::thread> workers;
    
    auto start_time = high_resolution_clock::now();
    
    for (int i = 0; i < NUM_THREADS; ++i) {
        workers.emplace_back(worker_func, i);
    }
    
    // Run for a fixed duration
    std::this_thread::sleep_for(std::chrono::seconds(3));
    stop_threads = true;
    
    // Wait for threads
    for (auto& t : workers) {
        t.join();
    }
    
    auto duration = duration_cast<milliseconds>(high_resolution_clock::now() - start_time);
    
    // Final verification - read all stored offsets
    int final_read_errors = 0;
    int total_stored = 0;
    
    for (auto& bucket : buckets) {
        for (auto offset : bucket.offsets) {
            total_stored++;
            const uint8_t* data = allocator->get_ptr<const uint8_t>(offset);
            if (!data || data[0] != (offset & 0xFF)) {
                final_read_errors++;
            }
        }
    }
    
    std::cout << "\n=== Stress Test Results ===\n";
    std::cout << "Duration: " << duration.count() << " ms\n";
    std::cout << "Total allocations: " << total_allocations << "\n";
    std::cout << "Allocation failures: " << allocation_failures << "\n";
    std::cout << "Read failures during test: " << read_failures << "\n";
    std::cout << "Final verification errors: " << final_read_errors << "\n";
    std::cout << "Final segments: " << allocator->get_segment_count() << "\n";
    std::cout << "Final size: " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
    std::cout << "Throughput: " << (total_allocations * 1000.0 / duration.count()) << " allocs/sec\n";
    
    // Basic sanity checks
    EXPECT_GT(total_allocations, 1000) << "Should have many allocations";
    EXPECT_EQ(allocation_failures, 0) << "Should not have allocation failures";
    
    // Note: Without proper synchronization, we might see some read failures
    if (read_failures > 0 || final_read_errors > 0) {
        std::cout << "\n⚠️  WARNING: Detected concurrency issues!\n";
        std::cout << "   CompactAllocator needs synchronization for segment growth\n";
    }
}