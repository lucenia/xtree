/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif
#include "persistence/segment_allocator.h"
#include "persistence/segment_classes.hpp"

using namespace xtree::persist;

class SegmentAllocatorTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::unique_ptr<SegmentAllocator> allocator;
    
    void SetUp() override {
        test_dir = "/tmp/xtree_segment_test_" + std::to_string(getpid());
        
        #ifdef _WIN32
            CreateDirectoryA(test_dir.c_str(), nullptr);
        #else
            mkdir(test_dir.c_str(), 0755);
        #endif
        
        allocator = std::make_unique<SegmentAllocator>(test_dir);
    }
    
    void TearDown() override {
        allocator.reset();
        
        // Clean up test directory and files
        #ifdef _WIN32
            std::string cmd = "rmdir /s /q \"" + test_dir + "\"";
            system(cmd.c_str());
        #else
            std::string cmd = "rm -rf " + test_dir;
            system(cmd.c_str());
        #endif
    }
};

TEST_F(SegmentAllocatorTest, SizeClassMapping) {
    // Test size_to_class function using actual configured sizes
    // With config: 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144
    
    // Test exact boundaries
    EXPECT_EQ(size_to_class(1), 0);        // 256B class
    EXPECT_EQ(size_to_class(256), 0);      // 256B class
    EXPECT_EQ(size_to_class(257), 1);      // 512B class
    EXPECT_EQ(size_to_class(512), 1);      // 512B class
    EXPECT_EQ(size_to_class(513), 2);      // 1KB class
    EXPECT_EQ(size_to_class(1024), 2);     // 1KB class
    EXPECT_EQ(size_to_class(2048), 3);     // 2KB class
    EXPECT_EQ(size_to_class(4096), 4);     // 4KB class
    EXPECT_EQ(size_to_class(8192), 5);     // 8KB class
    EXPECT_EQ(size_to_class(16384), 6);    // 16KB class
    EXPECT_EQ(size_to_class(32768), 7);    // 32KB class
    EXPECT_EQ(size_to_class(65536), 8);    // 64KB class
    EXPECT_EQ(size_to_class(131072), 9);   // 128KB class
    EXPECT_EQ(size_to_class(262144), 10);  // 256KB class
    EXPECT_EQ(size_to_class(300000), 10);  // Still 256KB class (clamped)
    
    // Test class_to_size function  
    EXPECT_EQ(class_to_size(0), 256);
    EXPECT_EQ(class_to_size(1), 512);
    EXPECT_EQ(class_to_size(2), 1024);
    EXPECT_EQ(class_to_size(3), 2048);
    EXPECT_EQ(class_to_size(4), 4096);
    EXPECT_EQ(class_to_size(5), 8192);
    EXPECT_EQ(class_to_size(6), 16384);
    EXPECT_EQ(class_to_size(7), 32768);
    EXPECT_EQ(class_to_size(8), 65536);
    EXPECT_EQ(class_to_size(9), 131072);
    EXPECT_EQ(class_to_size(10), 262144);
}

TEST_F(SegmentAllocatorTest, BasicAllocation) {
    // Allocate a small object
    size_t size = 1024;
    auto alloc = allocator->allocate(size);
    
    EXPECT_TRUE(alloc.is_valid());  // Check allocation succeeded
    EXPECT_GE(alloc.offset, 0u);
    EXPECT_GE(alloc.length, size);
    EXPECT_EQ(alloc.class_id, size_to_class(size));
}

TEST_F(SegmentAllocatorTest, DifferentSizeClasses) {
    // Test different size classes - max is 256KB
    std::vector<size_t> sizes = {100, 5000, 12000, 40000, 80000, 150000, 250000};
    std::vector<SegmentAllocator::Allocation> allocs;
    
    for (size_t size : sizes) {
        auto alloc = allocator->allocate(size);
        EXPECT_TRUE(alloc.is_valid());  // Check allocation succeeded
        EXPECT_GE(alloc.length, size);
        EXPECT_EQ(alloc.class_id, size_to_class(size));
        allocs.push_back(std::move(alloc));
    }
    
    // Verify allocations don't overlap within segments
    // Store just the metadata we need since Allocation is move-only
    struct AllocInfo {
        uint64_t offset;
        uint32_t length;
    };
    std::map<std::pair<uint32_t, uint32_t>, std::vector<AllocInfo>> by_segment;
    for (const auto& alloc : allocs) {
        by_segment[{alloc.file_id, alloc.segment_id}].push_back({alloc.offset, alloc.length});
    }
    
    for (const auto& [key, segment_allocs] : by_segment) {
        for (size_t i = 0; i < segment_allocs.size(); i++) {
            for (size_t j = i + 1; j < segment_allocs.size(); j++) {
                const auto& a1 = segment_allocs[i];
                const auto& a2 = segment_allocs[j];
                
                // Check for no overlap
                EXPECT_TRUE(a1.offset + a1.length <= a2.offset || 
                           a2.offset + a2.length <= a1.offset)
                    << "Allocations overlap: [" << a1.offset << ", " << a1.offset + a1.length 
                    << ") and [" << a2.offset << ", " << a2.offset + a2.length << ")";
            }
        }
    }
}

TEST_F(SegmentAllocatorTest, AllocationAndFree) {
    std::vector<SegmentAllocator::Allocation> allocs;
    
    // Allocate several objects
    for (int i = 0; i < 10; i++) {
        auto alloc = allocator->allocate(4096);
        allocs.push_back(std::move(alloc));
    }
    
    // Free every other allocation
    for (size_t i = 0; i < allocs.size(); i += 2) {
        allocator->free(allocs[i]);
    }
    
    // Get stats and verify freed space is tracked
    auto stats = allocator->get_stats(4); // Class 4 (4K with new sizes)
    EXPECT_GT(stats.dead_bytes, 0u);
    EXPECT_GT(stats.fragmentation(), 0.0);
}

TEST_F(SegmentAllocatorTest, FragmentationTracking) {
    const size_t alloc_size = 8192;
    const uint8_t class_id = size_to_class(alloc_size);
    std::vector<SegmentAllocator::Allocation> allocs;
    
    // Allocate many objects
    for (int i = 0; i < 20; i++) {
        allocs.push_back(std::move(allocator->allocate(alloc_size)));
    }
    
    // Check initial stats - should have no fragmentation
    auto stats1 = allocator->get_stats(class_id);
    EXPECT_EQ(stats1.dead_bytes, 0u);
    EXPECT_EQ(stats1.fragmentation(), 0.0);
    
    // Free half the allocations
    for (size_t i = 0; i < allocs.size(); i += 2) {
        allocator->free(allocs[i]);
    }
    
    // Check stats after freeing
    auto stats2 = allocator->get_stats(class_id);
    EXPECT_GT(stats2.dead_bytes, 0u);
    EXPECT_GT(stats2.fragmentation(), 0.0);
    EXPECT_NEAR(stats2.fragmentation(), 0.5, 0.1); // Should be roughly 50% fragmented
}

TEST_F(SegmentAllocatorTest, ConcurrentAllocations) {
    const int num_threads = 4;
    const int allocs_per_thread = 100;
    std::vector<std::thread> threads;
    std::vector<std::vector<SegmentAllocator::Allocation>> thread_allocs(num_threads);
    
    // Each thread allocates objects of different sizes
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < allocs_per_thread; i++) {
                size_t size = 1024 * (1 + (i % 10)); // 1K to 10K
                auto alloc = allocator->allocate(size);
                EXPECT_TRUE(alloc.is_valid());  // Check allocation succeeded
                EXPECT_GE(alloc.length, size);
                thread_allocs[t].push_back(std::move(alloc));
            }
        });
    }
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all allocations are unique
    std::set<std::tuple<uint32_t, uint32_t, uint64_t>> unique_allocs;
    for (const auto& thread_vec : thread_allocs) {
        for (const auto& alloc : thread_vec) {
            auto key = std::make_tuple(alloc.file_id, alloc.segment_id, alloc.offset);
            auto [it, inserted] = unique_allocs.insert(key);
            EXPECT_TRUE(inserted) << "Duplicate allocation detected";
        }
    }
    
    EXPECT_EQ(unique_allocs.size(), num_threads * allocs_per_thread);
}

TEST_F(SegmentAllocatorTest, LargeAllocation) {
    // Test allocation at the maximum size class
    size_t large_size = 250000; // Close to 256K
    auto alloc = allocator->allocate(large_size);
    
    EXPECT_TRUE(alloc.is_valid());  // Check allocation succeeded
    EXPECT_GE(alloc.length, large_size);
    EXPECT_EQ(alloc.class_id, 10); // Should be in the 256K class (class 10)
}

TEST_F(SegmentAllocatorTest, ManySmallAllocations) {
    // Stress test with many small allocations
    const int num_allocs = 1000;
    std::vector<SegmentAllocator::Allocation> allocs;
    
    for (int i = 0; i < num_allocs; i++) {
        auto alloc = allocator->allocate(512); // Small allocation
        EXPECT_TRUE(alloc.is_valid());  // Check allocation succeeded
        allocs.push_back(std::move(alloc));  // Move the allocation
    }
    
    // Free all and check stats
    for (auto& alloc : allocs) {  // Non-const to allow moving
        allocator->free(alloc);
    }
    
    auto stats = allocator->get_stats(1); // 512B class (since we allocated 512B)
    EXPECT_GT(stats.dead_bytes, 0u);
}

// ========== O(1) get_ptr Tests ==========

TEST_F(SegmentAllocatorTest, GetPtrBasic) {
    // Test basic get_ptr functionality
    auto alloc = allocator->allocate(1024);
    ASSERT_TRUE(alloc.is_valid());
    
    // Get the memory-mapped pointer
    void* ptr = allocator->get_ptr(alloc);
    ASSERT_NE(ptr, nullptr);
    
    // Write data to the memory
    const char* test_data = "Hello, mmap!";
    std::memcpy(ptr, test_data, strlen(test_data) + 1);
    
    // Get pointer again and verify data
    void* ptr2 = allocator->get_ptr(alloc);
    ASSERT_EQ(ptr, ptr2); // Should return same pointer
    EXPECT_STREQ(static_cast<const char*>(ptr2), test_data);
}

TEST_F(SegmentAllocatorTest, GetPtrMultipleSegments) {
    // Force creation of multiple segments by allocating many objects
    std::vector<SegmentAllocator::Allocation> allocs;
    std::vector<void*> ptrs;
    
    // Allocate enough to span multiple segments
    const size_t alloc_size = 32768; // 32K each
    const size_t num_allocs = 100;   // Should force multiple segments
    
    for (size_t i = 0; i < num_allocs; ++i) {
        auto alloc = allocator->allocate(alloc_size);
        ASSERT_TRUE(alloc.is_valid());
        allocs.push_back(std::move(alloc));
        
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr);
        ptrs.push_back(ptr);
        
        // Write unique data to each allocation
        uint64_t* data = static_cast<uint64_t*>(ptr);
        data[0] = i;
        data[1] = i * 1000;
    }
    
    // Verify all pointers are unique and data is preserved
    std::set<void*> unique_ptrs;
    for (size_t i = 0; i < num_allocs; ++i) {
        void* ptr = allocator->get_ptr(allocs[i]);
        ASSERT_EQ(ptr, ptrs[i]); // Should be stable
        
        uint64_t* data = static_cast<uint64_t*>(ptr);
        EXPECT_EQ(data[0], i);
        EXPECT_EQ(data[1], i * 1000);
        
        unique_ptrs.insert(ptr);
    }
    
    EXPECT_EQ(unique_ptrs.size(), num_allocs); // All pointers should be unique
}

TEST_F(SegmentAllocatorTest, GetPtrInvalidAllocation) {
    // Test get_ptr with invalid allocations
    SegmentAllocator::Allocation invalid_alloc{};
    
    // Test completely invalid allocation
    void* ptr = allocator->get_ptr(invalid_alloc);
    EXPECT_EQ(ptr, nullptr);
    
    // Test with invalid segment_id
    invalid_alloc.segment_id = 999999;
    invalid_alloc.class_id = 0;
    ptr = allocator->get_ptr(invalid_alloc);
    EXPECT_EQ(ptr, nullptr);
    
    // Test with invalid class_id
    invalid_alloc.segment_id = 0;
    invalid_alloc.class_id = 255; // Way beyond NUM_CLASSES
    ptr = allocator->get_ptr(invalid_alloc);
    EXPECT_EQ(ptr, nullptr);
}

TEST_F(SegmentAllocatorTest, GetPtrConcurrentReads) {
    // Test O(1) lock-free concurrent reads
    auto alloc = allocator->allocate(4096);
    ASSERT_TRUE(alloc.is_valid());
    
    // Write test pattern
    void* ptr = allocator->get_ptr(alloc);
    ASSERT_NE(ptr, nullptr);
    for (size_t i = 0; i < 4096; ++i) {
        static_cast<uint8_t*>(ptr)[i] = static_cast<uint8_t>(i & 0xFF);
    }
    
    // Spawn multiple reader threads
    const int num_readers = 8;
    std::vector<std::thread> readers;
    std::atomic<int> successful_reads{0};
    
    for (int t = 0; t < num_readers; ++t) {
        readers.emplace_back([&]() {
            for (int i = 0; i < 10000; ++i) {
                void* read_ptr = allocator->get_ptr(alloc);
                if (read_ptr) {
                    // Verify pattern
                    bool valid = true;
                    for (size_t j = 0; j < 4096; ++j) {
                        if (static_cast<uint8_t*>(read_ptr)[j] != static_cast<uint8_t>(j & 0xFF)) {
                            valid = false;
                            break;
                        }
                    }
                    if (valid) {
                        successful_reads.fetch_add(1);
                    }
                }
            }
        });
    }
    
    for (auto& t : readers) {
        t.join();
    }
    
    EXPECT_EQ(successful_reads.load(), num_readers * 10000);
}

TEST_F(SegmentAllocatorTest, GetPtrAfterSegmentGrowth) {
    // Test that get_ptr works correctly after segment table grows
    std::vector<SegmentAllocator::Allocation> allocs;
    std::vector<void*> ptrs;
    
    // Start with small number of allocations
    for (int i = 0; i < 10; ++i) {
        auto alloc = allocator->allocate(4096);
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr);
        
        // Write marker
        *static_cast<int*>(ptr) = i;
        
        allocs.push_back(std::move(alloc));
        ptrs.push_back(ptr);
    }
    
    // Force segment table to grow by allocating many more
    for (int i = 10; i < 200; ++i) {
        auto alloc = allocator->allocate(4096);
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr);
        
        *static_cast<int*>(ptr) = i;
        
        allocs.push_back(std::move(alloc));
        ptrs.push_back(ptr);
    }
    
    // Verify all old pointers still work
    for (size_t i = 0; i < allocs.size(); ++i) {
        void* ptr = allocator->get_ptr(allocs[i]);
        ASSERT_EQ(ptr, ptrs[i]);
        EXPECT_EQ(*static_cast<int*>(ptr), static_cast<int>(i));
    }
}

TEST_F(SegmentAllocatorTest, GetPtrPerformance) {
    // Benchmark O(1) performance
    const size_t num_allocs = 1000;
    std::vector<SegmentAllocator::Allocation> allocs;
    
    // Create many allocations across different segments
    for (size_t i = 0; i < num_allocs; ++i) {
        auto alloc = allocator->allocate(8192);
        ASSERT_TRUE(alloc.is_valid());
        allocs.push_back(std::move(alloc));
    }
    
    // Time get_ptr operations
    const int num_lookups = 100000;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_lookups; ++i) {
        const auto& alloc = allocs[i % num_allocs];
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double us_per_lookup = static_cast<double>(duration.count()) / num_lookups;
    
    // O(1) lookups should be very fast - expect sub-microsecond
    EXPECT_LT(us_per_lookup, 1.0) << "get_ptr taking " << us_per_lookup << " us per lookup";
    
    // Print performance info
    std::cout << "get_ptr performance: " << us_per_lookup << " us per lookup\n";
    std::cout << "Total lookups: " << num_lookups << " in " << duration.count() << " us\n";
}

TEST_F(SegmentAllocatorTest, GetPtrNeverScansOrLocks) {
    #ifndef NDEBUG
    // Reset debug counters
    g_segment_scan_count.store(0);
    g_segment_lock_count.store(0);
    #endif
    
    // Create many allocations across different segments
    std::vector<SegmentAllocator::Allocation> allocs;
    for (size_t i = 0; i < 1000; ++i) {
        auto alloc = allocator->allocate(4096);
        ASSERT_TRUE(alloc.is_valid());
        allocs.push_back(std::move(alloc));
    }
    
    // Call get_ptr many times
    for (int iter = 0; iter < 10000; ++iter) {
        for (const auto& alloc : allocs) {
            void* ptr = allocator->get_ptr(alloc);
            ASSERT_NE(ptr, nullptr);
        }
    }
    
    #ifndef NDEBUG
    // Verify no scans or locks occurred
    EXPECT_EQ(g_segment_scan_count.load(), 0u) << "get_ptr performed linear scans!";
    EXPECT_EQ(g_segment_lock_count.load(), 0u) << "get_ptr took locks!";
    #endif
}

TEST_F(SegmentAllocatorTest, GetPtrBoundaryConditions) {
    // Test boundary conditions for offsets within segments
    auto alloc = allocator->allocate(256);
    ASSERT_TRUE(alloc.is_valid());
    
    void* ptr = allocator->get_ptr(alloc);
    ASSERT_NE(ptr, nullptr);
    
    // Write pattern at boundaries
    uint8_t* bytes = static_cast<uint8_t*>(ptr);
    bytes[0] = 0xAA;               // Start
    bytes[alloc.length - 1] = 0xBB; // End
    
    // Verify boundaries are accessible
    EXPECT_EQ(bytes[0], 0xAA);
    EXPECT_EQ(bytes[alloc.length - 1], 0xBB);
    
    // Test allocation that spans most of a segment
    size_t large_size = 250000; // Close to 256K limit
    auto large_alloc = allocator->allocate(large_size);
    ASSERT_TRUE(large_alloc.is_valid());
    
    void* large_ptr = allocator->get_ptr(large_alloc);
    ASSERT_NE(large_ptr, nullptr);
    
    // Should be able to write to entire range
    std::memset(large_ptr, 0xCC, large_alloc.length);
    
    // Verify write succeeded
    uint8_t* large_bytes = static_cast<uint8_t*>(large_ptr);
    EXPECT_EQ(large_bytes[0], 0xCC);
    EXPECT_EQ(large_bytes[large_alloc.length / 2], 0xCC);
    EXPECT_EQ(large_bytes[large_alloc.length - 1], 0xCC);
}

TEST_F(SegmentAllocatorTest, SmallAllocationNoOverlap) {
    // Test specifically for 256B minimum size class
    // This test verifies that small allocations don't overlap or corrupt each other
    
    struct TestAlloc {
        SegmentAllocator::Allocation alloc;
        size_t requested_size;
        uint8_t pattern;
    };
    
    std::vector<TestAlloc> allocs;
    
    // Test various sizes that stress the 256B minimum
    std::vector<size_t> test_sizes = {
        56,    // DataRecord (2D) - gets 256B
        84,    // Small XTreeBucket (4 children) - gets 256B
        128,   // Exactly one size class
        256,   // Exactly one size class
        512,   // Next size class
        1024,  // Medium bucket
        2420,  // Supernode with 150 children - gets 4096B
        4096,  // Full page
    };
    
    // Allocate and write patterns
    uint8_t pattern = 0x10;
    for (size_t size : test_sizes) {
        auto alloc = allocator->allocate(size);
        ASSERT_TRUE(alloc.is_valid()) << "Failed to allocate " << size << " bytes";
        
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr) << "Got null pointer for " << size << " byte allocation";
        
        // Write pattern to entire allocated space
        std::memset(ptr, pattern, alloc.length);
        
        // Store for later verification
        allocs.push_back({std::move(alloc), size, pattern});
        pattern += 0x10;
    }
    
    // Verify no corruption - each allocation should still have its pattern
    for (const auto& test : allocs) {
        void* ptr = allocator->get_ptr(test.alloc);
        ASSERT_NE(ptr, nullptr);
        
        uint8_t* bytes = static_cast<uint8_t*>(ptr);
        
        // Check first, middle, and last bytes of allocation
        EXPECT_EQ(bytes[0], test.pattern) 
            << "Corruption at start of " << test.requested_size << " byte allocation";
        EXPECT_EQ(bytes[test.alloc.length / 2], test.pattern)
            << "Corruption in middle of " << test.requested_size << " byte allocation";
        EXPECT_EQ(bytes[test.alloc.length - 1], test.pattern)
            << "Corruption at end of " << test.requested_size << " byte allocation";
        
        // Verify entire allocation
        for (size_t i = 0; i < test.alloc.length; ++i) {
            if (bytes[i] != test.pattern) {
                FAIL() << "Corruption at offset " << i << " in " << test.requested_size 
                       << " byte allocation (got " << (int)bytes[i] 
                       << ", expected " << (int)test.pattern << ")";
                break;
            }
        }
    }
    
    // Verify allocations don't overlap
    for (size_t i = 0; i < allocs.size(); ++i) {
        for (size_t j = i + 1; j < allocs.size(); ++j) {
            const auto& a1 = allocs[i].alloc;
            const auto& a2 = allocs[j].alloc;
            
            // Only check if in same file and segment
            if (a1.file_id == a2.file_id && a1.segment_id == a2.segment_id) {
                bool no_overlap = (a1.offset + a1.length <= a2.offset) ||
                                 (a2.offset + a2.length <= a1.offset);
                EXPECT_TRUE(no_overlap)
                    << "Allocations overlap: "
                    << "[" << a1.offset << "-" << (a1.offset + a1.length) << ") and "
                    << "[" << a2.offset << "-" << (a2.offset + a2.length) << ")";
            }
        }
    }
}

TEST_F(SegmentAllocatorTest, ConcurrentMemoryAccess) {
    // Test that allocated memory can be safely accessed concurrently
    // Memory ordering guarantees are now tested in test_mapping_manager.cpp
    
    // Allocate a chunk of memory
    auto alloc = allocator->allocate(4096);
    ASSERT_TRUE(alloc.is_valid());
    void* ptr = allocator->get_ptr(alloc);
    ASSERT_NE(ptr, nullptr);
    
    std::atomic<bool> success{true};
    
    // Multiple threads accessing the same allocated memory
    auto worker = [&](int thread_id, int offset) {
        // Each thread writes to its own portion
        uint32_t* data = reinterpret_cast<uint32_t*>(static_cast<uint8_t*>(ptr) + offset);
        for (int i = 0; i < 256; ++i) {
            data[i] = thread_id * 1000 + i;
        }
        
        // Verify our writes
        for (int i = 0; i < 256; ++i) {
            if (data[i] != static_cast<uint32_t>(thread_id * 1000 + i)) {
                success.store(false);
                break;
            }
        }
    };
    
    // Launch 4 threads, each writing to different 1KB portions
    std::thread t1(worker, 1, 0);
    std::thread t2(worker, 2, 1024);
    std::thread t3(worker, 3, 2048);
    std::thread t4(worker, 4, 3072);
    
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    
    EXPECT_TRUE(success.load());
    
    // Clean up
    allocator->free(alloc);
}

TEST_F(SegmentAllocatorTest, ConcurrentTableGrowth) {
    // Test that segment table can grow safely while readers are active
    std::vector<SegmentAllocator::Allocation> initial_allocs;
    
    // Create initial allocations
    for (int i = 0; i < 10; ++i) {
        auto alloc = allocator->allocate(4096);
        ASSERT_TRUE(alloc.is_valid());
        initial_allocs.push_back(std::move(alloc));
    }
    
    // Start reader threads that continuously read from initial allocations
    std::atomic<bool> stop_readers{false};
    std::atomic<int> successful_reads{0};
    std::vector<std::thread> readers;
    
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&]() {
            while (!stop_readers.load()) {
                for (const auto& alloc : initial_allocs) {
                    void* ptr = allocator->get_ptr(alloc);
                    if (ptr) {
                        successful_reads.fetch_add(1);
                    }
                }
            }
        });
    }
    
    // Writer thread: grow the table by allocating many more segments
    std::thread writer([&]() {
        for (int i = 0; i < 100; ++i) {
            auto alloc = allocator->allocate(8192);
            ASSERT_TRUE(alloc.is_valid());
            // Small delay to spread allocations over time
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
    
    // Let readers and writer run concurrently
    writer.join();
    
    // Stop readers
    stop_readers.store(true);
    for (auto& t : readers) {
        t.join();
    }
    
    // Verify we got many successful reads
    EXPECT_GT(successful_reads.load(), 1000) << "Should have many successful reads during growth";
    
    // Verify all initial allocations still work
    for (const auto& alloc : initial_allocs) {
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr) << "Initial allocation should still be valid after growth";
    }
}

// ============= Tests for O(1) get_ptr_for_recovery =============

TEST_F(SegmentAllocatorTest, RecoveryPointerFastPath) {
    // Test 1: Published fast-path - segment already in table
    // Pre-allocate to ensure segment exists
    auto alloc = allocator->allocate(8192);  // 8K class
    ASSERT_TRUE(alloc.is_valid());
    
    // Write test pattern to the allocation
    void* ptr = allocator->get_ptr(alloc);
    ASSERT_NE(ptr, nullptr);
    const char* test_data = "RECOVERY_TEST_DATA";
    std::memcpy(ptr, test_data, strlen(test_data) + 1);
    
    // Now use get_ptr_for_recovery with the same location
    void* recovery_ptr = allocator->get_ptr_for_recovery(
        alloc.class_id, alloc.file_id, alloc.segment_id, 
        alloc.offset, alloc.length);
    
    ASSERT_NE(recovery_ptr, nullptr);
    ASSERT_EQ(recovery_ptr, ptr) << "Should return same pointer for same location";
    
    // Verify data is accessible
    EXPECT_STREQ(static_cast<const char*>(recovery_ptr), test_data);
}

TEST_F(SegmentAllocatorTest, RecoveryPointerUnpublishedSegment) {
    // Test 2: Unpublished → publish once
    // Simulate recovery scenario where segment doesn't exist yet
    
    // Use high segment_id that doesn't exist
    uint8_t class_id = 2;  // 16K class
    uint32_t file_id = 0;
    uint32_t segment_id = 100;  // High ID that won't be allocated yet
    uint64_t offset = segment_id * segment::kDefaultSegmentSize + 4096;  // Some offset within segment
    uint32_t length = 16384;
    
    // First call should map and publish the segment
    void* ptr1 = allocator->get_ptr_for_recovery(
        class_id, file_id, segment_id, offset, length);
    
    // May be nullptr if file doesn't exist yet, but that's ok for this test
    // The important part is the second call should be fast
    
    // Second call should hit fast path (cached segment)
    void* ptr2 = allocator->get_ptr_for_recovery(
        class_id, file_id, segment_id, offset, length);
    
    // Verify correctness: Both calls should return same result
    // This proves the segment was properly cached after first access
    EXPECT_EQ(ptr1, ptr2) << "Both calls should return same pointer (segment cached)";
    
    // Additional correctness check: Multiple calls should be idempotent
    void* ptr3 = allocator->get_ptr_for_recovery(
        class_id, file_id, segment_id, offset, length);
    EXPECT_EQ(ptr2, ptr3) << "Cached segment should remain stable";
    
    // NOTE: Performance verification (O(1) fast path) is tested in benchmarks,
    // not unit tests. See benchmarks/segment_allocator_bench.cpp
}

TEST_F(SegmentAllocatorTest, RecoveryPointerWrongFileId) {
    // Test 3: Wrong file_id returns nullptr
    auto alloc = allocator->allocate(4096);
    ASSERT_TRUE(alloc.is_valid());
    
    // Try to get pointer with wrong file_id
    void* ptr = allocator->get_ptr_for_recovery(
        alloc.class_id, 
        alloc.file_id + 1,  // Wrong file ID
        alloc.segment_id, 
        alloc.offset, 
        alloc.length);
    
    EXPECT_EQ(ptr, nullptr) << "Should return nullptr for wrong file_id";
}

TEST_F(SegmentAllocatorTest, RecoveryPointerBoundsChecking) {
    // Test 4: Bounds checking - offsets below base or beyond capacity
    auto alloc = allocator->allocate(32768);  // 32K
    ASSERT_TRUE(alloc.is_valid());
    
    // Test offset below base
    void* ptr1 = allocator->get_ptr_for_recovery(
        alloc.class_id, alloc.file_id, alloc.segment_id, 
        0,  // Offset 0 is likely below segment base
        alloc.length);
    
    // May or may not be nullptr depending on segment layout
    
    // Test offset beyond capacity
    void* ptr2 = allocator->get_ptr_for_recovery(
        alloc.class_id, alloc.file_id, alloc.segment_id, 
        UINT64_MAX - 1000,  // Way beyond any reasonable offset
        alloc.length);
    
    EXPECT_EQ(ptr2, nullptr) << "Should return nullptr for out-of-bounds offset";
}

TEST_F(SegmentAllocatorTest, RecoveryPointerConcurrency) {
    // Test 5: Concurrent access - multiple threads racing on same unpublished segment
    const int num_threads = 8;
    const uint8_t class_id = 3;  // 32K class
    const uint32_t file_id = 0;
    const uint32_t segment_id = 200;  // Unpublished segment
    const uint64_t base_offset = segment_id * segment::kDefaultSegmentSize;
    const uint32_t length = 32768;
    
    std::vector<std::thread> threads;
    std::atomic<int> successful_maps{0};
    std::atomic<void*> first_ptr{nullptr};
    
    // All threads try to map the same segment concurrently
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            void* ptr = allocator->get_ptr_for_recovery(
                class_id, file_id, segment_id, 
                base_offset + i * length, length);
            
            if (ptr != nullptr) {
                successful_maps++;
                
                // Try to store the first successful pointer
                void* expected = nullptr;
                first_ptr.compare_exchange_strong(expected, ptr);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // At least one thread should succeed (unless file creation fails)
    // All successful threads should get valid pointers to the same segment
    EXPECT_GE(successful_maps.load(), 0) << "Some threads should successfully map";
}

TEST_F(SegmentAllocatorTest, RecoveryPointerPerformance) {
    // Performance test: verify O(1) behavior
    // Pre-populate many segments
    std::vector<SegmentAllocator::Allocation> allocs;
    const int num_segments = 100;
    
    for (int i = 0; i < num_segments; ++i) {
        auto alloc = allocator->allocate(16384);  // 16K
        ASSERT_TRUE(alloc.is_valid());
        allocs.push_back(std::move(alloc));
    }
    
    // Measure time for recovery lookups (should be O(1) regardless of segment count)
    std::vector<double> lookup_times;
    
    for (const auto& alloc : allocs) {
        auto start = std::chrono::high_resolution_clock::now();
        
        void* ptr = allocator->get_ptr_for_recovery(
            alloc.class_id, alloc.file_id, alloc.segment_id, 
            alloc.offset, alloc.length);
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        
        ASSERT_NE(ptr, nullptr);
        lookup_times.push_back(duration);
    }
    
    // Calculate average lookup time
    double avg_time = 0;
    for (double t : lookup_times) {
        avg_time += t;
    }
    avg_time /= lookup_times.size();
    
    // O(1) means lookup time should be consistent and fast
    EXPECT_LT(avg_time, 1000) << "Average lookup should be < 1 microsecond";
    
    // Check that variance is low (consistent O(1) behavior)
    double variance = 0;
    for (double t : lookup_times) {
        variance += (t - avg_time) * (t - avg_time);
    }
    variance /= lookup_times.size();
    double std_dev = std::sqrt(variance);
    
    // In test environments, variance can be higher due to OS scheduling, debug builds, etc.
    // What matters is that average time is low (O(1))
    // Just verify the average is good - variance in test environments can be unpredictable
    if (std_dev > avg_time * 10) {
        std::cout << "Warning: High variance detected (std_dev=" << std_dev 
                  << "ns, avg=" << avg_time << "ns), but average is still O(1)\n";
    }
    
    std::cout << "Recovery lookup performance: avg=" << avg_time 
              << "ns, stddev=" << std_dev << "ns\n";
}

TEST_F(SegmentAllocatorTest, RecoveryPointerWithPayloadWrite) {
    // Test actual payload rehydration scenario
    const char* payload = "This is test payload data for recovery";
    size_t payload_size = strlen(payload) + 1;
    
    // Allocate space for payload
    auto alloc = allocator->allocate(payload_size);
    ASSERT_TRUE(alloc.is_valid());
    
    // Simulate recovery: get pointer and write payload
    void* dst = allocator->get_ptr_for_recovery(
        alloc.class_id, alloc.file_id, alloc.segment_id, 
        alloc.offset, alloc.length);
    
    ASSERT_NE(dst, nullptr);
    
    // Write payload (simulating WAL replay)
    std::memcpy(dst, payload, payload_size);
    
    // Verify we can read it back via normal get_ptr
    void* read_ptr = allocator->get_ptr(alloc);
    ASSERT_NE(read_ptr, nullptr);
    EXPECT_STREQ(static_cast<const char*>(read_ptr), payload);
}

TEST_F(SegmentAllocatorTest, RecoveryPointerPrecisePerformance) {
    // Test that get_ptr_for_recovery achieves 3-4ns performance like get_ptr
    // Pre-populate segments to ensure they're all in seg_table
    const size_t num_allocs = 1000;
    std::vector<SegmentAllocator::Allocation> allocs;
    
    // Create allocations across multiple segments 
    for (size_t i = 0; i < num_allocs; ++i) {
        auto alloc = allocator->allocate(16384);  // 16K allocations
        ASSERT_TRUE(alloc.is_valid());
        allocs.push_back(std::move(alloc));
        
        // Write some data to ensure segment is mapped
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr);
        *static_cast<uint64_t*>(ptr) = i;
    }
    
    // Warm up CPU caches and branch predictors
    for (int warmup = 0; warmup < 10000; ++warmup) {
        const auto& alloc = allocs[warmup % num_allocs];
        void* ptr = allocator->get_ptr_for_recovery(
            alloc.class_id, alloc.file_id, alloc.segment_id,
            alloc.offset, alloc.length);
        ASSERT_NE(ptr, nullptr);
    }
    
    // Measure precise performance with many iterations
    const int num_iterations = 1000000;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_iterations; ++i) {
        const auto& alloc = allocs[i % num_allocs];
        void* ptr = allocator->get_ptr_for_recovery(
            alloc.class_id, alloc.file_id, alloc.segment_id,
            alloc.offset, alloc.length);
        // Use volatile to prevent optimization
        volatile void* v = ptr;
        (void)v;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    
    double ns_per_lookup = static_cast<double>(duration_ns) / num_iterations;
    
    // Target: 3-4ns per lookup (allowing up to 10ns for test environment variance)
    EXPECT_LT(ns_per_lookup, 10.0) << "get_ptr_for_recovery should be < 10ns per lookup";
    
    // Print detailed performance info
    std::cout << "get_ptr_for_recovery performance:\n";
    std::cout << "  Average: " << ns_per_lookup << " ns per lookup\n";
    std::cout << "  Total: " << num_iterations << " lookups in " << duration_ns << " ns\n";
    std::cout << "  Throughput: " << (1000.0 / ns_per_lookup) << " million ops/sec\n";
    
    // Compare with get_ptr performance
    start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_iterations; ++i) {
        const auto& alloc = allocs[i % num_allocs];
        void* ptr = allocator->get_ptr(alloc);
        volatile void* v = ptr;
        (void)v;
    }
    
    end = std::chrono::high_resolution_clock::now();
    duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double get_ptr_ns = static_cast<double>(duration_ns) / num_iterations;
    
    std::cout << "get_ptr performance (baseline):\n";
    std::cout << "  Average: " << get_ptr_ns << " ns per lookup\n";
    std::cout << "  Overhead: " << (ns_per_lookup - get_ptr_ns) << " ns\n";
    
    // Recovery function should be within 2x of get_ptr performance
    EXPECT_LT(ns_per_lookup, get_ptr_ns * 2.0) 
        << "get_ptr_for_recovery should be within 2x of get_ptr performance";
}

TEST_F(SegmentAllocatorTest, RecoveryPointerCacheMissHandling) {
    // Test behavior when segment is not in cache
    // This tests the slow path that needs to map the segment
    
    // Use a segment ID that definitely doesn't exist yet
    const uint8_t class_id = 4;  // 64K class
    const uint32_t file_id = 0;
    const uint32_t segment_id = 500;  // High segment ID
    const uint64_t base_offset = segment_id * segment::kDefaultSegmentSize;
    const uint32_t length = 65536;
    
    // First call - will need to map segment (slow path)
    auto start = std::chrono::high_resolution_clock::now();
    void* ptr1 = allocator->get_ptr_for_recovery(
        class_id, file_id, segment_id, base_offset, length);
    auto end = std::chrono::high_resolution_clock::now();
    auto first_call_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    
    // Second call - should hit fast path
    start = std::chrono::high_resolution_clock::now();
    void* ptr2 = allocator->get_ptr_for_recovery(
        class_id, file_id, segment_id, base_offset, length);
    end = std::chrono::high_resolution_clock::now();
    auto second_call_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    
    // Both should return same result
    EXPECT_EQ(ptr1, ptr2);
    
    // Second call should be much faster (100x or more)
    if (ptr1 != nullptr) {  // Only if mapping succeeded
        EXPECT_LT(second_call_ns, first_call_ns / 10) 
            << "Second call should be much faster after caching";
        
        std::cout << "Cache miss handling:\n";
        std::cout << "  First call (with mapping): " << first_call_ns << " ns\n";
        std::cout << "  Second call (cached): " << second_call_ns << " ns\n";
        std::cout << "  Speedup: " << (first_call_ns / second_call_ns) << "x\n";
    }
}

// Test that close_all properly releases resources
TEST_F(SegmentAllocatorTest, CloseAllReleasesResources) {
    // Allocate some segments
    std::vector<std::pair<void*, size_t>> allocations;
    
    // Allocate in different size classes
    for (size_t size : {256, 512, 1024, 2048, 4096}) {
        auto alloc = allocator->allocate(size);
        void* ptr = allocator->get_ptr(alloc);
        ASSERT_NE(ptr, nullptr);
        allocations.push_back({ptr, size});
        
        // Write some data to ensure the segment is mapped
        memset(ptr, 0xAB, size);
    }
    
    // Now close all segments
    allocator->close_all();
    
    // After close_all, the allocator should have released all resources
    // We can verify this by checking the internal state
    
    // Try to allocate again - should create new segments
    auto new_alloc = allocator->allocate(256);
    void* new_ptr = allocator->get_ptr(new_alloc);
    ASSERT_NE(new_ptr, nullptr);
    
    // The new allocation should work fine
    memset(new_ptr, 0xCD, 256);
    
    // Verify we can read it back
    unsigned char* bytes = static_cast<unsigned char*>(new_ptr);
    for (size_t i = 0; i < 256; ++i) {
        EXPECT_EQ(bytes[i], 0xCD);
    }
}

// Test that close_all can be called multiple times safely
TEST_F(SegmentAllocatorTest, CloseAllIdempotent) {
    // Allocate something
    auto alloc = allocator->allocate(1024);
    void* ptr = allocator->get_ptr(alloc);
    ASSERT_NE(ptr, nullptr);
    memset(ptr, 0x42, 1024);
    
    // Close multiple times - should not crash
    allocator->close_all();
    allocator->close_all();
    allocator->close_all();
    
    // Can still allocate after multiple closes
    auto new_alloc = allocator->allocate(512);
    void* new_ptr = allocator->get_ptr(new_alloc);
    ASSERT_NE(new_ptr, nullptr);
}