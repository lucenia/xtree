/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Tests for compact allocator and fast snapshot/reload
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <thread>
#include <set>
#include <cstring>
#include "../src/memmgr/compact_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"

using namespace xtree;
using namespace std::chrono;

class CompactAllocatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any test files
        std::remove("test_compact.snapshot");
        std::remove("test_compact.snapshot.tmp");
    }
    
    void TearDown() override {
        std::remove("test_compact.snapshot");
        std::remove("test_compact.snapshot.tmp");
    }
};

// Test basic allocation
TEST_F(CompactAllocatorTest, BasicAllocation) {
    CompactAllocator allocator(1024 * 1024); // 1MB
    
    // Allocate some memory
    auto offset1 = allocator.allocate(100);
    EXPECT_NE(offset1, CompactAllocator::INVALID_OFFSET);
    
    auto offset2 = allocator.allocate(200);
    EXPECT_NE(offset2, CompactAllocator::INVALID_OFFSET);
    EXPECT_GT(offset2, offset1);
    
    // Get pointers
    void* ptr1 = allocator.get_ptr(offset1);
    void* ptr2 = allocator.get_ptr(offset2);
    EXPECT_NE(ptr1, nullptr);
    EXPECT_NE(ptr2, nullptr);
    EXPECT_NE(ptr1, ptr2);
    
    // Write and read data
    *static_cast<int*>(ptr1) = 42;
    *static_cast<int*>(ptr2) = 84;
    EXPECT_EQ(*static_cast<int*>(ptr1), 42);
    EXPECT_EQ(*static_cast<int*>(ptr2), 84);
}

// Test typed allocation
TEST_F(CompactAllocatorTest, TypedAllocation) {
    CompactAllocator allocator(1024 * 1024);
    
    struct TestStruct {
        int a;
        double b;
        char c[16];
    };
    
    // Allocate typed object
    auto typed = allocator.allocate_typed<TestStruct>();
    EXPECT_TRUE(typed);
    
    typed->a = 123;
    typed->b = 456.789;
    strcpy(typed->c, "Hello");
    
    EXPECT_EQ(typed->a, 123);
    EXPECT_DOUBLE_EQ(typed->b, 456.789);
    EXPECT_STREQ(typed->c, "Hello");
}

// Test offset conversion
TEST_F(CompactAllocatorTest, OffsetConversion) {
    CompactAllocator allocator(1024 * 1024);
    
    auto offset = allocator.allocate(100);
    void* ptr = allocator.get_ptr(offset);
    
    // Convert back to offset
    auto offset2 = allocator.get_offset(ptr);
    EXPECT_EQ(offset, offset2);
    
    // Null pointer
    EXPECT_EQ(allocator.get_offset(nullptr), CompactAllocator::INVALID_OFFSET);
    
    // Invalid pointer
    int stack_var = 42;
    EXPECT_EQ(allocator.get_offset(&stack_var), CompactAllocator::INVALID_OFFSET);
}

// Test alignment
TEST_F(CompactAllocatorTest, Alignment) {
    CompactAllocator allocator(1024 * 1024);
    
    // CompactAllocator uses page alignment for optimal cache performance
    // This is by design for XTree's large node allocations
    auto offset1 = allocator.allocate(1);
    auto offset2 = allocator.allocate(1);
    
    // Expect page alignment (4096 bytes on most systems)
    size_t page_size = xtree::PageAlignedMemoryTracker::get_cached_page_size();
    EXPECT_EQ(offset2 - offset1, page_size);
    
    // Test realistic allocation sizes (like XTreeBucket)
    auto offset3 = allocator.allocate(4096);  // Page-sized allocation
    auto offset4 = allocator.allocate(4096);
    EXPECT_EQ(offset4 - offset3, page_size);
    
    // Test DataRecord-sized allocations
    struct TestRecord {
        char data[256];  // Typical record size
    };
    auto offset5 = allocator.allocate(sizeof(TestRecord));
    auto offset6 = allocator.allocate(sizeof(TestRecord));
    EXPECT_EQ(offset6 - offset5, page_size);
}

// Test snapshot save and load
TEST_F(CompactAllocatorTest, SnapshotSaveLoad) {
    const char* test_file = "test_compact.snapshot";
    
    // Create and populate allocator
    {
        CompactSnapshotManager manager(test_file);
        auto* allocator = manager.get_allocator();
        
        // Allocate some test data
        struct Record {
            int id;
            double value;
            char name[32];
        };
        
        std::vector<CompactAllocator::offset_t> offsets;
        for (int i = 0; i < 100; i++) {
            auto offset = allocator->allocate(sizeof(Record));
            offsets.push_back(offset);
            
            Record* rec = allocator->get_ptr<Record>(offset);
            rec->id = i;
            rec->value = i * 3.14;
            snprintf(rec->name, sizeof(rec->name), "Record_%d", i);
        }
        
        // Save snapshot
        manager.save_snapshot();
    }
    
    // Load snapshot and verify
    {
        CompactSnapshotManager manager(test_file);
        EXPECT_TRUE(manager.is_snapshot_loaded());
        
        auto* allocator = manager.get_allocator();
        EXPECT_TRUE(allocator->is_mmap_backed());
        
        // Instead of guessing offsets, allocate and find records
        // by knowing the allocation pattern
        struct Record {
            int id;
            double value;
            char name[32];
        };
        
        // CompactAllocator uses page alignment for all allocations
        size_t page_size = xtree::PageAlignedMemoryTracker::get_cached_page_size();
        
        // First allocation starts at page_size (first page reserved for metadata)
        size_t current_offset = page_size;
        
        for (int i = 0; i < 100; i++) {
            Record* rec = allocator->get_ptr<Record>(current_offset);
            ASSERT_NE(rec, nullptr) << "Failed to get record " << i << " at offset " << current_offset;
            
            EXPECT_EQ(rec->id, i);
            EXPECT_DOUBLE_EQ(rec->value, i * 3.14);
            
            char expected[32];
            snprintf(expected, sizeof(expected), "Record_%d", i);
            EXPECT_STREQ(rec->name, expected);
            
            // Move to next page-aligned offset
            current_offset += page_size;
        }
    }
}

// Test reload performance
TEST_F(CompactAllocatorTest, DISABLED_ReloadPerformance) {
    const char* test_file = "test_compact.snapshot";
    const int NUM_RECORDS = 10000;
    
    // Create large snapshot
    {
        CompactSnapshotManager manager(test_file, 10 * 1024 * 1024); // 10MB
        auto* allocator = manager.get_allocator();
        
        for (int i = 0; i < NUM_RECORDS; i++) {
            auto offset = allocator->allocate(1024); // 1KB each
            int* data = allocator->get_ptr<int>(offset);
            for (int j = 0; j < 256; j++) {
                data[j] = i * 256 + j;
            }
        }
        
        auto start = high_resolution_clock::now();
        manager.save_snapshot();
        auto save_time = high_resolution_clock::now() - start;
        
        std::cout << "Save time for " << NUM_RECORDS << " records: " 
                  << duration_cast<milliseconds>(save_time).count() << "ms" << std::endl;
    }
    
    // Measure reload time
    {
        auto start = high_resolution_clock::now();
        CompactSnapshotManager manager(test_file);
        auto load_time = high_resolution_clock::now() - start;
        
        std::cout << "Load time for " << NUM_RECORDS << " records: " 
                  << duration_cast<microseconds>(load_time).count() << "us" << std::endl;
        
        // Should be very fast (< 1ms) due to MMAP
        EXPECT_LT(duration_cast<milliseconds>(load_time).count(), 10);
        
        // Verify some data
        auto* allocator = manager.get_allocator();
        auto offset = sizeof(CompactAllocator::offset_t) + 5000 * 1024;
        int* data = allocator->get_ptr<int>(offset);
        EXPECT_EQ(data[0], 5000 * 256);
        EXPECT_EQ(data[255], 5000 * 256 + 255);
    }
}

// Test thread safety
TEST_F(CompactAllocatorTest, DISABLED_ThreadSafety) {
    CompactAllocator allocator(10 * 1024 * 1024); // 10MB
    
    const int NUM_THREADS = 4;
    const int ALLOCS_PER_THREAD = 1000;
    
    std::vector<std::thread> threads;
    std::vector<std::vector<CompactAllocator::offset_t>> thread_offsets(NUM_THREADS);
    
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&allocator, &thread_offsets, t]() {
            for (int i = 0; i < ALLOCS_PER_THREAD; i++) {
                auto offset = allocator.allocate(100);
                thread_offsets[t].push_back(offset);
                
                // Write unique value
                int* ptr = allocator.get_ptr<int>(offset);
                *ptr = t * ALLOCS_PER_THREAD + i;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all allocations are unique
    std::set<CompactAllocator::offset_t> all_offsets;
    for (const auto& vec : thread_offsets) {
        for (auto offset : vec) {
            EXPECT_TRUE(all_offsets.insert(offset).second);
        }
    }
    
    // Verify data integrity
    for (int t = 0; t < NUM_THREADS; t++) {
        for (int i = 0; i < ALLOCS_PER_THREAD; i++) {
            int* ptr = allocator.get_ptr<int>(thread_offsets[t][i]);
            EXPECT_EQ(*ptr, t * ALLOCS_PER_THREAD + i);
        }
    }
}