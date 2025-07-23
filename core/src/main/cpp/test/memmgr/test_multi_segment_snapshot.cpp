/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test multi-segment snapshot functionality
 */

#include <gtest/gtest.h>
#include "../../src/memmgr/compact_snapshot_manager.hpp"
#include <chrono>
#include <vector>
#include <random>

using namespace xtree;
using namespace std::chrono;

class MultiSegmentSnapshotTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test files
        std::remove("test_multi_segment.snapshot");
        std::remove("test_multi_segment.snapshot.tmp");
    }
    
    void TearDown() override {
        std::remove("test_multi_segment.snapshot");
        std::remove("test_multi_segment.snapshot.tmp");
    }
};

TEST_F(MultiSegmentSnapshotTest, SaveSingleSegmentSnapshot) {
    const char* test_file = "test_multi_segment.snapshot";
    
    // Create manager and allocate some data
    CompactSnapshotManager manager(test_file);
    auto* allocator = manager.get_allocator();
    
    // Allocate some test data (should fit in single segment)
    std::vector<CompactAllocator::offset_t> offsets;
    for (int i = 0; i < 1000; ++i) {
        auto offset = allocator->allocate(1024);
        ASSERT_NE(offset, CompactAllocator::INVALID_OFFSET);
        
        // Write test pattern
        int* data = allocator->get_ptr<int>(offset);
        for (int j = 0; j < 256; ++j) {
            data[j] = i * 1000 + j;
        }
        offsets.push_back(offset);
    }
    
    EXPECT_EQ(allocator->get_segment_count(), 1);
    std::cout << "Single segment test - Used size: " 
              << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
    
    // Save snapshot
    manager.save_snapshot();
    
    // Verify file exists
    struct stat st;
    ASSERT_EQ(stat(test_file, &st), 0);
    std::cout << "Snapshot file size: " << st.st_size / 1024.0 << " KB\n";
}

TEST_F(MultiSegmentSnapshotTest, SaveMultiSegmentSnapshot) {
    const char* test_file = "test_multi_segment.snapshot";
    
    // Create manager and force multi-segment allocation
    CompactSnapshotManager manager(test_file);
    auto* allocator = manager.get_allocator();
    
    // Allocate enough to trigger multiple segments
    // Initial segment is 64MB, so allocate ~100MB
    const size_t LARGE_ALLOC = 1024 * 1024;  // 1MB each
    std::vector<CompactAllocator::offset_t> offsets;
    
    std::cout << "Allocating large chunks to trigger multi-segment...\n";
    for (int i = 0; i < 100; ++i) {
        auto offset = allocator->allocate(LARGE_ALLOC);
        ASSERT_NE(offset, CompactAllocator::INVALID_OFFSET);
        
        // Write test pattern
        char* data = allocator->get_ptr<char>(offset);
        std::memset(data, i % 256, LARGE_ALLOC);
        offsets.push_back(offset);
        
        if (i % 20 == 0) {
            std::cout << "  Allocated " << (i + 1) << " MB, segments: " 
                      << allocator->get_segment_count() << "\n";
        }
    }
    
    size_t num_segments = allocator->get_segment_count();
    EXPECT_GT(num_segments, 1) << "Should have multiple segments";
    
    std::cout << "Multi-segment test - Segments: " << num_segments
              << ", Total used: " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
    
    // Save multi-segment snapshot
    auto start = high_resolution_clock::now();
    manager.save_snapshot();
    auto duration = duration_cast<milliseconds>(high_resolution_clock::now() - start);
    
    std::cout << "Multi-segment snapshot save time: " << duration.count() << " ms\n";
    
    // Verify file exists
    struct stat st;
    ASSERT_EQ(stat(test_file, &st), 0);
    std::cout << "Multi-segment snapshot file size: " << st.st_size / (1024.0 * 1024.0) << " MB\n";
    
    // The file should be larger than the used size due to metadata
    EXPECT_GT(st.st_size, allocator->get_used_size());
    
    // Verify data integrity by reading back through allocator
    for (size_t i = 0; i < offsets.size(); ++i) {
        char* data = allocator->get_ptr<char>(offsets[i]);
        ASSERT_NE(data, nullptr);
        
        // Verify first few bytes
        for (int j = 0; j < 10; ++j) {
            EXPECT_EQ(static_cast<unsigned char>(data[j]), i % 256)
                << "Data mismatch at offset " << i << ", byte " << j;
        }
    }
}

TEST_F(MultiSegmentSnapshotTest, PerformanceWithLargeDataset) {
    const char* test_file = "test_multi_segment.snapshot";
    
    // Test with a realistic large dataset
    CompactSnapshotManager manager(test_file);
    auto* allocator = manager.get_allocator();
    
    // Simulate XTree node allocations
    std::mt19937 gen(42);
    std::uniform_int_distribution<> size_dist(100, 2000);
    
    const int NUM_ALLOCATIONS = 50000;
    std::vector<CompactAllocator::offset_t> offsets;
    
    std::cout << "\nSimulating " << NUM_ALLOCATIONS << " XTree allocations...\n";
    
    auto alloc_start = high_resolution_clock::now();
    for (int i = 0; i < NUM_ALLOCATIONS; ++i) {
        size_t size = size_dist(gen);
        auto offset = allocator->allocate(size);
        ASSERT_NE(offset, CompactAllocator::INVALID_OFFSET);
        offsets.push_back(offset);
        
        if (i % 10000 == 0 && i > 0) {
            std::cout << "  Progress: " << i << "/" << NUM_ALLOCATIONS 
                      << ", segments: " << allocator->get_segment_count() << "\n";
        }
    }
    auto alloc_duration = duration_cast<milliseconds>(high_resolution_clock::now() - alloc_start);
    
    std::cout << "\nAllocation complete:\n";
    std::cout << "  Time: " << alloc_duration.count() << " ms\n";
    std::cout << "  Segments: " << allocator->get_segment_count() << "\n";
    std::cout << "  Total used: " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
    std::cout << "  Allocations/sec: " << (NUM_ALLOCATIONS * 1000.0 / alloc_duration.count()) << "\n";
    
    // Save snapshot (now supports multi-segment!)
    auto save_start = high_resolution_clock::now();
    manager.save_snapshot();
    auto save_duration = duration_cast<milliseconds>(high_resolution_clock::now() - save_start);
    
    struct stat st;
    ASSERT_EQ(stat(test_file, &st), 0);
    
    std::cout << "\nSnapshot save complete:\n";
    std::cout << "  Time: " << save_duration.count() << " ms\n";
    std::cout << "  File size: " << st.st_size / (1024.0 * 1024.0) << " MB\n";
    std::cout << "  Throughput: " << (st.st_size / (1024.0 * 1024.0)) / (save_duration.count() / 1000.0) 
              << " MB/s\n";
    
    if (allocator->get_segment_count() > 1) {
        std::cout << "  Multi-segment snapshot with " << allocator->get_segment_count() << " segments\n";
    }
}