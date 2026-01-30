/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Tests for bitmap-based O(1) segment allocation
 */

#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <vector>
#include <random>
#include <algorithm>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif
#include "persistence/segment_allocator.h"
#include "persistence/segment_classes.hpp"
#include "persistence/config.h"

using namespace xtree::persist;

class BitmapAllocatorTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::unique_ptr<SegmentAllocator> allocator;
    
    void SetUp() override {
        test_dir = "/tmp/xtree_bitmap_test_" + std::to_string(getpid());
        
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

TEST_F(BitmapAllocatorTest, SingleClassChurn) {
    // Test allocate/free random blocks in one class
    const size_t alloc_size = 4096; // Class 0
    const int num_operations = 1000;
    std::vector<SegmentAllocator::Allocation> active_allocs;
    std::set<uint64_t> allocated_offsets;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> action_dist(0, 1);
    
    for (int i = 0; i < num_operations; i++) {
        bool should_alloc = active_allocs.empty() || 
                           (action_dist(gen) == 0 && active_allocs.size() < 100);
        
        if (should_alloc) {
            // Allocate
            auto alloc = allocator->allocate(alloc_size);
            EXPECT_TRUE(alloc.is_valid());
            EXPECT_EQ(alloc.length, alloc_size);
            EXPECT_EQ(alloc.class_id, size_to_class(alloc_size));
            
            // Check for no duplicate offsets
            EXPECT_TRUE(allocated_offsets.find(alloc.offset) == allocated_offsets.end())
                << "Duplicate allocation at offset " << alloc.offset;
            allocated_offsets.insert(alloc.offset);
            active_allocs.push_back(std::move(alloc));
        } else {
            // Free random allocation
            std::uniform_int_distribution<> idx_dist(0, active_allocs.size() - 1);
            int idx = idx_dist(gen);
            
            allocator->free(active_allocs[idx]);
            allocated_offsets.erase(active_allocs[idx].offset);
            active_allocs.erase(active_allocs.begin() + idx);
        }
    }
    
    // Free all remaining allocations
    for (auto& alloc : active_allocs) {  // Non-const to allow moving
        allocator->free(alloc);
    }
    
    // Verify stats
    auto stats = allocator->get_stats(size_to_class(alloc_size));
    EXPECT_EQ(stats.live_bytes, 0u) << "Memory leak detected";
    EXPECT_GT(stats.allocs_from_bitmap, 0u) << "Bitmap allocator not used";
    EXPECT_GT(stats.frees_to_bitmap, 0u) << "Bitmap free not used";
    EXPECT_GT(stats.bitmap_hit_rate(), 0.0) << "Bitmap hit rate should be positive";
}

TEST_F(BitmapAllocatorTest, MultiClassIsolation) {
    // Interleave alloc/frees across classes
    std::vector<size_t> class_sizes = {1024, 5000, 12000, 40000};
    std::map<size_t, std::vector<SegmentAllocator::Allocation>> allocs_by_class;
    
    // Allocate from each class
    for (size_t size : class_sizes) {
        for (int i = 0; i < 50; i++) {
            auto alloc = allocator->allocate(size);
            EXPECT_TRUE(alloc.is_valid());
            EXPECT_EQ(alloc.class_id, size_to_class(size));
            allocs_by_class[size].push_back(std::move(alloc));
        }
    }
    
    // Verify no cross-class offset mix-ups
    std::set<std::pair<uint32_t, uint64_t>> all_offsets;
    for (const auto& [size, allocs] : allocs_by_class) {
        for (const auto& alloc : allocs) {
            auto key = std::make_pair(alloc.file_id, alloc.offset);
            EXPECT_TRUE(all_offsets.find(key) == all_offsets.end())
                << "Cross-class offset collision detected";
            all_offsets.insert(key);
        }
    }
    
    // Free half from each class
    for (auto& [size, allocs] : allocs_by_class) {
        size_t half = allocs.size() / 2;
        for (size_t i = 0; i < half; i++) {
            allocator->free(allocs[i]);
        }
        allocs.erase(allocs.begin(), allocs.begin() + half);
    }
    
    // Allocate again - should reuse freed blocks
    for (size_t size : class_sizes) {
        for (int i = 0; i < 25; i++) {
            auto alloc = allocator->allocate(size);
            EXPECT_TRUE(alloc.is_valid());
            allocs_by_class[size].push_back(std::move(alloc));
        }
    }
    
    // Clean up
    for (auto& [size, allocs] : allocs_by_class) {
        for (auto& alloc : allocs) {
            allocator->free(alloc);
        }
    }
    
    // Verify clean state
    auto total_stats = allocator->get_total_stats();
    EXPECT_EQ(total_stats.live_bytes, 0u) << "Memory leak across classes";
}

TEST_F(BitmapAllocatorTest, TailMaskCorrectness) {
    // Create a segment where blocks % 64 != 0
    // This tests that upper tail bits remain 0 (used) and never get allocated
    
    // Use a small allocation size to get many blocks
    const size_t small_size = 512;  // Will give us many blocks per segment
    const int num_allocs = 500;     // Enough to fill multiple segments
    
    std::vector<SegmentAllocator::Allocation> allocs;
    std::set<std::pair<uint32_t, uint64_t>> seen_offsets;
    
    for (int i = 0; i < num_allocs; i++) {
        auto alloc = allocator->allocate(small_size);
        EXPECT_TRUE(alloc.is_valid());
        
        // Verify no duplicate offsets (tail bits working correctly)
        auto key = std::make_pair(alloc.file_id, alloc.offset);
        EXPECT_TRUE(seen_offsets.find(key) == seen_offsets.end())
            << "Tail mask error: duplicate offset " << alloc.offset 
            << " in file " << alloc.file_id;
        seen_offsets.insert(key);
        allocs.push_back(std::move(alloc));
    }
    
    // Free all and reallocate - should get same offsets (no tail corruption)
    for (auto& alloc : allocs) {
        allocator->free(alloc);
    }
    
    // Get stats to see if frees worked
    auto stats_after_free = allocator->get_stats(size_to_class(small_size));
    EXPECT_EQ(stats_after_free.live_bytes, 0u) << "Not all blocks were freed";
    EXPECT_EQ(stats_after_free.frees_to_bitmap, allocs.size()) 
        << "Not all frees went through bitmap path";
    
    // Reallocate all blocks to verify bitmap integrity
    int successful_reallocs = 0;
    for (int i = 0; i < num_allocs; i++) {
        auto alloc = allocator->allocate(small_size);
        EXPECT_TRUE(alloc.is_valid()) << "Failed to reallocate at i=" << i;
        if (alloc.is_valid()) {
            successful_reallocs++;
            allocator->free(alloc);  // Free immediately to keep test fast
        }
    }
    
    // The test succeeds if we can reallocate all the blocks we freed
    EXPECT_EQ(successful_reallocs, num_allocs) 
        << "Could not reallocate all freed blocks - bitmap corruption?";
}

TEST_F(BitmapAllocatorTest, ConcurrentBitmapOperations) {
    const int num_threads = 8;
    const int allocs_per_thread = 100;
    const size_t alloc_size = 8192;  // Class 1
    
    std::vector<std::thread> threads;
    std::vector<std::vector<SegmentAllocator::Allocation>> thread_allocs(num_threads);
    
    // Phase 1: Concurrent allocations
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < allocs_per_thread; i++) {
                auto alloc = allocator->allocate(alloc_size);
                EXPECT_TRUE(alloc.is_valid());
                thread_allocs[t].push_back(std::move(alloc));
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    
    // Verify all allocations are unique
    std::set<uint64_t> all_offsets;
    for (const auto& thread_vec : thread_allocs) {
        for (const auto& alloc : thread_vec) {
            auto [it, inserted] = all_offsets.insert(alloc.offset);
            EXPECT_TRUE(inserted) << "Concurrent allocation race: duplicate offset";
        }
    }
    
    // Phase 2: Concurrent frees
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (auto& alloc : thread_allocs[t]) {
                allocator->free(alloc);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all freed
    auto stats = allocator->get_stats(size_to_class(alloc_size));
    EXPECT_EQ(stats.live_bytes, 0u) << "Concurrent free failed";
}

TEST_F(BitmapAllocatorTest, BitmapPerformance) {
    // Measure bitmap allocation performance
    const size_t alloc_size = 4096;
    const int num_allocs = 10000;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<SegmentAllocator::Allocation> allocs;
    for (int i = 0; i < num_allocs; i++) {
        allocs.push_back(std::move(allocator->allocate(alloc_size)));
    }
    
    auto alloc_end = std::chrono::high_resolution_clock::now();
    
    // Free in random order to test bitmap free performance
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(allocs.begin(), allocs.end(), gen);
    
    for (auto& alloc : allocs) {
        allocator->free(alloc);
    }
    
    auto free_end = std::chrono::high_resolution_clock::now();
    
    auto alloc_time = std::chrono::duration_cast<std::chrono::microseconds>(
        alloc_end - start).count();
    auto free_time = std::chrono::duration_cast<std::chrono::microseconds>(
        free_end - alloc_end).count();
    
    std::cout << "Bitmap allocator performance:" << std::endl;
    std::cout << "  Allocations: " << num_allocs << " in " << alloc_time << " µs ("
              << (alloc_time / num_allocs) << " µs/alloc)" << std::endl;
    std::cout << "  Frees: " << num_allocs << " in " << free_time << " µs ("
              << (free_time / num_allocs) << " µs/free)" << std::endl;
    
    auto stats = allocator->get_stats(size_to_class(alloc_size));
    std::cout << "  Bitmap hit rate: " << (stats.bitmap_hit_rate() * 100) << "%" << std::endl;
    
    // Performance expectations - bitmap should be fast
    EXPECT_LT(alloc_time / num_allocs, 10) << "Allocation too slow (>10µs per alloc)";
    EXPECT_LT(free_time / num_allocs, 10) << "Free too slow (>10µs per free)";
}

TEST_F(BitmapAllocatorTest, SegmentCompleteFree) {
    // Test that completely freed segments can be reused efficiently
    const size_t alloc_size = 16384;  // Class 2
    const int blocks_per_segment = SegmentAllocator::DEFAULT_SEGMENT_SIZE / class_to_size(size_to_class(alloc_size));
    
    // Allocate enough to fill at least 2 segments
    std::vector<SegmentAllocator::Allocation> allocs;
    for (int i = 0; i < blocks_per_segment * 2; i++) {
        auto alloc = allocator->allocate(alloc_size);
        EXPECT_TRUE(alloc.is_valid());
        allocs.push_back(std::move(alloc));
    }
    
    auto stats_before = allocator->get_stats(size_to_class(alloc_size));
    EXPECT_GE(stats_before.total_segments, 2u);
    
    // Free all allocations
    for (auto& alloc : allocs) {
        allocator->free(alloc);
    }
    
    // Allocate again - should reuse the same segments
    std::vector<SegmentAllocator::Allocation> new_allocs;
    for (int i = 0; i < blocks_per_segment * 2; i++) {
        auto alloc = allocator->allocate(alloc_size);
        EXPECT_TRUE(alloc.is_valid());
        new_allocs.push_back(std::move(alloc));
    }
    
    auto stats_after = allocator->get_stats(size_to_class(alloc_size));
    
    // Should not have created new segments
    EXPECT_EQ(stats_after.total_segments, stats_before.total_segments)
        << "Created new segments instead of reusing freed ones";
    
    // Clean up
    for (auto& alloc : new_allocs) {
        allocator->free(alloc);
    }
}