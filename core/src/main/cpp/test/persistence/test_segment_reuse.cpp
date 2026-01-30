/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test to verify segment reuse is actually happening
 */

#include <gtest/gtest.h>
#include "../../src/persistence/segment_allocator.h"
#include "../../src/persistence/config.h"
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;
using namespace xtree::persist;

class SegmentReuseTest : public ::testing::Test {
protected:
    std::string test_dir = "./test_reuse_data";
    SegmentAllocator* allocator = nullptr;
    
    void SetUp() override {
        if (fs::exists(test_dir)) {
            fs::remove_all(test_dir);
        }
        fs::create_directories(test_dir);
        allocator = new SegmentAllocator(test_dir);
    }
    
    void TearDown() override {
        if (allocator) {
            allocator->close_all();
            delete allocator;
        }
        if (fs::exists(test_dir)) {
            fs::remove_all(test_dir);
        }
    }
};

TEST_F(SegmentReuseTest, VerifyBitmapReuse) {
    // Test that freed segments are actually reused
    
    // Phase 1: Allocate some segments
    // Store just the info we need since Allocation has a non-copyable Pin
    struct AllocInfo {
        uint32_t file_id;
        uint32_t segment_id;
        uint64_t offset;
        uint32_t length;
        uint8_t class_id;
    };
    std::vector<AllocInfo> allocations;
    const size_t NUM_ALLOCS = 10;
    const size_t ALLOC_SIZE = 256;  // Use minimum size
    
    std::cout << "\n=== Phase 1: Initial allocations ===\n";
    for (size_t i = 0; i < NUM_ALLOCS; i++) {
        auto alloc = allocator->allocate(ALLOC_SIZE);
        ASSERT_TRUE(alloc.is_valid());
        allocations.push_back({alloc.file_id, alloc.segment_id, alloc.offset, alloc.length, alloc.class_id});
    }
    
    // Get stats for the size class we're using (256B is class 0)
    uint8_t class_id = 0;  // 256B size class
    auto stats1 = allocator->get_stats(class_id);
    std::cout << "After initial allocations:\n"
              << "  Total allocations: " << stats1.total_allocations << "\n"
              << "  Live bytes: " << stats1.live_bytes << "\n"
              << "  Dead bytes: " << stats1.dead_bytes << "\n"
              << "  Allocs from bitmap: " << stats1.allocs_from_bitmap << "\n";
    
    EXPECT_EQ(stats1.total_allocations, NUM_ALLOCS);
    EXPECT_EQ(stats1.allocs_from_bitmap, 0) << "First allocations should not be from bitmap";
    EXPECT_EQ(stats1.dead_bytes, 0) << "No dead bytes yet";
    
    // Phase 2: Free half of them
    std::cout << "\n=== Phase 2: Freeing half ===\n";
    for (size_t i = 0; i < NUM_ALLOCS / 2; i++) {
        // Reconstruct Allocation for freeing
        SegmentAllocator::Allocation to_free;
        to_free.file_id = allocations[i].file_id;
        to_free.segment_id = allocations[i].segment_id;
        to_free.offset = allocations[i].offset;
        to_free.length = allocations[i].length;
        to_free.class_id = allocations[i].class_id;
        allocator->free(to_free);
    }
    
    auto stats2 = allocator->get_stats(class_id);
    std::cout << "After freeing half:\n"
              << "  Total frees: " << stats2.total_frees << "\n"
              << "  Live bytes: " << stats2.live_bytes << "\n"
              << "  Dead bytes: " << stats2.dead_bytes << "\n"
              << "  Frees to bitmap: " << stats2.frees_to_bitmap << "\n";
    
    EXPECT_EQ(stats2.total_frees, NUM_ALLOCS / 2);
    EXPECT_GT(stats2.dead_bytes, 0) << "Should have dead bytes after freeing";
    EXPECT_EQ(stats2.frees_to_bitmap, NUM_ALLOCS / 2) << "All frees should go to bitmap";
    
    // Phase 3: Allocate again - should reuse freed segments
    std::cout << "\n=== Phase 3: New allocations (should reuse) ===\n";
    std::vector<AllocInfo> new_allocations;
    
    for (size_t i = 0; i < NUM_ALLOCS / 2; i++) {
        auto alloc = allocator->allocate(ALLOC_SIZE);
        ASSERT_TRUE(alloc.is_valid());
        new_allocations.push_back({alloc.file_id, alloc.segment_id, alloc.offset, alloc.length, alloc.class_id});
    }
    
    auto stats3 = allocator->get_stats(class_id);
    std::cout << "After reallocation:\n"
              << "  Total allocations: " << stats3.total_allocations << "\n"
              << "  Allocs from bitmap (reused): " << stats3.allocs_from_bitmap << "\n"
              << "  Allocs from bump: " << stats3.allocs_from_bump << "\n"
              << "  Live bytes: " << stats3.live_bytes << "\n"
              << "  Dead bytes: " << stats3.dead_bytes << "\n"
              << "  Bitmap hit rate: " << (stats3.bitmap_hit_rate() * 100) << "%\n";
    
    // Key assertion: We should have reused the freed segments
    EXPECT_GT(stats3.allocs_from_bitmap, 0) << "Should have reused freed segments";
    EXPECT_EQ(stats3.allocs_from_bitmap, NUM_ALLOCS / 2) 
        << "All new allocations should come from reused segments";
    
    // Dead bytes should decrease after reuse
    EXPECT_LT(stats3.dead_bytes, stats2.dead_bytes) 
        << "Dead bytes should decrease after reusing segments";
    
    // Verify the actual reuse by checking segment IDs
    std::cout << "\n=== Verifying segment reuse ===\n";
    bool found_reuse = false;
    for (const auto& new_alloc : new_allocations) {
        for (size_t i = 0; i < NUM_ALLOCS / 2; i++) {
            if (new_alloc.segment_id == allocations[i].segment_id &&
                new_alloc.offset == allocations[i].offset) {
                std::cout << "  Reused segment " << new_alloc.segment_id 
                         << " at offset " << new_alloc.offset << "\n";
                found_reuse = true;
            }
        }
    }
    
    EXPECT_TRUE(found_reuse) << "Should find at least one reused segment location";
}

TEST_F(SegmentReuseTest, ReuseWithDifferentSizes) {
    // Test that reuse works even with different allocation sizes
    
    std::cout << "\n=== Testing reuse with size changes ===\n";
    
    // Allocate a 512B segment
    auto alloc1 = allocator->allocate(512);
    ASSERT_TRUE(alloc1.is_valid());
    
    // Get stats for the size class we're using (256B is class 0)
    uint8_t class_id = 0;  // 256B size class
    auto stats1 = allocator->get_stats(class_id);
    std::cout << "After 512B allocation:\n"
              << "  Allocated segment in class " << (int)alloc1.class_id 
              << " (size " << alloc1.length << ")\n";
    
    // Free it
    allocator->free(alloc1);
    
    auto stats2 = allocator->get_stats(class_id);
    std::cout << "After freeing:\n"
              << "  Dead bytes: " << stats2.dead_bytes << "\n";
    
    // Try to allocate 256B - should reuse part of the 512B segment
    auto alloc2 = allocator->allocate(256);
    ASSERT_TRUE(alloc2.is_valid());
    
    auto stats3 = allocator->get_stats(class_id);
    std::cout << "After 256B allocation:\n"
              << "  Allocs from bitmap: " << stats3.allocs_from_bitmap << "\n"
              << "  Dead bytes: " << stats3.dead_bytes << "\n";
    
    // Should NOT reuse because 256B goes to different size class
    if (alloc1.class_id != alloc2.class_id) {
        EXPECT_EQ(stats3.allocs_from_bitmap, 0) 
            << "Different size classes should not reuse segments";
    } else {
        EXPECT_GT(stats3.allocs_from_bitmap, 0) 
            << "Same size class should reuse";
    }
    
    // Now free the 256B and allocate another 512B
    allocator->free(alloc2);
    auto alloc3 = allocator->allocate(512);
    
    auto stats4 = allocator->get_stats(class_id);
    std::cout << "After second 512B allocation:\n"
              << "  Allocs from bitmap: " << stats4.allocs_from_bitmap << "\n"
              << "  Comparing segments: alloc1.segment_id=" << alloc1.segment_id
              << " alloc3.segment_id=" << alloc3.segment_id << "\n";
    
    // This should reuse the original 512B segment
    if (alloc1.class_id == alloc3.class_id) {
        EXPECT_EQ(alloc1.segment_id, alloc3.segment_id)
            << "Should reuse the same segment";
        EXPECT_EQ(alloc1.offset, alloc3.offset)
            << "Should reuse the same offset";
    }
}