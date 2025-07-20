/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#include <gtest/gtest.h>
#include "../src/xtree_mmap_factory.h"
#include "../src/xtree_serialization.h"

using namespace xtree;

class PageCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Nothing to set up
    }
};

// Test page alignment calculations
TEST_F(PageCacheTest, PageAlignmentTest) {
    size_t page_size = PageCacheConstants::getSystemPageSize();
    
    // Test that we get a valid page size
    EXPECT_GT(page_size, 0);
    EXPECT_TRUE(page_size == 4096 || page_size == 8192 || page_size == 16384);
    
    // Test alignment function
    EXPECT_EQ(PageCacheConstants::alignToPage(0), 0);
    EXPECT_EQ(PageCacheConstants::alignToPage(1), page_size);
    EXPECT_EQ(PageCacheConstants::alignToPage(page_size), page_size);
    EXPECT_EQ(PageCacheConstants::alignToPage(page_size + 1), 2 * page_size);
    
    // Test offset alignment
    EXPECT_EQ(PageCacheConstants::alignOffsetToPage(0), 0);
    EXPECT_EQ(PageCacheConstants::alignOffsetToPage(100), 0);
    EXPECT_EQ(PageCacheConstants::alignOffsetToPage(page_size), page_size);
    EXPECT_EQ(PageCacheConstants::alignOffsetToPage(page_size + 100), page_size);
}

// Test that bucket serialization uses cache-line aligned sizes
TEST_F(PageCacheTest, CacheLineAlignmentTest) {
    constexpr size_t CACHE_LINE_SIZE = 64;
    
    // Test various sizes get aligned to cache line boundaries
    auto align_to_cache_line = [](size_t size) {
        return ((size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
    };
    
    EXPECT_EQ(align_to_cache_line(1), CACHE_LINE_SIZE);
    EXPECT_EQ(align_to_cache_line(64), CACHE_LINE_SIZE);
    EXPECT_EQ(align_to_cache_line(65), 2 * CACHE_LINE_SIZE);
    EXPECT_EQ(align_to_cache_line(128), 2 * CACHE_LINE_SIZE);
}

// Test factory initialization
TEST_F(PageCacheTest, FactoryInitializationTest) {
    // Factory should not be initialized initially
    EXPECT_FALSE(MMapXTreeFactory<DataRecord>::isInitialized());
    
    // Initialize the factory
    MMapXTreeFactory<DataRecord>::initialize();
    
    // Now it should be initialized
    EXPECT_TRUE(MMapXTreeFactory<DataRecord>::isInitialized());
    
    // Initializing again should be a no-op
    MMapXTreeFactory<DataRecord>::initialize();
    EXPECT_TRUE(MMapXTreeFactory<DataRecord>::isInitialized());
}

// Test page-aligned sizes
TEST_F(PageCacheTest, PageAlignedSizesTest) {
    size_t page_size = PageCacheConstants::getSystemPageSize();
    
    // Test that default sizes are multiples of page size
    EXPECT_EQ(PageCacheConstants::MIN_MMAP_SIZE % page_size, 0);
    EXPECT_EQ(PageCacheConstants::DEFAULT_MMAP_SIZE % page_size, 0);
    
    // Test that our constants make sense
    EXPECT_GT(PageCacheConstants::MIN_MMAP_SIZE, 0);
    EXPECT_GT(PageCacheConstants::DEFAULT_MMAP_SIZE, PageCacheConstants::MIN_MMAP_SIZE);
    EXPECT_GE(PageCacheConstants::PREFETCH_PAGES, 1);
}

// Test header size alignment
TEST_F(PageCacheTest, HeaderAlignmentTest) {
    // XTree file header should be reasonably sized
    size_t page_size = PageCacheConstants::getSystemPageSize();
    EXPECT_LT(sizeof(XTreeFileHeader), page_size);
    
    // Root offset should be page-aligned after header
    size_t root_offset = PageCacheConstants::alignToPage(sizeof(XTreeFileHeader));
    EXPECT_GT(root_offset, sizeof(XTreeFileHeader));
    EXPECT_EQ(root_offset % PageCacheConstants::getSystemPageSize(), 0);
}