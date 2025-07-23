/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test XTree integration with persistence layer
 */

#include <gtest/gtest.h>
#include "indexdetails.hpp"
#include "xtree.h"
#include "persistence/memory_store.h"
#include <memory>
#include <filesystem>
#include <ctime>

namespace xtree {

class PersistenceIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up dimension labels
        dims_ = {"x", "y", "z"};
        dim_ptrs_.clear();
        for (const auto& dim : dims_) {
            dim_ptrs_.push_back(dim.c_str());
        }
    }

    void TearDown() override {
        // Clear the global cache to prevent interference between tests
        IndexDetails<IRecord>::clearCache();
        IndexDetails<DataRecord>::clearCache();
    }

    std::vector<std::string> dims_;
    std::vector<const char*> dim_ptrs_;
};

TEST_F(PersistenceIntegrationTest, CreateIndexInMemoryMode) {
    // Create an index in IN_MEMORY mode
    IndexDetails<IRecord> index(
        3,  // dimensions
        2,  // precision
        &dim_ptrs_,
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "test_field",  // field name
        IndexDetails<IRecord>::PersistenceMode::IN_MEMORY
    );
    
    EXPECT_EQ(index.getPersistenceMode(), IndexDetails<IRecord>::PersistenceMode::IN_MEMORY);
    EXPECT_NE(index.getStore(), nullptr);
    EXPECT_EQ(index.getDimensionCount(), 3);
    EXPECT_EQ(index.getPrecision(), 2);
}

TEST_F(PersistenceIntegrationTest, AllocateBucketInMemoryMode) {
    // Create an index in IN_MEMORY mode
    IndexDetails<IRecord> index(
        3,  // dimensions
        2,  // precision
        &dim_ptrs_,
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "test_allocation",  // field name
        IndexDetails<IRecord>::PersistenceMode::IN_MEMORY
    );
    
    // Try to allocate a bucket through the new persistence layer
    auto* bucket = XTreeAllocatorTraits<IRecord>::allocate_bucket(
        &index,
        true,  // isRoot
        nullptr,  // key
        nullptr,  // sourceChildren
        0,  // split_index
        false,  // isLeaf
        0  // sourceN
    );
    
    EXPECT_NE(bucket, nullptr);
    
    // Clean up
    // The store should handle cleanup when it's destroyed
}

TEST_F(PersistenceIntegrationTest, StoreRootNodeID) {
    // Create an index in IN_MEMORY mode
    IndexDetails<IRecord> index(
        3,  // dimensions
        2,  // precision
        &dim_ptrs_,
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "test_root",  // field name
        IndexDetails<IRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* store = index.getStore();
    ASSERT_NE(store, nullptr);
    
    // Allocate a node through the store
    persist::AllocResult alloc = store->allocate_node(
        sizeof(XTreeBucket<IRecord>),
        persist::NodeKind::Internal
    );
    
    EXPECT_NE(alloc.id.raw(), persist::NodeID::invalid().raw());
    EXPECT_NE(alloc.writable, nullptr);
    
    // Set it as root
    store->set_root(alloc.id, 1, nullptr, 0, "");
    
    // Get it back
    persist::NodeID root = store->get_root();
    EXPECT_EQ(root.raw(), alloc.id.raw());
}

TEST_F(PersistenceIntegrationTest, MultipleAllocations) {
    // Create an index in IN_MEMORY mode
    IndexDetails<IRecord> index(
        3,  // dimensions
        2,  // precision
        &dim_ptrs_,
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "test_multi_alloc",  // field name
        IndexDetails<IRecord>::PersistenceMode::IN_MEMORY
    );
    
    std::vector<XTreeBucket<IRecord>*> buckets;
    
    // Allocate multiple buckets
    for (int i = 0; i < 10; ++i) {
        auto* bucket = XTreeAllocatorTraits<IRecord>::allocate_bucket(
            &index,
            i == 0,  // first one is root
            nullptr,  // key
            nullptr,  // sourceChildren
            0,  // split_index
            i % 2 == 0,  // alternate leaf/internal
            0  // sourceN
        );
        
        EXPECT_NE(bucket, nullptr);
        buckets.push_back(bucket);
    }
    
    // Verify all buckets are distinct
    for (size_t i = 0; i < buckets.size(); ++i) {
        for (size_t j = i + 1; j < buckets.size(); ++j) {
            EXPECT_NE(buckets[i], buckets[j]);
        }
    }
}

// Test for DURABLE mode - this will help us understand what needs to be implemented
TEST_F(PersistenceIntegrationTest, CreateIndexDurableMode) {
    // Create a temporary directory for the test
    std::string test_dir = "/tmp/xtree_test_" + std::to_string(std::time(nullptr));

    // Scope block ensures index is destroyed before cleanup
    {
        // Create an index in DURABLE mode
        IndexDetails<IRecord> index(
            3,  // dimensions
            2,  // precision
            &dim_ptrs_,
            nullptr,  // JNIEnv
            nullptr,  // jobject
            "durable_create_test",  // field name
            IndexDetails<IRecord>::PersistenceMode::DURABLE,
            test_dir
        );

        EXPECT_EQ(index.getPersistenceMode(), IndexDetails<IRecord>::PersistenceMode::DURABLE);
        EXPECT_NE(index.getStore(), nullptr);
        EXPECT_TRUE(index.hasDurableStore());

        // Close the index before exiting scope to release file handles
        index.close();
    }

    // Clean up test directory
    std::filesystem::remove_all(test_dir);
}

TEST_F(PersistenceIntegrationTest, AllocateBucketDurableMode) {
    // Create a temporary directory for the test
    std::string test_dir = "/tmp/xtree_test_" + std::to_string(std::time(nullptr));

    // Scope block ensures index is destroyed before cleanup
    {
        // Create an index in DURABLE mode
        IndexDetails<IRecord> index(
            3,  // dimensions
            2,  // precision
            &dim_ptrs_,
            nullptr,  // JNIEnv
            nullptr,  // jobject
            "durable_alloc_test",  // field name
            IndexDetails<IRecord>::PersistenceMode::DURABLE,
            test_dir
        );

        // Try to allocate a bucket through the new persistence layer
        // This should allocate through DurableStore and return a valid bucket
        auto* bucket = XTreeAllocatorTraits<IRecord>::allocate_bucket(
            &index,
            true,  // isRoot
            nullptr,  // key
            nullptr,  // sourceChildren
            0,  // split_index
            false,  // isLeaf
            0  // sourceN
        );

        EXPECT_NE(bucket, nullptr);

        // TODO: Verify that the bucket has a valid NodeID stored
        // This will require adding NodeID field to XTreeBucket

        // Close the index before exiting scope to release file handles
        index.close();
    }

    // Clean up test directory
    std::filesystem::remove_all(test_dir);
}

} // namespace xtree