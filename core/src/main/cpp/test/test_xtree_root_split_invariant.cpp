/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test for root split invariant: ensures setRootIdentity() is called
 * before on_root_split() to maintain proper cache state
 */

#include <gtest/gtest.h>
#include "indexdetails.hpp"
#include "datarecord.hpp"
#include "xtree.h"
#include "xtree.hpp"
#include "xtiter.h"
#include "xtree_allocator_traits.hpp"
#include <filesystem>
#include <memory>

namespace xtree {

// A tiny fixture that sets up a durable index with a very low max bucket size
class RootSplitInvariantTest : public ::testing::Test {
protected:
    std::string test_dir = "/tmp/xtree_root_split_test_" + std::to_string(std::time(nullptr));
    std::vector<std::string> dims_ = {"x", "y"};
    std::vector<const char*> dim_ptrs_;

    void SetUp() override {
        // Clean test dir between runs
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        // Set up dimension labels
        dim_ptrs_.clear();
        for (const auto& dim : dims_) {
            dim_ptrs_.push_back(dim.c_str());
        }
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir);
    }
};

TEST_F(RootSplitInvariantTest, RootSplitRegistersRootBeforeOnRootSplit) {
    IndexDetails<DataRecord> index(
        /*dims*/ 2,
        /*precision*/ 32,
        &dim_ptrs_,
        /*JNIEnv*/ nullptr,
        /*jobject*/ nullptr,
        "root_split_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );

    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());

    // Commit root creation so it's durable
    index.getStore()->commit(0);
    index.invalidate_root_cache();

    // Force inserts until a root split happens.
    // XTREE_M is typically 50, so we need at least that many inserts
    const int NUM_INSERTS = 60; // Slightly more than XTREE_M to ensure split

    persist::NodeID initial_root_id = index.root_node_id();

    for (int i = 0; i < NUM_INSERTS; i++) {
        auto* rec = new DataRecord(2, 32, "row_" + std::to_string(i));
        std::vector<double> pt = {double(i), double(i)};
        rec->putPoint(&pt);
        rec->putPoint(&pt);

        // Insert through IndexDetails root accessors
        // This will automatically get the fresh root after splits
        index.root_bucket<DataRecord>()->xt_insert(index.root_cache_node(), rec);
    }

    // By now a root split should have happened at least once.
    // The invariant is enforced by the assert in on_root_split().
    // If we reach here in a debug build, invariant held.
    auto* root = index.root_bucket<DataRecord>();
    ASSERT_NE(root, nullptr);

    // After split, root should have children
    ASSERT_GT(root->n(), 0) << "Root must have children after split";

    // Root ID should have changed
    persist::NodeID final_root_id = index.root_node_id();
    EXPECT_NE(initial_root_id.raw(), final_root_id.raw())
        << "Root ID should change after split";

    // The fact that we got here without assertion failure means:
    // 1. setRootIdentity() was called before on_root_split()
    // 2. The version tracking kept the cache consistent
    // 3. No attempt was made to reload from persistence when root was only in memory
}

TEST_F(RootSplitInvariantTest, MultipleSplitsStayConsistent) {
    IndexDetails<DataRecord> index(
        /*dims*/ 2,
        /*precision*/ 32,
        &dim_ptrs_,
        /*JNIEnv*/ nullptr,
        /*jobject*/ nullptr,
        "multi_split_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );

    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());

    // Insert enough records to cause multiple splits
    const int NUM_INSERTS = 500; // Should cause several splits

    for (int i = 0; i < NUM_INSERTS; i++) {
        auto* rec = new DataRecord(2, 32, "row_" + std::to_string(i));
        std::vector<double> pt = {double(i % 100), double(i % 100)};
        rec->putPoint(&pt);
        rec->putPoint(&pt);

        // Insert through IndexDetails root accessors
        index.root_bucket<DataRecord>()->xt_insert(index.root_cache_node(), rec);

        // Periodically commit
        if ((i + 1) % 100 == 0) {
            index.flush_dirty_buckets();
            index.getStore()->commit((i + 1) / 100);
        }
    }

    // Final flush and commit
    index.flush_dirty_buckets();
    index.getStore()->commit(999);

    // After multiple splits and commits, root should still be valid
    auto* finalRoot = index.root_bucket<DataRecord>();
    ASSERT_NE(finalRoot, nullptr);
    ASSERT_GT(finalRoot->n(), 0) << "Root must have children after multiple splits";

    // Test that we can still search
    auto* query = new DataRecord(2, 32, "query");
    std::vector<double> pt1 = {0.0, 0.0};
    std::vector<double> pt2 = {10.0, 10.0};
    query->putPoint(&pt1);
    query->putPoint(&pt2);

    auto* it = finalRoot->getIterator(index.root_cache_node(), query, INTERSECTS);
    ASSERT_NE(it, nullptr) << "Should be able to create iterator after multiple splits";

    int count = 0;
    while (it->hasNext()) {
        it->next();
        count++;
    }
    EXPECT_GT(count, 0) << "Should find some records in range";

    delete it;
    delete query;
}

} // namespace xtree