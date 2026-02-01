/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Incremental durability unit tests for XTree
 * Tests individual operations in isolation to identify specific issues
 */

#include <gtest/gtest.h>
#include "indexdetails.hpp"
#include "persistence/durable_store.h"
#include "xtree.h"
#include "xtree.hpp"
#include "datarecord.hpp"
#include "config.h"  // For XTREE_M
#include <memory>
#include <filesystem>
#include <set>
#include <unistd.h>  // for getpid()

namespace xtree {

class XTreeDurabilityUnitTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/xtree_durability_unit_" + std::to_string(getpid());
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);

        // Clear the static cache to prevent interference between tests
        IndexDetails<DataRecord>::clearCache();

        // Set up dimension labels
        dims_ = {"x", "y"};
        dim_ptrs_.clear();
        for (const auto& dim : dims_) {
            dim_ptrs_.push_back(dim.c_str());
        }
    }

    void TearDown() override {
        // Clear cache before removing files to ensure clean state
        IndexDetails<DataRecord>::clearCache();
        std::filesystem::remove_all(test_dir_);
    }

    std::string test_dir_;
    std::vector<std::string> dims_;
    std::vector<const char*> dim_ptrs_;
};

// Test 1: Root Allocation - verify root is properly initialized
TEST_F(XTreeDurabilityUnitTest, RootAllocationTest) {
    std::cout << "\n=== Test 1: Root Allocation ===" << std::endl;
    std::cout << "XTREE_M = " << XTREE_M << std::endl;

    IndexDetails<DataRecord> index(
        2, 32, &dim_ptrs_, nullptr, nullptr,
        "root_alloc_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    ASSERT_TRUE(index.hasDurableStore());
    auto* store = index.getStore();
    ASSERT_NE(store, nullptr);

    // Initialize root
    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());

    auto* cachedRoot = index.root_cache_node();
    ASSERT_NE(cachedRoot, nullptr);
    auto* root = index.root_bucket<DataRecord>();
    ASSERT_NE(root, nullptr);

    // Verify root properties
    persist::NodeID rootId = root->getNodeID();
    std::cout << "Root NodeID: " << rootId.raw()
              << " (handle=" << rootId.handle_index()
              << ", tag=" << static_cast<int>(rootId.tag()) << ")" << std::endl;
    ASSERT_TRUE(rootId.valid());

    // Check root has 0 children initially
    EXPECT_EQ(root->n(), 0) << "Root should start with 0 children";

    // Commit the root to make it visible
    store->commit(0);

    // Verify root is in ObjectTable after commit
    persist::NodeKind kind;
    bool found = store->get_node_kind(rootId, kind);
    ASSERT_TRUE(found) << "Root should be found in ObjectTable after commit";
    EXPECT_EQ(kind, persist::NodeKind::Leaf) << "Root should be a Leaf initially";

    std::cout << "✓ Root properly initialized as Leaf with 0 children" << std::endl;
}

// Test 2: Fill to Capacity - insert XTREE_M records without triggering split
TEST_F(XTreeDurabilityUnitTest, FillToCapacityTest) {
    std::cout << "\n=== Test 2: Fill to Capacity (no split) ===" << std::endl;

    IndexDetails<DataRecord> index(
        2, 32, &dim_ptrs_, nullptr, nullptr,
        "fill_capacity_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    ASSERT_TRUE(index.hasDurableStore());
    auto* store = index.getStore();

    // Initialize root
    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());
    auto* cachedRoot = index.root_cache_node();
    auto* root = index.root_bucket<DataRecord>();

    // Commit root so it's visible
    store->commit(0);

    std::cout << "Inserting " << XTREE_M << " records..." << std::endl;

    // Insert exactly XTREE_M records (should NOT trigger split)
    std::set<std::string> insertedIds;
    for (int i = 0; i < XTREE_M; ++i) {
        std::string recordId = "rec_" + std::to_string(i);
        DataRecord* dr = new DataRecord(2, 32, recordId);

        // Use clustered points to avoid immediate splits
        double x = i * 0.001;
        double y = i * 0.001;
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        dr->putPoint(&point);

        insertedIds.insert(recordId);

        // Always use fresh root references
        cachedRoot = index.root_cache_node();
        root = index.root_bucket<DataRecord>();

        // Debug output for first few inserts
        if (i < 3) {
            std::cout << "  Before insert " << i << ": root n=" << root->n()
                      << ", NodeID=" << root->getNodeID().raw() << std::endl;
        }

        try {
            root->xt_insert(cachedRoot, dr);
        } catch (const std::exception& e) {
            FAIL() << "Insert " << i << " failed: " << e.what();
        }

        // Periodic commits
        if ((i + 1) % 10 == 0) {
            store->commit(i + 1);
        }
    }

    // Final commit
    store->commit(XTREE_M);

    // Refresh root after all inserts
    cachedRoot = index.root_cache_node();
    root = index.root_bucket<DataRecord>();

    std::cout << "After " << XTREE_M << " inserts: root n=" << root->n() << std::endl;

    // Verify root is still a leaf (no split should have occurred)
    persist::NodeKind kind;
    bool found = store->get_node_kind(root->getNodeID(), kind);
    ASSERT_TRUE(found);
    EXPECT_EQ(kind, persist::NodeKind::Leaf) << "Root should still be a Leaf after " << XTREE_M << " inserts";

    // Verify all records are retrievable
    DataRecord* query = new DataRecord(2, 32, "query");
    std::vector<double> min_pt = {-1.0, -1.0};
    std::vector<double> max_pt = {100.0, 100.0};
    query->putPoint(&min_pt);
    query->putPoint(&max_pt);

    auto* iter = root->getIterator(cachedRoot, query, INTERSECTS);
    std::set<std::string> foundIds;
    while (iter->hasNext()) {
        auto* rec = iter->next();
        if (rec && rec->isDataNode()) {
            foundIds.insert(static_cast<DataRecord*>(rec)->getRowID());
        }
    }
    delete iter;
    delete query;

    EXPECT_EQ(foundIds.size(), insertedIds.size())
        << "Should find all " << XTREE_M << " inserted records";
    EXPECT_EQ(foundIds, insertedIds) << "Found records should match inserted records";

    std::cout << "✓ Successfully inserted " << XTREE_M
              << " records without split, all retrievable" << std::endl;
}

// Test 3: First Split - insert XTREE_M + 1 to trigger root split
TEST_F(XTreeDurabilityUnitTest, FirstSplitTest) {
    std::cout << "\n=== Test 3: First Split (XTREE_M + 1) ===" << std::endl;

    IndexDetails<DataRecord> index(
        2, 32, &dim_ptrs_, nullptr, nullptr,
        "first_split_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    ASSERT_TRUE(index.hasDurableStore());
    auto* store = index.getStore();

    // Initialize root
    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());
    auto* cachedRoot = index.root_cache_node();
    auto* root = index.root_bucket<DataRecord>();

    persist::NodeID originalRootId = root->getNodeID();

    // Commit root
    store->commit(0);

    std::cout << "Initial root NodeID: " << originalRootId.raw() << std::endl;

    // Insert XTREE_M records first
    std::set<std::string> insertedIds;
    for (int i = 0; i < XTREE_M; ++i) {
        std::string recordId = "rec_" + std::to_string(i);
        DataRecord* dr = new DataRecord(2, 32, recordId);

        double x = i * 0.001;
        double y = i * 0.001;
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        dr->putPoint(&point);

        insertedIds.insert(recordId);

        cachedRoot = index.root_cache_node();
        root = index.root_bucket<DataRecord>();

        try {
            root->xt_insert(cachedRoot, dr);
        } catch (const std::exception& e) {
            FAIL() << "Insert " << i << " failed: " << e.what();
        }

        if ((i + 1) % 10 == 0) {
            store->commit(i + 1);
        }
    }

    // Commit before the split-triggering insert
    store->commit(XTREE_M);

    // Verify root is still a leaf before the split
    cachedRoot = index.root_cache_node();
    root = index.root_bucket<DataRecord>();
    persist::NodeKind kindBefore;
    store->get_node_kind(root->getNodeID(), kindBefore);
    EXPECT_EQ(kindBefore, persist::NodeKind::Leaf) << "Root should be Leaf before split";

    std::cout << "Before split: root n=" << root->n()
              << ", kind=" << static_cast<int>(kindBefore) << std::endl;

    // Insert one more record to trigger split
    std::cout << "Inserting record " << XTREE_M << " to trigger split..." << std::endl;

    std::string triggerId = "rec_" + std::to_string(XTREE_M);
    DataRecord* trigger = new DataRecord(2, 32, triggerId);
    double tx = XTREE_M * 0.001;
    double ty = XTREE_M * 0.001;
    std::vector<double> triggerPt = {tx, ty};
    trigger->putPoint(&triggerPt);
    trigger->putPoint(&triggerPt);
    insertedIds.insert(triggerId);

    try {
        root->xt_insert(cachedRoot, trigger);
    } catch (const std::exception& e) {
        FAIL() << "Split-triggering insert failed: " << e.what();
    }

    // Commit after split
    store->commit(XTREE_M + 1);

    // Get fresh root after split
    cachedRoot = index.root_cache_node();
    root = index.root_bucket<DataRecord>();
    persist::NodeID newRootId = root->getNodeID();

    std::cout << "After split: new root NodeID=" << newRootId.raw()
              << ", n=" << root->n() << std::endl;

    // Verify root has changed and is now internal
    EXPECT_NE(newRootId.raw(), originalRootId.raw())
        << "Root NodeID should change after split";

    persist::NodeKind kindAfter;
    bool found = store->get_node_kind(newRootId, kindAfter);
    ASSERT_TRUE(found) << "New root should be in ObjectTable";
    EXPECT_EQ(kindAfter, persist::NodeKind::Internal)
        << "Root should be Internal after split";

    // Root should have 2 children after split
    EXPECT_EQ(root->n(), 2) << "Root should have 2 children after split";

    // Verify all records are still retrievable
    DataRecord* query = new DataRecord(2, 32, "query");
    std::vector<double> min_pt = {-1.0, -1.0};
    std::vector<double> max_pt = {100.0, 100.0};
    query->putPoint(&min_pt);
    query->putPoint(&max_pt);

    auto* iter = root->getIterator(cachedRoot, query, INTERSECTS);
    std::set<std::string> foundIds;
    while (iter->hasNext()) {
        auto* rec = iter->next();
        if (rec && rec->isDataNode()) {
            foundIds.insert(static_cast<DataRecord*>(rec)->getRowID());
        }
    }
    delete iter;
    delete query;

    EXPECT_EQ(foundIds.size(), XTREE_M + 1)
        << "Should find all " << (XTREE_M + 1) << " records after split";
    EXPECT_EQ(foundIds, insertedIds)
        << "All records should be retrievable after split";

    std::cout << "✓ Split successful: root is now Internal with 2 children, "
              << "all " << (XTREE_M + 1) << " records retrievable" << std::endl;
}

// Test 4: Commit and Reload - verify persistence across restart
TEST_F(XTreeDurabilityUnitTest, CommitReloadTest) {
    std::cout << "\n=== Test 4: Commit and Reload ===" << std::endl;

    std::set<std::string> insertedIds;
    persist::NodeID finalRootId;

    // Phase 1: Create tree with split and commit
    {
        IndexDetails<DataRecord> index(
            2, 32, &dim_ptrs_, nullptr, nullptr,
            "reload_test",
            IndexDetails<DataRecord>::PersistenceMode::DURABLE,
            test_dir_
        );

        auto* store = index.getStore();
        ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());

        // Commit root
        store->commit(0);

        // Insert XTREE_M + 1 records to trigger split
        for (int i = 0; i <= XTREE_M; ++i) {
            std::string recordId = "rec_" + std::to_string(i);
            DataRecord* dr = new DataRecord(2, 32, recordId);

            double x = i * 0.001;
            double y = i * 0.001;
            std::vector<double> point = {x, y};
            dr->putPoint(&point);
            dr->putPoint(&point);

            insertedIds.insert(recordId);

            auto* cachedRoot = index.root_cache_node();
            auto* root = index.root_bucket<DataRecord>();

            root->xt_insert(cachedRoot, dr);

            if ((i + 1) % 10 == 0) {
                store->commit(i + 1);
            }
        }

        // Final commit
        store->commit(XTREE_M + 1);

        auto* root = index.root_bucket<DataRecord>();
        finalRootId = root->getNodeID();

        std::cout << "Before close: root NodeID=" << finalRootId.raw()
                  << ", n=" << root->n() << std::endl;

        // Clean shutdown
        index.close();
    }

    std::cout << "Index closed. Reopening from disk..." << std::endl;

    // Phase 2: Reopen and verify
    {
        IndexDetails<DataRecord> index(
            2, 32, &dim_ptrs_, nullptr, nullptr,
            "reload_test",
            IndexDetails<DataRecord>::PersistenceMode::DURABLE,
            test_dir_
        );

        ASSERT_TRUE(index.hasDurableStore());

        // Root should be recovered automatically
        auto* cachedRoot = index.root_cache_node();
        ASSERT_NE(cachedRoot, nullptr) << "Root should be recovered";
        auto* root = index.root_bucket<DataRecord>();
        ASSERT_NE(root, nullptr) << "Root bucket should be recovered";

        persist::NodeID recoveredRootId = root->getNodeID();
        std::cout << "After reload: root NodeID=" << recoveredRootId.raw()
                  << ", n=" << root->n() << std::endl;

        // Verify root ID matches
        EXPECT_EQ(recoveredRootId.raw(), finalRootId.raw())
            << "Recovered root should have same NodeID";

        // Verify all records are retrievable
        DataRecord* query = new DataRecord(2, 32, "query");
        std::vector<double> min_pt = {-1.0, -1.0};
        std::vector<double> max_pt = {100.0, 100.0};
        query->putPoint(&min_pt);
        query->putPoint(&max_pt);

        auto* iter = root->getIterator(cachedRoot, query, INTERSECTS);
        std::set<std::string> foundIds;
        while (iter->hasNext()) {
            auto* rec = iter->next();
            if (rec && rec->isDataNode()) {
                foundIds.insert(static_cast<DataRecord*>(rec)->getRowID());
            }
        }
        delete iter;
        delete query;

        EXPECT_EQ(foundIds.size(), insertedIds.size())
            << "Should find all records after reload";
        EXPECT_EQ(foundIds, insertedIds)
            << "Recovered records should match inserted records";

        std::cout << "✓ Successfully recovered " << foundIds.size()
                  << " records after reload" << std::endl;
    }
}

// Test 8: Parent NodeID Update on Reallocation
TEST_F(XTreeDurabilityUnitTest, ParentNodeIDUpdatedOnRealloc) {
    std::cout << "\n=== Test 8: Parent NodeID Update on Reallocation ===" << std::endl;

    // Create index with durable store (it will set up runtime internally)
    IndexDetails<DataRecord> idx(
        2, 32, &dim_ptrs_, nullptr, nullptr,
        "parent_realloc_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    ASSERT_TRUE(idx.hasDurableStore());
    auto* store = idx.getStore();
    ASSERT_NE(store, nullptr);

    // This test needs a simpler approach since we can't easily access private members
    // Instead, we'll test by using the existing insertion path and checking behavior

    // Initialize root
    ASSERT_TRUE(idx.ensure_root_initialized<DataRecord>());
    auto* root = idx.root_bucket<DataRecord>();
    ASSERT_NE(root, nullptr);

    // Get root's initial state
    persist::NodeID initial_root_id = root->getNodeID();
    std::cout << "Initial root NodeID: " << initial_root_id.raw() << std::endl;

    // Insert many records to force bucket growth and splits
    for (int i = 0; i < 100; ++i) {
        std::string rowId = "row_" + std::to_string(i);
        DataRecord* record = XAlloc<DataRecord>::allocate_record(&idx, 2, 32, rowId);

        // Add points to the record
        std::vector<double> point = {
            static_cast<double>(i), static_cast<double>(i)
        };
        record->putPoint(&point);
        record->putPoint(&point);  // Add twice to create an MBR

        // Insert into tree using public API
        root->xt_insert(idx.root_cache_node(), record);
    }

    std::cout << "Inserted 100 records" << std::endl;

    // Get the root after insertions (might have changed due to splits)
    root = idx.root_bucket<DataRecord>();
    persist::NodeID final_root_id = root->getNodeID();

    std::cout << "Final root NodeID: " << final_root_id.raw() << std::endl;

    // Commit to persist everything
    store->commit(1);

    // Verification: Check that the tree structure is valid
    persist::NodeKind root_kind;
    ASSERT_TRUE(store->get_node_kind(final_root_id, root_kind))
        << "Root should have valid NodeID after insertions";

#ifndef NDEBUG
    // Verify parent-child NodeID consistency using debug helper
    // This is the key test: after reallocation, parents must update their child references
    int invalid_idx = -1;
    persist::NodeID expected_id, actual_id;

    bool consistent = root->debug_verify_child_consistency(
        invalid_idx, expected_id, actual_id);

    if (!consistent) {
        std::cout << "Parent-child inconsistency detected at child index " << invalid_idx
                  << "\n  Parent expects NodeID: " << expected_id.raw()
                  << "\n  Child actually has NodeID: " << actual_id.raw() << std::endl;
    }

    ASSERT_TRUE(consistent)
        << "Parent should have updated child NodeID after reallocation"
        << " (child[" << invalid_idx << "] expected=" << expected_id.raw()
        << " actual=" << actual_id.raw() << ")";

    // Also do a full tree consistency check
    std::cout << "Running full tree consistency check..." << std::endl;
    bool tree_consistent = root->debug_verify_tree_consistency();
    ASSERT_TRUE(tree_consistent)
        << "Full tree should have consistent parent-child NodeID relationships";
#endif

    std::cout << "✓ Tree remains valid after 100 insertions with potential reallocations" << std::endl;
#ifndef NDEBUG
    std::cout << "✓ Parent-child NodeID consistency verified (debug build)" << std::endl;
    std::cout << "✓ Full tree consistency verified (debug build)" << std::endl;
#endif
}

} // namespace xtree