/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test XTree split operations with focus on NodeID assignment
 * Ensures that during splits, all nodes get valid NodeIDs that survive persistence
 */

#include <gtest/gtest.h>
#include "indexdetails.hpp"
#include "xtree.hpp"
#include "xtree_allocator_traits.hpp"
#include "datarecord.hpp"
#include "config.h"  // For XTREE_M
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <unistd.h>  // for getpid()

namespace xtree {

// Debug helper to count data slots in tree structure
// Uses a broad search query to ensure we traverse all nodes
template <class R>
static size_t count_data_slots_via_iter(XTreeBucket<R>* bkt, LRUCacheNode<IRecord, UniqueId, LRUDeleteObject>* cachedBkt) {
    if (!bkt || !cachedBkt) return 0;
    
    // Create a very broad query that will match everything
    R* broadQuery = new R(2, 32, "count_query");
    std::vector<double> min_point = {-10000.0, -10000.0};
    std::vector<double> max_point = {10000.0, 10000.0};
    broadQuery->putPoint(&min_point);
    broadQuery->putPoint(&max_point);
    
    // Use INTERSECTS with a huge box to get everything
    auto iter = bkt->getIterator(cachedBkt, broadQuery, INTERSECTS);
    size_t count = 0;
    while (iter->hasNext()) {
        auto* rec = iter->next();
        if (rec && rec->isDataNode()) {
            count++;
        }
    }
    
    delete iter;
    delete broadQuery;
    return count;
}

class XTreeSplitNodeIDTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/test_xtree_split_nodeid_" + std::to_string(getpid());
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);
        
        // Set up dimension labels
        dims_ = {"x", "y"};
        dim_ptrs_.clear();
        for (const auto& dim : dims_) {
            dim_ptrs_.push_back(dim.c_str());
        }
    }
    
    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    // Helper function to initialize root properly for all tests
    XTreeBucket<DataRecord>* initRoot(IndexDetails<DataRecord>& index) {
        // Ensure root is initialized using the production code path
        EXPECT_TRUE(index.ensure_root_initialized<DataRecord>());

        // Get cached root
        auto* cachedRoot = index.root_cache_node();
        EXPECT_NE(cachedRoot, nullptr) << "Root cache node should exist";

        // Get root bucket
        auto* root = index.root_bucket<DataRecord>();
        EXPECT_NE(root, nullptr) << "Root bucket should exist";

        // Verify root has valid NodeID in durable mode
        if (index.hasDurableStore()) {
            EXPECT_TRUE(root->hasNodeID()) << "Root should have valid NodeID in durable mode";
        }

        return root;
    }

    std::string test_dir_;
    std::vector<std::string> dims_;
    std::vector<const char*> dim_ptrs_;
};

// Helper to check if a bucket itself has a valid NodeID
void checkBucketNodeID(XTreeBucket<DataRecord>* bucket, const std::string& desc) {
    ASSERT_NE(bucket, nullptr) << desc << ": bucket is null";
    
    persist::NodeID nid = bucket->getNodeID();
    ASSERT_TRUE(nid.valid()) << desc << ": bucket NodeID is not valid";
    ASSERT_NE(nid.raw(), 0) << desc << ": bucket NodeID is 0";
}

TEST_F(XTreeSplitNodeIDTest, MinimalSplitTest) {
    // Minimal test - just try to split once
    IndexDetails<DataRecord> index(
        2, 32, &dim_ptrs_, nullptr, nullptr,
        "minimal_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );
    
    ASSERT_TRUE(index.hasDurableStore());

    // Use helper to properly initialize root
    auto* root = initRoot(index);
    auto* cachedRoot = index.root_cache_node();
    
    // Insert exactly XTREE_M records to fill the bucket
    for (int i = 0; i < XTREE_M; ++i) {
        DataRecord* dr = new DataRecord(2, 32, "rec_" + std::to_string(i));
        std::vector<double> point = {0.0, 0.0};
        dr->putPoint(&point);
        dr->putPoint(&point);
        
        root->xt_insert(cachedRoot, dr);
    }
    
    // This should trigger a split
    DataRecord* trigger = new DataRecord(2, 32, "trigger");
    std::vector<double> point = {0.0, 0.0};
    trigger->putPoint(&point);
    trigger->putPoint(&point);
    
    root->xt_insert(cachedRoot, trigger);
}

TEST_F(XTreeSplitNodeIDTest, SimpleSplitTest) {
    // Create index in DURABLE mode with data directory
    IndexDetails<DataRecord> index(
        2,  // dimensions
        32, // precision
        &dim_ptrs_,
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "split_test",  // field name
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_  // data directory for DURABLE mode
    );
    
    // Verify we have a store
    ASSERT_TRUE(index.hasDurableStore()) << "Should have durable store";
    auto* store = index.getStore();
    ASSERT_NE(store, nullptr) << "Store should be created";
    
    // Use helper to properly initialize root
    auto* root = initRoot(index);
    auto* cachedRoot = index.root_cache_node();
    
    // Insert enough records to force a split
    const int nInserts = XTREE_M + 5;
    
    for (int i = 0; i < nInserts; ++i) {
        // Create DataRecord with tightly clustered points  
        std::string recordId = "rec_" + std::to_string(i);
        DataRecord* dr = new DataRecord(2, 32, recordId);
        double x = 0.1 + i * 1e-9;
        double y = 0.1 + i * 1e-9;
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        dr->putPoint(&point);  // Add twice for point MBR
        
        
        // Get current root (may change due to splits)
        cachedRoot = index.root_cache_node();
        if (!cachedRoot) {
            std::cout << "ERROR: root_cache_node() returned null at iteration " << i << std::endl;
            break;
        }
        root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
        
        
        // Insert using xt_insert
        root->xt_insert(cachedRoot, dr);
        // Note: dr is now freed in DURABLE mode - don't access it!
        
        
    }
    
    // Check that root has valid NodeID after splits
    cachedRoot = index.root_cache_node();
    ASSERT_NE(cachedRoot, nullptr) << "Root cache node should not be null";
    root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
    
    checkBucketNodeID(root, "Post-split root");
    ASSERT_GT(root->n(), 0) << "Root should have children after split";
    
    
    // Commit to make staged inserts visible for iteration
    if (index.hasDurableStore()) {
        auto* store = index.getStore();
        ASSERT_NE(store, nullptr) << "Durable store must exist in DURABLE mode";
        store->commit(0);  // epoch 0 for now
    }
    
    // Do a simple search to verify records are findable
    DataRecord* searchQuery = new DataRecord(2, 32, "search");
    std::vector<double> min_point = {-1000.0, -1000.0};
    std::vector<double> max_point = {1000.0, 1000.0};
    searchQuery->putPoint(&min_point);
    searchQuery->putPoint(&max_point);
    
    
    
    auto iter = root->getIterator(cachedRoot, searchQuery, INTERSECTS);
    int count = 0;
    int dataCount = 0;
    std::set<std::string> foundRecords;  // Track which records we found
    
    while (iter->hasNext()) {
        auto* rec = iter->next();
        count++;  // Count all returned items
        if (rec && rec->isDataNode()) {
            dataCount++;
            auto dr = static_cast<DataRecord*>(rec);
            foundRecords.insert(dr->getRowID());
        }
    }
    
    delete iter;
    delete searchQuery;
    
    ASSERT_EQ(count, nInserts) << "Should find all inserted records";
}

TEST_F(XTreeSplitNodeIDTest, SplitTriggerTest) {
    // Create index in DURABLE mode
    IndexDetails<DataRecord> index(
        2, 32, &dim_ptrs_, nullptr, nullptr,
        "trigger_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_  // data directory
    );
    
    ASSERT_TRUE(index.hasDurableStore());
    
    // Use helper to properly initialize root
    auto* root = initRoot(index);
    auto* cachedRoot = index.root_cache_node();
    
    // Fill to exactly capacity
    for (int i = 0; i < XTREE_M; ++i) {
        DataRecord* dr = new DataRecord(2, 32, "pre_" + std::to_string(i));
        std::vector<double> point = {0.0 + i * 1e-10, 0.0 + i * 1e-10};
        dr->putPoint(&point);
        dr->putPoint(&point);
        
        cachedRoot = index.root_cache_node();
        ASSERT_NE(cachedRoot, nullptr) << "Root cache node should not be null";
        root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
        
        root->xt_insert(cachedRoot, dr);
    }
    
    // Check pre-split state - get fresh root
    cachedRoot = index.root_cache_node();
    ASSERT_NE(cachedRoot, nullptr) << "Root cache node should not be null";
    root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
    int preChildren = root->n();
    
    // The next insert MUST trigger a split
    DataRecord* trigger = new DataRecord(2, 32, "trigger");
    std::vector<double> trigger_point = {0.0, 0.0};
    trigger->putPoint(&trigger_point);
    trigger->putPoint(&trigger_point);
    
    root->xt_insert(cachedRoot, trigger);
    
    // CRITICAL: Refresh root after split (old root may be freed!)
    cachedRoot = index.root_cache_node();
    ASSERT_NE(cachedRoot, nullptr) << "Root cache node should not be null after split";
    root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
    int postChildren = root->n();
    
    // After split, we should have a different state
    if (preChildren == XTREE_M) {
        // Root was full, should now have different number of children after split
        ASSERT_NE(postChildren, preChildren)
            << "Split should have changed the number of children";
    }
    
    // Verify root has valid NodeID
    checkBucketNodeID(root, "Post-split root");
    
    // Commit to make all inserts visible
    if (index.hasDurableStore()) {
        auto* store = index.getStore();
        ASSERT_NE(store, nullptr) << "Durable store must exist in DURABLE mode";
        store->commit(0);  // epoch 0 for now
    }
    
    // Verify all records are findable
    DataRecord* searchQuery = new DataRecord(2, 32, "search");
    std::vector<double> min_point = {-1.0, -1.0};
    std::vector<double> max_point = {1.0, 1.0};
    searchQuery->putPoint(&min_point);
    searchQuery->putPoint(&max_point);
    
    auto iter = root->getIterator(cachedRoot, searchQuery, INTERSECTS);
    int count = 0;
    bool foundTrigger = false;
    while (iter->hasNext()) {
        auto* rec = iter->next();
        count++;
        if (rec && rec->isDataNode()) {
            auto dr = static_cast<DataRecord*>(rec);
            if (dr->getRowID() == "trigger") {
                foundTrigger = true;
            }
        }
    }
    delete iter;
    delete searchQuery;
    
    ASSERT_EQ(count, XTREE_M + 1) 
        << "Should find all " << (XTREE_M + 1) << " records including trigger";
    ASSERT_TRUE(foundTrigger) << "Trigger record should be findable after split";
}

} // namespace xtree