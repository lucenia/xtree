/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test that demonstrates proper root tracking after splits
 */

#include <gtest/gtest.h>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"
#include <iostream>

using namespace xtree;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteObject>;

class RootTrackingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/root_tracking_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/root_tracking_test.dat");
    }
    
    // Helper to get the current root from the index
    XTreeBucket<DataRecord>* getCurrentRoot(IndexDetails<DataRecord>* index) {
        auto rootAddress = index->getRootAddress();
        if (rootAddress == 0) return nullptr;
        
        // The root address is actually the CacheNode pointer
        CacheNode* cacheNode = (CacheNode*)rootAddress;
        if (!cacheNode || !cacheNode->object) return nullptr;
        
        return (XTreeBucket<DataRecord>*)(cacheNode->object);
    }
    
    CacheNode* getCurrentCachedRoot(IndexDetails<DataRecord>* index) {
        auto rootAddress = index->getRootAddress();
        if (rootAddress == 0) return nullptr;
        return (CacheNode*)rootAddress;
    }
};

TEST_F(RootTrackingTest, DISABLED_ProperRootTracking) {
    std::cout << "\n=== Proper Root Tracking Test ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/root_tracking_test.dat"
    );
    
    // Initial root setup
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    std::cout << "Initial root address: " << index->getRootAddress() << "\n";
    
    // Insert marker points
    std::cout << "\nInserting 3 marker points...\n";
    for (int i = 0; i < 3; i++) {
        DataRecord* dr = new DataRecord(2, 32, "marker_" + std::to_string(i));
        std::vector<double> point = {50.0 + i, 50.0 + i};
        dr->putPoint(&point);
        
        // IMPORTANT: Always get the current root before insert
        root = getCurrentRoot(index);
        cachedRoot = getCurrentCachedRoot(index);
        
        root->xt_insert(cachedRoot, dr);
    }
    
    // Verify markers
    DataRecord* searchKey = new DataRecord(2, 32, "search");
    std::vector<double> min_pt = {49.0, 49.0};
    std::vector<double> max_pt = {54.0, 54.0};
    searchKey->putPoint(&min_pt);
    searchKey->putPoint(&max_pt);
    
    // Get current root for search
    root = getCurrentRoot(index);
    cachedRoot = getCurrentCachedRoot(index);
    
    auto iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
    int count = 0;
    while (iter->hasNext()) {
        iter->next();
        count++;
    }
    delete iter;
    
    std::cout << "Found " << count << " markers initially\n";
    EXPECT_EQ(count, 3);
    
    // Now bulk insert with proper root tracking
    std::cout << "\nInserting 10,000 points with root tracking...\n";
    long rootAddressBefore = index->getRootAddress();
    
    for (int i = 0; i < 10000; i++) {
        DataRecord* dr = new DataRecord(2, 32, "pt_" + std::to_string(i));
        std::vector<double> point = {(double)(i % 100), (double)(i / 100)};
        dr->putPoint(&point);
        
        // CRITICAL: Get current root before each insert
        root = getCurrentRoot(index);
        cachedRoot = getCurrentCachedRoot(index);
        
        root->xt_insert(cachedRoot, dr);
        
        // Check if root changed
        if (index->getRootAddress() != rootAddressBefore) {
            std::cout << "Root changed at insert " << i << "!\n";
            std::cout << "  Old address: " << rootAddressBefore << "\n";
            std::cout << "  New address: " << index->getRootAddress() << "\n";
            rootAddressBefore = index->getRootAddress();
        }
        
        if (i % 1000 == 999) {
            std::cout << "  Inserted " << (i + 1) << " points\n";
        }
    }
    
    std::cout << "\nSearching for markers after bulk insert...\n";
    
    // Get current root for final search
    root = getCurrentRoot(index);
    cachedRoot = getCurrentCachedRoot(index);
    
    auto iter2 = root->getIterator(cachedRoot, searchKey, INTERSECTS);
    count = 0;
    while (iter2->hasNext()) {
        auto* result = iter2->next();
        count++;
        std::cout << "  Found: " << result->getRowID() << "\n";
    }
    delete iter2;
    delete searchKey;
    
    std::cout << "Found " << count << " markers after bulk insert\n";
    EXPECT_EQ(count, 3) << "Should still find all 3 markers!";
    
    // Verify total count
    DataRecord* searchAll = new DataRecord(2, 32, "searchAll");
    std::vector<double> min_all = {-1000.0, -1000.0};
    std::vector<double> max_all = {1000.0, 1000.0};
    searchAll->putPoint(&min_all);
    searchAll->putPoint(&max_all);
    
    auto iterAll = root->getIterator(cachedRoot, searchAll, INTERSECTS);
    int totalCount = 0;
    while (iterAll->hasNext()) {
        iterAll->next();
        totalCount++;
    }
    delete iterAll;
    delete searchAll;
    
    std::cout << "Total records found: " << totalCount << " (expected 10003)\n";
    EXPECT_EQ(totalCount, 10003);
    
    delete index;
}

TEST_F(RootTrackingTest, ConcurrentSearchWithProperRoot) {
    std::cout << "\n=== Concurrent Search with Proper Root ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/root_tracking_test.dat"
    );
    
    // Initial setup
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert test data
    std::cout << "Inserting test data...\n";
    for (int i = 0; i < 1000; i++) {
        DataRecord* dr = new DataRecord(2, 32, "pt_" + std::to_string(i));
        std::vector<double> point = {(double)(i % 50), (double)(i / 50)};
        dr->putPoint(&point);
        
        // Get current root
        root = getCurrentRoot(index);
        cachedRoot = getCurrentCachedRoot(index);
        
        root->xt_insert(cachedRoot, dr);
    }
    
    // Simulate concurrent searches - each gets fresh root reference
    std::cout << "\nSimulating concurrent searches...\n";
    
    for (int s = 0; s < 5; s++) {
        // Each search thread would get its own root reference
        auto* searchRoot = getCurrentRoot(index);
        auto* searchCachedRoot = getCurrentCachedRoot(index);
        
        DataRecord* searchKey = new DataRecord(2, 32, "search_" + std::to_string(s));
        double minX = s * 10;
        double maxX = (s + 1) * 10;
        std::vector<double> min_pt = {minX, 0};
        std::vector<double> max_pt = {maxX, 20};
        searchKey->putPoint(&min_pt);
        searchKey->putPoint(&max_pt);
        
        auto iter = searchRoot->getIterator(searchCachedRoot, searchKey, INTERSECTS);
        int count = 0;
        while (iter->hasNext()) {
            iter->next();
            count++;
        }
        delete iter;
        delete searchKey;
        
        std::cout << "  Search " << s << " [" << minX << "," << maxX << "] found " << count << " records\n";
        EXPECT_GT(count, 0);
    }
    
    delete index;
}