/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Simple test to reproduce the 10K point search failure
 */

#include <gtest/gtest.h>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"
#include <iostream>

using namespace xtree;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class TenKIssueTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/10k_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/10k_test.dat");
    }
};

TEST_F(TenKIssueTest, DISABLED_SimpleReproduction) {
    std::cout << "\n=== 10K Point Issue Reproduction ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/10k_test.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert 3 marker points
    std::cout << "Inserting 3 marker points...\n";
    for (int i = 0; i < 3; i++) {
        DataRecord* dr = new DataRecord(2, 32, "marker_" + std::to_string(i));
        std::vector<double> point = {50.0 + i, 50.0 + i};
        dr->putPoint(&point);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Verify we can find them
    std::cout << "Searching for markers before bulk insert...\n";
    DataRecord* searchKey = new DataRecord(2, 32, "search");
    std::vector<double> min_pt = {49.0, 49.0};
    std::vector<double> max_pt = {54.0, 54.0};
    searchKey->putPoint(&min_pt);
    searchKey->putPoint(&max_pt);
    
    auto iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
    int count = 0;
    while (iter->hasNext()) {
        auto* result = iter->next();
        count++;
        std::cout << "  Found: " << result->getRowID() << "\n";
    }
    delete iter;
    
    std::cout << "Found " << count << " markers (expected 3)\n";
    EXPECT_EQ(count, 3);
    
    // Now insert exactly 10,000 points
    std::cout << "\nInserting 10,000 points...\n";
    for (int i = 0; i < 10000; i++) {
        DataRecord* dr = new DataRecord(2, 32, "pt_" + std::to_string(i));
        std::vector<double> point = {(double)(i % 100), (double)(i / 100)};
        dr->putPoint(&point);
        root->xt_insert(cachedRoot, dr);
        
        if (i % 1000 == 999) {
            std::cout << "  Inserted " << (i + 1) << " points\n";
        }
    }
    
    std::cout << "\nSearching for markers after bulk insert...\n";
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
    
    if (count != 3) {
        std::cout << "\nDEBUG INFO:\n";
        std::cout << "Root has " << root->n() << " entries\n";
        
        // Try a different search
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
        
        std::cout << "Search all found " << totalCount << " total records\n";
        
        // Check memory usage
        auto* compact_alloc = index->getCompactAllocator();
        std::cout << "Memory used: " << (compact_alloc->get_snapshot_manager()->get_snapshot_size() / (1024.0 * 1024.0)) << " MB\n";
    }
    
    EXPECT_EQ(count, 3) << "Lost marker points after bulk insert!";
    
    delete index;
}

TEST_F(TenKIssueTest, IncrementalTest) {
    std::cout << "\n=== Incremental Search Test ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/10k_test.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Test at different insertion counts
    std::vector<int> testPoints = {100, 500, 1000, 2000, 5000, 8000, 10000, 12000};
    
    int totalInserted = 0;
    for (int targetCount : testPoints) {
        // Insert up to target
        while (totalInserted < targetCount) {
            DataRecord* dr = new DataRecord(2, 32, "pt_" + std::to_string(totalInserted));
            std::vector<double> point = {(double)(totalInserted % 100), (double)(totalInserted / 100)};
            dr->putPoint(&point);
            root->xt_insert(cachedRoot, dr);
            totalInserted++;
        }
        
        // Test search
        DataRecord* searchKey = new DataRecord(2, 32, "search");
        std::vector<double> min_pt = {0.0, 0.0};
        std::vector<double> max_pt = {99.0, 99.0};
        searchKey->putPoint(&min_pt);
        searchKey->putPoint(&max_pt);
        
        auto iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
        int count = 0;
        while (iter->hasNext()) {
            iter->next();
            count++;
        }
        delete iter;
        delete searchKey;
        
        std::cout << "After " << totalInserted << " inserts: search found " << count << " records\n";
        
        if (count == 0 && totalInserted > 0) {
            std::cout << "  WARNING: Search returned 0 results!\n";
            break;
        }
    }
    
    delete index;
}