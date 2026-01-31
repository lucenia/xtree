/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Simple test to verify XTree works with concurrent allocator
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

class SimpleXTreeConcurrentTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up
        std::remove("/tmp/simple_xtree_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/simple_xtree_test.dat");
    }
};

TEST_F(SimpleXTreeConcurrentTest, BasicInsertAndSearch) {
    std::cout << "\n=== Simple XTree Concurrent Test ===\n";
    
    // Create index with MMAP persistence
    std::vector<const char*> dimLabels = {"x", "y"};
    
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/simple_xtree_test.dat"
    );
    
    // Get allocator info
    auto* compact_alloc = index->getCompactAllocator();
    ASSERT_NE(compact_alloc, nullptr);
    
    // Create root
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    std::cout << "Inserting records...\n";
    
    // Insert some records
    for (int i = 0; i < 100; i++) {
        DataRecord* record = new DataRecord(2, 32, "rec_" + std::to_string(i));
        std::vector<double> min_pt = {(double)i, (double)i};
        std::vector<double> max_pt = {(double)i + 0.5, (double)i + 0.5};
        record->putPoint(&min_pt);
        record->putPoint(&max_pt);
        root->xt_insert(cachedRoot, record);
    }
    
    std::cout << "Inserted 100 records\n";
    std::cout << "Memory used: " << (compact_alloc->get_snapshot_manager()->get_snapshot_size() / 1024.0) << " KB\n";
    
    // Get the current root (it may have changed due to splits)
    cachedRoot = reinterpret_cast<CacheNode*>(index->getRootAddress());
    root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
    
    // Search for records
    DataRecord* searchKey = new DataRecord(2, 32, "search");
    std::vector<double> min_point = {10, 10};
    std::vector<double> max_point = {50, 50};
    searchKey->putPoint(&min_point);
    searchKey->putPoint(&max_point);
    
    auto iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
    
    int count = 0;
    while (iter->hasNext()) {
        auto* result = iter->next();
        count++;
    }
    
    std::cout << "Search found " << count << " records\n";
    EXPECT_GT(count, 0);
    
    delete iter;
    delete searchKey;
    delete index;
}

TEST_F(SimpleXTreeConcurrentTest, SegmentedAllocation) {
    std::cout << "\n=== Segmented Allocation Test ===\n";
    
    std::vector<const char*> dimLabels = {"x", "y"};
    
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/simple_xtree_test.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert just a few records first to verify basic functionality
    std::cout << "Inserting initial test records...\n";
    
    // Insert specific test records
    std::vector<std::pair<int, std::pair<double, double>>> testRecords = {
        {1, {10.0, 10.0}},
        {2, {20.0, 20.0}},
        {3, {15.0, 15.0}},
        {4, {25.0, 25.0}},
        {5, {30.0, 30.0}}
    };
    
    for (const auto& tr : testRecords) {
        DataRecord* record = new DataRecord(2, 32, "rec_" + std::to_string(tr.first));
        std::vector<double> min_pt = {tr.second.first, tr.second.second};
        std::vector<double> max_pt = {tr.second.first + 1.0, tr.second.second + 1.0};
        record->putPoint(&min_pt);
        record->putPoint(&max_pt);
        root->xt_insert(cachedRoot, record);
        std::cout << "  Inserted record " << tr.first << " at bbox [(" << tr.second.first << ", " << tr.second.second << ") to (" << (tr.second.first + 1.0) << ", " << (tr.second.second + 1.0) << ")]\n";
    }
    
    // Now do a simple range search
    std::cout << "\nSearching for records in range: [10,10] to [25,25]\n";
    DataRecord* searchKey = new DataRecord(2, 32, "search");
    std::vector<double> min_point = {10, 10};
    std::vector<double> max_point = {25, 25};
    searchKey->putPoint(&min_point);
    searchKey->putPoint(&max_point);
    
    auto iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
    
    int count = 0;
    while (iter->hasNext()) {
        auto* result = iter->next();
        count++;
        std::cout << "  Found: " << result->getRowID() << "\n";
    }
    
    std::cout << "Search found " << count << " records\n";
    EXPECT_GE(count, 3);  // Should find at least records 1, 2, 3
    
    delete iter;
    delete searchKey;
    
    // Now insert many more records to test segmented allocation
    const int NUM_RECORDS = 10000;
    std::cout << "\nInserting " << NUM_RECORDS << " records for segmented test...\n";
    
    for (int i = 0; i < NUM_RECORDS; i++) {
        DataRecord* record = new DataRecord(2, 32, "rec_" + std::to_string(i + 100));
        double x = (double)(i % 100);
        double y = (double)(i / 100);
        std::vector<double> min_pt = {x, y};
        std::vector<double> max_pt = {x + 0.1, y + 0.1};  // Small bounding box
        record->putPoint(&min_pt);
        record->putPoint(&max_pt);
        root->xt_insert(cachedRoot, record);
        
        if (i % 1000 == 0) {
            std::cout << "  Inserted " << i << " records\n";
        }
    }
    
    auto* compact_alloc = index->getCompactAllocator();
    std::cout << "Total memory used: " << (compact_alloc->get_snapshot_manager()->get_snapshot_size() / (1024.0 * 1024.0)) << " MB\n";
    
    // Get the current root after all inserts (it may have changed due to splits)
    cachedRoot = reinterpret_cast<CacheNode*>(index->getRootAddress());
    root = reinterpret_cast<XTreeBucket<DataRecord>*>(cachedRoot->object);
    std::cout << "Root has " << root->n() << " entries\n";
    
    // First verify we can find a specific point
    std::cout << "\nTesting exact point lookup for record at (50, 50)\n";
    DataRecord* exactKey2 = new DataRecord(2, 32, "exact2");
    std::vector<double> exact_point2 = {50, 50};
    exactKey2->putPoint(&exact_point2);
    
    auto exactIter2 = root->getIterator(cachedRoot, exactKey2, CONTAINS);
    int exactCount2 = 0;
    while (exactIter2->hasNext()) {
        auto* result = exactIter2->next();
        exactCount2++;
        std::cout << "  Found exact match: " << result->getRowID() << "\n";
    }
    std::cout << "Exact search found " << exactCount2 << " records\n";
    delete exactIter2;
    delete exactKey2;
    
    // Search again after bulk insert
    std::cout << "\nSearching again after bulk insert for range: [40,40] to [60,60]\n";
    std::cout << "Expected to find records like:\n";
    std::cout << "  rec_4140 (x=40, y=41), rec_4141 (x=41, y=41), etc.\n";
    std::cout << "  rec_5040 (x=40, y=50), rec_5041 (x=41, y=50), etc.\n";
    
    DataRecord* searchKey2 = new DataRecord(2, 32, "search2");
    std::vector<double> min_point2 = {40, 40};
    std::vector<double> max_point2 = {60, 60};
    searchKey2->putPoint(&min_point2);
    searchKey2->putPoint(&max_point2);
    
    auto iter2 = root->getIterator(cachedRoot, searchKey2, INTERSECTS);
    
    int count2 = 0;
    while (iter2->hasNext()) {
        auto* result = iter2->next();
        count2++;
        if (count2 <= 5) {
            std::cout << "  Found: " << result->getRowID() << "\n";
        }
    }
    
    std::cout << "Search found " << count2 << " records\n";
    EXPECT_GT(count2, 0);
    
    delete iter2;
    delete searchKey2;
    delete index;
}