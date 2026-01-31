/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test that verifies XTree works correctly with CompactAllocator
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

class XTreeAllocatorIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up
        std::remove("/tmp/xtree_allocator_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/xtree_allocator_test.dat");
    }
    
    DataRecord* createDataRecord(const std::string& id, double x1, double y1, double x2, double y2) {
        DataRecord* dr = new DataRecord(2, 32, id);
        std::vector<double> min_pt = {x1, y1};
        std::vector<double> max_pt = {x2, y2};
        dr->putPoint(&min_pt);
        dr->putPoint(&max_pt);
        return dr;
    }
};

TEST_F(XTreeAllocatorIntegrationTest, BasicOperationsWithCompactAllocator) {
    std::cout << "\n=== XTree Allocator Integration Test ===\n";
    
    // Create index with MMAP persistence
    std::vector<const char*> dimLabels = {"x", "y"};
    
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/xtree_allocator_test.dat"
    );
    
    // Verify allocator is set up
    auto* compact_alloc = index->getCompactAllocator();
    ASSERT_NE(compact_alloc, nullptr);
    
    // Create root
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert some test records with known locations
    std::cout << "Inserting test records...\n";
    
    std::vector<std::tuple<std::string, double, double, double, double>> testData = {
        {"A", 0, 0, 10, 10},
        {"B", 5, 5, 15, 15},
        {"C", 20, 20, 30, 30},
        {"D", 25, 0, 35, 10},
        {"E", 0, 25, 10, 35}
    };
    
    for (const auto& data : testData) {
        DataRecord* dr = createDataRecord(
            std::get<0>(data),
            std::get<1>(data), std::get<2>(data),
            std::get<3>(data), std::get<4>(data)
        );
        root->xt_insert(cachedRoot, dr);
        std::cout << "  Inserted " << std::get<0>(data) << " at [" 
                  << std::get<1>(data) << "," << std::get<2>(data) << " - "
                  << std::get<3>(data) << "," << std::get<4>(data) << "]\n";
    }
    
    std::cout << "Root has " << root->n() << " entries\n";
    std::cout << "Memory used: " << (compact_alloc->get_snapshot_manager()->get_snapshot_size() / 1024.0) << " KB\n";
    
    // Test 1: Search that should find A and B
    std::cout << "\nTest 1: Search [5,5] to [10,10] (should find A, B)\n";
    DataRecord* search1 = createDataRecord("search1", 5, 5, 10, 10);
    auto iter1 = root->getIterator(cachedRoot, search1, INTERSECTS);
    
    std::set<std::string> found1;
    while (iter1->hasNext()) {
        DataRecord* result = iter1->next();
        found1.insert(result->getRowID());
        std::cout << "  Found: " << result->getRowID() << "\n";
    }
    delete iter1;
    delete search1;
    
    EXPECT_TRUE(found1.count("A") > 0);
    EXPECT_TRUE(found1.count("B") > 0);
    EXPECT_EQ(found1.size(), 2);
    
    // Test 2: Search that should find only C
    std::cout << "\nTest 2: Search [22,22] to [28,28] (should find C)\n";
    DataRecord* search2 = createDataRecord("search2", 22, 22, 28, 28);
    auto iter2 = root->getIterator(cachedRoot, search2, INTERSECTS);
    
    std::set<std::string> found2;
    while (iter2->hasNext()) {
        DataRecord* result = iter2->next();
        found2.insert(result->getRowID());
        std::cout << "  Found: " << result->getRowID() << "\n";
    }
    delete iter2;
    delete search2;
    
    EXPECT_TRUE(found2.count("C") > 0);
    EXPECT_EQ(found2.size(), 1);
    
    // Test 3: Search all
    std::cout << "\nTest 3: Search [-100,-100] to [100,100] (should find all)\n";
    DataRecord* searchAll = createDataRecord("searchAll", -100, -100, 100, 100);
    auto iterAll = root->getIterator(cachedRoot, searchAll, INTERSECTS);
    
    std::set<std::string> foundAll;
    while (iterAll->hasNext()) {
        DataRecord* result = iterAll->next();
        foundAll.insert(result->getRowID());
        std::cout << "  Found: " << result->getRowID() << "\n";
    }
    delete iterAll;
    delete searchAll;
    
    EXPECT_EQ(foundAll.size(), 5);
    
    // Now insert many more records to test segmented allocation
    std::cout << "\nInserting 1000 more records...\n";
    for (int i = 0; i < 1000; i++) {
        double x = (i % 50) * 2;
        double y = (i / 50) * 2;
        DataRecord* dr = createDataRecord(
            "bulk_" + std::to_string(i),
            x, y, x + 1, y + 1
        );
        root->xt_insert(cachedRoot, dr);
        
        if (i % 100 == 0) {
            std::cout << "  Inserted " << i << " records\n";
        }
    }
    
    std::cout << "Total memory used: " << (compact_alloc->get_snapshot_manager()->get_snapshot_size() / (1024.0 * 1024.0)) << " MB\n";
    
    // Verify we can still find the original records
    std::cout << "\nVerifying original records are still findable...\n";
    DataRecord* verifySearch = createDataRecord("verify", 5, 5, 10, 10);
    auto verifyIter = root->getIterator(cachedRoot, verifySearch, INTERSECTS);
    
    bool foundA = false, foundB = false;
    while (verifyIter->hasNext()) {
        DataRecord* result = verifyIter->next();
        if (result->getRowID() == "A") foundA = true;
        if (result->getRowID() == "B") foundB = true;
    }
    delete verifyIter;
    delete verifySearch;
    
    EXPECT_TRUE(foundA);
    EXPECT_TRUE(foundB);
    
    std::cout << "\nAll tests passed!\n";
    
    delete index;
}