/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Analyze bounding box overlap in tree
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class OverlapAnalysis : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/overlap_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/overlap_test.dat");
    }
};

TEST_F(OverlapAnalysis, MeasureQueryOverlap) {
    std::cout << "\n=== Query Overlap Analysis ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert 1000 random points
    std::cout << "Inserting 1000 random points...\n";
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dist(0, 100);
    
    for (int i = 0; i < 1000; i++) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
            "pt_" + std::to_string(i));
        std::vector<double> point = {dist(gen), dist(gen)};
        dr->putPoint(&point);
        
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        root->xt_insert(cachedRoot, dr);
    }
    
    root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
    cachedRoot = (CacheNode*)index->getRootAddress();
    
    std::cout << "Tree built with root having " << root->n() << " entries\n\n";
    
    // Test queries of different sizes
    double querySizes[] = {1.0, 5.0, 10.0, 20.0};
    
    for (double qSize : querySizes) {
        std::cout << "Query size " << qSize << "x" << qSize << ":\n";
        
        // Test at different locations
        double locations[][2] = {{10, 10}, {50, 50}, {90, 90}};
        
        for (auto& loc : locations) {
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {loc[0] - qSize/2, loc[1] - qSize/2};
            std::vector<double> max_pt = {loc[0] + qSize/2, loc[1] + qSize/2};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            int count = 0;
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                count++;
            }
            delete iter;
            
            std::cout << "  At (" << loc[0] << "," << loc[1] << "): " << count << " results\n";
        }
    }
    
    // Now test a specific problematic query
    std::cout << "\nTesting specific query [45,45] to [55,55]:\n";
    DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
    std::vector<double> min_pt = {45.0, 45.0};
    std::vector<double> max_pt = {55.0, 55.0};
    query->putPoint(&min_pt);
    query->putPoint(&max_pt);
    
    int count = 0;
    std::set<std::string> foundIds;
    auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
    while (iter->hasNext()) {
        auto* result = iter->next();
        foundIds.insert(result->getRowID());
        count++;
        
        // Print first few results to debug
        if (count <= 5) {
            std::cout << "  Found: " << result->getRowID() << "\n";
        }
    }
    delete iter;
    
    std::cout << "Total results: " << count << "\n";
    std::cout << "Unique IDs: " << foundIds.size() << "\n";
    
    // Check for duplicates
    if (count != foundIds.size()) {
        std::cout << "WARNING: Found " << (count - foundIds.size()) << " duplicate results!\n";
    }
    
    delete index;
}