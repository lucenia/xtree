/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Debug benchmark to analyze tree structure issues
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class TreeStructureDebug : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/tree_debug.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/tree_debug.dat");
    }
    
    void analyzeTreeStructure(XTreeBucket<DataRecord>* node, CacheNode* cacheNode, int depth = 0) {
        if (!node) return;
        
        // Just analyze what we can access
        if (depth < 3) {  // Only print first few levels
            std::cout << std::string(depth * 2, ' ') << "Node at depth " << depth 
                      << ": " << node->n() << " entries\n";
        }
    }
};

TEST_F(TreeStructureDebug, AnalyzeTreeStructure) {
    std::cout << "\n=== Tree Structure Analysis ===\n";
    
    // Test 1: Grid pattern tree
    {
        std::cout << "\nTest 1: Grid Pattern Tree\n";
        std::vector<const char*> dimLabels = {"x", "y"};
        auto* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
        
        auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress((long)cachedRoot);
        
        // Insert 10K points in grid
        const int GRID_SIZE = 100;
        for (int x = 0; x < GRID_SIZE; x++) {
            for (int y = 0; y < GRID_SIZE; y++) {
                DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                    "grid_" + std::to_string(x) + "_" + std::to_string(y));
                std::vector<double> point = {(double)x, (double)y};
                dr->putPoint(&point);
                
                root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
                cachedRoot = (CacheNode*)index->getRootAddress();
                
                root->xt_insert(cachedRoot, dr);
            }
        }
        
        // Analyze final structure
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        std::cout << "Root node has " << root->n() << " entries\n";
        // Can't access isLeaf() - it's protected
        
        // Test a few queries
        std::cout << "\nTesting queries on grid tree:\n";
        auto start = high_resolution_clock::now();
        int results = 0;
        
        for (int i = 0; i < 1000; i++) {
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {45.0, 45.0};
            std::vector<double> max_pt = {55.0, 55.0};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                results++;
            }
            delete iter;
        }
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start);
        std::cout << "1000 queries found " << results << " results in " << duration.count()/1000.0 << " ms\n";
        
        delete index;
    }
    
    // Test 2: Random pattern tree
    {
        std::cout << "\nTest 2: Random Pattern Tree\n";
        std::vector<const char*> dimLabels = {"x", "y"};
        auto* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
        
        auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress((long)cachedRoot);
        
        // Insert 10K random points
        std::mt19937 gen(42);
        std::uniform_real_distribution<> dist(0, 100);
        
        for (int i = 0; i < 10000; i++) {
            DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                "random_" + std::to_string(i));
            std::vector<double> point = {dist(gen), dist(gen)};
            dr->putPoint(&point);
            
            root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
            cachedRoot = (CacheNode*)index->getRootAddress();
            
            root->xt_insert(cachedRoot, dr);
            
            // Check if root changed at key points
            if (i == 999 || i == 4999 || i == 9999) {
                std::cout << "After " << (i+1) << " inserts: Root has " << root->n() 
                          << " entries\n";
            }
        }
        
        // Analyze final structure
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        std::cout << "Final root node has " << root->n() << " entries\n";
        
        // Test same queries
        std::cout << "\nTesting queries on random tree:\n";
        
        // First check a single query to see how many results
        DataRecord* testQuery = XAlloc<DataRecord>::allocate_record(index, 2, 32, "test_query");
        std::vector<double> test_min = {45.0, 45.0};
        std::vector<double> test_max = {55.0, 55.0};
        testQuery->putPoint(&test_min);
        testQuery->putPoint(&test_max);
        
        int singleQueryResults = 0;
        auto testIter = root->getIterator(cachedRoot, testQuery, INTERSECTS);
        while (testIter->hasNext()) {
            testIter->next();
            singleQueryResults++;
        }
        delete testIter;
        
        std::cout << "Single query [45,45] to [55,55] returns: " << singleQueryResults << " results\n";
        
        // Now time 1000 queries
        auto start = high_resolution_clock::now();
        int results = 0;
        
        for (int i = 0; i < 1000; i++) {
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {45.0, 45.0};
            std::vector<double> max_pt = {55.0, 55.0};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                results++;
            }
            delete iter;
        }
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start);
        std::cout << "1000 queries total " << results << " results in " << duration.count()/1000.0 << " ms\n";
        std::cout << "Average time per query: " << duration.count()/1000.0/1000 << " ms\n";
        
        delete index;
    }
    
    // Test 3: Check tree after specific number of inserts
    {
        std::cout << "\nTest 3: Checking tree growth\n";
        std::vector<const char*> dimLabels = {"x", "y"};
        auto* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
        
        auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress((long)cachedRoot);
        
        std::mt19937 gen(42);
        std::uniform_real_distribution<> dist(0, 100);
        
        int checkpoints[] = {10, 50, 100, 500, 1000, 5000, 10000};
        int inserted = 0;
        
        for (int checkpoint : checkpoints) {
            // Insert up to checkpoint
            while (inserted < checkpoint) {
                DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                    "pt_" + std::to_string(inserted));
                std::vector<double> point = {dist(gen), dist(gen)};
                dr->putPoint(&point);
                
                root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
                cachedRoot = (CacheNode*)index->getRootAddress();
                
                root->xt_insert(cachedRoot, dr);
                inserted++;
            }
            
            root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
            std::cout << "After " << checkpoint << " inserts: "
                      << "Root entries=" << root->n() << "\n";
        }
        
        delete index;
    }
}