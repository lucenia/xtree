/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Debug benchmark to analyze low QPS issue
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

class QPSDebugBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/qps_debug.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/qps_debug.dat");
    }
};

TEST_F(QPSDebugBenchmark, CompareQueryPatterns) {
    std::cout << "\n=== QPS Debug Benchmark ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert 100K points in grid pattern (same as ParallelSIMDBenchmark)
    std::cout << "Inserting 100K points in grid pattern...\n";
    const int GRID_SIZE = 316;  // ~100K points
    
    for (int x = 0; x < GRID_SIZE; x++) {
        for (int y = 0; y < GRID_SIZE; y++) {
            DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                "grid_" + std::to_string(x) + "_" + std::to_string(y));
            std::vector<double> point = {(double)x, (double)y};
            dr->putPoint(&point);
            
            // Update root before insert
            root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
            cachedRoot = (CacheNode*)index->getRootAddress();
            
            root->xt_insert(cachedRoot, dr);
        }
    }
    
    std::cout << "Inserted " << (GRID_SIZE * GRID_SIZE) << " points\n\n";
    
    // Get final root
    auto rootAddress = index->getRootAddress();
    CacheNode* cacheNode = (CacheNode*)rootAddress;
    XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
    
    // Test different query patterns
    const int NUM_QUERIES = 10000;
    std::mt19937 gen(42);
    
    // Test 1: Small box queries (10x10) - same as ParallelSIMDBenchmark
    {
        std::cout << "Test 1: Small box queries (10x10)\n";
        std::uniform_real_distribution<> dis(0, GRID_SIZE - 10);
        
        auto start = high_resolution_clock::now();
        int totalResults = 0;
        
        for (int i = 0; i < NUM_QUERIES; i++) {
            double x = dis(gen);
            double y = dis(gen);
            
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {x, y};
            std::vector<double> max_pt = {x + 10, y + 10};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            delete iter;
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        double qps = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Avg results: " << (totalResults / (double)NUM_QUERIES) << "\n\n";
    }
    
    // Test 2: Point queries (0.01 x 0.01) - similar to RealWorldScenario neighborhood
    {
        std::cout << "Test 2: Point queries (0.01 x 0.01)\n";
        std::uniform_real_distribution<> dis(0, GRID_SIZE - 0.01);
        
        auto start = high_resolution_clock::now();
        int totalResults = 0;
        
        for (int i = 0; i < NUM_QUERIES; i++) {
            double x = dis(gen);
            double y = dis(gen);
            
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {x, y};
            std::vector<double> max_pt = {x + 0.01, y + 0.01};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            delete iter;
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        double qps = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Avg results: " << (totalResults / (double)NUM_QUERIES) << "\n\n";
    }
    
    // Test 3: Random data distribution (like RealWorldScenario)
    {
        std::cout << "Test 3: Creating new index with random distribution...\n";
        
        // Create new index with random data
        auto* index2 = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
        
        auto* root2 = XAlloc<DataRecord>::allocate_bucket(index2, true);
        auto* cachedRoot2 = index2->getCache().add(index2->getNextNodeID(), root2);
        index2->setRootAddress((long)cachedRoot2);
        
        // Insert 100K random points
        std::uniform_real_distribution<> x_dist(0, 316);
        std::uniform_real_distribution<> y_dist(0, 316);
        
        for (int i = 0; i < 100000; i++) {
            DataRecord* dr = XAlloc<DataRecord>::allocate_record(index2, 2, 32, 
                "random_" + std::to_string(i));
            std::vector<double> point = {x_dist(gen), y_dist(gen)};
            dr->putPoint(&point);
            
            root2 = (XTreeBucket<DataRecord>*)((CacheNode*)index2->getRootAddress())->object;
            cachedRoot2 = (CacheNode*)index2->getRootAddress();
            
            root2->xt_insert(cachedRoot2, dr);
        }
        
        // Get final root
        auto rootAddress2 = index2->getRootAddress();
        CacheNode* cacheNode2 = (CacheNode*)rootAddress2;
        XTreeBucket<DataRecord>* currentRoot2 = (XTreeBucket<DataRecord>*)(cacheNode2->object);
        
        std::cout << "Testing queries on random distribution...\n";
        std::uniform_real_distribution<> dis(0, 306);
        
        auto start = high_resolution_clock::now();
        int totalResults = 0;
        
        for (int i = 0; i < NUM_QUERIES; i++) {
            double x = dis(gen);
            double y = dis(gen);
            
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index2, 2, 32, "query");
            std::vector<double> min_pt = {x, y};
            std::vector<double> max_pt = {x + 10, y + 10};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            auto iter = currentRoot2->getIterator(cacheNode2, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            delete iter;
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        double qps = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Avg results: " << (totalResults / (double)NUM_QUERIES) << "\n\n";
        
        delete index2;
    }
    
    delete index;
}