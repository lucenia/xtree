/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Analyze tree depth and structure
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include <queue>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class TreeDepthAnalysis : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/depth_analysis.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/depth_analysis.dat");
    }
};

TEST_F(TreeDepthAnalysis, AnalyzeTreeDepth) {
    std::cout << "\n=== Tree Depth and Node Count Analysis ===\n";
    
    // Build trees with different data patterns
    struct TestCase {
        std::string name;
        bool useGrid;
        int numPoints;
    };
    
    std::vector<TestCase> testCases = {
        {"Grid 1K", true, 1000},
        {"Random 1K", false, 1000},
        {"Grid 10K", true, 10000},
        {"Random 10K", false, 10000},
    };
    
    for (const auto& test : testCases) {
        std::cout << "\n--- " << test.name << " ---\n";
        
        std::vector<const char*> dimLabels = {"x", "y"};
        auto* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
        
        auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress((long)cachedRoot);
        
        // Insert points
        std::mt19937 gen(42);
        std::uniform_real_distribution<> dist(0, 100);
        
        if (test.useGrid) {
            int gridSize = (int)sqrt(test.numPoints);
            double step = 100.0 / gridSize;
            int id = 0;
            
            for (int x = 0; x < gridSize; x++) {
                for (int y = 0; y < gridSize; y++) {
                    DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                        "pt_" + std::to_string(id++));
                    std::vector<double> point = {x * step, y * step};
                    dr->putPoint(&point);
                    
                    root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
                    cachedRoot = (CacheNode*)index->getRootAddress();
                    root->xt_insert(cachedRoot, dr);
                }
            }
        } else {
            for (int i = 0; i < test.numPoints; i++) {
                DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                    "pt_" + std::to_string(i));
                std::vector<double> point = {dist(gen), dist(gen)};
                dr->putPoint(&point);
                
                root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
                cachedRoot = (CacheNode*)index->getRootAddress();
                root->xt_insert(cachedRoot, dr);
            }
        }
        
        // Analyze final tree structure
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        std::cout << "Root entries: " << root->n() << "\n";
        
        // Measure query performance with breakdown
        int numQueries = 1000;
        double totalQueryTime = 0;
        double totalIterTime = 0;
        double totalNextTime = 0;
        int totalResults = 0;
        
        for (int q = 0; q < numQueries; q++) {
            double qx = dist(gen);
            double qy = dist(gen);
            if (qx > 90) qx = 90;
            if (qy > 90) qy = 90;
            
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {qx, qy};
            std::vector<double> max_pt = {qx + 10, qy + 10};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            auto queryStart = high_resolution_clock::now();
            
            // Time iterator creation
            auto iterStart = high_resolution_clock::now();
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            auto iterTime = duration_cast<nanoseconds>(high_resolution_clock::now() - iterStart).count();
            
            // Time result retrieval
            auto nextStart = high_resolution_clock::now();
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            auto nextTime = duration_cast<nanoseconds>(high_resolution_clock::now() - nextStart).count();
            
            delete iter;
            
            auto queryTime = duration_cast<nanoseconds>(high_resolution_clock::now() - queryStart).count();
            
            totalQueryTime += queryTime;
            totalIterTime += iterTime;
            totalNextTime += nextTime;
        }
        
        std::cout << "Query performance breakdown (avg per query):\n";
        std::cout << "  Total time: " << std::fixed << std::setprecision(3) 
                  << (totalQueryTime / numQueries / 1000.0) << " μs\n";
        std::cout << "  Iterator creation: " << (totalIterTime / numQueries / 1000.0) << " μs\n";
        std::cout << "  Result retrieval: " << (totalNextTime / numQueries / 1000.0) << " μs\n";
        std::cout << "  Avg results per query: " << (totalResults / (double)numQueries) << "\n";
        
        double qps = numQueries / (totalQueryTime / 1000000000.0);
        std::cout << "  QPS: " << std::setprecision(0) << qps << "\n";
        
        delete index;
    }
    
    // Test supernode behavior
    std::cout << "\n\n=== Supernode Behavior Test ===\n";
    
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert clustered points to force overlaps
    std::cout << "Inserting clustered points to test supernode creation...\n";
    
    // Insert 100 points in a small cluster
    for (int i = 0; i < 100; i++) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
            "cluster_" + std::to_string(i));
        // All points in [45-55, 45-55] range
        std::vector<double> point = {45.0 + (i % 10), 45.0 + (i / 10)};
        dr->putPoint(&point);
        
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        int entriesBefore = root->n();
        root->xt_insert(cachedRoot, dr);
        
        auto* newRoot = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        if (newRoot->n() > 50 && i > 50) {
            std::cout << "Supernode created? Root has " << newRoot->n() 
                      << " entries after insert " << i << "\n";
        }
    }
    
    delete index;
}