/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Analyze iterator performance bottlenecks and test optimizations
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include <atomic>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/config.h"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class IteratorOptimizationAnalysis : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/iter_opt.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/iter_opt.dat");
    }
};

TEST_F(IteratorOptimizationAnalysis, AnalyzeIteratorPerformance) {
    std::cout << "\n=== Iterator Performance Analysis ===\n";
    std::cout << "XTREE_M: " << XTREE_M << "\n";
    std::cout << "XTREE_ITER_PAGE_SIZE: " << XTREE_ITER_PAGE_SIZE << "\n\n";
    
    // Test with different node capacities
    struct TestCase {
        std::string name;
        int numPoints;
        bool useGrid;
    };
    
    std::vector<TestCase> testCases = {
        {"Grid 10K", 10000, true},
        {"Random 10K", 10000, false},
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
        
        // Get final root
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        std::cout << "Root entries: " << root->n() << "\n";
        
        // Analyze iterator creation with different query sizes
        struct QueryTest {
            std::string name;
            double boxSize;
            int expectedResults;
        };
        
        std::vector<QueryTest> queryTests = {
            {"Small query (10x10)", 10.0, 100},
            {"Tiny query (1x1)", 1.0, 1},
            {"Large query (50x50)", 50.0, 2500},
        };
        
        for (const auto& qtest : queryTests) {
            std::cout << "\n  " << qtest.name << ":\n";
            
            // Create query box
            DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> min_pt = {45.0, 45.0};
            std::vector<double> max_pt = {45.0 + qtest.boxSize, 45.0 + qtest.boxSize};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            // Time just iterator creation
            const int numIterCreations = 1000;
            auto startCreate = high_resolution_clock::now();
            
            for (int i = 0; i < numIterCreations; i++) {
                auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
                delete iter;
            }
            
            auto createDuration = duration_cast<microseconds>(high_resolution_clock::now() - startCreate);
            double avgCreateTime = createDuration.count() / (double)numIterCreations;
            
            std::cout << "    Avg iterator creation time: " << std::fixed << std::setprecision(3) 
                      << avgCreateTime << " μs\n";
            
            // Now analyze what happens during a full query
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            int firstBatchSize = 0;
            int totalResults = 0;
            
            // Count results in first batch
            while (iter->hasNext() && firstBatchSize < XTREE_ITER_PAGE_SIZE) {
                iter->next();
                firstBatchSize++;
                totalResults++;
            }
            
            // Count remaining results
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            
            delete iter;
            
            std::cout << "    First batch size: " << firstBatchSize << " (max " << XTREE_ITER_PAGE_SIZE << ")\n";
            std::cout << "    Total results: " << totalResults << "\n";
            
            // Expected results
            double expectedForUniform = (qtest.boxSize * qtest.boxSize) / (100.0 * 100.0) * test.numPoints;
            std::cout << "    Expected (uniform): " << std::setprecision(1) << expectedForUniform << "\n";
        }
        
        delete index;
    }
    
    // Test the impact of XTREE_ITER_PAGE_SIZE
    std::cout << "\n\n=== Impact of Iterator Page Size ===\n";
    std::cout << "Current XTREE_ITER_PAGE_SIZE: " << XTREE_ITER_PAGE_SIZE << "\n";
    std::cout << "This controls how many results are fetched during iterator creation\n";
    std::cout << "A smaller value would make iterator creation faster but require more\n";
    std::cout << "subsequent fetches. The optimal value depends on typical query result sizes.\n";
}

TEST_F(IteratorOptimizationAnalysis, ProfileIteratorTraversal) {
    std::cout << "\n=== Iterator Traversal Profiling ===\n";
    
    // Build a tree with known structure
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
            "pt_" + std::to_string(i));
        std::vector<double> point = {dist(gen), dist(gen)};
        dr->putPoint(&point);
        
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        root->xt_insert(cachedRoot, dr);
    }
    
    // Get final root
    root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
    cachedRoot = (CacheNode*)index->getRootAddress();
    
    std::cout << "Built tree with 10K random points\n";
    std::cout << "Root has " << root->n() << " entries\n\n";
    
    // Profile different query patterns
    struct Profile {
        double x, y, size;
        std::string desc;
    };
    
    std::vector<Profile> profiles = {
        {50, 50, 10, "Center query"},
        {0, 0, 10, "Corner query"},
        {90, 90, 10, "Far corner query"},
        {50, 50, 1, "Point query"},
        {50, 50, 50, "Large query"},
    };
    
    for (const auto& prof : profiles) {
        std::cout << prof.desc << " [" << prof.x << "," << prof.y << "] size " << prof.size << ":\n";
        
        DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
        std::vector<double> min_pt = {prof.x, prof.y};
        std::vector<double> max_pt = {prof.x + prof.size, prof.y + prof.size};
        query->putPoint(&min_pt);
        query->putPoint(&max_pt);
        
        // Time iterator creation multiple times
        const int runs = 100;
        double totalTime = 0;
        int totalResults = 0;
        
        for (int r = 0; r < runs; r++) {
            auto start = high_resolution_clock::now();
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            auto createTime = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count();
            
            int results = 0;
            while (iter->hasNext()) {
                iter->next();
                results++;
            }
            delete iter;
            
            totalTime += createTime;
            totalResults += results;
        }
        
        std::cout << "  Avg creation time: " << (totalTime / runs / 1000.0) << " μs\n";
        std::cout << "  Avg results: " << (totalResults / (double)runs) << "\n\n";
    }
    
    delete index;
}