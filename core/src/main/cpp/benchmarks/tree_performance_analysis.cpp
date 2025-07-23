/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Deep analysis of tree performance issues
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include <map>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class TreePerformanceAnalysis : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/perf_analysis.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/perf_analysis.dat");
    }
    
    struct QueryStats {
        int resultsFound = 0;
        double timeMs = 0;
    };
};

TEST_F(TreePerformanceAnalysis, DetailedPerformanceAnalysis) {
    std::cout << "\n=== Detailed Tree Performance Analysis ===\n";
    
    // Test configurations
    struct TestConfig {
        std::string name;
        int numPoints;
        bool useGrid;
        double spaceSize;
    };
    
    std::vector<TestConfig> configs = {
        {"Grid 10K in 100x100", 10000, true, 100},
        {"Random 10K in 100x100", 10000, false, 100},
        {"Random 10K in 316x316", 10000, false, 316},
        {"Random 1K in 100x100", 1000, false, 100},
    };
    
    for (const auto& config : configs) {
        std::cout << "\n--- " << config.name << " ---\n";
        
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
        std::uniform_real_distribution<> dist(0, config.spaceSize);
        
        auto insertStart = high_resolution_clock::now();
        
        if (config.useGrid) {
            int gridSize = (int)sqrt(config.numPoints);
            double step = config.spaceSize / gridSize;
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
            for (int i = 0; i < config.numPoints; i++) {
                DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
                    "pt_" + std::to_string(i));
                std::vector<double> point = {dist(gen), dist(gen)};
                dr->putPoint(&point);
                
                root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
                cachedRoot = (CacheNode*)index->getRootAddress();
                root->xt_insert(cachedRoot, dr);
            }
        }
        
        auto insertDuration = duration_cast<milliseconds>(high_resolution_clock::now() - insertStart);
        std::cout << "Insert time: " << insertDuration.count() << " ms\n";
        
        // Get final root
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        std::cout << "Root entries: " << root->n() << "\n";
        
        // Test different query patterns
        struct QueryPattern {
            std::string name;
            double boxSize;
            int numQueries;
        };
        
        std::vector<QueryPattern> patterns = {
            {"Point queries (1x1)", 1.0, 1000},
            {"Small queries (10x10)", 10.0, 1000},
            {"Medium queries (50x50)", 50.0, 100},
        };
        
        for (const auto& pattern : patterns) {
            std::cout << "\n  " << pattern.name << ":\n";
            
            // Collect statistics
            std::vector<QueryStats> allStats;
            double totalTime = 0;
            int totalResults = 0;
            
            for (int q = 0; q < pattern.numQueries; q++) {
                QueryStats stats;
                
                // Random query location
                double qx = dist(gen);
                double qy = dist(gen);
                if (qx + pattern.boxSize > config.spaceSize) 
                    qx = config.spaceSize - pattern.boxSize;
                if (qy + pattern.boxSize > config.spaceSize) 
                    qy = config.spaceSize - pattern.boxSize;
                
                DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
                std::vector<double> min_pt = {qx, qy};
                std::vector<double> max_pt = {qx + pattern.boxSize, qy + pattern.boxSize};
                query->putPoint(&min_pt);
                query->putPoint(&max_pt);
                
                auto queryStart = high_resolution_clock::now();
                
                auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
                while (iter->hasNext()) {
                    iter->next();
                    stats.resultsFound++;
                }
                delete iter;
                
                auto queryDuration = duration_cast<microseconds>(high_resolution_clock::now() - queryStart);
                stats.timeMs = queryDuration.count() / 1000.0;
                
                allStats.push_back(stats);
                totalTime += stats.timeMs;
                totalResults += stats.resultsFound;
            }
            
            // Analyze statistics
            double avgTime = totalTime / pattern.numQueries;
            double avgResults = totalResults / (double)pattern.numQueries;
            double qps = pattern.numQueries / (totalTime / 1000.0);
            
            // Find min/max times
            double minTime = 999999, maxTime = 0;
            for (const auto& s : allStats) {
                minTime = std::min(minTime, s.timeMs);
                maxTime = std::max(maxTime, s.timeMs);
            }
            
            std::cout << "    Avg time: " << std::fixed << std::setprecision(3) << avgTime << " ms\n";
            std::cout << "    Min/Max time: " << minTime << " / " << maxTime << " ms\n";
            std::cout << "    QPS: " << std::setprecision(0) << qps << "\n";
            std::cout << "    Avg results: " << std::setprecision(1) << avgResults << "\n";
            
            // Expected results for uniform distribution
            double expectedResults = (pattern.boxSize * pattern.boxSize) / 
                                   (config.spaceSize * config.spaceSize) * config.numPoints;
            std::cout << "    Expected results: " << expectedResults << "\n";
            
            if (avgResults > expectedResults * 1.5) {
                std::cout << "    WARNING: Getting " << (avgResults/expectedResults) 
                          << "x more results than expected!\n";
            }
        }
        
        delete index;
    }
    
    // Now let's analyze tree node structure more deeply
    std::cout << "\n\n=== Tree Node Analysis ===\n";
    
    // Build a tree and analyze its structure
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert points and track when splits occur
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dist(0, 100);
    
    int lastRootEntries = 0;
    for (int i = 0; i < 1000; i++) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
            "pt_" + std::to_string(i));
        std::vector<double> point = {dist(gen), dist(gen)};
        dr->putPoint(&point);
        
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        int entriesBefore = root->n();
        root->xt_insert(cachedRoot, dr);
        
        // Check if root changed
        auto* newRoot = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        int entriesAfter = newRoot->n();
        
        if (entriesAfter < entriesBefore || newRoot != root) {
            std::cout << "Split at insert " << i << ": " << entriesBefore 
                      << " -> " << entriesAfter << " entries\n";
        }
        
        // Track capacity issues
        if (entriesAfter > 100 && entriesAfter != lastRootEntries) {
            std::cout << "WARNING: Root has " << entriesAfter << " entries at insert " << i << "\n";
            lastRootEntries = entriesAfter;
        }
    }
    
    delete index;
}