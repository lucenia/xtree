/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Optimized query benchmark to identify performance bottlenecks
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class OptimizedQueryBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/optimized_benchmark.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/optimized_benchmark.dat");
    }
    
    DataRecord* createPointRecord(const std::string& id, double x, double y) {
        DataRecord* dr = new DataRecord(2, 32, id);
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        return dr;
    }
};

TEST_F(OptimizedQueryBenchmark, CompareQueryStrategies) {
    std::cout << "\n=== Optimized Query Performance Comparison ===\n";
    
    // Create and populate index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY  // Use in-memory for pure performance
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Populate with grid data
    std::cout << "Populating tree with 10,000 points...\n";
    const int GRID_SIZE = 100; // 10K points for faster testing
    
    for (int x = 0; x < GRID_SIZE; x++) {
        for (int y = 0; y < GRID_SIZE; y++) {
            DataRecord* dr = createPointRecord(
                "grid_" + std::to_string(x) + "_" + std::to_string(y),
                (double)x,
                (double)y
            );
            root->xt_insert(cachedRoot, dr);
        }
    }
    
    const int NUM_QUERIES = 100000;
    
    // Pre-generate query positions
    std::vector<std::pair<double, double>> queryPositions;
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(0, GRID_SIZE - 10);
    for (int i = 0; i < NUM_QUERIES; i++) {
        queryPositions.push_back({dis(gen), dis(gen)});
    }
    
    // Test 1: Original approach (get root every time)
    {
        std::cout << "\nTest 1: Original approach (get root + create/delete query):\n";
        auto startTime = high_resolution_clock::now();
        int totalResults = 0;
        
        for (const auto& pos : queryPositions) {
            // Get root every time (unnecessary)
            auto rootAddress = index->getRootAddress();
            CacheNode* cacheNode = (CacheNode*)rootAddress;
            XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
            
            // Create query object
            DataRecord* query = new DataRecord(2, 32, "query");
            std::vector<double> min_pt = {pos.first, pos.second};
            std::vector<double> max_pt = {pos.first + 10, pos.second + 10};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            // Execute query
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            delete iter;
            delete query;
        }
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        double qps = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Total results: " << totalResults << "\n";
    }
    
    // Test 2: Cache root, reuse query object
    {
        std::cout << "\nTest 2: Optimized (cache root, reuse query object):\n";
        auto startTime = high_resolution_clock::now();
        int totalResults = 0;
        
        // Cache root once
        auto rootAddress = index->getRootAddress();
        CacheNode* cacheNode = (CacheNode*)rootAddress;
        XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
        
        // Create reusable query object
        DataRecord* query = new DataRecord(2, 32, "query");
        
        for (const auto& pos : queryPositions) {
            // Update query bounds
            query->getKey()->reset();
            std::vector<double> min_pt = {pos.first, pos.second};
            std::vector<double> max_pt = {pos.first + 10, pos.second + 10};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            // Execute query
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            while (iter->hasNext()) {
                iter->next();
                totalResults++;
            }
            delete iter;
        }
        
        delete query;
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        double qps = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Total results: " << totalResults << "\n";
    }
    
    // Test 3: Direct KeyMBR comparison (theoretical maximum)
    {
        std::cout << "\nTest 3: Direct MBR intersection test (theoretical max):\n";
        
        // Create a simple vector of MBRs for comparison
        std::vector<KeyMBR*> mbrs;
        for (int x = 0; x < GRID_SIZE; x++) {
            for (int y = 0; y < GRID_SIZE; y++) {
                KeyMBR* mbr = new KeyMBR(2, 32);
                std::vector<double> point = {(double)x, (double)y};
                mbr->expandWithPoint(&point);
                mbrs.push_back(mbr);
            }
        }
        
        auto startTime = high_resolution_clock::now();
        int totalResults = 0;
        
        KeyMBR queryMBR(2, 32);
        
        for (const auto& pos : queryPositions) {
            // Update query bounds
            queryMBR.reset();
            std::vector<double> min_pt = {pos.first, pos.second};
            std::vector<double> max_pt = {pos.first + 10, pos.second + 10};
            queryMBR.expandWithPoint(&min_pt);
            queryMBR.expandWithPoint(&max_pt);
            
            // Simple linear scan
            for (auto* mbr : mbrs) {
                if (queryMBR.intersects(*mbr)) {
                    totalResults++;
                }
            }
        }
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        double qps = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Total results: " << totalResults << "\n";
        
        // Cleanup
        for (auto* mbr : mbrs) {
            delete mbr;
        }
    }
    
    // Test 4: Measure iterator overhead
    {
        std::cout << "\nTest 4: Iterator creation/deletion overhead:\n";
        auto startTime = high_resolution_clock::now();
        
        // Cache root
        auto rootAddress = index->getRootAddress();
        CacheNode* cacheNode = (CacheNode*)rootAddress;
        XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
        
        // Create a dummy query
        DataRecord* query = new DataRecord(2, 32, "query");
        std::vector<double> min_pt = {50.0, 50.0};
        std::vector<double> max_pt = {60.0, 60.0};
        query->putPoint(&min_pt);
        query->putPoint(&max_pt);
        
        // Just create and delete iterators
        for (int i = 0; i < NUM_QUERIES; i++) {
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            delete iter;
        }
        
        delete query;
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        double ops_per_sec = NUM_QUERIES * 1000000.0 / duration.count();
        
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  Iterator create/delete rate: " << std::fixed << std::setprecision(0) 
                  << ops_per_sec << " ops/sec\n";
    }
    
    std::cout << "\n=== Analysis ===\n";
    std::cout << "The low QPS is likely due to:\n";
    std::cout << "1. Unnecessary root lookups on every query\n";
    std::cout << "2. Memory allocation overhead (creating/deleting query objects)\n";
    std::cout << "3. Iterator creation overhead\n";
    std::cout << "4. Multi-segment pointer translation overhead\n";
    
    delete index;
}

TEST_F(OptimizedQueryBenchmark, ProfileSingleQuery) {
    std::cout << "\n=== Single Query Profiling ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert some test data
    for (int i = 0; i < 1000; i++) {
        DataRecord* dr = createPointRecord(
            "pt_" + std::to_string(i),
            (double)(i % 100),
            (double)(i / 100)
        );
        root->xt_insert(cachedRoot, dr);
    }
    
    // Profile different parts of a query
    const int ITERATIONS = 100000;
    
    // Time root lookup
    {
        auto start = high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; i++) {
            auto rootAddress = index->getRootAddress();
            CacheNode* cacheNode = (CacheNode*)rootAddress;
            XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
            (void)currentRoot; // Prevent optimization
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        std::cout << "Root lookup: " << (duration.count() / ITERATIONS) << " ns/op\n";
    }
    
    // Time query object creation
    {
        auto start = high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; i++) {
            DataRecord* query = new DataRecord(2, 32, "query");
            std::vector<double> min_pt = {10.0, 10.0};
            std::vector<double> max_pt = {20.0, 20.0};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            delete query;
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        std::cout << "Query object create/delete: " << (duration.count() / ITERATIONS) << " ns/op\n";
    }
    
    // Time iterator creation
    {
        DataRecord* query = new DataRecord(2, 32, "query");
        std::vector<double> min_pt = {10.0, 10.0};
        std::vector<double> max_pt = {20.0, 20.0};
        query->putPoint(&min_pt);
        query->putPoint(&max_pt);
        
        auto start = high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; i++) {
            auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
            delete iter;
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        std::cout << "Iterator create/delete: " << (duration.count() / ITERATIONS) << " ns/op\n";
        
        delete query;
    }
    
    delete index;
}