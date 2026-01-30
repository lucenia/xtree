/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Simplified concurrent benchmark
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <iomanip>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/xtree_allocator_traits.hpp"

using namespace xtree;
using namespace std::chrono;

TEST(ConcurrentSimple, BasicQPS) {
    std::cout << "\n=== Simple Concurrent QPS Test ===\n";
    
    // Use the same initialization as MultiSegmentBenchmark
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/concurrent_simple.dat"
    );
    
    // Initial root setup
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Pre-populate with 10000 points
    std::cout << "Populating with 10000 points...\n";
    for (int i = 0; i < 10000; i++) {
        auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(i));
        std::vector<double> pt = {(double)(i % 1000), (double)(i / 1000)};
        record->putPoint(&pt);
        
        // Get current root
        auto rootAddr = index->getRootAddress();
        cachedRoot = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddr;
        root = (XTreeBucket<DataRecord>*)(cachedRoot->object);
        root->xt_insert(cachedRoot, record);
    }
    
    std::cout << "Initial population complete\n\n";
    
    // Test configurations
    struct TestConfig {
        int readers;
        int writers;
        const char* description;
    };
    
    std::vector<TestConfig> configs = {
        {1, 0, "1 reader, 0 writers"},
        {4, 0, "4 readers, 0 writers"},
        {4, 1, "4 readers, 1 writer"},
        {8, 2, "8 readers, 2 writers"}
    };
    
    for (const auto& config : configs) {
        std::atomic<int> queries{0};
        std::atomic<int> inserts{0};
        std::atomic<bool> stop{false};
        
        auto start = high_resolution_clock::now();
        std::vector<std::thread> threads;
        
        // Reader threads
        for (int i = 0; i < config.readers; i++) {
            threads.emplace_back([&, i]() {
                std::mt19937 gen(i);
                std::uniform_real_distribution<> dis(0, 1000);
                
                while (!stop) {
                    // Create query
                    auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
                    double x = dis(gen);
                    double y = dis(gen);
                    std::vector<double> minPt = {x - 50, y - 50};
                    std::vector<double> maxPt = {x + 50, y + 50};
                    query->putPoint(&minPt);
                    query->putPoint(&maxPt);
                    
                    // Get current root and search
                    auto rootAddr = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddr;
                    auto* root = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    
                    auto iter = root->getIterator(cacheNode, query, INTERSECTS);
                    int count = 0;
                    while (iter->hasNext()) {
                        if (iter->next()) count++;
                    }
                    delete iter;
                    
                    queries++;
                }
            });
        }
        
        // Writer threads
        int nextId = 10000;
        for (int i = 0; i < config.writers; i++) {
            threads.emplace_back([&, i]() {
                std::mt19937 gen(1000 + i);
                std::uniform_real_distribution<> dis(0, 1000);
                
                while (!stop) {
                    auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(nextId++));
                    std::vector<double> pt = {dis(gen), dis(gen)};
                    record->putPoint(&pt);
                    
                    // Get current root and insert
                    auto rootAddr = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddr;
                    auto* root = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    root->xt_insert(cacheNode, record);
                    
                    inserts++;
                }
            });
        }
        
        // Run for 5 seconds
        std::this_thread::sleep_for(seconds(5));
        stop = true;
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        double qps = queries * 1000.0 / duration.count();
        double ips = inserts * 1000.0 / duration.count();
        
        std::cout << config.description << ":\n";
        std::cout << "  Queries: " << queries << " (" << std::fixed << std::setprecision(0) << qps << " QPS)\n";
        std::cout << "  Inserts: " << inserts << " (" << std::fixed << std::setprecision(0) << ips << " IPS)\n\n";
    }
    
    delete index;
}