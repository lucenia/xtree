/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Realistic data distribution benchmark for XTree
 * Tests performance with real-world spatial data patterns
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <iomanip>
#include <cmath>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/xtree_allocator_traits.hpp"

using namespace xtree;
using namespace std::chrono;

class RealisticDataBenchmark : public ::testing::Test {
protected:
    // Generate clustered data points around centers
    std::vector<std::pair<double, double>> generateClusteredData(
        int numPoints, 
        const std::vector<std::pair<double, double>>& clusterCenters,
        double clusterStdDev,
        std::mt19937& gen) {
        
        std::vector<std::pair<double, double>> points;
        std::normal_distribution<> cluster_dist(0, clusterStdDev);
        std::uniform_int_distribution<> center_choice(0, clusterCenters.size() - 1);
        
        for (int i = 0; i < numPoints; i++) {
            auto& center = clusterCenters[center_choice(gen)];
            double x = center.first + cluster_dist(gen);
            double y = center.second + cluster_dist(gen);
            points.push_back({x, y});
        }
        
        return points;
    }
    
    // Generate data following Zipf distribution (power law)
    std::vector<std::pair<double, double>> generateZipfData(
        int numPoints,
        double xMin, double xMax,
        double yMin, double yMax,
        double alpha, // Zipf parameter (typically 1.0 to 2.0)
        std::mt19937& gen) {
        
        std::vector<std::pair<double, double>> points;
        
        // Create hotspots based on Zipf distribution
        int numHotspots = 20;
        std::vector<std::pair<double, double>> hotspots;
        std::uniform_real_distribution<> x_dist(xMin, xMax);
        std::uniform_real_distribution<> y_dist(yMin, yMax);
        
        for (int i = 0; i < numHotspots; i++) {
            hotspots.push_back({x_dist(gen), y_dist(gen)});
        }
        
        // Zipf distribution for selecting hotspots
        std::vector<double> weights;
        for (int i = 1; i <= numHotspots; i++) {
            weights.push_back(1.0 / std::pow(i, alpha));
        }
        std::discrete_distribution<> zipf_dist(weights.begin(), weights.end());
        
        // Generate points around hotspots
        std::normal_distribution<> scatter(0, 5.0);
        for (int i = 0; i < numPoints; i++) {
            int hotspot_idx = zipf_dist(gen);
            double x = hotspots[hotspot_idx].first + scatter(gen);
            double y = hotspots[hotspot_idx].second + scatter(gen);
            points.push_back({x, y});
        }
        
        return points;
    }
    
    // Generate mixed density data (urban vs rural pattern)
    std::vector<std::pair<double, double>> generateMixedDensityData(
        int numPoints,
        std::mt19937& gen) {
        
        std::vector<std::pair<double, double>> points;
        
        // Define urban centers with high density
        std::vector<std::tuple<double, double, double, int>> urbanAreas = {
            {100, 100, 10, 40},  // x, y, radius, density_weight
            {300, 200, 15, 30},
            {500, 400, 20, 50},
            {200, 450, 12, 35}
        };
        
        // 80% of points in urban areas, 20% rural
        int urbanPoints = numPoints * 0.8;
        int ruralPoints = numPoints * 0.2;
        
        // Urban points - tightly clustered
        std::uniform_int_distribution<> urban_choice(0, urbanAreas.size() - 1);
        for (int i = 0; i < urbanPoints; i++) {
            auto& urban = urbanAreas[urban_choice(gen)];
            std::normal_distribution<> urban_dist(0, std::get<2>(urban) / 3);
            double x = std::get<0>(urban) + urban_dist(gen);
            double y = std::get<1>(urban) + urban_dist(gen);
            points.push_back({x, y});
        }
        
        // Rural points - widely scattered
        std::uniform_real_distribution<> rural_dist(0, 1000);
        for (int i = 0; i < ruralPoints; i++) {
            points.push_back({rural_dist(gen), rural_dist(gen)});
        }
        
        return points;
    }
};

TEST_F(RealisticDataBenchmark, ClusteredDataPerformance) {
    std::cout << "\n=== Clustered Data Performance Test ===\n";
    std::cout << "Simulating city clusters with dense point concentrations\n\n";
    
    // Clean up any existing files
    std::remove("/tmp/realistic_clustered.dat");
    
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 128, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/realistic_clustered.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Define cluster centers (major cities pattern)
    std::vector<std::pair<double, double>> clusterCenters = {
        {100, 100}, {500, 100}, {300, 300}, {100, 500}, {500, 500},
        {250, 150}, {400, 400}, {150, 350}
    };
    
    std::mt19937 gen(42);
    
    // Generate 100K clustered points
    std::cout << "Generating 100,000 clustered points...\n";
    auto points = generateClusteredData(100000, clusterCenters, 20.0, gen);
    
    // Insert points
    auto insertStart = high_resolution_clock::now();
    for (size_t i = 0; i < points.size(); i++) {
        auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(i));
        std::vector<double> pt = {points[i].first, points[i].second};
        record->putPoint(&pt);
        
        auto rootAddress = index->getRootAddress();
        cachedRoot = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
        root = (XTreeBucket<DataRecord>*)(cachedRoot->object);
        root->xt_insert(cachedRoot, record);
    }
    auto insertEnd = high_resolution_clock::now();
    auto insertDuration = duration_cast<milliseconds>(insertEnd - insertStart);
    
    std::cout << "Insert time: " << insertDuration.count() << " ms\n";
    std::cout << "Insert rate: " << (points.size() * 1000.0 / insertDuration.count()) << " points/sec\n\n";
    
    // Test queries on clustered data
    std::cout << "Testing query performance on clustered data...\n";
    
    struct QueryTest {
        std::string name;
        double radius;
        int numQueries;
    };
    
    std::vector<QueryTest> queryTests = {
        {"Small radius (within cluster)", 5.0, 10000},
        {"Medium radius (crosses clusters)", 50.0, 5000},
        {"Large radius (multiple clusters)", 200.0, 1000}
    };
    
    for (const auto& test : queryTests) {
        auto queryStart = high_resolution_clock::now();
        int totalResults = 0;
        
        std::uniform_real_distribution<> pos_dist(0, 600);
        for (int i = 0; i < test.numQueries; i++) {
            double x = pos_dist(gen);
            double y = pos_dist(gen);
            
            auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> minPt = {x - test.radius, y - test.radius};
            std::vector<double> maxPt = {x + test.radius, y + test.radius};
            query->putPoint(&minPt);
            query->putPoint(&maxPt);
            
            auto rootAddress = index->getRootAddress();
            auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
            auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
            
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            while (iter->hasNext()) {
                if (iter->next()) totalResults++;
            }
            delete iter;
        }
        
        auto queryEnd = high_resolution_clock::now();
        auto queryDuration = duration_cast<milliseconds>(queryEnd - queryStart);
        
        std::cout << test.name << ":\n";
        std::cout << "  Queries: " << test.numQueries << "\n";
        std::cout << "  Time: " << queryDuration.count() << " ms\n";
        std::cout << "  QPS: " << (test.numQueries * 1000.0 / queryDuration.count()) << "\n";
        std::cout << "  Avg results: " << (totalResults / (double)test.numQueries) << "\n\n";
    }
    
    // Multi-threaded query performance test
    std::cout << "\n=== Multi-threaded Query Performance ===\n";
    std::cout << "Testing scalability with multiple reader threads...\n\n";
    
    for (int numThreads : {1, 2, 4, 8, 16}) {
        std::atomic<int> totalQueries{0};
        std::atomic<int> totalResults{0};
        const int queriesPerThread = 5000;
        
        auto start = high_resolution_clock::now();
        std::vector<std::thread> threads;
        
        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back([&, t]() {
                std::mt19937 local_gen(t);
                std::uniform_real_distribution<> pos_dist(0, 600);
                
                for (int i = 0; i < queriesPerThread; i++) {
                    double x = pos_dist(local_gen);
                    double y = pos_dist(local_gen);
                    
                    auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
                    std::vector<double> minPt = {x - 50, y - 50};
                    std::vector<double> maxPt = {x + 50, y + 50};
                    query->putPoint(&minPt);
                    query->putPoint(&maxPt);
                    
                    auto rootAddress = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                    auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    
                    auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
                    int count = 0;
                    while (iter->hasNext()) {
                        if (iter->next()) count++;
                    }
                    delete iter;
                    
                    totalResults += count;
                    totalQueries++;
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        double qps = (totalQueries.load() * 1000.0) / duration.count();
        double avgResults = totalResults.load() / (double)totalQueries.load();
        
        std::cout << "Threads: " << std::setw(2) << numThreads 
                  << " | Queries: " << std::setw(6) << totalQueries.load()
                  << " | Time: " << std::setw(6) << duration.count() << " ms"
                  << " | QPS: " << std::setw(8) << std::fixed << std::setprecision(1) << qps
                  << " | Avg results: " << std::setprecision(1) << avgResults << "\n";
    }
    
    delete index;
}

TEST_F(RealisticDataBenchmark, ZipfDistributionPerformance) {
    std::cout << "\n=== Zipf Distribution Performance Test ===\n";
    std::cout << "Simulating power-law distribution (common in geographic data)\n\n";
    
    std::remove("/tmp/realistic_zipf.dat");
    
    std::vector<const char*> dimLabels = {"lon", "lat"};
    auto* index = new IndexDetails<DataRecord>(
        2, 128, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/realistic_zipf.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    std::mt19937 gen(42);
    
    // Generate Zipf-distributed data
    std::cout << "Generating 100,000 points with Zipf distribution (alpha=1.5)...\n";
    auto points = generateZipfData(100000, 0, 1000, 0, 1000, 1.5, gen);
    
    // Insert and measure
    auto insertStart = high_resolution_clock::now();
    for (size_t i = 0; i < points.size(); i++) {
        auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(i));
        std::vector<double> pt = {points[i].first, points[i].second};
        record->putPoint(&pt);
        
        auto rootAddress = index->getRootAddress();
        cachedRoot = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
        root = (XTreeBucket<DataRecord>*)(cachedRoot->object);
        root->xt_insert(cachedRoot, record);
    }
    auto insertEnd = high_resolution_clock::now();
    
    std::cout << "Insert time: " << duration_cast<milliseconds>(insertEnd - insertStart).count() << " ms\n\n";
    
    // Query performance on Zipf-distributed data
    std::cout << "Testing query performance on Zipf-distributed data...\n";
    
    struct ZipfQuery {
        std::string name;
        double radius;
        int numQueries;
        bool targetHotspots;
    };
    
    std::vector<ZipfQuery> queries = {
        {"Hotspot queries (small radius)", 10.0, 5000, true},
        {"Random queries (small radius)", 10.0, 5000, false},
        {"Hotspot queries (large radius)", 100.0, 2000, true},
        {"Random queries (large radius)", 100.0, 2000, false}
    };
    
    for (const auto& test : queries) {
        auto start = high_resolution_clock::now();
        int totalResults = 0;
        
        for (int i = 0; i < test.numQueries; i++) {
            double x, y;
            if (test.targetHotspots) {
                // Query around first few hotspots (high density areas)
                std::uniform_int_distribution<> hotspot_idx(0, 4);
                int idx = hotspot_idx(gen);
                x = 100 + idx * 200 + std::uniform_real_distribution<>(-50, 50)(gen);
                y = 100 + idx * 200 + std::uniform_real_distribution<>(-50, 50)(gen);
            } else {
                // Random queries across space
                x = std::uniform_real_distribution<>(0, 1000)(gen);
                y = std::uniform_real_distribution<>(0, 1000)(gen);
            }
            
            auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
            std::vector<double> minPt = {x - test.radius, y - test.radius};
            std::vector<double> maxPt = {x + test.radius, y + test.radius};
            query->putPoint(&minPt);
            query->putPoint(&maxPt);
            
            auto rootAddress = index->getRootAddress();
            auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
            auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
            
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            int count = 0;
            while (iter->hasNext()) {
                if (iter->next()) count++;
            }
            delete iter;
            totalResults += count;
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        double qps = (test.numQueries * 1000.0) / duration.count();
        double avgResults = totalResults / (double)test.numQueries;
        
        std::cout << "\n" << test.name << ":\n";
        std::cout << "  Queries: " << test.numQueries << "\n";
        std::cout << "  Time: " << duration.count() << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(1) << qps << "\n";
        std::cout << "  Avg results: " << avgResults << "\n";
    }
    
    // Multi-threaded test on Zipf data
    std::cout << "\n=== Multi-threaded Performance on Zipf Data ===\n";
    
    for (int numThreads : {1, 4, 8, 16}) {
        std::atomic<int> totalQueries{0};
        const int queriesPerThread = 2500;
        
        auto start = high_resolution_clock::now();
        std::vector<std::thread> threads;
        
        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back([&, t]() {
                std::mt19937 local_gen(t);
                std::uniform_real_distribution<> pos_dist(0, 1000);
                
                for (int i = 0; i < queriesPerThread; i++) {
                    double x = pos_dist(local_gen);
                    double y = pos_dist(local_gen);
                    
                    auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
                    std::vector<double> minPt = {x - 25, y - 25};
                    std::vector<double> maxPt = {x + 25, y + 25};
                    query->putPoint(&minPt);
                    query->putPoint(&maxPt);
                    
                    auto rootAddress = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                    auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    
                    auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
                    while (iter->hasNext()) {
                        iter->next();
                    }
                    delete iter;
                    
                    totalQueries++;
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        
        double qps = (totalQueries.load() * 1000.0) / duration.count();
        
        std::cout << "Threads: " << std::setw(2) << numThreads 
                  << " | Queries: " << std::setw(6) << totalQueries.load()
                  << " | QPS: " << std::setw(10) << std::fixed << std::setprecision(1) << qps << "\n";
    }
    
    delete index;
}

TEST_F(RealisticDataBenchmark, MixedDensityConcurrent) {
    std::cout << "\n=== Mixed Density Concurrent Test ===\n";
    std::cout << "Simulating urban/rural mixed density with concurrent access\n\n";
    
    std::remove("/tmp/realistic_mixed.dat");
    
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 128, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/realistic_mixed.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Pre-populate with mixed density data
    std::mt19937 gen(42);
    auto points = generateMixedDensityData(50000, gen);
    
    std::cout << "Inserting 50,000 mixed density points...\n";
    for (size_t i = 0; i < points.size(); i++) {
        auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(i));
        std::vector<double> pt = {points[i].first, points[i].second};
        record->putPoint(&pt);
        
        auto rootAddress = index->getRootAddress();
        cachedRoot = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
        root = (XTreeBucket<DataRecord>*)(cachedRoot->object);
        root->xt_insert(cachedRoot, record);
    }
    
    // Concurrent test
    std::cout << "\nRunning concurrent test with 4 readers, 2 writers...\n";
    
    std::atomic<int> queries{0};
    std::atomic<int> inserts{0};
    std::atomic<bool> stop{false};
    
    auto start = high_resolution_clock::now();
    std::vector<std::thread> threads;
    
    // Reader threads - bias queries toward urban areas
    for (int i = 0; i < 4; i++) {
        threads.emplace_back([&, i]() {
            std::mt19937 local_gen(i);
            std::uniform_real_distribution<> area_choice(0, 1);
            std::uniform_real_distribution<> urban_dist(0, 600);
            std::uniform_real_distribution<> rural_dist(0, 1000);
            
            while (!stop) {
                // 70% queries in urban areas
                bool queryUrban = area_choice(local_gen) < 0.7;
                double x = queryUrban ? urban_dist(local_gen) : rural_dist(local_gen);
                double y = queryUrban ? urban_dist(local_gen) : rural_dist(local_gen);
                double radius = queryUrban ? 10.0 : 50.0; // Smaller radius in urban
                
                auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
                std::vector<double> minPt = {x - radius, y - radius};
                std::vector<double> maxPt = {x + radius, y + radius};
                query->putPoint(&minPt);
                query->putPoint(&maxPt);
                
                auto rootAddress = index->getRootAddress();
                auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
                
                auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
                int count = 0;
                while (iter->hasNext()) {
                    if (iter->next()) count++;
                }
                delete iter;
                
                queries++;
            }
        });
    }
    
    // Writer threads - continue mixed pattern
    int nextId = 50000;
    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&, i]() {
            std::mt19937 local_gen(1000 + i);
            auto newPoints = generateMixedDensityData(10000, local_gen);
            size_t idx = 0;
            
            while (!stop && idx < newPoints.size()) {
                auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(nextId++));
                std::vector<double> pt = {newPoints[idx].first, newPoints[idx].second};
                record->putPoint(&pt);
                
                auto rootAddress = index->getRootAddress();
                auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
                currentRoot->xt_insert(cacheNode, record);
                
                inserts++;
                idx++;
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
    
    double qps = queries.load() * 1000.0 / duration.count();
    double ips = inserts.load() * 1000.0 / duration.count();
    
    std::cout << "\nResults:\n";
    std::cout << "  Duration: " << (duration.count() / 1000.0) << " seconds\n";
    std::cout << "  Queries: " << queries << " (" << std::fixed << std::setprecision(1) << qps << " QPS)\n";
    std::cout << "  Inserts: " << inserts << " (" << std::fixed << std::setprecision(1) << ips << " IPS)\n";
    
    // Test different thread configurations
    std::cout << "\n=== Scaling Test on Mixed Density Data ===\n";
    
    struct ThreadConfig {
        int readers;
        int writers;
    };
    
    std::vector<ThreadConfig> configs = {
        {1, 0},   // Read-only baseline
        {4, 0},   // 4 readers
        {8, 0},   // 8 readers
        {16, 0},  // 16 readers
        {8, 2},   // Mixed workload
        {16, 4}   // Heavy mixed workload
    };
    
    for (const auto& config : configs) {
        std::atomic<int> testQueries{0};
        std::atomic<int> testInserts{0};
        std::atomic<bool> testStop{false};
        
        auto testStart = high_resolution_clock::now();
        std::vector<std::thread> testThreads;
        
        // Reader threads
        for (int i = 0; i < config.readers; i++) {
            testThreads.emplace_back([&, i]() {
                std::mt19937 local_gen(i);
                std::uniform_real_distribution<> area_choice(0, 1);
                std::uniform_real_distribution<> urban_dist(0, 600);
                std::uniform_real_distribution<> rural_dist(0, 1000);
                
                while (!testStop) {
                    bool queryUrban = area_choice(local_gen) < 0.7;
                    double x = queryUrban ? urban_dist(local_gen) : rural_dist(local_gen);
                    double y = queryUrban ? urban_dist(local_gen) : rural_dist(local_gen);
                    double radius = queryUrban ? 10.0 : 50.0;
                    
                    auto* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
                    std::vector<double> minPt = {x - radius, y - radius};
                    std::vector<double> maxPt = {x + radius, y + radius};
                    query->putPoint(&minPt);
                    query->putPoint(&maxPt);
                    
                    auto rootAddress = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                    auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    
                    auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
                    while (iter->hasNext()) {
                        iter->next();
                    }
                    delete iter;
                    
                    testQueries++;
                }
            });
        }
        
        // Writer threads  
        int baseId = 100000 + config.writers * 10000;
        for (int i = 0; i < config.writers; i++) {
            testThreads.emplace_back([&, i, baseId]() {
                std::mt19937 local_gen(1000 + i);
                int id = baseId + i * 1000;
                
                while (!testStop) {
                    auto pt = generateMixedDensityData(1, local_gen)[0];
                    auto* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, std::to_string(id++));
                    std::vector<double> point = {pt.first, pt.second};
                    record->putPoint(&point);
                    
                    auto rootAddress = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                    auto* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    currentRoot->xt_insert(cacheNode, record);
                    
                    testInserts++;
                }
            });
        }
        
        // Run for 3 seconds
        std::this_thread::sleep_for(seconds(3));
        testStop = true;
        
        for (auto& t : testThreads) {
            t.join();
        }
        
        auto testEnd = high_resolution_clock::now();
        auto testDuration = duration_cast<milliseconds>(testEnd - testStart);
        
        double testQps = testQueries.load() * 1000.0 / testDuration.count();
        double testIps = testInserts.load() * 1000.0 / testDuration.count();
        
        std::cout << "R:" << std::setw(2) << config.readers 
                  << " W:" << std::setw(2) << config.writers
                  << " | QPS: " << std::setw(10) << std::fixed << std::setprecision(1) << testQps
                  << " | IPS: " << std::setw(8) << std::fixed << std::setprecision(1) << testIps << "\n";
    }
    
    delete index;
}