/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Optimized multi-segment benchmark with proper query patterns
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

class OptimizedMultiSegmentBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/optimized_multi_seg.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/optimized_multi_seg.dat");
    }
    
    // Helper to get the current root from the index
    XTreeBucket<DataRecord>* getCurrentRoot(IndexDetails<DataRecord>* index) {
        auto rootAddress = index->getRootAddress();
        if (rootAddress == 0) return nullptr;
        
        CacheNode* cacheNode = (CacheNode*)rootAddress;
        if (!cacheNode || !cacheNode->object) return nullptr;
        
        return (XTreeBucket<DataRecord>*)(cacheNode->object);
    }
    
    CacheNode* getCurrentCachedRoot(IndexDetails<DataRecord>* index) {
        auto rootAddress = index->getRootAddress();
        if (rootAddress == 0) return nullptr;
        return (CacheNode*)rootAddress;
    }
    
    DataRecord* createPointRecord(IndexDetails<DataRecord>* index, const std::string& id, double x, double y) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, id);
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        return dr;
    }
};

TEST_F(OptimizedMultiSegmentBenchmark, OptimizedQueryPerformance) {
    std::cout << "\n=== Optimized Multi-Segment Query Performance ===\n";
    
    // Create index with MMAP persistence
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/optimized_multi_seg.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert test data
    std::cout << "Populating tree with 100,000 points...\n";
    const int GRID_SIZE = 316; // ~100K points
    
    auto insertStart = high_resolution_clock::now();
    for (int x = 0; x < GRID_SIZE; x++) {
        for (int y = 0; y < GRID_SIZE; y++) {
            DataRecord* dr = createPointRecord(index, 
                "grid_" + std::to_string(x) + "_" + std::to_string(y),
                (double)x,
                (double)y
            );
            root->xt_insert(cachedRoot, dr);
        }
    }
    auto insertEnd = high_resolution_clock::now();
    auto insertDuration = duration_cast<seconds>(insertEnd - insertStart);
    std::cout << "Insert completed in " << insertDuration.count() << " seconds\n\n";
    
    // Cache root once (proper pattern)
    auto rootAddress = index->getRootAddress();
    CacheNode* cacheNode = (CacheNode*)rootAddress;
    XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
    
    // Pre-generate query positions
    const int QUERIES_PER_TEST = 100000;
    std::vector<std::pair<double, double>> queryPositions;
    std::mt19937 gen(42);
    
    // Test different query patterns
    struct QueryTest {
        double boxSize;
        std::string description;
    };
    
    std::vector<QueryTest> queryTests = {
        {1.0, "Point queries (1x1 box)"},
        {10.0, "Small range queries (10x10 box)"},
        {50.0, "Medium range queries (50x50 box)"}
    };
    
    for (const auto& test : queryTests) {
        // Generate query positions
        queryPositions.clear();
        std::uniform_real_distribution<> dis(0, GRID_SIZE - test.boxSize);
        for (int i = 0; i < QUERIES_PER_TEST; i++) {
            queryPositions.push_back({dis(gen), dis(gen)});
        }
        
        // Create reusable query object
        DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
        
        // Run benchmark
        auto startTime = high_resolution_clock::now();
        int totalResults = 0;
        
        for (const auto& pos : queryPositions) {
            // Update query bounds efficiently
            query->getKey()->reset();
            std::vector<double> min_pt = {pos.first, pos.second};
            std::vector<double> max_pt = {pos.first + test.boxSize, pos.second + test.boxSize};
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
        
        // Query is managed by allocator, don't delete directly
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        
        double qps = QUERIES_PER_TEST * 1000000.0 / duration.count();
        double avgResults = totalResults / (double)QUERIES_PER_TEST;
        
        std::cout << test.description << ":\n";
        std::cout << "  Queries: " << QUERIES_PER_TEST << "\n";
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Avg results: " << std::setprecision(1) << avgResults << "\n\n";
    }
    
    // Memory usage
    auto* compact_alloc = index->getCompactAllocator();
    double memoryMB = compact_alloc->get_snapshot_manager()->get_allocator()->get_used_size() / (1024.0 * 1024.0);
    std::cout << "Total memory used: " << std::setprecision(2) << memoryMB << " MB\n";
    std::cout << "Memory per point: " << std::setprecision(2) 
              << (memoryMB * 1024 * 1024 / (GRID_SIZE * GRID_SIZE)) << " bytes\n";
    
    // Explicitly save before deleting
    std::cout << "[DEBUG] Explicitly saving snapshot...\n" << std::flush;
    if (compact_alloc && compact_alloc->get_snapshot_manager()) {
        compact_alloc->get_snapshot_manager()->save_snapshot();
        std::cout << "[DEBUG] Snapshot saved explicitly\n" << std::flush;
    }
    
    std::cout << "[DEBUG] About to delete index...\n" << std::flush;
    delete index;
    std::cout << "[DEBUG] Index deleted\n" << std::flush;
}

TEST_F(OptimizedMultiSegmentBenchmark, RealWorldScenario) {
    std::cout << "\n=== Real-World Scenario: Geospatial Points ===\n" << std::flush;
    std::cout << "Note: This test inserts 1M points to test large-scale performance\n" << std::flush;
    std::cout << "Test starting...\n" << std::flush;
    
    // Create index
    std::cout << "Creating index...\n" << std::flush;
    std::vector<const char*> dimLabels = {"lon", "lat"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/optimized_multi_seg.dat"
    );
    std::cout << "Index created\n" << std::flush;
    
    std::cout << "Allocating root bucket...\n" << std::flush;
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    std::cout << "Adding to cache...\n" << std::flush;
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    std::cout << "Setting root address...\n" << std::flush;
    index->setRootAddress((long)cachedRoot);
    std::cout << "Root setup complete\n" << std::flush;
    
    // Insert realistic point data (US cities simulation)
    const int NUM_POINTS = 100000;  // 100K points for faster testing
    std::cout << "Inserting " << NUM_POINTS << " geographic points...\n";
    
    // Memory tracking will be shown during inserts
    
    std::mt19937 gen(42);
    std::uniform_real_distribution<> lon_dist(-125.0, -66.0);  // US longitude range
    std::uniform_real_distribution<> lat_dist(24.0, 49.0);     // US latitude range
    
    auto insertStart = high_resolution_clock::now();
    std::cout << "Starting insertions...\n" << std::flush;
    
    // Track memory usage
    auto* compact_alloc = index->getCompactAllocator();
    
    for (int i = 0; i < NUM_POINTS; i++) {
        if (i < 10 || i % 1000 == 0 || (i > 9990 && i < 10015)) {  // Changed to print every 1000
            std::cout << "About to insert point " << i;
            if (compact_alloc) {
                double memoryMB = compact_alloc->get_snapshot_manager()->get_allocator()->get_used_size() / (1024.0 * 1024.0);
                std::cout << " (memory: " << std::setprecision(2) << memoryMB << " MB)";
            }
            std::cout << "...\n" << std::flush;
        }
        
        if (i == 10000 || i == 10010) {
            std::cout << "DEBUG: Creating record " << i << "...\n" << std::flush;
        }
        
        if (i == 10010) {
            std::cout << "DEBUG: About to call createPointRecord for 10010...\n" << std::flush;
        }
        
        double lon = (i == 10010) ? -100.0 : lon_dist(gen);
        double lat = (i == 10010) ? 40.0 : lat_dist(gen);
        
        DataRecord* dr = createPointRecord(index,
            "loc_" + std::to_string(i),
            lon,
            lat
        );
        
        if (i == 10010) {
            std::cout << "DEBUG: createPointRecord returned dr=" << dr << "\n" << std::flush;
        }
        if (i == 10000 || i == 10010) {
            std::cout << "DEBUG: Record created, getting current root...\n" << std::flush;
        }
        
        // CRITICAL: Update root and cachedRoot before each insert
        // The tree may split and change the root
        XTreeBucket<DataRecord>* oldRoot = root;
        root = getCurrentRoot(index);
        cachedRoot = getCurrentCachedRoot(index);
        
        if (i == 10000 || (i > 9990 && i < 10010)) {
            std::cout << "DEBUG[" << i << "]: Old root=" << oldRoot << ", New root=" << root 
                      << ", cachedRoot=" << cachedRoot << "\n" << std::flush;
            if (oldRoot != root) {
                std::cout << "DEBUG[" << i << "]: ROOT CHANGED!\n" << std::flush;
            }
        }
        
        if (!root || !cachedRoot) {
            std::cout << "ERROR: Failed to get valid root at point " << i << "\n" << std::flush;
            throw std::runtime_error("Invalid root");
        }
        
        if (i == 10000 || i == 10010) {
            std::cout << "DEBUG: Calling xt_insert for record " << i << "...\n" << std::flush;
        }
        
        root->xt_insert(cachedRoot, dr);
        
        if (i == 10000 || i == 10010) {
            std::cout << "DEBUG: xt_insert completed for record " << i << "\n" << std::flush;
        }
        
        if (i % 100000 == 99999) {
            std::cout << "  Inserted " << (i + 1) << " points";
            // Memory info would be here if we had access to allocator stats
            std::cout << "\n";
        }
    }
    auto insertEnd = high_resolution_clock::now();
    auto insertDuration = duration_cast<seconds>(insertEnd - insertStart);
    
    std::cout << "Insert completed in " << insertDuration.count() << " seconds ("
              << (NUM_POINTS / insertDuration.count()) << " inserts/sec)\n\n";
    
    // Cache root
    auto rootAddress = index->getRootAddress();
    CacheNode* cacheNode = (CacheNode*)rootAddress;
    XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
    
    // Test realistic queries
    std::cout << "Running geospatial queries...\n" << std::flush;
    std::cout << "[DEBUG] Root address: " << rootAddress << ", cacheNode: " << cacheNode 
              << ", currentRoot: " << currentRoot << "\n" << std::flush;
    
    struct GeoQuery {
        std::string description;
        double lon_center, lat_center;
        double radius;  // degrees
        int num_queries;
    };
    
    std::vector<GeoQuery> geoQueries = {
        {"Neighborhood search (1km radius)", -122.4194, 37.7749, 0.01, 10000},
        {"City-wide search (10km radius)", -122.4194, 37.7749, 0.1, 5000},
        {"Regional search (100km radius)", -122.4194, 37.7749, 1.0, 1000}
    };
    
    DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "geo_query");
    
    for (const auto& geo : geoQueries) {
        std::cout << "[DEBUG] Starting query test: " << geo.description << "\n" << std::flush;
        std::cout << "[DEBUG] Number of queries to run: " << geo.num_queries << "\n" << std::flush;
        
        auto startTime = high_resolution_clock::now();
        int totalResults = 0;
        
        std::uniform_real_distribution<> offset_dist(-geo.radius, geo.radius);
        
        for (int i = 0; i < geo.num_queries; i++) {
            if (i == 0 || i % 1000 == 0) {
                std::cout << "[DEBUG] Running query " << i << " of " << geo.num_queries << "\n" << std::flush;
            }
            
            double lon = geo.lon_center + offset_dist(gen);
            double lat = geo.lat_center + offset_dist(gen);
            
            query->getKey()->reset();
            std::vector<double> min_pt = {lon - geo.radius, lat - geo.radius};
            std::vector<double> max_pt = {lon + geo.radius, lat + geo.radius};
            query->putPoint(&min_pt);
            query->putPoint(&max_pt);
            
            if (i == 0) {
                std::cout << "[DEBUG] Query bounds: [" << min_pt[0] << "," << min_pt[1] 
                          << "] to [" << max_pt[0] << "," << max_pt[1] << "]\n" << std::flush;
                std::cout << "[DEBUG] Creating iterator...\n" << std::flush;
            }
            
            auto iter = currentRoot->getIterator(cacheNode, query, INTERSECTS);
            
            if (i == 0) {
                std::cout << "[DEBUG] Iterator created, checking hasNext()...\n" << std::flush;
            }
            
            int queryResults = 0;
            while (iter->hasNext()) {
                iter->next();
                queryResults++;
                totalResults++;
            }
            
            if (i == 0) {
                std::cout << "[DEBUG] Query 0 found " << queryResults << " results\n" << std::flush;
            }
            
            delete iter;
        }
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        
        double qps = geo.num_queries * 1000000.0 / duration.count();
        double avgResults = totalResults / (double)geo.num_queries;
        
        std::cout << "\n" << geo.description << ":\n";
        std::cout << "  Queries: " << geo.num_queries << "\n";
        std::cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << " queries/sec\n";
        std::cout << "  Avg points found: " << std::setprecision(1) << avgResults << "\n";
    }
    
    std::cout << "[DEBUG] Finished all query tests\n" << std::flush;
    // Note: query is managed by the allocator, don't delete it manually
    
    std::cout << "[DEBUG] Computing final stats...\n" << std::flush;
    // Final stats
    double memoryMB = compact_alloc->get_snapshot_manager()->get_allocator()->get_used_size() / (1024.0 * 1024.0);
    std::cout << "\nFinal statistics:\n";
    std::cout << "  Total points: " << NUM_POINTS << "\n";
    std::cout << "  Memory used: " << std::setprecision(2) << memoryMB << " MB\n";
    std::cout << "  Memory per point: " << std::setprecision(0) 
              << (memoryMB * 1024 * 1024 / NUM_POINTS) << " bytes\n";
    
    // Explicitly save before deleting
    std::cout << "[DEBUG] Explicitly saving snapshot...\n" << std::flush;
    if (compact_alloc && compact_alloc->get_snapshot_manager()) {
        compact_alloc->get_snapshot_manager()->save_snapshot();
        std::cout << "[DEBUG] Snapshot saved explicitly\n" << std::flush;
    }
    
    std::cout << "[DEBUG] About to delete index...\n" << std::flush;
    delete index;
    std::cout << "[DEBUG] Index deleted\n" << std::flush;
}