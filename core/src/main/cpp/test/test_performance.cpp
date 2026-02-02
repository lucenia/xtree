/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

/*
 * Performance benchmark for XTree operations
 */

#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <vector>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/xtiter.h"
#include "../src/perf_macros.h"

using namespace xtree;
using namespace std;
using namespace std::chrono;

class XTreePerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize test data
        srand(42); // Fixed seed for reproducibility
    }
    
    // Helper to generate random points
    vector<double> generateRandomPoint() {
        return {
            (rand() / (double)RAND_MAX) * 1000.0 - 500.0,
            (rand() / (double)RAND_MAX) * 1000.0 - 500.0
        };
    }
};

TEST_F(XTreePerformanceTest, KeyMBROperations) {
    const int NUM_OPERATIONS = 1000000;
    
    // Test expand performance
    {
        KeyMBR mbr1(2, 32);
        KeyMBR mbr2(2, 32);
        
        // Initialize mbr2
        vector<double> p1 = {100.0, 100.0};
        vector<double> p2 = {200.0, 200.0};
        mbr2.expandWithPoint(&p1);
        mbr2.expandWithPoint(&p2);
        
        auto start = high_resolution_clock::now();
        
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            mbr1.expand(mbr2);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        cout << "Expand operations: " << NUM_OPERATIONS 
             << " in " << duration.count() << " microseconds" 
             << " (" << (NUM_OPERATIONS * 1000.0 / duration.count()) << " ops/ms)" << endl;
    }
    
    // Test intersects performance
    {
        KeyMBR mbr1(2, 32);
        KeyMBR mbr2(2, 32);
        
        vector<double> p1 = {0.0, 0.0};
        vector<double> p2 = {100.0, 100.0};
        mbr1.expandWithPoint(&p1);
        mbr1.expandWithPoint(&p2);
        
        vector<double> p3 = {50.0, 50.0};
        vector<double> p4 = {150.0, 150.0};
        mbr2.expandWithPoint(&p3);
        mbr2.expandWithPoint(&p4);
        
        auto start = high_resolution_clock::now();
        
        bool result = false;
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            result = mbr1.intersects(mbr2);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        cout << "Intersects operations: " << NUM_OPERATIONS 
             << " in " << duration.count() << " microseconds" 
             << " (" << (NUM_OPERATIONS * 1000.0 / duration.count()) << " ops/ms)" << endl;
        
        EXPECT_TRUE(result); // Ensure compiler doesn't optimize away
    }
    
    // Test area calculation performance
    {
        KeyMBR mbr(2, 32);
        
        vector<double> p1 = {0.0, 0.0};
        vector<double> p2 = {100.0, 100.0};
        mbr.expandWithPoint(&p1);
        mbr.expandWithPoint(&p2);
        
        auto start = high_resolution_clock::now();
        
        double totalArea = 0;
        for (int i = 0; i < NUM_OPERATIONS / 10; i++) { // Less iterations as area is more expensive
            totalArea += mbr.area();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        cout << "Area calculations: " << NUM_OPERATIONS / 10
             << " in " << duration.count() << " microseconds" 
             << " (" << (NUM_OPERATIONS * 100.0 / duration.count()) << " ops/ms)" << endl;
        
        EXPECT_GT(totalArea, 0); // Ensure compiler doesn't optimize away
    }
}

TEST_F(XTreePerformanceTest, BulkInsertions) {
    const int NUM_POINTS = 100000;
    
    // Create index
    vector<const char*>* dimLabels = new vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    
    IndexDetails<DataRecord>* idx = new IndexDetails<DataRecord>(
        2, 32, dimLabels, nullptr, nullptr,
        "test_bulk_insertions",
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY);
    
    XTreeBucket<DataRecord>* root = new XTreeBucket<DataRecord>(idx, true, nullptr, nullptr, 0, true, 0);
    
    auto& cache = IndexDetails<DataRecord>::getCache();
    auto cachedRoot = cache.add(idx->getNextNodeID(), static_cast<IRecord*>(root));
    
    // Generate test data
    vector<DataRecord*> records;
    for (int i = 0; i < NUM_POINTS; i++) {
        DataRecord* dr = new DataRecord(2, 32, "point_" + to_string(i));
        auto point = generateRandomPoint();
        dr->putPoint(&point);
        records.push_back(dr);
    }
    
    // Measure insertion time
    auto start = high_resolution_clock::now();
    
    for (auto dr : records) {
        root->xt_insert(cachedRoot, dr);
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);
    
    cout << "Bulk insertion: " << NUM_POINTS 
         << " points in " << duration.count() << " milliseconds" 
         << " (" << (NUM_POINTS * 1000.0 / duration.count()) << " inserts/second)" << endl;
    
    EXPECT_GT(root->n(), 0);

    // NOTE: We do NOT call clearCache() before delete idx because:
    // - clearCache() deletes cached objects (including the root bucket)
    // - Then ~IndexDetails() tries to unpin the freed root → use-after-free
    // The cache is global and shared across tests; each test is responsible for
    // cleaning up its own index via delete, not clearing the shared cache.

    delete idx;
    delete dimLabels;
    
}

TEST_F(XTreePerformanceTest, SpatialQueries) {
    const int NUM_POINTS = 50000;
    const int NUM_QUERIES = 1000;
    
    // Setup tree with data
    vector<const char*>* dimLabels = new vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    
    IndexDetails<DataRecord>* idx = new IndexDetails<DataRecord>(
        2, 32, dimLabels, nullptr, nullptr,
        "test_spatial_queries",
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY);
    
    XTreeBucket<DataRecord>* root = new XTreeBucket<DataRecord>(idx, true, nullptr, nullptr, 0, true, 0);
    
    auto& cache = IndexDetails<DataRecord>::getCache();
    auto cachedRoot = cache.add(idx->getNextNodeID(), static_cast<IRecord*>(root));
    
    // Insert points
    for (int i = 0; i < NUM_POINTS; i++) {
        DataRecord* dr = new DataRecord(2, 32, "point_" + to_string(i));
        auto point = generateRandomPoint();
        dr->putPoint(&point);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Prepare queries
    vector<DataRecord*> queries;
    for (int i = 0; i < NUM_QUERIES; i++) {
        DataRecord* query = new DataRecord(2, 32, "query");
        double x = (rand() / (double)RAND_MAX) * 900.0 - 450.0;
        double y = (rand() / (double)RAND_MAX) * 900.0 - 450.0;
        vector<double> min = {x, y};
        vector<double> max = {x + 50.0, y + 50.0};
        query->putPoint(&min);
        query->putPoint(&max);
        queries.push_back(query);
    }
    
    // Measure query time
    auto start = high_resolution_clock::now();
    
    int totalResults = 0;
    for (auto query : queries) {
        auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
        while (iter->hasNext()) {
            auto result = iter->next();
            if (result) totalResults++;
        }
        delete iter;
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start);
    
    cout << "Range queries: " << NUM_QUERIES 
         << " queries in " << duration.count() << " microseconds" 
         << " (" << (NUM_QUERIES * 1000000.0 / duration.count()) << " queries/second)" << endl;
    cout << "Average results per query: " << (totalResults / (double)NUM_QUERIES) << endl;
    
    EXPECT_GT(totalResults, 0);
    
    // Cleanup
    for (auto query : queries) {
        delete query;
    }

    // NOTE: We do NOT call clearCache() before delete idx because:
    // - clearCache() deletes cached objects (including the root bucket)
    // - Then ~IndexDetails() tries to unpin the freed root → use-after-free

    delete idx;
    delete dimLabels;
    
}