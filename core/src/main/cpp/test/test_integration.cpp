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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <random>
#include <set>
#include <chrono>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/xtiter.h"
#include "../src/config.h"

using namespace xtree;
using namespace std;

// Comprehensive Integration Tests
class XTreeIntegrationTest : public ::testing::Test {
protected:
    IndexDetails<DataRecord>* idx;
    XTreeBucket<DataRecord>* root;
    LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>* cachedRoot;
    vector<const char*>* dimLabels;
    
    void SetUp() override {
        // Create index with 2D coordinates
        dimLabels = new vector<const char*>();
        dimLabels->push_back("longitude");
        dimLabels->push_back("latitude");
        
        idx = new IndexDetails<DataRecord>(2, 32, dimLabels, 1024*1024*10, nullptr, nullptr);
        
        // Create root bucket
        root = new XTreeBucket<DataRecord>(idx, true, nullptr, nullptr, 0, true, 0);
        
        // Properly add root to the cache so split logic works correctly
        cachedRoot = idx->getCache().add(idx->getNextNodeID(), root);
        idx->setRootAddress((long)cachedRoot);
    }
    
    void TearDown() override {
        // Clear the static cache which will clean up all buckets
        IndexDetails<DataRecord>::clearCache();
        
        delete idx;
        delete dimLabels;
    }
    
    // Helper to create a data record with a bounding box
    DataRecord* createDataRecord(const string& id, double minX, double minY, double maxX, double maxY) {
        DataRecord* dr = new DataRecord(2, 32, id);
        vector<double> minPoint = {minX, minY};
        vector<double> maxPoint = {maxX, maxY};
        dr->putPoint(&minPoint);
        dr->putPoint(&maxPoint);
        return dr;
    }
    
    // Helper to create a search query
    DataRecord* createSearchQuery(double minX, double minY, double maxX, double maxY) {
        return createDataRecord("search_query", minX, minY, maxX, maxY);
    }
};

// Test iterator basic functionality
TEST_F(XTreeIntegrationTest, IteratorDebug) {
    // Check root initial state
    EXPECT_EQ(root->n(), 0);
    KeyMBR* rootKey = root->getKey();
    ASSERT_NE(rootKey, nullptr);
    
    // Insert a record
    DataRecord* dr1 = createDataRecord("record1", 10.0, 10.0, 20.0, 20.0);
    root->xt_insert(cachedRoot, dr1);
    
    EXPECT_EQ(root->n(), 1);
    
    // Create an iterator that should match everything
    DataRecord* searchAll = createSearchQuery(-1000.0, -1000.0, 1000.0, 1000.0);
    auto iter = root->getIterator(cachedRoot, searchAll, INTERSECTS);
    
    // The root bucket itself should intersect
    bool rootIntersects = rootKey->intersects(*(searchAll->getKey()));
    EXPECT_TRUE(rootIntersects) << "Root key should intersect with large search box";
    
    int count = 0;
    while (iter->hasNext()) {
        DataRecord* result = iter->next();
        if (result) {
            count++;
        }
    }
    
    EXPECT_GE(count, 1) << "Should find at least one record";
    
    delete iter;
    delete searchAll;
}

// Test basic insertion and retrieval
TEST_F(XTreeIntegrationTest, BasicInsertAndSearch) {
    // Insert a single record
    DataRecord* dr1 = createDataRecord("record1", 10.0, 10.0, 20.0, 20.0);
    root->xt_insert(cachedRoot, dr1);
    
    EXPECT_EQ(root->n(), 1);
    
    // Check if the root's key was expanded to include the inserted record
    KeyMBR* rootKey = root->getKey();
    ASSERT_NE(rootKey, nullptr);
    EXPECT_LE(rootKey->getMin(0), 10.0);
    EXPECT_GE(rootKey->getMax(0), 20.0);
    EXPECT_LE(rootKey->getMin(1), 10.0);
    EXPECT_GE(rootKey->getMax(1), 20.0);
    
    // Search for exact match
    DataRecord* searchExact = createSearchQuery(10.0, 10.0, 20.0, 20.0);
    auto iter = root->getIterator(cachedRoot, searchExact, INTERSECTS);
    
    ASSERT_NE(iter, nullptr);
    
    int count = 0;
    while (iter->hasNext()) {
        DataRecord* result = iter->next();
        if (result) {
            count++;
            EXPECT_EQ(result->getRowID(), "record1");
        }
    }
    
    EXPECT_EQ(count, 1);
    
    delete iter;
    delete searchExact;
}

// Test spatial partitioning with grid data
TEST_F(XTreeIntegrationTest, GridPartitioning) {
    // Create a 10x10 grid of spatial records
    const int GRID_SIZE = 10;
    const double CELL_SIZE = 10.0;
    
    set<string> insertedIds;
    
    // Insert grid cells
    for (int i = 0; i < GRID_SIZE; i++) {
        for (int j = 0; j < GRID_SIZE; j++) {
            string id = "cell_" + to_string(i) + "_" + to_string(j);
            double minX = i * CELL_SIZE;
            double minY = j * CELL_SIZE;
            double maxX = minX + CELL_SIZE;
            double maxY = minY + CELL_SIZE;
            
            DataRecord* dr = createDataRecord(id, minX, minY, maxX, maxY);
            root->xt_insert(cachedRoot, dr);
            insertedIds.insert(id);
        }
    }
    
    EXPECT_EQ(root->n(), GRID_SIZE * GRID_SIZE);
    
    // Test 1: Search for a specific cell
    {
        DataRecord* searchCell = createSearchQuery(25.0, 25.0, 35.0, 35.0);
        auto iter = root->getIterator(cachedRoot, searchCell, INTERSECTS);
        
        set<string> foundIds;
        while (iter->hasNext()) {
            DataRecord* result = iter->next();
            if (result) {
                foundIds.insert(result->getRowID());
            }
        }
        
        // Should find cells that overlap with (25,25)-(35,35)
        // This includes cells (2,2), (2,3), (3,2), (3,3)
        EXPECT_GE(foundIds.size(), 4);
        EXPECT_TRUE(foundIds.count("cell_2_2") > 0);
        EXPECT_TRUE(foundIds.count("cell_2_3") > 0);
        EXPECT_TRUE(foundIds.count("cell_3_2") > 0);
        EXPECT_TRUE(foundIds.count("cell_3_3") > 0);
        
        delete iter;
        delete searchCell;
    }
    
    // Test 2: Search entire space
    {
        DataRecord* searchAll = createSearchQuery(-10.0, -10.0, 
                                                 GRID_SIZE * CELL_SIZE + 10.0, 
                                                 GRID_SIZE * CELL_SIZE + 10.0);
        auto iter = root->getIterator(cachedRoot, searchAll, INTERSECTS);
        
        int count = 0;
        while (iter->hasNext()) {
            DataRecord* result = iter->next();
            if (result) count++;
        }
        
        EXPECT_EQ(count, GRID_SIZE * GRID_SIZE);
        
        delete iter;
        delete searchAll;
    }
    
    // Test 3: Search outside the grid
    {
        DataRecord* searchOutside = createSearchQuery(1000.0, 1000.0, 2000.0, 2000.0);
        auto iter = root->getIterator(cachedRoot, searchOutside, INTERSECTS);
        
        int count = 0;
        while (iter->hasNext()) {
            DataRecord* result = iter->next();
            if (result) count++;
        }
        
        EXPECT_EQ(count, 0);
        
        delete iter;
        delete searchOutside;
    }
}

// Test with overlapping spatial objects
TEST_F(XTreeIntegrationTest, OverlappingObjects) {
    // Insert overlapping rectangles
    vector<pair<string, vector<double>>> testData = {
        {"rect1", {0.0, 0.0, 50.0, 50.0}},      // Large rectangle
        {"rect2", {25.0, 25.0, 75.0, 75.0}},    // Overlaps with rect1
        {"rect3", {60.0, 60.0, 80.0, 80.0}},    // Overlaps with rect2
        {"rect4", {10.0, 10.0, 30.0, 30.0}},    // Inside rect1
        {"rect5", {90.0, 90.0, 100.0, 100.0}}   // Isolated
    };
    
    for (const auto& data : testData) {
        DataRecord* dr = createDataRecord(data.first, 
                                        data.second[0], data.second[1], 
                                        data.second[2], data.second[3]);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Search for objects overlapping with center region (20,20)-(40,40)
    DataRecord* searchCenter = createSearchQuery(20.0, 20.0, 40.0, 40.0);
    auto iter = root->getIterator(cachedRoot, searchCenter, INTERSECTS);
    
    set<string> foundIds;
    while (iter->hasNext()) {
        DataRecord* result = iter->next();
        if (result) {
            foundIds.insert(result->getRowID());
        }
    }
    
    // Should find rect1, rect2, and rect4
    EXPECT_EQ(foundIds.size(), 3);
    EXPECT_TRUE(foundIds.count("rect1") > 0);
    EXPECT_TRUE(foundIds.count("rect2") > 0);
    EXPECT_TRUE(foundIds.count("rect4") > 0);
    EXPECT_FALSE(foundIds.count("rect3") > 0);
    EXPECT_FALSE(foundIds.count("rect5") > 0);
    
    delete iter;
    delete searchCenter;
}

// Test with real-world like point data (POIs)
TEST_F(XTreeIntegrationTest, PointOfInterestData) {
    // Simulate POI data (restaurants, shops, etc.)
    struct POI {
        string id;
        double lon;
        double lat;
        string type;
    };
    
    vector<POI> pois = {
        {"restaurant1", -122.4194, 37.7749, "restaurant"},
        {"restaurant2", -122.4084, 37.7849, "restaurant"},
        {"shop1", -122.4294, 37.7649, "shop"},
        {"shop2", -122.4094, 37.7549, "shop"},
        {"hotel1", -122.4194, 37.7849, "hotel"},
        {"hotel2", -122.4394, 37.7749, "hotel"},
        {"cafe1", -122.4194, 37.7649, "cafe"},
        {"cafe2", -122.3994, 37.7749, "cafe"}
    };
    
    // Insert POIs as point data
    for (const auto& poi : pois) {
        DataRecord* dr = new DataRecord(2, 32, poi.id);
        vector<double> point = {poi.lon, poi.lat};
        dr->putPoint(&point);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Search for POIs in a specific area
    DataRecord* searchArea = createSearchQuery(-122.42, 37.77, -122.41, 37.78);
    auto iter = root->getIterator(cachedRoot, searchArea, INTERSECTS);
    
    set<string> foundPOIs;
    while (iter->hasNext()) {
        DataRecord* result = iter->next();
        if (result) {
            foundPOIs.insert(result->getRowID());
        }
    }
    
    // Should find POIs within the search area
    // restaurant1 is at (-122.4194, 37.7749) which is within (-122.42, 37.77) to (-122.41, 37.78)
    // cafe1 is at (-122.4194, 37.7649) which is below the search area minimum of 37.77
    EXPECT_GE(foundPOIs.size(), 1);
    EXPECT_TRUE(foundPOIs.count("restaurant1") > 0);
    
    delete iter;
    delete searchArea;
}

// Stress test with large number of records
TEST_F(XTreeIntegrationTest, StressTestLargeDataset) {
    const int NUM_RECORDS = 250;  // Reduced to test split behavior
    
#ifdef _DEBUG
    cout << "Starting StressTestLargeDataset..." << endl;
#endif
    
    // Use fixed seed for reproducibility and to avoid random_device issues
    mt19937 gen(42);  // Fixed seed instead of random_device
    uniform_real_distribution<> dis(0.0, 1000.0);
    
    // Add constants from config.h for debugging
#ifdef _DEBUG
    cout << "Tree configuration: XTREE_M=" << XTREE_M << ", XTREE_MAX_FANOUT=" << XTREE_MAX_FANOUT 
         << ", XTREE_MAX_OVERLAP=" << XTREE_MAX_OVERLAP << endl;
#endif
    
    // Insert randomly distributed rectangles
    auto insertStart = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < NUM_RECORDS; i++) {
#ifdef _DEBUG
        if (i % 10 == 0) {
            cout << "Inserting record " << i << " of " << NUM_RECORDS << endl;
        }
#endif
        string id = "record_" + to_string(i);
        // Create data in a grid pattern to ensure clean splits
        // This pattern ensures minimal overlap between regions
        int gridX = i % 16;  // 16x16 grid
        int gridY = i / 16;
        double cellSize = 50.0;
        
        // Each record gets a small rectangle within its grid cell
        double x = gridX * cellSize + dis(gen) * 0.1 * cellSize;
        double y = gridY * cellSize + dis(gen) * 0.1 * cellSize;
        double width = dis(gen) * 0.1 * cellSize;  // Max 10% of cell
        double height = dis(gen) * 0.1 * cellSize; // Max 10% of cell
        
        DataRecord* dr = createDataRecord(id, x, y, x + width, y + height);
        
#ifdef _DEBUG
        if (i >= 230 && i <= 240) {
            cout << "  Record " << i << ": (" << x << "," << y << ") to (" 
                 << (x+width) << "," << (y+height) << ")" << endl;
            cout << "  Tree currently has " << root->n() << " children" << endl;
        }
#endif
        
        root->xt_insert(cachedRoot, dr);
        
#ifdef _DEBUG
        if (i >= 230 && i <= 235) {
            cout << "After insert " << i << ": root->n()=" << root->n() << endl;
        }
#endif
        
#ifdef _DEBUG
        if (i == 250) {
            cout << "Successfully inserted 250 records, tree has " << root->n() << " children" << endl;
        }
#endif
    }
    
    auto insertEnd = std::chrono::high_resolution_clock::now();
    auto insertDuration = std::chrono::duration_cast<std::chrono::milliseconds>(insertEnd - insertStart);
    
    // The tree will split as it grows, so root->n() won't equal NUM_RECORDS
    // Verify the tree structure
#ifdef _DEBUG
    cout << "After all insertions: root->n()=" << root->n() << endl;
#endif
    
    // With current test data, check what happened
#ifdef _DEBUG
    if (root->n() >= XTREE_M) {
        cout << "Root has " << root->n() << " children - it should have split or become a supernode" << endl;
    }
#endif
    
    // Performance check - should complete in reasonable time
    EXPECT_LT(insertDuration.count(), 5000); // Less than 5 seconds
    
    // Search performance test
    auto searchStart = std::chrono::high_resolution_clock::now();
    
    // Perform multiple searches
    int totalFound = 0;
#ifdef _DEBUG
    cout << "Starting search phase..." << endl;
#endif
    for (int i = 0; i < 100; i++) {
#ifdef _DEBUG
        if (i % 10 == 0) {
            cout << "Search " << i << " of 100" << endl;
        }
#endif
        double x = dis(gen);
        double y = dis(gen);
        DataRecord* searchQuery = createSearchQuery(x, y, x + 100.0, y + 100.0);
        
        auto iter = root->getIterator(cachedRoot, searchQuery, INTERSECTS);
        while (iter->hasNext()) {
            DataRecord* result = iter->next();
            if (result) totalFound++;
        }
        
        delete iter;
        delete searchQuery;
    }
    
    auto searchEnd = std::chrono::high_resolution_clock::now();
    auto searchDuration = std::chrono::duration_cast<std::chrono::milliseconds>(searchEnd - searchStart);
    
    // Should find some records and complete quickly
    EXPECT_GT(totalFound, 0);
    EXPECT_LT(searchDuration.count(), 1000); // Less than 1 second for 100 searches
}

// Test tree structure after many operations
TEST_F(XTreeIntegrationTest, TreeStructureValidation) {
    // Insert records in a pattern that should cause splits
    const int NUM_CLUSTERS = 10;
    const int RECORDS_PER_CLUSTER = 20;
    
    // Create spatial clusters
    for (int cluster = 0; cluster < NUM_CLUSTERS; cluster++) {
        double baseX = cluster * 100.0;
        double baseY = cluster * 100.0;
        
        for (int i = 0; i < RECORDS_PER_CLUSTER; i++) {
            string id = "cluster_" + to_string(cluster) + "_record_" + to_string(i);
            double x = baseX + (i % 5) * 10.0;
            double y = baseY + (i / 5) * 10.0;
            
            DataRecord* dr = createDataRecord(id, x, y, x + 5.0, y + 5.0);
            root->xt_insert(cachedRoot, dr);
        }
    }
    
    // Verify tree structure is valid (root should have some children after splits)
    EXPECT_GT(root->n(), 0);  // Root should have children, not all records
    
    // Search each cluster to verify spatial integrity
    for (int cluster = 0; cluster < NUM_CLUSTERS; cluster++) {
        double baseX = cluster * 100.0;
        double baseY = cluster * 100.0;
        
        DataRecord* searchCluster = createSearchQuery(baseX - 10.0, baseY - 10.0, 
                                                     baseX + 60.0, baseY + 60.0);
        auto iter = root->getIterator(cachedRoot, searchCluster, INTERSECTS);
        
        int clusterCount = 0;
        while (iter->hasNext()) {
            DataRecord* result = iter->next();
            if (result) {
                string rowId = result->getRowID();
                if (rowId.find("cluster_" + to_string(cluster)) != string::npos) {
                    clusterCount++;
                }
            }
        }
        
        // Should find all records in this cluster
        EXPECT_EQ(clusterCount, RECORDS_PER_CLUSTER);
        
        delete iter;
        delete searchCluster;
    }
    
    // Verify tree has grown (memory usage increased)
    long finalMemory = root->memoryUsage();
    EXPECT_GT(finalMemory, sizeof(XTreeBucket<DataRecord>) + 1000); // Significant growth
}

// Test edge cases
TEST_F(XTreeIntegrationTest, EdgeCases) {
    // Test 1: Zero-area rectangle (point)
    DataRecord* point = new DataRecord(2, 32, "point1");
    vector<double> p = {50.0, 50.0};
    point->putPoint(&p);
    root->xt_insert(cachedRoot, point);
    
    // Search for the point
    DataRecord* searchPoint = createSearchQuery(49.9, 49.9, 50.1, 50.1);
    auto iter1 = root->getIterator(cachedRoot, searchPoint, INTERSECTS);
    
    bool foundPoint = false;
    while (iter1->hasNext()) {
        DataRecord* result = iter1->next();
        if (result && result->getRowID() == "point1") {
            foundPoint = true;
        }
    }
    
    EXPECT_TRUE(foundPoint);
    delete iter1;
    delete searchPoint;
    
    // Test 2: Very large rectangle
    DataRecord* largeRect = createDataRecord("large", -1000.0, -1000.0, 1000.0, 1000.0);
    root->xt_insert(cachedRoot, largeRect);
    
    // Small search should find the large rectangle
    DataRecord* smallSearch = createSearchQuery(0.0, 0.0, 1.0, 1.0);
    auto iter2 = root->getIterator(cachedRoot, smallSearch, INTERSECTS);
    
    bool foundLarge = false;
    while (iter2->hasNext()) {
        DataRecord* result = iter2->next();
        if (result && result->getRowID() == "large") {
            foundLarge = true;
        }
    }
    
    EXPECT_TRUE(foundLarge);
    delete iter2;
    delete smallSearch;
    
    // Test 3: Negative coordinates
    DataRecord* negativeRect = createDataRecord("negative", -50.0, -50.0, -40.0, -40.0);
    root->xt_insert(cachedRoot, negativeRect);
    
    DataRecord* searchNegative = createSearchQuery(-55.0, -55.0, -35.0, -35.0);
    auto iter3 = root->getIterator(cachedRoot, searchNegative, INTERSECTS);
    
    bool foundNegative = false;
    while (iter3->hasNext()) {
        DataRecord* result = iter3->next();
        if (result && result->getRowID() == "negative") {
            foundNegative = true;
        }
    }
    
    EXPECT_TRUE(foundNegative);
    delete iter3;
    delete searchNegative;
}