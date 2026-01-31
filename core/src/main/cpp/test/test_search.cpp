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
#include <chrono>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/xtiter.h"

using namespace xtree;
using namespace std;

// Search-specific KeyMBR Intersection Tests
class KeyMBRSearchTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(KeyMBRSearchTest, NonOverlappingMBRs) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1: box from (0,0) to (10,10)
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (20,20) to (30,30)
    vector<double> p2_min = {20.0, 20.0};
    vector<double> p2_max = {30.0, 30.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_FALSE(mbr1.intersects(mbr2));
    EXPECT_FALSE(mbr2.intersects(mbr1));
}

TEST_F(KeyMBRSearchTest, OverlappingMBRs) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1: box from (0,0) to (10,10)
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (5,5) to (15,15) - overlaps with mbr1
    vector<double> p2_min = {5.0, 5.0};
    vector<double> p2_max = {15.0, 15.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_TRUE(mbr1.intersects(mbr2));
    EXPECT_TRUE(mbr2.intersects(mbr1));
}

TEST_F(KeyMBRSearchTest, ContainedMBRs) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1: box from (0,0) to (20,20)
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {20.0, 20.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (5,5) to (15,15) - contained within mbr1
    vector<double> p2_min = {5.0, 5.0};
    vector<double> p2_max = {15.0, 15.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_TRUE(mbr1.intersects(mbr2));
    EXPECT_TRUE(mbr2.intersects(mbr1));
}

TEST_F(KeyMBRSearchTest, EdgeTouchingMBRs) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1: box from (0,0) to (10,10)
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (10,0) to (20,10) - shares edge with mbr1
    vector<double> p2_min = {10.0, 0.0};
    vector<double> p2_max = {20.0, 10.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    // Edge touching should be considered an intersection
    EXPECT_TRUE(mbr1.intersects(mbr2));
    EXPECT_TRUE(mbr2.intersects(mbr1));
}

TEST_F(KeyMBRSearchTest, PointBoxIntersection) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1 as a point at (5,5)
    vector<double> point = {5.0, 5.0};
    mbr1.expandWithPoint(&point);
    
    // Set up mbr2: box from (0,0) to (10,10)
    vector<double> p2_min = {0.0, 0.0};
    vector<double> p2_max = {10.0, 10.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    // Point should intersect with box that contains it
    EXPECT_TRUE(mbr1.intersects(mbr2));
    EXPECT_TRUE(mbr2.intersects(mbr1));
}

// MBR Expansion Tests
TEST(MBRExpansionTest, ProgressiveExpansion) {
    KeyMBR mbr(2, 32);
    
    // Initial state should be inverted (max float, -max float)
    EXPECT_EQ(mbr.getMin(0), numeric_limits<float>::max());
    EXPECT_EQ(mbr.getMax(0), -numeric_limits<float>::max());
    
    // Add first point
    vector<double> p1 = {5.0, 5.0};
    mbr.expandWithPoint(&p1);
    
    EXPECT_EQ(mbr.getMin(0), 5.0);
    EXPECT_EQ(mbr.getMax(0), 5.0);
    EXPECT_EQ(mbr.getMin(1), 5.0);
    EXPECT_EQ(mbr.getMax(1), 5.0);
    
    // Add second point - should expand the MBR
    vector<double> p2 = {10.0, 3.0};
    mbr.expandWithPoint(&p2);
    
    EXPECT_EQ(mbr.getMin(0), 5.0);
    EXPECT_EQ(mbr.getMax(0), 10.0);
    EXPECT_EQ(mbr.getMin(1), 3.0);
    EXPECT_EQ(mbr.getMax(1), 5.0);
    
    // Add third point - some dimensions expand, some don't
    vector<double> p3 = {7.0, 8.0};
    mbr.expandWithPoint(&p3);
    
    EXPECT_EQ(mbr.getMin(0), 5.0);
    EXPECT_EQ(mbr.getMax(0), 10.0);
    EXPECT_EQ(mbr.getMin(1), 3.0);
    EXPECT_EQ(mbr.getMax(1), 8.0);
}

// DataRecord MBR Expansion Test
TEST(DataRecordExpansionTest, MultiplePoints) {
    DataRecord dr(2, 32, "test_row");
    
    // Add multiple points and verify MBR expands correctly
    vector<double> p1 = {1.0, 1.0};
    vector<double> p2 = {5.0, 2.0};
    vector<double> p3 = {3.0, 6.0};
    vector<double> p4 = {-1.0, 4.0};
    
    dr.putPoint(&p1);
    dr.putPoint(&p2);
    dr.putPoint(&p3);
    dr.putPoint(&p4);
    
    KeyMBR* mbr = dr.getKey();
    
    // Check that MBR encompasses all points
    EXPECT_EQ(mbr->getMin(0), -1.0);  // min x
    EXPECT_EQ(mbr->getMax(0), 5.0);   // max x
    EXPECT_EQ(mbr->getMin(1), 1.0);   // min y
    EXPECT_EQ(mbr->getMax(1), 6.0);   // max y
}

// 3D Intersection Tests
TEST(ThreeDimensionalTest, OverlappingMBRs3D) {
    KeyMBR mbr1(3, 32);
    KeyMBR mbr2(3, 32);
    
    // Set up mbr1: box from (0,0,0) to (10,10,10)
    vector<double> p1_min = {0.0, 0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (5,5,5) to (15,15,15) - overlaps with mbr1
    vector<double> p2_min = {5.0, 5.0, 5.0};
    vector<double> p2_max = {15.0, 15.0, 15.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_TRUE(mbr1.intersects(mbr2));
    EXPECT_TRUE(mbr2.intersects(mbr1));
}

TEST(ThreeDimensionalTest, NonOverlappingMBRs3D) {
    KeyMBR mbr1(3, 32);
    KeyMBR mbr2(3, 32);
    
    // Set up mbr1: box from (0,0,0) to (10,10,10)
    vector<double> p1_min = {0.0, 0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (20,20,20) to (30,30,30) - no overlap
    vector<double> p2_min = {20.0, 20.0, 20.0};
    vector<double> p2_max = {30.0, 30.0, 30.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_FALSE(mbr1.intersects(mbr2));
    EXPECT_FALSE(mbr2.intersects(mbr1));
}

// Tree Search Tests
class TreeSearchTest : public ::testing::Test {
protected:
    IndexDetails<DataRecord>* idx;
    XTreeBucket<DataRecord>* root;
    LRUCacheNode<IRecord, UniqueId, LRUDeleteObject>* cachedRoot;
    vector<const char*>* dimLabels;
    
    void SetUp() override {
        // Create index
        dimLabels = new vector<const char*>();
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        idx = new IndexDetails<DataRecord>(2, 32, dimLabels, nullptr, nullptr, "test_search");
        
        // Create root bucket
        root = new XTreeBucket<DataRecord>(idx, true, nullptr, nullptr, 0, true, 0);
        
        // For testing, we create a fake cache node that points to our root
        // but isn't actually in the cache. This avoids memory leaks from the
        // static cache persisting between tests.
        cachedRoot = new LRUCacheNode<IRecord, UniqueId, LRUDeleteObject>(
            idx->getNextNodeID(), static_cast<IRecord*>(root), nullptr);
    }
    
    void TearDown() override {
        // Delete the fake cache node (which doesn't delete root since we're managing it)
        cachedRoot->object = nullptr; // Prevent the cache node from deleting root
        delete cachedRoot;
        // Now delete root manually
        delete root;
        delete idx;
        delete dimLabels;
        
        // Clear the static cache to prevent memory leaks from splitRoot operations
        // This is important because splitRoot adds new buckets to the real cache
        IndexDetails<DataRecord>::clearCache();
    }
};

TEST_F(TreeSearchTest, MultipleRecordInsertion) {
    // Insert several data records with different spatial locations
    vector<pair<string, vector<vector<double>>>> testData = {
        {"row1", {{0.0, 0.0}, {2.0, 2.0}}},      // Bottom-left region
        {"row2", {{8.0, 8.0}, {10.0, 10.0}}},    // Top-right region
        {"row3", {{4.0, 4.0}, {6.0, 6.0}}},      // Center region
        {"row4", {{0.0, 8.0}, {2.0, 10.0}}},     // Top-left region
        {"row5", {{8.0, 0.0}, {10.0, 2.0}}}      // Bottom-right region
    };
    
    for (const auto& data : testData) {
        DataRecord* dr = new DataRecord(2, 32, data.first);
        for (const auto& point : data.second) {
            auto p = point;  // Make a copy to avoid const issues
            dr->putPoint(&p);
        }
        root->xt_insert(cachedRoot, dr);
    }
    
    EXPECT_EQ(root->n(), testData.size());
}

TEST_F(TreeSearchTest, SearchBottomLeftQuadrant) {
    // Insert test data
    vector<pair<string, vector<vector<double>>>> testData = {
        {"row1", {{0.0, 0.0}, {2.0, 2.0}}},      // Bottom-left region
        {"row2", {{8.0, 8.0}, {10.0, 10.0}}},    // Top-right region
        {"row3", {{4.0, 4.0}, {6.0, 6.0}}}       // Center region
    };
    
    for (const auto& data : testData) {
        DataRecord* dr = new DataRecord(2, 32, data.first);
        for (const auto& point : data.second) {
            auto p = point;
            dr->putPoint(&p);
        }
        root->xt_insert(cachedRoot, dr);
    }
    
    // Search for records in bottom-left quadrant
    DataRecord* searchRecord = new DataRecord(2, 32, "search1");
    vector<double> searchMin = {-1.0, -1.0};
    vector<double> searchMax = {3.0, 3.0};
    searchRecord->putPoint(&searchMin);
    searchRecord->putPoint(&searchMax);
    
    auto iter = root->getIterator(cachedRoot, searchRecord, INTERSECTS);
    
    int count = 0;
    set<string> foundRows;
    std::string_view rid;
    while (iter->nextRowID(rid)) {
        count++;
        foundRows.insert(std::string(rid));
    }
    
    EXPECT_GE(count, 1);  // Should find at least row1
    EXPECT_TRUE(foundRows.count("row1") > 0);
    
    delete iter;
    delete searchRecord;
}

TEST_F(TreeSearchTest, SearchAllRecords) {
    // Insert test data
    vector<pair<string, vector<vector<double>>>> testData = {
        {"row1", {{0.0, 0.0}, {2.0, 2.0}}},
        {"row2", {{8.0, 8.0}, {10.0, 10.0}}},
        {"row3", {{4.0, 4.0}, {6.0, 6.0}}}
    };
    
    for (const auto& data : testData) {
        DataRecord* dr = new DataRecord(2, 32, data.first);
        for (const auto& point : data.second) {
            auto p = point;
            dr->putPoint(&p);
        }
        root->xt_insert(cachedRoot, dr);
    }
    
    // Search with large bounding box
    DataRecord* searchRecord = new DataRecord(2, 32, "search2");
    vector<double> searchMin = {-100.0, -100.0};
    vector<double> searchMax = {100.0, 100.0};
    searchRecord->putPoint(&searchMin);
    searchRecord->putPoint(&searchMax);
    
    auto iter = root->getIterator(cachedRoot, searchRecord, INTERSECTS);
    
    int count = 0;
    while (iter->nextData()) {
        count++;
    }
    
    EXPECT_EQ(count, testData.size());  // Should find all records
    
    delete iter;
    delete searchRecord;
}

TEST_F(TreeSearchTest, SearchNoRecords) {
    // Insert test data
    vector<pair<string, vector<vector<double>>>> testData = {
        {"row1", {{0.0, 0.0}, {2.0, 2.0}}},
        {"row2", {{8.0, 8.0}, {10.0, 10.0}}}
    };
    
    for (const auto& data : testData) {
        DataRecord* dr = new DataRecord(2, 32, data.first);
        for (const auto& point : data.second) {
            auto p = point;
            dr->putPoint(&p);
        }
        root->xt_insert(cachedRoot, dr);
    }
    
    // Search with non-overlapping box
    DataRecord* searchRecord = new DataRecord(2, 32, "search3");
    vector<double> searchMin = {20.0, 20.0};
    vector<double> searchMax = {30.0, 30.0};
    searchRecord->putPoint(&searchMin);
    searchRecord->putPoint(&searchMax);
    
    auto iter = root->getIterator(cachedRoot, searchRecord, INTERSECTS);
    
    int count = 0;
    while (iter->nextData()) {
        count++;
    }
    
    EXPECT_EQ(count, 0);  // Should find no records
    
    delete iter;
    delete searchRecord;
}

// Performance Tests
TEST(IntersectionPerformanceTest, HighVolumeIntersectionChecks) {
    const int NUM_ITERATIONS = 100000;
    
    // Create two MBRs
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    vector<double> p2_min = {5.0, 5.0};
    vector<double> p2_max = {15.0, 15.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    bool result = false;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        result = mbr1.intersects(mbr2);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double avgTime = static_cast<double>(duration.count()) / NUM_ITERATIONS;
    
    EXPECT_TRUE(result);  // Sanity check
    EXPECT_LT(avgTime, 1.0);  // Should be very fast (less than 1μs per check)
}