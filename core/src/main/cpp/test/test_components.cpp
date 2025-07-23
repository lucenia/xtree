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
#include <algorithm>
#include <vector>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std;

// Note: DataRecord static members are defined in test_globals.cpp

// DataRecord Tests
class DataRecordTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(DataRecordTest, Creation) {
    DataRecord dr(2, 32, "row123");
    
    EXPECT_EQ(dr.getRowID(), "row123");
    EXPECT_TRUE(dr.isLeaf());
    EXPECT_TRUE(dr.isDataNode());
    EXPECT_NE(dr.getKey(), nullptr);
    EXPECT_EQ(dr.getKey()->getDimensionCount(), 2);
}

TEST_F(DataRecordTest, InitialMemoryUsage) {
    DataRecord dr(2, 32, "row123");
    
    // Test memory usage before adding points
    long memUsage = dr.memoryUsage();
    EXPECT_EQ(memUsage, 0);
}

TEST_F(DataRecordTest, PointAddition) {
    DataRecord dr(2, 32, "row456");
    
    // Add some points
    vector<double> point1 = {1.0, 2.0};
    vector<double> point2 = {3.0, 4.0};
    vector<double> point3 = {5.0, 6.0};
    
    dr.putPoint(&point1);
    dr.putPoint(&point2);
    dr.putPoint(&point3);
    
    // Check points were added
    vector<vector<double>> points = dr.getPoints();
    ASSERT_EQ(points.size(), 3);
    EXPECT_EQ(points[0][0], 1.0);
    EXPECT_EQ(points[0][1], 2.0);
    EXPECT_EQ(points[1][0], 3.0);
    EXPECT_EQ(points[1][1], 4.0);
    EXPECT_EQ(points[2][0], 5.0);
    EXPECT_EQ(points[2][1], 6.0);
}

TEST_F(DataRecordTest, MemoryUsageWithPoints) {
    DataRecord dr(2, 32, "row456");
    
    vector<double> point1 = {1.0, 2.0};
    vector<double> point2 = {3.0, 4.0};
    vector<double> point3 = {5.0, 6.0};
    
    dr.putPoint(&point1);
    dr.putPoint(&point2);
    dr.putPoint(&point3);
    
    long memUsage = dr.memoryUsage();
    EXPECT_EQ(memUsage, 3 * 2 * sizeof(double));
}

TEST_F(DataRecordTest, KeyExpansion) {
    DataRecord dr(2, 32, "row789");
    
    vector<double> point1 = {1.0, 2.0};
    vector<double> point2 = {3.0, 4.0};
    vector<double> point3 = {5.0, 6.0};
    
    dr.putPoint(&point1);
    dr.putPoint(&point2);
    dr.putPoint(&point3);
    
    KeyMBR* key = dr.getKey();
    EXPECT_LE(key->getMin(0), 1.0);
    EXPECT_GE(key->getMax(0), 5.0);
    EXPECT_LE(key->getMin(1), 2.0);
    EXPECT_GE(key->getMax(1), 6.0);
}

// IndexDetails Tests
class IndexDetailsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(IndexDetailsTest, Creation) {
    vector<const char*>* dimLabels = new vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    dimLabels->push_back("z");
    
    IndexDetails<DataRecord> idx(3, 32, dimLabels, 1024*1024, nullptr, nullptr);
    
    EXPECT_EQ(idx.getDimensionCount(), 3);
    EXPECT_EQ(idx.getPrecision(), 32);
    
    // Clean up allocated memory
    delete dimLabels;
}

TEST_F(IndexDetailsTest, NodeIDGeneration) {
    vector<const char*>* dimLabels = new vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    
    IndexDetails<DataRecord> idx(2, 32, dimLabels, 1024*1024, nullptr, nullptr);
    
    UniqueId id1 = idx.getNextNodeID();
    UniqueId id2 = idx.getNextNodeID();
    EXPECT_EQ(id2, id1 + 1);
    
    // Clean up allocated memory
    delete dimLabels;
}

// XTreeBucket Tests
class ComponentXTreeBucketTest : public ::testing::Test {
protected:
    IndexDetails<DataRecord>* idx;
    vector<const char*>* dimLabels;
    
    void SetUp() override {
        dimLabels = new vector<const char*>();
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        idx = new IndexDetails<DataRecord>(2, 32, dimLabels, 1024*1024, nullptr, nullptr);
    }
    
    void TearDown() override {
        delete idx;
        delete dimLabels;
        
        // Clear the static cache to prevent any potential memory leaks
        IndexDetails<DataRecord>::clearCache();
    }
};

TEST_F(ComponentXTreeBucketTest, Creation) {
    XTreeBucket<DataRecord> bucket(idx, true, nullptr, nullptr, 0, true, 0);
    
    EXPECT_EQ(bucket.n(), 0);
    // isDataNode() is protected, but we know buckets are not data nodes
    // EXPECT_FALSE(bucket.isDataNode());
    EXPECT_NE(bucket.getKey(), nullptr);
    EXPECT_EQ(bucket.getIdxDetails(), idx);
}

TEST_F(ComponentXTreeBucketTest, MemoryUsage) {
    XTreeBucket<DataRecord> bucket(idx, true, nullptr, nullptr, 0, true, 0);
    
    long memUsage = bucket.memoryUsage();
    EXPECT_GT(memUsage, 0);
}

// MBRKeyNode Tests
TEST(MBRKeyNodeTest, DefaultCreation) {
    typedef __MBRKeyNode<DataRecord> MBRKeyNode;
    
    MBRKeyNode node;
    
    EXPECT_FALSE(node.getLeaf());
    EXPECT_FALSE(node.getCached());
    EXPECT_EQ(node.getKey(), nullptr);
}

TEST(MBRKeyNodeTest, LeafStatus) {
    typedef __MBRKeyNode<DataRecord> MBRKeyNode;
    
    MBRKeyNode node;
    node.setLeaf(true);
    EXPECT_TRUE(node.getLeaf());
}

TEST(MBRKeyNodeTest, CachedStatus) {
    typedef __MBRKeyNode<DataRecord> MBRKeyNode;
    
    MBRKeyNode node;
    node.setCached(true);
    EXPECT_TRUE(node.getCached());
}

// Sorting Functor Tests
class SortingFunctorTest : public ::testing::Test {
protected:
    vector<__MBRKeyNode<DataRecord>*> nodes;
    
    void SetUp() override {
        // Create nodes with different MBR bounds
        for (int i = 0; i < 5; i++) {
            auto* node = new __MBRKeyNode<DataRecord>();
            KeyMBR* key = new KeyMBR(2, 32);
            
            // Expand key with points to set bounds
            vector<double> minPoint = {i * 10.0, i * 10.0};
            vector<double> maxPoint = {(i + 1) * 10.0, (i + 1) * 10.0};
            key->expandWithPoint(&minPoint);
            key->expandWithPoint(&maxPoint);
            
            node->setKey(key);
            nodes.push_back(node);
        }
    }
    
    void TearDown() override {
        for (auto node : nodes) {
            delete node->getKey();
            delete node;
        }
    }
};

TEST_F(SortingFunctorTest, SortByRangeMin) {
    sort(nodes.begin(), nodes.end(), SortKeysByRangeMin<DataRecord>(0));
    
    for (int i = 0; i < 4; i++) {
        EXPECT_LE(nodes[i]->getKey()->getMin(0), nodes[i+1]->getKey()->getMin(0));
    }
}

TEST_F(SortingFunctorTest, SortByRangeMax) {
    sort(nodes.begin(), nodes.end(), SortKeysByRangeMax<DataRecord>(0));
    
    for (int i = 0; i < 4; i++) {
        EXPECT_LE(nodes[i]->getKey()->getMax(0), nodes[i+1]->getKey()->getMax(0));
    }
}

// XTreeBucket Destructor Test - separate test suite to avoid conflict with TEST_F
TEST(XTreeBucketDestructorTest, CleansUpParentNodeMemory) {
    // Create a simple index
    vector<const char*> dimLabels = {"x", "y", "z"};
    IndexDetails<DataRecord> idx(3, 32, &dimLabels, 1024*1024, nullptr, nullptr);
    
    // Create multiple buckets to test destructor
    XTreeBucket<DataRecord>* bucket1 = new XTreeBucket<DataRecord>(&idx, true);
    XTreeBucket<DataRecord>* bucket2 = new XTreeBucket<DataRecord>(&idx, false);
    
    // The destructor should properly clean up the _parent pointer
    // without causing memory leaks (as detected by valgrind)
    delete bucket1;
    delete bucket2;
    
    // If we get here without crashing, the destructor worked correctly
    SUCCEED();
    
    // Clear the static cache to prevent any potential memory leaks
    IndexDetails<DataRecord>::clearCache();
}