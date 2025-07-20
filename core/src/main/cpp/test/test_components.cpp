/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <algorithm>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.h"

using namespace xtree;
using namespace std;

// Initialize static members of IndexDetails (only once per test binary)
template<> JNIEnv* IndexDetails<DataRecord>::jvm = nullptr;
template<> LRUCache<IRecord, UniqueId, LRUDeleteObject> IndexDetails<DataRecord>::cache(1024*1024*10); // 10MB cache

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
}

TEST_F(IndexDetailsTest, NodeIDGeneration) {
    vector<const char*>* dimLabels = new vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    
    IndexDetails<DataRecord> idx(2, 32, dimLabels, 1024*1024, nullptr, nullptr);
    
    UniqueId id1 = idx.getNextNodeID();
    UniqueId id2 = idx.getNextNodeID();
    EXPECT_EQ(id2, id1 + 1);
}

// XTreeBucket Tests
class XTreeBucketTest : public ::testing::Test {
protected:
    IndexDetails<DataRecord>* idx;
    
    void SetUp() override {
        vector<const char*>* dimLabels = new vector<const char*>();
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        idx = new IndexDetails<DataRecord>(2, 32, dimLabels, 1024*1024, nullptr, nullptr);
    }
    
    void TearDown() override {
        delete idx;
    }
};

TEST_F(XTreeBucketTest, Creation) {
    XTreeBucket<DataRecord> bucket(idx, true, nullptr, nullptr, 0, true, 0);
    
    EXPECT_EQ(bucket.n(), 0);
    // isDataNode() is protected, but we know buckets are not data nodes
    // EXPECT_FALSE(bucket.isDataNode());
    EXPECT_NE(bucket.getKey(), nullptr);
    EXPECT_EQ(bucket.getIdxDetails(), idx);
}

TEST_F(XTreeBucketTest, MemoryUsage) {
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