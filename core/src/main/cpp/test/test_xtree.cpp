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
#include <cmath>
#include <vector>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.h"

// #include "xtiter.hpp"

using namespace xtree;
using namespace std;
using ::testing::Return;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

// Define a mock Record to insert into the XTree
class MockRecord : public IRecord {
public:
    MOCK_METHOD(const bool, isLeaf, (), (const, override));
    MOCK_METHOD(const bool, isDataNode, (), (const, override));
    MOCK_METHOD(KeyMBR*, getKey, (), (const, override));
    MOCK_METHOD(long, memoryUsage, (), (const, override));
    MOCK_METHOD(void, purge, (), (override));
};

// Initialize static members of IndexDetails for MockRecord
template<> JNIEnv* IndexDetails<MockRecord>::jvm = nullptr;
template<> std::vector<IndexDetails<MockRecord>*> IndexDetails<MockRecord>::indexes = std::vector<IndexDetails<MockRecord>*>();
template<> LRUCache<IRecord, UniqueId, LRUDeleteNone> IndexDetails<MockRecord>::cache(1024*1024*10); // 10MB cache

class TestableXTreeBucket : public XTreeBucket<MockRecord> {
public:
    using XTreeBucket<MockRecord>::XTreeBucket;
    using XTreeBucket<MockRecord>::_insert;  // Expose protected method for testing
};

// Minimal test fixture
class XTreeBucketTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup a default IndexDetails and root
        std::vector<const char*> dummyFields = {};
        index = new IndexDetails<MockRecord>(2, 4, &dummyFields, 1024L * 1024L, nullptr, nullptr);
        root = new TestableXTreeBucket(index, true);
        
        // For testing, we create a fake cache node that points to our root
        // but isn't actually in the cache. This avoids memory leaks from the
        // static cache persisting between tests.
        cachedRoot = new LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>(
            index->getNextNodeID(), static_cast<IRecord*>(root), nullptr);
    }

    void TearDown() override {
        // Delete the fake cache node (which doesn't delete root since we're managing it)
        cachedRoot->object = nullptr; // Prevent the cache node from deleting root
        delete cachedRoot;
        // Now delete root manually
        delete root;
        delete index;
        
        // Clear the static cache to prevent memory leaks from splitRoot operations
        // This is important because splitRoot adds new buckets to the real cache
        IndexDetails<MockRecord>::clearCache();
    }

    IndexDetails<MockRecord>* index;
    TestableXTreeBucket* root;
    CacheNode* cachedRoot;
};


TEST_F(XTreeBucketTest, MockBucketCreation) {
    EXPECT_NE(root, nullptr);
    EXPECT_EQ(root->n(), 0);
    // isLeaf() is const, so we know it's created as a leaf (default)
    EXPECT_NE(root->getKey(), nullptr);
    EXPECT_EQ(root->getIdxDetails(), index);
}

TEST_F(XTreeBucketTest, MockBucketInsertion) {
    // First verify the bucket was created properly
    ASSERT_NE(root, nullptr);
    EXPECT_EQ(root->n(), 0);
    
    // Create a mock record with proper expectations
    auto* mock = new MockRecord();
    auto* mockKey = new KeyMBR(2, 32);
    
    // Expand the key to have actual bounds
    vector<double> minPoint = {0.0, 0.0};
    vector<double> maxPoint = {10.0, 10.0};
    mockKey->expandWithPoint(&minPoint);
    mockKey->expandWithPoint(&maxPoint);
    
    // Set up expectations - these calls will happen during insertion
    EXPECT_CALL(*mock, getKey())
        .WillRepeatedly(Return(mockKey));
    EXPECT_CALL(*mock, isLeaf())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mock, isDataNode())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mock, memoryUsage())
        .WillRepeatedly(Return(100L));
    
    // For testing with mocks, we need to create a fake cache node manually
    // instead of using xt_insert which would add the mock to the real cache
    auto* cachedMock = new LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>(
        index->getNextNodeID(), static_cast<IRecord*>(mock), nullptr);
    
    // Use the internal _insert method directly
    root->_insert(cachedRoot, cachedMock);
    
    // Verify insertion
    EXPECT_EQ(root->n(), 1);
    
    // Clean up: prevent the cache node from deleting the mock
    cachedMock->object = nullptr;
    delete cachedMock;
    delete mockKey;
    delete mock;
}

TEST_F(XTreeBucketTest, MockMultipleInsertions) {
    const int NUM_RECORDS = 5;
    vector<MockRecord*> mocks;
    vector<KeyMBR*> mockKeys;
    vector<CacheNode*> cachedMocks;
    
    for (int i = 0; i < NUM_RECORDS; ++i) {
        auto* mock = new MockRecord();
        auto* mockKey = new KeyMBR(2, 32);
        mocks.push_back(mock);
        mockKeys.push_back(mockKey);
        
        // Expand the key to have different bounds for each record
        vector<double> minPoint = {i * 10.0, i * 10.0};
        vector<double> maxPoint = {(i + 1) * 10.0, (i + 1) * 10.0};
        mockKey->expandWithPoint(&minPoint);
        mockKey->expandWithPoint(&maxPoint);
        
        // Set up expectations
        EXPECT_CALL(*mock, getKey())
            .WillRepeatedly(Return(mockKey));
        EXPECT_CALL(*mock, isLeaf())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*mock, isDataNode())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*mock, memoryUsage())
            .WillRepeatedly(Return(100L));
        
        // Create fake cache node for testing
        auto* cachedMock = new LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>(
            index->getNextNodeID(), static_cast<IRecord*>(mock), nullptr);
        cachedMocks.push_back(cachedMock);
        
        // Insert using _insert directly
        root->_insert(cachedRoot, cachedMock);
    }
    
    EXPECT_EQ(root->n(), NUM_RECORDS);
    
    // Clean up
    for (auto* cachedMock : cachedMocks) {
        cachedMock->object = nullptr;  // Prevent cache node from deleting mock
        delete cachedMock;
    }
    for (auto* mockKey : mockKeys) {
        delete mockKey;
    }
    for (auto* mock : mocks) {
        delete mock;
    }
}

TEST_F(XTreeBucketTest, MockInsertionWithSplitScenario) {
    // Insert enough records to potentially trigger a split
    const int LARGE_NUM_RECORDS = 50;  // Should be > XTREE_M
    vector<MockRecord*> mocks;
    vector<KeyMBR*> mockKeys;
    vector<CacheNode*> cachedMocks;
    
    for (int i = 0; i < LARGE_NUM_RECORDS; ++i) {
        auto* mock = new MockRecord();
        auto* mockKey = new KeyMBR(2, 32);
        mocks.push_back(mock);
        mockKeys.push_back(mockKey);
        
        // Create spatially distributed points
        double angle = (2.0 * M_PI * i) / LARGE_NUM_RECORDS;
        vector<double> point = {cos(angle) * 100, sin(angle) * 100};
        mockKey->expandWithPoint(&point);
        
        EXPECT_CALL(*mock, getKey())
            .WillRepeatedly(Return(mockKey));
        EXPECT_CALL(*mock, isLeaf())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*mock, isDataNode())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*mock, memoryUsage())
            .WillRepeatedly(Return(100L));
        
        // Create fake cache node for testing
        auto* cachedMock = new LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>(
            index->getNextNodeID(), static_cast<IRecord*>(mock), nullptr);
        cachedMocks.push_back(cachedMock);
        
        // Insert using _insert directly
        root->_insert(cachedRoot, cachedMock);
    }
    
    // After many insertions, the tree structure should have grown
    EXPECT_GE(root->n(), 1);  // Root should have at least one child
    EXPECT_GT(root->memoryUsage(), sizeof(XTreeBucket<MockRecord>));
    
    // Clean up
    for (auto* cachedMock : cachedMocks) {
        cachedMock->object = nullptr;  // Prevent cache node from deleting mock
        delete cachedMock;
    }
    for (auto* mockKey : mockKeys) {
        delete mockKey;
    }
    for (auto* mock : mocks) {
        delete mock;
    }
}