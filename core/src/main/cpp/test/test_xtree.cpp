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
#include <cmath>
#include <vector>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"

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
    
    // Stub for persistence - MockRecord only used in IN_MEMORY tests
    persist::NodeID getNodeID() const {
        return persist::NodeID::invalid(); // Always invalid for mock
    }
};

// Initialize static members of IndexDetails for MockRecord
template<> JNIEnv* IndexDetails<MockRecord>::jvm = nullptr;
template<> std::vector<IndexDetails<MockRecord>*> IndexDetails<MockRecord>::indexes = std::vector<IndexDetails<MockRecord>*>();

// Provide a minimal implementation of initializeDurableStore for MockRecord
// This is only used in tests and should never be called since we use IN_MEMORY mode
template<>
void IndexDetails<MockRecord>::initializeDurableStore(const std::string& data_dir) {
    // Should never be called for test MockRecord which uses IN_MEMORY mode
    throw std::runtime_error("MockRecord should only use IN_MEMORY mode");
}

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
        index = new IndexDetails<MockRecord>(2, 4, &dummyFields, nullptr, nullptr, "test_xtree");
        root = new TestableXTreeBucket(index, true);
        
        // Use the real cache system instead of fake nodes
        // This ensures splits work correctly in IN_MEMORY mode
        auto& cache = index->getCache();
        cachedRoot = cache.add(index->getNextNodeID(), static_cast<IRecord*>(root));
    }

    void TearDown() override {
        // Clear the cache first (which will handle the nodes)
        // This is important because splitRoot adds new buckets to the real cache
        IndexDetails<MockRecord>::clearCache();
        
        // Don't manually delete root since it's managed by the cache
        // Just delete the index
        delete index;
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
    
    // Add the mock to the real cache for proper split handling
    auto& cache = index->getCache();
    auto* cachedMock = cache.add(index->getNextNodeID(), static_cast<IRecord*>(mock));
    cache.pin(cachedMock, cachedMock->id);  // _insert requires pinned nodes

    // Use the internal _insert method directly
    root->_insert(cachedRoot, cachedMock);

    cache.unpin(cachedMock, cachedMock->id);  // Release pin after insertion
    
    // Verify insertion
    EXPECT_EQ(root->n(), 1);
    
    // Clean up: cache manages the nodes now
    // Just clean up the mock objects themselves
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
        
        // Add to real cache for proper handling
        auto& cache = index->getCache();
        auto* cachedMock = cache.add(index->getNextNodeID(), static_cast<IRecord*>(mock));
        cache.pin(cachedMock, cachedMock->id);  // _insert requires pinned nodes
        cachedMocks.push_back(cachedMock);

        // Insert using _insert directly
        root->_insert(cachedRoot, cachedMock);

        cache.unpin(cachedMock, cachedMock->id);  // Release pin after insertion
    }

    EXPECT_EQ(root->n(), NUM_RECORDS);

    // Clean up
    // Cache manages the nodes - no need to delete them
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
        
        // Add to real cache for proper handling
        auto& cache = index->getCache();
        auto* cachedMock = cache.add(index->getNextNodeID(), static_cast<IRecord*>(mock));
        cache.pin(cachedMock, cachedMock->id);  // _insert requires pinned nodes
        cachedMocks.push_back(cachedMock);

        // Insert using _insert directly
        root->_insert(cachedRoot, cachedMock);

        cache.unpin(cachedMock, cachedMock->id);  // Release pin after insertion
    }

    // After many insertions, the tree structure should have grown
    EXPECT_GE(root->n(), 1);  // Root should have at least one child
    EXPECT_GT(root->memoryUsage(), sizeof(XTreeBucket<MockRecord>));
    
    // Clean up
    // Cache manages the nodes - no need to delete them
    for (auto* mockKey : mockKeys) {
        delete mockKey;
    }
    for (auto* mock : mocks) {
        delete mock;
    }
}