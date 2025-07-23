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
#include <thread>
#include <chrono>
#include "../src/indexdetails.hpp"
#include "../src/xtree.h"
#include "../src/xtree.hpp"

using namespace xtree;
using namespace std;

class COWIntegrationTest : public ::testing::Test {
protected:
    vector<const char*>* dimLabels;
    
    void SetUp() override {
        dimLabels = new vector<const char*>;
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        // Clean up any existing snapshot files
        remove("test_cow.snapshot");
        remove("test_cow.snapshot.tmp");
    }
    
    void TearDown() override {
        delete dimLabels;
        
        // Clean up after tests
        remove("test_cow.snapshot");
        remove("test_cow.snapshot.tmp");
    }
};

TEST_F(COWIntegrationTest, BasicCOWFunctionality) {
    // Create COW-enabled index
    IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
        2, 32, dimLabels, 1024*1024*10, nullptr, nullptr, 
        true, "test_cow.snapshot"  // Enable COW
    );
    
    ASSERT_TRUE(index->hasCOWManager());
    ASSERT_NE(index->getCOWManager(), nullptr);
    ASSERT_NE(index->getCOWAllocator(), nullptr);
    
    // Configure COW manager
    index->getCOWManager()->set_operations_threshold(100);
    
    // Create root using COW allocation
    XTreeBucket<DataRecord>* root = nullptr;
    if (index->hasCOWManager()) {
        root = index->getCOWAllocator()->allocate_bucket(index, true);
    } else {
        root = new XTreeBucket<DataRecord>(index, true);
    }
    ASSERT_NE(root, nullptr);
    
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress(reinterpret_cast<long>(cachedRoot));
    
    // Insert enough records to trigger a snapshot
    for (int i = 0; i < 150; i++) {
        DataRecord* dr = new DataRecord(2, 32, "rec_" + to_string(i));
        vector<double> point = {(double)i, (double)i * 2};
        dr->putPoint(&point);
        
        root->xt_insert(cachedRoot, dr);
    }
    
    // Give background thread time to create snapshot
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Check that snapshot was created
    FILE* fp = fopen("test_cow.snapshot", "r");
    EXPECT_NE(fp, nullptr) << "Snapshot file should exist";
    if (fp) fclose(fp);
    
    // Verify stats
    auto stats = index->getCOWManager()->get_stats();
    EXPECT_GT(stats.tracked_memory_bytes, 0);
    EXPECT_FALSE(stats.cow_protection_active);  // Should be done by now
    
    delete index;
}

TEST_F(COWIntegrationTest, CompareWithAndWithoutCOW) {
    const int NUM_RECORDS = 1000;
    
    // Test 1: Without COW - create index outside timing
    IndexDetails<DataRecord>* idx1 = new IndexDetails<DataRecord>(
        2, 32, dimLabels, 1024*1024*10, nullptr, nullptr);
    
    XTreeBucket<DataRecord>* root1 = new XTreeBucket<DataRecord>(idx1, true);
    auto* cachedRoot1 = idx1->getCache().add(idx1->getNextNodeID(), root1);
    idx1->setRootAddress(reinterpret_cast<long>(cachedRoot1));
    
    // Time only the insertions
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_RECORDS; i++) {
        DataRecord* dr = new DataRecord(2, 32, "rec_" + to_string(i));
        vector<double> point = {(double)i, (double)i};
        dr->putPoint(&point);
        root1->xt_insert(cachedRoot1, dr);
    }
    auto time_without_cow = std::chrono::high_resolution_clock::now() - start;
    
    // Clean up
    idx1->clearCache();
    delete idx1;
    
    // Test 2: With COW - create index outside timing
    IndexDetails<DataRecord>* idx2 = new IndexDetails<DataRecord>(
        2, 32, dimLabels, 1024*1024*10, nullptr, nullptr,
        true, "test_cow.snapshot");  // Enable COW
    
    // Don't trigger snapshots during test
    idx2->getCOWManager()->set_operations_threshold(NUM_RECORDS + 1);
    
    XTreeBucket<DataRecord>* root2 = idx2->getCOWAllocator()->allocate_bucket(idx2, true);
    auto* cachedRoot2 = idx2->getCache().add(idx2->getNextNodeID(), root2);
    idx2->setRootAddress(reinterpret_cast<long>(cachedRoot2));
    
    // Time only the insertions
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_RECORDS; i++) {
        DataRecord* dr = new DataRecord(2, 32, "rec_" + to_string(i));
        vector<double> point = {(double)i, (double)i};
        dr->putPoint(&point);
        root2->xt_insert(cachedRoot2, dr);
    }
    auto time_with_cow = std::chrono::high_resolution_clock::now() - start;
    
    // Clean up
    idx2->clearCache();
    delete idx2;
    
    // Calculate overhead
    auto us_without = std::chrono::duration_cast<std::chrono::microseconds>(time_without_cow).count();
    auto us_with = std::chrono::duration_cast<std::chrono::microseconds>(time_with_cow).count();
    
    double overhead_percent = ((double)(us_with - us_without) / us_without) * 100.0;
    
    cout << "Performance comparison for " << NUM_RECORDS << " operations:\n";
    cout << "  Without COW: " << us_without << " us\n";
    cout << "  With COW: " << us_with << " us\n";
    cout << "  Overhead: " << overhead_percent << "%\n";
    
    // COW overhead should be reasonable
    EXPECT_LT(overhead_percent, 10.0) << "COW overhead should be less than 10%";
}

TEST_F(COWIntegrationTest, SnapshotValidation) {
    // Create index with COW
    IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
        2, 32, dimLabels, 1024*1024*10, nullptr, nullptr,
        true, "test_cow.snapshot");
    
    index->getCOWManager()->set_operations_threshold(50);
    
    XTreeBucket<DataRecord>* root = index->getCOWAllocator()->allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress(reinterpret_cast<long>(cachedRoot));
    
    // Insert data
    for (int i = 0; i < 100; i++) {
        DataRecord* dr = new DataRecord(2, 32, "rec_" + to_string(i));
        vector<double> point = {(double)i, (double)i};
        dr->putPoint(&point);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Wait for snapshot
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Validate snapshot
    EXPECT_TRUE(index->getCOWManager()->validate_snapshot("test_cow.snapshot"));
    
    // Check snapshot header
    auto header = index->getCOWManager()->get_snapshot_header("test_cow.snapshot");
    EXPECT_EQ(header.magic, COW_SNAPSHOT_MAGIC);
    EXPECT_EQ(header.version, COW_SNAPSHOT_VERSION);
    EXPECT_EQ(header.dimension, 2);
    EXPECT_EQ(header.precision, 32);
    EXPECT_GT(header.total_regions, 0u);
    EXPECT_GT(header.total_size, 0u);
    
    index->clearCache();
    delete index;
}

TEST_F(COWIntegrationTest, AllocatorUsage) {
    // Test that the allocator traits work correctly
    
    // Without COW
    {
        IndexDetails<DataRecord> index(2, 32, dimLabels, 1024*1024, nullptr, nullptr);
        
        // Should use standard allocation path
        XTreeBucket<DataRecord>* bucket = XAlloc<DataRecord>::allocate_bucket(
            &index, false);
        EXPECT_NE(bucket, nullptr);
        delete bucket;
    }
    
    // With COW
    {
        IndexDetails<DataRecord> index(2, 32, dimLabels, 1024*1024, nullptr, nullptr,
                                     true, "test_cow.snapshot");
        
        // Should use COW allocation path
        XTreeBucket<DataRecord>* bucket = XAlloc<DataRecord>::allocate_bucket(
            &index, false);
        EXPECT_NE(bucket, nullptr);
        
        // Should be tracked by COW manager
        auto stats = index.getCOWManager()->get_stats();
        EXPECT_GT(stats.tracked_memory_bytes, 0);
        
        // Clean up
        if (index.hasCOWManager()) {
            index.getCOWAllocator()->deallocate(bucket);
        } else {
            delete bucket;
        }
    }
}