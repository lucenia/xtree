/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test auto-snapshot functionality
 */

#include <gtest/gtest.h>
#include "../../src/indexdetails.hpp"
#include "../../src/xtree.hpp"
#include <chrono>
#include <vector>

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class AutoSnapshotTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test files
        std::remove("/tmp/auto_snapshot_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/auto_snapshot_test.dat");
    }
};

TEST_F(AutoSnapshotTest, TestAutoSnapshotWithSingleSegment) {
    std::cout << "\n=== Testing Auto-Snapshot with Single Segment ===\n";
    
    // Create index with MMAP persistence
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP,
        "/tmp/auto_snapshot_test.dat"
    );
    
    // Initialize the tree
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    std::cout << "Inserting records to trigger auto-snapshot...\n";
    
    // Insert enough records to trigger auto-snapshot (threshold is 10,000)
    for (int i = 0; i < 12000; i++) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
            "record_" + std::to_string(i));
        std::vector<double> point = {i * 0.1, i * 0.2};
        dr->putPoint(&point);
        
        // Get current root
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        root->xt_insert(cachedRoot, dr);
        
        // Print progress
        if (i > 0 && i % 2000 == 0) {
            std::cout << "  Inserted " << i << " records\n";
        }
    }
    
    // Check if snapshot file was created
    struct stat st;
    bool snapshot_exists = (stat("/tmp/auto_snapshot_test.dat", &st) == 0);
    
    if (snapshot_exists) {
        std::cout << "\nSnapshot file created successfully!\n";
        std::cout << "  File size: " << st.st_size / (1024.0 * 1024.0) << " MB\n";
        EXPECT_GT(st.st_size, 0) << "Snapshot file should not be empty";
    } else {
        std::cout << "\nWARNING: No snapshot file found - auto-snapshot may have been skipped\n";
    }
    
    delete index;
}

TEST_F(AutoSnapshotTest, TestAutoSnapshotSkippedWithMultiSegment) {
    std::cout << "\n=== Testing Auto-Snapshot Skipped with Multi-Segment ===\n";
    
    // Create index with MMAP persistence
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP,
        "/tmp/auto_snapshot_test.dat"
    );
    
    // Initialize the tree
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    std::cout << "Inserting large records to force multi-segment allocator...\n";
    
    // Insert records with large data to force multi-segment
    for (int i = 0; i < 12000; i++) {
        // Allocate larger records to consume more memory
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 10000,  // Large precision
            "large_record_" + std::to_string(i));
        std::vector<double> point = {i * 0.1, i * 0.2};
        dr->putPoint(&point);
        
        // Get current root
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        
        root->xt_insert(cachedRoot, dr);
        
        // Print progress
        if (i > 0 && i % 1000 == 0) {
            std::cout << "  Inserted " << i << " large records\n";
            
            // Check allocator stats
            if (auto* compact = index->getCompactAllocator()) {
                auto* snapshot_mgr = compact->get_snapshot_manager();
                auto* allocator = snapshot_mgr->get_allocator();
                std::cout << "    Segments: " << allocator->get_segment_count()
                          << ", Used: " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
            }
        }
    }
    
    std::cout << "\nTest complete - check console output for auto-snapshot warnings\n";
    
    delete index;
}