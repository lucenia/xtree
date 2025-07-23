/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test that demonstrates full XTree reload with MMAP persistence
 */

#include <gtest/gtest.h>
#include "../src/indexdetails.hpp"
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include <sys/stat.h>
#include <cstdio>

using namespace xtree;

class MMAPFullReloadTest : public ::testing::Test {
protected:
    vector<const char*> dimLabels = {"x", "y"};
    
    void SetUp() override {
        // Clean up any existing snapshot files
        remove("test_full_reload.snapshot");
    }
    
    void TearDown() override {
        remove("test_full_reload.snapshot");
    }
};

TEST_F(MMAPFullReloadTest, FullXTreePersistenceAndReload) {
    const string snapshot_file = "test_full_reload.snapshot";
    const int NUM_RECORDS = 15000; // Test with 15K records to trigger splits/supernodes
    
    std::cout << "Starting test...\n";
    
    // Phase 1: Create and populate XTree
    {
        std::cout << "Phase 1: Creating index...\n";
        std::cout.flush();
        
        IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::MMAP, snapshot_file
        );
        
        std::cout << "Index created\n";
        std::cout.flush();
        
        std::cout << "Getting compact allocator...\n";
        std::cout.flush();
        
        auto* compact_allocator = index->getCompactAllocator();
        ASSERT_NE(compact_allocator, nullptr);
        
        std::cout << "Compact allocator obtained\n";
        std::cout.flush();
        
        // Create root bucket
        std::cout << "Creating root bucket...\n";
        std::cout.flush();
        
        XTreeBucket<DataRecord>* root = XAlloc<DataRecord>::allocate_bucket(index, true);
        
        std::cout << "Root bucket created\n";
        std::cout.flush();
        
        std::cout << "Getting next node ID...\n";
        std::cout.flush();
        auto nodeId = index->getNextNodeID();
        std::cout << "Next node ID: " << nodeId << "\n";
        std::cout.flush();
        
        std::cout << "Getting cache reference...\n";
        std::cout.flush();
        auto& cache = index->getCache();
        std::cout << "Cache reference obtained\n";
        std::cout.flush();
        
        std::cout << "Adding to cache...\n";
        std::cout.flush();
        auto* cachedRoot = cache.add(nodeId, root);
        std::cout << "Added to cache\n";
        std::cout.flush();
        
        std::cout << "Setting root address...\n";
        std::cout.flush();
        index->setRootAddress(reinterpret_cast<long>(cachedRoot));
        std::cout << "Root address set\n";
        std::cout.flush();
        
        // Insert records
        std::cout << "Starting to insert " << NUM_RECORDS << " records...\n";
        std::cout.flush();
        for (int i = 0; i < NUM_RECORDS; i++) {
            if (i % 10 == 0) {
                std::cout << "Inserting record " << i << "...\n";
                std::cout.flush();
            }
            DataRecord* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, "rec_" + to_string(i));
            if (!record) {
                std::cout << "Failed to allocate record " << i << "\n";
                FAIL();
            }
            vector<double> point = {static_cast<double>(i % 100), static_cast<double>(i / 100)};
            record->putPoint(&point);
            root->xt_insert(cachedRoot, record);
        }
        
        std::cout << "All records inserted successfully\n";
        std::cout.flush();
        
        // Save the root bucket offset before snapshot
        std::cout << "Saving root bucket offset...\n";
        std::cout.flush();
        compact_allocator->set_root_bucket(root);
        std::cout << "Root bucket offset saved\n";
        std::cout.flush();
        
        // Trigger snapshot
        std::cout << "Saving snapshot...\n";
        std::cout.flush();
        compact_allocator->save_snapshot();
        std::cout << "Snapshot saved\n";
        std::cout.flush();
        
        // Verify root was created
        std::cout << "Root bucket address: " << root << "\n";
        std::cout << "Successfully inserted " << NUM_RECORDS << " records\n";
        std::cout.flush();
        
        std::cout << "Deleting index...\n";
        std::cout.flush();
        delete index;
        std::cout << "Index deleted\n";
        std::cout.flush();
    }
    
    std::cout << "Phase 1 complete\n";
    std::cout.flush();
    
    // Verify snapshot file exists
    std::cout << "Verifying snapshot file...\n";
    std::cout.flush();
    struct stat st;
    ASSERT_EQ(stat(snapshot_file.c_str(), &st), 0) << "Snapshot file does not exist";
    std::cout << "Snapshot size: " << st.st_size << " bytes (" << (st.st_size / (1024.0 * 1024.0)) << " MB)\n";
    
    // Phase 2: Reload and query
    {
        std::cout << "Phase 2: Reloading index...\n";
        std::cout.flush();
        
        IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::MMAP, snapshot_file
        );
        std::cout << "Index reloaded successfully\n";
        std::cout.flush();
        
        std::cout << "Getting compact allocator from reloaded index...\n";
        std::cout.flush();
        auto* compact_allocator = index->getCompactAllocator();
        ASSERT_NE(compact_allocator, nullptr);
        std::cout << "Got compact allocator\n";
        std::cout.flush();
        
        // Get the reloaded root bucket
        std::cout << "Getting root bucket from compact allocator...\n";
        std::cout.flush();
        XTreeBucket<DataRecord>* root = compact_allocator->get_root_bucket(index);
        std::cout << "Got root bucket: " << root << "\n";
        std::cout.flush();
        ASSERT_NE(root, nullptr) << "Root bucket should be restored from snapshot";
        
        // Set up the cache for the reloaded root
        std::cout << "Adding reloaded root to cache...\n";
        std::cout.flush();
        auto nodeId = index->getNextNodeID();
        std::cout << "Got node ID: " << nodeId << "\n";
        std::cout.flush();
        auto* cachedRoot = index->getCache().add(nodeId, root);
        std::cout << "Added to cache, setting root address...\n";
        std::cout.flush();
        index->setRootAddress(reinterpret_cast<long>(cachedRoot));
        std::cout << "Root address set\n";
        std::cout.flush();
        
        // Verify root is restored
        std::cout << "Reloaded root bucket address: " << root << "\n";
        std::cout.flush();
        
        // Try to insert a new record to verify the tree is functional
        std::cout << "Allocating new record...\n";
        std::cout.flush();
        DataRecord* newRecord = XAlloc<DataRecord>::allocate_record(index, 2, 32, "new_after_reload");
        if (!newRecord) {
            std::cout << "Failed to allocate new record\n";
            FAIL();
        }
        std::cout << "New record allocated\n";
        std::cout.flush();
        vector<double> point = {50.0, 50.0};
        std::cout << "Setting point on new record...\n";
        std::cout.flush();
        newRecord->putPoint(&point);
        std::cout << "Point set\n";
        std::cout.flush();
        
        // If we can insert without crashing, the tree structure is intact
        std::cout << "Inserting new record into tree...\n";
        std::cout.flush();
        root->xt_insert(cachedRoot, newRecord);
        std::cout << "Successfully inserted new record after reload\n";
        std::cout.flush();
        
        // Add more records to the reloaded tree
        for (int i = NUM_RECORDS; i < NUM_RECORDS + 100; i++) {
            DataRecord* record = XAlloc<DataRecord>::allocate_record(index, 2, 32, "rec_" + to_string(i));
            if (!record) {
                std::cout << "Failed to allocate record " << i << "\n";
                FAIL();
            }
            vector<double> point = {static_cast<double>(i % 100), static_cast<double>(i / 100)};
            record->putPoint(&point);
            root->xt_insert(cachedRoot, record);
        }
        
        std::cout << "Successfully added 100 more records to reloaded tree\n";
        
        delete index;
    }
}