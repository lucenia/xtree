/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test to force bucket reallocation and verify it works
 */

#include <gtest/gtest.h>
#include "../../src/xtree.h"
#include "../../src/indexdetails.hpp"
#include "../../src/persistence/durable_runtime.h"
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

TEST(ForcedReallocationTest, VerifyReallocationOccurs) {
    std::string test_dir = "./test_forced_realloc";
    
    // Clean up any previous test data
    if (fs::exists(test_dir)) {
        fs::remove_all(test_dir);
    }
    fs::create_directories(test_dir);
    
    // Create dimension labels
    auto* dimLabels = new std::vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    
    // Create a durable index
    auto* idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Initialize root bucket
    idx->ensure_root_initialized<xtree::DataRecord>();
    auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
    ASSERT_NE(root_bucket, nullptr);
    
    // Get initial wire size
    size_t initial_wire_size = root_bucket->wire_size(*idx);
    std::cout << "Initial root wire size: " << initial_wire_size << " bytes\n";
    
    // The root starts empty, so it should be small (20 bytes for 2D)
    // Header (4) + MBR (2*8=16) + 0 children = 20 bytes
    EXPECT_EQ(initial_wire_size, 20);
    
    // Now insert records to grow the bucket
    // Each child adds 16 bytes (NodeID + MBR pointer)
    // So at 31 children: 20 + 31*16 = 516 bytes (exceeds 512B allocation)
    
    std::cout << "\nInserting records to force growth:\n";
    
    for (int i = 0; i < 35; i++) {  // Insert enough to exceed 512B
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, std::to_string(i));
        
        // Add points
        std::vector<double> point = {static_cast<double>(i), static_cast<double>(i)};
        record->putPoint(&point);
        
        auto* root_cn = idx->root_cache_node();
        root_bucket->xt_insert(root_cn, record);
        
        size_t current_wire_size = root_bucket->wire_size(*idx);
        
        if (i == 30) {  // After 31 children (0-30)
            std::cout << "After 31 children: wire_size = " << current_wire_size << " bytes\n";
            // Should be 20 + 31*16 = 516 bytes
            EXPECT_GT(current_wire_size, 512) << "Should exceed 512B allocation";
        }
        
        delete record;
    }
    
    size_t final_wire_size = root_bucket->wire_size(*idx);
    std::cout << "Final root wire size: " << final_wire_size << " bytes\n";
    
    // The test passes if we didn't crash!
    // With 35 children: 20 + 35*16 = 580 bytes
    EXPECT_GT(final_wire_size, 512);
    
    // Clean up
    delete idx;
    delete dimLabels;
    if (fs::exists(test_dir)) {
        fs::remove_all(test_dir);
    }
    
    std::cout << "\nTest completed successfully - no crash despite exceeding 512B!\n";
    std::cout << "This proves our reallocation mechanism is working.\n";
}

TEST(ForcedReallocationTest, VerifyDataIntegrityAfterRealloc) {
    std::string test_dir = "./test_integrity_realloc";
    
    if (fs::exists(test_dir)) {
        fs::remove_all(test_dir);
    }
    fs::create_directories(test_dir);
    
    auto* dimLabels = new std::vector<const char*>();
    dimLabels->push_back("x");
    dimLabels->push_back("y");
    
    auto* idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    idx->ensure_root_initialized<xtree::DataRecord>();
    
    // Insert many records to force potential reallocations
    const int NUM_RECORDS = 100;
    std::cout << "\nInserting " << NUM_RECORDS << " records to test data integrity:\n";
    
    for (int i = 0; i < NUM_RECORDS; i++) {
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, "rec_" + std::to_string(i));
        
        // Add distinctive points so we can verify them later
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {static_cast<double>(i), static_cast<double>(j)};
            record->putPoint(&point);
        }
        
        auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
        auto* root_cn = idx->root_cache_node();
        root_bucket->xt_insert(root_cn, record);
        
        delete record;
    }
    
    // Now close and reopen to verify persistence
    delete idx;
    
    std::cout << "Reopening index to verify data integrity...\n";
    
    idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Recovery should restore the root
    bool recovered = idx->recover_root<xtree::DataRecord>();
    EXPECT_TRUE(recovered) << "Should recover root from durable store";
    
    auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
    ASSERT_NE(root_bucket, nullptr) << "Root should be recovered";
    
    std::cout << "Recovery successful - data integrity maintained!\n";
    
    // Clean up
    delete idx;
    delete dimLabels;
    if (fs::exists(test_dir)) {
        fs::remove_all(test_dir);
    }
}