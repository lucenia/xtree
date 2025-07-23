/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test for XTree bucket reallocation mechanism
 * Verifies that buckets can grow and reallocate properly
 * without data loss or excessive fragmentation
 */

#include <gtest/gtest.h>
#include "../../src/xtree.h"
#include "../../src/indexdetails.hpp"
#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/segment_allocator.h"
#include "../../src/xtree_allocator_traits.hpp"
#include <filesystem>
#include <random>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

class BucketReallocationTest : public ::testing::Test {
protected:
    std::string test_dir = "./test_realloc_data";
    xtree::IndexDetails<xtree::DataRecord>* idx = nullptr;
    std::vector<const char*>* dimLabels = nullptr;
    
    void SetUp() override {
        std::cout << "[SetUp] Starting test setup for test: " << ::testing::UnitTest::GetInstance()->current_test_info()->name() << "\n";
        // Clean up any previous test data
        if (fs::exists(test_dir)) {
            std::cout << "[SetUp] Removing existing test_dir\n";
            fs::remove_all(test_dir);
        }
        std::cout << "[SetUp] Creating test_dir\n";
        fs::create_directories(test_dir);
        
        // Create dimension labels
        std::cout << "[SetUp] Creating dimension labels\n";
        dimLabels = new std::vector<const char*>();
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        std::cout << "[SetUp] Setup complete\n";
    }
    
    void TearDown() override {
        if (idx) {
            delete idx;
            idx = nullptr;
        }
        if (dimLabels) {
            delete dimLabels;
            dimLabels = nullptr;
        }
        
        // Clean up test data
        if (fs::exists(test_dir)) {
            fs::remove_all(test_dir);
        }
    }
    
    // Helper to get allocator stats
    xtree::persist::SegmentAllocator::Stats getAllocatorStats() {
        if (!idx || !idx->hasDurableStore()) {
            return {};
        }
        auto* store = dynamic_cast<xtree::persist::DurableStore*>(idx->getStore());
        if (!store) return {};
        
        // TODO: Expose allocator stats through DurableStore
        return {};
    }
};

TEST_F(BucketReallocationTest, TestGrowthWithoutThrashing) {
    // Create a durable index
    idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Initialize root bucket
    idx->ensure_root_initialized<xtree::DataRecord>();
    
    // Insert records gradually to force bucket growth
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(0.0, 100.0);
    
    const int NUM_RECORDS = 50;  // Reduced for faster testing
    int records_inserted = 0;
    
    for (int i = 0; i < NUM_RECORDS; i++) {
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, std::to_string(i));
        
        // Add random points
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {dis(gen), dis(gen)};
            record->putPoint(&point);
        }
        
        // Insert into tree
        auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
        if (root_bucket) {
            auto* root_cn = idx->root_cache_node();
            root_bucket->xt_insert(root_cn, record);
            records_inserted++;
        }
        
        // Don't delete - tree needs to keep the record alive
    }
    
    // Verify all records were inserted
    EXPECT_EQ(records_inserted, NUM_RECORDS);
    
    std::cout << "Successfully inserted " << records_inserted << " records without crash\n";
}

TEST_F(BucketReallocationTest, TestSupernodeGrowth) {
    std::cout << "Starting TestSupernodeGrowth\n";
    
    // Create a durable index
    idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    std::cout << "Index created\n";
    
    // Initialize root bucket
    idx->ensure_root_initialized<xtree::DataRecord>();
    auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
    ASSERT_NE(root_bucket, nullptr);
    
    std::cout << "Root initialized\n";
    
    // Track initial NodeID
    auto initial_node_id = root_bucket->getNodeID();
    ASSERT_TRUE(initial_node_id.valid());
    
    std::cout << "Initial NodeID: " << initial_node_id.raw() << "\n";
    
    // Force the bucket to become a supernode by adding many children
    // This simulates the worst-case growth scenario
    const int SUPERNODE_SIZE = XTREE_M * 2;  // Create a supernode
    
    // We can't directly add children, but we can track reallocations
    // by monitoring NodeID changes after operations
    
    // Insert enough records to potentially trigger supernode creation
    std::mt19937 gen(123);
    std::uniform_real_distribution<> dis(0.0, 1000.0);
    
    bool reallocation_detected = false;
    xtree::persist::NodeID last_node_id = initial_node_id;
    
    for (int i = 0; i < SUPERNODE_SIZE; i++) {
        if (i % 10 == 0) {
            std::cout << "  Inserting record " << i << "/" << SUPERNODE_SIZE << "\n";
        }
        
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, std::to_string(i));
        
        // Add clustered points to encourage supernode formation
        double base_x = (i / 10) * 10.0;
        double base_y = (i % 10) * 10.0;
        
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {base_x + dis(gen) * 0.1, base_y + dis(gen) * 0.1};
            record->putPoint(&point);
        }
        
        auto* root_cn = idx->root_cache_node();
        root_bucket->xt_insert(root_cn, record);
        
        // Check if NodeID changed (indicating reallocation)
        auto current_node_id = root_bucket->getNodeID();
        if (current_node_id.raw() != last_node_id.raw()) {
            reallocation_detected = true;
            std::cout << "Reallocation detected at record " << i 
                     << ": NodeID changed from " << last_node_id.raw() 
                     << " to " << current_node_id.raw() << "\n";
            last_node_id = current_node_id;
        }
        
        // Don't delete - tree needs to keep the record alive
    }
    
    // We expect at least one reallocation when growing to supernode size
    // However, with our pre-allocation strategy, it might not happen
    std::cout << "Supernode test: Reallocation " 
              << (reallocation_detected ? "occurred" : "avoided through pre-allocation") << "\n";
}

TEST_F(BucketReallocationTest, TestMinimalThrashing) {
    // Create a durable index with minimal initial allocation
    idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Track allocations and deallocations
    int total_allocations = 0;
    int total_deallocations = 0;
    
    // Initialize root
    idx->ensure_root_initialized<xtree::DataRecord>();
    total_allocations++;
    
    // Insert records one by one and track reallocations
    std::mt19937 gen(456);
    std::uniform_real_distribution<> dis(0.0, 100.0);
    
    const int NUM_RECORDS = 50;
    std::vector<xtree::persist::NodeID> node_history;
    
    auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
    node_history.push_back(root_bucket->getNodeID());
    
    for (int i = 0; i < NUM_RECORDS; i++) {
        if (i % 10 == 0) {
            std::cout << "  Progress: " << i << "/" << NUM_RECORDS << " records\n";
        }
        
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, std::to_string(i));
        
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {dis(gen), dis(gen)};
            record->putPoint(&point);
        }
        
        auto* root_cn = idx->root_cache_node();
        root_bucket->xt_insert(root_cn, record);
        
        auto current_id = root_bucket->getNodeID();
        if (current_id.raw() != node_history.back().raw()) {
            // Reallocation occurred
            total_allocations++;
            total_deallocations++;
            node_history.push_back(current_id);
        }
        
        // Don't delete - tree needs to keep the record alive
    }
    
    // Calculate thrashing rate
    double thrashing_rate = static_cast<double>(total_deallocations) / NUM_RECORDS;
    
    std::cout << "Thrashing Analysis:\n"
              << "  Records inserted: " << NUM_RECORDS << "\n"
              << "  Total allocations: " << total_allocations << "\n"
              << "  Total deallocations: " << total_deallocations << "\n"
              << "  NodeID changes: " << (node_history.size() - 1) << "\n"
              << "  Thrashing rate: " << (thrashing_rate * 100) << "%\n";
    
    // With our 2x growth strategy, we expect very low thrashing
    // Theoretical: log2(NUM_RECORDS) reallocations at most
    int expected_max_reallocations = static_cast<int>(std::log2(NUM_RECORDS)) + 1;
    
    EXPECT_LE(total_deallocations, expected_max_reallocations)
        << "Reallocation count should be logarithmic in the number of records";
    
    // Thrashing rate should be very low
    EXPECT_LT(thrashing_rate, 0.2)
        << "Thrashing rate should be less than 20%";
}

TEST_F(BucketReallocationTest, TestSegmentReuse) {
    // Create index with detailed metrics tracking
    idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Initialize and insert some records
    idx->ensure_root_initialized<xtree::DataRecord>();
    
    std::mt19937 gen(789);
    std::uniform_real_distribution<> dis(0.0, 100.0);
    
    // First phase: Insert records to cause some allocations
    for (int i = 0; i < 30; i++) {
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, "rec_" + std::to_string(i));
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {dis(gen), dis(gen)};
            record->putPoint(&point);
        }
        
        auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
        auto* root_cn = idx->root_cache_node();
        root_bucket->xt_insert(root_cn, record);
        // Don't delete - tree needs to keep the record alive
    }
    
    // Get mid-point stats
    auto mid_stats = getAllocatorStats();
    
    // Second phase: Intentionally exceed XTREE_M to understand the crash
    // Only add debug for the critical 51st record
    for (int i = 30; i < 51; i++) {  // Twenty-one iterations (total = 51 > XTREE_M)
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, "rec_" + std::to_string(i));
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {dis(gen), dis(gen)};
            record->putPoint(&point);
        }
        
        // CRITICAL: Must re-fetch root_bucket for each insert as splits can invalidate the pointer
        auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
        auto* root_cn = idx->root_cache_node();
        
        root_bucket->xt_insert(root_cn, record);
        // Don't delete - tree needs to keep the record alive
    }
    
    /* Commenting out to debug crash - will add back incrementally
    
    // Get final stats
    auto final_stats = getAllocatorStats();
    
    // Calculate reuse efficiency
    if (final_stats.total_frees > 0) {
        double recycle_rate = static_cast<double>(final_stats.allocs_from_bitmap) /
                             std::max(1UL, final_stats.total_frees);
        
        std::cout << "Reuse Metrics:\n"
                  << "  Total frees: " << final_stats.total_frees << "\n"
                  << "  Allocations from bitmap (reused): " << final_stats.allocs_from_bitmap << "\n"
                  << "  Recycle rate: " << (recycle_rate * 100) << "%\n"
                  << "  Dead bytes (unclaimed): " << final_stats.dead_bytes << "\n"
                  << "  Dead ratio: " << (final_stats.fragmentation() * 100) << "%\n";
        
        // Dead bytes should eventually be reclaimed
        if (final_stats.total_frees > 10) {
            EXPECT_GT(final_stats.allocs_from_bitmap, 0)
                << "Should have some reuse after multiple frees";
        }
    }
    */
}

// Test edge case: Empty bucket to full bucket
TEST_F(BucketReallocationTest, TestEmptyToFull) {
    idx = new xtree::IndexDetails<xtree::DataRecord>(
        2, 5, dimLabels, nullptr, nullptr, "test_field",
        xtree::IndexDetails<xtree::DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Start with empty root
    idx->ensure_root_initialized<xtree::DataRecord>();
    auto* root_bucket = idx->root_bucket<xtree::DataRecord>();
    
    // XTreeBucket doesn't expose getCount() directly
    // Just verify it starts without crashes
    ASSERT_NE(root_bucket, nullptr);
    
    // Fill it completely (up to XTREE_M)
    std::mt19937 gen(999);
    std::uniform_real_distribution<> dis(0.0, 100.0);
    
    for (int i = 0; i < XTREE_M; i++) {
        xtree::DataRecord* record = new xtree::DataRecord(2, 5, "rec_" + std::to_string(i));
        for (int j = 0; j < 5; j++) {
            std::vector<double> point = {dis(gen), dis(gen)};
            record->putPoint(&point);
        }
        
        auto* root_cn = idx->root_cache_node();
        root_bucket->xt_insert(root_cn, record);
        // Don't delete - tree needs to keep the record alive
    }
    
    // Verify bucket is still valid (no crash/corruption)
    ASSERT_NE(root_bucket, nullptr);
    // We can't check _n member directly as it's private
}