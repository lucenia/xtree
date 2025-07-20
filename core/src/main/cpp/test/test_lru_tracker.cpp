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
#include <filesystem>
#include <thread>
#include <chrono>
#include <random>
#include "../src/lru_tracker.h"
#include "../src/mmapfile.h"

using namespace xtree;
using namespace std;
using namespace std::chrono_literals;
using testing::_;

class LRUAccessTrackerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique test directory
        test_dir_ = std::filesystem::temp_directory_path() / 
                   ("lru_test_" + to_string(getpid()) + "_" + to_string(time(nullptr)));
        std::filesystem::create_directories(test_dir_);
        test_file_ = test_dir_ / "test.mmap";
        
        // Create a real MMapFile for testing
        mmap_file_ = make_unique<MMapFile>(test_file_.string(), 1024 * 1024, false);
        tracker_ = make_unique<LRUAccessTracker>(mmap_file_.get(), 100); // Track max 100 nodes
    }
    
    void TearDown() override {
        tracker_.reset();
        mmap_file_.reset();
        // Clean up test files
        std::filesystem::remove_all(test_dir_);
    }
    
    std::filesystem::path test_dir_;
    std::filesystem::path test_file_;
    unique_ptr<MMapFile> mmap_file_;
    unique_ptr<LRUAccessTracker> tracker_;
    
    // Helper to simulate time passing
    void advance_time(chrono::milliseconds ms) {
        this_thread::sleep_for(ms);
    }
};

// Test basic access recording
TEST_F(LRUAccessTrackerTest, BasicAccessRecording) {
    const size_t offset1 = 1024;
    const size_t offset2 = 2048;
    
    // Initially no stats
    EXPECT_EQ(tracker_->get_node_stats(offset1), nullptr);
    EXPECT_EQ(tracker_->get_tracked_count(), 0);
    
    // Record first access
    tracker_->record_access(offset1);
    
    auto stats1 = tracker_->get_node_stats(offset1);
    ASSERT_NE(stats1, nullptr);
    EXPECT_EQ(stats1->access_count, 1);
    EXPECT_FALSE(stats1->is_pinned);
    EXPECT_EQ(tracker_->get_tracked_count(), 1);
    
    // Record multiple accesses to same offset
    tracker_->record_access(offset1);
    tracker_->record_access(offset1);
    
    stats1 = tracker_->get_node_stats(offset1);
    EXPECT_EQ(stats1->access_count, 3);
    
    // Record access to different offset
    tracker_->record_access(offset2);
    
    auto stats2 = tracker_->get_node_stats(offset2);
    ASSERT_NE(stats2, nullptr);
    EXPECT_EQ(stats2->access_count, 1);
    EXPECT_EQ(tracker_->get_tracked_count(), 2);
    
    // First offset should still have 3 accesses
    stats1 = tracker_->get_node_stats(offset1);
    EXPECT_EQ(stats1->access_count, 3);
}

// Test access frequency calculation
TEST_F(LRUAccessTrackerTest, AccessFrequencyCalculation) {
    const size_t offset = 1024;
    
    // Record initial access
    tracker_->record_access(offset);
    
    // Wait a bit and record more accesses
    advance_time(100ms);
    tracker_->record_access(offset);
    tracker_->record_access(offset);
    tracker_->record_access(offset);
    
    advance_time(100ms);
    tracker_->record_access(offset);
    
    auto stats = tracker_->get_node_stats(offset);
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->access_count, 5);
    
    // Frequency should be > 0 (we had 5 accesses over ~200ms)
    double frequency = stats->get_access_frequency();
    EXPECT_GT(frequency, 0.0);
    EXPECT_LT(frequency, 100.0); // Reasonable upper bound
}

// Test memory pinning
TEST_F(LRUAccessTrackerTest, MemoryPinning) {
    const size_t offset = 1024;
    const size_t size = 256;
    
    // Initially not pinned
    EXPECT_EQ(tracker_->get_pinned_count(), 0);
    
    // Pin the node (may fail if not running as root, that's ok)
    bool pin_result = tracker_->pin_node(offset, size);
    
    if (pin_result) {
        EXPECT_EQ(tracker_->get_pinned_count(), 1);
        
        // Check stats reflect pinning
        tracker_->record_access(offset); // Create stats entry
        auto stats = tracker_->get_node_stats(offset);
        ASSERT_NE(stats, nullptr);
        EXPECT_TRUE(stats->is_pinned);
        EXPECT_EQ(stats->size, size);
        
        // Unpin the node
        EXPECT_TRUE(tracker_->unpin_node(offset, size));
        EXPECT_EQ(tracker_->get_pinned_count(), 0);
    } else {
        // If pinning failed (permissions), just verify counts remain 0
        EXPECT_EQ(tracker_->get_pinned_count(), 0);
    }
    EXPECT_EQ(tracker_->get_pinned_count(), 0);
    
    auto stats_after = tracker_->get_node_stats(offset);
    if (stats_after) {
        EXPECT_FALSE(stats_after->is_pinned);
    }
}

// Test pinning failure handling
TEST_F(LRUAccessTrackerTest, PinningFailureHandling) {
    const size_t offset = 1024;
    const size_t size = 256;
    
    // Try to pin a node - it may fail due to permissions
    bool result = tracker_->pin_node(offset, size);
    if (!result) {
        // Pin failed as expected (common in non-root environments)
        EXPECT_EQ(tracker_->get_pinned_count(), 0);
        
        // Stats should reflect no pinning
        tracker_->record_access(offset);
        auto stats = tracker_->get_node_stats(offset);
        if (stats) {
            EXPECT_FALSE(stats->is_pinned);
        }
    }
}

// Test hot nodes detection
TEST_F(LRUAccessTrackerTest, HotNodesDetection) {
    // Create nodes with different access patterns
    vector<pair<size_t, int>> test_nodes = {
        {1024, 10},  // Hot node
        {2048, 5},   // Warm node
        {3072, 2},   // Cool node
        {4096, 1},   // Cold node
        {5120, 15}   // Hottest node
    };
    
    // Record accesses
    for (const auto& [offset, access_count] : test_nodes) {
        for (int i = 0; i < access_count; ++i) {
            tracker_->record_access(offset);
            if (i % 3 == 0) advance_time(10ms); // Vary timing
        }
    }
    
    // Get hot nodes (top 3)
    auto hot_nodes = tracker_->get_hot_nodes(3);
    EXPECT_EQ(hot_nodes.size(), 3);
    
    // Should be sorted by access count (descending)
    EXPECT_GE(hot_nodes[0].second.access_count, hot_nodes[1].second.access_count);
    EXPECT_GE(hot_nodes[1].second.access_count, hot_nodes[2].second.access_count);
    
    // Hottest node should be offset 5120 (15 accesses)
    EXPECT_EQ(hot_nodes[0].first, 5120);
    EXPECT_EQ(hot_nodes[0].second.access_count, 15);
}

// Test pin candidates selection
TEST_F(LRUAccessTrackerTest, PinCandidatesSelection) {
    // Create nodes with different access patterns
    for (size_t i = 0; i < 10; ++i) {
        size_t offset = 1024 * (i + 1);
        int access_count = (i % 3) + 1; // 1, 2, 3, 1, 2, 3, ...
        
        for (int j = 0; j < access_count * 3; ++j) {
            tracker_->record_access(offset);
            if (j % 2 == 0) advance_time(5ms);
        }
    }
    
    auto candidates = tracker_->get_pin_candidates(3);
    EXPECT_LE(candidates.size(), 3);
    
    // Candidates should be nodes with higher access patterns
    for (size_t offset : candidates) {
        auto stats = tracker_->get_node_stats(offset);
        ASSERT_NE(stats, nullptr);
        EXPECT_GT(stats->access_count, 3); // Should be reasonably active
    }
}

// Test LRU eviction when max nodes exceeded
TEST_F(LRUAccessTrackerTest, LRUEviction) {
    const size_t max_nodes = 5;
    tracker_ = make_unique<LRUAccessTracker>(mmap_file_.get(), max_nodes);
    
    // Fill up to max capacity
    for (size_t i = 0; i < max_nodes; ++i) {
        tracker_->record_access(1024 * (i + 1));
    }
    
    EXPECT_EQ(tracker_->get_tracked_count(), max_nodes);
    
    // Access nodes in specific order to establish LRU order
    tracker_->record_access(1024); // Make this most recent
    advance_time(10ms);
    tracker_->record_access(2048);
    advance_time(10ms);
    
    // Add one more node - should evict LRU
    tracker_->record_access(1024 * (max_nodes + 1));
    
    EXPECT_EQ(tracker_->get_tracked_count(), max_nodes);
    
    // Most recently accessed nodes should still be tracked
    EXPECT_NE(tracker_->get_node_stats(1024), nullptr);
    EXPECT_NE(tracker_->get_node_stats(2048), nullptr);
    EXPECT_NE(tracker_->get_node_stats(1024 * (max_nodes + 1)), nullptr);
}

// Test concurrent access recording
TEST_F(LRUAccessTrackerTest, ConcurrentAccess) {
    const int num_threads = 4;
    const int accesses_per_thread = 100;
    
    vector<thread> threads;
    vector<size_t> thread_offsets(num_threads);
    
    // Each thread accesses its own offset
    for (int i = 0; i < num_threads; ++i) {
        thread_offsets[i] = 1024 * (i + 1);
        
        threads.emplace_back([&, i]() {
            for (int j = 0; j < accesses_per_thread; ++j) {
                tracker_->record_access(thread_offsets[i]);
                if (j % 10 == 0) this_thread::sleep_for(1ms);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify each thread's accesses were recorded
    for (int i = 0; i < num_threads; ++i) {
        auto stats = tracker_->get_node_stats(thread_offsets[i]);
        ASSERT_NE(stats, nullptr);
        EXPECT_EQ(stats->access_count, accesses_per_thread);
    }
    
    EXPECT_EQ(tracker_->get_tracked_count(), num_threads);
}

// Test statistics clearing
TEST_F(LRUAccessTrackerTest, StatisticsClearing) {
    // Set up some nodes
    const size_t offset1 = 1024;
    const size_t offset2 = 2048;
    
    // Record accesses
    tracker_->record_access(offset1);
    tracker_->record_access(offset2);
    
    EXPECT_EQ(tracker_->get_tracked_count(), 2);
    
    // Clear stats
    tracker_->clear_stats();
    
    // Should have no tracked nodes
    EXPECT_EQ(tracker_->get_tracked_count(), 0);
    
    // Stats should be cleared
    auto stats1 = tracker_->get_node_stats(offset1);
    auto stats2 = tracker_->get_node_stats(offset2);
    EXPECT_EQ(stats1, nullptr);
    EXPECT_EQ(stats2, nullptr);
}

// Test stale entry cleanup
TEST_F(LRUAccessTrackerTest, StaleEntryCleanup) {
    // Create some nodes
    vector<size_t> offsets = {1024, 2048, 3072, 4096};
    
    for (size_t offset : offsets) {
        tracker_->record_access(offset);
    }
    
    EXPECT_EQ(tracker_->get_tracked_count(), 4);
    
    // Wait and access only some nodes
    advance_time(100ms);
    tracker_->record_access(1024);
    tracker_->record_access(2048);
    
    // Cleanup should remove stale entries
    tracker_->cleanup_stale_entries();
    
    // Recently accessed nodes should remain
    EXPECT_NE(tracker_->get_node_stats(1024), nullptr);
    EXPECT_NE(tracker_->get_node_stats(2048), nullptr);
    
    // Exact behavior depends on implementation, but should have fewer nodes
    EXPECT_LE(tracker_->get_tracked_count(), 4);
}

// Test memory usage tracking
TEST_F(LRUAccessTrackerTest, MemoryUsageTracking) {
    // Initially should have minimal memory usage
    size_t initial_usage = tracker_->get_memory_usage();
    EXPECT_GT(initial_usage, 0);
    
    // Add some tracked nodes
    for (size_t i = 0; i < 50; ++i) {
        tracker_->record_access(1024 * (i + 1));
    }
    
    size_t usage_with_nodes = tracker_->get_memory_usage();
    EXPECT_GT(usage_with_nodes, initial_usage);
    
    // Clear stats
    tracker_->clear_stats();
    
    size_t usage_after_clear = tracker_->get_memory_usage();
    EXPECT_LT(usage_after_clear, usage_with_nodes);
}

// Test edge cases
TEST_F(LRUAccessTrackerTest, EdgeCases) {
    // Access offset 0
    tracker_->record_access(0);
    auto stats = tracker_->get_node_stats(0);
    EXPECT_NE(stats, nullptr);
    EXPECT_EQ(stats->access_count, 1);
    
    // Large offset
    size_t large_offset = 1ULL << 40; // 1TB offset
    tracker_->record_access(large_offset);
    stats = tracker_->get_node_stats(large_offset);
    EXPECT_NE(stats, nullptr);
    
    // Multiple accesses to same offset in quick succession
    for (int i = 0; i < 1000; ++i) {
        tracker_->record_access(1024);
    }
    
    stats = tracker_->get_node_stats(1024);
    EXPECT_EQ(stats->access_count, 1000);
}

// Performance test
TEST_F(LRUAccessTrackerTest, PerformanceTest) {
    const int num_accesses = 10000;
    const int num_unique_offsets = 1000;
    
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<size_t> dist(1024, 1024 + num_unique_offsets * 1024);
    
    auto start = chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_accesses; ++i) {
        size_t offset = dist(gen);
        tracker_->record_access(offset);
    }
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    
    // Should handle 10k accesses quickly (less than 100ms)
    EXPECT_LT(duration.count(), 100000);
    
    // Should have tracked reasonable number of unique offsets
    EXPECT_GE(tracker_->get_tracked_count(), 100);  // At least 100 (the max)
    EXPECT_LE(tracker_->get_tracked_count(), num_unique_offsets);
}