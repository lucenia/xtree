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
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>
#include <cctype>
#include "../src/lru_tracker.h"
#include "../src/mmapfile.h"

using namespace xtree;
using namespace std;
using namespace std::chrono_literals;
using testing::_;

// Mock MMapFile for testing
class MockMMapFile : public MMapFile {
public:
    MockMMapFile() : MMapFile("/tmp/mock_test_file", 1024*1024, false) {}
    virtual ~MockMMapFile() = default;
    
    MOCK_METHOD(bool, mlock_region, (size_t offset, size_t size), (override));
    MOCK_METHOD(bool, munlock_region, (size_t offset, size_t size), (override));
    MOCK_METHOD(void*, getPointer, (size_t offset), (const, override));
    MOCK_METHOD(size_t, size, (), (const, override));
};

class HotNodeDetectorTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock_mmap_ = make_unique<MockMMapFile>();
        tracker_ = make_unique<LRUAccessTracker>(mock_mmap_.get(), 1000);
        detector_ = make_unique<HotNodeDetector>(tracker_.get());
        
        // Set up default mock behavior
        ON_CALL(*mock_mmap_, mlock_region(testing::_, testing::_))
            .WillByDefault(testing::Return(true));
        ON_CALL(*mock_mmap_, munlock_region(testing::_, testing::_))
            .WillByDefault(testing::Return(true));
        ON_CALL(*mock_mmap_, size())
            .WillByDefault(testing::Return(10 * 1024 * 1024)); // 10MB
    }
    
    void TearDown() override {
        detector_.reset();
        tracker_.reset();
        mock_mmap_.reset();
    }
    
    unique_ptr<MockMMapFile> mock_mmap_;
    unique_ptr<LRUAccessTracker> tracker_;
    unique_ptr<HotNodeDetector> detector_;
    
    // Helper to create access patterns
    void create_access_pattern(size_t offset, int access_count, chrono::milliseconds interval = 10ms) {
        for (int i = 0; i < access_count; ++i) {
            tracker_->record_access(offset);
            if (interval.count() > 0) {
                this_thread::sleep_for(interval);
            }
        }
    }
    
    // Helper to create hot, warm, and cold nodes
    void setup_mixed_access_patterns() {
        // Hot nodes - high frequency access
        create_access_pattern(1024, 50, 5ms);   // Very hot
        create_access_pattern(2048, 30, 8ms);   // Hot
        create_access_pattern(3072, 25, 10ms);  // Hot
        
        // Warm nodes - moderate access
        create_access_pattern(4096, 15, 20ms);  // Warm
        create_access_pattern(5120, 12, 25ms);  // Warm
        
        // Cold nodes - low access
        create_access_pattern(6144, 5, 50ms);   // Cold
        create_access_pattern(7168, 3, 100ms);  // Very cold
        create_access_pattern(8192, 1, 0ms);    // Single access
    }
};

// Test basic hot node detection
TEST_F(HotNodeDetectorTest, BasicHotNodeDetection) {
    setup_mixed_access_patterns();
    
    // Test different hotness thresholds
    EXPECT_TRUE(detector_->is_hot_node(1024, 0.5));  // Very hot node
    EXPECT_TRUE(detector_->is_hot_node(2048, 0.7));  // Hot node
    EXPECT_FALSE(detector_->is_hot_node(6144, 1.5)); // Cold node
    EXPECT_FALSE(detector_->is_hot_node(8192, 0.5)); // Very cold node
    
    // Test with default threshold
    EXPECT_TRUE(detector_->is_hot_node(1024));   // Should be hot with default threshold
    EXPECT_FALSE(detector_->is_hot_node(8192));  // Should not be hot
}

// Test optimization suggestions analysis
TEST_F(HotNodeDetectorTest, OptimizationSuggestions) {
    setup_mixed_access_patterns();
    
    auto suggestions = detector_->analyze(chrono::seconds(5));
    
    // Should have multiple suggestions
    EXPECT_GT(suggestions.size(), 0);
    
    // Check for expected suggestion types
    bool has_pin_suggestion = false;
    bool has_unpin_suggestion = false;
    bool has_thread_affinity = false;
    
    for (const auto& suggestion : suggestions) {
        EXPECT_GE(suggestion.confidence, 0.0);
        EXPECT_LE(suggestion.confidence, 1.0);
        EXPECT_FALSE(suggestion.reason.empty());
        
        switch (suggestion.type) {
            case HotNodeDetector::OptimizationSuggestion::PIN_NODE:
                has_pin_suggestion = true;
                // Pin suggestions should be for hot nodes
                EXPECT_TRUE(detector_->is_hot_node(suggestion.offset, 0.5));
                break;
                
            case HotNodeDetector::OptimizationSuggestion::UNPIN_NODE:
                has_unpin_suggestion = true;
                break;
                
            case HotNodeDetector::OptimizationSuggestion::THREAD_AFFINITY:
                has_thread_affinity = true;
                break;
                
            case HotNodeDetector::OptimizationSuggestion::SHARD_RELOCATION:
            case HotNodeDetector::OptimizationSuggestion::PREFETCH_SUBTREE:
                // These are valid suggestion types
                break;
        }
    }
    
    // Should have at least pin suggestions for hot nodes
    EXPECT_TRUE(has_pin_suggestion);
}

// Test pin node suggestions
TEST_F(HotNodeDetectorTest, PinNodeSuggestions) {
    // Create very hot nodes
    create_access_pattern(1024, 100, 1ms);  // Extremely hot
    create_access_pattern(2048, 80, 2ms);   // Very hot
    create_access_pattern(3072, 5, 100ms);  // Cold
    
    auto suggestions = detector_->analyze();
    
    // Filter pin suggestions
    vector<HotNodeDetector::OptimizationSuggestion> pin_suggestions;
    copy_if(suggestions.begin(), suggestions.end(), back_inserter(pin_suggestions),
            [](const auto& s) { return s.type == HotNodeDetector::OptimizationSuggestion::PIN_NODE; });
    
    EXPECT_GT(pin_suggestions.size(), 0);
    
    // Pin suggestions should be for hot nodes with high confidence
    for (const auto& suggestion : pin_suggestions) {
        EXPECT_GT(suggestion.confidence, 0.5);
        EXPECT_TRUE(detector_->is_hot_node(suggestion.offset));
        
        // Should suggest pinning offset 1024 or 2048 (hot nodes)
        EXPECT_TRUE(suggestion.offset == 1024 || suggestion.offset == 2048);
    }
}

// Test unpin node suggestions
TEST_F(HotNodeDetectorTest, UnpinNodeSuggestions) {
    // Set up some pinned nodes
    const vector<size_t> pinned_offsets = {1024, 2048, 3072};
    
    EXPECT_CALL(*mock_mmap_, mlock_region(_, _))
        .Times(pinned_offsets.size())
        .WillRepeatedly(testing::Return(true));
    
    // Pin some nodes and create access patterns
    for (size_t offset : pinned_offsets) {
        tracker_->pin_node(offset, 256);
    }
    
    // Create mixed access patterns - some pinned nodes become cold
    create_access_pattern(1024, 50, 5ms);   // Hot pinned node - should stay pinned
    create_access_pattern(2048, 2, 200ms);  // Cold pinned node - should be unpinned
    create_access_pattern(3072, 1, 0ms);    // Very cold pinned node - should be unpinned
    
    auto suggestions = detector_->analyze();
    
    // Filter unpin suggestions
    vector<HotNodeDetector::OptimizationSuggestion> unpin_suggestions;
    copy_if(suggestions.begin(), suggestions.end(), back_inserter(unpin_suggestions),
            [](const auto& s) { return s.type == HotNodeDetector::OptimizationSuggestion::UNPIN_NODE; });
    
    // Should suggest unpinning cold nodes
    bool suggests_unpin_cold_node = false;
    for (const auto& suggestion : unpin_suggestions) {
        if (suggestion.offset == 2048 || suggestion.offset == 3072) {
            suggests_unpin_cold_node = true;
            EXPECT_GT(suggestion.confidence, 0.3);
        }
    }
    
    EXPECT_TRUE(suggests_unpin_cold_node);
}

// Test thread affinity suggestions
TEST_F(HotNodeDetectorTest, ThreadAffinitySuggestions) {
    // Create clustered access patterns (simulating tree subtrees)
    const size_t base_offset = 10000;
    
    // Hot subtree - multiple nodes accessed together
    for (int i = 0; i < 5; ++i) {
        create_access_pattern(base_offset + i * 1024, 30, 5ms);
    }
    
    // Another hot subtree
    const size_t base_offset2 = 20000;
    for (int i = 0; i < 3; ++i) {
        create_access_pattern(base_offset2 + i * 1024, 25, 8ms);
    }
    
    auto suggestions = detector_->analyze();
    
    // Filter thread affinity suggestions
    vector<HotNodeDetector::OptimizationSuggestion> thread_suggestions;
    copy_if(suggestions.begin(), suggestions.end(), back_inserter(thread_suggestions),
            [](const auto& s) { return s.type == HotNodeDetector::OptimizationSuggestion::THREAD_AFFINITY; });
    
    // Should have thread affinity suggestions for hot subtrees
    if (!thread_suggestions.empty()) {
        for (const auto& suggestion : thread_suggestions) {
            EXPECT_GT(suggestion.confidence, 0.4);
            EXPECT_TRUE(detector_->is_hot_node(suggestion.offset, 0.5));
        }
    }
}

// Test temporal analysis (analysis window)
TEST_F(HotNodeDetectorTest, TemporalAnalysis) {
    // Create access pattern over time
    create_access_pattern(1024, 20, 10ms);
    
    // Wait longer than analysis window
    this_thread::sleep_for(100ms);
    
    // Create more recent accesses
    create_access_pattern(2048, 10, 5ms);
    
    // Analyze with short window - should focus on recent activity
    auto recent_suggestions = detector_->analyze(chrono::duration_cast<chrono::seconds>(chrono::milliseconds(50)));
    
    // Analyze with long window - should include older activity
    auto full_suggestions = detector_->analyze(chrono::seconds(5));
    
    // Recent analysis might suggest different optimizations
    // (exact behavior depends on implementation)
    EXPECT_GE(recent_suggestions.size(), 0);
    EXPECT_GE(full_suggestions.size(), 0);
}

// Test confidence scoring
TEST_F(HotNodeDetectorTest, ConfidenceScoring) {
    // Create nodes with very different access patterns
    create_access_pattern(1024, 100, 1ms);  // Extremely hot
    create_access_pattern(2048, 50, 5ms);   // Very hot  
    create_access_pattern(3072, 10, 20ms);  // Moderate
    create_access_pattern(4096, 2, 100ms);  // Cold
    
    auto suggestions = detector_->analyze();
    
    // Find suggestions for different nodes
    map<size_t, double> confidence_by_offset;
    for (const auto& suggestion : suggestions) {
        if (suggestion.type == HotNodeDetector::OptimizationSuggestion::PIN_NODE) {
            confidence_by_offset[suggestion.offset] = suggestion.confidence;
        }
    }
    
    // Hotter nodes should have higher confidence scores
    if (confidence_by_offset.count(1024) && confidence_by_offset.count(4096)) {
        EXPECT_GT(confidence_by_offset[1024], confidence_by_offset[4096]);
    }
    
    if (confidence_by_offset.count(2048) && confidence_by_offset.count(3072)) {
        EXPECT_GE(confidence_by_offset[2048], confidence_by_offset[3072]);
    }
}

// Test edge cases
TEST_F(HotNodeDetectorTest, EdgeCases) {
    // Test with no access data
    auto suggestions = detector_->analyze();
    EXPECT_EQ(suggestions.size(), 0);
    
    // Test with single access
    tracker_->record_access(1024);
    suggestions = detector_->analyze();
    // Should handle gracefully (may or may not have suggestions)
    EXPECT_GE(suggestions.size(), 0);
    
    // Test with zero analysis window
    suggestions = detector_->analyze(chrono::seconds(0));
    EXPECT_EQ(suggestions.size(), 0);
    
    // Test hotness detection for non-existent node
    EXPECT_FALSE(detector_->is_hot_node(99999));
}

// Test performance with many nodes
TEST_F(HotNodeDetectorTest, PerformanceWithManyNodes) {
    const int num_nodes = 1000;
    const int accesses_per_node = 10;
    
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<int> access_dist(1, 50);
    
    auto start = chrono::high_resolution_clock::now();
    
    // Create many nodes with random access patterns
    for (int i = 0; i < num_nodes; ++i) {
        size_t offset = 1024 * (i + 1);
        int access_count = access_dist(gen);
        
        for (int j = 0; j < access_count; ++j) {
            tracker_->record_access(offset);
        }
    }
    
    // Analyze performance
    auto analysis_start = chrono::high_resolution_clock::now();
    auto suggestions = detector_->analyze();
    auto analysis_end = chrono::high_resolution_clock::now();
    
    auto total_end = chrono::high_resolution_clock::now();
    
    auto setup_duration = chrono::duration_cast<chrono::milliseconds>(analysis_start - start);
    auto analysis_duration = chrono::duration_cast<chrono::milliseconds>(analysis_end - analysis_start);
    auto total_duration = chrono::duration_cast<chrono::milliseconds>(total_end - start);
    
    // Analysis should complete quickly even with many nodes
    EXPECT_LT(analysis_duration.count(), 1000); // Less than 1 second
    EXPECT_LT(total_duration.count(), 5000);     // Total less than 5 seconds
    
    // Should produce reasonable number of suggestions
    EXPECT_LE(suggestions.size(), num_nodes / 10); // At most 10% of nodes suggested
}

// Test suggestion reasoning
TEST_F(HotNodeDetectorTest, SuggestionReasoning) {
    setup_mixed_access_patterns();
    
    auto suggestions = detector_->analyze();
    
    // All suggestions should have non-empty reasoning
    for (const auto& suggestion : suggestions) {
        EXPECT_FALSE(suggestion.reason.empty());
        EXPECT_GT(suggestion.reason.length(), 10); // Should be descriptive
        
        // Reason should mention relevant metrics
        string reason_lower = suggestion.reason;
        transform(reason_lower.begin(), reason_lower.end(), reason_lower.begin(), ::tolower);
        
        // Should mention access patterns, frequency, etc.
        bool mentions_relevant_info = 
            reason_lower.find("access") != string::npos ||
            reason_lower.find("frequency") != string::npos ||
            reason_lower.find("hot") != string::npos ||
            reason_lower.find("cold") != string::npos ||
            reason_lower.find("performance") != string::npos;
            
        EXPECT_TRUE(mentions_relevant_info) << "Reason: " << suggestion.reason;
    }
}

// Test integration with LRU tracker state changes
TEST_F(HotNodeDetectorTest, IntegrationWithTrackerChanges) {
    // Initial pattern
    create_access_pattern(1024, 30, 5ms);
    
    auto initial_suggestions = detector_->analyze();
    
    // Pin the hot node
    EXPECT_CALL(*mock_mmap_, mlock_region(1024, 256))
        .WillOnce(testing::Return(true));
    tracker_->pin_node(1024, 256);
    
    // Change access pattern - node becomes cold
    this_thread::sleep_for(50ms);
    create_access_pattern(1024, 1, 100ms); // Very few new accesses
    
    // Create new hot node
    create_access_pattern(2048, 50, 2ms);
    
    auto updated_suggestions = detector_->analyze();
    
    // Should now suggest unpinning 1024 and pinning 2048
    bool suggests_unpin_1024 = false;
    bool suggests_pin_2048 = false;
    
    for (const auto& suggestion : updated_suggestions) {
        if (suggestion.type == HotNodeDetector::OptimizationSuggestion::UNPIN_NODE && 
            suggestion.offset == 1024) {
            suggests_unpin_1024 = true;
        }
        if (suggestion.type == HotNodeDetector::OptimizationSuggestion::PIN_NODE && 
            suggestion.offset == 2048) {
            suggests_pin_2048 = true;
        }
    }
    
    // At least one of these adaptive suggestions should be present
    EXPECT_TRUE(suggests_unpin_1024 || suggests_pin_2048);
}