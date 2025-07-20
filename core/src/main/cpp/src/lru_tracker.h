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

#pragma once

#include <unordered_map>
#include <chrono>
#include <vector>
#include <memory>
#include "mmapfile.h"

namespace xtree {

/**
 * Statistics for a tracked node
 */
struct NodeStats {
    size_t access_count = 0;
    std::chrono::steady_clock::time_point last_access;
    std::chrono::steady_clock::time_point first_access;
    bool is_pinned = false;
    size_t size = 0;
    
    // Calculate access frequency (accesses per second)
    double get_access_frequency() const {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            last_access - first_access);
        if (duration.count() == 0) return 0.0;
        // Convert to accesses per second
        return static_cast<double>(access_count) * 1000.0 / duration.count();
    }
};

/**
 * LRU access tracker for memory-mapped nodes
 * 
 * Tracks access patterns without owning the actual data.
 * Used for statistics, hot node detection, and memory pinning decisions.
 */
class LRUAccessTracker {
public:
    explicit LRUAccessTracker(MMapFile* mmap_file, size_t max_tracked_nodes = 10000);
    ~LRUAccessTracker();
    
    /**
     * Record an access to a node at given offset
     */
    void record_access(size_t offset);
    
    /**
     * Pin a node in memory using mlock
     * @param offset File offset of the node
     * @param size Size of the node in bytes
     * @return true if successful
     */
    bool pin_node(size_t offset, size_t size);
    
    /**
     * Unpin a node
     * @param offset File offset of the node
     * @param size Size of the node in bytes
     * @return true if successful
     */
    bool unpin_node(size_t offset, size_t size);
    
    /**
     * Get statistics for a node
     * @param offset File offset of the node
     * @return Pointer to stats, or nullptr if not tracked
     */
    const NodeStats* get_node_stats(size_t offset) const;
    
    /**
     * Get the N most frequently accessed nodes
     * @param n Number of hot nodes to return
     * @return Vector of (offset, stats) pairs sorted by access frequency
     */
    std::vector<std::pair<size_t, NodeStats>> get_hot_nodes(size_t n = 10) const;
    
    /**
     * Get nodes that might be good candidates for pinning
     * Based on access frequency and recency
     */
    std::vector<size_t> get_pin_candidates(size_t max_candidates = 5) const;
    
    /**
     * Get total number of tracked nodes
     */
    size_t get_tracked_count() const { return stats_.size(); }
    
    /**
     * Get total number of pinned nodes
     */
    size_t get_pinned_count() const { return pinned_count_; }
    
    /**
     * Clear all tracking data (but keep pinned nodes pinned)
     */
    void clear_stats();
    
    /**
     * Remove old/stale entries to keep memory usage bounded
     * Removes nodes that haven't been accessed recently
     */
    void cleanup_stale_entries();
    
    /**
     * Get memory usage of the tracker itself
     */
    size_t get_memory_usage() const;
    
private:
    MMapFile* mmap_file_;
    size_t max_tracked_nodes_;
    size_t pinned_count_;
    
    // Map from file offset to node statistics
    std::unordered_map<size_t, NodeStats> stats_;
    
    // Doubly linked list for LRU ordering (for eviction from stats_)
    struct ListNode {
        size_t offset;
        ListNode* prev;
        ListNode* next;
    };
    
    ListNode* lru_head_;
    ListNode* lru_tail_;
    std::unordered_map<size_t, ListNode*> offset_to_node_;
    
    void move_to_front(size_t offset);
    void remove_lru_node();
    void add_to_front(size_t offset);
};

/**
 * Hot node detector for tree optimization decisions
 * 
 * Analyzes access patterns to suggest optimizations like:
 * - Shard reallocation (move hot nodes to faster storage)
 * - Thread affinity (assign hot subtrees to specific threads)
 * - Caching strategies
 */
class HotNodeDetector {
public:
    explicit HotNodeDetector(LRUAccessTracker* tracker);
    
    struct OptimizationSuggestion {
        enum Type {
            PIN_NODE,           // Pin this node in memory
            UNPIN_NODE,         // Unpin this node to free memory
            THREAD_AFFINITY,    // Assign this subtree to a specific thread
            SHARD_RELOCATION,   // Move this node to faster storage
            PREFETCH_SUBTREE    // Prefetch child nodes
        };
        
        Type type;
        size_t offset;
        double confidence;  // 0.0 to 1.0
        std::string reason;
    };
    
    /**
     * Analyze current access patterns and suggest optimizations
     */
    std::vector<OptimizationSuggestion> analyze(
        std::chrono::seconds analysis_window = std::chrono::seconds(300));
    
    /**
     * Check if a node should be considered "hot"
     */
    bool is_hot_node(size_t offset, double threshold = 1.0) const;
    
private:
    LRUAccessTracker* tracker_;
    
    double calculate_hotness_score(const NodeStats& stats) const;
};

} // namespace xtree