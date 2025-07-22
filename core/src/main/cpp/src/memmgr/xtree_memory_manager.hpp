#pragma once

#include "locality_allocator.hpp"
#include <utility>
#include <cstddef>

namespace xtree {

// Forward declarations
template<typename Record, bool UseMMap> class XTreeBucket;
class KeyMBR;

/**
 * XTree-specific memory manager that understands XTree allocation patterns
 * and optimizes for 500K QPS performance
 */
template<typename Record>
class XTreeMemoryManager : public LocalityAllocator {
public:
    XTreeMemoryManager(size_t hot_size = 1*GB, size_t cold_size = 1*GB) 
        : LocalityAllocator(hot_size, cold_size) {}
    
    /**
     * Allocate an XTreeBucket with smart hot/cold decision
     * This supports the hybrid approach where tree nodes can be hot or cold
     * based on access patterns and tree depth
     */
    template<bool UseMMap>
    XTreeBucket<Record, UseMMap>* allocate_bucket(bool isRoot, XTreeBucket<Record, UseMMap>* parent = nullptr, 
                                                  int depth = 0, bool preferHot = false) {
        AllocContext ctx;
        void* ptr = nullptr;
        
        // Decision logic for hot vs cold allocation
        if (isRoot || depth < 3) {
            // Root and top levels always hot for fast access
            ctx = AllocContext::ROOT_LEVEL;
            ptr = allocate_node<XTreeBucket<Record, UseMMap>>(ctx);
        } else if (preferHot || (parent && is_in_hot_region(parent))) {
            // Allocate hot if explicitly requested or parent is hot
            ctx = AllocContext::SIBLING_GROUP;
            ptr = allocate_node<XTreeBucket<Record, UseMMap>>(ctx);
        } else {
            // Default to cold allocation for deeper nodes
            // This allows the tree to grow beyond hot memory limits
            ctx = AllocContext::BULK_COLD;
            ptr = memory_.allocate_cold(sizeof(XTreeBucket<Record, UseMMap>));
        }
        
        // Track allocation
        if (ptr) {
            bucket_allocations_++;
            if (is_in_hot_region(ptr)) {
                hot_bucket_count_++;
            } else {
                cold_bucket_count_++;
            }
        }
        
        return static_cast<XTreeBucket<Record, UseMMap>*>(ptr);
    }
    
    /**
     * Allocate siblings during split - PERFECT LOCALITY!
     * Both siblings are allocated contiguously for cache efficiency
     * Respects hot/cold decision based on parent location
     */
    template<bool UseMMap>
    std::pair<XTreeBucket<Record, UseMMap>*, XTreeBucket<Record, UseMMap>*> allocate_split_siblings(
        XTreeBucket<Record, UseMMap>* parent = nullptr, int depth = 0) {
        
        void* left_ptr = nullptr;
        void* right_ptr = nullptr;
        
        // Decide hot or cold based on parent and depth
        bool use_hot = (depth < 3) || (parent && is_in_hot_region(parent));
        
        if (use_hot) {
            // Try contiguous hot allocation for perfect locality
            auto* siblings = allocate_sibling_batch<XTreeBucket<Record, UseMMap>>(2);
            if (siblings) {
                hot_bucket_count_ += 2;
                bucket_allocations_ += 2;
                return {&siblings[0], &siblings[1]};
            }
        } else {
            // Cold allocation - still try for contiguous
            size_t sibling_size = sizeof(XTreeBucket<Record, UseMMap>) * 2;
            void* cold_mem = memory_.allocate_cold(sibling_size);
            if (cold_mem) {
                left_ptr = cold_mem;
                right_ptr = static_cast<char*>(cold_mem) + sizeof(XTreeBucket<Record, UseMMap>);
                cold_bucket_count_ += 2;
                bucket_allocations_ += 2;
                return {static_cast<XTreeBucket<Record, UseMMap>*>(left_ptr),
                        static_cast<XTreeBucket<Record, UseMMap>*>(right_ptr)};
            }
        }
        
        // Fallback to individual allocation
        auto* left = allocate_bucket<UseMMap>(false, parent, depth);
        auto* right = allocate_bucket<UseMMap>(false, parent, depth);
        return {left, right};
    }
    
    /**
     * Allocate batch of nodes for supernode expansion
     * Supernodes need extra child nodes, allocate them together for locality
     */
    template<bool UseMMap>
    void* allocate_node_batch(size_t total_size, XTreeBucket<Record, UseMMap>* parent, int depth) {
        // Supernodes can occur at any level (including root) when data has high overlap
        // For now, just use cold allocation for simplicity since supernodes are rare
        // and we want to preserve hot memory for regular tree nodes
        return memory_.allocate_cold(total_size);
    }
    
    /**
     * Allocate KeyMBR near its bucket for locality
     * Returns raw memory - caller must construct with placement new
     */
    void* allocate_keymbr_memory(void* near_bucket, int dimensions) {
        // Calculate actual size needed
        // KeyMBR base size + sortable integer array
        size_t key_size = 64 + 2 * dimensions * sizeof(int32_t);
        
        // Try to allocate near the bucket
        void* ptr = nullptr;
        if (near_bucket && is_in_hot_region(near_bucket)) {
            // Hot bucket - allocate key in hot region too
            ptr = allocate_from_context(&hot_siblings_context_, key_size);
        }
        
        if (!ptr) {
            // Fallback to general allocation
            ptr = memory_.allocate_hot_fast(key_size);
            if (!ptr) {
                ptr = memory_.allocate_cold(key_size);
            }
        }
        
        return ptr;
    }
    
    /**
     * Allocate DataRecord with its points
     * Returns raw memory - caller must construct
     */
    void* allocate_datarecord_memory(size_t point_count, int dimensions) {
        // Calculate size for DataRecord + embedded points
        size_t record_size = 256 + point_count * dimensions * sizeof(double);
        
        // Data records typically go to cold storage unless very small
        void* ptr = nullptr;
        if (record_size < 4*KB) {
            ptr = memory_.allocate_hot_fast(record_size);
        }
        
        if (!ptr) {
            ptr = memory_.allocate_cold(record_size);
        }
        
        return ptr;
    }
    
    /**
     * Get memory statistics
     */
    struct MemoryStats {
        size_t hot_used;
        size_t hot_total;
        size_t cold_used;
        size_t cold_total;
        double hot_utilization;
        double cold_utilization;
        size_t bucket_count;
        size_t hot_bucket_count;
        size_t cold_bucket_count;
        size_t key_count;
        size_t record_count;
    };
    
    MemoryStats get_memory_stats() const {
        auto base_stats = memory_.get_stats();
        MemoryStats stats;
        stats.hot_used = base_stats.hot_used;
        stats.hot_total = base_stats.hot_total;
        stats.cold_used = base_stats.cold_used;
        stats.cold_total = base_stats.cold_total;
        stats.hot_utilization = base_stats.hot_utilization;
        stats.cold_utilization = base_stats.cold_utilization;
        stats.bucket_count = bucket_allocations_.load();
        stats.hot_bucket_count = hot_bucket_count_.load();
        stats.cold_bucket_count = cold_bucket_count_.load();
        stats.key_count = key_allocations_.load();
        stats.record_count = record_allocations_.load();
        return stats;
    }
    
    /**
     * Direct access to memory manager for special cases
     */
    HybridMemoryManager& get_memory_manager() { return memory_; }
    
private:
    // Track allocation counts for monitoring
    std::atomic<size_t> bucket_allocations_{0};
    std::atomic<size_t> hot_bucket_count_{0};
    std::atomic<size_t> cold_bucket_count_{0};
    std::atomic<size_t> key_allocations_{0};
    std::atomic<size_t> record_allocations_{0};
    
    // Make LocalityAllocator members accessible
    using LocalityAllocator::memory_;
    using LocalityAllocator::hot_siblings_context_;
    using LocalityAllocator::allocate_from_context;
};

} // namespace xtree