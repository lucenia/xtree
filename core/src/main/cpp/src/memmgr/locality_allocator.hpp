#pragma once

#include "hybrid_memmgr.hpp"
#include <chrono>

// Intelligent allocation strategy that creates natural locality
class LocalityAllocator {
protected:
    HybridMemoryManager memory_;
    
private:
    
    // Allocation contexts for different locality patterns
    struct AllocationContext {
        char* region_start;
        std::atomic<char*> current_pos;
        char* region_end;
        size_t chunk_size;
        std::string context_name;
        
        // Constructor
        AllocationContext() : region_start(nullptr), current_pos(nullptr), 
                            region_end(nullptr), chunk_size(0) {}
    };
    
    // Different allocation contexts for different use cases
protected:
    AllocationContext hot_root_context_;      // Root and high-level nodes
    AllocationContext hot_siblings_context_;   // Sibling nodes in same split
    AllocationContext hot_working_set_;        // Recently promoted nodes
    AllocationContext cold_bulk_context_;      // Cold storage
    
public:
    LocalityAllocator(size_t hot_size = 1*GB, size_t cold_size = 1*GB) 
        : memory_(hot_size, cold_size) {
        setup_contexts();
    }
    
    // Smart allocation based on tree operation context
    enum class AllocContext {
        ROOT_LEVEL,           // Root and near-root nodes
        SIBLING_GROUP,        // Nodes created during same split
        PROMOTED_HOT,         // Recently promoted from cold
        BULK_COLD            // Default cold allocation
    };
    
    template<typename NodeType>
    NodeType* allocate_node(AllocContext context, size_t count = 1) {
        AllocationContext* alloc_ctx = get_context(context);
        size_t total_size = sizeof(NodeType) * count;
        
        void* ptr = allocate_from_context(alloc_ctx, total_size);
        if (!ptr) {
            // Fallback allocation
            ptr = memory_.allocate_hot_fast(total_size);
            if (!ptr) {
                ptr = memory_.allocate_cold(total_size);
            }
        }
        
        return static_cast<NodeType*>(ptr);
    }
    
    // Batch allocation for splits - creates perfect locality
    template<typename NodeType>
    NodeType* allocate_sibling_batch(size_t sibling_count) {
        size_t total_size = sizeof(NodeType) * sibling_count;
        
        // Try to allocate all siblings contiguously
        void* ptr = allocate_from_context(&hot_siblings_context_, total_size);
        if (ptr) {
            return static_cast<NodeType*>(ptr);
        }
        
        // Fallback to regular allocation
        return allocate_node<NodeType>(AllocContext::SIBLING_GROUP, sibling_count);
    }
    
    // Allocate with explicit locality hint
    template<typename NodeType>
    NodeType* allocate_near(void* reference_node) {
        // Try to allocate near the reference node
        char* ref_ptr = static_cast<char*>(reference_node);
        
        // Check which region the reference is in
        if (is_in_hot_region(ref_ptr)) {
            return allocate_node<NodeType>(AllocContext::SIBLING_GROUP);
        } else {
            return allocate_node<NodeType>(AllocContext::BULK_COLD);
        }
    }

private:
    void setup_contexts() {
        char* hot_base = memory_.get_hot_base();
        char* cold_base = memory_.get_cold_base();
        
        // Partition hot region into contexts
        size_t hot_chunk = memory_.get_hot_size() / 3;
        
        hot_root_context_.region_start = hot_base;
        hot_root_context_.current_pos = hot_base;
        hot_root_context_.region_end = hot_base + hot_chunk;
        hot_root_context_.chunk_size = hot_chunk;
        hot_root_context_.context_name = "hot_root";
        
        hot_siblings_context_.region_start = hot_base + hot_chunk;
        hot_siblings_context_.current_pos = hot_base + hot_chunk;
        hot_siblings_context_.region_end = hot_base + (hot_chunk * 2);
        hot_siblings_context_.chunk_size = hot_chunk;
        hot_siblings_context_.context_name = "hot_siblings";
        
        hot_working_set_.region_start = hot_base + (hot_chunk * 2);
        hot_working_set_.current_pos = hot_base + (hot_chunk * 2);
        hot_working_set_.region_end = hot_base + (hot_chunk * 3);
        hot_working_set_.chunk_size = hot_chunk;
        hot_working_set_.context_name = "hot_working";
        
        // Cold region uses single context
        cold_bulk_context_.region_start = cold_base;
        cold_bulk_context_.current_pos = cold_base;
        cold_bulk_context_.region_end = cold_base + memory_.get_cold_size();
        cold_bulk_context_.chunk_size = memory_.get_cold_size();
        cold_bulk_context_.context_name = "cold_bulk";
    }
    
protected:
    AllocationContext* get_context(AllocContext ctx) {
        switch (ctx) {
            case AllocContext::ROOT_LEVEL: return &hot_root_context_;
            case AllocContext::SIBLING_GROUP: return &hot_siblings_context_;
            case AllocContext::PROMOTED_HOT: return &hot_working_set_;
            case AllocContext::BULK_COLD: return &cold_bulk_context_;
        }
        return &cold_bulk_context_;
    }
    
protected:
    void* allocate_from_context(AllocationContext* ctx, size_t size) {
        char* current = ctx->current_pos.load(std::memory_order_relaxed);
        char* new_pos = current + size;
        
        if (new_pos <= ctx->region_end) {
            while (!ctx->current_pos.compare_exchange_weak(
                current, new_pos,
                std::memory_order_relaxed, std::memory_order_relaxed)) {
                new_pos = current + size;
                if (new_pos > ctx->region_end) {
                    return nullptr;
                }
            }
            return current;
        }
        return nullptr;
    }
    
    // Delegate to memory manager
    bool is_in_hot_region(void* ptr) {
        return memory_.is_in_hot_region(ptr);
    }
};
// Example tree implementations removed - see integration with actual XTree