/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * COW Memory Manager with Compact Allocator for fast snapshot/reload
 */

#pragma once

#include "cow_memmgr.hpp"
#include "compact_allocator.hpp"
#include "compact_snapshot_manager.hpp"
#include <memory>
#include <string>

namespace xtree {

/**
 * COW Memory Manager that uses compact allocator for ultra-fast reload
 * This extends DirectMemoryCOWManager to use CompactAllocator instead of
 * traditional heap allocation, enabling instant snapshot reload
 */
template<typename RecordType>
class CompactCOWManager : public DirectMemoryCOWManager<RecordType> {
private:
    std::unique_ptr<CompactSnapshotManager> snapshot_manager_;
    CompactAllocator* compact_allocator_ = nullptr;
    std::string snapshot_path_;
    
public:
    CompactCOWManager(size_t operations_threshold = 10000,
                      double memory_threshold_multiplier = 1.5,
                      size_t initial_arena_size = 64 * 1024 * 1024,
                      const std::string& snapshot_path = "xtree.snapshot")
        : DirectMemoryCOWManager<RecordType>(operations_threshold, memory_threshold_multiplier),
          snapshot_path_(snapshot_path) {
        
        // Create compact snapshot manager
        snapshot_manager_ = std::make_unique<CompactSnapshotManager>(
            snapshot_path, initial_arena_size
        );
        
        compact_allocator_ = snapshot_manager_->get_allocator();
        
        // If snapshot was loaded, update our tracking
        if (snapshot_manager_->is_snapshot_loaded()) {
            // Register the entire loaded memory region with COW tracker
            size_t used_size = compact_allocator_->get_used_size();
            if (used_size > sizeof(CompactAllocator::offset_t)) {
                void* base = compact_allocator_->get_ptr(sizeof(CompactAllocator::offset_t));
                this->memory_tracker_.register_memory_region(base, used_size - sizeof(CompactAllocator::offset_t));
            }
        }
    }
    
    ~CompactCOWManager() = default;
    
    // Override allocation to use compact allocator
    void* allocate_and_register(size_t size, bool prefer_huge_page = false) {
        if (!compact_allocator_) {
            throw std::runtime_error("Compact allocator not initialized");
        }
        
        // Allocate from compact allocator
        auto offset = compact_allocator_->allocate(size);
        if (offset == CompactAllocator::INVALID_OFFSET) {
            return nullptr;
        }
        
        void* ptr = compact_allocator_->get_ptr(offset);
        
        // Register with COW tracker
        this->memory_tracker_.register_memory_region(ptr, size);
        
        // Trigger snapshot if needed
        this->check_and_trigger_snapshot();
        
        return ptr;
    }
    
    // Override snapshot trigger to save compact snapshot
    void trigger_memory_snapshot() {
        if (this->commit_in_progress_.exchange(true)) {
            return;
        }
        
        // Reset counter
        this->operations_since_snapshot_ = 0;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        try {
            // Save compact snapshot
            snapshot_manager_->save_snapshot();
            
            auto save_time = std::chrono::high_resolution_clock::now() - start;
            auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(save_time);
            
            // Log performance
            std::cout << "Compact snapshot saved in " 
                      << microseconds.count() << " microseconds" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Failed to save compact snapshot: " << e.what() << std::endl;
        }
        
        this->commit_in_progress_ = false;
    }
    
    // Get compact allocator for direct use
    CompactAllocator* get_compact_allocator() {
        return compact_allocator_;
    }
    
    // Check if using snapshot
    bool is_snapshot_loaded() const {
        return snapshot_manager_ && snapshot_manager_->is_snapshot_loaded();
    }
    
    // Get snapshot size
    size_t get_snapshot_size() const {
        return snapshot_manager_ ? snapshot_manager_->get_snapshot_size() : 0;
    }
};

/**
 * Compact COW Allocator for XTree nodes
 * Uses compact allocator with relative offsets
 */
template<typename T>
class CompactCOWAllocator {
public:
    typedef T value_type;
    typedef CompactAllocator::offset_t offset_type;
    
    CompactCOWAllocator(CompactCOWManager<DataRecord>* cow_manager = nullptr) 
        : cow_manager_(cow_manager) {}
    
    template<typename U>
    CompactCOWAllocator(const CompactCOWAllocator<U>& other) 
        : cow_manager_(other.cow_manager_) {}
    
    T* allocate(size_t n) {
        if (!cow_manager_) {
            throw std::runtime_error("COW manager not set");
        }
        
        size_t size = n * sizeof(T);
        void* ptr = cow_manager_->allocate_and_register(size);
        
        if (!ptr) {
            throw std::bad_alloc();
        }
        
        // Zero initialize
        std::memset(ptr, 0, size);
        
        return static_cast<T*>(ptr);
    }
    
    void deallocate(T* ptr, size_t n) {
        // Compact allocator doesn't support deallocation
        // Memory is reclaimed when the entire arena is freed
    }
    
    // Convert pointer to offset for persistence
    offset_type to_offset(const T* ptr) const {
        if (!ptr || !cow_manager_) {
            return CompactAllocator::INVALID_OFFSET;
        }
        
        auto* allocator = cow_manager_->get_compact_allocator();
        return allocator->get_offset(ptr);
    }
    
    // Convert offset to pointer for loading
    T* from_offset(offset_type offset) const {
        if (offset == CompactAllocator::INVALID_OFFSET || !cow_manager_) {
            return nullptr;
        }
        
        auto* allocator = cow_manager_->get_compact_allocator();
        return allocator->get_ptr<T>(offset);
    }
    
    template<typename U>
    bool operator==(const CompactCOWAllocator<U>& other) const {
        return cow_manager_ == other.cow_manager_;
    }
    
    template<typename U>
    bool operator!=(const CompactCOWAllocator<U>& other) const {
        return !(*this == other);
    }
    
    CompactCOWManager<DataRecord>* cow_manager_;
};

} // namespace xtree