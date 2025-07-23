/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Thread-safe wrapper for CompactAllocator with read-write lock support
 */

#pragma once

#include "compact_allocator.hpp"
#include <shared_mutex>
#include <atomic>
#include <vector>

namespace xtree {

/**
 * Concurrent wrapper for CompactAllocator that provides:
 * 1. Thread-safe allocation
 * 2. Multiple concurrent readers (searches)
 * 3. Exclusive writer access when needed
 * 4. Epoch-based memory reclamation for safe concurrent operations
 */
class ConcurrentCompactAllocator {
private:
    std::unique_ptr<CompactAllocator> allocator_;
    mutable std::shared_mutex rw_mutex_;  // Allows multiple readers, single writer
    
    // Epoch-based memory management for safe reclamation
    struct Epoch {
        std::atomic<uint64_t> global_epoch{0};
        thread_local static uint64_t local_epoch;
        std::vector<std::pair<void*, uint64_t>> deferred_deletes;
        std::mutex delete_mutex;
    };
    
    Epoch epoch_;
    
public:
    using offset_t = CompactAllocator::offset_t;
    static constexpr offset_t INVALID_OFFSET = CompactAllocator::INVALID_OFFSET;
    
    explicit ConcurrentCompactAllocator(size_t initial_size = 64 * 1024 * 1024,
                                       CompactAllocator::SegmentStrategy strategy = 
                                       CompactAllocator::DEFAULT_STRATEGY)
        : allocator_(std::make_unique<CompactAllocator>(initial_size, strategy)) {
    }
    
    // Constructor for loading from MMAP
    ConcurrentCompactAllocator(void* mmap_base, size_t size, size_t used_size,
                              CompactAllocator::SegmentStrategy strategy = 
                              CompactAllocator::DEFAULT_STRATEGY)
        : allocator_(std::make_unique<CompactAllocator>(mmap_base, size, used_size, strategy)) {
    }
    
    // Allocate memory (requires exclusive write lock)
    offset_t allocate(size_t size) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->allocate(size);
    }
    
    // Get pointer for reading (shared lock)
    template<typename T = void>
    const T* get_ptr_read(offset_t offset) const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_ptr<T>(offset);
    }
    
    // Get pointer for writing (exclusive lock)
    template<typename T = void>
    T* get_ptr_write(offset_t offset) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_ptr<T>(offset);
    }
    
    // Get offset from pointer (read operation)
    offset_t get_offset(const void* ptr) const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_offset(ptr);
    }
    
    // Snapshot operations (require exclusive access)
    const void* get_arena_base() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_arena_base();
    }
    
    void* get_arena_base() {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_arena_base();
    }
    
    size_t get_used_size() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_used_size();
    }
    
    void set_used_size(size_t used_size) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        allocator_->set_used_size(used_size);
    }
    
    size_t get_arena_size() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->get_arena_size();
    }
    
    bool is_mmap_backed() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return allocator_->is_mmap_backed();
    }
    
    // Typed allocation helpers with thread safety
    template<typename T>
    struct ConcurrentTypedPtr {
        offset_t offset;
        ConcurrentCompactAllocator* allocator;
        
        const T* get_read() const { 
            return allocator->get_ptr_read<T>(offset); 
        }
        
        T* get_write() { 
            return allocator->get_ptr_write<T>(offset); 
        }
        
        operator bool() const { return offset != INVALID_OFFSET; }
    };
    
    template<typename T>
    ConcurrentTypedPtr<T> allocate_typed() {
        offset_t offset = allocate(sizeof(T));
        return ConcurrentTypedPtr<T>{offset, this};
    }
    
    template<typename T>
    ConcurrentTypedPtr<T> allocate_array(size_t count) {
        offset_t offset = allocate(sizeof(T) * count);
        return ConcurrentTypedPtr<T>{offset, this};
    }
    
    // Enter read epoch (for safe traversal)
    class ReadEpochGuard {
    private:
        ConcurrentCompactAllocator* allocator_;
        
    public:
        explicit ReadEpochGuard(ConcurrentCompactAllocator* alloc) 
            : allocator_(alloc) {
            // Enter read epoch
            Epoch::local_epoch = allocator_->epoch_.global_epoch.load();
        }
        
        ~ReadEpochGuard() {
            // Exit read epoch
            Epoch::local_epoch = 0;
        }
    };
    
    ReadEpochGuard enter_read_epoch() {
        return ReadEpochGuard(this);
    }
    
    // Advance epoch (for writers)
    void advance_epoch() {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        epoch_.global_epoch.fetch_add(1);
        
        // Process deferred deletions from old epochs
        // (implementation depends on memory reclamation strategy)
    }
    
    // Get underlying allocator (for snapshot operations)
    CompactAllocator* get_allocator() {
        // Note: Caller must ensure proper synchronization
        return allocator_.get();
    }
};

// Thread-local epoch tracking - moved to implementation file to avoid duplicate symbols

} // namespace xtree