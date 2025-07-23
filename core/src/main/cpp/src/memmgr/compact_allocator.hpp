/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

 
 /*
 * Compact memory allocator with relative offsets for fast snapshot/reload
 */

#pragma once

#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <iostream>
#include "cow_memmgr.hpp"
#include "../util/log.h"

namespace xtree {

/**
 * Compact allocator that allocates from a contiguous memory region
 * and uses relative offsets instead of pointers. This enables:
 * 1. Zero-copy snapshot persistence (just write the memory block)
 * 2. Instant reload (just mmap the file)
 * 3. No pointer fixup needed
 * 4. Compact memory layout (no fragmentation)
 */
class CompactAllocator {
public:
    // Configurable segmented offset strategy
    // Default: 10-bit segment (1024 segments) × 4GB = 4TB total capacity
    // Can be configured for different trade-offs:
    //   - 6-bit segment (64 segments) × 4GB = 256GB (fastest)
    //   - 8-bit segment (256 segments) × 4GB = 1TB (fast)
    //   - 10-bit segment (1024 segments) × 4GB = 4TB (balanced - default)
    //   - 12-bit segment (4K segments) × 4GB = 16TB (high capacity)
    //   - 16-bit segment (65K segments) × 4GB = 256TB (maximum capacity)
    using offset_t = uint64_t;  
    static constexpr offset_t INVALID_OFFSET = 0;
    
    enum class SegmentStrategy {
        FAST_256GB = 6,      // 6-bit segment ID, fastest access
        FAST_1TB = 8,        // 8-bit segment ID, fast access
        BALANCED_4TB = 10,   // 10-bit segment ID, good balance (default)
        LARGE_16TB = 12,     // 12-bit segment ID, large capacity
        HUGE_256TB = 16      // 16-bit segment ID, maximum capacity
    };
    
    // Configuration (can be set at compile time or runtime)
    static constexpr SegmentStrategy DEFAULT_STRATEGY = SegmentStrategy::BALANCED_4TB;
    
private:
    // Runtime configuration
    uint32_t segment_bits_;
    uint32_t offset_bits_;
    uint64_t segment_mask_;
    uint64_t offset_mask_;
    uint64_t segment_size_;
    uint32_t max_segments_;
    
private:
    // Segmented memory arena
    struct Segment {
        std::unique_ptr<char[]> data;
        size_t size;
        size_t used;
    };
    
    std::vector<Segment> segments_;
    std::atomic<uint32_t> current_segment_{0};
    std::atomic<size_t> current_offset_{0}; // Offset within current segment
    size_t total_size_{0};
    size_t initial_segment_size_{16 * 1024 * 1024}; // Initial segment size
    
    // For MMAP mode
    void* mmap_base_ = nullptr;
    bool is_mmap_ = false;
    
    // Thread safety
    mutable std::mutex alloc_mutex_;  // For allocation operations
    mutable std::shared_mutex segments_mutex_;  // For segment vector access
    
    // Page alignment for optimal cache performance
    static size_t get_page_alignment() {
        // Use the same cached page size as the rest of the system
        return xtree::PageAlignedMemoryTracker::get_cached_page_size();
    }
    
    static size_t align_up(size_t size) {
        size_t alignment = get_page_alignment();
        return (size + alignment - 1) & ~(alignment - 1);
    }
    
    void initialize_strategy(SegmentStrategy strategy) {
        segment_bits_ = static_cast<uint32_t>(strategy);
        offset_bits_ = 32;  // Always use 32 bits for offset within segment
        segment_mask_ = ((1ULL << segment_bits_) - 1) << offset_bits_;
        offset_mask_ = (1ULL << offset_bits_) - 1;
        segment_size_ = 1ULL << offset_bits_;  // 4GB per segment
        max_segments_ = 1U << segment_bits_;
    }

public:
    explicit CompactAllocator(size_t initial_size = 64 * 1024 * 1024, 
                             SegmentStrategy strategy = DEFAULT_STRATEGY)
        : segment_size_(1ULL << 32) {  // 4GB segments
        initialize_strategy(strategy);
        // Create first segment
        segments_.emplace_back();
        segments_[0].size = std::min(initial_size, static_cast<size_t>(segment_size_));
        segments_[0].data = std::make_unique<char[]>(segments_[0].size);
        segments_[0].used = 0;
        
        // Start allocation at page boundary for optimal alignment
        size_t page_size = get_page_alignment();
        current_offset_ = page_size; // First page reserved for metadata/null
        segments_[0].used = page_size;
        
        // Initialize first page to zeros (for null offset handling)
        std::memset(segments_[0].data.get(), 0, page_size);
        
        total_size_ = segments_[0].size;
    }
    
    // Constructor for loading from MMAP
    CompactAllocator(void* mmap_base, size_t size, size_t used_size,
                     SegmentStrategy strategy = DEFAULT_STRATEGY)
        : mmap_base_(mmap_base),
          is_mmap_(true) {
        initialize_strategy(strategy);
        // For MMAP mode, we treat the entire mapped region as one segment
        segments_.emplace_back();
        segments_[0].size = size;
        segments_[0].data = nullptr; // Will use mmap_base_ instead
        segments_[0].used = used_size;
        current_offset_ = used_size;
        total_size_ = size;
    }
    
    ~CompactAllocator() = default;
    
    // Allocate memory and return segmented offset
    offset_t allocate(size_t size) {
        if (size == 0) return INVALID_OFFSET;
        
        size = align_up(size);
        
        std::lock_guard<std::mutex> lock(alloc_mutex_);
        
        // Try to allocate in current segment
        uint32_t seg_id = current_segment_.load();
        size_t offset_in_seg = current_offset_.load();
        
        // Debug output
        {
            std::shared_lock<std::shared_mutex> lock(segments_mutex_);
            if (seg_id >= segments_.size()) {
                trace() << "[CompactAllocator::allocate] ERROR: current_segment_ (" << seg_id 
                          << ") >= segments_.size() (" << segments_.size() << ")\n";
            }
        }
        
        if (seg_id < segments_.size() && offset_in_seg + size <= segments_[seg_id].size) {
            // Fits in current segment
            current_offset_ += size;
            segments_[seg_id].used = offset_in_seg + size;
            
            // Memory barrier to ensure write visibility
            std::atomic_thread_fence(std::memory_order_release);
            
            // Encode segment:offset
            return (static_cast<offset_t>(seg_id) << offset_bits_) | offset_in_seg;
        }
        
        // Need new segment
        // For MMAP allocator, segment 0 is MMAP-backed, additional segments are heap-allocated
        // This provides a hybrid approach: fast MMAP loading + ability to grow
        
        if (segments_.size() >= max_segments_) {
            trace() << "[CompactAllocator] ERROR: Maximum segments (" << max_segments_ << ") reached!\n";
            trace() << "Current segment: " << seg_id << ", segments size: " << segments_.size() << "\n";
            throw std::runtime_error("Maximum number of segments reached");
        }
        
        // Create new segment - need exclusive lock for segment vector modification
        {
            std::unique_lock<std::shared_mutex> seg_lock(segments_mutex_);
            std::cout << "[CompactAllocator] Growing: adding segment " << segments_.size() 
                      << " (current used: " << (total_size_ / (1024*1024)) << " MB)\n";
            segments_.emplace_back();
            seg_id = segments_.size() - 1;
            segments_[seg_id].size = segment_size_;
            segments_[seg_id].data = std::make_unique<char[]>(segment_size_);
            segments_[seg_id].used = size;
            
            // Initialize the allocated region to ensure memory is committed
            std::memset(segments_[seg_id].data.get(), 0, size);
            
            std::cout << "[CompactAllocator] New segment " << seg_id << " created with size: " 
                      << (segment_size_ / (1024*1024*1024)) << " GB\n";
        }
        
        current_segment_ = seg_id;
        current_offset_ = size;
        total_size_ += segment_size_;
        
        // Memory barrier to ensure segment creation is visible
        std::atomic_thread_fence(std::memory_order_release);
        
        // Return offset in new segment
        return (static_cast<offset_t>(seg_id) << offset_bits_) | 0;
    }
    
    // Convert offset to pointer
    template<typename T = void>
    T* get_ptr(offset_t offset) {
        if (offset == INVALID_OFFSET) return nullptr;
        
        // Memory barrier to ensure we see all writes
        std::atomic_thread_fence(std::memory_order_acquire);
        
        // Extract segment and offset
        uint32_t seg_id = offset >> offset_bits_;
        uint32_t offset_in_seg = offset & offset_mask_;
        
        // Shared lock for reading segment info
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        
        if (seg_id >= segments_.size()) {
            trace() << "[CompactAllocator::get_ptr] ERROR: seg_id=" << seg_id 
                      << " >= segments_.size()=" << segments_.size() 
                      << " (offset=" << offset << ")\n";
            return nullptr;
        }
        
        // For MMAP allocator: segment 0 uses mmap_base_, others use heap
        char* base;
        if (is_mmap_ && seg_id == 0) {
            base = static_cast<char*>(mmap_base_);
        } else {
            base = segments_[seg_id].data.get();
            if (!base) {
                trace() << "[CompactAllocator::get_ptr] ERROR: segment " << seg_id 
                          << " has null data pointer!\n";
                return nullptr;
            }
        }
        return reinterpret_cast<T*>(base + offset_in_seg);
    }
    
    // Convert offset to const pointer
    template<typename T = void>
    const T* get_ptr(offset_t offset) const {
        if (offset == INVALID_OFFSET) return nullptr;
        
        // Memory barrier to ensure we see all writes
        std::atomic_thread_fence(std::memory_order_acquire);
        
        // Extract segment and offset
        uint32_t seg_id = offset >> offset_bits_;
        uint32_t offset_in_seg = offset & offset_mask_;
        
        // Shared lock for reading segment info
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        
        if (seg_id >= segments_.size()) return nullptr;
        
        // For MMAP allocator: segment 0 uses mmap_base_, others use heap
        const char* base = (is_mmap_ && seg_id == 0) ? static_cast<const char*>(mmap_base_) : segments_[seg_id].data.get();
        return reinterpret_cast<const T*>(base + offset_in_seg);
    }
    
    // Convert pointer back to offset (for existing pointers)
    offset_t get_offset(const void* ptr) const {
        if (!ptr) return INVALID_OFFSET;
        
        const char* p = static_cast<const char*>(ptr);
        
        // Shared lock for reading segment info
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        
        // Search through segments to find which one contains this pointer
        for (size_t seg_id = 0; seg_id < segments_.size(); ++seg_id) {
            const char* base = (is_mmap_ && seg_id == 0) ? static_cast<const char*>(mmap_base_) : segments_[seg_id].data.get();
            ptrdiff_t diff = p - base;
            
            if (diff >= 0 && static_cast<size_t>(diff) < segments_[seg_id].used) {
                // Found the segment containing this pointer
                return (static_cast<offset_t>(seg_id) << offset_bits_) | static_cast<uint32_t>(diff);
            }
        }
        
        return INVALID_OFFSET;
    }
    
    // Get base pointer for snapshot (only valid for single segment)
    const void* get_arena_base() const {
        if (segments_.empty()) return nullptr;
        return is_mmap_ ? mmap_base_ : segments_[0].data.get();
    }
    
    // Get mutable base pointer (for loading - only valid for single segment)
    void* get_arena_base() {
        if (segments_.empty()) return nullptr;
        return is_mmap_ ? mmap_base_ : segments_[0].data.get();
    }
    
    // Get current allocation size (total across all segments)
    size_t get_used_size() const {
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        size_t total = 0;
        for (const auto& seg : segments_) {
            total += seg.used;
        }
        return total;
    }
    
    // Set used size (for loading from snapshot - only works with single segment)
    void set_used_size(size_t used_size) {
        if (!segments_.empty()) {
            segments_[0].used = used_size;
            current_offset_.store(used_size, std::memory_order_relaxed);
        }
    }
    
    // Get total arena size
    size_t get_arena_size() const {
        return total_size_;
    }
    
    // Check if using MMAP
    bool is_mmap_backed() const { return is_mmap_; }
    
    // Multi-segment snapshot support
    size_t get_segment_count() const { 
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        return segments_.size(); 
    }
    
    // Get segment data for snapshot
    std::pair<const void*, size_t> get_segment_data(size_t seg_id) const {
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        
        if (seg_id >= segments_.size()) {
            return {nullptr, 0};
        }
        
        const void* data = (is_mmap_ && seg_id == 0) 
            ? mmap_base_ 
            : segments_[seg_id].data.get();
            
        return {data, segments_[seg_id].used};
    }
    
    // Get segment size
    size_t get_segment_size(size_t seg_id) const {
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        if (seg_id >= segments_.size()) return 0;
        return segments_[seg_id].size;
    }
    
    // Load a segment from snapshot data
    void load_segment_from_snapshot(const void* data, size_t segment_size, size_t used_size) {
        // Need exclusive lock for modifying segments
        std::unique_lock<std::shared_mutex> lock(segments_mutex_);
        
        // Add a new segment
        size_t seg_id = segments_.size();
        segments_.emplace_back();
        auto& segment = segments_[seg_id];
        
        // Allocate memory for the segment
        segment.data = std::unique_ptr<char[]>(new char[segment_size]);
        segment.size = segment_size;
        segment.used = used_size;
        
        // Copy the snapshot data
        std::memcpy(segment.data.get(), data, used_size);
        
        // Update total size
        total_size_ += segment_size;
        
        std::cout << "[CompactAllocator::load_segment_from_snapshot] Loaded segment " 
                  << seg_id << " with " << (used_size / (1024.0 * 1024.0)) << " MB used of "
                  << (segment_size / (1024.0 * 1024.0)) << " MB total\n";
    }
    
    // Restore allocator state after loading segments
    void restore_state_after_load(uint32_t last_segment_id, size_t last_segment_used) {
        // Make sure the segment actually exists
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        
        // Find the actual last segment that was loaded
        uint32_t actual_last_segment = segments_.size() - 1;
        
        if (last_segment_id >= segments_.size()) {
            std::cout << "[CompactAllocator::restore_state_after_load] WARNING: last_segment_id=" 
                      << last_segment_id << " but only " << segments_.size() << " segments loaded. "
                      << "Using last loaded segment: " << actual_last_segment << "\n";
            last_segment_id = actual_last_segment;
            // Get the actual used size of the last loaded segment
            last_segment_used = segments_[actual_last_segment].used;
        }
        
        // CRITICAL: Only set current_segment_ if that segment has room for more allocations
        // Otherwise, the next allocation will create a new segment
        if (last_segment_used < segments_[last_segment_id].size) {
            // There's still room in the last segment
            current_segment_ = last_segment_id;
            current_offset_ = last_segment_used;
        } else {
            // Last segment is full, prepare to create a new one
            // But don't actually create it yet - let allocate() handle that
            current_segment_ = last_segment_id;
            current_offset_ = segments_[last_segment_id].size; // Mark as full
        }
        
        std::cout << "[CompactAllocator::restore_state_after_load] State restored: "
                  << "current_segment=" << current_segment_.load() 
                  << ", current_offset=" << current_offset_.load() 
                  << ", segment size=" << segments_[last_segment_id].size << "\n";
    }
    
    // Typed allocation helper
    template<typename T>
    struct TypedPtr {
        offset_t offset;
        CompactAllocator* allocator;
        
        T* get() { return allocator->get_ptr<T>(offset); }
        const T* get() const { return allocator->get_ptr<T>(offset); }
        T* operator->() { return get(); }
        const T* operator->() const { return get(); }
        T& operator*() { return *get(); }
        const T& operator*() const { return *get(); }
        operator bool() const { return offset != INVALID_OFFSET; }
    };
    
    // Allocate typed object
    template<typename T>
    TypedPtr<T> allocate_typed() {
        offset_t offset = allocate(sizeof(T));
        return TypedPtr<T>{offset, this};
    }
    
    // Allocate typed array
    template<typename T>
    TypedPtr<T> allocate_array(size_t count) {
        offset_t offset = allocate(sizeof(T) * count);
        return TypedPtr<T>{offset, this};
    }
};

} // namespace xtree