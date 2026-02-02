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
#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <array>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <cassert>
#include <iostream>
#ifdef _MSC_VER
#include <intrin.h>
#endif
#include <string>
#include <atomic>
#include <cassert>
#include "config.h"
#include "storage_config.h"
#include "platform_fs.h"
#include "file_handle_registry.h"
#include "mapping_manager.h"
#include "node_id.hpp"

// Branch prediction hints for hot path
#ifdef __GNUC__
#define LIKELY(x)   __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

// Force inline on MSVC for better performance
#ifdef _MSC_VER
#define FORCE_INLINE __forceinline
#else
#define FORCE_INLINE inline __attribute__((always_inline))
#endif

namespace xtree { 
    namespace persist {

        #ifndef NDEBUG
        // Debug counters to enforce O(1) guarantee - should always be 0
        extern std::atomic<uint64_t> g_segment_scan_count;  // Increment if a linear scan occurs
        extern std::atomic<uint64_t> g_segment_lock_count;  // Increment if any lock is taken in get_ptr()
        #endif

        class SegmentAllocator {
        public:
            // Size classes from config.h
            static constexpr uint8_t NUM_CLASSES = size_class::kNumClasses;
            static constexpr auto CLASS_SIZES = size_class::kSizes;
            static constexpr size_t MIN_CLASS_SIZE = size_class::kMinSize;
            static constexpr size_t MAX_CLASS_SIZE = size_class::kMaxSize;
            static constexpr size_t DEFAULT_SEGMENT_SIZE = segment::kDefaultSegmentSize;

            struct Allocation { 
                uint32_t file_id = 0;
                uint32_t segment_id = 0; 
                uint64_t offset = 0; 
                uint32_t length = 0; 
                uint8_t class_id = 0;
                
                // NEW: Pin for memory access (RAII handle) - only used for special cases
                mutable MappingManager::Pin pin;
                
                // Access the memory - delegates to get_ptr for O(1) lookup
                void* ptr() const; 
                
                // Check if this allocation is valid
                bool is_valid() const {
                    // An allocation is valid if it has non-zero length
                    return length > 0;
                }
            };

            // NEW: Constructor that takes the registries for windowed mmap
            SegmentAllocator(const std::string& data_dir,
                           FileHandleRegistry& fhr,
                           MappingManager& mm);
            
            // Constructor with explicit configuration
            SegmentAllocator(const std::string& data_dir,
                           FileHandleRegistry& fhr,
                           MappingManager& mm,
                           const StorageConfig& config);
            
            // Legacy constructor for backward compatibility (creates internal registries)
            explicit SegmentAllocator(const std::string& data_dir);
            
            // Constructor with config (creates internal registries)
            SegmentAllocator(const std::string& data_dir, const StorageConfig& config);

            // Destructor - ensures all pins are released before destruction
            ~SegmentAllocator();

            Allocation allocate(size_t size, NodeKind kind = NodeKind::Internal);
            void       free(Allocation& a);  // Non-const now (moves the pin)
            void       close_all();  // Close all segments and mappings for clean shutdown

            // Read-only mode for serverless readers
            void set_read_only(bool read_only) { read_only_ = read_only; }
            bool is_read_only() const { return read_only_; }

            // Lazy remapping support for multi-field memory scaling
            // Release pins from segments not accessed within threshold_ns (default 60s)
            // Returns number of pins released
            size_t release_cold_pins(uint64_t threshold_ns = 60000000000ULL);

            // Get count of currently pinned segments (for diagnostics)
            size_t get_pinned_segment_count() const;
            
            // PERFORMANCE: O(1) lookup using segment table
            FORCE_INLINE void* get_ptr(const Allocation& a) noexcept;
            
            // Recovery support: Get pointer for a specific location (O(1) with class_id)
            FORCE_INLINE void* get_ptr_for_recovery(uint8_t class_id,
                                                    uint32_t file_id,
                                                    uint32_t segment_id,
                                                    uint64_t offset,
                                                    uint32_t length) noexcept;
            
            // Helper methods for DurableStore's read_node_pinned
            std::string get_file_path(uint32_t file_id, bool is_data_file) const;
            size_t get_segment_size() const { return DEFAULT_SEGMENT_SIZE; }
            MappingManager& get_mapping_manager() { return *mapping_manager_; }

            struct Stats { 
                size_t live_bytes = 0;
                size_t dead_bytes = 0;
                size_t total_segments = 0;
                size_t active_segments = 0;
                size_t allocs_from_freelist = 0;  // Allocations served from free list
                size_t allocs_from_bump = 0;      // Allocations from bump pointer
                size_t allocs_from_bitmap = 0;    // Allocations from bitmap
                size_t frees_to_bitmap = 0;       // Frees handled by bitmap
                size_t total_allocations = 0;     // Total allocation requests
                size_t total_frees = 0;           // Total free operations
                double fragmentation() const {
                    size_t total = live_bytes + dead_bytes;
                    return total > 0 ? static_cast<double>(dead_bytes) / total : 0.0;
                }
                double freelist_hit_rate() const {
                    return total_allocations > 0 ? 
                        static_cast<double>(allocs_from_freelist) / total_allocations : 0.0;
                }
                double bitmap_hit_rate() const {
                    return total_allocations > 0 ? 
                        static_cast<double>(allocs_from_bitmap) / total_allocations : 0.0;
                }
            };
            Stats get_stats(uint8_t class_id) const;
            Stats get_total_stats() const;

            // Get info about segments
            size_t get_segment_count() const;
            size_t get_active_segment_count() const;
            
            // Segment utilization statistics
            struct SegmentUtilization {
                size_t total_segments = 0;
                size_t total_capacity = 0;
                size_t total_used = 0;
                size_t total_wasted = 0;
                double avg_utilization = 0.0;
                double min_utilization = 100.0;
                double max_utilization = 0.0;
                size_t segments_under_25_percent = 0;
                size_t segments_under_50_percent = 0;
                size_t segments_under_75_percent = 0;
            };
            SegmentUtilization get_segment_utilization() const;
            
        private:
            static uint8_t size_to_class(size_t sz);
            static size_t  class_to_size(uint8_t c);
            
            // Helper to compute block index during free
            static inline uint32_t block_index_from_offset(uint64_t base_offset,
                                                          uint64_t off,
                                                          uint32_t class_sz) {
                return uint32_t((off - base_offset) / class_sz);
            }
            
            struct Segment {
                uint32_t file_id;
                uint32_t segment_id;      // Dense 0..N-1 within class
                uint8_t  class_id;
                uint64_t base_offset;     // File-relative base byte offset
                size_t   capacity;        // Bytes in this segment
                size_t   used;            // Bytes used (for bump allocation)
                void*    base_vaddr = nullptr;  // Virtual address (valid only when pin is valid)

                // Lazy remapping support: pin can be released to allow mmap eviction
                MappingManager::Pin pin;  // RAII handle - can be empty if evicted
                mutable std::mutex remap_mutex;  // Protects remapping slow path
                bool writable = true;     // For remapping with correct permissions
                std::atomic<uint64_t> last_access_ns{0};  // LRU tracking for pin release
                
                // Bitmap allocation fields
                uint32_t blocks = 0;              // capacity / class_size
                uint32_t free_count = 0;          // number of free blocks
                uint32_t max_allocated = 0;       // high water mark of allocated blocks
                std::vector<uint64_t> bm;         // 1=free, 0=used
                
                bool has_space(size_t size) const {
                    return used + size <= capacity;
                }
                
                // Calculate utilization percentage (0-100)
                double utilization() const {
                    return capacity > 0 ? (static_cast<double>(used) * 100.0 / capacity) : 0.0;
                }
                
                // Calculate wasted bytes
                size_t wasted_bytes() const {
                    return capacity - used;
                }
                
                // Bitmap-based has_space check
                inline bool has_free_blocks() const noexcept { 
                    return free_count > 0; 
                }
                
                // Find index of a free bit, or -1 if none. Fast FFS over 64-bit words.
                inline int find_free_bit() const noexcept {
                    const size_t n = bm.size();
                    for (size_t w = 0; w < n; ++w) {
                        uint64_t word = bm[w];
                        if (word) {
                            #if defined(__GNUC__) || defined(__clang__)
                            int bit = __builtin_ctzll(word); // 0..63
                            #elif defined(_MSC_VER)
                            unsigned long bit;
                            _BitScanForward64(&bit, word);
                            return int(w * 64 + bit);
                            #else
                            // Portable fallback
                            int bit = 0; 
                            uint64_t t = word; 
                            while ((t & 1ull) == 0) { t >>= 1; ++bit; }
                            #endif
                            return int(w * 64 + bit);
                        }
                    }
                    return -1;
                }
                
                // REMOVED: get_ptr() - now using MappingManager pins
            };
            
            struct alignas(64) ClassAllocator {  // Cache-line aligned to prevent false sharing
                // O(1) segment lookup - published atomically
                static constexpr size_t kInitialSegments = 64;  // Start larger to avoid early resize
                
                // CRITICAL: Both root pointer and size must be atomic for lock-free reads
                // These hot atomics are at the start of the cache-line aligned struct
                std::atomic<std::atomic<Segment*>*> seg_table_root{nullptr};  // Atomic root pointer
                std::atomic<size_t> seg_table_size{0};  // Number of valid slots
                std::atomic<uint32_t> next_segment_id{0};
                
                // All segments owned by this class (for cleanup)
                std::vector<std::unique_ptr<Segment>> segments;
                
                // Active segment for new allocations
                Segment* active_segment = nullptr;
                
                // Creation/modification mutex (only for segment creation)
                mutable std::mutex create_mu;
                
                // Free list and stats
                std::vector<Allocation> free_list;
                size_t live_bytes = 0;
                size_t dead_bytes = 0;
                
                // Per-class file management
                uint32_t current_file_seq = 0;        // Current file sequence for this class
                size_t bytes_in_current_file = 0;     // Bytes used in current file
                
                // Statistics
                size_t allocs_from_freelist = 0;
                size_t allocs_from_bump = 0;
                size_t allocs_from_bitmap = 0;
                size_t frees_to_bitmap = 0;
                size_t total_allocations = 0;
                size_t total_frees = 0;
                
                // Retired segment tables for safe memory reclamation
                std::vector<void*> retired_tables;
                
                mutable std::mutex mu;  // Mutable to allow locking in const methods
                
                ClassAllocator() {
                    free_list.reserve(1024);  // Pre-reserve to avoid reallocation churn
                    
                    // Initialize segment table with reasonable initial capacity
                    const size_t cap = kInitialSegments;
                    auto* table = new std::atomic<Segment*>[cap];
                    for (size_t i = 0; i < cap; ++i) {
                        table[i].store(nullptr, std::memory_order_relaxed);
                    }
                    // Publish table root first, then size
                    seg_table_root.store(table, std::memory_order_release);
                    seg_table_size.store(cap, std::memory_order_release);
                }
                
                ~ClassAllocator() {
                    // Clean up the active atomic table
                    auto* table = seg_table_root.load(std::memory_order_relaxed);
                    delete[] table;
                    
                    // Clean up retired tables
                    for (void* t : retired_tables) {
                        delete[] static_cast<std::atomic<Segment*>*>(t);
                    }
                }
            };
            
            std::string data_dir_;
            ClassAllocator allocators_[NUM_CLASSES];
            std::atomic<uint32_t> next_segment_id_{0};
            std::atomic<uint32_t> global_file_seq_{0};  // Only used when !kFilePerSizeClass
            
            // NEW: Registry pointers for windowed mmap
            FileHandleRegistry* file_registry_ = nullptr;
            MappingManager* mapping_manager_ = nullptr;

            // Read-only mode flag for serverless readers
            bool read_only_ = false;

            // Internal registries (owned) for legacy constructor
            std::unique_ptr<FileHandleRegistry> owned_file_registry_;
            std::unique_ptr<MappingManager> owned_mapping_manager_;
            
            // Configuration
            StorageConfig config_;
            
            Segment* allocate_new_segment(uint8_t class_id, NodeKind kind = NodeKind::Internal);
            std::string get_data_file_path(uint32_t file_id) const;
            
            FSResult ensure_file_size(const std::string& path, size_t min_size);
            
            // Recovery helpers
            void ensure_seg_table_capacity_locked(ClassAllocator& ca, size_t min_capacity);
            std::unique_ptr<Segment> map_segment_for_recovery_locked(uint8_t class_id,
                                                                     uint32_t file_id,
                                                                     uint32_t segment_id,
                                                                     uint64_t offset);

            // Lazy remapping helper - ensures segment is mapped (thread-safe)
            // Called from get_ptr() slow path when pin has been released
            void ensure_segment_mapped(Segment* seg);
        };
        
        // INLINE IMPLEMENTATION for hot path performance - REMOVED
        // Now using MappingManager::Pin in Allocation struct
        
        /* OLD IMPLEMENTATION - KEPT FOR REFERENCE
        FORCE_INLINE void* SegmentAllocator::get_ptr(const Allocation& a) noexcept {
            #ifndef NDEBUG
            // Track that we're not taking any locks
            const uint64_t pre_lock = g_segment_lock_count.load(std::memory_order_relaxed);
            const uint64_t pre_scan = g_segment_scan_count.load(std::memory_order_relaxed);
            #endif
            
            if (UNLIKELY(a.class_id >= NUM_CLASSES)) return nullptr;
            
            auto& ca = allocators_[a.class_id];
            
            // O(1) lock-free lookup - CRITICAL: No locks, no scans
            // Load size with acquire - this synchronizes with the writer's release store
            const size_t size = ca.seg_table_size.load(std::memory_order_acquire);
            if (UNLIKELY(a.segment_id >= size)) return nullptr;
            
            // OPTIMIZATION: Use relaxed load for root pointer
            // The acquire on size already ensures we see the root pointer written before size
            // Writer order: root.store(release) -> size.store(release)
            // Reader order: size.load(acquire) -> root.load(relaxed) is safe
            auto* table = ca.seg_table_root.load(std::memory_order_relaxed);
            if (UNLIKELY(!table)) return nullptr;
            
            // Acquire-load the segment pointer from the table
            Segment* seg = table[a.segment_id].load(std::memory_order_acquire);
            if (UNLIKELY(!seg)) return nullptr;
            
            // Fast path: skip optional checks if everything looks good
            // File ID check - usually matches
            if (UNLIKELY(seg->file_id != a.file_id)) return nullptr;
            
            // Safe offset math: prevent underflow & overflow
            if (UNLIKELY(a.offset < seg->base_offset)) return nullptr;
            const uint64_t rel = a.offset - seg->base_offset;
            
            // Class constraints (debug assert + runtime guard)
            const size_t class_size = size_class::kSizes[a.class_id];
            #ifndef NDEBUG
            assert((rel % class_size) == 0 && "Misaligned offset for size class");
            assert(a.length == class_size && "Allocation size doesn't match size class");
            #endif
            // Runtime checks for production safety (these should rarely fail)
            if (UNLIKELY((rel % class_size) != 0)) return nullptr;
            if (UNLIKELY(a.length != class_size)) return nullptr;
            
            // Capacity check without overflow
            if (UNLIKELY(seg->capacity < rel || seg->capacity - rel < a.length)) return nullptr;
            
            #ifndef NDEBUG
            // Ensure no lock/scan in this O(1) path
            assert(g_segment_lock_count.load(std::memory_order_relaxed) == pre_lock &&
                   "get_ptr() must not take any locks");
            assert(g_segment_scan_count.load(std::memory_order_relaxed) == pre_scan &&
                   "get_ptr() must not perform linear scans");
            #endif
            
            // Map to virtual address - direct pointer arithmetic
            return static_cast<uint8_t*>(seg->base_vaddr) + rel;
        }
        
        // O(1) recovery pointer lookup - uses same lock-free pattern as get_ptr
        FORCE_INLINE void* SegmentAllocator::get_ptr_for_recovery(
            uint8_t  class_id,
            uint32_t file_id,
            uint32_t segment_id,
            uint64_t offset,
            uint32_t length) noexcept 
        {
            if (UNLIKELY(class_id >= NUM_CLASSES)) return nullptr;
            auto& ca = allocators_[class_id];

            // 1) Lock-free fast path: seg_table lookup
            //    Pairs with release-store on publish.
            size_t size = ca.seg_table_size.load(std::memory_order_acquire);
            auto*  root = ca.seg_table_root.load(std::memory_order_acquire);
            if (root && segment_id < size) {
                Segment* seg = root[segment_id].load(std::memory_order_acquire);
                if (seg) {
                    // Segment exists - check file_id match
                    if (seg->file_id != file_id) {
                        // Wrong file_id - this is an error, don't try to create
                        return nullptr;
                    }
                    // Correct segment found - do bounds checking
                    if (offset < seg->base_offset) return nullptr;
                    uint64_t rel = offset - seg->base_offset;
                    if (seg->capacity < rel || seg->capacity - rel < length) return nullptr;
                    return static_cast<uint8_t*>(seg->base_vaddr) + rel;
                }
            }

            // 2) Slow path: segment not published yet → map and publish exactly once.
            //    Use the class's creation mutex.
            std::lock_guard<std::mutex> lk(ca.create_mu);

            // Re-check after taking the lock (someone else may have published).
            size  = ca.seg_table_size.load(std::memory_order_acquire);
            root  = ca.seg_table_root.load(std::memory_order_acquire);
            if (root && segment_id < size) {
                Segment* seg = root[segment_id].load(std::memory_order_acquire);
                if (seg) {
                    if (seg->file_id != file_id) {
                        // Wrong file_id - don't create a new segment
                        return nullptr;
                    }
                    // Correct segment found
                    if (offset < seg->base_offset) return nullptr;
                    uint64_t rel = offset - seg->base_offset;
                    if (seg->capacity < rel || seg->capacity - rel < length) return nullptr;
                    return static_cast<uint8_t*>(seg->base_vaddr) + rel;
                }
            }

            // 3) Need to create/map the segment and publish it.
            std::unique_ptr<Segment> seg_uptr = map_segment_for_recovery_locked(
                class_id, file_id, segment_id);

            if (!seg_uptr) return nullptr;
            Segment* seg = seg_uptr.get();

            // Ensure seg_table can address segment_id; grow with copy-on-publish.
            ensure_seg_table_capacity_locked(ca, segment_id + 1);

            // Install into table (publish):
            ca.seg_table_root.load(std::memory_order_relaxed)[segment_id].store(
                seg, std::memory_order_release);

            // Keep ownership in ca.segments (so it lives as long as the allocator)
            ca.segments.emplace_back(std::move(seg_uptr));

            // 4) Now compute pointer
            if (offset < seg->base_offset) return nullptr;
            const uint64_t rel = offset - seg->base_offset;
            if (seg->capacity < rel || seg->capacity - rel < length) return nullptr;
            return static_cast<uint8_t*>(seg->base_vaddr) + rel;
        }
        */
        
        // O(1) recovery pointer lookup with publish-once slow path.
        FORCE_INLINE void* SegmentAllocator::get_ptr_for_recovery(
            uint8_t  class_id,
            uint32_t file_id,
            uint32_t segment_id,
            uint64_t offset,
            uint32_t length) noexcept
        {
            if (UNLIKELY(class_id >= NUM_CLASSES)) return nullptr;
            auto& ca = allocators_[class_id];

            // -------- Fast path: lock-free seg_table lookup --------
            // Pairs with release-stores done when publishing a segment.
            size_t size = ca.seg_table_size.load(std::memory_order_acquire);
            auto*  root = ca.seg_table_root.load(std::memory_order_relaxed);
            if (LIKELY(root && segment_id < size)) {
                Segment* seg = root[segment_id].load(std::memory_order_acquire);
                if (LIKELY(seg)) {
                    // Verify we're looking at the right file and bounds.
                    if (UNLIKELY(seg->file_id != file_id)) return nullptr;
                    if (UNLIKELY(offset < seg->base_offset)) return nullptr;
                    const uint64_t rel = offset - seg->base_offset;

                    // Ensure class-size/length alignment & capacity
                    const size_t class_sz = size_class::kSizes[class_id];
                    if (UNLIKELY((rel % class_sz) != 0)) return nullptr;
                    if (UNLIKELY(length != class_sz))     return nullptr;
                    if (UNLIKELY(seg->capacity < rel || seg->capacity - rel < length)) return nullptr;

                    return static_cast<uint8_t*>(seg->base_vaddr) + rel;
                }
            }

            // -------- Slow path: not published yet → map & publish exactly once --------
            // This reuses your existing helpers that know the canonical file path.
            std::lock_guard<std::mutex> lk(ca.create_mu);

            // Re-check after taking the lock (another thread may have published).
            size  = ca.seg_table_size.load(std::memory_order_acquire);
            root  = ca.seg_table_root.load(std::memory_order_relaxed);
            if (root && segment_id < size) {
                Segment* seg = root[segment_id].load(std::memory_order_acquire);
                if (seg) {
                    if (UNLIKELY(seg->file_id != file_id)) return nullptr;
                    if (UNLIKELY(offset < seg->base_offset)) return nullptr;
                    const uint64_t rel = offset - seg->base_offset;

                    const size_t class_sz = size_class::kSizes[class_id];
                    if (UNLIKELY((rel % class_sz) != 0)) return nullptr;
                    if (UNLIKELY(length != class_sz))     return nullptr;
                    if (UNLIKELY(seg->capacity < rel || seg->capacity - rel < length)) return nullptr;

                    return static_cast<uint8_t*>(seg->base_vaddr) + rel;
                }
            }

            // Create/map the segment and publish it.
            // NOTE: map_segment_for_recovery_locked SHOULD use MappingManager internally and
            //       MUST NOT create a file when mapping read-only.
            std::unique_ptr<Segment> seg_uptr = map_segment_for_recovery_locked(
                class_id, file_id, segment_id, offset);
            if (!seg_uptr) return nullptr;

            Segment* seg = seg_uptr.get();

            // Ensure table capacity and publish with release-store.
            ensure_seg_table_capacity_locked(ca, segment_id + 1);
            ca.seg_table_root.load(std::memory_order_relaxed)[segment_id]
                .store(seg, std::memory_order_release);

            // Keep ownership so the segment lives for allocator lifetime.
            ca.segments.emplace_back(std::move(seg_uptr));

            // Bounds & alignment checks again (defensive).
            if (UNLIKELY(offset < seg->base_offset)) return nullptr;
            const uint64_t rel = offset - seg->base_offset;
            if (UNLIKELY(seg->capacity < rel || seg->capacity - rel < length)) return nullptr;

            const size_t class_sz = size_class::kSizes[class_id];
            if (UNLIKELY((rel % class_sz) != 0)) return nullptr;
            if (UNLIKELY(length != class_sz))     return nullptr;

            return static_cast<uint8_t*>(seg->base_vaddr) + rel;
        }
        
        // O(1) pointer lookup for regular allocations using segment table
        // With lazy remapping: if segment's pin was released, we remap on demand
        FORCE_INLINE void* SegmentAllocator::get_ptr(const Allocation& a) noexcept {
            // First try using the pin if it's valid (for special allocations)
            if (a.pin.get()) {
                return a.pin.get();
            }

            // Otherwise use the segment table for O(1) lookup
            if (UNLIKELY(a.class_id >= NUM_CLASSES)) return nullptr;
            auto& ca = allocators_[a.class_id];

            // Fast path: lock-free seg_table lookup
            size_t size = ca.seg_table_size.load(std::memory_order_acquire);
            if (UNLIKELY(a.segment_id >= size)) return nullptr;

            auto* table = ca.seg_table_root.load(std::memory_order_relaxed);
            if (UNLIKELY(!table)) return nullptr;

            Segment* seg = table[a.segment_id].load(std::memory_order_acquire);
            if (UNLIKELY(!seg)) return nullptr;

            // Verify file_id match
            if (UNLIKELY(seg->file_id != a.file_id)) return nullptr;

            // LAZY REMAPPING: Check if segment's pin is still valid
            // If not, we need to remap (slow path)
            if (UNLIKELY(!seg->pin)) {
                // Slow path: remap the segment (thread-safe)
                const_cast<SegmentAllocator*>(this)->ensure_segment_mapped(seg);
                if (UNLIKELY(!seg->base_vaddr)) return nullptr;
            }

            // Update last access time for LRU-based pin release
            using namespace std::chrono;
            seg->last_access_ns.store(
                duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count(),
                std::memory_order_relaxed);

            // Compute offset
            if (UNLIKELY(a.offset < seg->base_offset)) return nullptr;
            const uint64_t rel = a.offset - seg->base_offset;

            // Debug asserts for alignment and size class
            #ifndef NDEBUG
            const size_t class_sz = size_class::kSizes[a.class_id];
            assert((rel % class_sz) == 0 && "Misaligned offset for size class");
            assert(a.length == class_sz && "Allocation size doesn't match size class");
            #endif

            // Bounds check
            if (UNLIKELY(seg->capacity < rel || seg->capacity - rel < a.length)) return nullptr;
            
            // Return pointer using stable base_vaddr
            return static_cast<uint8_t*>(seg->base_vaddr) + rel;
        }

    } // namespace persist
} // namespace xtree