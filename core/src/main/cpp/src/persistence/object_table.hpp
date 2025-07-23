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

#include <vector>
#include <array>
#include <memory>
#include <mutex>
#include <cstdint>
#include <cassert>
#include <cstdlib>
#include <stdexcept>
#ifndef NDEBUG
#include <unordered_set>
#endif
#ifndef NDEBUG
#include <sstream>  // For ostringstream in assert_kind (debug builds only)
#endif
#include "config.h"
#include "node_id.hpp"
#include "ot_delta_log.h"
#include "ot_entry.h"
#include "segment_allocator.h"
#include "ot_checkpoint.h"

namespace xtree {
    namespace persist {

        /**
         * ShardBits - Handle layout for sharded configurations
         * Defines how global handles encode shard information.
         * Used by both ObjectTable (for normalization) and ObjectTableSharded.
         *
         * NodeID is 64 bits: [63:16] = handle_index, [15:0] = tag
         * We carve the 48-bit handle_index as:
         * - [47:42] = shard_id (6 bits, up to 64 shards)
         * - [41:0] = local_handle (42 bits)
         */
        struct ShardBits {
            static constexpr uint32_t kTagBits   = 16;        // Low bits [15:0] for tag
            static constexpr uint32_t kShardBits = 6;         // Bits [47:42] of handle_index for shard_id
            static constexpr uint32_t kHBits     = 48;        // Total handle bits [63:16] in NodeID
            static constexpr uint32_t kLocalBits = kHBits - kShardBits; // 42 bits [41:0] for local handle
            static constexpr uint64_t kShardMask = (1ull << kShardBits) - 1;  // 0x3F
            static constexpr uint64_t kLocalMask = (1ull << kLocalBits) - 1;  // 42-bit mask

            // Build global 48-bit handle_index() value from shard+local
            static inline constexpr uint64_t make_global_handle_idx(uint32_t shard, uint64_t local) {
                return ((uint64_t(shard) & kShardMask) << kLocalBits) |
                       (local & kLocalMask);
            }

            // Extract shard from a 48-bit handle_index()
            static inline constexpr uint32_t shard_from_handle_idx(uint64_t handle_idx) {
                return uint32_t((handle_idx >> kLocalBits) & kShardMask);
            }

            // Extract local handle from a 48-bit handle_index()
            static inline constexpr uint64_t local_from_handle_idx(uint64_t handle_idx) {
                return (handle_idx & kLocalMask);
            }
        };

        struct OTDeltaSink {
            virtual ~OTDeltaSink() = default;
            virtual void append(const std::vector<OTDeltaRec>& batch) = 0; 
        };

        struct OTAllocSink {
            virtual ~OTAllocSink() = default;
            virtual SegmentAllocator::Allocation alloc(size_t size) = 0;
            virtual void free(const SegmentAllocator::Allocation& a) = 0;
        };

        struct CommitBatch {
            std::vector<OTDeltaRec> deltas;
            void clear() { deltas.clear(); }
        };

        // Optional helper to pack an OTRec from an entry/handle/tag
        OTDeltaRec make_delta(uint64_t handle_idx, const OTEntry& e) noexcept;

        /**
         * Object table for managing persistent object metadata.
         * Uses a paged slab allocator to ensure OTEntry objects never move,
         * which is required since they contain std::atomic members.
         */
        class ObjectTable {
        private:
            // Compute slab configuration at runtime (can be overridden)
            static size_t compute_entries_per_slab() {
                size_t slab_kb = object_table::kSlabTargetBytes / 1024;
                
                // Check for environment variable override
                const char* env_slab_kb = std::getenv(object_table::kSlabSizeEnvVar);
                if (env_slab_kb) {
                    size_t env_kb = std::strtoul(env_slab_kb, nullptr, 10);
                    if (env_kb >= object_table::kMinSlabKB && 
                        env_kb <= object_table::kMaxSlabKB) {
                        slab_kb = env_kb;
                    }
                }
                
                size_t target_bytes = slab_kb * 1024;
                size_t entries = target_bytes / sizeof(OTEntry);
                
                // Round down to nearest power of 2 for efficient masking
                // Use __builtin_clz (count leading zeros) to find highest bit
                if (entries == 0) entries = 1;
                unsigned int highest_bit = 31 - __builtin_clz(static_cast<unsigned int>(entries));
                return 1u << highest_bit;
            }
            
            const size_t entries_per_slab_;
            const uint32_t slab_shift_;  
            const uint64_t slab_mask_;
            
        public:
            explicit ObjectTable(size_t initial_capacity = object_table::kInitialCapacity) 
                : entries_per_slab_(compute_entries_per_slab()),
                  slab_shift_(31 - __builtin_clz(static_cast<unsigned int>(entries_per_slab_))),
                  slab_mask_(entries_per_slab_ - 1) {
                // Initialize outer table to nullptr
                for (auto& seg : slab_segments_) {
                    seg.store(nullptr, std::memory_order_relaxed);
                }
                // Note: slabs will be allocated lazily as needed
            }
            
            ~ObjectTable() {
                // Clean up all allocated slabs and segments
                const uint32_t published = slab_count_.load(std::memory_order_relaxed);
                
                // Free all slabs
                for (uint32_t slab_idx = 0; slab_idx < published; slab_idx++) {
                    const uint32_t sidx = seg_idx(slab_idx);
                    const uint32_t off = seg_off(slab_idx);
                    
                    SlabSegment* seg = slab_segments_[sidx].load(std::memory_order_relaxed);
                    if (seg) {
                        OTEntry* slab = seg->slabs[off].load(std::memory_order_relaxed);
                        delete[] slab;
                    }
                }
                
                // Free all segments
                for (auto& seg_ptr : slab_segments_) {
                    SlabSegment* seg = seg_ptr.load(std::memory_order_relaxed);
                    delete seg;
                }
            }

            /**
             * Allocate a new NodeID with the given properties.
             * Thread-safe. Uses release memory ordering on tag to publish all fields.
             */
            NodeID allocate(NodeKind kind, uint8_t class_id, const OTAddr& addr, uint64_t birth_epoch);
            
            /**
             * Retire a NodeID at the given epoch.
             * Thread-safe and idempotent - multiple calls with same ID are safe.
             */
            void   retire(NodeID id, uint64_t retire_epoch);
            
            /**
             * Reserve a NodeID for marking live, potentially bumping tag if handle was reused.
             * Returns the final NodeID that MUST be used in WAL.
             * Does NOT change birth/retire epochs yet.
             * Thread-safe. Call this before building WAL batch.
             * 
             * @param proposed The NodeID from allocation
             * @param birth_epoch The epoch when the node will become visible (for validation)
             * @return Final NodeID with correct tag for WAL, or invalid if failed
             */
            NodeID mark_live_reserve(NodeID proposed, uint64_t birth_epoch);
            
            /**
             * Commit a previously reserved mark_live operation.
             * Sets birth/retire epochs and publishes the tag.
             * Thread-safe. Call this AFTER WAL append succeeds.
             * 
             * @param final_id The NodeID returned from mark_live_reserve
             * @param birth_epoch The epoch when the node becomes visible
             */
            void mark_live_commit(NodeID final_id, uint64_t birth_epoch);

            /**
             * Abort a RESERVED entry (never published): validate tag/state, clear metadata,
             * bump tag (ABA), push handle to freelist. DOES NOT free segment storage —
             * caller must free the captured allocation, if any.
             * @param id The NodeID to abort (must be RESERVED with matching tag)
             * @return true if aborted successfully; false otherwise.
             */
            bool abort_reservation(NodeID id);

            /**
             * Get the OTEntry for a given NodeID.
             * 
             * IMPORTANT: Callers must use proper memory ordering when reading:
             * ```
             * const auto& e = ot.get(id);
             * uint16_t tag = e.tag.load(std::memory_order_acquire);
             * if (tag != id.tag()) { // handle stale reference }
             * // Now safe to read e.addr, e.kind, etc after acquire
             * ```
             * 
             * @param id The NodeID to retrieve.
             * @return The OTEntry corresponding to the NodeID.
             */
            inline const OTEntry& get(NodeID id) const { 
                return const_cast<ObjectTable*>(this)->slot_safe(id.handle_index());
            }
            
            /**
             * Get an entry by handle index WITHOUT tag validation.
             * UNSAFE: Use only when you hold the handle and know it's not freed.
             * This is needed during the allocate->publish->commit flow before tag is written.
             * @param handle The handle index to look up.
             * @return Reference to the entry.
             */
            inline const OTEntry& get_by_handle_unsafe(uint64_t handle) const {
                return const_cast<ObjectTable*>(this)->slot_safe(handle);
            }

            /**
             * Try to get entry by handle without tag validation.
             * Safe version that returns nullptr for invalid handles.
             * Accepts either a local handle (non-sharded) or a global handle (sharded),
             * by always normalizing to the local portion.
             * @param handle The handle index to retrieve (local or global).
             * @return Pointer to entry or nullptr if invalid/out of bounds.
             */
            inline const OTEntry* try_get_by_handle(uint64_t handle) const noexcept {
                // Normalize: strip shard bits if present; no-op if not using sharded handles
                // Use ShardBits helper for consistency with sharded implementation
                // This masks to 42 bits, removing any shard bits in [47:42]
                const uint64_t local = ShardBits::local_from_handle_idx(handle);

                const uint32_t slab_idx = static_cast<uint32_t>(local >> slab_shift_);
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) {
                    return nullptr;
                }

                OTEntry* slab = get_slab_ptr(slab_idx);  // must already be published
                if (!slab) {
                    return nullptr;
                }

                const uint32_t idx_in_slab = static_cast<uint32_t>(local & slab_mask_);
                return &slab[idx_in_slab];
            }

            /**
             * Get a mutable reference to the OTEntry for a given NodeID.
             * Note: Writers should use proper synchronization when modifying.
             * @param id The NodeID to retrieve.
             * @return A mutable reference to the OTEntry corresponding to the NodeID.
             */
            inline OTEntry&       get_mut(NodeID id)   { 
                return slot_safe(id.handle_index());
            }

            /**
             * Check if a NodeID is valid (not retired).
             * @param id The NodeID to check.
             * @return True if the NodeID is valid, false otherwise.
             */
            inline bool is_valid(NodeID id) const {
                uint64_t h = id.handle_index();
                uint32_t slab_idx = h >> slab_shift_;
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) return false;
                
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) return false;
                
                return slab[h & slab_mask_].is_valid();
            }

            /**
             * Validate the tag of a NodeID against the stored entry.
             * Uses acquire memory ordering to synchronize with writer's release.
             * After this returns true, it's safe to read other fields.
             * @param id The NodeID to validate.
             * @return True if the tag matches, false otherwise.
             */
            inline bool   validate_tag(NodeID id) const { 
                uint64_t h = id.handle_index();
                uint32_t slab_idx = h >> slab_shift_;
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) return false;
                
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) return false;
                
                // Acquire to synchronize with release store in allocate()
                // This ensures we see all fields written before the tag was published
                return slab[h & slab_mask_].tag.load(std::memory_order_acquire) == id.tag(); 
            }
            
            /**
             * Try to get an entry with tag validation.
             * Returns nullptr if tag doesn't match or handle is invalid.
             * 
             * Example usage:
             * ```
             * if (const OTEntry* e = ot.try_get(id)) {
             *     // Tag already validated with acquire, safe to read fields
             *     OTAddr addr = e->addr;
             * }
             * ```
             */
            const OTEntry* try_get(NodeID id) const {
                uint64_t h = id.handle_index();
                uint32_t slab_idx = h >> slab_shift_;
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) return nullptr;
                
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) return nullptr;
                
                const OTEntry& e = slab[h & slab_mask_];
                // Acquire to synchronize with release store in allocate()
                uint16_t tag = e.tag.load(std::memory_order_acquire);
                if (tag != id.tag()) return nullptr;
                
                // Tag validated, safe to return entry
                return &e;
            }
            
            /**
             * Try to get an entry with tag validation (via output parameter).
             * Returns false if tag doesn't match or handle is invalid.
             * 
             * Example usage:
             * ```
             * const OTEntry* entry;
             * if (ot.try_get(id, entry)) {
             *     // Tag already validated, safe to use entry
             * }
             * ```
             */
            bool try_get(NodeID id, const OTEntry*& out) const {
                out = try_get(id);
                return out != nullptr;
            }
            
            /**
             * Safe reader pattern - validates and calls function if valid.
             * Returns true if valid and function was called.
             */
            template<typename Func>
            bool try_get_safe(NodeID id, Func&& func) const {
                if (const OTEntry* e = try_get(id)) {
                    func(*e);
                    return true;
                }
                return false;
            }

            /**
             * Reclaim handles retired before the given epoch.
             * Returns the number of handles reclaimed.
             */
            size_t reclaim_before_epoch(uint64_t safe_epoch); // returns freed count

            void   reserve(size_t n) { 
                // With fixed-size two-level table, reserve is a no-op
                // We allocate segments lazily as needed
                // Just validate we don't exceed capacity
                size_t slabs_needed = (n + entries_per_slab_ - 1) / entries_per_slab_;
                if (slabs_needed > kMaxSegments * kSlabsPerSegment) {
                    throw std::runtime_error("Requested capacity exceeds maximum object table size");
                }
            }
            
            /**
             * Get configuration info for debugging/monitoring
             */
            size_t get_entries_per_slab() const { return entries_per_slab_; }
            size_t get_slab_count() const { return slab_count_.load(std::memory_order_acquire); }
            size_t get_allocated_slabs() const {
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                size_t count = 0;
                for (uint32_t i = 0; i < published; i++) {
                    if (get_slab_ptr(i)) count++;
                }
                return count;
            }
            
            /**
             * Statistics for monitoring and tuning
             */
            struct Stats {
                size_t total_allocations = 0;
                size_t total_retires = 0;
                size_t total_reclaims = 0;
                size_t bytes_reclaimed = 0;
                size_t free_handles_count = 0;
                size_t retired_handles_count = 0;
                size_t max_handle_allocated = 0;
                std::array<size_t, 256> bytes_per_class{};    // Bytes reclaimed by size class
                std::array<size_t, 256> reclaims_per_class{};  // Count of reclaims by class
                size_t last_reclaim_count = 0;                 // Items reclaimed in last run
            };
            
            Stats get_stats() const {
                std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(mu_));
                Stats s = stats_;
                s.free_handles_count = free_count_;  // Use exact count from bitmap
                s.retired_handles_count = retired_handles_.size();
                s.max_handle_allocated = max_handle_;
                return s;
            }
            
            /**
             * Get entry by handle index without tag validation (for checkpointing).
             * Returns nullptr if handle is out of bounds or slab not allocated.
             * Caller must check retire_epoch == ~0ull to ensure entry is live.
             */
            const OTEntry* get_by_handle_unchecked(uint64_t handle_idx) const {
                uint32_t slab_idx = handle_idx >> slab_shift_;
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) {
                    return nullptr;
                }
                
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) {
                    return nullptr;
                }
                
                return &slab[handle_idx & slab_mask_];
            }
            
            /**
             * Restore a handle with specific index and properties (for recovery).
             * This bypasses normal allocation and directly sets the handle at the given index.
             * Used during checkpoint recovery to preserve NodeID references.
             * @param handle_idx The exact handle index to restore
             * @param entry The persistent entry data to restore
             */
            void restore_handle(uint64_t handle_idx, const OTCheckpoint::PersistentEntry& entry);
            
            /**
             * Apply a delta record during recovery replay.
             * Uses proper memory ordering to ensure consistency.
             * Thread-safe.
             */
            void apply_delta(const OTDeltaRec& rec);
            
            /**
             * Begin recovery mode - builds bitmap for O(1) free list lookups.
             * Call this before replaying deltas.
             */
            void begin_recovery();
            
            /**
             * End recovery mode - clears the recovery bitmap.
             * Call this after all deltas are replayed.
             */
            void end_recovery();
            
            /**
             * Take a stable snapshot of live entries for checkpointing.
             * Holds the OT lock briefly to ensure consistency.
             * @param out Vector to fill with persistent entries
             * @return Number of live entries captured
             */
            template<typename PersistentEntry>
            size_t iterate_live_snapshot(std::vector<PersistentEntry>& out) {
                std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(mu_));
                
                // Estimate live entries for reservation
                // max_handle_ + 1 gives us total allocated handles
                // Subtract free and retired to get approximate live count
                size_t total_handles = max_handle_ + 1;
                size_t est_live = total_handles > (free_handles_.size() + retired_handles_.size()) 
                                ? total_handles - free_handles_.size() - retired_handles_.size()
                                : 0;
                out.clear();
                if (est_live > 0) {
                    out.reserve(est_live);
                }
                
                // Iterate through all allocated handles
                for (uint64_t handle_idx = 0; handle_idx <= max_handle_; handle_idx++) {
                    // Skip handle 0 - it's reserved and never used
                    if (handle_idx == 0) {
                        continue;
                    }
                    
                    uint32_t slab_idx = handle_idx >> slab_shift_;
                    const uint32_t published = slab_count_.load(std::memory_order_relaxed);  // Under lock, can use relaxed
                    if (slab_idx >= published) {
                        continue;
                    }
                    
                    OTEntry* slab = get_slab_ptr(slab_idx);
                    if (!slab) {
                        continue;
                    }
                    
                    const OTEntry& entry = slab[handle_idx & slab_mask_];
                    uint64_t retire_epoch = entry.retire_epoch.load(std::memory_order_acquire);
                    uint64_t birth_epoch = entry.birth_epoch.load(std::memory_order_acquire);

                    // Skip non-live entries:
                    // - Never allocated (birth_epoch == 0)
                    // - OR retired (retire_epoch != ~0)
                    if (birth_epoch == 0 || retire_epoch != ~uint64_t{0}) {
                        continue;
                    }

                    uint16_t tag = entry.tag.load(std::memory_order_acquire);

                    // Capture stable snapshot under lock
                    PersistentEntry pe{};
                    pe.handle_idx = handle_idx;
                    pe.file_id = entry.addr.file_id;
                    pe.segment_id = entry.addr.segment_id;
                    pe.offset = entry.addr.offset;
                    pe.length = entry.addr.length;
                    pe.class_id = entry.class_id;
                    pe.kind = static_cast<uint8_t>(entry.kind);
                    pe.tag = tag;
                    pe.birth_epoch = birth_epoch;
                    pe.retire_epoch = retire_epoch;
                    
                    out.push_back(pe);
                }
                
                return out.size();
            }

#ifndef NDEBUG
            /**
             * Debug-only semantic validation of a NodeID against expected kind.
             * Ensures that the ObjectTable entry for this handle exists and matches
             * the expected NodeKind. Replaces old NodeID parity checks.
             *
             * @param id           The NodeID to validate
             * @param expectedKind Expected kind (Leaf, Internal, DataRecord)
             */
            inline void assert_kind(const NodeID& id, NodeKind expectedKind) const {
                if (!id.valid()) {
                    throw std::runtime_error("assert_kind: invalid NodeID");
                }

                const OTEntry* entry = try_get(id);
                if (!entry) {
                    throw std::runtime_error("assert_kind: no ObjectTable entry for NodeID");
                }

                NodeKind actual = entry->kind;
                if (actual != expectedKind) {
                    std::ostringstream oss;
                    oss << "assert_kind: NodeID " << id.raw()
                        << " has kind=" << static_cast<int>(actual)
                        << " but expected kind=" << static_cast<int>(expectedKind);
                    throw std::runtime_error(oss.str());
                }
            }
#endif

        public:
#ifndef NDEBUG
            // Helper to convert debug state enum to string for logging
            static const char* dbg_state_name(uint8_t state) {
                switch(state) {
                    case OTEntry::DBG_FREE:     return "FREE";
                    case OTEntry::DBG_RESERVED: return "RESERVED";
                    case OTEntry::DBG_LIVE:     return "LIVE";
                    case OTEntry::DBG_RETIRED:  return "RETIRED";
                    default:                    return "UNKNOWN";
                }
            }
#endif

        private:
            // Constant for "live" marker in retire_epoch field (means not retired)
            static constexpr uint64_t RETIRE_LIVE_MARKER = ~uint64_t{0};

            /**
             * Get the OTEntry at the given handle (unsafe - no bounds check).
             * Handle encoding: [63:slab_shift] = slab index, [slab_shift-1:0] = slot within slab
             */
            inline OTEntry& slot(uint64_t h) {
                uint32_t slab_idx = h >> slab_shift_;
                uint32_t slot_idx = h & slab_mask_;
                OTEntry* slab = get_slab_ptr(slab_idx);
                assert(slab != nullptr && "Handle points to unpublished slab");
                return slab[slot_idx];
            }

            inline const OTEntry& slot(uint64_t h) const {
                uint32_t slab_idx = h >> slab_shift_;
                uint32_t slot_idx = h & slab_mask_;
                OTEntry* slab = get_slab_ptr(slab_idx);
                assert(slab != nullptr && "Handle points to unpublished slab");
                return slab[slot_idx];
            }
            
            /**
             * Get the OTEntry at the given handle with bounds checking.
             */
            inline OTEntry& slot_safe(uint64_t h) {
                uint32_t slab_idx = h >> slab_shift_;
                uint32_t slot_idx = h & slab_mask_;
                
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) {
                    throw std::out_of_range("ObjectTable: Handle slab index out of bounds");
                }
                
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) {
                    throw std::runtime_error("ObjectTable: Handle points to unpublished slab");
                }
                if (slot_idx >= entries_per_slab_) {
                    throw std::out_of_range("ObjectTable: Handle slot index out of bounds");
                }
                
                return slab[slot_idx];
            }
            
            inline const OTEntry& slot_safe(uint64_t h) const {
                uint32_t slab_idx = h >> slab_shift_;
                uint32_t slot_idx = h & slab_mask_;
                
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) {
                    throw std::out_of_range("ObjectTable: Handle slab index out of bounds");
                }
                
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) {
                    throw std::runtime_error("ObjectTable: Handle points to unpublished slab");
                }
                if (slot_idx >= entries_per_slab_) {
                    throw std::out_of_range("ObjectTable: Handle slot index out of bounds");
                }
                
                return slab[slot_idx];
            }

            /**
             * Acquire a free handle, allocating a new slab if necessary.
             * Must be called with mu_ held.
             */
            uint64_t acquire_handle_locked();
            
            /**
             * Set the segment allocator for freeing physical space.
             * Should be called during initialization.
             */
            void set_segment_allocator(SegmentAllocator* alloc) {
                segment_allocator_ = alloc;
            }
            
            /**
             * Initialize a new slab's entries to safe defaults.
             * Must be called with mu_ held.
             */
            void initialize_slab_locked(size_t slab_idx);
            
            /**
             * Add a new slab to the two-level table.
             * Must be called with mu_ held.
             * Returns true if successful, false if table is full.
             */
            bool add_slab_locked();
            
            // Bitmap helper methods - used at all times, not just recovery mode
            // Bitmap semantics: 1 = free, 0 = used
            // This allows us to skip zero words quickly during rebuild
            inline void bm_set(size_t h) {     // Mark handle as free
                if (!bm_test(h)) {  // Only increment if changing from used to free
                    free_bitmap_[h >> 6] |= (1ull << (h & 63)); 
                    ++free_count_;
                }
            }
            
            inline void bm_clear(size_t h) {   // Mark handle as used
                if (bm_test(h)) {  // Only decrement if changing from free to used
                    free_bitmap_[h >> 6] &= ~(1ull << (h & 63)); 
                    --free_count_;
                }
            }
            
            inline bool bm_test(size_t h) const {  // Check if handle is free
                return (free_bitmap_[h >> 6] >> (h & 63)) & 1ull; 
            }
            
            /**
             * Refill the free handle cache from the bitmap when empty.
             * Must be called with mu_ held.
             * @param target_batch Target number of handles to refill (default 256)
             */
            void refill_free_cache_locked(size_t target_batch = 256);

            // Two-level segmented table for lock-free reads
            static constexpr uint32_t kSlabsPerSegment = 64;    // 64 slabs per segment (cache-friendly)
            static constexpr uint32_t kMaxSegments = 256;       // Max 256*64 = 16K slabs
            
            struct SlabSegment {
                std::array<std::atomic<OTEntry*>, kSlabsPerSegment> slabs;
                SlabSegment() {
                    for (auto& s : slabs) {
                        s.store(nullptr, std::memory_order_relaxed);
                    }
                }
            };
            
            // Fixed-size outer table (never reallocates)
            std::array<std::atomic<SlabSegment*>, kMaxSegments> slab_segments_;
            std::atomic<uint32_t> slab_count_{0};  // Total published slabs for lock-free reads
            
            // Helper functions for segment indexing
            inline uint32_t seg_idx(uint32_t slab_idx) const { 
                return slab_idx / kSlabsPerSegment; 
            }
            inline uint32_t seg_off(uint32_t slab_idx) const { 
                return slab_idx % kSlabsPerSegment; 
            }
            
            // Lock-free reader helper to get slab pointer
            inline OTEntry* get_slab_ptr(uint32_t slab_idx) const {
                const uint32_t published = slab_count_.load(std::memory_order_acquire);
                if (slab_idx >= published) return nullptr;
                
                const uint32_t sidx = seg_idx(slab_idx);
                const uint32_t off = seg_off(slab_idx);
                
                SlabSegment* seg = slab_segments_[sidx].load(std::memory_order_acquire);
                if (!seg) return nullptr;  // Defensive (shouldn't happen with correct publish order)
                
                return seg->slabs[off].load(std::memory_order_acquire);
            }
            
            std::vector<uint64_t> free_handles_;             // Free handle cache (LIFO)
            std::vector<uint64_t> retired_handles_;          // Handles pending reclamation
            uint64_t max_handle_ = 0;                        // Highest handle ever allocated
            mutable std::mutex mu_;                          // Protects allocation/free/retire

#ifndef NDEBUG
            // Debug-only duplicate detection
            std::unordered_set<uint64_t> free_set_dbg_;      // Track free handles to detect duplicates
#endif
            SegmentAllocator* segment_allocator_ = nullptr;  // Optional allocator for physical space
            Stats stats_{};                                  // Performance statistics
            
            // Bitmap support - now used at all times for O(1) operations
            bool recovery_mode_ = false;                     // True during recovery replay
            std::vector<uint64_t> free_bitmap_;              // 1 bit per handle; bit=1 means "free"
            size_t free_count_ = 0;                          // Exact count of free handles
            size_t free_scan_cursor_ = 0;                    // Word index cursor to refill cache
        }; // class ObjectTable

        /**
         * Helper to lookup NodeKind from ObjectTable.
         * Returns true if found, false otherwise.
         * Used by cache_or_load to determine correct loader.
         */
        inline bool try_lookup_kind(const ObjectTable* ot,
                                    const NodeID& id,
                                    NodeKind& out) noexcept {
            if (!ot || !id.valid()) return false;
            if (const auto* e = ot->try_get(id)) {
                out = e->kind;
                return true;
            }
            return false;
        }

    } // namespace persist
} // namespace xtree
