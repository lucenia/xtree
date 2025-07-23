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

#include "object_table.hpp"
#include "ot_checkpoint.h"
#include "../util/log.h"
#include <cassert>
#include <algorithm>
#include <iostream>

/**
 * // Pseudocode in the writer module
 * CommitBatch batch;
 * // ... after building nodes via COW ...
 * // For each new or updated handle h:
 * const auto& e = ot.get(NodeID::from_raw((h<<8) | e.tag.load()));
 * batch.deltas.push_back(make_delta(h, e));
 * 
 * ot_log.append(batch.deltas); // fsync handled by publish step
 * batch.clear();
 * 
 * Rationale: Allocations belong in SegmentAllocator. 
 * The OT records where an object lives and emits deltas; 
 * the writer/commit path decides when to flush and fsync.
 */

namespace xtree { 
    namespace persist {

        OTDeltaRec make_delta(uint64_t handle_idx, const OTEntry& e) noexcept {
            OTDeltaRec r{};
            r.handle_idx = handle_idx;
            
            // Use acquire for tag to synchronize with writer's release
            // This ensures we see all fields written before tag was published
            r.tag        = e.tag.load(std::memory_order_acquire);
            
            // Now safe to read other fields with relaxed ordering
            // since acquire on tag synchronized-with the writer's release
            r.class_id   = e.class_id;
            r.kind       = static_cast<uint8_t>(e.kind);
            r.file_id    = e.addr.file_id;
            r.segment_id = e.addr.segment_id;
            r.offset     = e.addr.offset;
            r.length     = e.addr.length;
            r.birth_epoch  = e.birth_epoch.load(std::memory_order_relaxed);
            r.retire_epoch = e.retire_epoch.load(std::memory_order_relaxed);

            return r;
        }

        void ObjectTable::initialize_slab_locked(size_t slab_idx) {
            // Must be called with mu_ held
            // Initialize all entries in the slab to safe defaults (free state)
            OTEntry* slab = get_slab_ptr(slab_idx);
            if (!slab) return;  // Shouldn't happen, but defensive
            
            for (size_t i = 0; i < entries_per_slab_; ++i) {
                auto& e = slab[i];
                // Free slot: birth_epoch = 0, retire_epoch = U64_MAX
                e.retire_epoch.store(~uint64_t{0}, std::memory_order_relaxed);
                e.birth_epoch.store(0, std::memory_order_relaxed);
                e.tag.store(0, std::memory_order_relaxed);
                e.kind = NodeKind::Invalid;  // Free OT slot, never visible to readers
                e.class_id = 0;
                e.addr = {};
#ifndef NDEBUG
                e.dbg_state.store(OTEntry::DBG_FREE, std::memory_order_relaxed);
                e.dbg_magic = OTEntry::DBG_MAGIC;
#endif
            }
        }
        
        bool ObjectTable::add_slab_locked() {
            // Must be called with mu_ held
            const uint32_t current_count = slab_count_.load(std::memory_order_relaxed);
            
            // Check if we've reached maximum capacity
            if (current_count >= kMaxSegments * kSlabsPerSegment) {
                return false;  // Table is full
            }
            
            const uint32_t slab_idx = current_count;
            const uint32_t sidx = seg_idx(slab_idx);
            const uint32_t off = seg_off(slab_idx);
            
            // Allocate segment if needed
            SlabSegment* seg = slab_segments_[sidx].load(std::memory_order_relaxed);
            if (!seg) {
                seg = new SlabSegment();
                slab_segments_[sidx].store(seg, std::memory_order_release);
            }
            
            // Allocate the slab
            OTEntry* new_slab = new OTEntry[entries_per_slab_];
            
            // Initialize all entries to safe defaults
            for (size_t i = 0; i < entries_per_slab_; ++i) {
                auto& e = new_slab[i];
                e.retire_epoch.store(~uint64_t{0}, std::memory_order_relaxed);
                e.birth_epoch.store(0, std::memory_order_relaxed);
                e.tag.store(0, std::memory_order_relaxed);
                e.kind = NodeKind::Invalid;
                e.class_id = 0;
                e.addr = {};
            }
            
            // Publish the slab
            seg->slabs[off].store(new_slab, std::memory_order_release);
            
            // Finally increment the count to make it visible to readers
            slab_count_.store(slab_idx + 1, std::memory_order_release);
            
            // Ensure bitmap capacity
            const size_t capacity = (slab_idx + 1) * entries_per_slab_;
            const size_t need_words = (capacity + 63) / 64;
            if (free_bitmap_.size() < need_words) {
                free_bitmap_.resize(need_words, 0);
            }
            
            // Mark all new handles free in the bitmap (except handle 0 in slab 0)
            const uint64_t base = static_cast<uint64_t>(slab_idx) << slab_shift_;
            for (size_t i = 0; i < entries_per_slab_; ++i) {
                const uint64_t h = base + i;
                if (slab_idx == 0 && h == 0) continue;  // Skip reserved handle 0
                bm_set(h);  // 1 = free; also increments free_count_
            }
            
            // Count available handles (excluding reserved handle 0)
            const size_t to_reserve = (slab_idx == 0) ? (entries_per_slab_ - 1) : entries_per_slab_;

            // Prime cache from this slab - no parity filtering
            // Push in reverse order so pop_back() returns the lowest handle first
            if (to_reserve > 0) {
                free_handles_.reserve(free_handles_.size() + to_reserve);

                for (size_t i = entries_per_slab_; i-- > 0; ) {
                    uint64_t h = base + i;
                    if (slab_idx == 0 && h == 0) continue;  // Skip reserved handle 0
#ifndef NDEBUG
                    // Debug: track in free set
                    auto [it, inserted] = free_set_dbg_.insert(h);
                    assert(inserted && "Handle pushed to free list twice during slab init!");
#endif
                    free_handles_.push_back(h);
                }
            }
            
            return true;
        }
        
        void ObjectTable::refill_free_cache_locked(size_t target_batch) {
            // Early exit guards
            if (target_batch == 0) return;          // No-op
            if (free_bitmap_.empty()) return;       // Nothing to scan
            if (free_count_ == 0) return;           // No free bits at all
            
            const size_t capacity = slab_count_.load(std::memory_order_acquire) * entries_per_slab_;
            if (capacity == 0) return;
            
            // Pre-reserve cache to avoid reallocation churn
            const size_t want = std::min(target_batch, free_count_);
            if (want > 0) {
                free_handles_.reserve(free_handles_.size() + want);
            }
            
            const size_t nwords = free_bitmap_.size();
            size_t added = 0;
            size_t w = free_scan_cursor_;
            
            while (added < target_batch) {
                uint64_t word = (w < nwords) ? free_bitmap_[w] : 0;
                while (word && added < target_batch) {
                    // Find lowest set bit
                    #if defined(__GNUC__) || defined(__clang__)
                        int bit = __builtin_ctzll(word);
                    #elif defined(_MSC_VER)
                        unsigned long bit_ul;
                        _BitScanForward64(&bit_ul, word);
                        int bit = static_cast<int>(bit_ul);
                    #else
                        // Fallback implementation
                        int bit = 0;
                        uint64_t temp = word;
                        while ((temp & 1) == 0) {
                            temp >>= 1;
                            bit++;
                        }
                    #endif
                    
                    size_t h = (w << 6) + static_cast<size_t>(bit);
                    word &= (word - 1); // Clear lowest set bit
                    
                    if (h == 0 || h >= capacity) continue; // Skip reserved/overflow

                    // No parity filtering - all free handles are acceptable

                    // We don't clear the bitmap here; allocation clears it when popped
#ifndef NDEBUG
                    // Debug: track in free set, skip if already queued
                    assert(bm_test(h) && "Free bitmap not set for handle being queued by refill");
                    auto [it, inserted] = free_set_dbg_.insert(h);
                    if (!inserted) {
                        // Already queued in free_handles_; skip this handle
                        continue;
                    }
#endif
                    free_handles_.push_back(h);
                    ++added;
                }
                
                if (++w >= nwords) w = 0;           // Wrap around
                if (w == free_scan_cursor_) break;  // Full loop with no additions
            }
            
            free_scan_cursor_ = w;
        }
        
        uint64_t ObjectTable::acquire_handle_locked() {
            // Must be called with mu_ held

            // Helper lambda to pop handle and maintain debug set consistency
            auto pop_handle_debug = [this]() -> uint64_t {
                if (free_handles_.empty()) return 0;
                uint64_t h = free_handles_.back();
                free_handles_.pop_back();
#ifndef NDEBUG
                // Debug: remove from set and verify it was there
                size_t erased = free_set_dbg_.erase(h);
                if (erased != 1) {
                    trace() << "[OT_ERROR] acquire_handle_locked: h=" << h
                              << " not in free_set_dbg_! free_handles_.size()=" << free_handles_.size()
                              << " free_set_dbg_.size()=" << free_set_dbg_.size() << std::endl;
                    assert(erased == 1 && "Handle not in free set!");
                }
                // Extra defensive check
                assert(h != 0 && "Handle 0 should never enter free_handles_!");
#endif
                return h;
            };

            // Helper lambda to try acquiring a handle
            auto try_acquire = [this](uint64_t h, const char* context) -> std::optional<uint64_t> {
                if (h != 0 && bm_test(h)) {       // Still free?
#ifndef NDEBUG
                    // Verify bitmap shows FREE before clearing
                    assert(bm_test(h) && "Bitmap not FREE for handle being acquired!");
#endif
                    bm_clear(h);                   // Commit to used
                    if (h > max_handle_) {
                        max_handle_ = h;
                    }
                    return h;
                }
#ifndef NDEBUG
                else if (h != 0) {
                    // Log stale handle for diagnostics with context
                    trace() << "[OT_WARN] acquire_handle_locked: stale handle h=" << h
                              << " skipped in " << context << " phase (bitmap says not free)" << std::endl;
                }
#endif
                return std::nullopt;
            };

            // Pop until we find a *currently* free handle
            while (!free_handles_.empty()) {
                if (auto h = try_acquire(pop_handle_debug(), "initial")) {
                    return *h;
                }
                // else stale entry; skip without O(n) erase
            }

            // Cache empty: refill from bitmap
            refill_free_cache_locked();
            if (!free_handles_.empty()) {
                if (auto h = try_acquire(pop_handle_debug(), "refill")) {
                    return *h;
                }
            }

            // No free bits -> add slab
            if (!add_slab_locked()) {
                throw std::runtime_error("ObjectTable: Cannot allocate new slab - table is full");
            }
            // DO NOT call refill here; add_slab_locked already primed the cache

            // Pop one from the freshly primed cache
            while (!free_handles_.empty()) {
                if (auto h = try_acquire(pop_handle_debug(), "slab")) {
                    return *h;
                }
            }

            // If we get here, something is inconsistent
            throw std::runtime_error("ObjectTable: no free handle after slab add");
        }

        NodeID ObjectTable::allocate(NodeKind kind, uint8_t class_id, const OTAddr& addr, uint64_t /*birth_epoch_unused*/) {
            std::lock_guard<std::mutex> lk(mu_);

            // Pick a FREE handle and mark it RESERVED (bitmap clear happens inside)
            uint64_t h = acquire_handle_locked();

            OTEntry& e = slot_safe(h);
#ifndef NDEBUG
            // Debug: Verify slot is in FREE state before allocation
            assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted before allocate!");
            assert(e.dbg_state.load() == OTEntry::DBG_FREE && "Slot not FREE in allocate!");
            assert(e.birth_epoch.load(std::memory_order_relaxed) == 0 && "birth_epoch not 0 for FREE slot!");
            assert(e.kind == NodeKind::Invalid && "kind not Invalid for FREE slot!");
            // Mark as RESERVED
            e.dbg_state.store(OTEntry::DBG_RESERVED, std::memory_order_relaxed);
#endif
            e.addr = addr;
            e.class_id = class_id;
            e.kind = kind;

            // Keep entry non-live until publish/commit step
            e.birth_epoch.store(0, std::memory_order_relaxed);  // 0 = not live yet
            // Do not modify retire_epoch here; it's orthogonal to allocation

            // CRITICAL: Always bump tag on FREE->RESERVED to prevent ABA reissue
            // This guarantees each allocation of a handle produces a unique (handle, tag) pair
            uint16_t tag = e.tag.load(std::memory_order_relaxed);
            uint16_t new_tag = static_cast<uint16_t>(tag + 1);
            if (new_tag == 0) new_tag = 1;  // Skip 0 (reserved/invalid in NodeID encoding)
            e.tag.store(new_tag, std::memory_order_relaxed);

            // Update statistics
            stats_.total_allocations++;

            // Return the NodeID with handle index and NEW tag
            NodeID result = NodeID::from_parts(h, new_tag);

            // Special tracking for handle 1 reuse (can be removed after debugging)
            if (h == 1) {
                trace() << "[HANDLE_TRACE] allocate: h=1 NodeID=" << result.raw()
                          << " old_tag=" << static_cast<int>(tag)
                          << " new_tag=" << static_cast<int>(new_tag)
                          << " birth=" << e.birth_epoch.load(std::memory_order_relaxed)
                          << " retire=" << e.retire_epoch.load(std::memory_order_relaxed)
#ifndef NDEBUG
                          << " dbg_state=" << static_cast<int>(e.dbg_state.load())
                          << " magic=" << std::hex << e.dbg_magic << std::dec
#endif
                          << std::endl;
            }

            return result;
        }

        bool ObjectTable::abort_reservation(NodeID id) {
            const uint64_t h = id.handle_index();
            if (h == 0 || h > max_handle_) {
                return false;  // Invalid handle
            }

            OTEntry& e = slot_safe(h);

#ifndef NDEBUG
            // Verify magic in debug
            assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted in abort_reservation");
#endif

            // Must be RESERVED (never LIVE), birth==0
            if (e.birth_epoch.load(std::memory_order_relaxed) != 0) {
                return false;  // Not RESERVED
            }

#ifndef NDEBUG
            // Debug state must be RESERVED
            if (e.dbg_state.load(std::memory_order_relaxed) != OTEntry::DBG_RESERVED) {
                trace() << "[OT_ERROR] abort_reservation: entry not in RESERVED state"
                          << " h=" << h << " dbg_state="
                          << static_cast<int>(e.dbg_state.load()) << std::endl;
                return false;
            }
#endif

            // Tag must match
            if (e.tag.load(std::memory_order_relaxed) != id.tag()) {
                return false;  // Tag mismatch
            }

            // Clear address/length/class and kind
            e.addr = {};
            e.class_id = 0;
            e.kind = NodeKind::Invalid;

            // Bump tag to avoid ABA
            const uint16_t new_tag = static_cast<uint16_t>(e.tag.load(std::memory_order_relaxed) + 1);
            e.tag.store(new_tag, std::memory_order_relaxed);

#ifndef NDEBUG
            // Transition RESERVED -> FREE
            e.dbg_state.store(OTEntry::DBG_FREE, std::memory_order_release);

            trace() << "[OT_ABORT] h=" << h
                      << " old_tag=" << static_cast<int>(id.tag())
                      << " new_tag=" << static_cast<int>(new_tag)
                      << " -> FREE" << std::endl;
#endif

            // Return handle to the freelist (with lock)
            {
                std::lock_guard<std::mutex> lk(mu_);
#ifndef NDEBUG
                auto inserted = free_set_dbg_.insert(h).second;
                assert(inserted && "Handle pushed to free list twice during abort!");
#endif
                free_handles_.push_back(h);
                free_count_++;
            }

            return true;
        }

        void ObjectTable::retire(NodeID id, uint64_t retire_epoch) {
            const uint64_t h = id.handle_index();

            // Guard against invalid ids early
            if (!id.valid() || h == 0) {
                trace() << "[OT_ERROR] retire called with invalid id (handle=" << h
                          << ", tag=" << id.tag() << ", raw=" << id.raw() << ")" << std::endl;
                assert(false && "Cannot retire invalid/handle-0 NodeID");
                return;
            }

            auto& e = slot_safe(h);

#ifndef NDEBUG
            // Debug: Validate slot state before attempting retire
            {
                const uint16_t stored_tag = e.tag.load(std::memory_order_relaxed);
                if (stored_tag != id.tag()) {
                    trace() << "[OT_ERROR] retire: tag mismatch h=" << h
                              << " stored_tag=" << stored_tag
                              << " id.tag()=" << id.tag() << std::endl;
                    assert(false && "Tag mismatch in retire");
                    return;
                }

                const uint64_t b = e.birth_epoch.load(std::memory_order_relaxed);
                if (b == 0) {
                    const auto dbg_st = e.dbg_state.load(std::memory_order_relaxed);
                    trace() << "[OT_ERROR] retire: attempting to retire non-live entry!"
                              << " NodeID=" << id.raw()
                              << " h=" << h
                              << " birth=" << b
                              << " retire=" << e.retire_epoch.load()
                              << " kind=" << static_cast<int>(e.kind)
                              << " dbg_state=" << dbg_state_name(dbg_st) << " (" << static_cast<int>(dbg_st) << ")"
                              << " magic=" << std::hex << e.dbg_magic << std::dec
                              << std::endl;
                    assert(false && "cannot retire a free/unallocated entry");
                    return;
                }

                // Check magic watermark
                assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted in retire!");
            }
#endif

            // Make retire idempotent - only the first retire wins
            uint64_t expect = RETIRE_LIVE_MARKER;  // expect LIVE marker
            if (e.retire_epoch.compare_exchange_strong(expect, retire_epoch,
                    std::memory_order_release, std::memory_order_relaxed)) {
#ifndef NDEBUG
                // LIVE → RETIRED state transition
                auto st = e.dbg_state.load(std::memory_order_relaxed);
                // Allowed transitions into retire: LIVE only (RESERVED must not retire)
                if (st != OTEntry::DBG_LIVE) {
                    trace() << "[OT_ERROR] retire: invalid state transition!"
                              << " h=" << h << " state=" << dbg_state_name(st) << " (" << static_cast<int>(st) << ")"
                              << " (expected " << dbg_state_name(OTEntry::DBG_LIVE) << "=2)" << std::endl;
                    assert(st == OTEntry::DBG_LIVE && "retire() only valid from LIVE");
                }
                // Use release ordering to align with the CAS above
                e.dbg_state.store(OTEntry::DBG_RETIRED, std::memory_order_release);
#endif
                // Successfully retired - add to retired list for efficient reclamation
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    retired_handles_.push_back(h);
                    stats_.total_retires++;
                }
            } else {
#ifndef NDEBUG
                // CAS failed - entry was already retired or not yet committed
                const uint64_t cur_birth = e.birth_epoch.load(std::memory_order_relaxed);
                const uint64_t cur_retire = e.retire_epoch.load(std::memory_order_relaxed);
                if (cur_birth == 0) {
                    trace() << "[OT_WARN] retire called before commit: h=" << h
                              << " birth=" << cur_birth
                              << " retire_epoch=" << cur_retire << std::endl;
                } else if (cur_retire != RETIRE_LIVE_MARKER) {
                    // Already retired - this is fine (idempotent)
                    // Log only if verbose debugging is needed
                    if (std::getenv("OT_DEBUG_VERBOSE")) {
                        trace() << "[OT_DEBUG] retire: already retired h=" << h
                                  << " retire_epoch=" << cur_retire << std::endl;
                    }
                }
#endif
            }
        }
        
        NodeID ObjectTable::mark_live_reserve(NodeID proposed, uint64_t birth_epoch) {
            std::lock_guard<std::mutex> lk(mu_);
            const uint64_t h = proposed.handle_index();
            auto& e = slot_safe(h);

            // If already live in this process/batch, don't change anything
            if (e.birth_epoch.load(std::memory_order_acquire) != 0) {
                return proposed; // same handle, same tag
            }

            // Tag was already bumped in allocate() on FREE->RESERVED transition
            // Just return the proposed NodeID as-is
            // Do NOT set birth_epoch here; commit path does that after WAL is durable
            return proposed;
        }
        
        void ObjectTable::mark_live_commit(NodeID final_id, uint64_t birth_epoch) {
            uint64_t h = final_id.handle_index();
            auto& e = slot_safe(h);

            // Defensive clamp: 0 means "not live"; make sure we publish > 0
            if (birth_epoch == 0) birth_epoch = 1;

            // Check tag first - must always match
            const uint16_t stored_tag = e.tag.load(std::memory_order_relaxed);
            if (stored_tag != final_id.tag()) {
#ifndef NDEBUG
                trace() << "[OT_ERROR] Tag mismatch in mark_live_commit: h=" << h
                          << " stored_tag=" << stored_tag
                          << " final_tag=" << final_id.tag() << std::endl;
#endif
                assert(stored_tag == final_id.tag() && "Tag mismatch in mark_live_commit");
                return; // Safety: don't corrupt on tag mismatch
            }

            // Check if already LIVE (idempotent)
            const uint64_t cur_birth = e.birth_epoch.load(std::memory_order_acquire);
            if (cur_birth != 0) {
                // Already LIVE: this is a duplicate commit
#ifndef NDEBUG
                auto current_state = e.dbg_state.load();
                if (current_state != OTEntry::DBG_LIVE) {
                    trace() << "[OT_ERROR] Slot has birth_epoch=" << cur_birth
                              << " but state=" << static_cast<int>(current_state)
                              << " (expected LIVE=2)" << std::endl;
                    assert(false && "Inconsistent state: birth_epoch set but not LIVE");
                }
                // Check epoch consistency - warn but don't assert
                // A node can be validly re-committed in a later epoch if it was modified
                if (cur_birth != birth_epoch) {
                    // This is expected when a bucket is modified across epochs
                    // (e.g., during splits that span multiple operations)
                    // The node is already LIVE, so this is safe - just informational
                    // Suppress verbose warning for now
                    // trace() << "[OT_INFO] Re-commit with different epoch: h=" << h
                    //           << " NodeID=" << final_id.raw()
                    //           << " current_epoch=" << cur_birth
                    //           << " new_epoch=" << birth_epoch << std::endl;
                }
                trace() << "[OT_WARN] Double commit detected for h=" << h
                          << " NodeID=" << final_id.raw()
                          << " epoch=" << birth_epoch
                          << " - ignoring (idempotent)" << std::endl;
#endif
                return; // Idempotent: already committed
            }

#ifndef NDEBUG
            // Debug: Verify state transition RESERVED -> LIVE
            assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted in mark_live_commit!");
            auto current_state = e.dbg_state.load();
            if (current_state != OTEntry::DBG_RESERVED) {
                trace() << "[OT_ERROR] mark_live_commit: h=" << h
                          << " NodeID=" << final_id.raw()
                          << " expected_state=RESERVED(1) actual_state=" << static_cast<int>(current_state)
                          << " birth=" << cur_birth
                          << " retire=" << e.retire_epoch.load()
                          << std::endl;
                assert(false && "Expected RESERVED state in mark_live_commit");
            }
            // Transition to LIVE
            e.dbg_state.store(OTEntry::DBG_LIVE, std::memory_order_relaxed);
#endif

            // Make visible as "not retired"
            // (Readers check retire_epoch==MAX || >= cur_epoch,
            // but the key 'live' bit is birth_epoch > 0)
            e.retire_epoch.store(~uint64_t{0}, std::memory_order_relaxed);

            // PUBLISH LIVENESS LAST with release semantics so readers who do
            // acquire on birth_epoch see all prior stores (addr/kind/class_id/tag/retire_epoch)
            e.birth_epoch.store(birth_epoch, std::memory_order_release);
        }

        size_t ObjectTable::reclaim_before_epoch(uint64_t safe_epoch) {
            struct ToFree {
                SegmentAllocator::Allocation alloc;
                uint64_t handle;
            };
            
            std::vector<ToFree> to_free;
            std::vector<uint64_t> reclaimed_handles;
            std::vector<uint64_t> still_retired;  // Build this but don't assign until Phase 3
            size_t freed = 0;
            
            // Reserve space to avoid reallocations during reclaim
            // Do this before acquiring the lock to minimize time under lock
            size_t retired_count = 0;
            {
                std::lock_guard<std::mutex> lk(mu_);
                retired_count = retired_handles_.size();
            }
            to_free.reserve(retired_count);
            reclaimed_handles.reserve(retired_count);
            still_retired.reserve(retired_count);
            
            // Phase 1: Under lock, identify what to free but DO NOT clear entries yet
            // This ensures crash-safety: if we crash before freeing, entries remain
            // in retired state and can be reclaimed in a future pass
            {
                std::lock_guard<std::mutex> lk(mu_);
                
                // Process only retired handles - O(retired) not O(capacity)
                
                for (uint64_t h : retired_handles_) {
                    auto& e = slot_safe(h);
                    uint64_t r = e.retire_epoch.load(std::memory_order_acquire);
                    
                    if (r == ~uint64_t{0}) {
                        // Not retired (anymore) - was already reclaimed or double-pushed
                        // Skip and do NOT keep in retired list
                        continue;
                    }
                    
                    if (r < safe_epoch) {
                        // Track statistics at decision time (Phase 1)
                        // We update stats here even though clearing happens in Phase 3
                        // This ensures stats reflect our reclaim decisions regardless of free failures
                        uint8_t class_id = e.class_id;
                        size_t bytes = e.addr.length;
                        if (class_id < stats_.bytes_per_class.size()) {
                            stats_.bytes_per_class[class_id] += bytes;      // Track bytes reclaimed per class
                            stats_.reclaims_per_class[class_id]++;          // Track count of reclaims per class
                        }
                        stats_.bytes_reclaimed += bytes;
                        stats_.total_reclaims++;
                        
                        // Safe to reclaim - collect allocation info if we have an allocator
                        if (segment_allocator_ && e.addr.length > 0) {
                            to_free.push_back(ToFree{
                                SegmentAllocator::Allocation{
                                    e.addr.file_id,
                                    e.addr.segment_id,
                                    e.addr.offset,
                                    e.addr.length,
                                    e.class_id,
                                    MappingManager::Pin()  // Empty pin - no memory mapped yet
                                },
                                h
                            });
                        } else {
                            // No allocator or nothing to free, just track handle
                            reclaimed_handles.push_back(h);
                        }
                        
                        // DO NOT clear the entry here - keep it in retired state
                        // We'll clear it in Phase 3 after the allocator free completes
                        
                        freed++;
                    } else {
                        // Still retired but not safe to reclaim yet
                        still_retired.push_back(h);
                    }
                }
                
                // IMPORTANT: Do NOT assign retired_handles_ here
                // We only finalize it in Phase 3 after allocator frees complete
            }
            // Lock released here
            
            // Phase 2: Outside lock, free physical segments (may do I/O or lock allocator)
            for (auto& tf : to_free) {  // Non-const so we can move the allocation
                segment_allocator_->free(tf.alloc);
                reclaimed_handles.push_back(tf.handle);
            }
            
            // Phase 3: Finalize - clear entries, update retired list, return handles to free list
            {
                std::lock_guard<std::mutex> lk(mu_);
                
                // Clear entries to free state
                for (uint64_t h : reclaimed_handles) {
                    auto& e = slot_safe(h);

#ifndef NDEBUG
                    // Debug: Verify we're reclaiming a RETIRED entry and transition to FREE
                    assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted in reclaim!");
                    assert(e.dbg_state.load() == OTEntry::DBG_RETIRED && "Can only reclaim RETIRED entries!");
                    // Transition RETIRED -> FREE
                    e.dbg_state.store(OTEntry::DBG_FREE, std::memory_order_relaxed);
#endif

                    // Reset to free state but KEEP retire_epoch as breadcrumb for reuse detection
                    // DO NOT reset retire_epoch to ~uint64_t{0} - this is our reuse indicator!
                    // e.retire_epoch stays as-is (the epoch when it was retired)
                    e.birth_epoch.store(0, std::memory_order_relaxed);
                    // Single-bump invariant: do NOT touch tag here

                    // Clear other fields for safety
                    e.addr = {};
                    e.class_id = 0;
                    e.kind = NodeKind::Invalid;  // Free OT slot, never visible to readers
                }
                
                // NOW it's safe to update retired_handles_ after allocator frees are done
                retired_handles_ = std::move(still_retired);
                
                // Ensure bitmap capacity and mark handles as free
                size_t max_handle_idx = 0;
                for (uint64_t h : reclaimed_handles) {
                    if (h > max_handle_idx) max_handle_idx = h;
                }
                if (max_handle_idx > 0) {
                    const size_t need_words = (max_handle_idx >> 6) + 1;
                    if (free_bitmap_.size() < need_words) {
                        free_bitmap_.resize(need_words, 0);
                    }
                }
                
                // Mark handles as free in bitmap and prime cache for immediate reuse
                for (uint64_t h : reclaimed_handles) {
                    if (h == 0) continue;                 // Never queue handle 0
                    if (!bm_test(h)) bm_set(h);           // 1 = free; bumps free_count_ only if not already free
#ifndef NDEBUG
                    auto [it, inserted] = free_set_dbg_.insert(h);
                    assert(inserted && "Handle pushed to free list twice during reclaim!");
#endif
                    free_handles_.push_back(h);           // LIFO: reclaimed handles allocated next
                }
                
                stats_.last_reclaim_count = freed;
            }
            
            return freed;
        }
        
        void ObjectTable::begin_recovery() {
            std::lock_guard<std::mutex> lk(mu_);
            recovery_mode_ = true;
            
            const uint32_t published = slab_count_.load(std::memory_order_relaxed);
            const size_t capacity = published * entries_per_slab_;
            free_bitmap_.assign((capacity + 63) / 64, 0);
            free_count_ = 0;  // Reset counter, will be updated by bm_set calls
            
            // One-time O(N) scan (cold start): mark free handles in the bitmap.
            // This is simpler and more robust than stitching from freelist alone.
            for (uint32_t si = 0; si < published; ++si) {
                OTEntry* slab = get_slab_ptr(si);
                if (!slab) continue;  // Skip unallocated slabs
                
                for (size_t i = 0; i < entries_per_slab_; ++i) {
                    const size_t h = (si * entries_per_slab_) + i;
                    const OTEntry& e = slab[i];
                    // Use the canonical FREE definition: birth==0 && kind==Invalid
                    // This correctly handles post-reclaim entries with retire breadcrumbs
                    if (e.is_free()) {
                        bm_set(h);  // This also increments free_count_
                    }
                }
            }
            
            // We won't mutate free_handles_ during recovery;
            // leave it untouched and rebuild it in end_recovery().
        }
        
        void ObjectTable::end_recovery() {
            std::lock_guard<std::mutex> lk(mu_);
            free_handles_.clear();
            retired_handles_.clear();  // Also rebuild retired list
#ifndef NDEBUG
            free_set_dbg_.clear();  // Clear debug set before rebuilding
#endif
            
            // Compute capacity once and reserve space
            const uint32_t published = slab_count_.load(std::memory_order_relaxed);
            const size_t capacity = published * entries_per_slab_;
            const size_t num_words = free_bitmap_.size();
            
            // Reserve upper bound to avoid reallocation during rebuild
            // Could use popcount for exact count, but upper bound is cheap
            free_handles_.reserve(std::min(capacity, num_words * 64));
            
            // Rebuild free_handles_ using word-wise operations for speed
            for (size_t w = 0; w < num_words; ++w) {
                uint64_t word = free_bitmap_[w];
                if (word == 0) continue;  // Skip words with no free handles
                
                // Process each set bit in the word
                while (word != 0) {
                    // Find the position of the lowest set bit
                    #ifdef __GNUC__
                        int bit_pos = __builtin_ctzll(word);  // Count trailing zeros
                    #elif defined(_MSC_VER)
                        unsigned long bit_pos_ul;
                        _BitScanForward64(&bit_pos_ul, word);
                        int bit_pos = static_cast<int>(bit_pos_ul);
                    #else
                        // Fallback for other compilers
                        int bit_pos = 0;
                        uint64_t temp = word;
                        while ((temp & 1) == 0) {
                            bit_pos++;
                            temp >>= 1;
                        }
                    #endif
                    
                    // Calculate the handle index
                    size_t handle = (w << 6) + static_cast<size_t>(bit_pos);
                    
                    // Reserve handle 0 forever - ensures raw() is never 0
                    if (handle == 0) {
                        word &= word - 1;  // Clear the processed bit
                        continue;
                    }
                    
                    // Only add if within capacity (last word might have extra bits)
                    if (handle < capacity) {
#ifndef NDEBUG
                        auto [it, inserted] = free_set_dbg_.insert(handle);
                        assert(inserted && "Handle pushed to free list twice during recovery!");
#endif
                        free_handles_.push_back(handle);
                    }
                    
                    // Clear the processed bit
                    word &= word - 1;  // Clear lowest set bit
                }
            }
            
            // Keep allocator behavior: pop lowest handle first
            // We pushed low->high, so reverse to get high->low for pop_back()
            std::reverse(free_handles_.begin(), free_handles_.end());
            
            // Conservatively reset max_handle_ to top of current slabs
            max_handle_ = capacity ? (capacity - 1) : 0;
            
            // Also rebuild retired_handles_ for entries that ended up retired
            // This ensures the first reclaim pass doesn't miss anything
            retired_handles_.reserve(capacity / 4);  // Heuristic: most won't be retired
            
            for (uint32_t slab_idx = 0; slab_idx < published; ++slab_idx) {
                OTEntry* slab = get_slab_ptr(slab_idx);
                if (!slab) continue;
                
                for (size_t slot = 0; slot < entries_per_slab_; ++slot) {
                    uint64_t handle = (slab_idx << slab_shift_) | slot;
                    if (handle == 0) continue;  // Skip reserved handle 0
                    
                    const OTEntry& e = slab[slot];
                    if (e.is_retired()) {
                        retired_handles_.push_back(handle);
                    }
                }
            }
            
            // Don't clear the bitmap anymore - we keep it permanently for O(1) operations
            // free_bitmap_ stays populated and free_count_ is already accurate from bm_set calls
            recovery_mode_ = false;
        }
        
        void ObjectTable::restore_handle(uint64_t handle_idx, const OTCheckpoint::PersistentEntry& pe) {
            std::lock_guard<std::mutex> lk(mu_);
            
            // Ensure we have enough slabs allocated
            uint32_t slab_idx = handle_idx >> slab_shift_;
            uint32_t current_count = slab_count_.load(std::memory_order_acquire);
            
            // Allocate slabs up to the required index
            while (current_count <= slab_idx) {
                if (!add_slab_locked()) {
                    throw std::runtime_error("ObjectTable: Cannot allocate slab for restore - table is full");
                }
                current_count = slab_count_.load(std::memory_order_acquire);
            }
            
            // Ensure bitmap has capacity for this handle
            const size_t need_words = (handle_idx >> 6) + 1;
            if (free_bitmap_.size() < need_words) {
                free_bitmap_.resize(need_words, 0);  // Default 0 = used
            }
            
            // Always use bitmap for O(1) removal (handle is being restored as used)
            bm_clear(handle_idx);
            // No need to remove from cache - stale entries are handled by acquire_handle_locked
            
            // Guard against illegal "free" shape sneaking in from checkpoint
            // (shouldn't happen with well-formed checkpoints, but defensive)
            if (pe.birth_epoch == 0 && pe.retire_epoch == ~uint64_t{0}) {
                // This is a free slot that shouldn't be in the checkpoint; skip
                return;
            }
            
            // Restore the entry at the exact handle index
            OTEntry& entry = slot(handle_idx);
            
            // Set address
            entry.addr.file_id = pe.file_id;
            entry.addr.segment_id = pe.segment_id;
            entry.addr.offset = pe.offset;
            entry.addr.length = pe.length;
            
            // Set metadata
            entry.class_id = pe.class_id;
            entry.kind = static_cast<NodeKind>(pe.kind);
            
            // Set epochs - use relaxed since we're under lock
            entry.birth_epoch.store(pe.birth_epoch, std::memory_order_relaxed);
            entry.retire_epoch.store(pe.retire_epoch, std::memory_order_relaxed);
            
            // Set tag last with release to publish all fields
            // Use the tag from checkpoint or default to 1 if missing
            entry.tag.store(pe.tag ? pe.tag : uint16_t{1}, std::memory_order_release);
            
            // Update max_handle_ if necessary
            if (handle_idx > max_handle_) {
                max_handle_ = handle_idx;
            }
            
            // Don't track as allocation - this is restoration not allocation
        }
        
        void ObjectTable::apply_delta(const OTDeltaRec& rec) {
            std::lock_guard<std::mutex> lk(mu_);
            
            // Ensure we have enough slabs allocated
            uint32_t slab_idx = rec.handle_idx >> slab_shift_;
            uint32_t current_count = slab_count_.load(std::memory_order_relaxed);
            
            // Allocate slabs up to the required index
            while (current_count <= slab_idx) {
                if (!add_slab_locked()) {
                    throw std::runtime_error("ObjectTable: Cannot allocate slab for delta - table is full");
                }
                current_count = slab_count_.load(std::memory_order_relaxed);
            }
            
            // Check if we need to resize bitmap during recovery mode
            // Ensure bitmap has capacity for this handle (always maintain bitmap now)
            const size_t need_words = (static_cast<size_t>(rec.handle_idx) >> 6) + 1;
            if (free_bitmap_.size() < need_words) {
                free_bitmap_.resize(need_words, 0);  // Default 0 = used
            }
            
            // Get the entry
            OTEntry& entry = slot(rec.handle_idx);
            
            // Apply all fields from the delta
            entry.addr.file_id = rec.file_id;
            entry.addr.segment_id = rec.segment_id;
            entry.addr.offset = rec.offset;
            entry.addr.length = rec.length;
            
            entry.class_id = rec.class_id;
            entry.kind = static_cast<NodeKind>(rec.kind);
            
            // Set epochs with relaxed ordering since we're under lock
            entry.birth_epoch.store(rec.birth_epoch, std::memory_order_relaxed);
            entry.retire_epoch.store(rec.retire_epoch, std::memory_order_relaxed);
            
            // Update bitmap based on whether this is now free or used
            // FREE state: birth=0 && kind=Invalid (ignore retire_epoch breadcrumb)
            const bool is_free = (rec.birth_epoch == 0 && 
                                  rec.kind == static_cast<uint8_t>(NodeKind::Invalid));
            
            // Always use bitmap for O(1) operations
            if (is_free) {
                bm_set(rec.handle_idx);    // Mark as free
                // Optional fast path: add to cache if it's getting low
                if (!recovery_mode_ && free_handles_.size() < 64) {
#ifndef NDEBUG
                    auto [it, inserted] = free_set_dbg_.insert(rec.handle_idx);
                    if (!inserted) {
                        // In apply_delta, duplicates are possible if handle was already in cache
                        // This is OK - just skip adding it again
                    } else {
#endif
                        free_handles_.push_back(rec.handle_idx);
#ifndef NDEBUG
                    }
#endif
                }
            } else {
                bm_clear(rec.handle_idx);  // Mark as used
                // No vector erase needed - stale entries in cache are handled by acquire_handle_locked
            }
            
            // Set debug state based on birth/retire epochs (for debug builds)
#ifndef NDEBUG
            if (is_free) {
                entry.dbg_state.store(OTEntry::DBG_FREE, std::memory_order_relaxed);
            } else if (rec.retire_epoch != ~uint64_t{0}) {
                // Retired but not yet reclaimed
                entry.dbg_state.store(OTEntry::DBG_RETIRED, std::memory_order_relaxed);
            } else {
                // Live entry
                entry.dbg_state.store(OTEntry::DBG_LIVE, std::memory_order_relaxed);
            }
#endif

            // Publish tag last with release memory ordering
            // This ensures all fields are visible before tag is published
            entry.tag.store(rec.tag, std::memory_order_release);

            // Update max_handle_ safely
            max_handle_ = std::max<uint64_t>(max_handle_, rec.handle_idx);
        }

    }
} // namespace xtree::persist