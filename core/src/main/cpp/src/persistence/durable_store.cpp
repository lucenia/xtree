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

#include "durable_store.h"
#include "durable_runtime.h"  // Added for root management
#include "ot_delta_log.h"
#include "platform_fs.h"
#include "checksums.h"
#include <cassert>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>

#include "../util/log.h"  // For logging (trace, debug, info, warn, error)

namespace xtree {
    namespace persist {
        
        // Define thread-local batch storage
        thread_local DurableStore::ThreadBatch DurableStore::tl_batch_;
        
        // Helper functions for creating delta records
        static inline OTDeltaRec make_alloc_delta(NodeID id, const OTEntry& e) {
            OTDeltaRec d{};
            d.handle_idx    = id.handle_index();
            d.tag           = id.tag(); // must match OT tag at append time
            d.kind          = static_cast<uint8_t>(e.kind);
            d.class_id      = e.class_id;
            d.file_id       = e.addr.file_id;
            d.segment_id    = e.addr.segment_id;
            d.offset        = e.addr.offset;
            d.length        = static_cast<uint32_t>(e.addr.length); // allocation size, not payload
            d.data_crc32c   = 0;    // set later if payload included
            d.birth_epoch   = 0;    // stamped at commit
            d.retire_epoch  = ~uint64_t{0}; // live
            return d;
        }
        
        static inline OTDeltaRec make_retire_delta(NodeID id, const OTEntry& e) {
            OTDeltaRec d{};
            d.handle_idx    = id.handle_index();
            d.tag           = id.tag();
            d.kind          = static_cast<uint8_t>(e.kind);
            d.class_id      = e.class_id;
            d.file_id       = e.addr.file_id;
            d.segment_id    = e.addr.segment_id;
            d.offset        = e.addr.offset;
            d.length        = static_cast<uint32_t>(e.addr.length);
            d.data_crc32c   = 0;
            d.birth_epoch   = e.birth_epoch; // preserve original birth
            d.retire_epoch  = 0;  // stamped at commit
            return d;
        }

        DurableStore::DurableStore(DurableContext& ctx, std::string name, DurabilityPolicy policy) 
            : ctx_(ctx), name_(std::move(name)), policy_(std::move(policy)) {}

        DurableStore::~DurableStore() {
            // Ensure any pending writes are flushed
            if (!tl_batch_.writes.empty() || !tl_batch_.retirements.empty()) {
                // Log warning - uncommitted writes at destruction
                // In production, consider throwing or forcing a commit
            }
        }

        AllocResult DurableStore::allocate_node(size_t min_len, NodeKind kind) {
            // Choose size-class and allocate from SegmentAllocator
            // Pass NodeKind to determine file type (.xi for tree nodes, .xd for data records)
            auto a = ctx_.alloc.allocate(min_len, kind);
            
            // Check if allocation failed (returns all zeros)
            if (!a.is_valid()) {
                // Segment allocator is full - backpressure to caller
                throw std::runtime_error("Failed to allocate segment: out of space or too many segments");
            }

            // Get the memory-mapped pointer directly from the allocator
            void* vaddr = ctx_.alloc.get_ptr(a);
            if (!vaddr) {
                // Failed to get mapped pointer - this is critical
                throw std::runtime_error("Failed to get memory-mapped pointer for allocation");
            }
            
            // Zero out the allocation to prevent garbage in padding areas
            // Critical for smaller allocations (256B, 512B) that get reused frequently
            // Without this, deserialization may read garbage data from padding
            std::memset(vaddr, 0, a.length);
            
            // Create the address with the memory-mapped pointer
            OTAddr addr{a.file_id, a.segment_id, a.offset, static_cast<uint32_t>(a.length), vaddr};
            
            // Birth epoch = 0 until commit (invisible to readers)
            const uint64_t birth = 0;  // Will be stamped by mark_live() at commit
            
            // Allocate handle in ObjectTable
            // This returns a NodeID with the NEXT tag (not yet stored in OT)
            NodeID id = ctx_.ot.allocate(kind, a.class_id, addr, birth);

            // Debug: trace high-shard allocations (shard >= 9)
            uint32_t shard = (id.handle_index() >> 42) & 0x3F;
            if (shard >= 9) {
                trace() << "[ALLOC_HIGH_SHARD] shard=" << shard
                          << " NodeID=" << id.raw()
                          << " handle_idx=" << id.handle_index()
                          << " tag=" << static_cast<int>(id.tag())
                          << std::endl;
            }

            // Stage the uncommitted node for tx-local visibility
            // This allows the writer to see its own uncommitted nodes
            tl_batch_.pending_nodes[id.handle_index()] = { vaddr, a.length };

            // Return the allocation result with the destination pointer
            return { id, vaddr, a.length };
        }

        void DurableStore::publish_node(NodeID id, const void* data, size_t len) {
            // Debug: trace high-shard publishes (shard >= 9)
            uint32_t pub_shard = (id.handle_index() >> 42) & 0x3F;
            if (pub_shard >= 9) {
                trace() << "[PUBLISH_HIGH_SHARD] shard=" << pub_shard
                          << " NodeID=" << id.raw()
                          << " handle_idx=" << id.handle_index()
                          << " tag=" << static_cast<int>(id.tag())
                          << " len=" << len
                          << std::endl;
            }

            // Debug assertion: NodeID must be valid (non-zero)
            assert(id.valid() && "publish_node called with invalid NodeID (0)");
            assert(id.raw() != 0 && "publish_node called with NodeID.raw() == 0");
            
            // DO NOT call ctx_.ot.get(id) - the tag isn't published yet!
            // We must have captured the destination pointer at allocation time
            
            // Find the pending allocation with this NodeID
            // For now, we'll need to track it or pass it through somehow
            // Actually, we should save the vaddr in allocate_node result
            
            // TEMPORARY: We need to get the allocation info somehow
            // The caller has the AllocResult with vaddr from allocate_node()
            // But publish_node() doesn't receive it...
            // 
            // For now, we'll have to peek at the OT entry WITHOUT validating tag
            // This is safe because we hold the handle (it's not in free list)
            uint64_t h = id.handle_index();
            const auto& e = ctx_.ot.get_by_handle_unsafe(h);
            void* dst_vaddr = e.addr.vaddr;
            size_t capacity = e.addr.length;
            
            // Bounds check - CRITICAL: Detect buffer overflow
            if (len > capacity) {
                // Node has grown beyond its allocation - this is a critical error
                // This typically happens when XTreeBucket becomes a supernode
                // 
                // The proper fix requires one of:
                // 1. Pre-allocate larger segments for buckets that might grow
                // 2. Implement bucket reallocation with NodeID updates at XTree layer
                // 3. Use out-of-line storage for large child vectors (as designed)
                
                std::stringstream error_msg;
                error_msg << "Buffer overflow detected in publish_node:\n"
                          << "  NodeID: " << id.raw() << "\n"
                          << "  Wire size: " << len << " bytes\n"
                          << "  Allocated: " << capacity << " bytes\n"
                          << "  Overflow: " << (len - capacity) << " bytes\n"
                          << "  Node kind: " << static_cast<int>(e.kind) << "\n"
                          << "This typically occurs when an XTreeBucket grows into a supernode.\n"
                          << "Immediate workarounds:\n"
                          << "  1. Increase minimum size class (currently " << size_class::kMinSize << ")\n"
                          << "  2. Reduce XTREE_M to limit bucket fanout\n";
                
                // Throw exception to prevent data corruption
                // The alternative (clamping) causes silent data loss
                throw std::runtime_error(error_msg.str());
            }
            
            // Build delta with the NodeID's tag (computed at allocation)
            OTDeltaRec delta = make_alloc_delta(id, e);
            delta.tag = id.tag();  // Use the tag from allocation
            
            // Policy-based staging (NO WAL APPENDS HERE)
            switch (policy_.mode) {
                case DurabilityMode::STRICT: {
                    // STRICT: Copy to segment, track for flush at commit
                    // No CRC needed - data is flushed before WAL
                    if (data && dst_vaddr && len > 0) {
                        std::memcpy(dst_vaddr, data, len);
                        // Track for flush at commit (reduces syscalls)
                        tl_batch_.dirty_ranges.push_back(DirtyRange{
                            dst_vaddr,
                            static_cast<uint32_t>(len)
                        });
                    }
                    // DO NOT append to WAL here - happens in commit()
                    break;
                }
                
                case DurabilityMode::BALANCED: {
                    // BALANCED: Small payloads staged for WAL, large to segments
                    if (data && dst_vaddr && len > 0) {
                        // Always copy to segment first
                        std::memcpy(dst_vaddr, data, len);
                        
                        if (len <= policy_.max_payload_in_wal) {
                            // Small node: compute CRC for WAL inclusion
                            delta.data_crc32c = crc32c(data, len);
                        } else {
                            // Large node: compute CRC and track for flush
                            delta.data_crc32c = crc32c(dst_vaddr, len);
                            tl_batch_.dirty_ranges.push_back(DirtyRange{
                                dst_vaddr,
                                static_cast<uint32_t>(len)
                            });
                        }
                    }
                    // DO NOT append to WAL here - happens in commit()
                    break;
                }
                
                case DurabilityMode::EVENTUAL: {
                    // EVENTUAL: Prefer payload-in-WAL for small nodes
                    if (data && dst_vaddr && len > 0) {
                        // Always copy to segment
                        std::memcpy(dst_vaddr, data, len);
                        
                        if (len <= policy_.max_payload_in_wal) {
                            // Small node: CRC for payload-in-WAL
                            delta.data_crc32c = crc32c(data, len);
                        } else {
                            // Large node: no CRC in EVENTUAL mode (best-effort)
                            delta.data_crc32c = 0;
                            // Track dirty for checkpoint/rotation flush
                            tl_batch_.dirty_ranges.push_back(DirtyRange{
                                dst_vaddr,
                                static_cast<uint32_t>(len)
                            });
                        }
                    }
                    // DO NOT append to WAL here - happens in commit()
                    break;
                }
            }
            
            // Track for batch operations (DO NOT mark_live here)
            tl_batch_.stage_write(PendingWrite{
                .id = id,
                .len = static_cast<uint32_t>(len),
                .delta = delta,
                .dst_vaddr = dst_vaddr,
                .include_payload = (len > 0 && len <= policy_.max_payload_in_wal)
            });

            // Update tx-local staging with the published content
            // This ensures writer can read back its own uncommitted changes
            // We update with actual length used (not allocated capacity)
            if (dst_vaddr && len > 0) {
                tl_batch_.pending_nodes[id.handle_index()] = { dst_vaddr, len };
            }
        }
        
        void DurableStore::publish_node_in_place(NodeID id, size_t len) {
            // Debug assertion: NodeID must be valid (non-zero)
            assert(id.valid() && "publish_node_in_place called with invalid NodeID (0)");
            assert(id.raw() != 0 && "publish_node_in_place called with NodeID.raw() == 0");
            
            // NOTE: As in publish_node(), do NOT use ctx_.ot.get(id) here
            // because the tag may not be published yet. Use handle-index unsafe get.
            const uint64_t h = id.handle_index();
            const auto& e = ctx_.ot.get_by_handle_unsafe(h);
            
            void* dst_vaddr = e.addr.vaddr;
            size_t capacity = e.addr.length;
            
            // Basic guards
            if (!dst_vaddr || len == 0) {
                // Nothing to persist or invalid destination — treat as a no-op for robustness
                return;
            }
            
            // Bounds check — keep identical behavior to publish_node()
            if (len > capacity) {
                std::stringstream ss;
                ss << "publish_node_in_place overflow:\n"
                   << "  NodeID: " << id.raw() << "\n"
                   << "  Wire size: " << len << " bytes\n"
                   << "  Allocated: " << capacity << " bytes\n"
                   << "  Overflow: " << (len - capacity) << " bytes\n"
                   << "  Node kind: " << int(e.kind) << "\n";
                throw std::runtime_error(ss.str());
            }
            
            // Build delta using the same helper as publish_node()
            OTDeltaRec delta = make_alloc_delta(id, e);
            delta.tag = id.tag();  // Tag assigned at allocation time
            
            // Policy handling — mirror publish_node(), but use dst_vaddr as the source.
            bool include_payload_in_wal = false;
            
            switch (policy_.mode) {
                case DurabilityMode::STRICT: {
                    // STRICT: no WAL payload; data must be flushed before WAL append.
                    // Track dirty range so commit() can msync.
                    tl_batch_.dirty_ranges.push_back(DirtyRange{
                        dst_vaddr, static_cast<uint32_t>(len)
                    });
                    
                    // No CRC required for STRICT in your current design
                    delta.data_crc32c = 0;
                    include_payload_in_wal = false;
                    break;
                }
                
                case DurabilityMode::BALANCED: {
                    if (len <= policy_.max_payload_in_wal) {
                        // Small: WAL carries payload; compute CRC over the mapped bytes.
                        delta.data_crc32c = crc32c(dst_vaddr, len);
                        include_payload_in_wal = true;
                        
                        // Keep behavior consistent with publish_node(): for small payloads,
                        // do not force a dirty-range flush (WAL has the payload).
                    } else {
                        // Large: no WAL payload; flush mapped bytes and include CRC over dst.
                        delta.data_crc32c = crc32c(dst_vaddr, len);
                        tl_batch_.dirty_ranges.push_back(DirtyRange{
                            dst_vaddr, static_cast<uint32_t>(len)
                        });
                        include_payload_in_wal = false;
                    }
                    break;
                }
                
                case DurabilityMode::EVENTUAL: {
                    if (len <= policy_.max_payload_in_wal) {
                        // Small: WAL carries payload; compute CRC over dst for integrity.
                        delta.data_crc32c = crc32c(dst_vaddr, len);
                        include_payload_in_wal = true;
                        // No mandatory dirty-range tracking; eventual mode favors WAL for small.
                    } else {
                        // Large: best-effort; skip CRC, rely on future checkpoint/flush.
                        delta.data_crc32c = 0;
                        tl_batch_.dirty_ranges.push_back(DirtyRange{
                            dst_vaddr, static_cast<uint32_t>(len)
                        });
                        include_payload_in_wal = false;
                    }
                    break;
                }
            }
            
            // Queue the write for the batch; WAL builder can read from dst_vaddr at commit.
            tl_batch_.stage_write(PendingWrite{
                .id = id,
                .len = static_cast<uint32_t>(len),
                .delta = delta,
                .dst_vaddr = dst_vaddr,
                .include_payload = include_payload_in_wal
            });

            // Update tx-local staging for in-place published content
            // The data is already at dst_vaddr from caller's serialization
            if (dst_vaddr && len > 0) {
                tl_batch_.pending_nodes[id.handle_index()] = { dst_vaddr, len };
            }

            // IMPORTANT: no memcpy here — payload already resides at dst_vaddr
            //            from the caller's serialization step.
        }

        // Internal helper: resolve OTEntry for a NodeID, handling uncommitted visibility
        const OTEntry* DurableStore::resolve_entry(NodeID id, bool& is_uncommitted) const noexcept {
            const uint64_t h = id.handle_index();
            const OTEntry* e = ctx_.ot.try_get_by_handle(h);
            if (!e) {
#ifndef NDEBUG
                debug() << "resolve_entry - try_get_by_handle(" << h << ") returned nullptr"
                        << " for NodeID " << id.raw();
#endif
                return nullptr;
            }

            is_uncommitted = (e->birth_epoch == 0);

            if (!is_uncommitted) {
                // Committed path: require ABA-safe tag match
                if (!ctx_.ot.validate_tag(id)) {
                    return nullptr;
                }

                // Enforce epoch visibility
                uint64_t epoch = ctx_.mvcc.get_global_epoch();
                if (e->birth_epoch > epoch) return nullptr;
                if (e->retire_epoch != ~uint64_t{0} && e->retire_epoch <= epoch) return nullptr;
            } else {
                // TODO: check writer-thread ownership for safety
                // if (!ctx_.mvcc.is_writer_thread()) return nullptr;
            }

            return e;
        }

        NodeBytes DurableStore::read_node(NodeID id) const {
            bool is_uncommitted = false;
            const OTEntry* e = resolve_entry(id, is_uncommitted);
            if (!e) return { nullptr, 0 };

            // Check tx-local staging for uncommitted nodes (writer visibility)
            if (is_uncommitted) {
                auto it = tl_batch_.pending_nodes.find(id.handle_index());
                if (it != tl_batch_.pending_nodes.end()) {
                    // Return the staged buffer for this uncommitted node
                    return it->second;
                }
                // Uncommitted but not in tx-local staging - can happen during reads
                // after inserts where node allocation hasn't been fully committed
#ifndef NDEBUG
                trace() << "[WARN] Uncommitted node " << id.raw()
                          << " not in staging for read - returning empty\n";
#endif
                // Return empty - caller should handle gracefully
                return { nullptr, 0 };
            }

            // Committed node: use the address from ObjectTable
            void* ptr = e->addr.vaddr;
            if (ptr == nullptr) {
                // Debug: trace all recovery reads for class 6
                if (e->class_id == 6) {
                    trace() << "[READ_CLASS6] NodeID=" << id.raw()
                              << " file_id=" << e->addr.file_id
                              << " segment_id=" << e->addr.segment_id
                              << " offset=" << e->addr.offset
                              << " length=" << e->addr.length
                              << std::endl;
                }

                // Need to get pointer from allocator for recovered nodes
                // Note: file_id=0 is valid (first file)
                ptr = ctx_.alloc.get_ptr_for_recovery(
                    e->class_id, e->addr.file_id, e->addr.segment_id,
                    e->addr.offset, e->addr.length);

                // Debug: trace recovery pointer failures
                if (!ptr) {
                    trace() << "[READ_FAIL] NodeID=" << id.raw()
                              << " class_id=" << static_cast<int>(e->class_id)
                              << " file_id=" << e->addr.file_id
                              << " segment_id=" << e->addr.segment_id
                              << " offset=" << e->addr.offset
                              << " length=" << e->addr.length
                              << std::endl;
                }
            }

            // Return mapped bytes
            return { ptr, e->addr.length };
        }
        
        StoreInterface::PinnedBytes DurableStore::read_node_pinned(NodeID id) const {
            bool is_uncommitted = false;
            const OTEntry* e = resolve_entry(id, is_uncommitted);
            if (!e) return {};

            // Check tx-local staging for uncommitted nodes
            if (is_uncommitted) {
                auto it = tl_batch_.pending_nodes.find(id.handle_index());
                if (it != tl_batch_.pending_nodes.end()) {
                    // For uncommitted nodes, we can't truly "pin" since they're in mmap'd segments
                    // but not yet committed. Return a pseudo-pinned reference to the staging buffer.
                    // Note: The Pin will be empty, but data/size are valid
                    return { MappingManager::Pin{}, const_cast<void*>(it->second.data), it->second.size };
                }
#ifndef NDEBUG
                // This can happen during read operations after inserts if the node
                // was published but not yet committed to ObjectTable
                trace() << "[WARN] Uncommitted node " << id.raw()
                          << " not in staging for pinned read - will fallback\n";
#endif
                return {};
            }

            // Committed node: create a proper pinned mapping
            // Resolve file + offset for pinned mapping
            uint32_t file_id = e->addr.segment_id >> 16;  // Top 16 bits are file_id
            uint32_t local_seg = e->addr.segment_id & 0xFFFF;  // Bottom 16 bits are local segment

            // Determine if this is a data file or index file based on NodeKind
            bool is_data_file = (e->kind == NodeKind::DataRecord || e->kind == NodeKind::ValueVec);

            // Get file path from segment allocator
            std::string filepath = ctx_.alloc.get_file_path(file_id, is_data_file);

            // Calculate byte offset in the file
            size_t offset = static_cast<size_t>(local_seg) * ctx_.alloc.get_segment_size() + e->addr.offset;

            try {
                auto pin = ctx_.alloc.get_mapping_manager().pin(filepath, offset, e->addr.length, /*writable=*/false);
                return { std::move(pin), pin.get(), e->addr.length };
            } catch (...) {
                // Optional: fallback to non-pinned read for uncommitted or unmapped
                return {};
            }
        }

        void DurableStore::retire_node(NodeID id,
                                       uint64_t retire_epoch_hint,
                                       RetireReason why,
                                       const char* file,
                                       int line) {
#ifndef NDEBUG
            // Log the retire call for debugging
            trace() << "[RETIRE_CALL]"
                      << " id=" << id.raw()
                      << " handle=" << id.handle_index()
                      << " tag=" << static_cast<int>(id.tag())
                      << " reason=" << retire_reason_str(why)
                      << " at " << (file ? file : "?") << ":" << line;

            // Get entry for delta info (and debug inspection)
            const auto& e = ctx_.ot.get(id);

            // Dump the OTEntry state inline for compact logging
            trace() << " | birth=" << e.birth_epoch.load(std::memory_order_relaxed)
                      << " retire=" << e.retire_epoch.load(std::memory_order_relaxed)
                      << " kind=" << static_cast<int>(e.kind)
                      << " tag=" << e.tag.load(std::memory_order_relaxed);

            // Include debug state if available
#ifndef NDEBUG
            const auto dbg_st = e.dbg_state.load(std::memory_order_relaxed);
            trace() << " dbg_state=" << ObjectTable::dbg_state_name(dbg_st) << std::endl;
#endif

            // Verify magic watermark
#ifndef NDEBUG
            assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted in retire_node!");
#endif

            // Check if attempting to retire a non-LIVE entry
            if (e.birth_epoch.load(std::memory_order_relaxed) == 0) {
                // Check if this node will be made live in this batch
                if (!tl_batch_.will_publish(id)) {
                    trace() << "    [ERROR] Attempting to retire RESERVED node NOT in pending writes!" << std::endl;
                    trace() << "    This is a bug - node won't be made live before retirement" << std::endl;
                    assert(false && "Retire called on uncommitted node not in same batch");
                    return;  // Don't stage this retirement
                } else {
                    trace() << "    [INFO] Retiring RESERVED node that will be made live in same commit" << std::endl;
                }
            } else if (dbg_st != OTEntry::DBG_LIVE) {
                trace() << "    [WARNING] Attempting to retire entry not in LIVE debug state!" << std::endl;
            }
#else
            // Get entry for delta info
            const auto& e = ctx_.ot.get(id);
#endif

            // Build retirement delta (epoch stamped at commit)
            OTDeltaRec delta = make_retire_delta(id, e);

            // DO NOT append to WAL here - just stage it

            // Track for batch operations (DO NOT mark_retired here)
            tl_batch_.retirements.emplace_back(delta);
            
            // Note: retire_epoch_hint is ignored - we always use commit epoch
            (void)retire_epoch_hint;
        }
        
        void DurableStore::free_node(NodeID id) {
            // DEPRECATED: Forward to instrumented version
            free_node_immediate(id, RetireReason::Unknown, nullptr, 0);
        }

        void DurableStore::free_node_immediate(NodeID id,
                                              RetireReason why,
                                              const char* file,
                                              int line) {
            // Fast guards
            if (!id.valid() || id.handle_index() == 0) {
                throw std::runtime_error("free_node_immediate: invalid/handle-0 NodeID");
            }

#ifndef NDEBUG
            trace() << "[FREE_IMMEDIATE]"
                      << " id=" << id.raw()
                      << " handle=" << id.handle_index()
                      << " tag=" << static_cast<int>(id.tag())
                      << " reason=" << retire_reason_str(why)
                      << " at " << (file ? file : "?") << ":" << line;
#endif

            // Resolve entry
            const uint64_t h = id.handle_index();
            const auto& e = ctx_.ot.get_by_handle_unsafe(h);

#ifndef NDEBUG
            assert(e.dbg_magic == OTEntry::DBG_MAGIC && "Magic corrupted in free_node_immediate");
            // Catch ABA/tag misuse in debug
            const uint16_t stored_tag = e.tag.load(std::memory_order_relaxed);
            if (stored_tag != id.tag()) {
                trace() << " | [ERROR] tag mismatch: stored=" << stored_tag
                          << " id.tag()=" << static_cast<int>(id.tag()) << std::endl;
                assert(false && "free_node_immediate tag mismatch (possible ABA)");
            }
#endif

            const uint64_t b = e.birth_epoch.load(std::memory_order_relaxed);
            const uint64_t r = e.retire_epoch.load(std::memory_order_relaxed);

#ifndef NDEBUG
            // Snapshot debug state in debug builds only
            const auto dbg_st = e.dbg_state.load(std::memory_order_relaxed);

            trace() << " | birth=" << b
                      << " retire=" << r
                      << " state=" << ObjectTable::dbg_state_name(dbg_st) << std::endl;

            // Enforce invariants in debug builds
            if (dbg_st == OTEntry::DBG_RETIRED || dbg_st == OTEntry::DBG_FREE) {
                throw std::runtime_error(
                    std::string("free_node_immediate on ") +
                    ObjectTable::dbg_state_name(dbg_st) +
                    " (reason=" + retire_reason_str(why) + ", at " +
                    (file ? file : "?") + ":" + std::to_string(line) + ")"
                );
            }

            if (dbg_st == OTEntry::DBG_LIVE &&
                !(why == RetireReason::Reallocation ||
                  why == RetireReason::AbortRollback ||
                  why == RetireReason::Evict ||
                  why == RetireReason::TreeDestroy)) {
                throw std::runtime_error(
                    std::string("Immediate free on LIVE requires allowed reason (got ") +
                    retire_reason_str(why) + ", at " +
                    (file ? file : "?") + ":" + std::to_string(line) + ")"
                );
            }

            // RESERVED path: cancel any staged write, abort reservation, then free captured segment
            if (b == 0 && dbg_st == OTEntry::DBG_RESERVED) {
#else
            // In release builds, use MVCC state for basic validation
            if (b > 0 && r == ~uint64_t{0}) {
                // This is a live entry, check if immediate free is allowed
                if (!(why == RetireReason::Reallocation ||
                      why == RetireReason::AbortRollback ||
                      why == RetireReason::Evict ||
                      why == RetireReason::TreeDestroy)) {
                    throw std::runtime_error(
                        std::string("Immediate free on LIVE requires allowed reason (got ") +
                        retire_reason_str(why) + ", at " +
                        (file ? file : "?") + ":" + std::to_string(line) + ")"
                    );
                }
            }

            // RESERVED path: handle new reservation
            if (b == 0) {
#endif
                if (!(why == RetireReason::AbortRollback || why == RetireReason::Reallocation)) {
                    throw std::runtime_error(
                        std::string("Invalid immediate free on RESERVED (reason=") +
                        retire_reason_str(why) + ", at " +
                        (file ? file : "?") + ":" + std::to_string(line) + ")"
                    );
                }

                // If this NodeID is in the current batch, remove it so we won't try to commit it
                const uint64_t raw = id.raw();
                bool canceled = tl_batch_.cancel_write_by_raw(raw);
#ifndef NDEBUG
                if (canceled) {
                    trace() << "[FREE_IMMEDIATE] Canceled staged write for NodeID " << raw << std::endl;
                }
#endif

                // Capture allocation BEFORE abort (abort clears the addr)
                SegmentAllocator::Allocation alloc{
                    e.addr.file_id, e.addr.segment_id, e.addr.offset, e.addr.length, e.class_id, {}
                };

#ifndef NDEBUG
                trace() << "[FREE_IMMEDIATE] Aborting reservation for NodeID " << id.raw()
                          << " (never committed)" << std::endl;
#endif
                bool ok = ctx_.ot.abort_reservation(id);
                if (!ok) {
#ifndef NDEBUG
                    trace() << "[FREE_ERROR] abort_reservation failed for NodeID "
                              << id.raw() << " handle=" << id.handle_index()
                              << " tag=" << static_cast<int>(id.tag()) << std::endl;
#endif
                    throw std::runtime_error("abort_reservation failed unexpectedly");
                }

                // Now that the reservation is safely aborted, free the segment space
                ctx_.alloc.free(alloc);
                return;
            }

            // LIVE path: free the segment allocation and retire immediately
            SegmentAllocator::Allocation alloc{
                e.addr.file_id, e.addr.segment_id, e.addr.offset, e.addr.length, e.class_id, {}
            };
            ctx_.alloc.free(alloc);

            // Mark the OT entry retired immediately (not at commit)
            ctx_.ot.retire(id, ctx_.mvcc.get_global_epoch());
        }

        NodeID DurableStore::get_root(std::string_view name) const {
            // Use the provided name or default to store name
            const std::string key = name.empty() ? name_ : std::string(name);
            
            // Delegate to runtime's catalog (single source of truth)
            return ctx_.runtime.get_root(key);
        }

        void DurableStore::set_root(NodeID id, uint64_t epoch, 
                                    const float* mbr, size_t mbr_size,
                                    std::string_view name) {
            // Use the provided name or default to store name
            const std::string key = name.empty() ? name_ : std::string(name);
            
            // Store the root ID in pending roots so we can update it with reserved ID at commit
            tl_batch_.pending_roots[key] = id;
            
            // Delegate to runtime's catalog (single source of truth)
            // No fsync here - durability comes when commit() publishes
            ctx_.runtime.set_root(key, id, epoch, mbr, mbr_size);
        }

        void DurableStore::commit(uint64_t hint_epoch) {
            // Guard: block commits in read-only mode
            if (ctx_.runtime.is_read_only()) {
                throw std::logic_error("Cannot commit in read-only mode (serverless reader)");
            }

            // Fast path: nothing to commit
            if (tl_batch_.writes.empty() && tl_batch_.retirements.empty()) {
                return;
            }
            
            // Get the single commit epoch for this batch
            const uint64_t commit_epoch = ctx_.mvcc.advance_epoch();
            
            // Stamp epochs in all staged deltas
            for (auto& w : tl_batch_.writes) {
                w.delta.birth_epoch = commit_epoch;
                w.delta.retire_epoch = ~uint64_t{0};
            }
            for (auto& r : tl_batch_.retirements) {
                if (r.retire_epoch == 0) {
                    r.retire_epoch = commit_epoch;
                }
            }
            
            // Dispatch to policy-specific flush
            switch (policy_.mode) {
                case DurabilityMode::STRICT:
                    flush_strict_mode(commit_epoch);
                    break;
                case DurabilityMode::BALANCED:
                    flush_balanced_mode(commit_epoch);
                    break;
                case DurabilityMode::EVENTUAL:
                    flush_eventual_mode(commit_epoch);
                    break;
            }
            
            // Clear staged buffers
            tl_batch_.clear();
            
            (void)hint_epoch; // Ignored - we use our own epoch
        }
        
        void DurableStore::flush_strict_mode(uint64_t epoch) {
            // Fast path: nothing to commit
            if (tl_batch_.writes.empty() && tl_batch_.retirements.empty() &&
                tl_batch_.dirty_ranges.empty() && tl_batch_.pending_roots.empty()) {
                return;
            }

            auto log = ctx_.coord.get_active_log();
            if (!log) {
                throw std::runtime_error("No active delta log during STRICT commit");
            }

            // 1) Reserve final NodeIDs and build O(1) lookup map
            std::vector<NodeID> reserved_ids;
            std::unordered_map<uint64_t, NodeID> reserved_by_raw;
            reserved_ids.reserve(tl_batch_.writes.size());
            reserved_by_raw.reserve(tl_batch_.writes.size());

            for (const auto& w : tl_batch_.writes) {
                NodeID reserved = ctx_.ot.mark_live_reserve(w.id, epoch);
                reserved_ids.push_back(reserved);
                auto [it, inserted] = reserved_by_raw.emplace(w.id.raw(), reserved);
#ifndef NDEBUG
                if (!inserted) {
                    trace() << "[BUG][STRICT] Duplicate NodeID after coalescing at epoch=" << epoch
                              << " raw=" << w.id.raw()
                              << " (handle=" << w.id.handle_index()
                              << " tag=" << static_cast<int>(w.id.tag()) << ")" << std::endl;
                    trace() << "  Previous reservation was NodeID " << it->second.raw()
                              << " (handle=" << it->second.handle_index()
                              << " tag=" << static_cast<int>(it->second.tag()) << ")" << std::endl;
                    trace() << "  This should not happen - stage_write should have coalesced duplicates" << std::endl;
                    trace() << "  Batch state: writes=" << tl_batch_.writes.size()
                              << " retirements=" << tl_batch_.retirements.size() << std::endl;
                }
                assert(inserted && "duplicate raw NodeID in writes batch after coalescing");
#endif
            }
            
            // 2) Build WAL batch with reserved tags
            std::vector<OTDeltaRec> wal_batch;
            wal_batch.reserve(tl_batch_.writes.size() + tl_batch_.retirements.size());
            for (size_t i = 0; i < tl_batch_.writes.size(); ++i) {
                const auto& w = tl_batch_.writes[i];
                OTDeltaRec delta = w.delta;
                delta.birth_epoch = epoch;
                delta.retire_epoch = ~uint64_t{0};
                delta.tag = reserved_ids[i].tag();  // Use reserved tag
                wal_batch.push_back(delta);
            }
            for (const auto& r : tl_batch_.retirements) {
                OTDeltaRec delta = r;
                delta.birth_epoch = r.birth_epoch;  // Keep original birth
                delta.retire_epoch = epoch;
                wal_batch.push_back(delta);
            }

#ifndef NDEBUG
            // Verify WAL batch contains all operations
            if (wal_batch.size() != tl_batch_.writes.size() + tl_batch_.retirements.size()) {
                trace() << "[ASSERT] WAL batch size mismatch: expected="
                          << (tl_batch_.writes.size() + tl_batch_.retirements.size())
                          << " got=" << wal_batch.size()
                          << " (writes=" << tl_batch_.writes.size()
                          << " retirements=" << tl_batch_.retirements.size() << ")";

                // Dump sample IDs for debugging
                trace() << " | first few write IDs: ";
                for (size_t i = 0; i < std::min<size_t>(3, tl_batch_.writes.size()); ++i) {
                    trace() << tl_batch_.writes[i].id.raw() << " ";
                }
                trace() << " | first few retire IDs: ";
                for (size_t i = 0; i < std::min<size_t>(3, tl_batch_.retirements.size()); ++i) {
                    trace() << NodeID::from_parts(tl_batch_.retirements[i].handle_idx,
                                                    tl_batch_.retirements[i].tag).raw() << " ";
                }
                trace() << std::endl;
                assert(false && "wal_batch size mismatch");
            }
#endif

            // 3) Flush dirty pages BEFORE WAL write (STRICT requirement)
            // This uses msync(MS_SYNC) to ensure data hits disk
            for (const auto& dr : tl_batch_.dirty_ranges) {
                PlatformFS::flush_view(dr.vaddr, dr.length);
            }
            
            // 4) Append to WAL and sync
            if (!wal_batch.empty()) {
                // Debug: Log WAL writes count and a sample
                trace() << "[WAL_COMMIT] epoch=" << epoch
                          << " writes=" << tl_batch_.writes.size()
                          << " retirements=" << tl_batch_.retirements.size()
                          << " total_deltas=" << wal_batch.size() << std::endl;

                // Log first few deltas and any with shard > 0
                for (size_t i = 0; i < wal_batch.size() && i < 5; ++i) {
                    const auto& d = wal_batch[i];
                    trace() << "[WAL_DELTA] #" << i
                              << " handle_idx=" << d.handle_idx
                              << " tag=" << static_cast<int>(d.tag)
                              << " birth=" << d.birth_epoch
                              << " retire=" << d.retire_epoch
                              << std::endl;
                }

                log->append(wal_batch);
            }
            log->sync();
            
            // 5) NOW commit OT state (after WAL is durable)
            // These MUST succeed - WAL is already visible
#ifndef NDEBUG
            assert(reserved_ids.size() == tl_batch_.writes.size() &&
                   "reserved_ids and writes size mismatch");
#endif

            for (size_t i = 0; i < reserved_ids.size(); ++i) {
                ctx_.ot.mark_live_commit(reserved_ids[i], epoch);
            }

#ifndef NDEBUG
            // Debug: Verify all published nodes are actually LIVE with correct tags
            for (size_t i = 0; i < reserved_ids.size(); ++i) {
                const auto& w = tl_batch_.writes[i];
                const auto& e_live = ctx_.ot.get(reserved_ids[i]);

                // The node we just committed must be LIVE now
                assert(e_live.dbg_state.load(std::memory_order_relaxed) == OTEntry::DBG_LIVE &&
                       "Post-commit invariant: published node not LIVE");

                // Birth epoch must be stamped
                assert(e_live.birth_epoch.load(std::memory_order_relaxed) == epoch &&
                       "Post-commit invariant: birth_epoch mismatch");

                // Tag must match the reserved tag
                assert(e_live.tag.load(std::memory_order_relaxed) == reserved_ids[i].tag() &&
                       "Post-commit invariant: reserved tag not committed");

                // Handle index must not have changed
                assert(reserved_ids[i].handle_index() == w.id.handle_index() &&
                       "Post-commit invariant: handle index changed across reservation");

                // Sanity: the original id we staged must be in writes index
                assert(tl_batch_.will_publish(w.id) && "writes[] entry not indexed");
            }

            // Only build committed set if we have retirements to check
            if (!tl_batch_.retirements.empty()) {
                std::unordered_set<uint64_t> committed_raw;
                committed_raw.reserve(reserved_ids.size());
                for (const auto& rid : reserved_ids) {
                    committed_raw.insert(rid.raw());
                }

                // Verify retirement preconditions
                for (const auto& r : tl_batch_.retirements) {
                    NodeID retire_id = NodeID::from_parts(r.handle_idx, r.tag);
                    const auto& e = ctx_.ot.get(retire_id);

                    if (e.birth_epoch.load(std::memory_order_relaxed) == 0) {
                        // Must be a same-batch publish
                        if (!tl_batch_.will_publish(retire_id)) {
                            trace() << "[COMMIT_ORDER_ERROR] Retiring RESERVED node "
                                      << retire_id.raw() << " not in writes batch!" << std::endl;
                            assert(false && "Commit ordering violation: retiring RESERVED node not in writes batch");
                        }
                    }

                    // Should not retire a just-committed NodeID (should retire previous version)
                    if (committed_raw.count(retire_id.raw())) {
                        trace() << "[COMMIT_ORDER_ERROR] Retiring just-committed NodeID "
                                  << retire_id.raw() << " (should retire previous version)" << std::endl;
                        assert(false && "Retiring a just-committed NodeID");
                    }
                }
            }
#endif

            for (const auto& r : tl_batch_.retirements) {
                ctx_.ot.retire(NodeID::from_parts(r.handle_idx, r.tag), epoch);
            }

#ifndef NDEBUG
            // Verify all retired nodes are actually in RETIRED state with correct epoch
            for (const auto& r : tl_batch_.retirements) {
                NodeID rid = NodeID::from_parts(r.handle_idx, r.tag);
                const auto& e_after = ctx_.ot.get(rid);
                assert(e_after.dbg_state.load(std::memory_order_relaxed) == OTEntry::DBG_RETIRED &&
                       "Post-retire invariant: node not in RETIRED state");
                assert(e_after.retire_epoch.load(std::memory_order_relaxed) == epoch &&
                       "Post-retire invariant: retire_epoch mismatch");
            }
#endif
            
            // 6) Update catalog with reserved IDs for any roots in this batch
            for (const auto& [name, original_id] : tl_batch_.pending_roots) {
                if (auto it = reserved_by_raw.find(original_id.raw()); it != reserved_by_raw.end()) {
                    ctx_.runtime.set_root(name, it->second, epoch);
                }
            }
            
            // 7) Persist catalog if dirty (before superblock publish)
            if (ctx_.runtime.is_catalog_dirty()) {
                ctx_.runtime.persist_catalog_to_manifest(epoch);
            }
            
            // 8) Publish primary root + epoch to superblock
            NodeID root_id = ctx_.runtime.get_root("");
            if (auto it = reserved_by_raw.find(root_id.raw()); it != reserved_by_raw.end()) {
                root_id = it->second;
            }
            if (root_id.valid()) {
                ctx_.coord.try_publish(root_id, epoch);
            }
            
            // 9) Clear pending roots after successful commit
            tl_batch_.pending_roots.clear();
        }
        
        void DurableStore::flush_eventual_mode(uint64_t epoch) {
            // Fast path: nothing to commit
            if (tl_batch_.writes.empty() && tl_batch_.retirements.empty() &&
                tl_batch_.dirty_ranges.empty() && tl_batch_.pending_roots.empty()) {
                return;
            }

            auto log = ctx_.coord.get_active_log();
            if (!log) {
                throw std::runtime_error("No active delta log during EVENTUAL commit");
            }

            // 1) Reserve final NodeIDs and build O(1) lookup map
            std::vector<NodeID> reserved_ids;
            std::unordered_map<uint64_t, NodeID> reserved_by_raw;
            reserved_ids.reserve(tl_batch_.writes.size());
            reserved_by_raw.reserve(tl_batch_.writes.size());

            for (const auto& w : tl_batch_.writes) {
                NodeID reserved = ctx_.ot.mark_live_reserve(w.id, epoch);
                reserved_ids.push_back(reserved);
                auto [it, inserted] = reserved_by_raw.emplace(w.id.raw(), reserved);
#ifndef NDEBUG
                if (!inserted) {
                    trace() << "[BUG][EVENTUAL] Duplicate NodeID after coalescing at epoch=" << epoch
                              << " raw=" << w.id.raw()
                              << " (handle=" << w.id.handle_index()
                              << " tag=" << static_cast<int>(w.id.tag()) << ")" << std::endl;
                    trace() << "  Previous reservation was NodeID " << it->second.raw()
                              << " (handle=" << it->second.handle_index()
                              << " tag=" << static_cast<int>(it->second.tag()) << ")" << std::endl;
                    trace() << "  This should not happen - stage_write should have coalesced duplicates" << std::endl;
                    trace() << "  Batch state: writes=" << tl_batch_.writes.size()
                              << " retirements=" << tl_batch_.retirements.size() << std::endl;
                }
                assert(inserted && "duplicate raw NodeID in writes batch after coalescing");
#endif
            }
            
            // 2) Build WAL batch with reserved tags and payloads for small nodes
            std::vector<OTDeltaLog::DeltaWithPayload> dwp;
            dwp.reserve(tl_batch_.writes.size() + tl_batch_.retirements.size());
            
            for (size_t i = 0; i < tl_batch_.writes.size(); ++i) {
                const auto& w = tl_batch_.writes[i];
                OTDeltaRec delta = w.delta;
                delta.birth_epoch = epoch;
                delta.retire_epoch = ~uint64_t{0};
                delta.tag = reserved_ids[i].tag();  // Use reserved tag
                if (w.include_payload && w.dst_vaddr) {
                    // Small node with payload
                    dwp.push_back({delta, w.dst_vaddr, w.len});
                } else {
                    // Large node - metadata only
                    dwp.push_back({delta, nullptr, 0});
                }
            }
            for (const auto& r : tl_batch_.retirements) {
                OTDeltaRec delta = r;
                delta.birth_epoch = r.birth_epoch;
                delta.retire_epoch = epoch;
                dwp.push_back({delta, nullptr, 0});
            }

#ifndef NDEBUG
            // Verify WAL batch contains all operations
            if (dwp.size() != tl_batch_.writes.size() + tl_batch_.retirements.size()) {
                trace() << "[ASSERT] WAL batch size mismatch in EVENTUAL: expected="
                          << (tl_batch_.writes.size() + tl_batch_.retirements.size())
                          << " got=" << dwp.size()
                          << " (writes=" << tl_batch_.writes.size()
                          << " retirements=" << tl_batch_.retirements.size() << ")" << std::endl;
                assert(false && "dwp size mismatch");
            }
#endif

            // 3) Append to WAL
            if (!dwp.empty()) {
                log->append_with_payloads(dwp);
            }

            // 4) Optional sync for EVENTUAL
            if (policy_.group_commit_interval_ms == 0 && policy_.sync_on_commit) {
                log->sync();
            }
            // Dirty ranges are best-effort in EVENTUAL
            
            // 5) NOW commit OT state (after WAL is written)
            for (size_t i = 0; i < reserved_ids.size(); ++i) {
                ctx_.ot.mark_live_commit(reserved_ids[i], epoch);
            }

#ifndef NDEBUG
            // Verify basic state transitions (lighter than STRICT mode)
            for (size_t i = 0; i < reserved_ids.size(); ++i) {
                const auto& e_live = ctx_.ot.get(reserved_ids[i]);
                assert(e_live.dbg_state.load(std::memory_order_relaxed) == OTEntry::DBG_LIVE &&
                       "Post-commit invariant: published node not LIVE");
            }

            // Verify retirement ordering (same as STRICT - these are logical invariants)
            if (!tl_batch_.retirements.empty()) {
                std::unordered_set<uint64_t> committed_raw;
                committed_raw.reserve(reserved_ids.size());
                for (const auto& rid : reserved_ids) {
                    committed_raw.insert(rid.raw());
                }

                for (const auto& r : tl_batch_.retirements) {
                    NodeID retire_id = NodeID::from_parts(r.handle_idx, r.tag);
                    const auto& e = ctx_.ot.get(retire_id);

                    if (e.birth_epoch.load(std::memory_order_relaxed) == 0) {
                        // Must be a same-batch publish
                        if (!tl_batch_.will_publish(retire_id)) {
                            trace() << "[COMMIT_ORDER_ERROR][EVENTUAL] Retiring RESERVED node "
                                      << retire_id.raw() << " not in writes batch!" << std::endl;
                            assert(false && "Commit ordering violation in EVENTUAL mode");
                        }
                    }

                    // Should not retire a just-committed NodeID
                    if (committed_raw.count(retire_id.raw())) {
                        trace() << "[COMMIT_ORDER_ERROR][EVENTUAL] Retiring just-committed NodeID "
                                  << retire_id.raw() << std::endl;
                        assert(false && "Retiring a just-committed NodeID in EVENTUAL");
                    }
                }
            }
#endif

            for (const auto& r : tl_batch_.retirements) {
                ctx_.ot.retire(NodeID::from_parts(r.handle_idx, r.tag), epoch);
            }

#ifndef NDEBUG
            // Verify retirements succeeded
            for (const auto& r : tl_batch_.retirements) {
                NodeID rid = NodeID::from_parts(r.handle_idx, r.tag);
                const auto& e_after = ctx_.ot.get(rid);
                assert(e_after.dbg_state.load(std::memory_order_relaxed) == OTEntry::DBG_RETIRED &&
                       "Post-retire invariant: node not in RETIRED state");
            }
#endif
            
            // 6) Update catalog with reserved IDs for any roots in this batch
            for (const auto& [name, original_id] : tl_batch_.pending_roots) {
                if (auto it = reserved_by_raw.find(original_id.raw()); it != reserved_by_raw.end()) {
                    ctx_.runtime.set_root(name, it->second, epoch);
                }
            }
            
            // 7) Persist catalog if dirty (before superblock publish)
            if (ctx_.runtime.is_catalog_dirty()) {
                ctx_.runtime.persist_catalog_to_manifest(epoch);
            }
            
            // 8) Publish primary root + epoch to superblock
            NodeID root_id = ctx_.runtime.get_root("");
            if (auto it = reserved_by_raw.find(root_id.raw()); it != reserved_by_raw.end()) {
                root_id = it->second;
            }
            if (root_id.valid()) {
                ctx_.coord.try_publish(root_id, epoch);
            }
            
            // 9) Clear pending roots after successful commit
            tl_batch_.pending_roots.clear();
        }
        
        void DurableStore::flush_balanced_mode(uint64_t epoch) {
            // Fast path: nothing to commit
            if (tl_batch_.writes.empty() && tl_batch_.retirements.empty() &&
                tl_batch_.dirty_ranges.empty() && tl_batch_.pending_roots.empty()) {
                return;
            }

            auto log = ctx_.coord.get_active_log();
            if (!log) {
                throw std::runtime_error("No active delta log during BALANCED commit");
            }

            // 1) Reserve final NodeIDs and build O(1) lookup map
            std::vector<NodeID> reserved_ids;
            std::unordered_map<uint64_t, NodeID> reserved_by_raw;
            reserved_ids.reserve(tl_batch_.writes.size());
            reserved_by_raw.reserve(tl_batch_.writes.size());

            for (const auto& w : tl_batch_.writes) {
                NodeID reserved = ctx_.ot.mark_live_reserve(w.id, epoch);
                reserved_ids.push_back(reserved);
                auto [it, inserted] = reserved_by_raw.emplace(w.id.raw(), reserved);
#ifndef NDEBUG
                if (!inserted) {
                    trace() << "[BUG][BALANCED] Duplicate NodeID after coalescing at epoch=" << epoch
                              << " raw=" << w.id.raw()
                              << " (handle=" << w.id.handle_index()
                              << " tag=" << static_cast<int>(w.id.tag()) << ")" << std::endl;
                    trace() << "  Previous reservation was NodeID " << it->second.raw()
                              << " (handle=" << it->second.handle_index()
                              << " tag=" << static_cast<int>(it->second.tag()) << ")" << std::endl;
                    trace() << "  This should not happen - stage_write should have coalesced duplicates" << std::endl;
                    trace() << "  Batch state: writes=" << tl_batch_.writes.size()
                              << " retirements=" << tl_batch_.retirements.size() << std::endl;
                }
                assert(inserted && "duplicate raw NodeID in writes batch after coalescing");
#endif
            }
            
            // 2) Build WAL batch with reserved tags and payloads for small nodes
            std::vector<OTDeltaLog::DeltaWithPayload> dwp;
            dwp.reserve(tl_batch_.writes.size() + tl_batch_.retirements.size());
            
            for (size_t i = 0; i < tl_batch_.writes.size(); ++i) {
                const auto& w = tl_batch_.writes[i];
                OTDeltaRec delta = w.delta;
                delta.birth_epoch = epoch;
                delta.retire_epoch = ~uint64_t{0};
                delta.tag = reserved_ids[i].tag();  // Use reserved tag
                if (w.include_payload && w.dst_vaddr) {
                    // Small node with payload in WAL
                    dwp.push_back({delta, w.dst_vaddr, w.len});
                } else {
                    // Large node - metadata only
                    dwp.push_back({delta, nullptr, 0});
                }
            }
            for (const auto& r : tl_batch_.retirements) {
                OTDeltaRec delta = r;
                delta.birth_epoch = r.birth_epoch;
                delta.retire_epoch = epoch;
                dwp.push_back({delta, nullptr, 0});
            }

#ifndef NDEBUG
            // Verify WAL batch contains all operations
            if (dwp.size() != tl_batch_.writes.size() + tl_batch_.retirements.size()) {
                trace() << "[ASSERT] WAL batch size mismatch in BALANCED: expected="
                          << (tl_batch_.writes.size() + tl_batch_.retirements.size())
                          << " got=" << dwp.size()
                          << " (writes=" << tl_batch_.writes.size()
                          << " retirements=" << tl_batch_.retirements.size() << ")" << std::endl;
                assert(false && "dwp size mismatch in BALANCED");
            }
#endif

            // 3) Append to WAL
            if (!dwp.empty()) {
                // Debug: Log WAL writes count and sample
                trace() << "[WAL_COMMIT_BALANCED] epoch=" << epoch
                          << " writes=" << tl_batch_.writes.size()
                          << " retirements=" << tl_batch_.retirements.size()
                          << " total_deltas=" << dwp.size() << std::endl;

                // Log first few deltas
                for (size_t i = 0; i < dwp.size() && i < 5; ++i) {
                    const auto& d = dwp[i].delta;
                    trace() << "[WAL_DELTA_B] #" << i
                              << " handle_idx=" << d.handle_idx
                              << " tag=" << static_cast<int>(d.tag)
                              << " birth=" << d.birth_epoch
                              << " retire=" << d.retire_epoch
                              << std::endl;
                }

                // Check for shard 10+ handles in the batch (debug high-shard issue)
                for (size_t i = 0; i < dwp.size(); ++i) {
                    uint32_t shard = (dwp[i].delta.handle_idx >> 42) & 0x3F;
                    if (shard >= 9) {
                        trace() << "[WAL_HIGH_SHARD] epoch=" << epoch
                                  << " idx=" << i
                                  << " shard=" << shard
                                  << " handle_idx=" << dwp[i].delta.handle_idx
                                  << " tag=" << static_cast<int>(dwp[i].delta.tag)
                                  << " birth=" << dwp[i].delta.birth_epoch
                                  << " payload_size=" << dwp[i].payload_size
                                  << std::endl;
                    }
                }

                log->append_with_payloads(dwp);
            }

            // 4) Handle sync and dirty ranges
            if (policy_.group_commit_interval_ms == 0 || policy_.sync_on_commit || epoch == 0) {
                // Sync immediately if: no group commit, explicit sync_on_commit, or final commit
                // Final commit (epoch 0) must always sync to ensure durability at close
                log->sync();
            }
            // Otherwise let group commit handle WAL sync
            
            // CRITICAL FIX: Always flush dirty ranges in BALANCED mode
            // This ensures updated MBRs are persisted to disk
            // Without this, tree nodes with updated MBRs remain only in memory
            for (const auto& dr : tl_batch_.dirty_ranges) {
                PlatformFS::flush_view(dr.vaddr, dr.length);
            }
            
            // 5) NOW commit OT state (after WAL is written/synced)
#ifndef NDEBUG
            assert(reserved_ids.size() == tl_batch_.writes.size() &&
                   "reserved_ids and writes size mismatch (BALANCED)");
#endif

            for (size_t i = 0; i < reserved_ids.size(); ++i) {
                ctx_.ot.mark_live_commit(reserved_ids[i], epoch);
            }

#ifndef NDEBUG
            // Verify basic state transitions (lighter than STRICT, but with epoch checks)
            for (size_t i = 0; i < reserved_ids.size(); ++i) {
                const auto& e_live = ctx_.ot.get(reserved_ids[i]);
                assert(e_live.dbg_state.load(std::memory_order_relaxed) == OTEntry::DBG_LIVE &&
                       "Post-commit invariant: published node not LIVE");
                // For nodes that were already LIVE (double commit), birth_epoch may differ
                // Only assert for nodes freshly committed in this epoch
                uint64_t actual_epoch = e_live.birth_epoch.load(std::memory_order_relaxed);
                // Skip epoch check if node was already live from a previous epoch
                // (actual_epoch > 0 && actual_epoch != epoch is valid for double commits)
            }

            // Verify retirement ordering (same logical invariants as other modes)
            if (!tl_batch_.retirements.empty()) {
                std::unordered_set<uint64_t> committed_raw;
                committed_raw.reserve(reserved_ids.size());
                for (const auto& rid : reserved_ids) {
                    committed_raw.insert(rid.raw());
                }

                for (const auto& r : tl_batch_.retirements) {
                    NodeID retire_id = NodeID::from_parts(r.handle_idx, r.tag);
                    const auto& e = ctx_.ot.get(retire_id);

                    if (e.birth_epoch.load(std::memory_order_relaxed) == 0) {
                        // Must be a same-batch publish
                        if (!tl_batch_.will_publish(retire_id)) {
                            trace() << "[COMMIT_ORDER_ERROR][BALANCED] Retiring RESERVED node "
                                      << retire_id.raw() << " not in writes batch!" << std::endl;
                            assert(false && "Commit ordering violation in BALANCED mode");
                        }
                    }

                    // Should not retire a just-committed NodeID
                    if (committed_raw.count(retire_id.raw())) {
                        trace() << "[COMMIT_ORDER_ERROR][BALANCED] Retiring just-committed NodeID "
                                  << retire_id.raw() << std::endl;
                        assert(false && "Retiring a just-committed NodeID in BALANCED");
                    }
                }
            }
#endif

            for (const auto& r : tl_batch_.retirements) {
                ctx_.ot.retire(NodeID::from_parts(r.handle_idx, r.tag), epoch);
            }

#ifndef NDEBUG
            // Verify retirements succeeded with correct epoch
            for (const auto& r : tl_batch_.retirements) {
                NodeID rid = NodeID::from_parts(r.handle_idx, r.tag);
                const auto& e_after = ctx_.ot.get(rid);
                assert(e_after.dbg_state.load(std::memory_order_relaxed) == OTEntry::DBG_RETIRED &&
                       "Post-retire invariant: node not in RETIRED state");
                // BALANCED can assert epoch since it syncs
                assert(e_after.retire_epoch.load(std::memory_order_relaxed) == epoch &&
                       "Post-retire invariant: retire_epoch mismatch in BALANCED");
            }
#endif
            
            // 6) Update catalog with reserved IDs for any roots in this batch
            for (const auto& [name, original_id] : tl_batch_.pending_roots) {
                if (auto it = reserved_by_raw.find(original_id.raw()); it != reserved_by_raw.end()) {
                    ctx_.runtime.set_root(name, it->second, epoch);
                }
            }
            
            // 7) Persist catalog if dirty (before superblock publish)
            if (ctx_.runtime.is_catalog_dirty()) {
                ctx_.runtime.persist_catalog_to_manifest(epoch);
            }
            
            // 8) Publish primary root + epoch to superblock
            NodeID root_id = ctx_.runtime.get_root("");
            if (auto it = reserved_by_raw.find(root_id.raw()); it != reserved_by_raw.end()) {
                root_id = it->second;
            }
            if (root_id.valid()) {
                ctx_.coord.try_publish(root_id, epoch);
            }
            
            // 9) Clear pending roots after successful commit
            tl_batch_.pending_roots.clear();
        }
        
        void* DurableStore::get_mapped_address(NodeID id) {
            // Get the OT entry for this node
            const uint64_t h = id.handle_index();
            const auto& e = ctx_.ot.get_by_handle_unsafe(h);
            
            // Check if the node is valid
            if (e.addr.length == 0) {
                return nullptr;  // Node not allocated
            }
            
            // Return the cached vaddr from OT entry
            // This was populated during allocation
            return e.addr.vaddr;
        }
        
        size_t DurableStore::get_capacity(NodeID id) {
            // Get the OT entry for this node
            const uint64_t h = id.handle_index();
            const auto& e = ctx_.ot.get_by_handle_unsafe(h);

            // Return the allocated capacity from OT entry
            return e.addr.length;
        }

        /**
         * Resolve the logical kind of a node.
         * Uses resolve_entry() to apply uncommitted/committed visibility rules.
         * @return true if the node exists and is visible, false otherwise.
         */
        bool DurableStore::get_node_kind(NodeID id, NodeKind& out_kind) const {
            bool is_uncommitted = false;
            const OTEntry* e = resolve_entry(id, is_uncommitted);
            if (!e) {
                // Special logging for NodeID 65537 to track its disappearance
                if (id.raw() == 65537) {
                    xtree::trace() << "[OT_TRACE] NodeID 65537 NOT FOUND in get_node_kind"
                            << " [handle=" << id.handle_index() << ", tag=" << id.tag() << "]";
                }
#ifndef NDEBUG
                debug() << "get_node_kind(" << id.raw() << ") - resolve_entry returned nullptr"
                        << " [handle=" << id.handle_index() << ", tag=" << id.tag() << "]";
#endif
                return false;
            }

            // Only report kind for LIVE entries (not RESERVED/uncommitted)
            if (is_uncommitted) {
#ifndef NDEBUG
                debug() << "get_node_kind(" << id.raw() << ") - entry is uncommitted (RESERVED), returning false";
#endif
                return false;
            }

            // Special logging for NodeID 65537 to track its lifecycle
            if (id.raw() == 65537) {
                xtree::trace() << "[OT_TRACE] NodeID 65537 FOUND in get_node_kind"
                        << " kind=" << static_cast<int>(e->kind)
                        << " birth=" << e->birth_epoch
                        << " retire=" << e->retire_epoch;
            }

            out_kind = e->kind;
            return true;
        }

        bool DurableStore::is_node_present(NodeID id) const {
            // Wrapper that calls the overloaded version with nullptr
            return is_node_present(id, nullptr);
        }

        bool DurableStore::is_node_present(NodeID id, bool* out_is_staged) const {
            // Check if node exists in RESERVED or LIVE state
            // This is more permissive than get_node_kind which only accepts LIVE

            // Use resolve_entry which handles both uncommitted and committed entries
            bool is_uncommitted = false;
            const OTEntry* e = resolve_entry(id, is_uncommitted);

            if (out_is_staged) *out_is_staged = is_uncommitted;

            if (!e && !is_uncommitted) {
                return false;  // Invalid handle or out of range
            }

            if (is_uncommitted) {
                if (id.raw() == 65537) {
                    xtree::trace() << "[OT_TRACE] is_node_present(" << id.raw() << "): present=1 (staged)";
                }
                return true;  // staged entry is "present enough" for splitRoot checks
            }

            // committed path: it's safe to read the entry
#ifndef NDEBUG
            // Check the debug state to determine if present (only load once)
            const auto state = e->dbg_state.load(std::memory_order_relaxed);
            const bool present = (state == OTEntry::DBG_RESERVED ||
                                 state == OTEntry::DBG_LIVE);
#else
            // In release builds, use MVCC state
            const uint64_t birth = e->birth_epoch.load(std::memory_order_relaxed);
            const uint64_t retire = e->retire_epoch.load(std::memory_order_relaxed);
            const bool present = (birth > 0 || retire != ~uint64_t{0});
#endif

#ifndef NDEBUG
            if (id.raw() == 65537) {  // Special tracking for our test case
                const auto b = e->birth_epoch.load(std::memory_order_relaxed);
                const auto r = e->retire_epoch.load(std::memory_order_relaxed);
                xtree::trace() << "[OT_TRACE] is_node_present(" << id.raw() << "): "
                        << "dbg_state=" << static_cast<int>(state)
                        << " present=" << present
                        << " is_uncommitted=0"
                        << " birth=" << b
                        << " retire=" << r;
            }
#endif

            return present;
        }

        SegmentAllocator::SegmentUtilization DurableStore::get_segment_utilization() const {
            return ctx_.alloc.get_segment_utilization();
        }

    } // namespace persistence
} // namespace xtree