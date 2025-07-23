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
#include "checkpoint_coordinator.h"
#include "store_interface.h"
#include "object_table_sharded.hpp"
#include "segment_allocator.h"
#include "durability_policy.h"
#include "ot_delta_log.h"
#include <unordered_map>
#include <vector>
#include <cstring>

namespace xtree {
    namespace persist {

        // Forward declaration
        class DurableRuntime;

        struct DurableContext {
            ObjectTableSharded&   ot;
            SegmentAllocator&     alloc;
            CheckpointCoordinator& coord;
            MVCCContext&          mvcc;
            DurableRuntime&       runtime;  // Added runtime reference for root management
        };

        class DurableStore final : public StoreInterface {
        public:
            DurableStore(DurableContext& ctx, std::string name, 
                        DurabilityPolicy policy = DurabilityPolicy{});
            ~DurableStore();

            AllocResult allocate_node(size_t min_len, NodeKind kind) override;

            void publish_node(NodeID id, const void* data, size_t len) override;
            
            // DurableStore supports in-place publishing
            bool supports_in_place_publish() const override { return true; }
            
            // No-copy publish: the payload is already in the mapped destination.
            // Builds the OT delta, schedules WAL and flush bookkeeping, but does not memcpy.
            void publish_node_in_place(NodeID id, size_t len) override;
            
            NodeBytes read_node(NodeID id) const override;
            
            // Pinned read for zero-copy access
            PinnedBytes read_node_pinned(NodeID id) const override;
            
            void retire_node(NodeID id,
                           uint64_t retire_epoch,
                           RetireReason why = RetireReason::Unknown,
                           const char* file = nullptr,
                           int line = 0) override;
            void free_node(NodeID id) override;
            void free_node_immediate(NodeID id,
                                   RetireReason why = RetireReason::Reallocation,
                                   const char* file = nullptr,
                                   int line = 0) override;

            NodeID  get_root(std::string_view name) const override;
            void    set_root(NodeID id, uint64_t epoch, 
                           const float* mbr, size_t mbr_size,
                           std::string_view name) override;

            void commit(uint64_t epoch) override;
            
            // Zero-copy access for in-place updates
            void* get_mapped_address(NodeID id) override;
            size_t get_capacity(NodeID id) override;

            // Metadata lookup for determining node type
            bool get_node_kind(NodeID id, NodeKind& out_kind) const override;

            // Check if node is present (RESERVED or LIVE)
            bool is_node_present(NodeID id) const override;

            // Check if node is present with out param for staged status
            bool is_node_present(NodeID id, bool* out_is_staged) const override;

            // Get segment utilization statistics (wrapper for internal SegmentAllocator)
            SegmentAllocator::SegmentUtilization get_segment_utilization() const;

        private:
            // Internal helper: resolve OTEntry for a NodeID, handling uncommitted visibility
            const OTEntry* resolve_entry(NodeID id, bool& is_uncommitted) const noexcept;

            // Thread-local write batching
            struct PendingWrite {
                NodeID id;              // NodeID with tag from allocation
                uint32_t len;           // Actual payload size written
                OTDeltaRec delta;       // Delta without epochs (filled at commit)
                void* dst_vaddr;        // Destination pointer for WAL payload reads
                bool include_payload;   // True if payload should be in WAL
            };
            
            struct DirtyRange {
                void* vaddr;         // Direct pointer for fast flush
                uint32_t length;     // Length to flush
            };
            
            struct ThreadBatch {
                std::vector<PendingWrite> writes;
                std::vector<OTDeltaRec> retirements;
                std::vector<DirtyRange> dirty_ranges;  // For coalesced flushing
                std::unordered_map<std::string, NodeID> pending_roots;  // Track roots to update with reserved IDs
                // Tx-local staging for uncommitted nodes (writer visibility)
                std::unordered_map<uint64_t, NodeBytes> pending_nodes;  // handle -> {ptr, len}

                // Index to coalesce multiple publishes per NodeID in the same batch
                std::unordered_map<uint64_t, size_t> write_index_by_raw;

#ifndef NDEBUG
                // Debug-only index for O(1) "will publish" checks
                std::unordered_set<uint64_t> writes_raw_index;

                bool will_publish(NodeID id) const {
                    return writes_raw_index.count(id.raw()) != 0;
                }

                void index_write(NodeID id) {
                    writes_raw_index.insert(id.raw());
                }

                void clear_debug() {
                    writes_raw_index.clear();
                }
#endif

                // Atomic write staging with coalescing
                void stage_write(PendingWrite&& w) {
                    const uint64_t raw = w.id.raw();
                    auto it = write_index_by_raw.find(raw);
                    if (it == write_index_by_raw.end()) {
                        // First write for this NodeID
                        write_index_by_raw.emplace(raw, writes.size());
#ifndef NDEBUG
                        index_write(w.id);
#endif
                        writes.push_back(std::move(w));
                    } else {
                        // Coalesce: last write wins
                        auto& prev = writes[it->second];

                        // Keep the most recent payload pointer/size
                        prev.len = w.len;
                        prev.dst_vaddr = w.dst_vaddr;
                        prev.include_payload = w.include_payload;

                        // Merge delta: keep the most recent delta fields (but preserve id/handle)
                        prev.delta = w.delta;

#ifndef NDEBUG
                        trace() << "[DUP_PUBLISH_COALESCE] raw=" << raw
                                  << " handle=" << w.id.handle_index()
                                  << " tag=" << static_cast<int>(w.id.tag())
                                  << " replaced_idx=" << it->second << std::endl;
#endif
                    }
                }

                // NEW: remove a staged write if present (by exact raw NodeID)
                bool cancel_write_by_raw(uint64_t raw) {
                    auto it = write_index_by_raw.find(raw);
                    if (it == write_index_by_raw.end()) return false;

                    size_t idx = it->second;
#ifndef NDEBUG
                    // For debug O(1) check
                    writes_raw_index.erase(raw);
#endif
                    // Swap-erase from writes
                    size_t last = writes.size() - 1;
                    if (idx != last) {
                        // Move last into idx
                        uint64_t moved_raw = writes[last].id.raw();
                        writes[idx] = std::move(writes[last]);
                        write_index_by_raw[moved_raw] = idx;
                    }
                    writes.pop_back();
                    write_index_by_raw.erase(it);
                    return true;
                }

                void clear() {
                    writes.clear();
                    retirements.clear();
                    dirty_ranges.clear();
                    pending_roots.clear();
                    pending_nodes.clear();
                    write_index_by_raw.clear();
#ifndef NDEBUG
                    clear_debug();
#endif
                }
            };

            static thread_local ThreadBatch tl_batch_;
            
            // Helper methods
            void flush_strict_mode(uint64_t epoch);
            void flush_eventual_mode(uint64_t epoch);
            void flush_balanced_mode(uint64_t epoch);

            // Helper to stringify RetireReason (available in all builds for error messages)
            static const char* retire_reason_str(RetireReason why) {
                switch(why) {
                    case RetireReason::Unknown:       return "Unknown";
                    case RetireReason::SplitReplace:  return "SplitReplace";
                    case RetireReason::MergeDelete:   return "MergeDelete";
                    case RetireReason::Evict:         return "Evict";
                    case RetireReason::AbortRollback: return "AbortRollback";
                    case RetireReason::Reallocation:  return "Reallocation";
                    case RetireReason::TreeDestroy:   return "TreeDestroy";
                    default:                          return "INVALID";
                }
            }

        private:
            DurableContext& ctx_;
            std::string name_;
            DurabilityPolicy policy_;
        };
    } // namespace persist
} // namespace xtree