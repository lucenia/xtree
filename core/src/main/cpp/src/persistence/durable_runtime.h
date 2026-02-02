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
#include <memory>
#include <string>
#include <unordered_map>
#include <atomic>
#include "store_interface.h"
#include "mvcc_context.h"
#include "object_table.hpp"
#include "object_table_sharded.hpp"
#include "segment_allocator.h"
#include "ot_delta_log.h"
#include "ot_log_gc.h"
#include "ot_checkpoint.h"
#include "manifest.h"
#include "superblock.hpp"
#include "checkpoint_coordinator.h"
#include "recovery.h"

namespace xtree {
    namespace persist {

        struct Paths {
            std::string data_dir;       // segment files root
            std::string manifest;       // manifest.json
            std::string superblock;     // superblock.bin (A/B optional)
            std::string active_log;     // current delta log path
        };

        class DurableRuntime {
        public:
            // Open a durable runtime
            // read_only: If true, skip WAL replay and open in checkpoint-only mode
            //            for fast serverless startup. Writes are blocked.
            // field_name: Optional field name for per-index memory tracking
            static std::unique_ptr<DurableRuntime> open(const Paths& paths,
                                                        const CheckpointPolicy& policy,
                                                        bool use_payload_recovery = false,
                                                        bool read_only = false,
                                                        const std::string& field_name = "");

            ~DurableRuntime(); // stops coordinator

            // Check if this runtime is in read-only mode
            bool is_read_only() const { return read_only_; }

            // Accessors used by DurableStore
            inline MVCCContext&          mvcc()         { return *mvcc_; }
            inline ObjectTableSharded&   ot()           { return *ot_sharded_; }
            inline SegmentAllocator&     allocator()    { return *alloc_; }
            inline CheckpointCoordinator& coordinator() { return *coordinator_; }

            // Root catalog helpers (stored via superblock/manifest policy you chose)
            NodeID  get_root(std::string_view name) const;
            void    set_root(std::string_view name, NodeID id, uint64_t epoch,
                           const float* mbr = nullptr, size_t mbr_size = 0);
            
            // Catalog persistence for multi-field support
            void persist_catalog_to_manifest(uint64_t epoch);
            void load_catalog_from_manifest();
            bool is_catalog_dirty() const { return catalog_dirty_.load(std::memory_order_acquire); }

        private:
            DurableRuntime(Paths, CheckpointPolicy, bool read_only = false,
                          const std::string& field_name = "");
            void start();
            void stop();

        private:
            Paths paths_;
            CheckpointPolicy policy_;
            bool read_only_ = false;  // True = checkpoint-only mode, no WAL replay
            std::string field_name_;  // Field name for per-index memory tracking

            std::unique_ptr<Manifest>           manifest_;
            std::unique_ptr<MVCCContext>        mvcc_;
            std::unique_ptr<ObjectTableSharded> ot_sharded_;
            std::unique_ptr<SegmentAllocator>   alloc_;
            std::unique_ptr<Superblock>         superblock_;
            std::shared_ptr<OTDeltaLog> active_log_;
            std::unique_ptr<OTLogGC>            log_gc_;
            std::unique_ptr<Reclaimer>          reclaimer_;
            std::unique_ptr<CheckpointCoordinator> coordinator_;

            // In-memory catalog with change tracking
            mutable std::mutex catalog_mu_;
            std::unordered_map<std::string, NodeID> catalog_;
            std::unordered_map<std::string, std::vector<float>> catalog_mbrs_;  // Root MBRs
            std::atomic<bool> catalog_dirty_{false};
            std::atomic<uint64_t> catalog_epoch_{0};
        };

    } // namespace persist
} // namespace xtree
