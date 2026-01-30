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

#include "durable_runtime.h"
#include "recovery.h"
#include "ot_checkpoint.h"
#include "node_id.hpp"

#include <stdexcept>
#include <iostream>
#include <utility>
#include <cassert>
#include <algorithm>
#include <filesystem>

namespace xtree { 
    namespace persist {

        std::unique_ptr<DurableRuntime>
        DurableRuntime::open(const Paths& paths, const CheckpointPolicy& policy, bool use_payload_recovery) {
        auto rt = std::unique_ptr<DurableRuntime>(new DurableRuntime(paths, policy));

        // Recovery: build OT from latest checkpoint + replay deltas
        // Create recovery helper
        OTCheckpoint checkpoint(paths.data_dir);
        OTDeltaLog recovery_log(paths.active_log);
        Recovery recovery(*rt->superblock_, *rt->ot_sharded_, recovery_log, checkpoint, 
                         *rt->manifest_, rt->alloc_.get());
        
        // Use enhanced recovery if EVENTUAL mode is enabled
        if (use_payload_recovery) {
            recovery.cold_start_with_payloads();
        } else {
            recovery.cold_start();
        }

        // Load catalog from manifest first (if available)
        rt->load_catalog_from_manifest();
        
        // Then load primary root from superblock (authoritative for "" root)
        auto snapshot = rt->superblock_->load();
        if (snapshot.root.valid()) {
            std::lock_guard<std::mutex> lk(rt->catalog_mu_);
            rt->catalog_[""] = snapshot.root;
            // Mark not dirty since this is recovered state
            rt->catalog_dirty_.store(false, std::memory_order_release);
        }
        
        // Restore MVCC epoch to the recovered epoch
        // Use O(1) recovery setter - safe because no threads are running yet
        if (snapshot.epoch > 0) {
            rt->mvcc_->recover_set_epoch(snapshot.epoch);
        }

        rt->start();
        return rt;
        }

        DurableRuntime::DurableRuntime(Paths p, CheckpointPolicy pol)
        : paths_(std::move(p)), policy_(pol) {
            manifest_   = std::make_unique<Manifest>(paths_.data_dir);
            mvcc_       = std::make_unique<MVCCContext>();
            // Use sharded ObjectTable for concurrent allocation
            // Starts with 1 active shard, grows as needed
            ot_sharded_ = std::make_unique<ObjectTableSharded>(100000, ObjectTableSharded::DEFAULT_NUM_SHARDS);
            alloc_      = std::make_unique<SegmentAllocator>(paths_.data_dir);
            superblock_ = std::make_unique<Superblock>(paths_.superblock);
            // Initialize active log to nullptr - coordinator will create/adopt the initial log
            active_log_ = nullptr;
            
            // Load existing manifest if present
            manifest_->load();
            
            log_gc_     = std::make_unique<OTLogGC>(*manifest_, *mvcc_);
            reclaimer_  = std::make_unique<Reclaimer>(*ot_sharded_, *mvcc_);
            coordinator_= std::make_unique<CheckpointCoordinator>(
                                *ot_sharded_, *superblock_, *manifest_, 
                                active_log_,
                                *log_gc_, *mvcc_, policy_, reclaimer_.get());
        }

        DurableRuntime::~DurableRuntime() { 
            stop(); 
            
            // Seal the current log in the manifest before closing
            if (manifest_) {
                auto log = std::atomic_load(&active_log_);
                if (log) {
                    // Get the current epoch as the end epoch
                    uint64_t end_epoch = mvcc_ ? mvcc_->get_global_epoch() : 0;
                    
                    // Find and update the log entry in the manifest
                    auto logs = manifest_->get_delta_logs();
                    for (auto& log_info : logs) {
                        if (log_info.path == log->path() && log_info.end_epoch == 0) {
                            // This is our active log - seal it
                            manifest_->close_delta_log(log_info.path, end_epoch, log->get_end_offset());
                            manifest_->store();
                            break;
                        }
                    }
                }
            }
            
            // Note: The coordinator owns and closes the active log in stop()
            // We just need to reset our pointer to it
            std::atomic_store(&active_log_, std::shared_ptr<OTDeltaLog>{});
            
            // Close all allocator segments and mappings
            if (alloc_) {
                alloc_->close_all();
            }
            
            // Close superblock mapping
            if (superblock_) {
                // Superblock destructor will handle unmapping
                superblock_.reset();
            }
        }

        void DurableRuntime::start() {
            coordinator_->start();
        }

        void DurableRuntime::stop() {
            if (coordinator_) coordinator_->stop();
            // Note: We don't close the log here - that's done in destructor
        }

        NodeID DurableRuntime::get_root(std::string_view name) const {
            std::lock_guard<std::mutex> lk(catalog_mu_);
            auto it = catalog_.find(std::string(name));
            if (it != catalog_.end()) return it->second;
            
            // For empty name (default tree), fallback to superblock's root
            if (name.empty() && superblock_) {
                auto snapshot = superblock_->load();
                return snapshot.root;
            }
            
            return NodeID{}; // Return invalid NodeID if not found
        }

        void DurableRuntime::set_root(std::string_view name, NodeID id, uint64_t epoch,
                                      const float* mbr, size_t mbr_size) {
            {
                std::lock_guard<std::mutex> lk(catalog_mu_);
                catalog_[std::string(name)] = id;
                
                // Store MBR if provided
                if (mbr && mbr_size > 0) {
                    catalog_mbrs_[std::string(name)] = std::vector<float>(mbr, mbr + mbr_size);
                }
                
                catalog_dirty_.store(true, std::memory_order_release);
                
                // Also set the primary root if this is the first/only tree
                // This ensures single-tree systems work correctly
                if (catalog_.size() == 1 || name.empty()) {
                    catalog_[""] = id;
                    if (mbr && mbr_size > 0) {
                        catalog_mbrs_[""] = std::vector<float>(mbr, mbr + mbr_size);
                    }
                }
            }
            
            // Only publish to superblock if this is the primary root
            // Named roots are persisted via manifest catalog
            if (name.empty()) {
                coordinator_->try_publish(id, epoch); // leader will sync log + publish superblock
            }
        }
        
        void DurableRuntime::persist_catalog_to_manifest(uint64_t epoch) {
            std::vector<Manifest::RootEntry> entries;
            {
                std::lock_guard<std::mutex> lk(catalog_mu_);
                entries.reserve(catalog_.size());
                for (const auto& [name, id] : catalog_) {
                    Manifest::RootEntry entry{name, id.raw(), epoch};
                    
                    // Add MBR if we have it
                    auto mbr_it = catalog_mbrs_.find(name);
                    if (mbr_it != catalog_mbrs_.end()) {
                        entry.mbr = mbr_it->second;
                    }
                    
                    entries.push_back(entry);
                }
            }
            
            // Update manifest with root catalog
            manifest_->set_roots(entries);
            if (manifest_->store()) {
                catalog_epoch_.store(epoch, std::memory_order_release);
                catalog_dirty_.store(false, std::memory_order_release);
            } else {
                // Log error but continue - superblock still has primary root
            }
        }
        
        void DurableRuntime::load_catalog_from_manifest() {
            // Try to load existing manifest
            if (!manifest_->load()) {
                return; // No manifest yet or failed to load
            }
            
            const auto& roots = manifest_->get_roots();
            if (roots.empty()) {
                return; // No catalog in manifest
            }
            
            // Populate catalog from manifest
            std::lock_guard<std::mutex> lk(catalog_mu_);
            catalog_.clear();
            catalog_mbrs_.clear();
            
            uint64_t max_epoch = 0;
            for (const auto& entry : roots) {
                catalog_[entry.name] = NodeID::from_raw(entry.node_id_raw);
                if (!entry.mbr.empty()) {
                    catalog_mbrs_[entry.name] = entry.mbr;
                }
                max_epoch = std::max(max_epoch, entry.epoch);
            }
            
            catalog_epoch_.store(max_epoch, std::memory_order_release);
            catalog_dirty_.store(false, std::memory_order_release);
        }

    } // namespace persist
} // namespace xtree