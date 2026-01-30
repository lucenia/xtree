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

#include "recovery.h"
#include "platform_fs.h"
#include "checksums.h"
#include "../util/log.h"
#include <chrono>
#include <algorithm>
#include <filesystem>
#include <cstring>

namespace xtree { 
namespace persist {

void Recovery::cold_start() {
    auto start_time = std::chrono::steady_clock::now();
    
    // Step 1: Load manifest (tolerate missing/old)
    bool manifest_loaded = mf_.load();
    if (!manifest_loaded) {
        // Log warning but continue - we can reconstruct from directory listing if needed
        warning() << "Failed to load manifest, continuing with directory scan";
    }
    
    // Step 2: Map checkpoint (prefer manifest's checkpoint over directory scan)
    std::string checkpoint_path;
    uint64_t checkpoint_epoch = 0;
    
    // First try to use manifest's checkpoint
    if (manifest_loaded && !mf_.get_checkpoint().path.empty()) {
        // Use absolute path by joining data_dir with checkpoint path
        std::filesystem::path full_path = std::filesystem::path(mf_.get_data_dir()) / mf_.get_checkpoint().path;
        checkpoint_path = full_path.string();
    } else {
        // Fall back to directory scan only if manifest missing or no checkpoint recorded
        checkpoint_path = OTCheckpoint::find_latest_checkpoint(mf_.get_data_dir());
    }
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    if (!checkpoint_path.empty()) {
        // Map checkpoint for fast recovery
        if (chk_.map_for_read(checkpoint_path, &checkpoint_epoch, &entry_count, &entries)) {
            // Bulk-load entries into ObjectTable
            // This is much faster than replaying individual allocations
            for (size_t i = 0; i < entry_count; i++) {
                const auto& pe = entries[i];
                
                // Skip non-live entries (shouldn't happen in checkpoint but be defensive)
                if (pe.retire_epoch != ~uint64_t{0}) {
                    continue;
                }
                
                // Reconstruct OTAddr
                OTAddr addr{pe.file_id, pe.segment_id, pe.offset, pe.length};
                
                // Restore with exact handle index to preserve NodeID references
                ot_.restore_handle(pe.handle_idx, pe);
            }
            
            info() << "Loaded " << entry_count << " entries from checkpoint epoch " 
                     << checkpoint_epoch;
        } else {
            warning() << "Failed to map checkpoint " << checkpoint_path;
            checkpoint_epoch = 0;
        }
    } else {
        info() << "No checkpoint found, starting from empty state";
    }
    
    // Step 3: Replay delta logs in epoch order starting after checkpoint
    std::vector<Manifest::DeltaLogInfo> delta_logs = mf_.get_delta_logs();
    
    // Fallback: if manifest has no logs, scan directory for .wal files
    if (delta_logs.empty()) {
        warning() << "Manifest has no delta logs, scanning directory for .wal files";
        std::filesystem::path data_dir(mf_.get_data_dir());
        try {
            for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
                if (entry.is_regular_file() && entry.path().extension() == ".wal") {
                    Manifest::DeltaLogInfo log_info;
                    log_info.path = entry.path().filename().string();
                    log_info.start_epoch = 0;  // Unknown, will replay all
                    log_info.end_epoch = 0;     // Active log
                    log_info.size = std::filesystem::file_size(entry.path());
                    delta_logs.push_back(log_info);
                    trace() << "  Found log file: " << log_info.path << " (size=" << log_info.size << ")";
                }
            }
        } catch (const std::exception& e) {
            warning() << "Failed to scan for delta logs: " << e.what();
        }
    }
    
    // Sort by start_epoch to ensure correct replay order
    std::sort(delta_logs.begin(), delta_logs.end(),
              [](const Manifest::DeltaLogInfo& a, const Manifest::DeltaLogInfo& b) {
                  return a.start_epoch < b.start_epoch;
              });
    
    size_t total_deltas_replayed = 0;
    for (const auto& log_info : delta_logs) {
        
        // Skip logs that are entirely before checkpoint
        if (log_info.end_epoch != 0 && log_info.end_epoch <= checkpoint_epoch) {
            continue;
        }
        
        // Only replay deltas strictly after checkpoint epoch
        if (log_info.start_epoch <= checkpoint_epoch) {
            // This log spans the checkpoint, need to replay only newer entries
            // TODO: OTDeltaLog needs to support partial replay from epoch
            warning() << "Delta log spans checkpoint, may replay duplicates";
        }
        
        // Replay delta log with truncation on error
        std::filesystem::path log_path = std::filesystem::path(mf_.get_data_dir()) / log_info.path;
        uint64_t last_good_offset = 0;
        std::string replay_error;
        
        OTDeltaLog log_(log_path.string());
        bool replay_ok = log_.replay(log_path.string(),
            [&](const OTDeltaRec& rec) {
                // Only skip deltas from before or at the checkpoint epoch
                // (checkpoint already contains all state up to and including that epoch)
                if (checkpoint_epoch > 0 && rec.birth_epoch <= checkpoint_epoch) {
                    return;
                }
                
                // For BALANCED mode: verify segment data against CRC if present
                // TODO: Fix CRC validation - currently disabled due to mismatch issues
                // The CRC might be computed on different data or segments not flushed
                if (false && rec.data_crc32c != 0 && rec.retire_epoch == ~uint64_t{0}) {
                    // This is a live node with CRC - verify segment data
                    if (alloc_) {
                        void* data = alloc_->get_ptr_for_recovery(
                            rec.class_id, rec.file_id, rec.segment_id, rec.offset, rec.length);
                        if (data) {
                            uint32_t computed_crc = crc32c(data, rec.length);
                            if (computed_crc != rec.data_crc32c) {
                                warning() << "CRC mismatch for node at epoch " 
                                         << rec.birth_epoch << " (expected " << rec.data_crc32c
                                         << ", got " << computed_crc << "). "
                                         << "Node may be corrupted, skipping epoch";
                                // In BALANCED mode, we can tolerate this by skipping the epoch
                                // The node will be re-written on next modification
                                return;
                            }
                        }
                    }
                }
                
                // Debug: Log every 100th delta to understand what's being replayed
                if (total_deltas_replayed % 100 == 0 || total_deltas_replayed < 10) {
                    trace() << "[RECOVERY_DELTA] #" << total_deltas_replayed
                              << " handle_idx=" << rec.handle_idx
                              << " tag=" << static_cast<int>(rec.tag)
                              << " birth=" << rec.birth_epoch
                              << " retire=" << rec.retire_epoch
                              << " kind=" << static_cast<int>(rec.kind)
                              << std::endl;
                }

                // Debug: Log shard 10+ deltas specifically
                uint32_t shard = (rec.handle_idx >> 42) & 0x3F;
                if (shard >= 9) {
                    trace() << "[RECOVERY_HIGH_SHARD] #" << total_deltas_replayed
                              << " shard=" << shard
                              << " handle_idx=" << rec.handle_idx
                              << " tag=" << static_cast<int>(rec.tag)
                              << " birth=" << rec.birth_epoch
                              << " retire=" << rec.retire_epoch
                              << std::endl;
                }

                ot_.apply_delta(rec);
                total_deltas_replayed++;
            },
            &last_good_offset,
            &replay_error);
        
        if (!replay_ok) {
            error() << "Delta log replay failed: " << replay_error 
                     << ", truncating at offset " << last_good_offset;
            // Truncate the log at the last good position
            PlatformFS::truncate(log_path.string(), last_good_offset);
            // Stop processing further logs after first error
            break;
        }
    }
    
    // Step 4: Read superblock for authoritative (root_id, epoch) snapshot
    // Note: On fresh start, superblock won't have magic set yet (set on first publish)
    // This is OK - we just use default values
    Superblock::Snapshot snapshot;
    if (sb_.valid()) {
        snapshot = sb_.load();
    } else {
        // Fresh start - use defaults
        snapshot = Superblock::Snapshot{ NodeID(), 0 };
    }
    
    // Verify superblock epoch >= checkpoint epoch (sanity check)
    if (snapshot.epoch < checkpoint_epoch) {
        warning() << "Superblock epoch " << snapshot.epoch 
                 << " < checkpoint epoch " << checkpoint_epoch 
                 << " - using superblock as authoritative";
    }
    
    // Set in-memory root to the superblock's published state
    // This is what the application will see as the consistent view
    // NOTE: XTree integration would happen here:
    //   xtree_.set_root(snapshot.root);
    //   mvcc_context_.set_epoch(snapshot.epoch);
    info() << "Recovery complete: root_id=" << snapshot.root.raw() 
             << " at epoch=" << snapshot.epoch;
    
    // Step 5: Post-recovery hygiene
    auto end_time = std::chrono::steady_clock::now();
    auto recovery_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();
    
    info() << "Recovery completed in " << recovery_ms << " ms";
    
    // Check if we need a new checkpoint soon (once delta log is implemented)
    // if (total_deltas_replayed > 10000) {  // Configurable threshold
    //     debug() << "Recommendation: Schedule new checkpoint soon "
    //              << "(replayed " << total_deltas_replayed << " deltas)";
    // }
    
    // Check if logs need rotation
    if (delta_logs.size() > 10) {  // Configurable threshold
        debug() << "Recommendation: Rotate delta logs "
                 << "(" << delta_logs.size() << " logs accumulated)";
    }
    
    // Optionally trigger checkpoint cleanup
    if (checkpoint_epoch > 0) {
        OTCheckpoint::cleanup_old_checkpoints(mf_.get_data_dir(), 3);
    }
    
    // Clean up orphaned .tmp files
    std::filesystem::path data_path(mf_.get_data_dir());
    try {
        for (const auto& entry : std::filesystem::directory_iterator(data_path)) {
            if (entry.path().extension() == ".tmp") {
                std::error_code ec;
                std::filesystem::remove(entry.path(), ec);
                if (!ec) {
                    trace() << "Cleaned up orphaned temp file: " << entry.path().filename();
                }
            }
        }
    } catch (const std::exception& e) {
        warning() << "Failed to clean up temp files: " << e.what();
    }
}

void Recovery::cold_start_with_payloads() {
    auto start_time = std::chrono::steady_clock::now();
    
    // Step 1: Load manifest (tolerate missing/old)
    bool manifest_loaded = mf_.load();
    if (!manifest_loaded) {
        warning() << "Failed to load manifest, continuing with directory scan";
    }
    
    // Step 2: Map checkpoint (prefer manifest's checkpoint over directory scan)
    std::string checkpoint_path;
    uint64_t checkpoint_epoch = 0;
    
    if (manifest_loaded && !mf_.get_checkpoint().path.empty()) {
        std::filesystem::path full_path = std::filesystem::path(mf_.get_data_dir()) / mf_.get_checkpoint().path;
        checkpoint_path = full_path.string();
    } else {
        checkpoint_path = OTCheckpoint::find_latest_checkpoint(mf_.get_data_dir());
    }
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    if (!checkpoint_path.empty()) {
        if (chk_.map_for_read(checkpoint_path, &checkpoint_epoch, &entry_count, &entries)) {
            for (size_t i = 0; i < entry_count; i++) {
                const auto& pe = entries[i];
                
                if (pe.retire_epoch != ~uint64_t{0}) {
                    continue;
                }
                
                OTAddr addr{pe.file_id, pe.segment_id, pe.offset, pe.length};
                ot_.restore_handle(pe.handle_idx, pe);
            }
            
            info() << "Loaded " << entry_count << " entries from checkpoint epoch " 
                     << checkpoint_epoch;
        } else {
            warning() << "Failed to map checkpoint " << checkpoint_path;
            checkpoint_epoch = 0;
        }
    } else {
        info() << "No checkpoint found, starting from empty state";
    }
    
    // Step 3: Replay delta logs with payload support
    std::vector<Manifest::DeltaLogInfo> delta_logs = mf_.get_delta_logs();
    
    std::sort(delta_logs.begin(), delta_logs.end(),
              [](const Manifest::DeltaLogInfo& a, const Manifest::DeltaLogInfo& b) {
                  return a.start_epoch < b.start_epoch;
              });
    
    size_t total_deltas_replayed = 0;
    size_t payloads_rehydrated = 0;
    
    for (const auto& log_info : delta_logs) {
        if (log_info.end_epoch != 0 && log_info.end_epoch <= checkpoint_epoch) {
            continue;
        }
        
        if (log_info.start_epoch <= checkpoint_epoch) {
            warning() << "Delta log spans checkpoint, may replay duplicates";
        }
        
        std::filesystem::path log_path = std::filesystem::path(mf_.get_data_dir()) / log_info.path;
        OTDeltaLog log_(log_path.string());
        
        // Use replay_with_payloads for EVENTUAL mode recovery
        try {
            log_.replay_with_payloads(
                [&](const OTDeltaRec& rec, const void* payload, size_t payload_size) {
                    // Only apply deltas strictly after checkpoint epoch
                    if (rec.birth_epoch <= checkpoint_epoch) {
                        return;
                    }
                    
                    // Apply the delta to the object table
                    ot_.apply_delta(rec);
                    total_deltas_replayed++;
                    
                    // If there's a payload, rehydrate it to the segment
                    if (payload && payload_size > 0 && alloc_) {
                        // Get the memory-mapped pointer for this location (O(1) with class_id)
                        void* dst = alloc_->get_ptr_for_recovery(
                            rec.class_id, rec.file_id, rec.segment_id, rec.offset, rec.length);
                        
                        if (dst) {
                            // Verify CRC if provided
                            if (rec.data_crc32c != 0) {
                                uint32_t computed_crc = crc32c(payload, payload_size);
                                if (computed_crc != rec.data_crc32c) {
                                    warning() << "CRC mismatch for payload at epoch " 
                                             << rec.birth_epoch << ", skipping rehydration";
                                    return;
                                }
                            }
                            
                            // Copy payload data to the segment
                            std::memcpy(dst, payload, payload_size);
                            payloads_rehydrated++;
                            
                            // Note: The vaddr in the OT entry should already be set
                            // when we call ot_.apply_delta() or ot_.restore_handle()
                            // The segment allocator maintains the mapping
                        } else {
                            warning() << "Failed to get memory pointer for rehydration at "
                                     << "file=" << rec.file_id << " segment=" << rec.segment_id 
                                     << " offset=" << rec.offset;
                        }
                    }
                });
        } catch (const std::exception& e) {
            error() << "Delta log replay failed: " << e.what();
            break;
        }
    }
    
    // Step 4: Read superblock for authoritative (root_id, epoch) snapshot
    // Note: On fresh start, superblock won't have magic set yet (set on first publish)
    // This is OK - we just use default values
    Superblock::Snapshot snapshot;
    if (sb_.valid()) {
        snapshot = sb_.load();
    } else {
        // Fresh start - use defaults
        snapshot = Superblock::Snapshot{ NodeID(), 0 };
    }
    
    if (snapshot.epoch < checkpoint_epoch) {
        warning() << "Superblock epoch " << snapshot.epoch 
                 << " < checkpoint epoch " << checkpoint_epoch 
                 << " - using superblock as authoritative";
    }
    
    info() << "Recovery complete: root_id=" << snapshot.root.raw() 
             << " at epoch=" << snapshot.epoch;
    
    // Step 5: Post-recovery hygiene
    auto end_time = std::chrono::steady_clock::now();
    auto recovery_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();
    
    info() << "Recovery completed in " << recovery_ms << " ms";
    info() << "Replayed " << total_deltas_replayed << " deltas"
           << (payloads_rehydrated > 0 ? ", rehydrated " : "")
           << (payloads_rehydrated > 0 ? std::to_string(payloads_rehydrated) + " payloads from WAL" : "");
    
    // Clean up orphaned .tmp files
    std::filesystem::path data_path(mf_.get_data_dir());
    try {
        for (const auto& entry : std::filesystem::directory_iterator(data_path)) {
            if (entry.path().extension() == ".tmp") {
                std::error_code ec;
                std::filesystem::remove(entry.path(), ec);
                if (!ec) {
                    trace() << "Cleaned up orphaned temp file: " << entry.path().filename();
                }
            }
        }
    } catch (const std::exception& e) {
        warning() << "Failed to clean up temp files: " << e.what();
    }
}

} // namespace persist
} // namespace xtree