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

#include "ot_log_gc.h"
#include "platform_fs.h"
#include <filesystem>
#include <chrono>
#include <algorithm>

namespace xtree { 
    namespace persist {

        OTLogGC::OTLogGC(Manifest& manifest, MVCCContext& mvcc, const Config& config)
            : manifest_(manifest), mvcc_(mvcc), config_(config), 
              data_dir_(manifest.get_data_dir()) {
        }
        
        bool OTLogGC::check_rotation_needed(uint64_t current_epoch, uint64_t checkpoint_epoch) {
            // Check if checkpoint is needed based on interval
            if (current_epoch - checkpoint_epoch >= config_.checkpoint_interval) {
                return true;
            }
            
            // Check current log size
            auto logs = manifest_.get_delta_logs();
            if (!logs.empty()) {
                const auto& current_log = logs.back();
                size_t log_size = get_log_size(current_log.path);
                if (log_size >= config_.max_log_size) {
                    return true;
                }
                
                // Check log age
                size_t age_sec = get_log_age_sec(current_log.path);
                if (age_sec >= config_.max_log_age_sec) {
                    return true;
                }
            }
            
            return false;
        }
        
        size_t OTLogGC::truncate_logs_before_checkpoint(uint64_t checkpoint_epoch) {
            size_t deleted_count = 0;
            
            // Get logs that are entirely before checkpoint
            auto logs = manifest_.get_delta_logs();
            std::vector<Manifest::DeltaLogInfo> logs_to_keep;
            
            std::string log_dir;
            for (const auto& log : logs) {
                // Never delete active log (end_epoch == 0)
                // Only delete if log ends before or at checkpoint
                if (log.end_epoch != 0 && log.end_epoch <= checkpoint_epoch) {
                    // Build full path: data_dir + "/" + relative_path
                    // This is fine since we're not in hot path (only after checkpoint)
                    std::filesystem::path full_path = std::filesystem::path(data_dir_) / log.path;
                    
                    // Delete the log file
                    std::error_code ec;
                    std::filesystem::remove(full_path, ec);
                    if (!ec) {
                        deleted_count++;
                        // Remember directory for fsync
                        if (log_dir.empty()) {
                            log_dir = full_path.parent_path().string();
                        }
                    }
                } else {
                    // Keep logs that overlap or are after checkpoint
                    logs_to_keep.push_back(log);
                }
            }
            
            // Fsync directory after deletions (best effort)
            if (deleted_count > 0 && !log_dir.empty()) {
                PlatformFS::fsync_directory(log_dir);
            }
            
            // Update manifest with remaining logs
            if (deleted_count > 0) {
                manifest_.set_delta_logs(logs_to_keep);
                manifest_.store();
            }
            
            return deleted_count;
        }
        
        std::string OTLogGC::rotate_log(const std::string& current_log_path, uint64_t new_epoch) {
            // Generate new log filename with epoch
            std::filesystem::path current_path(current_log_path);
            std::filesystem::path dir = current_path.parent_path();
            
            // Create new filename: delta_<epoch>.wal
            std::string new_filename = "delta_" + std::to_string(new_epoch) + ".wal";
            std::filesystem::path new_path = dir / new_filename;
            
            // Update manifest to mark current log as ended
            auto logs = manifest_.get_delta_logs();
            if (!logs.empty() && logs.back().path == current_log_path) {
                logs.back().end_epoch = new_epoch - 1;
                logs.back().size = get_log_size(current_log_path);
            }
            
            // Add new log to manifest
            Manifest::DeltaLogInfo new_log;
            new_log.path = new_path.string();
            new_log.start_epoch = new_epoch;
            new_log.end_epoch = 0; // Still active
            new_log.size = 0;
            logs.push_back(new_log);
            
            manifest_.set_delta_logs(logs);
            manifest_.store();
            
            return new_path.string();
        }
        
        size_t OTLogGC::cleanup_old_logs(uint64_t min_active_epoch) {
            size_t deleted_count = 0;
            
            // Get minimum epoch we must keep
            uint64_t safe_epoch = min_active_epoch;
            
            // Keep at least min_logs_to_keep
            auto logs = manifest_.get_delta_logs();
            if (logs.size() <= config_.min_logs_to_keep) {
                return 0; // Don't delete if we're at minimum
            }
            
            // Sort logs by start epoch
            std::sort(logs.begin(), logs.end(), 
                [](const auto& a, const auto& b) { 
                    return a.start_epoch < b.start_epoch; 
                });
            
            // Keep the newest min_logs_to_keep logs regardless of epoch
            size_t logs_to_delete = logs.size() - config_.min_logs_to_keep;
            std::vector<Manifest::DeltaLogInfo> remaining_logs;
            std::string log_dir;
            
            for (size_t i = 0; i < logs.size(); ++i) {
                const auto& log = logs[i];
                
                // Delete if:
                // 1. We haven't reached min_logs_to_keep yet AND
                // 2. The log is entirely before safe_epoch
                if (i < logs_to_delete && 
                    log.end_epoch != 0 && log.end_epoch < safe_epoch) {
                    std::filesystem::path full_path = std::filesystem::path(data_dir_) / log.path;
                    std::error_code ec;
                    std::filesystem::remove(full_path, ec);
                    if (!ec) {
                        deleted_count++;
                        // Remember directory for fsync
                        if (log_dir.empty()) {
                            log_dir = full_path.parent_path().string();
                        }
                    }
                } else {
                    remaining_logs.push_back(log);
                }
            }
            
            // Fsync directory after deletions (best effort)
            if (deleted_count > 0 && !log_dir.empty()) {
                PlatformFS::fsync_directory(log_dir);
            }
            
            // Update manifest if we deleted anything
            if (deleted_count > 0) {
                manifest_.set_delta_logs(remaining_logs);
                manifest_.store();
            }
            
            return deleted_count;
        }
        
        size_t OTLogGC::get_log_size(const std::string& path) {
            std::error_code ec;
            auto size = std::filesystem::file_size(path, ec);
            return ec ? 0 : size;
        }
        
        size_t OTLogGC::get_log_age_sec(const std::string& path) {
            std::error_code ec;
            auto ftime = std::filesystem::last_write_time(path, ec);
            if (ec) return 0;
            
            auto now = std::filesystem::file_time_type::clock::now();
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - ftime);
            return age.count();
        }

    } // namespace persist
} // namespace xtree