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
#include <string>
#include <vector>
#include <cstdint>
#include "manifest.h"
#include "mvcc_context.h"

namespace xtree { 
    namespace persist {

        /**
         * Manages lifecycle of delta logs - truncation, rotation, cleanup
         */
        class OTLogGC {
        public:
            struct Config {
                size_t max_log_size;         // Max size before rotation
                size_t max_log_age_sec;      // Max age before rotation
                size_t min_logs_to_keep;     // Keep at least N logs
                size_t checkpoint_interval;  // Checkpoint every N commits
                
                Config() 
                    : max_log_size(100 * 1024 * 1024)  // 100MB
                    , max_log_age_sec(3600)            // 1 hour
                    , min_logs_to_keep(2)              // Keep at least 2
                    , checkpoint_interval(10000)       // Every 10k commits
                {}
            };
            
            OTLogGC(Manifest& manifest, MVCCContext& mvcc, const Config& config = Config());
            
            /**
             * Check if logs need rotation or truncation
             * @param current_epoch Current commit epoch
             * @param checkpoint_epoch Last checkpoint epoch
             * @return true if checkpoint is recommended
             */
            bool check_rotation_needed(uint64_t current_epoch, uint64_t checkpoint_epoch);
            
            /**
             * Truncate logs that are entirely before the checkpoint
             * @param checkpoint_epoch Epoch of last checkpoint
             * @return Number of log files deleted
             */
            size_t truncate_logs_before_checkpoint(uint64_t checkpoint_epoch);
            
            /**
             * Rotate current log to a new file
             * @param current_log_path Path to current log
             * @param new_epoch Starting epoch for new log
             * @return Path to new log file
             */
            std::string rotate_log(const std::string& current_log_path, uint64_t new_epoch);
            
            /**
             * Clean up old log files based on policy
             * @param min_active_epoch Minimum active reader epoch
             * @return Number of files deleted
             */
            size_t cleanup_old_logs(uint64_t min_active_epoch);
            
        private:
            Manifest& manifest_;
            MVCCContext& mvcc_;
            Config config_;
            std::string data_dir_;  // Cached from manifest to avoid repeated string ops
            
            /**
             * Get size of a log file
             */
            size_t get_log_size(const std::string& path);
            
            /**
             * Get age of a log file in seconds
             */
            size_t get_log_age_sec(const std::string& path);
        };

    } // namespace persist
} // namespace xtree