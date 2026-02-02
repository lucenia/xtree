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
#include <cstdlib>
#include <string>
#include "config.h"  // For defaults

namespace xtree {
namespace persist {

/**
 * Runtime configuration for storage layer
 * Can be customized per-index instead of compile-time constants
 */
struct StorageConfig {
    // File organization
    size_t max_file_size       = files::kMaxFileSize;      // Default 1GB
    size_t mmap_window_size    = 128ULL << 20;             // Default 128MB (reduced for better granularity)
    size_t target_file_size    = files::kTargetFileSize;   // Default 256MB

    // Memory budget for mmap
    size_t max_mmap_memory     = 4ULL << 30;               // Default 4GB (0 = unlimited)
    float mmap_eviction_headroom = 0.1f;                   // 10% hysteresis

    // Checkpoint policy
    size_t checkpoint_keep_count = 2;                      // Keep N checkpoints (reduced for space)

    // Segment allocation
    size_t segment_alignment   = segment::kSegmentAlignment; // Default 4KB

    // File handle limits
    size_t max_open_files      = 256;                      // Max FDs to use

    // Global registry usage
    bool use_global_registries = true;                     // Use global MappingManager/FileHandleRegistry
    
    /**
     * Create config with defaults, optionally reading from environment
     */
    static StorageConfig defaults() {
        StorageConfig cfg;

        // Check environment variables for overrides
        if (const char* env = std::getenv("XTREE_MAX_FILE_SIZE")) {
            cfg.max_file_size = std::stoull(env);
        }

        if (const char* env = std::getenv("XTREE_MMAP_WINDOW_SIZE")) {
            cfg.mmap_window_size = parseMemorySize(env);
        }

        if (const char* env = std::getenv("XTREE_MMAP_BUDGET")) {
            cfg.max_mmap_memory = parseMemorySize(env);
        }

        if (const char* env = std::getenv("XTREE_MMAP_HEADROOM")) {
            cfg.mmap_eviction_headroom = std::stof(env);
        }

        if (const char* env = std::getenv("XTREE_CHECKPOINT_KEEP_COUNT")) {
            cfg.checkpoint_keep_count = std::stoull(env);
        }

        if (const char* env = std::getenv("XTREE_MAX_OPEN_FILES")) {
            cfg.max_open_files = std::stoull(env);
        }

        if (const char* env = std::getenv("XTREE_USE_GLOBAL_REGISTRIES")) {
            cfg.use_global_registries = (std::string(env) != "0" && std::string(env) != "false");
        }

        return cfg;
    }

    // Parse memory size with suffixes (e.g., "4GB", "512MB", "1024KB")
    static size_t parseMemorySize(const char* str) {
        std::string val(str);
        size_t multiplier = 1;
        if (val.size() > 2) {
            std::string suffix = val.substr(val.size() - 2);
            if (suffix == "GB" || suffix == "gb") {
                multiplier = 1ULL << 30;
                val = val.substr(0, val.size() - 2);
            } else if (suffix == "MB" || suffix == "mb") {
                multiplier = 1ULL << 20;
                val = val.substr(0, val.size() - 2);
            } else if (suffix == "KB" || suffix == "kb") {
                multiplier = 1ULL << 10;
                val = val.substr(0, val.size() - 2);
            }
        }
        return std::stoull(val) * multiplier;
    }
    
    /**
     * Create config for large datasets (10M+ records)
     */
    static StorageConfig large_dataset() {
        StorageConfig cfg;
        cfg.max_file_size = 1ULL << 32;      // 4GB files
        cfg.mmap_window_size = 1ULL << 32;   // 4GB windows
        cfg.checkpoint_keep_count = 2;       // Save space
        return cfg;
    }
    
    /**
     * Create config for huge datasets (100M+ records)
     */
    static StorageConfig huge_dataset() {
        StorageConfig cfg;
        cfg.max_file_size = 1ULL << 34;      // 16GB files
        cfg.mmap_window_size = 1ULL << 34;   // 16GB windows
        cfg.checkpoint_keep_count = 2;       // Save space
        cfg.max_open_files = 512;            // More FDs
        return cfg;
    }
    
    /**
     * Create config for memory-constrained systems
     */
    static StorageConfig low_memory() {
        StorageConfig cfg;
        cfg.max_file_size = 256 * 1024 * 1024;     // 256MB files
        cfg.mmap_window_size = 256 * 1024 * 1024;  // 256MB windows
        cfg.checkpoint_keep_count = 2;             // Save space
        cfg.max_open_files = 128;                  // Fewer FDs
        return cfg;
    }
    
    /**
     * Validate configuration
     */
    bool validate() const {
        if (max_file_size < 1024 * 1024) {
            // Minimum 1MB file size
            return false;
        }
        if (mmap_window_size < 1024 * 1024) {
            // Minimum 1MB window size
            return false;
        }
        if (checkpoint_keep_count < 1) {
            // Must keep at least one checkpoint
            return false;
        }
        if (mmap_eviction_headroom < 0.0f || mmap_eviction_headroom > 0.5f) {
            // Headroom must be in [0%, 50%]
            return false;
        }
        return true;
    }
};

} // namespace persist
} // namespace xtree