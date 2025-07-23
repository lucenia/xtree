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
    size_t mmap_window_size    = files::kMMapWindowSize;   // Default 1GB
    size_t target_file_size    = files::kTargetFileSize;   // Default 256MB
    
    // Checkpoint policy
    size_t checkpoint_keep_count = 2;                      // Keep N checkpoints (reduced for space)
    
    // Segment allocation
    size_t segment_alignment   = segment::kSegmentAlignment; // Default 4KB
    
    // File handle limits
    size_t max_open_files      = 256;                      // Max FDs to use
    
    /**
     * Create config with defaults, optionally reading from environment
     */
    static StorageConfig defaults() {
        StorageConfig cfg;
        
        // Check environment variables for overrides
        if (const char* env = std::getenv("XTREE_MAX_FILE_SIZE")) {
            cfg.max_file_size = std::stoull(env);
            // Auto-adjust window size to match
            if (cfg.max_file_size > cfg.mmap_window_size) {
                cfg.mmap_window_size = cfg.max_file_size;
            }
        }
        
        if (const char* env = std::getenv("XTREE_MMAP_WINDOW_SIZE")) {
            cfg.mmap_window_size = std::stoull(env);
        }
        
        if (const char* env = std::getenv("XTREE_CHECKPOINT_KEEP_COUNT")) {
            cfg.checkpoint_keep_count = std::stoull(env);
        }
        
        if (const char* env = std::getenv("XTREE_MAX_OPEN_FILES")) {
            cfg.max_open_files = std::stoull(env);
        }
        
        return cfg;
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
        if (mmap_window_size < max_file_size) {
            // Window must be at least as large as file size
            return false;
        }
        if (max_file_size < 1024 * 1024) {
            // Minimum 1MB file size
            return false;
        }
        if (checkpoint_keep_count < 1) {
            // Must keep at least one checkpoint
            return false;
        }
        return true;
    }
};

} // namespace persist
} // namespace xtree