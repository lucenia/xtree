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
#include <ctime>

namespace xtree { 
namespace persist {

/**
 * Manifest - JSON file tracking all persistent files
 * 
 * Contains:
 * - Superblock path
 * - Latest checkpoint info  
 * - Delta log inventory
 * - Data file inventory
 * 
 * Written atomically via temp + rename pattern
 */
class Manifest {
public:
    // Checkpoint information
    struct CheckpointInfo {
        std::string path;
        uint64_t epoch;
        size_t size;
        size_t entries;
        uint32_t crc32c;
    };
    
    // Delta log information
    struct DeltaLogInfo {
        std::string path;
        uint64_t start_epoch;
        uint64_t end_epoch;  // 0 if still active
        size_t size;
    };
    
    // Data file information
    struct DataFileInfo {
        uint8_t class_id;    // Size class
        uint32_t seq;        // Sequence number
        std::string file;    // Filename
        size_t bytes;        // File size
    };
    
    // Root catalog entry for multi-field support
    struct RootEntry {
        std::string name;        // Field/tree name (empty string = primary)
        uint64_t node_id_raw;    // NodeID raw value (explicit naming)
        uint64_t epoch;          // Last update epoch
        std::vector<float> mbr;  // Root MBR: [min0, max0, min1, max1, ...] (dims*2 values)
    };
    
    explicit Manifest(const std::string& data_dir);
    ~Manifest() = default;
    
    // Load manifest from disk (returns false if missing/corrupt)
    bool load();
    
    // Store manifest to disk (atomic write via temp + rename)
    bool store();
    
    // Reload manifest from disk to see latest changes
    bool reload();
    
    // Getters
    const std::string& get_data_dir() const { return data_dir_; }
    const std::string& get_superblock_path() const { return superblock_path_; }
    const CheckpointInfo& get_checkpoint() const { return checkpoint_; }
    const std::vector<DeltaLogInfo>& get_delta_logs() const { return delta_logs_; }
    const std::vector<DataFileInfo>& get_data_files() const { return data_files_; }
    const std::vector<RootEntry>& get_roots() const { return roots_; }
    
    // Setters for recovery/checkpoint operations
    void set_superblock_path(const std::string& path) { 
        superblock_path_ = path; 
    }
    
    void set_checkpoint(const CheckpointInfo& info) {
        checkpoint_ = info;
    }
    
    void set_delta_logs(const std::vector<DeltaLogInfo>& logs) {
        delta_logs_ = logs;
    }
    
    void add_delta_log(const DeltaLogInfo& info) {
        delta_logs_.push_back(info);
    }
    
    bool close_delta_log(const std::string& path, uint64_t end_epoch, size_t final_size = 0) {
        for (auto& log : delta_logs_) {
            if (log.path == path && log.end_epoch == 0) {
                log.end_epoch = end_epoch;
                log.size = final_size;
                return true;
            }
        }
        return false;
    }
    
    void add_data_file(const DataFileInfo& info) {
        data_files_.push_back(info);
    }
    
    // Clean up old delta logs that are fully covered by checkpoint
    void prune_old_delta_logs(uint64_t checkpoint_epoch);
    
    // Get delta logs that need to be replayed (start_epoch > checkpoint_epoch)
    std::vector<DeltaLogInfo> get_logs_after_checkpoint(uint64_t checkpoint_epoch) const;
    
    // Helper to generate manifest path
    std::string get_manifest_path() const;  // Implementation in .cpp to use filesystem::path
    
    // Root catalog operations for multi-field support
    void set_roots(const std::vector<RootEntry>& roots) { roots_ = roots; }
    void clear_roots() { roots_.clear(); }
    bool has_roots() const { return !roots_.empty(); }
    
private:
    std::string data_dir_;
    
    // Manifest contents
    uint32_t version_ = 1;  // No need for version bump - not in production yet
    time_t created_unix_ = 0;
    std::string superblock_path_ = "superblock.bin";
    CheckpointInfo checkpoint_;
    std::vector<DeltaLogInfo> delta_logs_;
    std::vector<DataFileInfo> data_files_;
    std::vector<RootEntry> roots_;  // Named roots catalog
    
    // JSON serialization helpers
    std::string to_json() const;
    bool from_json(const std::string& json_str);
};

} // namespace persist
} // namespace xtree