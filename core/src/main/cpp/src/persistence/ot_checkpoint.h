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
#include <cstdint>
#include <vector>
#include <memory>
#include "ot_entry.h"
#include "platform_fs.h"

namespace xtree { 
namespace persist {

// Forward declarations
class ObjectTableSharded;

/**
 * OT Checkpoint - Binary snapshot format for fast recovery
 * 
 * File Layout (4KB aligned, little-endian):
 * +----------------------+ 0
 * | Header (4 KiB)       |
 * +----------------------+ 4 KiB
 * | Entry blocks ...     | (fixed-size rows, contiguous)
 * +----------------------+
 * | Footer (aligned)     |
 * +----------------------+
 */
class OTCheckpoint {
public:
    // Use portable packing for cross-platform compatibility
    #pragma pack(push, 1)
    
    // Header structure (4KB)
    struct Header {
        char magic[8];          // "OTCKPT1\0"
        uint32_t version;       // Format version (1)
        uint32_t _pad1;
        uint64_t epoch;         // Commit epoch of snapshot
        uint64_t entry_count;   // Number of OT rows
        uint32_t row_size;      // Size of each row (48 or 56)
        uint32_t block_bytes;   // CRC granularity (0 = none)
        uint8_t reserved[4052]; // Pad to 4KB (4096 - 44 bytes of fields = 4052)
        uint32_t header_crc32c; // CRC32C of header (this field zeroed)
    };
    
    static_assert(sizeof(Header) == 4096, "Header must be exactly 4KB");
    
    // Persisted OT entry (matches in-memory layout minus pointers)
    struct PersistentEntry {
        uint64_t handle_idx;
        uint32_t file_id;
        uint32_t segment_id;
        uint64_t offset;
        uint32_t length;
        uint8_t class_id;
        uint8_t kind;           // NodeKind
        uint16_t tag;           // Widened to 16 bits for better ABA protection
        uint64_t birth_epoch;
        uint64_t retire_epoch;
    };
    
    static_assert(sizeof(PersistentEntry) == 48, "Entry must be 48 bytes");
    
    // Footer structure
    struct Footer {
        uint64_t total_bytes;    // File size
        uint32_t entries_crc32c; // CRC over all rows
        uint32_t footer_crc32c;  // CRC of footer (this field zeroed)
    };
    
    #pragma pack(pop)
    
    explicit OTCheckpoint(const std::string& dir_path);
    ~OTCheckpoint();
    
    // Write checkpoint from ObjectTable (atomic & durable)
    // Creates temp file, writes with CRCs, atomic rename
    // TODO: Add snapshot isolation - either stop writers briefly or
    // implement iterate_live_snapshot() to avoid races
    bool write(ObjectTableSharded* ot, uint64_t epoch);
    
    // Map checkpoint for fast recovery
    // Returns entry count and pointer to first entry
    bool map_for_read(const std::string& checkpoint_path,
                      uint64_t* out_epoch,
                      size_t* out_entry_count,
                      const PersistentEntry** out_entries);
    
    // Get latest checkpoint file in directory
    static std::string find_latest_checkpoint(const std::string& dir_path);
    
    // Clean up old checkpoints (keep N most recent)
    static void cleanup_old_checkpoints(const std::string& dir_path, 
                                        size_t keep_count = 3);
    
private:
    std::string dir_path_;
    
    // Memory-mapped checkpoint for reading (zero-initialized)
    MappedRegion mapped_region_ = {};
    const Header* mapped_header_ = nullptr;
    const PersistentEntry* mapped_entries_ = nullptr;
    const Footer* mapped_footer_ = nullptr;
    
    // Helper to compute CRC with field zeroed
    static uint32_t compute_crc32c_zeroed(const void* data, size_t len, 
                                          size_t zero_offset, size_t zero_len);
};

} // namespace persist
} // namespace xtree