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

#include "ot_checkpoint.h"
#include "object_table_sharded.hpp"
#include "checksums.h"
#include <cstring>
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <sstream>
#include <chrono>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#else
#include <windows.h>
#endif

namespace xtree { 
namespace persist {

namespace fs = std::filesystem;

OTCheckpoint::OTCheckpoint(const std::string& dir_path)
    : dir_path_(dir_path) {
    // Ensure directory exists
    PlatformFS::ensure_directory(dir_path);
}

OTCheckpoint::~OTCheckpoint() {
    // Unmap if currently mapped
    if (mapped_region_.addr) {
        PlatformFS::unmap(mapped_region_);
    }
}

bool OTCheckpoint::write(ObjectTableSharded* ot, uint64_t epoch) {
    if (!ot) {
        return false;
    }
    
    // Create temporary checkpoint file path
    std::ostringstream tmp_path;
    tmp_path << dir_path_ << "/ot_checkpoint_epoch-" << epoch << ".bin.tmp";
    std::string temp_file = tmp_path.str();
    
    // Final checkpoint file path
    std::ostringstream final_path;
    final_path << dir_path_ << "/ot_checkpoint_epoch-" << epoch << ".bin";
    std::string checkpoint_file = final_path.str();
    
    // Open temp file for writing
    std::ofstream file(temp_file, std::ios::binary | std::ios::trunc);
    if (!file) {
        return false;
    }
    
    // Enable exceptions for fatal I/O errors (bad, but not fail)
    file.exceptions(std::ofstream::badbit);
    
    // Prepare header
    Header header{};
    std::memcpy(header.magic, "OTCKPT1\0", 8);
    header.version = 1;
    header.epoch = epoch;
    header.entry_count = 0;  // Will update after counting entries
    header.row_size = sizeof(PersistentEntry);
    header.block_bytes = 0;  // No per-block CRC for now
    header.header_crc32c = 0;  // Will compute after header is complete
    
    // Take a stable snapshot of live entries under lock
    // This ensures consistency - no writer races
    std::vector<PersistentEntry> snapshot;
    size_t live_count = ot->iterate_live_snapshot(snapshot);
    
    header.entry_count = live_count;
    
    // Write header placeholder (will update CRC later)
    file.write(reinterpret_cast<const char*>(&header), sizeof(Header));
    if (!file) {
        return false;
    }
    
    // Stream snapshot entries and compute rolling CRC
    CRC32C crc_obj;
    
    for (const auto& pe : snapshot) {
        // Write entry
        file.write(reinterpret_cast<const char*>(&pe), sizeof(PersistentEntry));
        
        // Update rolling CRC
        crc_obj.update(&pe, sizeof(PersistentEntry));
        
        if (!file.good()) {
            return false;
        }
    }
    
    if (!file) {
        return false;
    }
    
    // Finalize entries CRC
    uint32_t entries_crc = crc_obj.finalize();
    
    // Write footer
    Footer footer{};
    footer.total_bytes = sizeof(Header) + (live_count * sizeof(PersistentEntry)) + sizeof(Footer);
    footer.entries_crc32c = entries_crc;
    footer.footer_crc32c = 0;
    
    // Compute footer CRC with footer_crc32c field zeroed
    footer.footer_crc32c = compute_crc32c_zeroed(&footer, sizeof(Footer),
                                                 offsetof(Footer, footer_crc32c),
                                                 sizeof(footer.footer_crc32c));
    
    file.write(reinterpret_cast<const char*>(&footer), sizeof(Footer));
    if (!file) {
        return false;
    }
    
    // Compute and update header CRC
    header.header_crc32c = compute_crc32c_zeroed(&header, sizeof(Header),
                                                 offsetof(Header, header_crc32c),
                                                 sizeof(header.header_crc32c));
    
    // Seek back to header and update with CRC
    file.seekp(0);
    file.write(reinterpret_cast<const char*>(&header), sizeof(Header));
    if (!file) {
        return false;
    }
    
    // Flush stream
    file.flush();
    file.close();
    
    // Sync the temp file to disk using PlatformFS for consistency
    // We need to reopen the file to get a handle for fsync
    #ifndef _WIN32
        int fd = ::open(temp_file.c_str(), O_RDWR);
        if (fd >= 0) {
            FSResult sync_result = PlatformFS::flush_file(static_cast<intptr_t>(fd));
            ::close(fd);
            if (!sync_result.ok) {
                // Log warning but continue - rename will fail if file isn't ready
            }
        }
    #else
        HANDLE hFile = CreateFileA(temp_file.c_str(), 
                                 GENERIC_WRITE,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE,
                                 NULL,
                                 OPEN_EXISTING,
                                 FILE_ATTRIBUTE_NORMAL,
                                 NULL);
        if (hFile != INVALID_HANDLE_VALUE) {
            FSResult sync_result = PlatformFS::flush_file(reinterpret_cast<intptr_t>(hFile));
            CloseHandle(hFile);
            if (!sync_result.ok) {
                // Log warning but continue
            }
        }
    #endif
    
    // Atomic rename temp → final
    FSResult rename_res = PlatformFS::atomic_replace(temp_file, checkpoint_file);
    if (!rename_res.ok) {
        // Clean up temp file
        std::remove(temp_file.c_str());
        return false;
    }
    
    // Fsync the parent directory to ensure rename is durable
    PlatformFS::fsync_directory(dir_path_);
    
    return true;
}

bool OTCheckpoint::map_for_read(const std::string& checkpoint_path,
                                uint64_t* out_epoch,
                                size_t* out_entry_count,
                                const PersistentEntry** out_entries) {
    // Unmap any existing mapping
    if (mapped_region_.addr) {
        PlatformFS::unmap(mapped_region_);
        mapped_header_ = nullptr;
        mapped_entries_ = nullptr;
        mapped_footer_ = nullptr;
    }
    
    // Get file size
    auto [size_res, file_size] = PlatformFS::file_size(checkpoint_path);
    if (!size_res.ok || file_size < sizeof(Header) + sizeof(Footer)) {
        return false;
    }
    
    // Memory map the entire file
    FSResult map_res = PlatformFS::map_file(checkpoint_path,
                                           0,
                                           file_size,
                                           MapMode::ReadOnly,
                                           &mapped_region_);
    if (!map_res.ok || !mapped_region_.addr) {
        return false;
    }
    
    // Validate header
    const Header* header = reinterpret_cast<const Header*>(mapped_region_.addr);
    if (std::memcmp(header->magic, "OTCKPT1\0", 8) != 0) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    if (header->version != 1) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    if (header->row_size != sizeof(PersistentEntry)) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    // Verify header CRC
    uint32_t computed_header_crc = compute_crc32c_zeroed(header, sizeof(Header),
                                                         offsetof(Header, header_crc32c),
                                                         sizeof(header->header_crc32c));
    if (computed_header_crc != header->header_crc32c) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    // Calculate expected file size
    size_t expected_size = sizeof(Header) + 
                          (header->entry_count * sizeof(PersistentEntry)) +
                          sizeof(Footer);
    if (file_size != expected_size) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    // Get pointers to entries and footer
    const PersistentEntry* entries = reinterpret_cast<const PersistentEntry*>(
        static_cast<const uint8_t*>(mapped_region_.addr) + sizeof(Header));
    
    const Footer* footer = reinterpret_cast<const Footer*>(
        static_cast<const uint8_t*>(mapped_region_.addr) + sizeof(Header) +
        (header->entry_count * sizeof(PersistentEntry)));
    
    // Verify footer CRC
    uint32_t computed_footer_crc = compute_crc32c_zeroed(footer, sizeof(Footer),
                                                         offsetof(Footer, footer_crc32c),
                                                         sizeof(footer->footer_crc32c));
    if (computed_footer_crc != footer->footer_crc32c) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    // Verify entries CRC
    uint32_t computed_entries_crc = CRC32C::compute(entries,
                                                    header->entry_count * sizeof(PersistentEntry));
    
    if (computed_entries_crc != footer->entries_crc32c) {
        PlatformFS::unmap(mapped_region_);
        return false;
    }
    
    // Success - store pointers
    mapped_header_ = header;
    mapped_entries_ = entries;
    mapped_footer_ = footer;
    
    // Return values
    if (out_epoch) {
        *out_epoch = header->epoch;
    }
    if (out_entry_count) {
        *out_entry_count = header->entry_count;
    }
    if (out_entries) {
        *out_entries = entries;
    }
    
    return true;
}

std::string OTCheckpoint::find_latest_checkpoint(const std::string& dir_path) {
    std::string latest_path;
    uint64_t latest_epoch = 0;
    
    try {
        for (const auto& entry : fs::directory_iterator(dir_path)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            
            std::string filename = entry.path().filename().string();
            
            // Parse checkpoint filename: ot_checkpoint_epoch-<epoch>.bin
            if (filename.find("ot_checkpoint_epoch-") != 0) {
                continue;
            }
            if (filename.rfind(".bin") != filename.length() - 4) {
                continue;
            }
            
            // Extract epoch
            size_t start = strlen("ot_checkpoint_epoch-");
            size_t end = filename.rfind(".bin");
            std::string epoch_str = filename.substr(start, end - start);
            
            try {
                uint64_t epoch = std::stoull(epoch_str);
                if (epoch > latest_epoch) {
                    latest_epoch = epoch;
                    latest_path = entry.path().string();
                }
            } catch (...) {
                // Invalid epoch number, skip
                continue;
            }
        }
    } catch (...) {
        // Directory iteration failed
        return "";
    }
    
    return latest_path;
}

void OTCheckpoint::cleanup_old_checkpoints(const std::string& dir_path, 
                                          size_t keep_count) {
    struct CheckpointInfo {
        std::string path;
        uint64_t epoch;
    };
    
    std::vector<CheckpointInfo> checkpoints;
    
    try {
        // Collect all checkpoint files
        for (const auto& entry : fs::directory_iterator(dir_path)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            
            std::string filename = entry.path().filename().string();
            
            // Parse checkpoint filename
            if (filename.find("ot_checkpoint_epoch-") != 0) {
                continue;
            }
            if (filename.rfind(".bin") != filename.length() - 4) {
                continue;
            }
            
            // Extract epoch
            size_t start = strlen("ot_checkpoint_epoch-");
            size_t end = filename.rfind(".bin");
            std::string epoch_str = filename.substr(start, end - start);
            
            try {
                uint64_t epoch = std::stoull(epoch_str);
                checkpoints.push_back({entry.path().string(), epoch});
            } catch (...) {
                continue;
            }
        }
        
        // Sort by epoch descending
        std::sort(checkpoints.begin(), checkpoints.end(),
                 [](const CheckpointInfo& a, const CheckpointInfo& b) {
                     return a.epoch > b.epoch;
                 });
        
        // Delete old checkpoints beyond keep_count
        bool deleted_any = false;
        for (size_t i = keep_count; i < checkpoints.size(); i++) {
            if (std::remove(checkpoints[i].path.c_str()) == 0) {
                deleted_any = true;
            }
        }
        
        // Optionally fsync directory after deletions for belt-and-suspenders durability
        if (deleted_any) {
            PlatformFS::fsync_directory(dir_path);
        }
        
    } catch (...) {
        // Cleanup is best-effort
    }
}

uint32_t OTCheckpoint::compute_crc32c_zeroed(const void* data, size_t len,
                                            size_t zero_offset, size_t zero_len) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    
    // Compute CRC in three parts: before, zeroed, after
    CRC32C crc_obj;
    
    // Part 1: Before the zeroed field
    if (zero_offset > 0) {
        crc_obj.update(bytes, zero_offset);
    }
    
    // Part 2: Zero bytes for the field (allocation-free)
    uint8_t zero_buf[256] = {0};
    size_t remain = zero_len;
    while (remain > 0) {
        size_t chunk = std::min(remain, sizeof(zero_buf));
        crc_obj.update(zero_buf, chunk);
        remain -= chunk;
    }
    
    // Part 3: After the zeroed field
    size_t after_offset = zero_offset + zero_len;
    if (after_offset < len) {
        crc_obj.update(bytes + after_offset, len - after_offset);
    }
    
    return crc_obj.finalize();
}

} // namespace persist
} // namespace xtree