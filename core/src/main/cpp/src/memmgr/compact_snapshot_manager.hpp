/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Fast snapshot manager using compact allocator and MMAP
 */

#pragma once

#include "compact_allocator.hpp"
// #include "cow_mmap_manager.hpp" // Removed - using Arena-based MMAP instead
#include "arena.hpp"
#include <fstream>
#include <cstdlib>
#include <sys/stat.h>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <sys/stat.h>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#ifdef _WIN32
#include <windows.h>
#endif

namespace xtree {

/**
 * Header for compact snapshots
 */
struct CompactSnapshotHeader {
    uint32_t magic = 0x58545245; // 'XTRE'
    uint32_t version = 1;
    uint64_t snapshot_time;
    uint32_t used_size;
    uint32_t arena_size;
    uint16_t dimension;
    uint16_t precision;
    uint32_t record_count;
    uint32_t checksum;
    uint32_t root_offset; // Offset of the root XTreeBucket
    char padding[12]; // Future use
};

/**
 * Manages fast snapshots using compact allocator
 */
class CompactSnapshotManager {
private:
    std::unique_ptr<CompactAllocator> allocator_;
    // Use Arena with MMAP mode for fast loading
    std::unique_ptr<Arena> snapshot_arena_;
    std::string snapshot_path_;
    bool is_loaded_ = false;
    uint32_t root_offset_ = CompactAllocator::INVALID_OFFSET;
    
public:
    explicit CompactSnapshotManager(const std::string& snapshot_path, 
                                   size_t initial_size = 64 * 1024 * 1024)
        : snapshot_path_(snapshot_path) {
        
        // Check if snapshot exists
        if (file_exists(snapshot_path)) {
            std::cout << "[CompactSnapshotManager] Loading existing snapshot: " << snapshot_path << "\n";
            load_snapshot();
            std::cout << "[CompactSnapshotManager] Snapshot loaded successfully\n";
        } else {
            // Create new allocator
            std::cout << "[CompactSnapshotManager] Creating new allocator with size: " << initial_size << "\n";
            allocator_ = std::make_unique<CompactAllocator>(initial_size);
        }
    }
    
    ~CompactSnapshotManager() {
        // Arena cleanup is automatic via unique_ptr
        // Auto-save on destruction if dirty
        std::cout << "[CompactSnapshotManager destructor] allocator_=" << allocator_.get() 
                  << ", is_loaded_=" << is_loaded_ << "\n" << std::flush;
        if (allocator_ && !is_loaded_) {
            try {
                std::cout << "[CompactSnapshotManager destructor] Calling save_snapshot()...\n" << std::flush;
                save_snapshot();
                std::cout << "[CompactSnapshotManager destructor] save_snapshot() completed\n" << std::flush;
            } catch (const std::exception& e) {
                trace() << "[CompactSnapshotManager destructor] save_snapshot() failed: " << e.what() << "\n" << std::flush;
            } catch (...) {
                trace() << "[CompactSnapshotManager destructor] save_snapshot() failed with unknown error\n" << std::flush;
            }
        }
    }
    
    // Get the allocator
    CompactAllocator* get_allocator() {
        return allocator_.get();
    }
    
    // Save snapshot to disk
    void save_snapshot() {
        if (!allocator_) {
            throw std::runtime_error("No allocator to save");
        }
        
        // Prepare header
        CompactSnapshotHeader header;
        header.snapshot_time = std::chrono::system_clock::now().time_since_epoch().count();
        header.used_size = static_cast<uint32_t>(allocator_->get_used_size());
        header.arena_size = static_cast<uint32_t>(allocator_->get_arena_size());
        header.root_offset = root_offset_;
        // TODO: Set dimension, precision, record_count from index
        // Handle multi-segment allocators
        if (allocator_->get_segment_count() > 1) {
            std::cout << "[save_snapshot] Detected " << allocator_->get_segment_count() 
                      << " segments, calling save_multi_segment_snapshot()\n" << std::flush;
            save_multi_segment_snapshot();
            return;
        }
        header.checksum = calculate_checksum(allocator_->get_arena_base(), header.used_size);
        
        // Create temporary file - only save what's actually used
        std::string temp_path = snapshot_path_ + ".tmp";
        size_t total_size = sizeof(header) + header.used_size;
        
        std::cout << "[CompactSnapshotManager::save_snapshot] Saving snapshot with used_size=" 
                  << header.used_size << " (" << (header.used_size / (1024.0 * 1024.0)) << " MB) of "
                  << header.arena_size << " (" << (header.arena_size / (1024.0 * 1024.0)) << " MB) arena"
                  << ", file size will be " << total_size << " (" << (total_size / (1024.0 * 1024.0)) << " MB)\n";
        
        // Use direct file I/O for compact saves
        #ifdef _WIN32
            // Windows implementation
            HANDLE file = CreateFileA(temp_path.c_str(), GENERIC_WRITE, 0, NULL, 
                                    CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
            if (file == INVALID_HANDLE_VALUE) {
                throw std::runtime_error("Failed to create snapshot file");
            }
            
            DWORD written;
            WriteFile(file, &header, sizeof(header), &written, NULL);
            // TODO: Support multi-segment writes
            WriteFile(file, allocator_->get_arena_base(), header.used_size, &written, NULL);
            CloseHandle(file);
        #else
            // POSIX implementation
            int fd = open(temp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) {
                throw std::runtime_error("Failed to create snapshot file");
            }
            
            // Write header and data
            if (write(fd, &header, sizeof(header)) != sizeof(header) ||
                write(fd, allocator_->get_arena_base(), header.used_size) != static_cast<ssize_t>(header.used_size)) {
                // TODO: Support multi-segment writes
                close(fd);
                unlink(temp_path.c_str());
                throw std::runtime_error("Failed to write snapshot data");
            }
            
            // Ensure data is flushed to disk
            fsync(fd);
            close(fd);
        #endif
        
        // Atomic rename
        if (std::rename(temp_path.c_str(), snapshot_path_.c_str()) != 0) {
            throw std::runtime_error("Failed to rename snapshot file");
        }
    }
    
    // Load snapshot from disk (ultra-fast with MMAP)
    void load_snapshot() {
        std::cout << "[CompactSnapshotManager::load_snapshot] Using Arena MMAP to load snapshot...\n";
        
        // Get file size
        struct stat st;
        if (stat(snapshot_path_.c_str(), &st) != 0) {
            throw std::runtime_error("Failed to stat snapshot file");
        }
        size_t file_size = st.st_size;
        
        // Create Arena with MMAP mode - this is FAST (O(1) mmap call)
        snapshot_arena_ = std::make_unique<Arena>(file_size, Arena::Mode::MMAP, snapshot_path_);
        snapshot_arena_->freeze();  // Mark as read-only
        
        std::cout << "[CompactSnapshotManager::load_snapshot] Arena MMAP loaded successfully (" 
                  << (file_size / (1024.0 * 1024.0)) << " MB) - O(1) operation!\n";
        
        // Check magic number to determine format version
        const uint32_t* magic_ptr = snapshot_arena_->get_ptr<uint32_t>(0);
        if (!magic_ptr) {
            throw std::runtime_error("Failed to read snapshot magic");
        }
        
        if (*magic_ptr == 0x58545245) { // 'XTRE' - v1 format
            load_v1_snapshot();
        } else if (*magic_ptr == 0x58545246) { // 'XTRF' - v2 format (multi-segment)
            load_v2_snapshot();
        } else {
            throw std::runtime_error("Unknown snapshot format");
        }
    }
    
    // Load v1 (single-segment) snapshot
    void load_v1_snapshot() {
        const CompactSnapshotHeader* header = 
            snapshot_arena_->get_ptr<CompactSnapshotHeader>(0);
        
        if (!header || header->version != 1) {
            throw std::runtime_error("Invalid v1 snapshot file format");
        }
        
        // Verify checksum
        const void* data_start = snapshot_arena_->get_ptr<void>(sizeof(CompactSnapshotHeader));
        uint32_t calc_checksum = calculate_checksum(data_start, header->used_size);
        if (calc_checksum != header->checksum) {
            throw std::runtime_error("Snapshot checksum mismatch");
        }
        
        // Create allocator backed by MMAP
        void* mmap_base = const_cast<void*>(data_start);  // CompactAllocator will manage this memory
        
        std::cout << "[CompactSnapshotManager::load_v1_snapshot] Loading snapshot with used_size=" 
                  << header->used_size << " (" << (header->used_size / (1024.0 * 1024.0)) << " MB)"
                  << ", arena_size=" << header->arena_size << " (" << (header->arena_size / (1024.0 * 1024.0)) << " MB)\n";
        
        allocator_ = std::make_unique<CompactAllocator>(mmap_base, header->arena_size, header->used_size);
        root_offset_ = header->root_offset;
        std::cout << "[CompactSnapshotManager::load_v1_snapshot] Root offset restored: " << root_offset_ << "\n";
        
        is_loaded_ = true;
    }
    
    // Load v2 (multi-segment) snapshot
    void load_v2_snapshot() {
        // Define v2 header structure (must match save_multi_segment_snapshot)
        struct CompactSnapshotHeaderV2 {
            uint32_t magic;
            uint32_t version;
            uint64_t snapshot_time;
            uint64_t total_used_size;
            uint32_t num_segments;
            uint64_t root_offset;
            uint16_t dimension;
            uint16_t precision;
            uint32_t record_count;
            uint32_t checksum;
            char padding[24];
        };
        
        struct SegmentInfo {
            uint64_t size;
            uint64_t used;
            uint64_t file_offset;
        };
        
        const CompactSnapshotHeaderV2* header = 
            snapshot_arena_->get_ptr<CompactSnapshotHeaderV2>(0);
        
        if (!header || header->version != 2) {
            throw std::runtime_error("Invalid v2 snapshot file format");
        }
        
        std::cout << "[CompactSnapshotManager::load_v2_snapshot] Loading multi-segment snapshot:\n"
                  << "  Segments: " << header->num_segments << "\n"
                  << "  Total used: " << (header->total_used_size / (1024.0 * 1024.0)) << " MB\n";
        
        // Read segment info
        const SegmentInfo* segment_infos = static_cast<const SegmentInfo*>(
            snapshot_arena_->get_ptr<SegmentInfo>(sizeof(CompactSnapshotHeaderV2)));
        
        if (!segment_infos) {
            throw std::runtime_error("Failed to read segment info");
        }
        
        // Verify checksum by reading all segment data
        uint32_t calc_checksum = 0;
        for (uint32_t i = 0; i < header->num_segments; ++i) {
            const void* seg_data = snapshot_arena_->get_ptr<void>(segment_infos[i].file_offset);
            if (seg_data && segment_infos[i].used > 0) {
                calc_checksum = calculate_checksum(seg_data, segment_infos[i].used, calc_checksum);
            }
        }
        
        if (calc_checksum != header->checksum) {
            throw std::runtime_error("Multi-segment snapshot checksum mismatch");
        }
        
        // For multi-segment loading, we need to reconstruct the allocator exactly
        // Start with the first segment mapped from file
        if (header->num_segments > 0) {
            void* first_seg_data = const_cast<void*>(
                snapshot_arena_->get_ptr<void>(segment_infos[0].file_offset)
            );
            
            // Create allocator with first segment from MMAP
            allocator_ = std::make_unique<CompactAllocator>(
                first_seg_data, segment_infos[0].size, segment_infos[0].used);
            
            // CRITICAL: Restore allocator state to ensure offsets work correctly
            // The allocator's current_offset_ must match the used size of the last segment
            // to ensure new allocations don't overwrite existing data
            
            // Add remaining segments as heap-based segments
            uint32_t last_loaded_segment = 0;
            size_t last_segment_used = segment_infos[0].used;
            
            for (uint32_t i = 1; i < header->num_segments; ++i) {
                std::cout << "[CompactSnapshotManager::load_v2_snapshot] Segment " << i 
                          << ": size=" << (segment_infos[i].size / (1024.0 * 1024.0)) << " MB"
                          << ", used=" << (segment_infos[i].used / (1024.0 * 1024.0)) << " MB\n";
                
                // Only load segments that have actual data
                if (segment_infos[i].used > 0) {
                    const void* seg_data = snapshot_arena_->get_ptr<void>(segment_infos[i].file_offset);
                    if (seg_data) {
                        allocator_->load_segment_from_snapshot(seg_data, segment_infos[i].size, segment_infos[i].used);
                        last_loaded_segment = i;
                        last_segment_used = segment_infos[i].used;
                    }
                } else {
                    std::cout << "[CompactSnapshotManager::load_v2_snapshot] Skipping empty segment " << i << "\n";
                }
            }
            
            // Restore allocator state - use the last segment that was actually loaded
            allocator_->restore_state_after_load(last_loaded_segment, last_segment_used);
        }
        
        root_offset_ = static_cast<uint32_t>(header->root_offset);
        std::cout << "[CompactSnapshotManager::load_v2_snapshot] Multi-segment snapshot loaded successfully\n"
                  << "  Root offset: " << root_offset_ << "\n";
        
        is_loaded_ = true;
    }
    
    // Get snapshot info
    bool is_snapshot_loaded() const { return is_loaded_; }
    
    size_t get_snapshot_size() const {
        return allocator_ ? allocator_->get_used_size() : 0;
    }
    
    // Set the root offset (should be called before save_snapshot)
    void set_root_offset(uint32_t offset) {
        root_offset_ = offset;
    }
    
    // Get the root offset (available after load_snapshot)
    uint32_t get_root_offset() const {
        return root_offset_;
    }
    
private:
    bool file_exists(const std::string& path) {
        struct stat st;
        return stat(path.c_str(), &st) == 0;
    }
    
    uint32_t calculate_checksum(const void* data, size_t size, uint32_t seed = 0) {
        // Simple checksum for now - could use CRC32
        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        uint32_t sum = seed;
        for (size_t i = 0; i < size; i++) {
            sum = (sum << 1) ^ bytes[i];
        }
        return sum;
    }
    
    // Save multi-segment snapshot
    void save_multi_segment_snapshot() {
        // Prepare v2 header for multi-segment
        struct CompactSnapshotHeaderV2 {
            uint32_t magic = 0x58545246;     // 'XTRF' - v2 format
            uint32_t version = 2;
            uint64_t snapshot_time;
            uint64_t total_used_size;
            uint32_t num_segments;
            uint64_t root_offset;
            uint16_t dimension = 0;
            uint16_t precision = 0;
            uint32_t record_count = 0;
            uint32_t checksum;
            char padding[24];
        };
        
        struct SegmentInfo {
            uint64_t size;
            uint64_t used;
            uint64_t file_offset;
        };
        
        CompactSnapshotHeaderV2 header;
        header.snapshot_time = std::chrono::system_clock::now().time_since_epoch().count();
        header.root_offset = root_offset_;
        header.num_segments = allocator_->get_segment_count();
        header.total_used_size = allocator_->get_used_size();
        
        // Calculate checksum across all segments
        header.checksum = 0;
        for (size_t seg_id = 0; seg_id < allocator_->get_segment_count(); ++seg_id) {
            auto seg_data = allocator_->get_segment_data(seg_id);
            if (seg_data.first && seg_data.second > 0) {
                header.checksum = calculate_checksum(seg_data.first, seg_data.second, header.checksum);
            }
        }
        
        std::cout << "[CompactSnapshotManager::save_multi_segment_snapshot] Saving " 
                  << header.num_segments << " segments, total used: "
                  << (header.total_used_size / (1024.0 * 1024.0)) << " MB\n";
        
        // Prepare segment info
        std::vector<SegmentInfo> segment_infos(header.num_segments);
        uint64_t current_offset = sizeof(header) + sizeof(SegmentInfo) * header.num_segments;
        
        for (uint32_t i = 0; i < header.num_segments; ++i) {
            auto seg_data = allocator_->get_segment_data(i);
            segment_infos[i].size = allocator_->get_segment_size(i);
            segment_infos[i].used = seg_data.second;
            segment_infos[i].file_offset = current_offset;
            current_offset += seg_data.second;
            
            std::cout << "[DEBUG] Segment " << i << " info: size=" 
                      << (segment_infos[i].size / (1024.0 * 1024.0)) << " MB, used="
                      << (segment_infos[i].used / (1024.0 * 1024.0)) << " MB, file_offset="
                      << segment_infos[i].file_offset << "\n" << std::flush;
        }
        
        // Create temporary file
        std::string temp_path = snapshot_path_ + ".tmp";
        
        #ifdef _WIN32
            // Windows implementation
            HANDLE file = CreateFileA(temp_path.c_str(), GENERIC_WRITE, 0, NULL, 
                                    CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
            if (file == INVALID_HANDLE_VALUE) {
                throw std::runtime_error("Failed to create snapshot file");
            }
            
            DWORD written;
            WriteFile(file, &header, sizeof(header), &written, NULL);
            WriteFile(file, segment_infos.data(), sizeof(SegmentInfo) * header.num_segments, &written, NULL);
            
            for (uint32_t i = 0; i < header.num_segments; ++i) {
                auto seg_data = allocator_->get_segment_data(i);
                if (seg_data.first && seg_data.second > 0) {
                    WriteFile(file, seg_data.first, seg_data.second, &written, NULL);
                }
            }
            CloseHandle(file);
        #else
            // POSIX implementation
            int fd = open(temp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) {
                throw std::runtime_error("Failed to create snapshot file");
            }
            
            if (write(fd, &header, sizeof(header)) != sizeof(header)) {
                close(fd);
                unlink(temp_path.c_str());
                throw std::runtime_error("Failed to write snapshot header");
            }
            if (write(fd, segment_infos.data(), sizeof(SegmentInfo) * header.num_segments) != 
                static_cast<ssize_t>(sizeof(SegmentInfo) * header.num_segments)) {
                close(fd);
                unlink(temp_path.c_str());
                throw std::runtime_error("Failed to write segment info");
            }
            
            for (uint32_t i = 0; i < header.num_segments; ++i) {
                auto seg_data = allocator_->get_segment_data(i);
                if (seg_data.first && seg_data.second > 0) {
                    std::cout << "[DEBUG] Writing segment " << i << ": " 
                              << (seg_data.second / (1024.0 * 1024.0)) << " MB\n" << std::flush;
                    
                    // Write in chunks to handle large segments (>2GB)
                    const size_t CHUNK_SIZE = 1024 * 1024 * 1024; // 1GB chunks
                    const char* data_ptr = static_cast<const char*>(seg_data.first);
                    size_t remaining = seg_data.second;
                    size_t offset = 0;
                    
                    while (remaining > 0) {
                        size_t to_write = std::min(remaining, CHUNK_SIZE);
                        ssize_t written = write(fd, data_ptr + offset, to_write);
                        if (written != static_cast<ssize_t>(to_write)) {
                            trace() << "[ERROR] Failed to write segment " << i 
                                      << " chunk at offset " << offset
                                      << ": expected " << to_write 
                                      << " bytes, wrote " << written << " bytes\n" << std::flush;
                            close(fd);
                            unlink(temp_path.c_str());
                            throw std::runtime_error("Failed to write segment data");
                        }
                        offset += written;
                        remaining -= written;
                        
                        if (remaining > 0) {
                            std::cout << "[DEBUG] Wrote chunk: " << (offset / (1024.0 * 1024.0)) 
                                      << " MB of " << (seg_data.second / (1024.0 * 1024.0)) 
                                      << " MB\n" << std::flush;
                        }
                    }
                    
                    std::cout << "[DEBUG] Segment " << i << " written successfully\n" << std::flush;
                }
            }
            
            fsync(fd);
            close(fd);
        #endif
        
        // Atomic rename
        std::rename(temp_path.c_str(), snapshot_path_.c_str());
        
        std::cout << "[CompactSnapshotManager] Multi-segment snapshot saved successfully\n";
    }
};

} // namespace xtree