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

#include "segment_allocator.h"
#include "config.h"
#include "segment_classes.hpp"
#include <algorithm>
#include <cassert>
#include <sstream>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include "../util/log.h"

#ifndef _WIN32
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#else
#include <windows.h>
#endif

namespace xtree { 
    namespace persist {
        
        #ifndef NDEBUG
        // Debug counters to enforce O(1) guarantee
        std::atomic<uint64_t> g_segment_scan_count{0};
        std::atomic<uint64_t> g_segment_lock_count{0};
        #endif

        SegmentAllocator::SegmentAllocator(const std::string& data_dir)
            : data_dir_(data_dir), config_(StorageConfig::defaults()) {
            // Use global registries if configured (default), otherwise create owned ones
            if (config_.use_global_registries) {
                file_registry_ = &FileHandleRegistry::global();
                mapping_manager_ = &MappingManager::global();
            } else {
                // Create internal registries for backward compatibility
                owned_file_registry_ = std::make_unique<FileHandleRegistry>(config_.max_open_files);
                owned_mapping_manager_ = std::make_unique<MappingManager>(
                    *owned_file_registry_,
                    config_.mmap_window_size,
                    8192);       // Max extents

                file_registry_ = owned_file_registry_.get();
                mapping_manager_ = owned_mapping_manager_.get();
            }

            // Ensure data directory exists
            FSResult dir_result = PlatformFS::ensure_directory(data_dir);
            if (!dir_result.ok) {
                // Log error but continue - individual file creation will fail later
            }

            // Initialize allocators for each size class
            // Actual file creation happens lazily on first allocation
        }
        
        // Static assertion to ensure size classes meet alignment requirements
        static_assert(persist::size_class::kMinSize % alignof(std::max_align_t) == 0,
                      "Minimum size class must satisfy max alignment requirements");
        
        // New constructor with explicit configuration
        SegmentAllocator::SegmentAllocator(const std::string& data_dir, const StorageConfig& config)
            : data_dir_(data_dir), config_(config) {
            if (!config_.validate()) {
                throw std::invalid_argument("Invalid storage configuration");
            }

            // Use global registries if configured, otherwise create owned ones
            if (config_.use_global_registries) {
                file_registry_ = &FileHandleRegistry::global();
                mapping_manager_ = &MappingManager::global();
            } else {
                // Create internal registries
                owned_file_registry_ = std::make_unique<FileHandleRegistry>(config_.max_open_files);
                owned_mapping_manager_ = std::make_unique<MappingManager>(
                    *owned_file_registry_,
                    config_.mmap_window_size,
                    8192);       // Max extents

                file_registry_ = owned_file_registry_.get();
                mapping_manager_ = owned_mapping_manager_.get();
            }

            // Ensure data directory exists
            FSResult dir_result = PlatformFS::ensure_directory(data_dir);
            if (!dir_result.ok) {
                // Log error but continue
            }
        }
        
        // Constructor that takes registries (for DurableStore)
        SegmentAllocator::SegmentAllocator(const std::string& data_dir,
                                           FileHandleRegistry& fhr,
                                           MappingManager& mm)
            : data_dir_(data_dir), file_registry_(&fhr), mapping_manager_(&mm),
              config_(StorageConfig::defaults()) {
            // Ensure data directory exists
            FSResult dir_result = PlatformFS::ensure_directory(data_dir);
            if (!dir_result.ok) {
                // Log error but continue
            }
        }
        
        // Constructor with registries and config
        SegmentAllocator::SegmentAllocator(const std::string& data_dir,
                                           FileHandleRegistry& fhr,
                                           MappingManager& mm,
                                           const StorageConfig& config)
            : data_dir_(data_dir), file_registry_(&fhr), mapping_manager_(&mm),
              config_(config) {
            if (!config_.validate()) {
                throw std::invalid_argument("Invalid storage configuration");
            }

            // Ensure data directory exists
            FSResult dir_result = PlatformFS::ensure_directory(data_dir);
            if (!dir_result.ok) {
                // Log error but continue
            }
        }

        SegmentAllocator::~SegmentAllocator() {
            // Ensure all pins are released before member destruction
            // This prevents accessing MappingManager during static destructor phase
            close_all();
        }

        SegmentAllocator::Allocation SegmentAllocator::allocate(size_t size, NodeKind kind) {
            // Guard: block allocations in read-only mode
            if (read_only_) {
                throw std::logic_error("Cannot allocate in read-only mode (serverless reader)");
            }

            uint8_t class_id = size_to_class(size);
            size_t alloc_size = class_to_size(class_id);

            auto& allocator = allocators_[class_id];
            std::lock_guard<std::mutex> lock(allocator.mu);
            
            // Track total allocations
            allocator.total_allocations++;
            
            // Use bitmap-based allocation for O(1) performance
            const uint32_t class_sz = class_to_size(class_id);
            
            // Prefer active segment
            Segment* seg = allocator.active_segment;
            if (!seg || !seg->has_free_blocks()) {
                // Find any segment with free space
                seg = nullptr;
                for (auto& up : allocator.segments) {
                    if (up->has_free_blocks()) {
                        seg = up.get();
                        break;
                    }
                }
                if (!seg) {
                    seg = allocate_new_segment(class_id, kind);
                    if (!seg) {
                        // Failed to allocate segment - critical error
                        return Allocation{0, 0, 0, 0, 0};
                    }
                }
                allocator.active_segment = seg;
            }
            
            int bit = seg->find_free_bit();
            if (bit < 0) {
                // Very rare race: another thread consumed last bit of active segment
                // Fall back to scanning/creating one more time
                seg = nullptr;
                for (auto& up : allocator.segments) {
                    if (up->has_free_blocks()) {
                        seg = up.get();
                        break;
                    }
                }
                if (!seg) {
                    seg = allocate_new_segment(class_id, kind);
                    if (!seg) {
                        return Allocation{0, 0, 0, 0, 0};
                    }
                }
                bit = seg->find_free_bit();
                if (bit < 0) {
                    // Should not happen
                    return Allocation{0, 0, 0, 0, 0};
                }
            }
            
            // Mark used: flip bit to 0
            const size_t w = size_t(bit) >> 6;
            const int b = bit & 63;
            seg->bm[w] &= ~(1ull << b);
            --seg->free_count;
            
            // Update used for consistency (though bitmap is authoritative)
            seg->used = (seg->blocks - seg->free_count) * class_sz;
            
            // Track if this is a reused block or fresh allocation
            uint32_t block_index = static_cast<uint32_t>(bit);
            bool is_reused = block_index < seg->max_allocated;
            
            // Update high water mark and stats based on allocation type
            if (!is_reused) {
                seg->max_allocated = block_index + 1;
                allocator.allocs_from_bump++;     // Fresh allocation
            } else {
                allocator.allocs_from_bitmap++;   // Reused a freed block
            }
            
            Allocation alloc;
            alloc.file_id = seg->file_id;
            alloc.segment_id = seg->segment_id;
            alloc.offset = seg->base_offset + uint64_t(bit) * class_sz;
            alloc.length = class_sz;
            alloc.class_id = class_id;
            
            // Don't create a new pin - the allocation will use the segment's base_vaddr
            // The pin in Allocation is not used for regular allocations (only for recovery)
            // Regular allocations rely on the segment's stable base_vaddr
            
            // Update stats
            allocator.live_bytes += class_sz;
            // Only decrement dead_bytes if we're actually reclaiming dead space
            if (allocator.dead_bytes >= class_sz) {
                allocator.dead_bytes -= class_sz;  // Reclaiming dead space
            }
            
            return alloc;
        }

        void SegmentAllocator::free(Allocation& a) {
            // Guard: block frees in read-only mode
            if (read_only_) {
                throw std::logic_error("Cannot free in read-only mode (serverless reader)");
            }

            // First, release the Pin (RAII will handle it, but be explicit)
            a.pin.reset();

            if (a.class_id >= NUM_CLASSES || a.length == 0) {
                return;  // Invalid allocation
            }
            
            const uint8_t cid = a.class_id;
            auto& allocator = allocators_[cid];
            std::lock_guard<std::mutex> lock(allocator.mu);
            
            // Track free operations
            allocator.total_frees++;
            
            // Find the segment
            Segment* seg = nullptr;
            for (auto& up : allocator.segments) {
                if (up->file_id == a.file_id && up->segment_id == a.segment_id) {
                    seg = up.get();
                    break;
                }
            }
            
            if (!seg) {
                // Unknown segment (corruption or race) - log warning
                return;
            }
            
            // Sanity checks for offset validity
            if (a.offset < seg->base_offset) {
                // Offset before segment base - corruption
                return;
            }
            
            const uint32_t class_sz = class_to_size(cid);
            const uint64_t offset_in_segment = a.offset - seg->base_offset;
            
            // Check alignment
            if (offset_in_segment % class_sz != 0) {
                // Misaligned offset - corruption
                return;
            }
            
            // Translate offset to block index and flip the bit back to 1
            const uint32_t bi = block_index_from_offset(seg->base_offset, a.offset, class_sz);
            if (bi >= seg->blocks) {
                // Out of bounds - log warning
                return;
            }
            
            const size_t w = size_t(bi) >> 6;
            const int b = bi & 63;
            uint64_t mask = (1ull << b);
            
            if (seg->bm[w] & mask) {
                // Double free detected - log warning and ignore
                return;
            } else {
                seg->bm[w] |= mask;
                ++seg->free_count;
                // Update used for consistency
                seg->used = (seg->blocks - seg->free_count) * class_sz;
                allocator.live_bytes -= a.length;
                allocator.dead_bytes += a.length;  // Track freed space as dead bytes
                allocator.frees_to_bitmap++;
            }
            
            // Optional: if seg->free_count == seg->blocks and seg != allocator.active_segment,
            // mark for compaction/retire later
        }
        
        // get_ptr is now inlined in the header for performance

        SegmentAllocator::Stats SegmentAllocator::get_stats(uint8_t class_id) const {
            if (class_id >= NUM_CLASSES) {
                return Stats{};
            }
            
            const auto& allocator = allocators_[class_id];
            std::lock_guard<std::mutex> lock(allocator.mu);
            
            Stats stats;
            stats.live_bytes = allocator.live_bytes;
            stats.dead_bytes = allocator.dead_bytes;
            stats.total_segments = allocator.segments.size();
            stats.active_segments = (allocator.active_segment != nullptr) ? 1 : 0;
            stats.allocs_from_freelist = allocator.allocs_from_freelist;
            stats.allocs_from_bump = allocator.allocs_from_bump;
            stats.allocs_from_bitmap = allocator.allocs_from_bitmap;
            stats.frees_to_bitmap = allocator.frees_to_bitmap;
            stats.total_allocations = allocator.total_allocations;
            stats.total_frees = allocator.total_frees;
            
            return stats;
        }
        
        SegmentAllocator::Stats SegmentAllocator::get_total_stats() const {
            Stats total;
            for (uint8_t i = 0; i < NUM_CLASSES; ++i) {
                Stats s = get_stats(i);
                total.live_bytes += s.live_bytes;
                total.dead_bytes += s.dead_bytes;
                total.total_segments += s.total_segments;
                total.active_segments += s.active_segments;
                total.allocs_from_freelist += s.allocs_from_freelist;
                total.allocs_from_bump += s.allocs_from_bump;
                total.allocs_from_bitmap += s.allocs_from_bitmap;
                total.frees_to_bitmap += s.frees_to_bitmap;
                total.total_allocations += s.total_allocations;
                total.total_frees += s.total_frees;
            }
            return total;
        }
        
        size_t SegmentAllocator::get_segment_count() const {
            size_t count = 0;
            for (uint8_t i = 0; i < NUM_CLASSES; ++i) {
                const auto& allocator = allocators_[i];
                std::lock_guard<std::mutex> lock(allocator.mu);
                count += allocator.segments.size();
            }
            return count;
        }
        
        size_t SegmentAllocator::get_active_segment_count() const {
            size_t count = 0;
            for (uint8_t i = 0; i < NUM_CLASSES; ++i) {
                const auto& allocator = allocators_[i];
                std::lock_guard<std::mutex> lock(allocator.mu);
                if (allocator.active_segment != nullptr) {
                    count++;
                }
            }
            return count;
        }
        
        void SegmentAllocator::close_all() {
            for (auto& ca : allocators_) {
                std::lock_guard<std::mutex> g(ca.create_mu);

                // Unpublish the O(1) table first so concurrent readers fail fast
                ca.seg_table_size.store(0, std::memory_order_release);
                auto* table = ca.seg_table_root.exchange(nullptr, std::memory_order_release);

                // Close all segments - IMPORTANT: release pins first to avoid
                // accessing MappingManager during static destructor phase
                for (auto& sp : ca.segments) {
                    if (sp) {
                        auto* s = sp.get();
                        // Explicitly release the pin BEFORE segment destruction
                        // This ensures MappingManager is accessed in controlled manner
                        {
                            std::lock_guard<std::mutex> seg_lock(s->remap_mutex);
                            s->pin.reset();  // Release pin - calls MappingManager::unpin
                            s->base_vaddr = nullptr;
                        }
                        // Clear the segment metadata
                        s->used = 0;
                        s->capacity = 0;
                        s->base_offset = 0;
                    }
                }
                ca.segments.clear();
                ca.free_list.clear();
                ca.active_segment = nullptr;

                // Delete the segment table
                if (table) {
                    delete[] table;
                }
            }
        }

        uint8_t SegmentAllocator::size_to_class(size_t sz) { 
            return persist::size_to_class(sz);
        }

        size_t SegmentAllocator::class_to_size(uint8_t c) { 
            return persist::class_to_size(c);
        }
        
        
        SegmentAllocator::Segment* SegmentAllocator::allocate_new_segment(uint8_t class_id, NodeKind kind) {
            auto& allocator = allocators_[class_id];
            
            // Called with allocator.mu held
            auto seg = std::make_unique<Segment>();
            
            // Align segment size to filesystem stripe boundary for better performance
            size_t aligned_segment_size = DEFAULT_SEGMENT_SIZE;
            if (segment::kSegmentAlignment > 0) {
                aligned_segment_size = ((DEFAULT_SEGMENT_SIZE + segment::kSegmentAlignment - 1) 
                                        / segment::kSegmentAlignment) * segment::kSegmentAlignment;
            }
            
            // Check if we need to rotate to a new file
            if (allocator.bytes_in_current_file + aligned_segment_size > config_.max_file_size) {
                // File would be too large, rotate to next file
                allocator.current_file_seq++;
                allocator.bytes_in_current_file = 0;
            }
            
            // Assign file and segment IDs based on configuration
            // Use bit 31 to distinguish file type: 0 = index (.xi), 1 = data (.xd)
            bool is_data_file = (kind == NodeKind::DataRecord || kind == NodeKind::ValueVec);
            uint32_t file_type_bit = is_data_file ? 0x80000000 : 0;
            
            uint32_t file_id;
            if (files::kFilePerSizeClass) {
                // Each class has its own file sequence
                // Bit 31: file type, Bits 30-24: class_id, Bits 23-0: per-class sequence
                file_id = file_type_bit | ((uint32_t(class_id) & 0x7F) << 24) | (allocator.current_file_seq & 0xFFFFFF);
            } else {
                // Global file sequence shared across all classes
                // Bit 31: file type, Bits 30-0: global sequence
                uint32_t seq = global_file_seq_.fetch_add(1) & 0x7FFFFFFF;
                file_id = file_type_bit | seq;
            }
            
            // Assign dense segment_id (0..N-1 within this class)
            const uint32_t class_segment_id = allocator.next_segment_id.fetch_add(1, std::memory_order_relaxed);
            
            seg->file_id = file_id;
            seg->segment_id = class_segment_id;  // Dense 0..N-1 within class
            seg->writable = !read_only_;  // For lazy remapping

            // Align base_offset to stripe boundary for better I/O
            if (segment::kSegmentAlignment > 0 && allocator.bytes_in_current_file > 0) {
                allocator.bytes_in_current_file = ((allocator.bytes_in_current_file + segment::kSegmentAlignment - 1) 
                                                   / segment::kSegmentAlignment) * segment::kSegmentAlignment;
            }
            
            seg->base_offset = allocator.bytes_in_current_file;  // Position within current file
            seg->capacity = aligned_segment_size;
            seg->used = 0;
            seg->class_id = class_id;
            
            // Initialize bitmap for O(1) allocation
            const uint32_t class_sz = class_to_size(class_id);
            seg->blocks = uint32_t(seg->capacity / class_sz);
            seg->free_count = seg->blocks;
            seg->max_allocated = 0;  // Fresh segment, nothing allocated yet
            seg->bm.assign((seg->blocks + 63) / 64, ~uint64_t{0});
            // Mask tail bits beyond seg->blocks as used (0)
            const uint32_t rem = seg->blocks & 63u;
            if (rem) {
                seg->bm.back() &= ((1ull << rem) - 1ull);
            }
            
            // Update bytes used in current file
            allocator.bytes_in_current_file += aligned_segment_size;
            
            // Get the data file path
            std::string file_path = get_data_file_path(seg->file_id);
            
            // Ensure file is large enough for this segment
            size_t required_size = seg->base_offset + seg->capacity;
            FSResult extend_result = ensure_file_size(file_path, required_size);
            if (!extend_result.ok) {
                // File extension failed - this is a critical error
                // Revert the bytes_in_current_file update since we're not using this segment
                allocator.bytes_in_current_file -= aligned_segment_size;
                // Return nullptr to indicate failure - caller must handle
                trace() << "Failed to extend file: " << file_path 
                          << " to size " << required_size 
                          << " (base_offset=" << seg->base_offset 
                          << ", capacity=" << seg->capacity 
                          << ", class_id=" << int(class_id)
                          << ", segment_id=" << seg->segment_id
                          << ", file_id=" << seg->file_id << ")" << std::endl;
                return nullptr;
            }
            
            // NEW: Use MappingManager to map the segment and get stable base_vaddr
            try {
                // Use canonical file path (same as ensure_file_size above)
                std::string data_path = get_data_file_path(seg->file_id);
                
                // Pin the segment memory - writable for new allocations
                seg->pin = mapping_manager_->pin(data_path,
                                                static_cast<size_t>(seg->base_offset),
                                                static_cast<size_t>(seg->capacity),
                                                /*writable=*/true);
                
                if (!seg->pin.get()) {
                    // Mapping failed
                    allocator.bytes_in_current_file -= aligned_segment_size;
                    trace() << "Failed to map segment: " << data_path 
                              << " at offset " << seg->base_offset
                              << " with capacity " << seg->capacity << std::endl;
                    return nullptr;
                }
                
                // Store stable virtual address
                seg->base_vaddr = seg->pin.get();
                
            } catch (const std::exception& e) {
                // Failed to map segment - this is critical
                allocator.bytes_in_current_file -= aligned_segment_size;
                trace() << "Failed to map segment: " << file_path 
                          << " at offset " << seg->base_offset
                          << " with capacity " << seg->capacity
                          << ": " << e.what() << std::endl;
                return nullptr;
            }
            
            // Prefetching now happens in MappingManager when memory is actually accessed
            
            // Ensure seg_table array is large enough
            const size_t current_size = allocator.seg_table_size.load(std::memory_order_relaxed);
            if (seg->segment_id >= current_size) {
                // Grow the table - safe because we hold allocator.mu
                size_t new_capacity = std::max<size_t>(
                    current_size * 2, 
                    static_cast<size_t>(seg->segment_id + 32));  // Some headroom
                
                // Allocate new table
                auto* new_table = new std::atomic<Segment*>[new_capacity];
                auto* old_table = allocator.seg_table_root.load(std::memory_order_relaxed);
                
                // Copy existing pointers with relaxed loads/stores
                for (size_t i = 0; i < current_size; ++i) {
                    new_table[i].store(old_table[i].load(std::memory_order_relaxed),
                                      std::memory_order_relaxed);
                }
                
                // Initialize new slots to nullptr
                for (size_t i = current_size; i < new_capacity; ++i) {
                    new_table[i].store(nullptr, std::memory_order_relaxed);
                }
                
                // CRITICAL: Publish new table root first, then size
                // This ordering ensures readers see a valid table for any valid index
                allocator.seg_table_root.store(new_table, std::memory_order_release);
                allocator.seg_table_size.store(new_capacity, std::memory_order_release);
                
                // Retire old table safely (avoid UAF with lock-free readers)
                if (old_table) {
                    allocator.retired_tables.push_back(old_table);
                }
            }
            
            // Get current table and publish the segment pointer
            auto* table = allocator.seg_table_root.load(std::memory_order_relaxed);
            Segment* ptr = seg.get();
            table[seg->segment_id].store(ptr, std::memory_order_release);
            
            allocator.segments.push_back(std::move(seg));
            allocator.active_segment = ptr;
            
            return ptr;
        }
        
        std::string SegmentAllocator::get_data_file_path(uint32_t file_id) const {
            std::ostringstream oss;
            oss << data_dir_ << "/";
            
            // Determine file type based on file_id encoding
            // We'll use bit 31 to distinguish: 0 = index (.xi), 1 = data (.xd)
            bool is_data_file = (file_id & 0x80000000) != 0;
            
            if (is_data_file) {
                oss << files::kDataPrefix;  // "xtree_data"
            } else {
                oss << files::kIndexPrefix; // "xtree"
            }
            
            if (files::kFilePerSizeClass) {
                // Encode class ID in filename for better locality
                // Format: xtree_c<class>_<file_id>.<ext>
                // This keeps same-size allocations in the same file
                uint8_t class_id = (file_id >> 24) & 0x7F;  // Top 7 bits for class (bit 31 is file type)
                uint32_t seq = file_id & 0xFFFFFF; // Bottom 24 bits for sequence
                oss << "_c" << int(class_id) << "_" << seq;
            } else {
                // Simple sequential numbering
                uint32_t seq = file_id & 0x7FFFFFFF; // Mask off file type bit
                oss << "_" << seq;
            }
            
            // Use appropriate extension
            if (is_data_file) {
                oss << files::kDataExtension;   // ".xd"
            } else {
                oss << files::kIndexExtension;  // ".xi"
            }
            
            return oss.str();
        }
        
        FSResult SegmentAllocator::ensure_file_size(const std::string& path, size_t min_size) {
            // Check current file size
            auto [result, current_size] = PlatformFS::file_size(path);
            
            if (!result.ok || current_size < min_size) {
                // Need to create or extend the file
                // First try to preallocate
                FSResult prealloc_result = PlatformFS::preallocate(path, min_size);
                if (prealloc_result.ok) {
                    // Verify the size immediately to catch sparse-file oddities
                    auto [verify_result, new_size] = PlatformFS::file_size(path);
                    if (!verify_result.ok || new_size < min_size) {
                        // Preallocate reported success but size is wrong, fall through to manual
                    } else {
                        return prealloc_result;
                    }
                }
                
                // Fall back to manual extension
                std::ofstream file(path, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);
                if (!file) {
                    // Could not open file
                    trace() << "Failed to open file " << path << " for extension: " << strerror(errno) << std::endl;
                    return FSResult{false, errno};
                }
                
                // Seek to desired size and write a byte to extend
                file.seekp(min_size - 1);
                if (!file.good()) {
                    trace() << "Failed to seek in file " << path << " to position " << (min_size - 1) 
                              << ": " << strerror(errno) << std::endl;
                    return FSResult{false, errno};
                }
                
                file.write("\0", 1);
                if (!file.good()) {
                    trace() << "Failed to write to file " << path << " at position " << (min_size - 1)
                              << ": " << strerror(errno) << std::endl;
                    return FSResult{false, errno};
                }
                
                // Flush the stream
                file.flush();
                
                // Close the file (this may trigger an implicit flush)
                file.close();
                
                // For durability, open the file again to fsync it
                #ifndef _WIN32
                    int fd = ::open(path.c_str(), O_RDWR);
                    if (fd >= 0) {
                        ::fsync(fd);
                        ::close(fd);
                    }
                #else
                    HANDLE hFile = CreateFileA(path.c_str(), 
                                             GENERIC_WRITE,
                                             FILE_SHARE_READ | FILE_SHARE_WRITE,
                                             NULL,
                                             OPEN_EXISTING,
                                             FILE_ATTRIBUTE_NORMAL,
                                             NULL);
                    if (hFile != INVALID_HANDLE_VALUE) {
                        FlushFileBuffers(hFile);
                        CloseHandle(hFile);
                    }
                #endif
                
                // Fsync the containing directory to ensure size change survives crash
                size_t last_slash = path.find_last_of("/\\");
                if (last_slash != std::string::npos) {
                    std::string dir_path = path.substr(0, last_slash);
                    PlatformFS::fsync_directory(dir_path);
                }
                
                // Verify the extension worked
                auto [verify_result, new_size] = PlatformFS::file_size(path);
                if (!verify_result.ok || new_size < min_size) {
                    trace() << "File extension verification failed for " << path 
                              << ": expected size=" << min_size << ", actual size=" << new_size 
                              << ", verify_ok=" << verify_result.ok << std::endl;
                    return FSResult{false, errno};
                }
            }
            
            return FSResult{true, 0};
        }
        
        // Helper: Ensure segment table can address the given segment_id
        void SegmentAllocator::ensure_seg_table_capacity_locked(ClassAllocator& ca, size_t min_capacity) {
            // Must be called under ca.create_mu lock
            
            size_t current_cap = ca.seg_table_size.load(std::memory_order_relaxed);
            if (current_cap >= min_capacity) {
                return; // Already sufficient
            }
            
            // Grow to next power of 2 that fits min_capacity
            size_t new_cap = current_cap ? current_cap : ClassAllocator::kInitialSegments;
            while (new_cap < min_capacity) {
                new_cap *= 2;
            }
            
            // Allocate new table
            auto* new_table = new std::atomic<Segment*>[new_cap];
            auto* old_table = ca.seg_table_root.load(std::memory_order_relaxed);
            
            // Initialize all entries to nullptr
            for (size_t i = 0; i < new_cap; ++i) {
                if (i < current_cap && old_table) {
                    // Copy existing entries
                    new_table[i].store(old_table[i].load(std::memory_order_relaxed), 
                                      std::memory_order_relaxed);
                } else {
                    // New entries start as nullptr
                    new_table[i].store(nullptr, std::memory_order_relaxed);
                }
            }
            
            // Publish new table atomically
            ca.seg_table_root.store(new_table, std::memory_order_release);
            ca.seg_table_size.store(new_cap, std::memory_order_release);
            
            // Retire old table safely (avoid UAF with lock-free readers)
            if (old_table) {
                ca.retired_tables.push_back(old_table);
            }
        }
        
        // Helper: Map a segment for recovery (must be called under lock)
        std::unique_ptr<SegmentAllocator::Segment> SegmentAllocator::map_segment_for_recovery_locked(
            uint8_t class_id, 
            uint32_t file_id, 
            uint32_t segment_id,
            uint64_t offset) 
        {
            // IMPORTANT ASSUMPTION: Segment geometry is derived from dense layout
            // This assumes segments are allocated at fixed intervals (kDefaultSegmentSize)
            // 
            // If allocate_new_segment() uses a different strategy (e.g., dynamic placement
            // based on bytes_in_current_file + alignment), this derivation will be WRONG.
            // 
            // PRODUCTION FIX: Persist actual segment metadata (base_offset, capacity) in
            // the manifest/OT and retrieve it here instead of deriving from segment_id.
            //
            // Current dense layout assumption:
            //   segment N starts at: N * kDefaultSegmentSize
            //   capacity is always: kDefaultSegmentSize
            
            const uint32_t class_size = class_to_size(class_id);
            // CRITICAL: Use aligned segment size consistent with allocate_new_segment
            size_t aligned_seg_size = segment::kDefaultSegmentSize;
            if (segment::kSegmentAlignment > 0) {
                aligned_seg_size = ((segment::kDefaultSegmentSize + segment::kSegmentAlignment - 1)
                                    / segment::kSegmentAlignment) * segment::kSegmentAlignment;
            }
            const uint64_t base_offset = (offset / aligned_seg_size) * aligned_seg_size;
            const size_t capacity = aligned_seg_size;
            
            // NEW: Use MappingManager to map segment for recovery
            // IMPORTANT: Use writable=true for WAL replay writes
            std::unique_ptr<Segment> seg;
            
            try {
                // Use canonical file path (same as allocate_new_segment)
                std::string file_path = get_data_file_path(file_id);

                // Debug: trace recovery segment mapping
                trace() << "[RECOVERY_MAP] class_id=" << int(class_id)
                          << " file_id=" << file_id
                          << " segment_id=" << segment_id
                          << " offset=" << offset
                          << " base_offset=" << base_offset
                          << " file=" << file_path
                          << std::endl;

                // Pin the segment memory - writable for WAL replay
                auto pin = mapping_manager_->pin(file_path,
                                                static_cast<size_t>(base_offset),
                                                static_cast<size_t>(capacity),
                                                /*writable=*/true);

                if (!pin.get()) {
                    // Mapping failed - file doesn't exist yet
                    trace() << "[RECOVERY_MAP_FAIL] file=" << file_path
                              << " offset=" << base_offset
                              << " capacity=" << capacity << std::endl;
                    return nullptr;
                }
                
                // Create segment object
                seg = std::make_unique<Segment>();
                seg->file_id = file_id;
                seg->segment_id = segment_id;
                seg->class_id = class_id;
                seg->base_offset = base_offset;
                seg->capacity = capacity;
                seg->writable = !read_only_;  // For lazy remapping
                seg->base_vaddr = pin.get();  // Store stable virtual address
                seg->pin = std::move(pin);      // Keep pin alive (by value)
                seg->used = 0; // Will be updated as we replay allocations
                
                // Calculate blocks and initialize bitmap
                seg->blocks = capacity / class_size;
                size_t bm_words = (seg->blocks + 63) >> 6;
                seg->bm.resize(bm_words, ~0ull); // All blocks initially free
                seg->free_count = seg->blocks;
                seg->max_allocated = 0;  // Will be updated during WAL replay
            } catch (const std::exception& e) {
                // Failed to map segment - file doesn't exist
                return nullptr;
            }
            
            // Also update the segment in the seg_table for O(1) future lookups
            // (caller will do this after ensuring capacity)
            
            return seg;
        }
        
        // Note: The O(1) get_ptr_for_recovery is now inlined in the header file
        
        SegmentAllocator::SegmentUtilization SegmentAllocator::get_segment_utilization() const {
            SegmentUtilization util;
            
            // Iterate through all allocators and their segments
            for (const auto& allocator : allocators_) {
                std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(allocator.mu));
                
                for (const auto& seg : allocator.segments) {
                    util.total_segments++;
                    util.total_capacity += seg->capacity;
                    util.total_used += seg->used;
                    util.total_wasted += seg->wasted_bytes();
                    
                    double segment_util = seg->utilization();
                    
                    // Update min/max
                    if (segment_util < util.min_utilization) {
                        util.min_utilization = segment_util;
                    }
                    if (segment_util > util.max_utilization) {
                        util.max_utilization = segment_util;
                    }
                    
                    // Count segments by utilization brackets
                    if (segment_util < 25.0) {
                        util.segments_under_25_percent++;
                    } else if (segment_util < 50.0) {
                        util.segments_under_50_percent++;
                    } else if (segment_util < 75.0) {
                        util.segments_under_75_percent++;
                    }
                }
            }
            
            // Calculate average utilization
            if (util.total_capacity > 0) {
                util.avg_utilization = (static_cast<double>(util.total_used) * 100.0) / util.total_capacity;
            }
            
            return util;
        }
        
        std::string SegmentAllocator::get_file_path(uint32_t file_id, bool is_data_file) const {
            // Reconstruct the file path from file_id and type
            std::ostringstream oss;
            oss << data_dir_ << "/";
            
            if (is_data_file) {
                oss << files::kDataPrefix;  // "xtree_data"
            } else {
                oss << files::kIndexPrefix; // "xtree"
            }
            
            // Extract the actual file number (without the data file bit)
            uint32_t file_num = file_id & 0x7FFFFFFF;
            
            if (files::kFilePerSizeClass) {
                // Extract class ID from file_id encoding if applicable
                // For now, just use file number
                oss << "_" << file_num;
            } else {
                oss << "_" << file_num;
            }
            
            if (is_data_file) {
                oss << files::kDataExtension;  // ".xd"
            } else {
                oss << files::kIndexExtension; // ".xi"
            }
            
            return oss.str();
        }

        // ============================================================================
        // Lazy Remapping Support
        // ============================================================================

        void SegmentAllocator::ensure_segment_mapped(Segment* seg) {
            // Thread-safe remapping using segment's mutex
            std::lock_guard<std::mutex> lock(seg->remap_mutex);

            // Double-check after acquiring lock (another thread may have remapped)
            if (seg->pin) {
                return;  // Already mapped
            }

            // Compute file path from file_id
            std::string file_path = get_data_file_path(seg->file_id);

            // Remap the segment using MappingManager
            seg->pin = mapping_manager_->pin(file_path,
                                             static_cast<size_t>(seg->base_offset),
                                             static_cast<size_t>(seg->capacity),
                                             seg->writable);

            if (seg->pin) {
                seg->base_vaddr = seg->pin.get();
                trace() << "[LAZY_REMAP] Remapped segment class=" << int(seg->class_id)
                        << " file=" << seg->file_id
                        << " seg=" << seg->segment_id
                        << " offset=" << seg->base_offset
                        << std::endl;
            } else {
                seg->base_vaddr = nullptr;
                trace() << "[LAZY_REMAP] FAILED to remap segment class=" << int(seg->class_id)
                        << " file=" << seg->file_id
                        << std::endl;
            }
        }

        size_t SegmentAllocator::release_cold_pins(uint64_t threshold_ns) {
            using namespace std::chrono;
            const uint64_t now_ns = duration_cast<nanoseconds>(
                high_resolution_clock::now().time_since_epoch()).count();

            size_t released = 0;

            for (auto& allocator : allocators_) {
                // Take the class allocator lock to iterate segments safely
                std::lock_guard<std::mutex> ca_lock(allocator.mu);

                for (auto& seg_ptr : allocator.segments) {
                    Segment* seg = seg_ptr.get();
                    if (!seg || !seg->pin) continue;  // Already released or invalid

                    // Check if segment hasn't been accessed recently
                    uint64_t last_access = seg->last_access_ns.load(std::memory_order_relaxed);
                    if (last_access == 0) {
                        // Never accessed via get_ptr - initialize to now and skip
                        seg->last_access_ns.store(now_ns, std::memory_order_relaxed);
                        continue;
                    }

                    if (now_ns - last_access > threshold_ns) {
                        // Segment is cold - release its pin
                        std::lock_guard<std::mutex> seg_lock(seg->remap_mutex);

                        // Double-check pin is still valid after lock
                        if (seg->pin) {
                            seg->pin.reset();
                            seg->base_vaddr = nullptr;
                            released++;

                            trace() << "[LAZY_REMAP] Released cold pin class=" << int(seg->class_id)
                                    << " file=" << seg->file_id
                                    << " seg=" << seg->segment_id
                                    << " age_ms=" << ((now_ns - last_access) / 1000000)
                                    << std::endl;
                        }
                    }
                }
            }

            if (released > 0) {
                trace() << "[LAZY_REMAP] Released " << released << " cold segment pins"
                        << std::endl;
            }

            return released;
        }

        size_t SegmentAllocator::get_pinned_segment_count() const {
            size_t count = 0;

            for (const auto& allocator : allocators_) {
                std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(allocator.mu));

                for (const auto& seg_ptr : allocator.segments) {
                    if (seg_ptr && seg_ptr->pin) {
                        count++;
                    }
                }
            }

            return count;
        }

    } // namespace persist
} // namespace xtree