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

#include "superblock.hpp"
#include "platform_fs.h"
#include "checksums.h"
#include <atomic>
#include <filesystem>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstddef>
#include <thread>
#include <cassert>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

namespace xtree { 
    namespace persist {

        Superblock::Superblock(const std::string& meta_path) : path_(meta_path) {
            // Create or map the superblock file
            std::filesystem::path sb_path(meta_path);
            std::filesystem::path parent = sb_path.parent_path();
            
            // Ensure parent directory exists
            if (!parent.empty()) {
                std::error_code ec;
                std::filesystem::create_directories(parent, ec);
                if (ec) {
                    // Failed to create directory - leave sb_ as nullptr
                    return;
                }
            }
            
            // Check if file exists
            bool exists = std::filesystem::exists(sb_path);
            size_t required_size = sizeof(SuperblockOnDisk);
            
            if (!exists) {
                // Create new superblock file
                #ifndef _WIN32
                int fd = ::open(meta_path.c_str(), O_RDWR | O_CREAT, 0644);
                if (fd >= 0) {
                    // Extend file to required size
                    ::ftruncate(fd, required_size);
                    ::close(fd);
                }
                #else
                HANDLE hFile = CreateFileA(meta_path.c_str(),
                                         GENERIC_READ | GENERIC_WRITE,
                                         FILE_SHARE_READ,
                                         NULL,
                                         CREATE_NEW,
                                         FILE_ATTRIBUTE_NORMAL,
                                         NULL);
                if (hFile != INVALID_HANDLE_VALUE) {
                    LARGE_INTEGER size;
                    size.QuadPart = required_size;
                    SetFilePointerEx(hFile, size, NULL, FILE_BEGIN);
                    SetEndOfFile(hFile);
                    CloseHandle(hFile);
                }
                #endif
            } else {
                // Check existing file size and fix if wrong
                std::error_code ec;
                auto current_size = std::filesystem::file_size(sb_path, ec);
                if (!ec && current_size != required_size) {
                    // File exists but wrong size - fix it
                    #ifndef _WIN32
                    int fd = ::open(meta_path.c_str(), O_RDWR);
                    if (fd >= 0) {
                        ::ftruncate(fd, required_size);
                        ::close(fd);
                    }
                    #else
                    HANDLE hFile = CreateFileA(meta_path.c_str(),
                                             GENERIC_WRITE,
                                             FILE_SHARE_READ,
                                             NULL,
                                             OPEN_EXISTING,
                                             FILE_ATTRIBUTE_NORMAL,
                                             NULL);
                    if (hFile != INVALID_HANDLE_VALUE) {
                        LARGE_INTEGER size;
                        size.QuadPart = required_size;
                        SetFilePointerEx(hFile, size, NULL, FILE_BEGIN);
                        SetEndOfFile(hFile);
                        CloseHandle(hFile);
                    }
                    #endif
                }
            }
            
            // Map the file
            FSResult res = PlatformFS::map_file(meta_path, 0, required_size, 
                                               MapMode::ReadWrite, &region_);
            if (res.ok && region_.addr) {
                sb_ = reinterpret_cast<SuperblockOnDisk*>(region_.addr);
                
                // Runtime alignment sanity checks (should always pass with your padding)
#ifndef NDEBUG
                auto aligned8 = [](const void* p){ return (reinterpret_cast<uintptr_t>(p) & 7u) == 0; };
                assert(aligned8(&sb_->root_id)      && "root_id not 8B aligned");
                assert(aligned8(&sb_->commit_epoch) && "commit_epoch not 8B aligned");
                assert(aligned8(&sb_->generation)   && "generation not 8B aligned");
                assert(aligned8(&sb_->created_unix) && "created_unix not 8B aligned");
#endif

                // Initialize if new - but don't set magic until first publish
                if (!exists) {
                    std::memset(sb_, 0, sizeof(SuperblockOnDisk));
                    sb_->version = SUPERBLOCK_VERSION;
                    sb_->header_size = sizeof(SuperblockOnDisk);
                    sb_->generation = 1;
                    sb_->created_unix = std::time(nullptr);
                    sb_->header_crc32c = 0; // Will be computed on publish

                    // Initialize seqlock/fields via atomic operations to be explicit
                    // With explicit padding, these fields are guaranteed to be properly aligned
                    std::atomic<uint32_t>* aseq = reinterpret_cast<std::atomic<uint32_t>*>(&sb_->seq);
                    std::atomic<uint64_t>* aroot = reinterpret_cast<std::atomic<uint64_t>*>(&sb_->root_id);
                    std::atomic<uint64_t>* aepoch = reinterpret_cast<std::atomic<uint64_t>*>(&sb_->commit_epoch);

                    aseq->store(0u, std::memory_order_relaxed);  // Even = consistent
                    aroot->store(0ull, std::memory_order_relaxed);
                    aepoch->store(0ull, std::memory_order_relaxed);

                    // Don't set magic yet - will be set on first publish
                }
                
            } else {
                // Failed to map file
            }
        }

        Superblock::~Superblock() {
            if (region_.addr) {
                PlatformFS::unmap(region_);
            }
        }

        Superblock::Snapshot Superblock::load() const {
            // Return invalid snapshot if not mapped or not valid
            if (!sb_ || sb_->magic != SUPERBLOCK_MAGIC) {
                return Snapshot{ NodeID::invalid(), 0 };
            }

            // Bind atomic pointers for seqlock read (const-cast is OK for reading)
            std::atomic<uint32_t>* aseq = reinterpret_cast<std::atomic<uint32_t>*>(const_cast<uint32_t*>(&sb_->seq));
            std::atomic<uint64_t>* aroot = reinterpret_cast<std::atomic<uint64_t>*>(const_cast<uint64_t*>(&sb_->root_id));
            std::atomic<uint64_t>* aepoch = reinterpret_cast<std::atomic<uint64_t>*>(const_cast<uint64_t*>(&sb_->commit_epoch));
            
            // Seqlock pattern to prevent torn reads
            uint32_t seq1, seq2;
            Snapshot s;
            
            do {
                // Read sequence number
                seq1 = aseq->load(std::memory_order_acquire);

                // If odd, writer is in progress - retry
                if (seq1 & 1u) {
                    std::this_thread::yield();
                    continue;
                }

                // Read the data
                s.root = NodeID::from_raw(aroot->load(std::memory_order_acquire));
                s.epoch = aepoch->load(std::memory_order_acquire);

                // Read sequence again
                seq2 = aseq->load(std::memory_order_acquire);

                // If sequences don't match or seq2 is odd, retry
            } while (seq1 != seq2 || (seq2 & 1));

            return s;
        }

        void Superblock::publish(NodeID new_root, uint64_t new_epoch) {
            if (!sb_) return; // Cannot publish without mapped superblock
            
            // Bind atomic pointers for seqlock write
            std::atomic<uint32_t>* aseq = reinterpret_cast<std::atomic<uint32_t>*>(&sb_->seq);
            std::atomic<uint64_t>* aroot = reinterpret_cast<std::atomic<uint64_t>*>(&sb_->root_id);
            std::atomic<uint64_t>* aepoch = reinterpret_cast<std::atomic<uint64_t>*>(&sb_->commit_epoch);

            // Seqlock pattern: increment to odd (writer in progress)
            uint32_t seq = aseq->load(std::memory_order_relaxed);
            aseq->store(seq + 1, std::memory_order_release);

            // Update all payload fields
            aroot->store(new_root.raw(), std::memory_order_relaxed);
            aepoch->store(new_epoch,     std::memory_order_relaxed);
            
            // Bump generation on every publish
            sb_->generation++; 
            
            // Set magic if first publish (ensures magic means fully written)
            if (sb_->magic != SUPERBLOCK_MAGIC) {
                sb_->magic = SUPERBLOCK_MAGIC;
            }
            
            // Compute CRC over the header (excluding the CRC field itself AND seq field)
            // This ensures CRC is stable for readers regardless of seq value
            sb_->header_crc32c = 0;  // Clear before computing
            CRC32C crc;
            
            // Update CRC for data before seq field
            crc.update(sb_, offsetof(SuperblockOnDisk, seq));
            
            // Skip seq field (4 bytes) - use the even value for consistency
            const uint32_t even_seq = seq + 2u;
            crc.update(&even_seq, sizeof(even_seq));
            
            // Continue from after seq to before header_crc32c
            const size_t after_seq = offsetof(SuperblockOnDisk, seq) + sizeof(sb_->seq);
            if (after_seq < offsetof(SuperblockOnDisk, header_crc32c)) {
                crc.update(reinterpret_cast<uint8_t*>(sb_) + after_seq,
                          offsetof(SuperblockOnDisk, header_crc32c) - after_seq);
            }
            
            // Skip the header_crc32c field (4 bytes of zeros)
            uint8_t zeros[4] = {0};
            crc.update(zeros, sizeof(zeros));
            
            // Continue with remaining data after header_crc32c
            const size_t after_crc = offsetof(SuperblockOnDisk, header_crc32c) + sizeof(sb_->header_crc32c);
            if (after_crc < sizeof(SuperblockOnDisk)) {
                crc.update(reinterpret_cast<uint8_t*>(sb_) + after_crc, 
                          sizeof(SuperblockOnDisk) - after_crc);
            }
            sb_->header_crc32c = crc.finalize();
            
            // End write: make it visible atomically to readers (even = consistent)
            aseq->store(seq + 2u, std::memory_order_release);
            
            // Ensure all data including seq is durable
            PlatformFS::flush_view(region_.addr, region_.size);
            
            // Final file-level durability
            #ifndef _WIN32
            if (region_.file_handle >= 0) {
                PlatformFS::flush_file(region_.file_handle);
            }
            #else
            if (region_.file_handle && region_.file_handle != INVALID_HANDLE_VALUE) {
                PlatformFS::flush_file(region_.file_handle);
            }
            #endif
            
            // Fsync parent directory for rename/link durability (only if using A/B superblocks)
            // Note: Not needed for single superblock file that's always mapped
        }

    } // namespace persist
} // namespace xtree