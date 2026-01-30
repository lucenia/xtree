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

#include "ot_delta_log.h"
#include "platform_fs.h"
#include "checksums.h"
#include "../util/endian.hpp"
#include <fstream>
#include <cstring>
#include <cstddef>
#include <thread>
#include <sys/stat.h>
#include <chrono>
#ifdef DEBUG
#include "../util/log.h"
#endif

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#endif

namespace xtree { 
    namespace persist {
        
        using xtree::persist::crc32c;
        using namespace xtree::util;  // For endian helpers (store_le*, load_le*)
        
        // Compile-time check that wire size matches our expectation
        static_assert(kWireRecSize == 52, "Wire format size mismatch");
        
        // Serialize OTDeltaRec to portable wire format
        static void serialize_delta_rec(uint8_t* buf, const OTDeltaRec& rec) {
            // Layout: handle_idx(8) | tag(2) | class_id(1) | kind(1) |
            //         file_id(4) | segment_id(4) | offset(8) | length(4) |
            //         data_crc32c(4) | birth_epoch(8) | retire_epoch(8)
            store_le64(buf, rec.handle_idx);       buf += 8;
            store_le16(buf, rec.tag);              buf += 2;
            *buf++ = rec.class_id;
            *buf++ = rec.kind;
            store_le32(buf, rec.file_id);          buf += 4;
            store_le32(buf, rec.segment_id);       buf += 4;
            store_le64(buf, rec.offset);           buf += 8;
            store_le32(buf, rec.length);           buf += 4;
            store_le32(buf, rec.data_crc32c);      buf += 4;
            store_le64(buf, rec.birth_epoch);      buf += 8;
            store_le64(buf, rec.retire_epoch);     // buf += 8;
        }
        
        // Deserialize OTDeltaRec from portable wire format
        static void deserialize_delta_rec(OTDeltaRec& rec, const uint8_t* buf) {
            rec.handle_idx = load_le64(buf);       buf += 8;
            rec.tag = load_le16(buf);              buf += 2;
            rec.class_id = *buf++;
            rec.kind = *buf++;
            rec.file_id = load_le32(buf);          buf += 4;
            rec.segment_id = load_le32(buf);       buf += 4;
            rec.offset = load_le64(buf);           buf += 8;
            rec.length = load_le32(buf);           buf += 4;
            rec.data_crc32c = load_le32(buf);      buf += 4;
            rec.birth_epoch = load_le64(buf);      buf += 8;
            rec.retire_epoch = load_le64(buf);     // buf += 8;
        }

        OTDeltaLog::OTDeltaLog(const std::string& path, size_t prealloc_chunk, uint64_t sequence) 
            : path_(path), end_offset_(0), closing_(false), in_flight_appends_(0),
              prealloc_chunk_(prealloc_chunk), max_epoch_(0), sequence_(sequence) {
#ifdef _WIN32
            fd_ = INVALID_HANDLE_VALUE;
#else
            fd_ = -1;
#endif
#ifdef DEBUG
            closed_ = false;  // Initialize for debug builds
#endif
            // Set creation time to current time
            auto now = std::chrono::system_clock::now();
            auto duration = now.time_since_epoch();
            auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
            created_sec_.store(static_cast<uint64_t>(seconds), std::memory_order_relaxed);
            
            // Open immediately - fail fast if we can't open
            if (!open_for_append()) {
                throw std::runtime_error("Failed to open delta log: " + path);
            }
        }
        
        OTDeltaLog::~OTDeltaLog() {
            close();
        }
        
        bool OTDeltaLog::open_for_append() {
#ifdef DEBUG
            // Reset closed flag when reopening
            closed_ = false;
#endif
            
            std::lock_guard<std::mutex> lk(open_close_mu_);
#ifdef _WIN32
            if (fd_ != INVALID_HANDLE_VALUE) {
                return true; // Already open
            }
            
            // Open/create file for writing
            fd_ = CreateFileA(path_.c_str(),
                            GENERIC_WRITE,
                            FILE_SHARE_READ | FILE_SHARE_WRITE,  // Allow concurrent reads and writes
                            nullptr,
                            OPEN_ALWAYS,      // Create if doesn't exist
                            FILE_ATTRIBUTE_NORMAL,
                            nullptr);
            if (fd_ == INVALID_HANDLE_VALUE) {
                return false;
            }
            
            // Get current file size
            LARGE_INTEGER size;
            if (!GetFileSizeEx(fd_, &size)) {
                CloseHandle(fd_);
                fd_ = INVALID_HANDLE_VALUE;
                return false;
            }
            end_offset_.store(size.QuadPart, std::memory_order_relaxed);
            
            // Preallocate in chunks to reduce fragmentation
            const int64_t chunk = static_cast<int64_t>(prealloc_chunk_);
            if (size.QuadPart % chunk < chunk / 2) {
                // We're in the first half of a chunk, preallocate the next chunk
                LARGE_INTEGER new_size;
                new_size.QuadPart = ((size.QuadPart / chunk) + 1) * chunk;
                
                // SetFileInformationByHandle for efficient preallocation
                FILE_ALLOCATION_INFO alloc_info;
                alloc_info.AllocationSize = new_size;
                SetFileInformationByHandle(fd_, FileAllocationInfo, &alloc_info, sizeof(alloc_info));
                // Note: Ignore errors - preallocation is best-effort optimization
            }
#else
            if (fd_ >= 0) {
                return true; // Already open
            }
            
            // Open without O_APPEND since we'll use pwrite with explicit offsets
            // O_CREAT to create if doesn't exist
            // O_WRONLY since we're only appending
            // O_CLOEXEC to prevent fd leaks to child processes
            fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
            if (fd_ < 0) {
                return false;
            }
            
            // Get current file size for the atomic end_offset
            off_t end = ::lseek(fd_, 0, SEEK_END);
            if (end < 0) {
                ::close(fd_);
                fd_ = -1;
                return false;
            }
            end_offset_.store(end, std::memory_order_relaxed);
            
            // Preallocate in chunks to reduce fragmentation
            const off_t chunk = static_cast<off_t>(prealloc_chunk_);
            if (end % chunk < chunk / 2) {
                // We're in the first half of a chunk, preallocate the next chunk
                off_t new_size = ((end / chunk) + 1) * chunk;
                
#ifdef __linux__
                // Linux: Use posix_fallocate for efficient preallocation
                posix_fallocate(fd_, 0, new_size);
                // Note: Ignore errors - preallocation is best-effort optimization
#elif defined(__APPLE__)
                // macOS: Use F_PREALLOCATE
                fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, new_size - end, 0};
                if (fcntl(fd_, F_PREALLOCATE, &store) == -1) {
                    // Try non-contiguous if contiguous fails
                    store.fst_flags = F_ALLOCATEALL;
                    fcntl(fd_, F_PREALLOCATE, &store);
                }
                // Note: Ignore errors - preallocation is best-effort optimization
#endif
            }
#endif
            
            return true;
        }
        
        void OTDeltaLog::prepare_close() {
            // Set closing flag to prevent new appends
            closing_.store(true, std::memory_order_release);
            
            // Wait for in-flight appends to complete efficiently
            std::unique_lock<std::mutex> lock(close_wait_mu_);
            close_wait_cv_.wait(lock, [this] {
                return in_flight_appends_.load(std::memory_order_acquire) == 0;
            });
        }
        
        void OTDeltaLog::close() {
#ifdef DEBUG
            // Assert we're not closing twice
            if (closed_) {
                error() << "OTDeltaLog::close() called on already closed log!";
                std::abort();
            }
#endif
            
            std::lock_guard<std::mutex> lk(open_close_mu_);
#ifdef _WIN32
            if (fd_ != INVALID_HANDLE_VALUE) {
                CloseHandle(fd_);
                fd_ = INVALID_HANDLE_VALUE;
            }
#else
            if (fd_ >= 0) {
                ::close(fd_);
                fd_ = -1;
            }
#endif
            end_offset_.store(0, std::memory_order_relaxed);
            closing_.store(false, std::memory_order_release);  // Reset for potential reopen
            
#ifdef DEBUG
            closed_ = true;  // Mark as closed in debug builds
#endif
        }
        
        // Helper to handle short writes and EINTR
#ifdef _WIN32
        static bool pwrite_all(HANDLE fd, const void* buf, size_t len, uint64_t offset) {
            const uint8_t* p = static_cast<const uint8_t*>(buf);
            size_t remaining = len;
            
            // Windows WriteFile uses DWORD, so chunk large writes
            constexpr DWORD MAX_CHUNK = 32 * 1024 * 1024;  // 32MB chunks
            
            while (remaining > 0) {
                DWORD chunk_size = (remaining > MAX_CHUNK) ? MAX_CHUNK : static_cast<DWORD>(remaining);
                
                OVERLAPPED overlapped = {};
                overlapped.Offset = static_cast<DWORD>(offset);
                overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
                
                DWORD written = 0;
                if (!WriteFile(fd, p, chunk_size, &written, &overlapped)) {
                    return false;
                }
                if (written == 0) {
                    return false;  // Shouldn't happen, but defensive
                }
                p += written;
                offset += written;
                remaining -= written;
            }
            return true;
        }
#else
        static bool pwrite_all(int fd, const void* buf, size_t len, off_t offset) {
            const uint8_t* p = static_cast<const uint8_t*>(buf);
            size_t remaining = len;
            
            while (remaining > 0) {
                ssize_t written = ::pwrite(fd, p, remaining, offset);
                if (written < 0) {
                    if (errno == EINTR) {
                        continue;  // Retry on interrupt
                    }
                    return false;  // Real error
                }
                p += written;
                offset += written;
                remaining -= written;
            }
            return true;
        }
#endif

        void OTDeltaLog::append_with_payloads(const std::vector<DeltaWithPayload>& batch) {
            if (batch.empty()) {
                return;
            }
            
            // Check if we're closing - fail fast
            if (closing_.load(std::memory_order_acquire)) {
                throw std::runtime_error("Cannot append: log is closing");
            }
            
            // Debug check - file should always be open after constructor
#ifdef DEBUG
            if (!is_open()) {
                throw std::runtime_error("BUG: Delta log not open in append_with_payloads");
            }
#endif
            
            // Track this append operation
            in_flight_appends_.fetch_add(1, std::memory_order_acq_rel);
            
            // Ensure cleanup on all paths
            struct ScopeGuard {
                std::atomic<uint32_t>& counter;
                std::condition_variable& cv;
                ~ScopeGuard() { 
                    counter.fetch_sub(1, std::memory_order_acq_rel);
                    cv.notify_all();  // Wake up prepare_close() if waiting
                }
            } guard{in_flight_appends_, close_wait_cv_};
            
            // Build the full buffer with frame headers and payloads
            size_t total_size = 0;
            for (const auto& item : batch) {
                total_size += kFrameHeaderSize + kWireRecSize;
                if (item.payload_size > 0) {
                    total_size += item.payload_size;
                }
            }
            
            std::vector<uint8_t> buffer;
            buffer.reserve(total_size);
            
            // Track max epoch for coordinator queries
            uint64_t batch_max_epoch = 0;
            
            // Serialize each item with frame header
            for (const auto& item : batch) {
                // Track max epoch in this batch
                if (item.delta.birth_epoch > batch_max_epoch) {
                    batch_max_epoch = item.delta.birth_epoch;
                }
                
                // Build frame header
                FrameHeader header;
                header.frame_type = (item.payload_size > 0) ? kFrameTypeDeltaWithPayload : kFrameTypeDeltaOnly;
                header.payload_size = static_cast<uint32_t>(item.payload_size);
                header.payload_crc = 0;
                if (item.payload_size > 0 && item.payload_data) {
                    header.payload_crc = crc32c(item.payload_data, item.payload_size);
                }
                
                // Compute header CRC (excluding the header_crc field itself)
                header.header_crc = crc32c(&header, offsetof(FrameHeader, header_crc));
                
                // Write frame header
                size_t old_size = buffer.size();
                buffer.resize(old_size + kFrameHeaderSize);
                uint8_t* header_buf = buffer.data() + old_size;
                store_le32(header_buf, header.frame_type);
                store_le32(header_buf + 4, header.payload_size);
                store_le32(header_buf + 8, header.payload_crc);
                store_le32(header_buf + 12, header.header_crc);
                
                // Write delta record
                old_size = buffer.size();
                buffer.resize(old_size + kWireRecSize);
                serialize_delta_rec(buffer.data() + old_size, item.delta);
                
                // Write payload if present
                if (item.payload_size > 0 && item.payload_data) {
                    old_size = buffer.size();
                    buffer.resize(old_size + item.payload_size);
                    std::memcpy(buffer.data() + old_size, item.payload_data, item.payload_size);
                }
            }
            
            // Atomically reserve space in the log
            const uint64_t write_offset = end_offset_.fetch_add(buffer.size(), std::memory_order_acq_rel);
            
            // Write to disk
#ifdef _WIN32
            bool success = pwrite_all(fd_, buffer.data(), buffer.size(), write_offset);
#else
            bool success = pwrite_all(fd_, buffer.data(), buffer.size(), write_offset);
#endif
            
            if (!success) {
                // Roll back the end offset on failure
                end_offset_.fetch_sub(buffer.size(), std::memory_order_acq_rel);
                throw std::runtime_error("Failed to write to delta log");
            }
            
            // Update max epoch atomically with proper ordering (once per batch)
            if (batch_max_epoch > 0) {
                uint64_t cur = max_epoch_.load(std::memory_order_relaxed);
                while (batch_max_epoch > cur) {
                    if (max_epoch_.compare_exchange_weak(
                            cur, batch_max_epoch,
                            std::memory_order_release,      // success - ensure write visibility
                            std::memory_order_relaxed)) {   // failure - cur gets updated
                        break;
                    }
                }
            }
        }
        
        void OTDeltaLog::append(const std::vector<OTDeltaRec>& batch) {
            // Convert to append_with_payloads format for consistency
            std::vector<DeltaWithPayload> dwp;
            dwp.reserve(batch.size());
            for (const auto& delta : batch) {
                dwp.push_back({delta, nullptr, 0});
            }
            append_with_payloads(dwp);
            
            // Note: Caller must still call sync() to ensure durability
        }
        
        void OTDeltaLog::sync() {
#ifdef _WIN32
            if (fd_ == INVALID_HANDLE_VALUE) {
                return;
            }
            
            // Flush file buffers on Windows
            if (!FlushFileBuffers(fd_)) {
                throw std::runtime_error("Failed to flush delta log");
            }
#else
            if (fd_ < 0) {
                return;
            }
            
            // Just fsync - no buffer to manage with pwrite approach
            if (::fsync(fd_) != 0) {
                throw std::runtime_error("Failed to fsync delta log");
            }
#endif
        }

        void OTDeltaLog::replay_with_payloads(
            std::function<void(const OTDeltaRec&, const void* payload, size_t payload_size)> apply) {
            
            // Check if file exists first
            auto [result, size] = PlatformFS::file_size(path_);
            if (!result.ok) {
                // File doesn't exist - that's OK for an empty log
                return;
            }
            
            std::ifstream file(path_, std::ios::binary);
            if (!file) {
                throw std::runtime_error("Failed to open delta log file for replay");
            }
            
            std::vector<uint8_t> header_buf(kFrameHeaderSize);
            std::vector<uint8_t> delta_buf(kWireRecSize);
            std::vector<uint8_t> payload_buf;
            
            while (file.good()) {
                // Read frame header
                file.read(reinterpret_cast<char*>(header_buf.data()), kFrameHeaderSize);
                if (file.gcount() == 0) {
                    break;  // Clean EOF
                }
                if (file.gcount() < static_cast<std::streamsize>(kFrameHeaderSize)) {
                    // Partial header at end - torn tail, this is OK in crash recovery
                    break;
                }
                
                // Parse frame header
                FrameHeader header;
                header.frame_type = load_le32(header_buf.data());
                header.payload_size = load_le32(header_buf.data() + 4);
                header.payload_crc = load_le32(header_buf.data() + 8);
                header.header_crc = load_le32(header_buf.data() + 12);
                
                // Verify header CRC
                uint32_t computed_crc = crc32c(header_buf.data(), 12);  // First 12 bytes
                if (computed_crc != header.header_crc) {
                    // Corrupted header
                    break;
                }
                
                // Handle old format (no frame header) for backward compatibility
                if (header.frame_type != kFrameTypeDeltaOnly && 
                    header.frame_type != kFrameTypeDeltaWithPayload) {
                    // This might be old format - try to read as delta directly
                    // Seek back and try old format
                    file.seekg(-static_cast<std::streamoff>(kFrameHeaderSize), std::ios::cur);
                    
                    // Read old format frame: [len:4][rec:52][crc:4]
                    uint8_t len_buf[4];
                    file.read(reinterpret_cast<char*>(len_buf), 4);
                    if (file.gcount() < 4) break;
                    
                    uint32_t len = load_le32(len_buf);
                    if (len != kWireRecSize) break;
                    
                    file.read(reinterpret_cast<char*>(delta_buf.data()), kWireRecSize);
                    if (file.gcount() < static_cast<std::streamsize>(kWireRecSize)) break;
                    
                    uint8_t crc_buf[4];
                    file.read(reinterpret_cast<char*>(crc_buf), 4);
                    if (file.gcount() < 4) break;
                    
                    // Parse and apply delta without payload
                    OTDeltaRec rec;
                    deserialize_delta_rec(rec, delta_buf.data());
                    apply(rec, nullptr, 0);
                    continue;
                }
                
                // Read delta record
                file.read(reinterpret_cast<char*>(delta_buf.data()), kWireRecSize);
                if (file.gcount() < static_cast<std::streamsize>(kWireRecSize)) {
                    // Incomplete delta at end - torn tail, this is OK
                    break;
                }
                
                // Parse delta
                OTDeltaRec rec;
                deserialize_delta_rec(rec, delta_buf.data());
                
                // Read payload if present
                if (header.frame_type == kFrameTypeDeltaWithPayload && header.payload_size > 0) {
                    if (payload_buf.size() < header.payload_size) {
                        payload_buf.resize(header.payload_size);
                    }
                    
                    file.read(reinterpret_cast<char*>(payload_buf.data()), header.payload_size);
                    if (file.gcount() < static_cast<std::streamsize>(header.payload_size)) {
                        // Incomplete payload at end - torn tail, this is OK
                        break;
                    }
                    
                    // Verify payload CRC
                    uint32_t computed_payload_crc = crc32c(payload_buf.data(), header.payload_size);
                    if (computed_payload_crc != header.payload_crc) {
                        break;  // Corrupted payload
                    }
                    
                    apply(rec, payload_buf.data(), header.payload_size);
                } else {
                    apply(rec, nullptr, 0);
                }
            }
        }
        
        void OTDeltaLog::replay(std::function<void(const OTDeltaRec&)> apply) {
            // Check if file exists first
            auto [result, size] = PlatformFS::file_size(path_);
            if (!result.ok) {
                // File doesn't exist - that's OK for an empty log
                return;
            }
            
            // Use replay_with_payloads but ignore payloads for backward compatibility
            replay_with_payloads([&apply](const OTDeltaRec& delta, const void*, size_t) {
                apply(delta);
            });
        }

        bool OTDeltaLog::replay(const std::string& path, 
                               std::function<void(const OTDeltaRec&)> apply,
                               uint64_t* last_good_offset,
                               std::string* error) {
            *last_good_offset = 0;
            
            std::ifstream file(path, std::ios::binary);
            if (!file) {
                if (error) *error = "Failed to open delta log file";
                return false;
            }
            
            std::vector<uint8_t> header_buf(kFrameHeaderSize);
            std::vector<uint8_t> delta_buf(kWireRecSize);
            
            while (file.good()) {
                uint64_t frame_start = file.tellg();
                
                // Read frame header
                file.read(reinterpret_cast<char*>(header_buf.data()), kFrameHeaderSize);
                if (file.gcount() == 0) {
                    // Clean EOF
                    *last_good_offset = frame_start;
                    break;
                }
                if (file.gcount() < static_cast<std::streamsize>(kFrameHeaderSize)) {
                    // Partial header at end - torn tail, this is OK in crash recovery
                    *last_good_offset = frame_start;
                    return true;  // Success - torn frame at end is expected
                }
                
                // Parse frame header
                FrameHeader header;
                header.frame_type = load_le32(header_buf.data());
                header.payload_size = load_le32(header_buf.data() + 4);
                header.payload_crc = load_le32(header_buf.data() + 8);
                header.header_crc = load_le32(header_buf.data() + 12);
                
                // Verify header CRC
                uint32_t computed_crc = crc32c(header_buf.data(), 12);  // First 12 bytes
                if (computed_crc != header.header_crc) {
                    if (error) *error = "Header CRC mismatch";
                    *last_good_offset = frame_start;
                    return false;
                }
                
                // Validate frame type
                if (header.frame_type != kFrameTypeDeltaOnly && 
                    header.frame_type != kFrameTypeDeltaWithPayload) {
                    if (error) *error = "Invalid frame type";
                    *last_good_offset = frame_start;
                    return false;
                }
                
                // Read delta record
                file.read(reinterpret_cast<char*>(delta_buf.data()), kWireRecSize);
                if (file.gcount() < static_cast<std::streamsize>(kWireRecSize)) {
                    // Incomplete delta at end - torn tail, this is OK
                    *last_good_offset = frame_start;
                    return true;  // Success - torn frame at end is expected
                }
                
                // Parse delta
                OTDeltaRec rec;
                deserialize_delta_rec(rec, delta_buf.data());
                
                // Skip payload if present (we're ignoring it in this function)
                if (header.frame_type == kFrameTypeDeltaWithPayload && header.payload_size > 0) {
                    // Try to skip payload
                    file.seekg(header.payload_size, std::ios::cur);
                    if (!file.good() && !file.eof()) {
                        // If we can't skip but we're at EOF, it's a torn tail
                        *last_good_offset = frame_start;
                        return true;  // Success - torn frame at end
                    }
                }
                
                // Apply the record
                apply(rec);
                
                // Update last good offset after successful processing
                *last_good_offset = file.tellg();
            }
            
            return true;
        }

        // Note: rotate_if_needed has been removed
        // All rotation decisions are now made by CheckpointCoordinator
        // which owns the rotation policy and coordinates with checkpoints
    
    } // namespace persist
} // namespace xtree