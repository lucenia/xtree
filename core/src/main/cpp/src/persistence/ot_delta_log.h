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
#include <functional>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#ifdef _WIN32
#include <windows.h>
#endif
#include "ot_entry.h"

namespace xtree { 
    namespace persist {

        struct OTDeltaRec {
            uint64_t handle_idx;
            uint16_t tag;
            uint8_t  class_id;
            uint8_t  kind; // NodeKind
            uint32_t file_id;
            uint32_t segment_id;
            uint64_t offset;
            uint32_t length;
            uint32_t data_crc32c;     // CRC32C of node data (for BALANCED mode validation)
            uint64_t birth_epoch;
            uint64_t retire_epoch; // U64_MAX if live
        };
        
        // Fixed wire format size - do NOT use sizeof(OTDeltaRec)
        // Layout: handle_idx(8) + tag(2) + class_id(1) + kind(1) +
        //         file_id(4) + segment_id(4) + offset(8) + length(4) +
        //         data_crc32c(4) + birth_epoch(8) + retire_epoch(8) = 52 bytes
        static constexpr size_t kWireRecSize = 52;
        
        // Frame header for payload-in-WAL support
        struct FrameHeader {
            uint32_t frame_type;    // 0 = delta only, 1 = delta + payload
            uint32_t payload_size;  // Size of payload (0 if frame_type == 0)
            uint32_t payload_crc;   // CRC32C of payload
            uint32_t header_crc;    // CRC32C of this header
        };
        static constexpr size_t kFrameHeaderSize = 16;
        static constexpr uint32_t kFrameTypeDeltaOnly = 0;
        static constexpr uint32_t kFrameTypeDeltaWithPayload = 1;

        // OTDeltaLog: Append-only delta log for Object Table updates
        // 
        // CONCURRENCY CONTRACT:
        // - append() is lock-free for high throughput (uses atomic end_offset_)
        // - close() must NOT be called while writers are active
        // - Coordination options:
        //   1. Higher-level coordinator ensures quiescence before close()
        //   2. Use prepare_close() → wait for in-flight → close() sequence
        //
        // ROTATION HANDOFF:
        // - OTLogGC::rotate_log() only updates manifest with new path
        // - Writer must: close old → open new → update pointer atomically
        // - Order matters for crash consistency
        //
        // GROUP COMMIT OPTIMIZATION (coordinator-level):
        // - append() and sync() are separate for a reason
        // - Multiple threads can call append() with high QPS
        // - A background flusher can coalesce fsync() calls every 1-5ms
        // - This reduces I/O syscalls and improves throughput
        // - Implementation: Timer-based or byte-threshold triggered flush
        //
        // GC THROTTLING (coordinator-level):
        // - check_rotation_needed() hits filesystem for size/mtime
        // - Don't call per-commit under heavy ingest
        // - Use timer (every N ms) or counter (every N appends)
        // - Reduces syscall overhead during write storms
        class OTDeltaLog {
        public:
            // Configuration constants
            static constexpr size_t kDefaultPreallocChunk = 64 * 1024 * 1024;  // 64MB default
            static constexpr size_t kTLBufSoftCap = 8 * 1024 * 1024;  // 8MB thread-local cap
            
            explicit OTDeltaLog(const std::string& path, 
                               size_t prealloc_chunk = kDefaultPreallocChunk,
                               uint64_t sequence = 0);
            ~OTDeltaLog();
            
            // Writing operations
            bool open_for_append();
            void append(const std::vector<OTDeltaRec>& batch);
            
            // Append with payload support (for EVENTUAL mode)
            struct DeltaWithPayload {
                OTDeltaRec delta;
                const void* payload_data;  // Optional payload bytes
                size_t payload_size;       // 0 if no payload
            };
            void append_with_payloads(const std::vector<DeltaWithPayload>& batch);
            
            // ---- Zero-allocation single-delta convenience wrappers ----
            // These forward to batch APIs without heap allocation
            inline void append_single(const OTDeltaRec& d) noexcept {
                // Stack array → no heap allocation
                const OTDeltaRec arr[1] = { d };
                append({arr, arr + 1});  // Creates vector from iterators
            }
            
            inline void append_with_payload(const OTDeltaRec& d,
                                           const void* payload,
                                           size_t size) noexcept {
                // Stack array → no heap allocation
                DeltaWithPayload arr[1] = { { d, payload, size } };
                append_with_payloads({arr, arr + 1});  // Creates vector from iterators
            }
            
            void sync();
            
            // Coordinated close - call prepare_close() first, wait for writers, then close()
            void prepare_close();  // Sets closing_ flag to block new appends
            bool is_closing() const { return closing_.load(std::memory_order_acquire); }
            void close();
            
            // Reading operations
            void replay(std::function<void(const OTDeltaRec&)> apply);
            
            // Replay with payload support (for recovery)
            void replay_with_payloads(
                std::function<void(const OTDeltaRec&, const void* payload, size_t payload_size)> apply);
            
            static bool replay(const std::string& path, 
                              std::function<void(const OTDeltaRec&)> apply,
                              uint64_t* last_good_offset,
                              std::string* error);
                       
            // Note: Rotation is entirely controlled by CheckpointCoordinator
            // Rotation methods do not belong here as rotation decisions are owned by the coordinator
            
            // Size information
            uint64_t get_end_offset() const { return end_offset_.load(std::memory_order_acquire); }
            
            // Non-blocking relaxed accessors for metrics (used by CheckpointCoordinator)
            size_t end_offset_relaxed() const noexcept { 
                return end_offset_.load(std::memory_order_relaxed); 
            }
            
            uint64_t end_epoch_relaxed() const noexcept {
                // Use acquire to ensure we see writes that happened-before the epoch update
                return max_epoch_.load(std::memory_order_acquire);
            }
            
            std::chrono::seconds age_seconds_relaxed(std::chrono::steady_clock::time_point now) const noexcept {
                uint64_t created = created_sec_.load(std::memory_order_relaxed);
                if (created == 0) return std::chrono::seconds{0};
                auto now_sec = std::chrono::duration_cast<std::chrono::seconds>(
                    now.time_since_epoch()).count();
                return std::chrono::seconds{now_sec - created};
            }
            
            // Get log sequence number (for manifest tracking)
            uint64_t sequence() const noexcept { return sequence_; }
            
            // Get log path
            const std::string& path() const noexcept { return path_; }
            
            // Check if file is open
            bool is_open() const {
#ifdef _WIN32
                return fd_ != INVALID_HANDLE_VALUE;
#else
                return fd_ >= 0;
#endif
            }
            
        private:
            std::string path_;
#ifdef _WIN32
            HANDLE fd_ = INVALID_HANDLE_VALUE;
#else
            int fd_ = -1;
#endif
            std::atomic<uint64_t> end_offset_{0};  // Current end of file for lock-free appends
            std::mutex open_close_mu_;  // Guards open/close to prevent races
            std::atomic<bool> closing_{false};  // Gate to prevent new appends during close
            std::atomic<uint32_t> in_flight_appends_{0};  // Count of active append operations
            std::mutex close_wait_mu_;  // For condition variable
            std::condition_variable close_wait_cv_;  // Efficient wait for in-flight completion
            size_t prealloc_chunk_;  // Configurable preallocation chunk size
            std::atomic<uint64_t> created_sec_{0};  // Unix timestamp when log was created
            std::atomic<uint64_t> max_epoch_{0};  // Highest epoch written to this log
            uint64_t sequence_{0};  // Log sequence number for manifest tracking
            
#ifdef DEBUG
            bool closed_ = false;  // Debug flag to catch double-close
#endif
        };

    } // namespace persist
} // namespace xtree