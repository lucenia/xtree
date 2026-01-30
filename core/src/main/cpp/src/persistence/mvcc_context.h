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
#include <atomic>
#include <vector>
#include <mutex>
#include <memory>
#include <cstdint>
#include <climits>

namespace xtree { 
    namespace persist {

        class MVCCContext {
        public:
            // Cache-line aligned pin to avoid false sharing
            struct alignas(64) Pin { 
                std::atomic<uint64_t> epoch{UINT64_MAX};  // UINT64_MAX = not pinned
                // Padding to ensure each Pin occupies its own cache line
                char padding[64 - sizeof(std::atomic<uint64_t>)];
            };
            
            // RAII guard for automatic pin/unpin
            class Guard {
            public:
                Guard(Pin* pin, uint64_t epoch) : pin_(pin) {
                    if (pin_) {
                        pin_->epoch.store(epoch, std::memory_order_release);
                    }
                }
                
                ~Guard() {
                    if (pin_) {
                        pin_->epoch.store(UINT64_MAX, std::memory_order_release);
                    }
                }
                
                // Disable copy to prevent double unpin
                Guard(const Guard&) = delete;
                Guard& operator=(const Guard&) = delete;
                
                // Enable move for better ergonomics
                Guard(Guard&& o) noexcept : pin_(o.pin_) { 
                    o.pin_ = nullptr; 
                }
                
                Guard& operator=(Guard&& o) noexcept {
                    if (this != &o) {
                        if (pin_) {
                            pin_->epoch.store(UINT64_MAX, std::memory_order_release);
                        }
                        pin_ = o.pin_;
                        o.pin_ = nullptr;
                    }
                    return *this;
                }
                
            private:
                Pin* pin_;
            };
            
            MVCCContext();
            ~MVCCContext();
            
            // Thread registration - call once per thread
            Pin* register_thread();
            
            // Thread deregistration - useful for thread pools and tests
            void deregister_thread();
            
            // Lock-free pin/unpin via direct atomic operations
            static void pin_epoch(Pin* p, uint64_t e) {
                if (p) p->epoch.store(e, std::memory_order_release);
            }
            static void unpin(Pin* p) {
                if (p) p->epoch.store(UINT64_MAX, std::memory_order_release);
            }
            
            // Epoch queries
            uint64_t min_active_epoch() const; // scans pins (called by reclaimer, not hot path)
            uint64_t get_global_epoch() const { return global_epoch_.load(std::memory_order_acquire); }
            uint64_t advance_epoch() { return global_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1; }
            
            // Recovery-only fast path (call before starting any threads)
            // Directly sets the epoch to the recovered value in O(1)
            void recover_set_epoch(uint64_t target) {
                // Monotonicity guard - never go backwards
                auto cur = global_epoch_.load(std::memory_order_relaxed);
                if (target <= cur) return;
                
                global_epoch_.store(target, std::memory_order_relaxed);
                // Ensure all readers see the new epoch
                std::atomic_thread_fence(std::memory_order_release);
            }
            
        private:
            mutable std::mutex registration_mutex_;  // Only for thread registration
            std::vector<std::unique_ptr<Pin>> pins_;  // Use unique_ptr for stable addresses
            std::atomic<uint64_t> global_epoch_{0};
            
            // Increased for large systems - can be made configurable via config.h
            static constexpr size_t MAX_THREADS = 8192;  // Max concurrent threads
        };

    } // namespace persist
} // namespace xtree