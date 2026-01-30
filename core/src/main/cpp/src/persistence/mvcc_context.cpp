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

#include "mvcc_context.h"
#include <algorithm>
#include <limits>

namespace xtree { 
    namespace persist {

        // Thread-local storage for each thread's Pin to prevent ghost pins
        thread_local MVCCContext::Pin* t_pin = nullptr;

        // Static assertions to ensure cache-line isolation
        static_assert(sizeof(MVCCContext::Pin) == 64, "Pin must be exactly one cache line");
        static_assert(alignof(MVCCContext::Pin) == 64, "Pin must be cache-line aligned");

        MVCCContext::MVCCContext() {
            // Pre-allocate pins to avoid allocation during registration
            pins_.reserve(MAX_THREADS);
        }

        MVCCContext::~MVCCContext() = default;

        MVCCContext::Pin* MVCCContext::register_thread() {
            // Fast path: thread already registered
            if (t_pin) {
                return t_pin;
            }
            
            // Slow path: one-time registration under lock
            std::lock_guard<std::mutex> lock(registration_mutex_);
            
            // Double-check after acquiring lock (another thread might have registered us)
            if (t_pin) {
                return t_pin;
            }
            
            // Allocate a new pin
            if (pins_.size() >= MAX_THREADS) {
                // Too many threads - this is an error condition
                return nullptr;
            }
            
            pins_.push_back(std::make_unique<Pin>());
            Pin* new_pin = pins_.back().get();
            
            // Cache in thread-local storage
            t_pin = new_pin;
            
            return new_pin;
        }

        void MVCCContext::deregister_thread() {
            if (t_pin) {
                // Mark as unpinned
                t_pin->epoch.store(UINT64_MAX, std::memory_order_release);
                // Clear thread-local cache
                t_pin = nullptr;
            }
        }

        uint64_t MVCCContext::min_active_epoch() const {
            // Note: We lock here because this is called infrequently by the reclaimer,
            // not on the read hot path. We could make this lock-free with more complexity.
            std::lock_guard<std::mutex> lock(registration_mutex_);
            
            uint64_t min_epoch = UINT64_MAX;
            
            // Scan all pins to find minimum active epoch
            for (const auto& pin : pins_) {
                uint64_t pin_epoch = pin->epoch.load(std::memory_order_acquire);
                if (pin_epoch != UINT64_MAX && pin_epoch < min_epoch) {
                    min_epoch = pin_epoch;
                }
            }
            
            // If no pins are active, return current global epoch
            if (min_epoch == UINT64_MAX) {
                min_epoch = global_epoch_.load(std::memory_order_acquire);
            }
            
            return min_epoch;
        }

    } // namespace persist
} // namespace xtree