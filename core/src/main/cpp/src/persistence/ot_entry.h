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
#include <cstdint>
#include "node_id.hpp"

namespace xtree { 
    namespace persist {

        struct OTAddr {
            uint32_t file_id = 0;
            uint32_t segment_id = 0;
            uint64_t offset = 0;
            uint32_t length = 0;
            void*    vaddr = nullptr; // optional cached vaddr
        };

        /**
         * Object Table Entry State Machine:
         * 
         * FREE:      birth=0, kind=Invalid, retire=<any> (breadcrumb ok)
         * ALLOCATED: birth=0, kind≠Invalid, retire=~0 (not yet live)
         * LIVE:      birth>0, kind≠Invalid, retire=~0 (visible to readers)
         * RETIRED:   birth>0, kind≠Invalid, retire<~0 (awaiting reclaim)
         * RECLAIMED: transitions back to FREE
         * 
         * The tag field provides ABA protection with release/acquire ordering.
         * All state transitions must maintain these invariants.
         */
        struct OTEntry {
            OTAddr   addr{};
            uint8_t  class_id = 0;
            NodeKind kind = NodeKind::Internal;
            std::atomic<uint16_t> tag{0}; // mirrors NodeID low 16 bits
            std::atomic<uint64_t> birth_epoch{0};
            std::atomic<uint64_t> retire_epoch{~uint64_t{0}}; // U64_MAX = live

#ifndef NDEBUG
            // Debug-only fields to catch state machine violations
            enum DbgState : int {
                DBG_FREE = 0,
                DBG_RESERVED = 1,
                DBG_LIVE = 2,
                DBG_RETIRED = 3
            };
            std::atomic<int> dbg_state{DBG_FREE};
            static constexpr uint32_t DBG_MAGIC = 0x0B1EC7A7;
            uint32_t dbg_magic = DBG_MAGIC;
            DbgState get_dbg_state() const noexcept { 
                return static_cast<DbgState>(dbg_state.load(std::memory_order_relaxed)); 
            }
#endif

            bool is_free() const {
                // FREE state: birth=0 && kind=Invalid (ignore retire_epoch breadcrumb)
                // Use relaxed ordering - correctness is enforced by tag's release/acquire
                return birth_epoch.load(std::memory_order_relaxed) == 0 &&
                       kind == NodeKind::Invalid;
            }
            
            bool is_live() const {
                // Use relaxed ordering - correctness is enforced by tag's release/acquire
                return birth_epoch.load(std::memory_order_relaxed) != 0 &&
                       retire_epoch.load(std::memory_order_relaxed) == ~uint64_t{0};
            }
            
            bool is_retired() const {
                // Use relaxed ordering - correctness is enforced by tag's release/acquire
                return birth_epoch.load(std::memory_order_relaxed) != 0 &&
                       retire_epoch.load(std::memory_order_relaxed) != ~uint64_t{0};
            }
            
            bool is_allocated() const {
                // "Allocated" means it has ever been assigned: live OR retired
                // Use relaxed ordering - correctness is enforced by tag's release/acquire
                return birth_epoch.load(std::memory_order_relaxed) != 0;
            }
            
            bool is_valid() const {
                // "Valid for reads now" = live
                return is_live();
            }
        };

    } // namespace persist
} // namespace xtree
