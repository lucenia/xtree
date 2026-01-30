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

#ifndef NDEBUG
#include <cstdio>
#if !defined(_WIN32)
#include <execinfo.h>
#include <unistd.h>
#endif
#endif

namespace xtree { 
    namespace persist {

        /**
         * NodeID is a compact identifier for nodes in the XTree.
         * It encodes the handle index and a tag to prevent ABA issues.
         *
         * The handle index is a 48-bit value, allowing for up to 2^48 unique node handles.
         * The tag is a 16-bit value (65,536 versions per handle).
         *
         * This allows for efficient tracking of node versions
         * without needing to store full pointers or complex structures.
         * Layout: [63:16] handle index, [15:0] ABA tag
         */
        class alignas(8) NodeID {
        public:
            static constexpr uint64_t INVALID_RAW = ~uint64_t{0};

            // Default all special members to make the class trivial
            NodeID() = default;
            ~NodeID() = default;
            NodeID(const NodeID&) = default;
            NodeID& operator=(const NodeID&) = default;
            NodeID(NodeID&&) = default;
            NodeID& operator=(NodeID&&) = default;

            // Factory method to create NodeID from raw value
            static NodeID from_raw(uint64_t v) { 
                NodeID n; 
                n.v_ = v; 
                return n; 
            }
            
            // Factory method to create invalid NodeID
            static NodeID invalid() {
                return from_raw(INVALID_RAW);
            }

            // Factory method from handle index and tag
            static NodeID from_parts(uint64_t handle_idx, uint16_t tag) {
                if (tag == 0) tag = 1;  // never use tag 0
                return from_raw((handle_idx << 16) | tag);
            }

            // Accessors for raw value, handle index, and tag
            constexpr uint64_t raw() const { 
                return v_; 
            }

            // Accessors for handle index and tag
            constexpr uint64_t handle_index() const {
                return v_ >> 16; 
            }

            /** 
             * Accessor for tag - Returns the low 16 bits of the raw value
             * 
             * This is used to prevent ABA issues in concurrent environments
             * where the same handle index might be reused
             * after a node has been deleted and recreated
             * 
             * The tag allows us to distinguish between different versions
             * of the same handle index (65,536 versions before wraparound)
             */
            constexpr uint16_t tag() const { 
                return uint16_t(v_ & 0xFFFF); 
            }

            /** Accessor for validity - Returns true if the NodeID is valid */
#ifdef NDEBUG
            constexpr bool valid() const {
                return v_ != INVALID_RAW;
            }
#else
            bool valid() const {
                auto up = reinterpret_cast<std::uintptr_t>(this);
                if ((up % alignof(NodeID)) != 0) {
                    std::fprintf(stderr,
                        "NodeID::valid() on misaligned this=%p (align=%zu, raw=%llu)\n",
                        this, alignof(NodeID), (unsigned long long)v_);
#if !defined(_WIN32)
                    void* bt[32];
                    int n = ::backtrace(bt, 32);
                    ::backtrace_symbols_fd(bt, n, STDERR_FILENO);
#endif
                    std::abort();
                }
                return v_ != INVALID_RAW;
            }
#endif
            /** 
             * Comparison operators for NodeID
             * 
             * These allow us to compare NodeIDs directly
             * without needing to extract raw values or handle indices
             */
            constexpr bool operator==(const NodeID& o) const { 
                return v_ == o.v_; 
            }

            constexpr bool operator!=(const NodeID& o) const { 
                return v_ != o.v_; 
            }

        private:
            uint64_t v_;  // No default member initializer to keep trivial
        }; // NodeID

        // Verify NodeID alignment at compile time
        static_assert(alignof(NodeID) == 8, "NodeID must be 8-byte aligned");
        static_assert(sizeof(NodeID) == 8, "NodeID must be exactly 8 bytes");

        /**
         * NodeKind enum defines the types of nodes in the XTree.
         * - Invalid: Free OT slot, never visible to readers (birth_epoch=0)
         * - Internal: Non-leaf nodes that contain child pointers (goes to .xi file)
         * - Leaf: Leaf nodes that reference data records (goes to .xi file)
         * - ChildVec: Nodes that store child pointers in a vector (goes to .xi file)
         * - ValueVec: Nodes that store data records in a vector
         * - DataRecord: Individual data record objects (goes to .xd file)
         * - Tombstone: Reserved for leaf-record MVCC (logically deleted but visible to some snapshots)
         */
        enum class NodeKind : uint8_t { 
            Invalid = 0,    // Free OT slot, never visible
            Internal = 1,   // Internal XTreeBucket nodes (goes to .xi file)
            Leaf = 2,       // Leaf XTreeBucket nodes (goes to .xi file) 
            ChildVec = 3,   // Child vector for supernodes (goes to .xi file)
            ValueVec = 4,   // Generic value vector
            DataRecord = 5, // DataRecord objects (goes to .xd file)
            Tombstone = 255 // For leaf-record MVCC only, not OT slots
        }; // NodeKind

    } // namespace persist
} // namespace xtree::persist
