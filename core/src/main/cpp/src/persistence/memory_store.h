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
#include "store_interface.h"
#include <unordered_map>
#include <vector>

namespace xtree {
    namespace persist {

        class MemoryStore final : public StoreInterface {
        public:
            AllocResult allocate_node(size_t min_len, NodeKind kind) override;
            void publish_node(NodeID id, const void* data, size_t len) override;
            NodeBytes read_node(NodeID id) const override;
            void retire_node(NodeID id,
                           uint64_t retire_epoch,
                           RetireReason why = RetireReason::Unknown,
                           const char* file = nullptr,
                           int line = 0) override;
            void free_node(NodeID id) override;
            void free_node_immediate(NodeID id,
                                   RetireReason why = RetireReason::Reallocation,
                                   const char* file = nullptr,
                                   int line = 0) override;
            NodeID  get_root(std::string_view) const override { 
                return root_;  // Already initialized to invalid, will be valid after set_root
            }
            void    set_root(NodeID id, uint64_t, const float*, size_t, std::string_view) override { root_ = id; }
            void commit(uint64_t) override {} // no-op
            
            // Zero-copy access - not applicable for in-memory store
            void* get_mapped_address(NodeID) override { return nullptr; }
            size_t get_capacity(NodeID id) override;  // Returns buffer size

            // Metadata lookup for determining node type
            bool get_node_kind(NodeID id, NodeKind& out_kind) const override;

            // Check if node is present
            bool is_node_present(NodeID id) const override;

        private:
            NodeID root_ = NodeID::invalid();  // Start with invalid NodeID
            struct Buf { 
                std::vector<uint8_t> bytes; 
                size_t alloc_len = 0;  // Track allocated length
            };
            std::unordered_map<uint64_t, Buf> table_; // key by NodeID::raw()
        };
    }
}