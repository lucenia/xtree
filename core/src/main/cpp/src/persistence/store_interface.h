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
#include <cstddef>
#include <cstdint>
#include <string_view>
#include "node_id.hpp"          // NodeID
#include "ot_entry.h"           // NodeKind
#include "mapping_manager.h"    // MappingManager::Pin

namespace xtree {
    namespace persist {
        struct NodeBytes {
            const void* data;
            size_t size;
        };

        struct AllocResult {
            NodeID  id;        // handle (stable)
            void*   writable;  // mapped/mutable bytes (may be null for pure copy-in APIs)
            size_t  capacity;  // reserved size in bytes
        };

        // Reason codes for retire operations (for debugging)
        enum class RetireReason : uint8_t {
            Unknown = 0,
            SplitReplace = 1,    // Node replaced during split
            MergeDelete = 2,     // Node deleted during merge
            Evict = 3,           // Node evicted from cache
            AbortRollback = 4,   // Transaction rollback
            Reallocation = 5,    // Node reallocated (grown)
            TreeDestroy = 6      // Tree being destroyed
        };

        class StoreInterface {
        public:
            virtual ~StoreInterface() = default;

            // 1) Space
            virtual AllocResult allocate_node(size_t min_len, NodeKind kind) = 0;

            // 2) Publish a new node version (bytes must be fully written)
            virtual void publish_node(NodeID id, const void* data, size_t len) = 0;
            
            // 2b) No-copy publish: payload is already in the mapped destination
            // Default implementation returns false - override in stores that support it
            virtual bool supports_in_place_publish() const { return false; }
            
            // Stores that support this will override to avoid memcpy
            virtual void publish_node_in_place(NodeID id, size_t len) {
                // Default: not supported, subclasses override if they support in-place
                throw std::runtime_error("publish_node_in_place not supported by this store");
            }

            // 3) Read-only lookup for the given snapshot
            virtual NodeBytes read_node(NodeID id) const = 0;
            
            // Pinned read for zero-copy access with MappingManager
            struct PinnedBytes {
                MappingManager::Pin pin;
                void*  data = nullptr;
                size_t size = 0;
            };
            
            // Returns pinned memory that stays valid while Pin is held
            // Default throws - override in stores that support pinned reads
            virtual PinnedBytes read_node_pinned(NodeID id) const {
                throw std::runtime_error("read_node_pinned not supported by this store");
            }

            // 4) Lifecycle
            virtual void retire_node(NodeID id,
                                    uint64_t retire_epoch,
                                    RetireReason why = RetireReason::Unknown,
                                    const char* file = nullptr,
                                    int line = 0) = 0;
            
            // DEPRECATED: Use free_node_immediate with instrumentation instead
            virtual void free_node(NodeID id) = 0;

            // Free a node's storage immediately (non-transactional)
            // Used for reallocation/rollback cases where we need immediate space reclamation
            // This bypasses transactional retirement and should only be used when necessary
            virtual void free_node_immediate(NodeID id,
                                            RetireReason why = RetireReason::Reallocation,
                                            const char* file = nullptr,
                                            int line = 0) = 0;

            // 5) Root management
            virtual NodeID  get_root(std::string_view name = {}) const = 0;
            // MBR can be nullptr for initial empty tree (will use infinity bounds)
            virtual void    set_root(NodeID id, uint64_t epoch, 
                                   const float* mbr, size_t mbr_size,
                                   std::string_view name = {}) = 0;

            // 6) Durability (group-commit friendly; no-op in InMemoryStore)
            virtual void commit(uint64_t epoch) = 0;
            
            // 7) Zero-copy access for in-place updates
            // Get the mapped address for an existing node (returns nullptr if not available)
            virtual void* get_mapped_address(NodeID id) = 0;

            // Get the allocated capacity for an existing node
            virtual size_t get_capacity(NodeID id) = 0;

            // 8) Metadata lookup for determining node type
            // Returns true if found, false otherwise
            virtual bool get_node_kind(NodeID id, NodeKind& out_kind) const = 0;

            // 9) Check if a node exists in any non-freed state (RESERVED or LIVE)
            // This is more permissive than get_node_kind, which only returns true for LIVE
            // Returns true if the node is RESERVED or LIVE, false if FREE/RETIRED/invalid
            virtual bool is_node_present(NodeID id) const = 0;

            // Overload with out param to indicate if node is staged/uncommitted
            // Default implementation just calls the single-arg version
            virtual bool is_node_present(NodeID id, bool* out_is_staged) const {
                if (out_is_staged) *out_is_staged = false;
                return is_node_present(id);
            }
        };


    } // namespace persist
} // namespace xtree

// Convenience macro for instrumented retire calls
#define DS_RETIRE(store_ptr, node_id, epoch, reason) \
    (store_ptr)->retire_node((node_id), (epoch), \
        ::xtree::persist::RetireReason::reason, __FILE__, __LINE__)

// Convenience macro for immediate free (non-transactional)
#define DS_FREE_IMMEDIATE(store_ptr, node_id, reason) \
    (store_ptr)->free_node_immediate((node_id), \
        ::xtree::persist::RetireReason::reason, __FILE__, __LINE__)