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

#include "memory_store.h"
#include "../util/log.h"
#include <cstring>
#include <stdexcept>
#include <atomic>
#include <iostream>

namespace xtree {
namespace persist {

AllocResult MemoryStore::allocate_node(size_t min_len, NodeKind kind) {
    // Generate a unique node ID
    static std::atomic<uint64_t> next_id{1};
    NodeID id = NodeID::from_raw(next_id.fetch_add(1));
    
    // Allocate memory buffer
    auto& buf = table_[id.raw()];
    buf.bytes.resize(min_len);
    buf.alloc_len = min_len;  // Track allocated length
    
    return {
        .id = id,
        .writable = buf.bytes.data(),
        .capacity = min_len
    };
}

void MemoryStore::publish_node(NodeID id, const void* data, size_t len) {
    auto it = table_.find(id.raw());
    if (it == table_.end()) {
        throw std::runtime_error("Publishing unknown node ID");
    }
    
    // If data pointer is different from our buffer, copy it
    if (data != it->second.bytes.data()) {
        it->second.bytes.resize(len);
        std::memcpy(it->second.bytes.data(), data, len);
    } else {
        // Data was written in-place, just resize if needed
        it->second.bytes.resize(len);
    }
}

NodeBytes MemoryStore::read_node(NodeID id) const {
    auto it = table_.find(id.raw());
    if (it == table_.end()) {
        throw std::runtime_error("Reading unknown node ID");
    }
    
    return {
        .data = it->second.bytes.data(),
        .size = it->second.bytes.size()
    };
}

void MemoryStore::retire_node(NodeID id,
                              uint64_t retire_epoch,
                              RetireReason why,
                              const char* file,
                              int line) {
#ifndef NDEBUG
    // Log retire calls in debug builds (same as DurableStore)
    if (file) {
        trace() << "[RETIRE_CALL][MemoryStore] id=" << id.raw()
                  << " reason=" << static_cast<int>(why)
                  << " at " << file << ":" << line << std::endl;
    }
#endif

    // For simple memory store, we don't actually reclaim memory
    // A more sophisticated implementation could track epochs and reclaim
    auto it = table_.find(id.raw());
    if (it == table_.end()) {
        throw std::runtime_error("Retiring unknown node ID");
    }
    // Just mark as retired (no-op for now)
    (void)retire_epoch; // Unused
}

void MemoryStore::free_node(NodeID id) {
    // DEPRECATED: Forward to instrumented version
    free_node_immediate(id, RetireReason::Unknown, nullptr, 0);
}

void MemoryStore::free_node_immediate(NodeID id,
                                      RetireReason why,
                                      const char* file,
                                      int line) {
#ifndef NDEBUG
    // Log the immediate free for consistency with DurableStore
    if (file) {
        trace() << "[FREE_IMMEDIATE][MemoryStore] id=" << id.raw()
                  << " reason=" << static_cast<int>(why)
                  << " at " << file << ":" << line << std::endl;
    }
#endif

    // For MemoryStore, immediately erase the node
    // This is safe since we don't have MVCC readers in IN_MEMORY mode
    table_.erase(id.raw());

    (void)why;  // Unused in release builds
}

size_t MemoryStore::get_capacity(NodeID id) {
    auto it = table_.find(id.raw());
    if (it == table_.end()) {
        return 0;  // Unknown node
    }
    // Return tracked allocated length, not vector capacity
    return it->second.alloc_len;
}

bool MemoryStore::get_node_kind(NodeID, NodeKind&) const {
    return false; // In-memory mode never needs OT metadata
}

bool MemoryStore::is_node_present(NodeID id) const {
    // In MemoryStore, just check if the node exists in the table
    return table_.find(id.raw()) != table_.end();
}

} // namespace persist
} // namespace xtree