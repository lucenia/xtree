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

#include <cstring>
#include "node_id.hpp"

namespace xtree::persist {

/**
 * Alignment-safe helpers for NodeID operations on packed/wire data.
 *
 * These functions avoid UBSan violations when working with NodeID data
 * that may not be properly aligned in memory (e.g., from packed structs
 * or wire format buffers).
 */

/**
 * Load a NodeID from possibly unaligned storage.
 * Uses memcpy which is alignment-safe on all platforms.
 *
 * @param p Pointer to NodeID data (may be unaligned)
 * @return Properly aligned NodeID copy
 */
inline NodeID load_node_id_unaligned(const void* p) noexcept {
    NodeID id;
    std::memcpy(&id, p, sizeof(NodeID));
    return id;
}

/**
 * Store a NodeID to possibly unaligned storage.
 * Uses memcpy which is alignment-safe on all platforms.
 *
 * @param p Destination pointer (may be unaligned)
 * @param id NodeID to store
 */
inline void store_node_id_unaligned(void* p, const NodeID& id) noexcept {
    std::memcpy(p, &id, sizeof(NodeID));
}

/**
 * Check if a NodeID in unaligned storage is valid.
 * Avoids calling methods on potentially misaligned objects.
 *
 * @param p Pointer to NodeID data (may be unaligned)
 * @return true if the NodeID is valid
 */
inline bool is_node_id_valid_unaligned(const void* p) noexcept {
    NodeID id = load_node_id_unaligned(p);
    return id.valid();
}

/**
 * Get the raw value from a NodeID in unaligned storage.
 *
 * @param p Pointer to NodeID data (may be unaligned)
 * @return Raw NodeID value
 */
inline uint64_t get_node_id_raw_unaligned(const void* p) noexcept {
    NodeID id = load_node_id_unaligned(p);
    return id.raw();
}

/**
 * Get the kind from a NodeID in unaligned storage.
 *
 * @param p Pointer to NodeID data (may be unaligned)
 * @return NodeID kind
 */
inline NodeKind get_node_id_kind_unaligned(const void* p) noexcept {
    NodeID id = load_node_id_unaligned(p);
    return id.kind();
}

} // namespace xtree::persist