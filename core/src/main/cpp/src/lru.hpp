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

#include "lru.h"

namespace xtree {

    // ------------------------------
    // add
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    typename LRUCache<T, Id, Del>::Node*
    LRUCache<T, Id, Del>::add(const Id& id, T* object) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        // Prevent duplicates
        assert(_mapId.find(id) == _mapId.end() && "Duplicate id in LRUCache");
        assert(_mapObj.find(object) == _mapObj.end() && "Duplicate object* in LRUCache");

        Node* node = new Node(id, object, _first);

        // Link into LRU head
        if (_first) _first->prev = node;
        _first = node;
        if (!_last) _last = node;

        // Index in maps
        _mapId.emplace(id, node);
        _mapObj.emplace(object, node);

        // Unpinned by default -> add to eviction list MRU
        addToEvictionListMRU(node);

        return node;
    }

    // ------------------------------
    // acquirePinned
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    typename LRUCache<T, Id, Del>::AcquireResult
    LRUCache<T, Id, Del>::acquirePinned(const Id& id, T* objIfAbsent) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        // Check if already exists
        auto it = _mapId.find(id);
        if (it != _mapId.end()) {
            Node* node = it->second;

            // Always remove from eviction list (idempotent)
            removeFromEvictionList(node);

            // Pin and promote
            node->pin();
            promoteToMRU(node);

            // If objIfAbsent was allocated but not needed, free it
            if (objIfAbsent) {
                freeObject(objIfAbsent);
            }

            return {node, false};  // existing node
        }

        // Doesn't exist - create new node already pinned
        Node* node = new Node(id, objIfAbsent, _first);
        node->pin();  // Start pinned - no eviction list

        // Link into LRU head
        if (_first) _first->prev = node;
        _first = node;
        if (!_last) _last = node;

        // Index in maps
        _mapId.emplace(id, node);
        _mapObj.emplace(objIfAbsent, node);

        // Don't add to eviction list since it's pinned
        return {node, true};  // new node created
    }

    // ------------------------------
    // acquirePinnedWithPersist
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    template<typename PersistFn>
    typename LRUCache<T, Id, Del>::AcquireResult
    LRUCache<T, Id, Del>::acquirePinnedWithPersist(const Id& id, T* objIfAbsent, PersistFn persistFn) noexcept {
        auto result = acquirePinned(id, objIfAbsent);

        if (result.created && objIfAbsent) {
            // Only persist if we actually created this node
            persistFn(objIfAbsent);
        } else if (!result.created && objIfAbsent) {
            // Cache rejected objIfAbsent because it already had one
            freeObject(objIfAbsent);
        }

        return result; // Return full AcquireResult with created flag
    }

    // ------------------------------
    // get
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    T* LRUCache<T, Id, Del>::get(const Id& id) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        auto it = _mapId.find(id);
        if (it == _mapId.end()) return nullptr;

        Node* node = it->second;

        // Promote to MRU in LRU list
        promoteToMRU(node);

        // If unpinned, refresh its position in eviction MRU
        if (!node->isPinned()) {
            removeFromEvictionList(node);
            addToEvictionListMRU(node);
        }

        return node->object;
    }

    // ------------------------------
    // peek (read-only, no LRU update)
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    T* LRUCache<T, Id, Del>::peek(const Id& id) const {
        std::shared_lock<std::shared_mutex> lock(_mtx);

        auto it = _mapId.find(id);
        if (it == _mapId.end()) return nullptr;

        return it->second->object;
    }

    // ------------------------------
    // removeOne (evict LRU-unpinned)
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    typename LRUCache<T, Id, Del>::Node*
    LRUCache<T, Id, Del>::removeOne() {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        if (!_evictLast) return nullptr; // nothing evictable
        return removeNodeAndReturn(_evictLast);
    }

    // ------------------------------
    // removeById / removeByObject
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    T* LRUCache<T, Id, Del>::removeById(const Id& id) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        auto it = _mapId.find(id);
        if (it == _mapId.end()) return nullptr;

        Node* node = it->second;

        // Don't remove pinned nodes - they're in use!
        if (node->isPinned()) return nullptr;

        T* object = node->object;

        // Remove from object map before deleting the node
        _mapObj.erase(object);

        // Prevent the object from being deleted when node is deleted
        node->object = nullptr;

        // Now remove the node (which will delete the node but not the object)
        removeNodeAndDelete(node);
        return object;
    }

    template<typename T, typename Id, LRUCacheDeleteType Del>
    bool LRUCache<T, Id, Del>::removeByObject(T* object) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        auto it = _mapObj.find(object);
        if (it == _mapObj.end()) return false;

        Node* n = it->second;

        // Never remove a pinned node
        if (n->isPinned()) return false;

        removeNodeAndDelete(n);
        return true;
    }

    // ------------------------------
    // clear
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    void LRUCache<T, Id, Del>::clear() {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        // Delete nodes by walking the LRU list once
        Node* cur = _first;
        while (cur) {
            Node* nxt = cur->next;
            delete cur;
            cur = nxt;
        }
        _first = _last = _evictFirst = _evictLast = nullptr;
        _mapId.clear();
        _mapObj.clear();

        // Sanity check: maps must be empty after clear
        assert(_mapId.size() == 0 && "mapId not empty after clear");
        assert(_mapObj.size() == 0 && "mapObj not empty after clear");
    }

    // ------------------------------
    // helpers: LRU list
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    void LRUCache<T, Id, Del>::promoteToMRU(Node* n) {
        if (!n || n == _first) return;

        // Unlink from current position
        if (n->prev) n->prev->next = n->next;
        if (n->next) n->next->prev = n->prev;
        if (_last == n) _last = n->prev;

        // Link at head
        n->next = _first;
        n->prev = nullptr;
        if (_first) _first->prev = n;
        _first = n;

        if (!_last) _last = n;
    }

    template<typename T, typename Id, LRUCacheDeleteType Del>
    void LRUCache<T, Id, Del>::unlinkFromLRU(Node* n) {
        if (!n) return;

        if (n->prev) n->prev->next = n->next;
        else         _first = n->next;

        if (n->next) n->next->prev = n->prev;
        else         _last = n->prev;

        n->next = n->prev = nullptr;
    }

    // ------------------------------
    // helpers: eviction list
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    void LRUCache<T, Id, Del>::removeFromEvictionList(Node* n) {
        if (!n) return;

        if (n->evictPrev) n->evictPrev->evictNext = n->evictNext;
        else              _evictFirst = n->evictNext;

        if (n->evictNext) n->evictNext->evictPrev = n->evictPrev;
        else              _evictLast  = n->evictPrev;

        n->evictPrev = n->evictNext = nullptr;
    }

    template<typename T, typename Id, LRUCacheDeleteType Del>
    void LRUCache<T, Id, Del>::addToEvictionListMRU(Node* n) {
        if (!n) return;

        // if already present, do nothing
        if (n->evictNext || n->evictPrev || n == _evictFirst) return;

        // Debug assertion to catch double-insertion
        assert((!n->evictNext && !n->evictPrev && n != _evictFirst) &&
               "Node already in eviction list");

        n->evictPrev = nullptr;
        n->evictNext = _evictFirst;
        if (_evictFirst) _evictFirst->evictPrev = n;
        else             _evictLast = n;  // first in list

        _evictFirst = n;
    }

    // ------------------------------
    // helpers: unified removals
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    void LRUCache<T, Id, Del>::removeNodeAndDelete(Node* n) {
        if (!n) return;
        // Remove from both lists
        removeFromEvictionList(n);
        unlinkFromLRU(n);

        // Remove from maps
        _mapId.erase(n->id);
        if (n->object) {  // Object might be nullptr if removeById was called
            _mapObj.erase(n->object);
        }

        // Defensive nulling to catch UAF in debug
        n->next = n->prev = nullptr;
        n->evictNext = n->evictPrev = nullptr;

        delete n; // node dtor frees object per delete policy
    }

    template<typename T, typename Id, LRUCacheDeleteType Del>
    typename LRUCache<T, Id, Del>::Node*
    LRUCache<T, Id, Del>::removeNodeAndReturn(Node* n) {
        if (!n) return nullptr;

        // Eviction list: we know n is in eviction list (called from removeOne)
        removeFromEvictionList(n);
        // LRU list unlink
        unlinkFromLRU(n);

        // Erase from maps
        _mapId.erase(n->id);
        _mapObj.erase(n->object);

        // Defensive nulling
        n->next = n->prev = nullptr;
        n->evictNext = n->evictPrev = nullptr;

        // Return detached node (caller takes ownership and must delete)
        return n;
    }

    // ------------------------------
    // rekey
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    bool LRUCache<T, Id, Del>::rekey(const Id& old_id, const Id& new_id) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        // Find the node by old_id
        auto it = _mapId.find(old_id);
        if (it == _mapId.end()) {
            return false;  // old_id not found
        }

        // Check if new_id already exists (would cause duplicate)
        if (_mapId.find(new_id) != _mapId.end()) {
            return false;  // new_id already exists
        }

        Node* node = it->second;

        // Remove from old key mapping
        _mapId.erase(it);

        // Update the node's internal id to the new key
        node->id = new_id;

        // Insert with new key
        _mapId.emplace(new_id, node);

        // Object mapping stays the same (same object pointer)
        // LRU position stays the same
        // Eviction list position stays the same
        // Pin count stays the same

        return true;
    }

    // ------------------------------
    // detach_node (for cross-shard rekey)
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    typename LRUCache<T, Id, Del>::Node*
    LRUCache<T, Id, Del>::detach_node(const Id& id) {
        std::unique_lock<std::shared_mutex> lock(_mtx);

        auto it = _mapId.find(id);
        if (it == _mapId.end()) {
            return nullptr;  // not found
        }

        Node* node = it->second;

        // Remove from maps (but don't delete the node)
        _mapId.erase(it);
        if (node->object) {
            _mapObj.erase(node->object);
        }

        // Remove from eviction list (if unpinned)
        removeFromEvictionList(node);

        // Remove from LRU list
        unlinkFromLRU(node);

        // Clear internal pointers but preserve object, id, and pin count
        node->next = node->prev = nullptr;
        node->evictNext = node->evictPrev = nullptr;

        return node;  // Caller takes ownership
    }

    // ------------------------------
    // attach_node (for cross-shard rekey)
    // ------------------------------
    template<typename T, typename Id, LRUCacheDeleteType Del>
    bool LRUCache<T, Id, Del>::attach_node(const Id& new_id, Node* node) {
        if (!node) return false;

        std::unique_lock<std::shared_mutex> lock(_mtx);

        // Check if new_id already exists
        if (_mapId.find(new_id) != _mapId.end()) {
            return false;  // already exists
        }

        // Check if object already exists
        if (node->object && _mapObj.find(node->object) != _mapObj.end()) {
            return false;  // object already in this shard
        }

        // Update node's id to new_id
        node->id = new_id;

        // Link into LRU head
        node->next = _first;
        node->prev = nullptr;
        if (_first) _first->prev = node;
        _first = node;
        if (!_last) _last = node;

        // Index in maps
        _mapId.emplace(new_id, node);
        if (node->object) {
            _mapObj.emplace(node->object, node);
        }

        // Add to eviction list only if unpinned
        if (!node->isPinned()) {
            addToEvictionListMRU(node);
        }

        return true;
    }

} // namespace xtree