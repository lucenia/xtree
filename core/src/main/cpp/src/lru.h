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

/*
 * LRUCache - High-Performance, Scalable LRU Cache
 * -----------------------------------------------
 *
 * Design Goals:
 * - Support billions of nodes with high churn.
 * - All core operations must be O(1):
 *     add, get, removeById, removeByObject, removeOne.
 * - Avoid O(n) scans: we removed the _nodes vector entirely.
 * - Use unordered_map for O(1) lookup:
 *     _mapId: IdType -> Node
 *     _mapObj: CachedObject* -> Node
 * - Maintain two doubly-linked lists:
 *     (1) LRU list (all nodes, MRU->LRU order)
 *     (2) Eviction list (unpinned nodes only, MRU->LRU order)
 * - Pin/unpin semantics protect nodes from eviction.
 * - Supports flexible delete policies: none, delete, delete[], free().
 * - Memory-efficient: node structs hold only pointers needed for LRU + eviction.
 *
 * Implementation Notes:
 * - Thread-safety: all public methods are wrapped with std::mutex for
 *   multi-threaded safety. If single-threaded, you may strip the lock.
 * - addToEvictionListMRU: asserts ensure a node cannot be double-inserted
 *   into the eviction list. Misuse will trigger early in debug builds.
 * - removeNodeAndDelete: nodes are unlinked, maps cleaned, and all
 *   next/prev/evict pointers nulled before delete to minimize dangling
 *   pointer misuse in debug builds.
 *
 * Current Structure:
 * - lru.h: class and node definitions
 * - lru.hpp: method implementations
 *
 * Future Considerations:
 * - Add capacity limit and eviction policy tuning.
 * - Make locking policy configurable (NoLock, StdMutex, RWLock).
 * - Optional statistics/metrics for hit/miss and churn.
 */

#pragma once

#include <cstddef>
#include <atomic>
#include <cassert>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

namespace xtree {

    enum LRUCacheDeleteType {
        LRUDeleteNone,
        LRUDeleteObject,
        LRUDeleteArray,
        LRUFreeMalloc
    };

    // ------------------------------
    // Node
    // ------------------------------
    template<typename CachedObjectType, typename IdType, LRUCacheDeleteType deleteType>
    struct LRUCacheNode {
        using _SelfType = LRUCacheNode<CachedObjectType, IdType, deleteType>;

        explicit LRUCacheNode(const IdType& i, CachedObjectType* o, _SelfType* n)
            : id(i), object(o), pin_count(0),
              next(n), prev(nullptr),
              evictNext(nullptr), evictPrev(nullptr) {}

        LRUCacheNode(const LRUCacheNode&) = delete;
        LRUCacheNode& operator=(const LRUCacheNode&) = delete;

        void pin()   { pin_count.fetch_add(1, std::memory_order_relaxed); }
        void unpin() {
            auto prev = pin_count.fetch_sub(1, std::memory_order_relaxed);
            assert(prev > 0 && "Unpin underflow");
        }
        inline bool isPinned() const noexcept { return pin_count.load(std::memory_order_relaxed) > 0; }
        static inline bool isPinned(const _SelfType* n) noexcept { return n && n->pin_count.load(std::memory_order_relaxed) > 0; }

        IdType id;
        CachedObjectType* object;
        std::atomic<uint32_t> pin_count;

        // LRU list (all nodes)
        _SelfType* next;
        _SelfType* prev;

        // Eviction list (only unpinned)
        _SelfType* evictNext;
        _SelfType* evictPrev;
    };

    // DeleteObject specialization
    template<typename CachedObjectType, typename IdType>
    struct LRUCacheNode<CachedObjectType, IdType, LRUDeleteObject> {
        using _SelfType = LRUCacheNode<CachedObjectType, IdType, LRUDeleteObject>;

        explicit LRUCacheNode(const IdType& i, CachedObjectType* o, _SelfType* n)
            : id(i), object(o), pin_count(0),
              next(n), prev(nullptr),
              evictNext(nullptr), evictPrev(nullptr) {}

        ~LRUCacheNode() { if (object) delete object; }

        LRUCacheNode(const LRUCacheNode&) = delete;
        LRUCacheNode& operator=(const LRUCacheNode&) = delete;

        void pin()   { pin_count.fetch_add(1, std::memory_order_relaxed); }
        void unpin() {
            auto prev = pin_count.fetch_sub(1, std::memory_order_relaxed);
            assert(prev > 0 && "Unpin underflow");
        }
        inline bool isPinned() const noexcept { return pin_count.load(std::memory_order_relaxed) > 0; }
        static inline bool isPinned(const _SelfType* n) noexcept { return n && n->pin_count.load(std::memory_order_relaxed) > 0; }

        IdType id;
        CachedObjectType* object;
        std::atomic<uint32_t> pin_count;

        _SelfType* next;
        _SelfType* prev;
        _SelfType* evictNext;
        _SelfType* evictPrev;
    };

    // DeleteArray specialization
    template<typename CachedObjectType, typename IdType>
    struct LRUCacheNode<CachedObjectType, IdType, LRUDeleteArray> {
        using _SelfType = LRUCacheNode<CachedObjectType, IdType, LRUDeleteArray>;

        explicit LRUCacheNode(const IdType& i, CachedObjectType* o, _SelfType* n)
            : id(i), object(o), pin_count(0),
              next(n), prev(nullptr),
              evictNext(nullptr), evictPrev(nullptr) {}

        ~LRUCacheNode() { if (object) delete[] object; }

        LRUCacheNode(const LRUCacheNode&) = delete;
        LRUCacheNode& operator=(const LRUCacheNode&) = delete;

        void pin()   { pin_count.fetch_add(1, std::memory_order_relaxed); }
        void unpin() {
            auto prev = pin_count.fetch_sub(1, std::memory_order_relaxed);
            assert(prev > 0 && "Unpin underflow");
        }
        inline bool isPinned() const noexcept { return pin_count.load(std::memory_order_relaxed) > 0; }
        static inline bool isPinned(const _SelfType* n) noexcept { return n && n->pin_count.load(std::memory_order_relaxed) > 0; }

        IdType id;
        CachedObjectType* object;
        std::atomic<uint32_t> pin_count;

        _SelfType* next;
        _SelfType* prev;
        _SelfType* evictNext;
        _SelfType* evictPrev;
    };

    // Free malloc specialization
    template<typename CachedObjectType, typename IdType>
    struct LRUCacheNode<CachedObjectType, IdType, LRUFreeMalloc> {
        using _SelfType = LRUCacheNode<CachedObjectType, IdType, LRUFreeMalloc>;

        explicit LRUCacheNode(const IdType& i, CachedObjectType* o, _SelfType* n)
            : id(i), object(o), pin_count(0),
              next(n), prev(nullptr),
              evictNext(nullptr), evictPrev(nullptr) {}

        ~LRUCacheNode() { if (object) free(object); }

        LRUCacheNode(const LRUCacheNode&) = delete;
        LRUCacheNode& operator=(const LRUCacheNode&) = delete;

        void pin()   { pin_count.fetch_add(1, std::memory_order_relaxed); }
        void unpin() {
            auto prev = pin_count.fetch_sub(1, std::memory_order_relaxed);
            assert(prev > 0 && "Unpin underflow");
        }
        inline bool isPinned() const noexcept { return pin_count.load(std::memory_order_relaxed) > 0; }
        static inline bool isPinned(const _SelfType* n) noexcept { return n && n->pin_count.load(std::memory_order_relaxed) > 0; }

        IdType id;
        CachedObjectType* object;
        std::atomic<uint32_t> pin_count;

        _SelfType* next;
        _SelfType* prev;
        _SelfType* evictNext;
        _SelfType* evictPrev;
    };

    // ------------------------------
    // Cache
    // ------------------------------
    template<typename CachedObjectType, typename IdType, LRUCacheDeleteType deleteObject>
    class LRUCache {
    public:
        using Node   = LRUCacheNode<CachedObjectType, IdType, deleteObject>;
        using IdMap  = std::unordered_map<IdType, Node*>;
        using ObjMap = std::unordered_map<CachedObjectType*, Node*>;

        // Result struct for acquirePinned
        struct AcquireResult {
            Node* node;
            bool created;  // true if new node was created, false if existing was returned
        };

        LRUCache()
            : _first(nullptr), _last(nullptr),
              _evictFirst(nullptr), _evictLast(nullptr) {}

        ~LRUCache() { clear(); }

        // O(1) add
        Node* add(const IdType& id, CachedObjectType* object);

        // O(1) atomic get-or-create, returns node already pinned
        // Thread-safe: If id exists, pins and returns existing node
        // If id doesn't exist, creates new node with objIfAbsent, pins it, and returns it
        // Caller is responsible for unpinning (use ScopedPin for RAII)
        // Returns AcquireResult with node pointer and created flag
        AcquireResult acquirePinned(const IdType& id, CachedObjectType* objIfAbsent);

        // Atomically acquire a pinned node, persisting only if created
        // persistFn is called exactly once if a new object is inserted
        // Returns AcquireResult with node pointer and created flag
        template<typename PersistFn>
        AcquireResult acquirePinnedWithPersist(const IdType& id, CachedObjectType* objIfAbsent, PersistFn persistFn) noexcept;

        // O(1) get + LRU promote
        CachedObjectType* get(const IdType& id);

        // O(1) peek without LRU update (read-only fast path)
        CachedObjectType* peek(const IdType& id) const;

        // Internal node lookup for cache coherence helpers
        // Returns the internal node for a given key if it exists, or nullptr.
        // Does NOT modify recency lists or pin counts.
        inline Node* find_node_internal(const IdType& id) noexcept {
            auto it = _mapId.find(id);
            return (it == _mapId.end()) ? nullptr : it->second;
        }

        inline const Node* find_node_internal(const IdType& id) const noexcept {
            auto it = _mapId.find(id);
            return (it == _mapId.end()) ? nullptr : it->second;
        }

        // O(1) eviction (returns detached node; caller deletes)
        Node* removeOne();

        // O(1) removals by key or by object
        // removeById: Removes node and returns the cached object pointer
        // IMPORTANT: Ownership transfers to caller - caller MUST delete/free the
        // returned object appropriately based on how it was allocated
        // Returns nullptr if ID not found
        CachedObjectType* removeById(const IdType& id);

        // removeByObject: Removes and deletes the node containing this object
        // Returns true if removed, false if not found or pinned
        bool removeByObject(CachedObjectType* object);

        // Back-compat wrapper (delete + remove)
        void remove(CachedObjectType* object) { (void)removeByObject(object); }

        // Atomically change the key for an existing cache entry.
        // This is needed when COW reallocates a bucket to a new NodeID.
        // The node and object pointer stay the same, only the index key changes.
        // Returns true if successful, false if old_id not found or new_id already exists.
        bool rekey(const IdType& old_id, const IdType& new_id);

        // Detach a node from the cache for transfer to another shard.
        // Unlike removeById, this works even for pinned nodes and returns
        // the node itself (not just the object) so pin state can be preserved.
        // Caller takes ownership of the returned node and must delete it or re-add it.
        // Returns nullptr if id not found.
        Node* detach_node(const IdType& id);

        // Re-attach a detached node with a new ID.
        // Used for cross-shard rekey operations.
        // Returns true if successful, false if new_id already exists.
        bool attach_node(const IdType& new_id, Node* node);

        // Pin helpers (update eviction membership)
        void pin(Node* n) {
            assert(n);
            std::unique_lock<std::shared_mutex> lock(_mtx);
            if (!n->isPinned()) {
                n->pin();
                removeFromEvictionList(n);
            } else {
                n->pin();
            }
        }
        void unpin(Node* n) {
            assert(n);
            std::unique_lock<std::shared_mutex> lock(_mtx);
            n->unpin();
            if (!n->isPinned()) addToEvictionListMRU(n);
        }
        static bool is_pinned(const Node* n) { return n && n->isPinned(); }

        // Destroy everything
        void clear();

        // Stats
        size_t size() const {
            std::shared_lock<std::shared_mutex> lock(_mtx);
            return _mapId.size();
        }

        // Count evictable (unpinned) nodes
        size_t evictableCount() const {
            std::shared_lock<std::shared_mutex> lock(_mtx);
            size_t count = 0;
            Node* cur = _evictFirst;
            while (cur) {
                count++;
                cur = cur->evictNext;
            }
            return count;
        }

        // Count pinned nodes
        size_t pinnedCount() const {
            std::shared_lock<std::shared_mutex> lock(_mtx);
            // Total - evictable = pinned
            return size() - evictableCount();
        }

    private:
        // Helper to free object based on delete policy
        static void freeObject(CachedObjectType* obj) {
            if (!obj) return;
            if constexpr (deleteObject == LRUDeleteObject) delete obj;
            else if constexpr (deleteObject == LRUDeleteArray) delete[] obj;
            else if constexpr (deleteObject == LRUFreeMalloc) free(obj);
            // else deleteObject == LRUDeleteNone → do nothing
        }

        // Thread safety - shared_mutex for read/write separation
        mutable std::shared_mutex _mtx;

        // Fast lookups
        IdMap  _mapId;
        ObjMap _mapObj;

        // LRU list (all nodes)
        Node* _first; // MRU
        Node* _last;  // LRU

        // Eviction list (only unpinned nodes)
        Node* _evictFirst; // MRU unpinned
        Node* _evictLast;  // LRU unpinned

        // List ops
        void promoteToMRU(Node* n);
        void unlinkFromLRU(Node* n);

        // Eviction list ops
        void removeFromEvictionList(Node* n);
        void addToEvictionListMRU(Node* n);

        // Unified internal removals
        void removeNodeAndDelete(Node* n);
        Node* removeNodeAndReturn(Node* n);
    };

    // ------------------------------
    // RAII pin helper - MUST use cache's pin/unpin for eviction list safety
    // ------------------------------
    template<typename Cache, typename Node>
    class ScopedPin {
        Cache& cache;
        Node* node;
    public:
        ScopedPin(Cache& c, Node* n) : cache(c), node(n) {
            if (node) cache.pin(node);     // updates eviction list + locks
        }
        ~ScopedPin() {
            if (node) cache.unpin(node);   // updates eviction list + locks
        }

        ScopedPin(const ScopedPin&) = delete;
        ScopedPin& operator=(const ScopedPin&) = delete;
        ScopedPin(ScopedPin&&) = delete;
        ScopedPin& operator=(ScopedPin&&) = delete;
    };

} // namespace xtree