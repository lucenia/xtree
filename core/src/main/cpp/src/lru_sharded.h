/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * ShardedLRUCache - Scalable Sharded LRU Cache for Billions of Nodes
 * -------------------------------------------------------------------
 *
 * Design:
 * - Shards the cache across N independent LRUCache instances
 * - Each shard has its own mutex, maps, and lists
 * - Operations are dispatched by hash(id) % numShards
 * - Eliminates lock contention for concurrent operations on different shards
 * - Scales linearly with number of shards (typically 2-4x CPU cores)
 *
 * Performance:
 * - All operations remain O(1) per shard
 * - Effective throughput = single-shard throughput × numShards
 * - Memory locality improves with smaller per-shard maps
 *
 * Trade-offs:
 * - Global LRU ordering is lost (each shard maintains local LRU)
 * - removeByObject requires global tracking or O(numShards) scan
 * - Eviction happens per-shard, not globally
 */

#pragma once

#include "lru.h"
#include <vector>
#include <atomic>
#include <functional>
#include <memory>

namespace xtree {

template<typename T, typename Id, LRUCacheDeleteType Del>
class ShardedLRUCache {
public:
    using Shard = LRUCache<T, Id, Del>;
    using Node = typename Shard::Node;
    using AcquireResult = typename Shard::AcquireResult;
    using IdType = Id;
    using CachedObjectType = T;

    // Function type for getting memory size of an object
    // Default implementation tries object->memoryUsage(), falls back to fixed estimate
    using MemorySizer = std::function<size_t(const T*)>;

    explicit ShardedLRUCache(size_t numShards = 32, bool enableGlobalObjMap = false)
        : _evictCounter(0),
          _currentMemory(0),
          _maxMemory(0),  // 0 = unlimited
          _useGlobalObjMap(enableGlobalObjMap) {
        // Pre-size power of 2 for efficient modulo
        size_t powerOf2 = 1;
        while (powerOf2 < numShards) powerOf2 <<= 1;
        _shardMask = powerOf2 - 1;

        // Reserve space and create each shard
        _shards.reserve(powerOf2);
        for (size_t i = 0; i < powerOf2; ++i) {
            _shards.emplace_back(std::make_unique<Shard>());
        }

        // Default memory sizer - uses a fixed estimate per object
        // Users can call setMemorySizer() for accurate tracking with types that have memoryUsage()
        _memorySizer = [](const T*) -> size_t {
            // Default: assume 256 bytes per object (reasonable estimate for cache entries)
            // For accurate tracking, call setMemorySizer() with a custom function
            return 256;
        };
    }

    // Helper to create a memory sizer for types with memoryUsage() method
    // Usage: cache.setMemorySizer(ShardedLRUCache::makeMemorySizer());
    static MemorySizer makeMemorySizer() {
        return [](const T* obj) -> size_t {
            if (!obj) return 0;
            return static_cast<size_t>(obj->memoryUsage());
        };
    }

    ~ShardedLRUCache() = default;

    // ========== Memory Budget Configuration ==========

    // Set maximum memory budget (0 = unlimited)
    // When budget is exceeded, LRU entries are evicted until under budget
    void setMaxMemory(size_t bytes) {
        _maxMemory.store(bytes, std::memory_order_relaxed);
        // Trigger eviction if we're now over budget
        if (bytes > 0) {
            evictToMemoryBudget();
        }
    }

    size_t getMaxMemory() const {
        return _maxMemory.load(std::memory_order_relaxed);
    }

    size_t getCurrentMemory() const {
        return _currentMemory.load(std::memory_order_relaxed);
    }

    // Set custom memory sizer function
    void setMemorySizer(MemorySizer sizer) {
        _memorySizer = std::move(sizer);
    }

    // Evict entries until memory usage is under budget
    // Returns number of entries evicted
    size_t evictToMemoryBudget() {
        size_t maxMem = _maxMemory.load(std::memory_order_relaxed);
        if (maxMem == 0) return 0;  // No limit

        size_t evicted = 0;
        while (_currentMemory.load(std::memory_order_relaxed) > maxMem) {
            Node* node = removeOne();
            if (!node) break;  // Nothing left to evict

            // Memory already decremented in removeOne()
            // Node is returned to caller who must handle deletion
            // For LRUDeleteObject/Array/Malloc, the node destructor handles object cleanup
            delete node;
            evicted++;
        }
        return evicted;
    }

    // O(1) add to appropriate shard
    // NOTE: Does NOT automatically evict - caller should call evictToMemoryBudget() at safe points
    // (e.g., after batch insert or commit) because evicting during tree traversal can cause
    // use-after-free when parent nodes are temporarily unpinned.
    Node* add(const Id& id, T* object) {
        // Default: cache owns the object and will delete it on eviction/clear
        return add(id, object, true);
    }

    // O(1) add with explicit ownership control
    // owns_object=true: cache owns object, will delete on eviction/clear (heap-allocated)
    // owns_object=false: cache does NOT own object, won't delete (mmap'd memory)
    // NOTE: Does NOT automatically evict - see add() comment for rationale
    Node* add(const Id& id, T* object, bool owns_object) {
        size_t shardIdx = getShardIndex(id);
        auto& shard = *_shards[shardIdx];
        Node* node = shard.add(id, object, owns_object);

        if (node) {
            // Track memory usage (only if budget is enabled)
            if (_maxMemory.load(std::memory_order_relaxed) > 0) {
                size_t objSize = _memorySizer(object);
                _currentMemory.fetch_add(objSize, std::memory_order_relaxed);
            }

            // Track in global map if enabled
            if (_useGlobalObjMap) {
                std::lock_guard<std::mutex> lock(_globalMapMtx);
                _globalObjMap[object] = shardIdx;
            }

            // NOTE: Do NOT evict here - it's unsafe during tree traversal
            // Caller should call evictToMemoryBudget() explicitly at safe points
        }

        return node;
    }

    // O(1) atomic get-or-create, returns node already pinned
    // Thread-safe: If id exists, pins and returns existing node
    // If id doesn't exist, creates new node with objIfAbsent, pins it, and returns it
    // NOTE: Does NOT automatically evict - see add() comment for rationale
    AcquireResult acquirePinned(const Id& id, T* objIfAbsent) {
        size_t shardIdx = getShardIndex(id);
        auto& shard = *_shards[shardIdx];
        AcquireResult result = shard.acquirePinned(id, objIfAbsent);

        // Track memory and global map if newly created
        if (result.created && result.node) {
            // Track memory usage (only if budget is enabled)
            if (_maxMemory.load(std::memory_order_relaxed) > 0) {
                size_t objSize = _memorySizer(result.node->object);
                _currentMemory.fetch_add(objSize, std::memory_order_relaxed);
            }

            if (_useGlobalObjMap) {
                std::lock_guard<std::mutex> lock(_globalMapMtx);
                _globalObjMap[result.node->object] = shardIdx;
            }
        }

        return result;
    }

    // Atomically acquire a pinned node, persisting only if created
    // persistFn is called exactly once if a new object is inserted
    // Returns AcquireResult with node pointer and created flag
    // NOTE: Does NOT automatically evict - see add() comment for rationale
    template<typename PersistFn>
    AcquireResult acquirePinnedWithPersist(const Id& id, T* objIfAbsent, PersistFn persistFn) noexcept {
        size_t shardIdx = getShardIndex(id);
        auto& shard = *_shards[shardIdx];

        auto result = shard.acquirePinnedWithPersist(id, objIfAbsent, persistFn);

        // Track memory and global map if newly created
        if (result.created && result.node) {
            // Track memory usage (only if budget is enabled)
            if (_maxMemory.load(std::memory_order_relaxed) > 0) {
                size_t objSize = _memorySizer(result.node->object);
                _currentMemory.fetch_add(objSize, std::memory_order_relaxed);
            }

            if (_useGlobalObjMap) {
                std::lock_guard<std::mutex> lock(_globalMapMtx);
                _globalObjMap[result.node->object] = shardIdx;
            }
        }

        return result; // Return full AcquireResult with created flag
    }

    // O(1) get from appropriate shard (with LRU update)
    T* get(const Id& id) {
        auto& shard = *getShard(id);
        return shard.get(id);
    }

    // O(1) peek without LRU update (read-only fast path)
    T* peek(const Id& id) const {
        auto& shard = *getShardConst(id);
        return shard.peek(id);
    }

    // O(1) remove by ID
    // Returns the removed object - caller takes ownership and must delete/free
    T* removeById(const Id& id) {
        auto& shard = *getShard(id);
        T* object = shard.removeById(id);

        if (object) {
            // Decrement memory usage (only if budget is enabled)
            if (_maxMemory.load(std::memory_order_relaxed) > 0) {
                size_t objSize = _memorySizer(object);
                _currentMemory.fetch_sub(objSize, std::memory_order_relaxed);
            }

            // Clean up global map if enabled
            if (_useGlobalObjMap) {
                std::lock_guard<std::mutex> lock(_globalMapMtx);
                _globalObjMap.erase(object);
            }
        }

        return object;  // Transfer ownership to caller
    }

    // O(1) with global map, O(numShards) without
    // Return true if removed, false if not found or pinned
    bool removeByObject(T* object) {
        // Only calculate memory if budget is enabled (avoid virtual call overhead)
        const bool trackMemory = _maxMemory.load(std::memory_order_relaxed) > 0;
        size_t objSize = trackMemory ? _memorySizer(object) : 0;

        if (_useGlobalObjMap) {
            // 1) Read shard index under the global map lock
            size_t shardIdx;
            {
                std::lock_guard<std::mutex> g(_globalMapMtx);
                auto it = _globalObjMap.find(object);
                if (it == _globalObjMap.end()) return false;  // not tracked
                shardIdx = it->second;
            }

            // 2) Do the shard removal WITHOUT the global map lock
            bool removed = _shards[shardIdx]->removeByObject(object);

            // 3) If removed, clean up memory tracking and global map
            if (removed) {
                if (trackMemory) {
                    _currentMemory.fetch_sub(objSize, std::memory_order_relaxed);
                }
                std::lock_guard<std::mutex> g(_globalMapMtx);
                _globalObjMap.erase(object);
            }
            return removed;
        } else {
            // O(numShards) scan path
            for (auto& shard : _shards) {
                if (shard->removeByObject(object)) {
                    if (trackMemory) {
                        _currentMemory.fetch_sub(objSize, std::memory_order_relaxed);
                    }
                    return true;
                }
            }
            return false;
        }
    }

    // Wrapper for legacy API compatibility
    bool remove(T* object) {
        return removeByObject(object);
    }

    // O(1) evict from round-robin shard
    Node* removeOne() {
        // Round-robin through shards for even eviction
        size_t startIdx = _evictCounter.fetch_add(1, std::memory_order_relaxed);

        // Only track memory if budget is enabled
        const bool trackMemory = _maxMemory.load(std::memory_order_relaxed) > 0;

        // Try each shard starting from current position
        for (size_t i = 0; i < _shards.size(); ++i) {
            size_t idx = (startIdx + i) & _shardMask;
            Node* evicted = _shards[idx]->removeOne();
            if (evicted) {
                // Decrement memory usage (only if budget is enabled)
                if (evicted->object) {
                    if (trackMemory) {
                        size_t objSize = _memorySizer(evicted->object);
                        _currentMemory.fetch_sub(objSize, std::memory_order_relaxed);
                    }

                    // Clean up global map
                    if (_useGlobalObjMap) {
                        std::lock_guard<std::mutex> lock(_globalMapMtx);
                        _globalObjMap.erase(evicted->object);
                    }
                }
                return evicted;
            }
        }

        return nullptr; // All shards empty or all nodes pinned
    }

    // Pin/unpin support
    void pin(Node* n, const Id& id) {
        auto& shard = *getShard(id);
        shard.pin(n);
    }

    void unpin(Node* n, const Id& id) {
        auto& shard = *getShard(id);
        shard.unpin(n);
    }

    // Atomically change the key for an existing cache entry.
    // This is needed when COW reallocates a bucket to a new NodeID.
    // The node and object pointer stay the same, only the index key changes.
    // Returns true if successful, false if old_id not found or new_id already exists.
    bool rekey(const Id& old_id, const Id& new_id) {
        size_t oldShardIdx = getShardIndex(old_id);
        size_t newShardIdx = getShardIndex(new_id);

        if (oldShardIdx == newShardIdx) {
            // Same shard - simple rekey within the shard
            return _shards[oldShardIdx]->rekey(old_id, new_id);
        }

        // Different shards - need to move the node between shards
        auto& oldShard = *_shards[oldShardIdx];
        auto& newShard = *_shards[newShardIdx];

        // Check if new_id already exists in the new shard
        if (newShard.peek(new_id) != nullptr) {
            return false;  // new_id already exists
        }

        // Detach the node from old shard (works for both pinned and unpinned)
        Node* node = oldShard.detach_node(old_id);
        if (!node) {
            return false;  // old_id not found
        }

        // Attach to new shard with new_id
        if (!newShard.attach_node(new_id, node)) {
            // Failed to attach - restore to old shard
            oldShard.attach_node(old_id, node);
            return false;
        }

        // Update global object map if enabled
        if (_useGlobalObjMap && node->object) {
            std::lock_guard<std::mutex> lock(_globalMapMtx);
            _globalObjMap[node->object] = newShardIdx;
        }

        return true;
    }

    // Clear all shards
    void clear() {
        for (auto& shard : _shards) {
            shard->clear();
        }

        // Reset memory tracking
        _currentMemory.store(0, std::memory_order_relaxed);

        if (_useGlobalObjMap) {
            std::lock_guard<std::mutex> lock(_globalMapMtx);
            _globalObjMap.clear();
        }
    }

    // Stats for monitoring
    struct Stats {
        size_t totalNodes = 0;
        size_t totalPinned = 0;
        size_t totalEvictable = 0;
        size_t currentMemory = 0;
        size_t maxMemory = 0;
        std::vector<size_t> nodesPerShard;
    };

    Stats getStats() const {
        Stats stats;
        stats.nodesPerShard.reserve(_shards.size());
        stats.currentMemory = _currentMemory.load(std::memory_order_relaxed);
        stats.maxMemory = _maxMemory.load(std::memory_order_relaxed);

        for (const auto& shard : _shards) {
            size_t shardSize = shard->size();
            stats.nodesPerShard.push_back(shardSize);
            stats.totalNodes += shardSize;
            stats.totalPinned += shard->pinnedCount();
            stats.totalEvictable += shard->evictableCount();
        }

        return stats;
    }

    // Detailed stats showing type breakdown and pin counts
    struct DetailedStats {
        size_t dataRecords = 0;
        size_t dataRecordsPinned = 0;
        size_t buckets = 0;
        size_t bucketsPinned = 0;
        size_t totalPinCount = 0;  // Sum of all pin counts
        size_t maxPinCount = 0;    // Highest pin count seen
    };

    // Analyze cache contents by type (requires IRecord interface)
    template<typename Analyzer>
    DetailedStats getDetailedStats(Analyzer isDataRecord) const {
        DetailedStats stats;
        for (const auto& shard : _shards) {
            shard->forEachNode([&](const Node* node) {
                if (!node || !node->object) return;

                bool isData = isDataRecord(node->object);
                uint32_t pinCount = node->getPinCount();

                if (isData) {
                    stats.dataRecords++;
                    if (pinCount > 0) stats.dataRecordsPinned++;
                } else {
                    stats.buckets++;
                    if (pinCount > 0) stats.bucketsPinned++;
                }

                stats.totalPinCount += pinCount;
                if (pinCount > stats.maxPinCount) stats.maxPinCount = pinCount;
            });
        }
        return stats;
    }

    // Semantic cache coherence helpers for XTree durability layer
    // Lookup or add a cache entry for a given key and record pointer.
    // Ensures coherence between CacheNode->object and the current record.
    // Note: add() handles memory tracking internally
    Node* lookup_or_attach(const Id& key, T* record) {
        return lookup_or_attach(key, record, true);  // Default: owns object
    }

    // Lookup or add with explicit ownership control
    // owns_object=false for DURABLE mode (mmap'd memory, don't delete)
    Node* lookup_or_attach(const Id& key, T* record, bool owns_object) {
        Node* cn = find_node(key);
        if (cn) {
            if (cn->object != record)
                cn->object = record;
            return cn;
        }
        return this->add(key, record, owns_object);  // add() tracks memory
    }

    // Lookup cache node only (no insert).
    Node* find(const Id& key) {
        return find_node(key);
    }

    // Attach a new object pointer to an existing key if present.
    Node* refresh(const Id& key, T* record) {
        Node* cn = find_node(key);
        if (cn) cn->object = record;
        return cn;
    }

private:
    // Internal helper to get Node* for a given key
    Node* find_node(const Id& key) {
        auto& shard = *getShard(key);
        return shard.find_node_internal(key);
    }
    std::vector<std::unique_ptr<Shard>> _shards;
    size_t _shardMask;  // For fast modulo with power-of-2
    mutable std::atomic<size_t> _evictCounter;

    // Memory budget tracking
    std::atomic<size_t> _currentMemory;  // Current memory usage in bytes
    std::atomic<size_t> _maxMemory;      // Maximum allowed memory (0 = unlimited)
    MemorySizer _memorySizer;            // Function to get object memory size

    // Optional global object->shard mapping for O(1) removeByObject
    bool _useGlobalObjMap;
    std::unordered_map<T*, size_t> _globalObjMap;
    mutable std::mutex _globalMapMtx;

    // Get shard index for an ID
    size_t getShardIndex(const Id& id) const {
        size_t hash = std::hash<Id>{}(id);
        return hash & _shardMask;
    }

    // Hash-based shard selection
    Shard* getShard(const Id& id) {
        return _shards[getShardIndex(id)].get();
    }

    const Shard* getShardConst(const Id& id) const {
        return _shards[getShardIndex(id)].get();
    }
};

// RAII helper for sharded cache - adds a pin/unpin pair
template<typename Cache, typename Node, typename Id>
class ShardedScopedPin {
    Cache& cache;
    Node* node;
    Id id;
public:
    ShardedScopedPin(Cache& c, Node* n, const Id& i)
        : cache(c), node(n), id(i) {
        if (node) cache.pin(node, id);
    }
    ~ShardedScopedPin() {
        if (node) cache.unpin(node, id);
    }

    ShardedScopedPin(const ShardedScopedPin&) = delete;
    ShardedScopedPin& operator=(const ShardedScopedPin&) = delete;
    ShardedScopedPin(ShardedScopedPin&&) = delete;
    ShardedScopedPin& operator=(ShardedScopedPin&&) = delete;
};

// RAII helper for acquirePinned - manages the initial pin from acquire
template<typename Cache>
class ShardedScopedAcquire {
    using Id = typename Cache::IdType;
    using T = typename Cache::CachedObjectType;

    Cache& cache;
    typename Cache::Node* node;
    Id id;
    bool created;

public:
    ShardedScopedAcquire(Cache& c, const Id& i, T* objIfAbsent)
        : cache(c), id(i) {
        auto result = cache.acquirePinned(id, objIfAbsent);
        node = result.node;
        created = result.created;
    }

    ~ShardedScopedAcquire() noexcept {
        if (node) cache.unpin(node, id);
    }

    typename Cache::Node* get() const { return node; }
    typename Cache::Node* operator->() const { return node; }
    bool wasCreated() const { return created; }

    ShardedScopedAcquire(const ShardedScopedAcquire&) = delete;
    ShardedScopedAcquire& operator=(const ShardedScopedAcquire&) = delete;
    ShardedScopedAcquire(ShardedScopedAcquire&&) = delete;
    ShardedScopedAcquire& operator=(ShardedScopedAcquire&&) = delete;
};

// RAII helper for acquirePinnedWithPersist - manages pinning/unpinning with persistence
template<typename Cache, typename T>
class ShardedScopedAcquireWithPersist {
    using Id = typename Cache::IdType;
    Cache& cache;
    typename Cache::Node* node = nullptr;
    Id id{};
    bool created = false; // true if node was newly inserted into cache

public:
    template<typename PersistFn>
    explicit ShardedScopedAcquireWithPersist(Cache& c, const Id& i, T* objIfAbsent, PersistFn persistFn)
        : cache(c), id(i) {
        auto result = cache.acquirePinnedWithPersist(id, objIfAbsent, persistFn);
        node = result.node;
        created = result.created;
    }

    ~ShardedScopedAcquireWithPersist() noexcept {
        if (node) cache.unpin(node, id);
    }

    typename Cache::Node* get() const noexcept { return node; }
    typename Cache::Node* operator->() const noexcept { return node; }
    bool wasCreated() const noexcept { return created; }
    explicit operator bool() const noexcept { return node != nullptr; }

    ShardedScopedAcquireWithPersist(const ShardedScopedAcquireWithPersist&) = delete;
    ShardedScopedAcquireWithPersist& operator=(const ShardedScopedAcquireWithPersist&) = delete;
    ShardedScopedAcquireWithPersist(ShardedScopedAcquireWithPersist&&) = delete;
    ShardedScopedAcquireWithPersist& operator=(ShardedScopedAcquireWithPersist&&) = delete;
};

} // namespace xtree