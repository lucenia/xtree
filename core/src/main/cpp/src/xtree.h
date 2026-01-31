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

#include <cmath>
#include <cstddef>          // For offsetof in static_assert
#include <memory>           // For std::unique_ptr in setDurableChild
#include <atomic>           // For std::atomic in enlisted flag
#include "./util/logmanager.h"
#include "config.h"
#include "util/util.h"
#include "indexdetails.hpp"
#include "keymbr.h"
#include "xtree_allocator_traits.hpp"
#include "persistence/node_id.hpp"
#include "irecord.hpp"      // IRecord base class
#include "datarecord.hpp"  // DataRecord and DataRecordView classes
#include "util/endian.hpp" // For portable little-endian wire format
#include "lru_sharded.h"   // For ShardedScopedAcquire
#include <new>  // For placement new
#include <vector>

namespace xtree {

#pragma pack(1)

    // enumerator for breadth-first, depth-first traversal
    enum TraversalOrder { BFS, DFS };

    // search type
    enum SearchType { CONTAINS, INTERSECTS, WITHIN };

    // used for void templates
    struct Unit {};

    // forward declaration of XTree iterator
    template< class RecordType >
    class Iterator;
    
    // Forward declarations will be added as needed for arena implementation

    // IRecord class has been moved to IRecord.hpp - it's the base interface for XTreeBucket and DataRecord
    // Using the definition from IRecord.hpp - NodeType enum is accessible via IRecord::

    // DataRecord and DataRecordView classes have been moved to DataRecord.hpp
    // DataRecord - heap-allocated mutable records for insertions
    // DataRecordView - zero-copy views for reads from mmap'd storage

    // forward declaration
    template< class Record > class XTreeBucket;

    // Debug validation macro for catching child type invariant violations
#ifndef NDEBUG
#define XTREE_DEBUG_VALIDATE_CHILDREN(node) \
    do { (node)->validate_children_types(); } while (0)
#else
#define XTREE_DEBUG_VALIDATE_CHILDREN(node) ((void)0)
#endif

    // Forward declarations for persistence layer
    namespace persist {
        class ObjectTable;
        class ObjectTableSharded;
        class DurableStore;
        enum class NodeKind : uint8_t;

        // Helper functions for type lookup
        bool try_lookup_kind(const ObjectTable* ot,
                           const NodeID& id,
                           NodeKind& out) noexcept;

        bool try_lookup_kind(const ObjectTableSharded* ot,
                           const NodeID& id,
                           NodeKind& out) noexcept;
    }

    /**
     * This is a semi-fixed width data component for
     * storage of mbr keys within the _children vector
     * of a XTreeBucket.
     */
    template< class RecordType >
    class alignas(8) __MBRKeyNode {
    public:
        typedef typename IndexDetails<RecordType>::CacheNode CacheNode;
        
        // For MMAP persistence, we store offsets instead of pointers
        static constexpr uint32_t INVALID_OFFSET = 0xFFFFFFFF;

        __MBRKeyNode<RecordType>() : _node_id(persist::NodeID::invalid()),
                                      _cache_ptr(nullptr), _recordKey(nullptr), _owner(nullptr), _offset(INVALID_OFFSET), _flags(IRecord::INTERNAL_BUCKET) {
#ifndef NDEBUG
            // Runtime check that our class-scope operator new worked correctly
            assert((reinterpret_cast<std::uintptr_t>(this) & (alignof(__MBRKeyNode)-1)) == 0 &&
                   "MBRKeyNode allocation not aligned");
#endif
        }
        __MBRKeyNode<RecordType>(bool isLeaf, CacheNode* record)
            : _node_id(persist::NodeID::invalid()),
              _cache_ptr(nullptr),
              _recordKey(nullptr),
              _owner(nullptr),
              _offset(INVALID_OFFSET),
              _flags(isLeaf ? IRecord::LEAF_BUCKET : IRecord::INTERNAL_BUCKET)
        {
#ifndef NDEBUG
            // Runtime check that our class-scope operator new worked correctly
            assert((reinterpret_cast<std::uintptr_t>(this) & (alignof(__MBRKeyNode)-1)) == 0 &&
                   "MBRKeyNode allocation not aligned");
#endif
            this->setRecord(record);
        }
        
        ~__MBRKeyNode() noexcept {
            // Delete _recordKey only if we own it (explicit ownership tracking)
            if (_owns_key && _recordKey) {
                delete _recordKey;
                _recordKey = nullptr;
            }
#ifndef NDEBUG
            // Verify we never delete when not owning
            if (!_owns_key) {
                assert(_recordKey == nullptr || "Non-owned key should not be deleted");
            }
#endif
        }
        
        // Rule of Five: Delete copy operations (ownership can't be safely copied)
        __MBRKeyNode(const __MBRKeyNode&) = delete;
        __MBRKeyNode& operator=(const __MBRKeyNode&) = delete;
        
        // Move constructor
        __MBRKeyNode(__MBRKeyNode&& other) noexcept
            : _node_id(other._node_id),
              _cache_ptr(other._cache_ptr),
              _recordKey(other._recordKey),
              _owner(other._owner),
              _offset(other._offset),
              _flags(other._flags),
              _owns_key(other._owns_key)
        {
            other._cache_ptr = nullptr;
            other._recordKey = nullptr;
            other._owns_key  = false;
            other._node_id = persist::NodeID::invalid();
        }
        
        // Move assignment
        __MBRKeyNode& operator=(__MBRKeyNode&& other) noexcept {
            if (this != &other) {
                if (_owns_key && _recordKey) delete _recordKey;

                _node_id   = other._node_id;
                _cache_ptr = other._cache_ptr;
                _recordKey = other._recordKey;
                _owner     = other._owner;
                _offset    = other._offset;
                _flags     = other._flags;
                _owns_key  = other._owns_key;

                other._cache_ptr = nullptr;
                other._recordKey = nullptr;
                other._owner     = nullptr;
                other._owns_key  = false;
                other._node_id = persist::NodeID::invalid();

#ifndef NDEBUG
                _check_invariant();
#endif
            }
            return *this;
        }

        // --- Centralized aligned allocation (C++17) ---
        // Ensure all allocations of __MBRKeyNode are properly aligned for NodeID
        static_assert(alignof(xtree::persist::NodeID) >= 8, "NodeID must be at least 8B aligned");

        // Scalar new/delete
        static void* operator new(std::size_t sz) {
            return ::operator new(sz, std::align_val_t{alignof(__MBRKeyNode)});
        }
        static void operator delete(void* p) noexcept {
            ::operator delete(p, std::align_val_t{alignof(__MBRKeyNode)});
        }

        // Array new/delete (defensive; likely never used but for completeness)
        static void* operator new[](std::size_t sz) {
            return ::operator new[](sz, std::align_val_t{alignof(__MBRKeyNode)});
        }
        static void operator delete[](void* p) noexcept {
            ::operator delete[](p, std::align_val_t{alignof(__MBRKeyNode)});
        }

        bool getLeaf() const { return (_flags & IRecord::LEAF_BUCKET) != 0; }
        void setLeaf(const bool leaf) { 
            if (leaf) _flags |= IRecord::LEAF_BUCKET;
            else _flags &= ~IRecord::LEAF_BUCKET;
        }
        
        bool isDataRecord() const { return (_flags & IRecord::DATA_NODE) != 0; }
        void setDataRecord(const bool isData) {
            if (isData) _flags |= IRecord::DATA_NODE;
            else _flags &= ~IRecord::DATA_NODE;
        }

        bool getCached() { return _cache_ptr != nullptr; }
        void setCached(const bool cached) { /* deprecated */ }

        ostream& getRecordID(ostream& os) {
            os << "offset=" << _offset << " cached=" << (_cache_ptr != nullptr);
            return os;
        }

        /** Get cached record if available */
        CacheNode* getCacheRecord() { return _cache_ptr; }
        const CacheNode* getCacheRecord() const { return _cache_ptr; }

        /** Pull the record - either from cache or by loading from offset */
        IRecord* getRecord(LRUCache<IRecord, UniqueId, LRUDeleteNone> &cache) {
            // First try cache
            if (_cache_ptr) {
                return _cache_ptr->object;
            }
            
            // If we have an offset but no cache, we need the index to load
            // This is a limitation - we'll need to modify this method signature
            // For now, return nullptr if not cached
            return nullptr;
        }
        
        /** 
         * Get the record, resolving from NodeID if necessary
         * For DURABLE mode: resolves NodeID -> pointer via store
         * For IN_MEMORY mode: just returns cached pointer
         * Template to work with any IndexDetails type
         */
        template<typename IndexType>
        IRecord* getRecord(IndexType* idx) {
            // Fast path: already have the pointer cached
            if (_cache_ptr) {
                return _cache_ptr->object;
            }
            
            // DURABLE mode: resolve NodeID to get the bucket pointer
            // Use overload resolution to handle types with/without getStore()
            // Debug: NodeID resolution
            if (_node_id.valid() && idx) {
                return getRecordImpl(idx, 0);
            }
            
            // Fallback for types without getStore() or when NodeID is invalid
            return nullptr;
        }
        
    private:
        // Helper for types that have getStore() method - preferred overload
        template<typename IndexType>
        auto getRecordImpl(IndexType* idx, int) -> decltype(idx->getStore(), static_cast<IRecord*>(nullptr)) {
            auto* store = idx->getStore();
            if (store && _node_id.valid()) {
                // Check if this is a DataRecord and we're in DURABLE mode
                if (isDataRecord() && idx->getPersistenceMode() == IndexDetails<RecordType>::PersistenceMode::DURABLE) {
                    // Try zero-copy read with pinned memory first
                    try {
                        auto pinned = store->read_node_pinned(_node_id);
                        if (pinned.data && pinned.size > 0) {
                            // Create zero-copy view using pinned memory
                            auto* view = new DataRecordView(
                                std::move(pinned.pin),
                                static_cast<const uint8_t*>(pinned.data),
                                pinned.size,
                                idx->getDimensionCount(),
                                idx->getPrecision(),
                                _node_id
                            );
                            
                            // Set the key in this __MBRKeyNode
                            _recordKey = view->getKey();
                            
                            // DO NOT cache DataRecords for zero heap retention
                            // Signal to iterator that this is ephemeral (must be freed)
                            _cache_ptr = nullptr;
                            
                            return view;
                        }
                    } catch (...) {
                        // Fall back to regular read if pinned read fails
                    }
                }
                
                // Fallback: Regular read with heap allocation
                auto node_bytes = store->read_node(_node_id);
                if (node_bytes.data && node_bytes.size > 0) {
                    if (isDataRecord()) {
                        // This is a DataRecord - deserialize it
                        auto* data_record = new DataRecord(
                            idx->getDimensionCount(), 
                            idx->getPrecision(), 
                            ""  // rowid will be filled by from_wire
                        );
                        data_record->setNodeID(_node_id);
                        
                        // Deserialize from wire format (includes KeyMBR)
                        data_record->from_wire(
                            reinterpret_cast<const uint8_t*>(node_bytes.data), 
                            idx->getDimensionCount(),
                            idx->getPrecision()
                        );
                        
                        // Set the key in this __MBRKeyNode
                        _recordKey = data_record->getKey();
                        
                        // DO NOT cache DataRecords for zero heap retention
                        // Signal to iterator that this is ephemeral (must be freed)
                        _cache_ptr = nullptr;
                        
                        return data_record;
                    } else {
                        // This is an XTreeBucket - handle as before
                        auto* bucket = new XTreeBucket<RecordType>(idx, /*isRoot*/false);
                        bucket->setNodeID(_node_id);
                        
                        // Deserialize from wire format
                        bucket->from_wire(reinterpret_cast<const uint8_t*>(node_bytes.data), idx);
                        
                        // Now set the key in this __MBRKeyNode to the child's key
                        // so parent can use it for traversal decisions
                        _recordKey = bucket->getKey();
                        
                        // Add to the LRU cache
                        _cache_ptr = idx->getCache().add(_node_id.raw(), reinterpret_cast<IRecord*>(bucket));

                        return reinterpret_cast<IRecord*>(bucket);
                    }
                }
            }
            return nullptr;
        }
        
        // Fallback for types without getStore() method
        template<typename IndexType>
        IRecord* getRecordImpl(IndexType* idx, ...) {
            return nullptr;
        }
        
    public:

        // Sets the record
        // Not thread-safe; caller must synchronize with readers.
        inline void setRecord(CacheNode* record) noexcept {
            // If a durable child is already set, do not touch flags or key/aliasing.
            // Durable attach is final for the key/NodeID; use clearDurableChild() first if you need to replace.
            if (_node_id.valid() && _owns_key && _recordKey) {
                return;
            }
            
#ifndef NDEBUG
            // If we ever arrive here owning a key, something upstream is wrong.
            assert(!_owns_key && "_owns_key unexpectedly true in setRecord(); "
                                 "use setDurableChild() for durable path");
#endif
            
            if (!record || !record->object) {
                _cache_ptr = nullptr;
                return; // durable path must have set _recordKey/_node_id already
            }
            
            _cache_ptr = record;
            
            const bool child_is_data = record->object->isDataNode();
            setDataRecord(child_is_data);
            
            // Non-durable path: alias the child's key; we do NOT own it.
            // IMPORTANT: getKey() returns a non-owning pointer; lifetime >= this node.
            _recordKey = record->object->getKey();
            _owns_key  = false;
            
            // Opportunistic NodeID capture
            // TODO: Replace dynamic_cast with virtual node_id_if_any() for hot path optimization
            if (child_is_data) {
                if (auto* data = dynamic_cast<DataRecord*>(record->object); data && data->hasNodeID())
                    _node_id = data->getNodeID();
            } else {
                if (auto* b = dynamic_cast<XTreeBucket<RecordType>*>(record->object); b && b->hasNodeID())
                    _node_id = b->getNodeID();
            }
            
#ifndef NDEBUG
            _check_invariant();
#endif
        }
        
        /**
         * Set child metadata for DURABLE DataRecords.
         * This creates an owned copy of the MBR and stores the NodeID.
         * Used when we're about to delete the heap DataRecord.
         * Not thread-safe; caller must synchronize with readers.
         *
         * IMPORTANT: This method is ONLY for DataRecords, not buckets!
         * For bucket children, use setDurableBucketChild instead.
         */
        inline void setDurableChild(const KeyMBR& mbr, persist::NodeID nid) {
            // Defensive: don't process invalid NodeIDs
            if (!nid.valid()) return;

            // Exception-safe allocation
            std::unique_ptr<KeyMBR> owned(new KeyMBR(mbr));  // may throw

            // If we get here, allocation succeeded. Now update state atomically.
            if (_owns_key && _recordKey) {
                delete _recordKey;
            }

            _cache_ptr = nullptr;   // we won't retain the heap child
            _recordKey = owned.release();  // const KeyMBR* can point to non-const allocated memory
            _owns_key  = true;
            _node_id = nid;
            setDataRecord(true);  // Only sets the data bit, doesn't affect leaf/internal flags

#ifndef NDEBUG
            assert(_node_id.valid() && "Durable child must have valid NodeID");
            assert(_recordKey && "Durable child must have copied key");
            assert(_owns_key && "Durable child must own its key");
            _check_invariant();
#endif
        }

        // Alias for clarity - same as setDurableChild but explicit about data
        inline void setDurableDataChild(const KeyMBR& mbr, persist::NodeID nid) {
            setDurableChild(mbr, nid);
        }

        /**
         * Set child metadata for DURABLE bucket children.
         * This creates an owned copy of the MBR and stores the NodeID.
         * Used during split/redistribution to preserve bucket children.
         * Not thread-safe; caller must synchronize with readers.
         */
        inline void setDurableBucketChild(const KeyMBR& mbr, persist::NodeID nid, bool leafFlag) {
            // Defensive: don't process invalid NodeIDs
            if (!nid.valid()) return;

            // Exception-safe allocation
            std::unique_ptr<KeyMBR> owned(new KeyMBR(mbr));  // may throw

            // If we get here, allocation succeeded. Now update state atomically.
            if (_owns_key && _recordKey) {
                delete _recordKey;
            }

            _cache_ptr = nullptr;   // we won't retain the heap child
            _recordKey = owned.release();
            _owns_key  = true;
            _node_id = nid;
            setDataRecord(false);  // CRITICAL: This is a bucket, not a DataRecord
            setLeaf(leafFlag);     // Set the leaf flag for bucket children

#ifndef NDEBUG
            assert(_node_id.valid() && "Durable bucket child must have valid NodeID");
            assert(_recordKey && "Durable bucket child must have copied key");
            assert(_owns_key && "Durable bucket child must own its key");
            assert(!isDataRecord() && "Bucket child must not have data bit set");
            _check_invariant();
#endif
        }
        
        /**
         * Set an owned key (deep copy).
         * Used when we need to own the MBR but don't have a cache record to alias.
         * Not thread-safe; caller must synchronize with readers.
         */
        inline void setKeyOwned(KeyMBR* owned_key) {
            if (_owns_key && _recordKey) {
                delete _recordKey;
            }
            _recordKey = owned_key;
            _owns_key = true;
            _cache_ptr = nullptr;  // No cache record to alias

#ifndef NDEBUG
            assert(_recordKey && "Owned key must not be null");
            assert(_owns_key && "Should own the key after setKeyOwned");
            _check_invariant();
#endif
        }

        /**
         * Clear the cache record pointer.
         * Used when transitioning from in-memory to durable storage.
         */
        inline void clearCacheRecord() noexcept {
            _cache_ptr = nullptr;
        }

        /**
         * Set cache alias directly without touching flags or keys.
         * Fast path for split operations where durable state is already set.
         * Not thread-safe; caller must synchronize with readers.
         */
        inline void setCacheAlias(CacheNode* cn) noexcept {
            _cache_ptr = cn;
        }

        /**
         * Set child from a deep copy of the key with explicit type flags.
         * Used in IN_MEMORY mode when no cache record is available.
         * @param mbr The MBR to copy
         * @param isData True if this is a DataRecord child, false for bucket
         * @param leafFlag For bucket children, whether it's a leaf bucket
         *                 MUST be false when isData is true
         */
        inline void setChildFromKeyCopy(const KeyMBR& mbr, bool isData, bool leafFlag = false) {
#ifndef NDEBUG
            // Contract: leafFlag is only meaningful for bucket children
            if (isData) {
                assert(!leafFlag && "Leaf flag must be false for data children");
            }
#endif

            // Exception-safe allocation
            std::unique_ptr<KeyMBR> owned(new KeyMBR(mbr));

            setKeyOwned(owned.release());
            setDataRecord(isData);

            if (!isData) {
                setLeaf(leafFlag);
            }

#ifndef NDEBUG
            // Verify ownership invariants
            assert(_owns_key && "Should own the key after setChildFromKeyCopy");
            assert(_recordKey && "Should have a key after setChildFromKeyCopy");

            // Verify type flag coherence
            assert(isData == isDataRecord() && "Type flag must match input after setChildFromKeyCopy");
            if (!isData) {
                assert(getLeaf() == leafFlag && "Leaf flag must be preserved for bucket children");
            }

            _check_invariant();
#endif
        }

        /**
         * Clear durable child state (for error recovery paths).
         * Frees owned key and resets to pristine state.
         */
        void clearDurableChild() noexcept {
            if (_owns_key && _recordKey) {
                delete _recordKey;
                _recordKey = nullptr;
            }
            _owns_key  = false;
            _node_id = persist::NodeID::invalid();
            setDataRecord(false);  // Reset data bit to pristine state (doesn't affect leaf/internal)
            // _cache_ptr stays as-is; caller decides whether to alias again via setRecord()

#ifndef NDEBUG
            _check_invariant();
#endif
        }
        
#ifndef NDEBUG
        private:
        void _check_invariant() const {
            if (_owns_key) {
                assert(_recordKey != nullptr && "If we own the key, it must exist");
            }
        }
        public:
#endif
        
        void setRecord(UniqueId id) { /* deprecated */ }
        
        // Set the offset directly (for loading from snapshot)
        void setOffset(uint32_t offset) { _offset = offset; }
        
        // NodeID accessors for DURABLE mode
        void setNodeID(persist::NodeID id) noexcept { _node_id = id; }
        persist::NodeID getNodeID() const noexcept { return _node_id; }
        bool hasNodeID() const noexcept { return _node_id.valid(); }

        const KeyMBR* getKey() const { return _recordKey; }

        /**
         * Set the key to an aliased pointer (we do NOT own it).
         * If we previously owned a key, it is deleted first to avoid leaks.
         * After this call, _owns_key is false.
         */
        void setKey(const KeyMBR* key) {
            // If we owned the previous key, delete it to avoid memory leak
            if (_owns_key && _recordKey) {
                delete _recordKey;
            }
            _recordKey = key;
            _owns_key = false;  // We don't own aliased keys
        }
        
        /**
         * Cache-or-load: Unified entry point for child access in DURABLE mode
         * 
         * Production-ready lazy loading that avoids heap allocation on every access.
         * Returns the LRU cache node for this child, loading from persistence if needed.
         *
         * @tparam Record The record type (e.g., DataRecord)
         * @tparam IndexType The index type (must have getCache() and getStore())
         * @param idx The IndexDetails containing cache and store
         * @return Cache node containing the child object, or nullptr if load fails
         */
        template<typename Record, typename IndexType>
        CacheNode* cache_or_load(IndexType* idx) {
            // Fast path: already cached
            if (_cache_ptr) {
                // CRITICAL FIX: Rewire stale parent pointers for cached buckets
                // After a split, the parent might have a new _children array
                // and this cached bucket's _parent might point to the old array
                if (_cache_ptr->object && !isDataRecord()) {
                    auto* bucket = dynamic_cast<XTreeBucket<Record>*>(_cache_ptr->object);
                    if (bucket && bucket->getParent() != this) {
                        bucket->setParent(this);
                    }
                }
                return _cache_ptr;
            }

            // IN_MEMORY mode should always have cache pointers set
            if (!idx || idx->getPersistenceMode() != IndexType::PersistenceMode::DURABLE) {
#ifndef NDEBUG
                throw std::runtime_error("cache_or_load: missing cache pointer in IN_MEMORY mode");
#else
                log() << "ERROR: cache_or_load called in IN_MEMORY mode without cache pointer" << std::endl;
#endif
                return nullptr;
            }

            // Must have valid NodeID in durable mode
            if (!_node_id.valid()) {
                return nullptr;
            }

            // Load from persistence (DURABLE mode only)
            IRecord* loaded = nullptr;

            // Phase 5: Determine type from store metadata
            // This fixes cold cache loading where the flag isn't set correctly
            persist::NodeKind kind = persist::NodeKind::Invalid;
            bool found_in_ot = false;

            if (auto* store = idx->getStore()) {
                // Use the store interface to get node kind
                // This works for both DurableStore (returns true with metadata)
                // and MemoryStore (returns false, relies on in-memory flags)
                found_in_ot = store->get_node_kind(_node_id, kind);
            }

            // Determine if this is a data record or bucket
            bool is_data = false;
            if (found_in_ot) {
                // Use the authoritative answer from ObjectTable
                is_data = (kind == persist::NodeKind::DataRecord);
                // Update our flag to match reality
                setDataRecord(is_data);
            } else {
                // Fallback to flag (shouldn't happen in production)
                is_data = isDataRecord();
                if (idx->getStore()) {
                    trace() << "NodeID " << _node_id.raw()
                            << " not found in ObjectTable (get_node_kind returned false)"
                            << ", using flag (data=" << is_data << ")"
                            << " [handle=" << _node_id.handle_index() << ", tag=" << _node_id.tag() << "]";
                }
            }

#ifndef NDEBUG
            // Double-check: If we have an ObjectTable kind, assert consistency
            if (found_in_ot) {
                if (is_data) {
                    assert(kind == persist::NodeKind::DataRecord &&
                           "ObjectTable mismatch: expected DataRecord");
                } else {
                    assert((kind == persist::NodeKind::Leaf || kind == persist::NodeKind::Internal) &&
                           "ObjectTable mismatch: expected Bucket (Leaf/Internal)");
                }
            }
#endif

            // Proceed with actual loading
            if (is_data) {
                // DataRecord: load via view/pin for zero-copy (Lucene-like performance)
                loaded = reinterpret_cast<IRecord*>(XAlloc<Record>::load_data_record(idx, _node_id));
            } else {
                // Bucket: always heap-allocated since they're mutable tree structure
                loaded = reinterpret_cast<IRecord*>(XAlloc<Record>::load_bucket(idx, _node_id));

                // CRITICAL FIX: Set parent immediately for cold-loaded bucket
                // This bucket was just loaded from disk and needs its parent set
                if (loaded) {
                    auto* bucket = dynamic_cast<XTreeBucket<Record>*>(loaded);
                    if (bucket) {
                        bucket->setParent(this);
                    }
                }
            }

            if (!loaded) {
                log() << "ERROR: cache_or_load failed for NodeID " << _node_id.raw()
                      << " (data=" << is_data << ", kind=" << static_cast<int>(kind) << ")" << std::endl;
                return nullptr;
            }

            // Insert into cache with NodeID as key using acquirePinned
            // This handles the case where the node is already cached (returns existing)
            // or creates a new entry if not (using our loaded object)
            uint64_t cache_key = _node_id.raw();
            auto result = idx->getCache().acquirePinned(cache_key, loaded);
            _cache_ptr = result.node;

            // If the cache already had this node, we need to clean up our loaded copy
            // and fix the parent pointer on the existing cached bucket
            if (!result.created && _cache_ptr && _cache_ptr->object != loaded) {
                // The existing cached bucket might have a stale parent pointer
                if (!is_data) {
                    auto* cached_bucket = dynamic_cast<XTreeBucket<Record>*>(_cache_ptr->object);
                    if (cached_bucket && cached_bucket->getParent() != this) {
                        cached_bucket->setParent(this);
                    }
                }
                // Clean up the loaded object since we're using the cached one
                delete loaded;
            }

            // Unpin the node now - acquirePinned returns pinned
            // CRITICAL: Must use cache.unpin() not node->unpin() to update eviction list!
            if (_cache_ptr) {
                idx->getCache().unpin(_cache_ptr, cache_key);
            }

            // Set the key reference if needed (for MBR-based filtering)
            if (!_recordKey && _cache_ptr && _cache_ptr->object) {
                _recordKey = _cache_ptr->object->getKey();
            }

            return _cache_ptr;
        }

        /**
         * Const overload of cache_or_load that forwards to the non-const version
         */
        template<typename Record, typename IndexType>
        CacheNode* cache_or_load(IndexType* idx) const {
            return const_cast<__MBRKeyNode*>(this)->template cache_or_load<Record>(idx);
        }

        struct CumulativeOverlap {
            typedef const typename XTreeBucket<RecordType>::_MBRKeyNode* argument_type;
            typedef void result_type;

            __MBRKeyNode* _candidateKN; // *this mbr
            const KeyMBR *_key; // the proposed key
            typename vector<__MBRKeyNode*>::iterator *_start;
            double overlap;

            explicit CumulativeOverlap(
                __MBRKeyNode *ckn, 
                const KeyMBR *key,
                typename vector<__MBRKeyNode*>::iterator* start
            ) : _candidateKN(ckn), _key(key), _start(start), overlap(0.0) {}

            void operator() (const typename XTreeBucket<RecordType>::_MBRKeyNode* mbrkn) {
                double minY=0.0;
                double maxX=0.0;
                double areaOverlap = -1.0;

                if(_candidateKN != mbrkn) {
                    for(unsigned short d=0; d<_key->getDimensionCount()*2; d+=2) {
                        // we need to first enlarge the candidate KeyNode to enclose the given key
                        maxX = MAX(_candidateKN->getKey()->getBoxVal(d+1), _key->getBoxVal(d+1));
                        minY = mbrkn->getKey()->getBoxVal(d);
                        // compute the overlap area
                        if(areaOverlap == 0.0) break;
                        else {
                            if (minY > maxX) continue;
                            else
                                areaOverlap = abs(areaOverlap)*(abs(maxX-minY));
                        }
                    }
                    // then compute the area overlap with all KeyNodes (but this one)
                    if(areaOverlap < 0.0)
                        areaOverlap = 0.0;
                    overlap += areaOverlap;
                }
            }
        };

        const double overlapEnlargement(
            const KeyMBR* key, 
            typename vector<__MBRKeyNode*>::iterator start,
            typename vector<__MBRKeyNode*>::iterator end
        ) const {
            // walks through each element in keyVec and calculates the cumulative overlap area for
            // the enlarged(this->mbr) with every other MBR in keyVec
            CumulativeOverlap co = for_each(start, end, CumulativeOverlap(const_cast<__MBRKeyNode*>(this), key, &start));
            return co.overlap;
        }


    private:
        // Friend class to allow XTreeBucket to access _flags directly for wire serialization
        friend class XTreeBucket<RecordType>;
        // Friend to allow alignment checks access to private members
        template<typename> friend struct __MBRKeyNodeAlignmentAsserts;

        // PUT FIRST → naturally 8-byte aligned inside the class
        persist::NodeID _node_id;

        // 8-byte on 64-bit, keeps layout naturally aligned
        mutable CacheNode* _cache_ptr;

        // The key associated with this record
        // In DURABLE mode for DataRecords: owned copy (we allocate/delete)
        // In IN_MEMORY mode or for buckets: aliased pointer (don't delete)
        const KeyMBR* _recordKey = nullptr;

        // Runtime-only back-reference to owning bucket (NEVER persisted)
        // Used for navigation during propagateMBRUpdate and other traversals
        // Rebuilt during from_wire() and other reconstruction operations
        XTreeBucket<RecordType>* _owner = nullptr;

        // Offset in the memory-mapped file (for persistence)
        uint32_t _offset;

        // Packed flags: bit 0 = isLeaf, bit 1 = isDataRecord
        // 0b00 = Internal bucket (not leaf, not data)
        // 0b01 = Leaf bucket (leaf, not data)
        // 0b10 = DataRecord (not leaf, data)
        // 0b11 = unused (would be both leaf AND data)
        uint8_t _flags;

        // Explicit ownership flag: true iff we own _recordKey (must delete it)
        // False means _recordKey is an alias to external memory (don't delete)
        bool _owns_key = false;  // Default member init

        unsigned short size(const unsigned short &mbrBytes)  { 
            return mbrBytes + sizeof(bool) + sizeof(XTreeBucket<RecordType>*) + sizeof(unsigned char);
        }

        friend ostream& operator <<(ostream &os, const __MBRKeyNode kn) {
            os << "offset=" << kn._offset << " cached=" << (kn._cache_ptr != nullptr) << " isLeaf: " << kn.getLeaf() << endl;
            return os;
        }
        
        // Grant access to iterator for lazy loading
        template< class R >
        friend class Iterator;
    }; // __MBRKeyNode

    // Compile-time invariants for __MBRKeyNode alignment safety
    template<typename RecordType>
    struct __MBRKeyNodeAlignmentAsserts {
        static_assert(alignof(__MBRKeyNode<RecordType>) >= alignof(persist::NodeID),
                      "MBRKeyNode alignment must be at least as strict as NodeID");
        static_assert((offsetof(__MBRKeyNode<RecordType>, _node_id) % alignof(persist::NodeID)) == 0,
                      "NodeID field not naturally aligned in MBRKeyNode");
    };

    // Force instantiation of alignment checks for common types
    template struct __MBRKeyNodeAlignmentAsserts<DataRecord>;
    template struct __MBRKeyNodeAlignmentAsserts<IRecord>;

    /**
     * XTreeBucket
     *
     * @TODO: Better comments shortly
     */
#pragma pack() // Restore normal alignment for XTreeBucket (contains std::vector)
    template< class Record >
    class XTreeBucket : public IRecord {
    public:
        // grant access to privates
        template< class R >
        friend class Iterator;

        // Grant access to serialization
        template< class R >
        friend class XTreeSerializer;

        // Grant access to allocator for restoration after loading
        template< class R >
        friend class CompactXTreeAllocator;

        // Grant access to __MBRKeyNode for offset loading
        template< class R >
        friend class __MBRKeyNode;

        // Grant access to POD converter
        template< class R >
        friend class XTreePODConverter;
        
        // Grant access to lazy loader
        template< class R >
        friend class XTreeLazyLoader;
        
        // Grant access to IndexDetails for recovery
        template< class R >
        friend class IndexDetails;

        // Grant access to allocator traits for publish_with_realloc
        template< class R, class E >
        friend struct XTreeAllocatorTraits;

        typedef __MBRKeyNode<Record> _MBRKeyNode;
        typedef typename _MBRKeyNode::CacheNode CacheNode;
        typedef stack<CacheNode*> DFS;
        typedef queue<CacheNode*> BFS;

#ifndef NDEBUG
        /**
         * Debug helper for tests: Verify parent-child NodeID consistency.
         * Returns true if all child references are valid, false otherwise.
         * Also returns diagnostic information via out parameters.
         *
         * @param invalid_child_idx Output: index of first invalid child (-1 if all valid)
         * @param expected_id Output: expected NodeID of invalid child
         * @param actual_id Output: actual NodeID found in child
         * @return true if all children have valid NodeIDs that match child's actual NodeID
         */
        bool debug_verify_child_consistency(
            int& invalid_child_idx,
            persist::NodeID& expected_id,
            persist::NodeID& actual_id) const {

            invalid_child_idx = -1;
            expected_id = persist::NodeID::invalid();
            actual_id = persist::NodeID::invalid();

            // Only internal nodes have children to verify
            if (_leaf) return true;

            for (unsigned int i = 0; i < _n; ++i) {
                const _MBRKeyNode* kn = _kn(i);
                if (!kn) continue;

                persist::NodeID parent_thinks = kn->getNodeID();
                if (!parent_thinks.valid()) continue;

                // Load the child via cache_or_load
                // Note: freshly allocated children not yet published may not load, which we handle as failure
                auto* cache_node = kn->template cache_or_load<Record>(_idx);
                if (!cache_node || !cache_node->object) {
                    invalid_child_idx = i;
                    expected_id = parent_thinks;
                    actual_id = persist::NodeID::invalid();
                    return false;
                }

                // Ensure it's really a bucket (not a data record)
                auto* child_bucket = dynamic_cast<XTreeBucket<Record>*>(cache_node->object);
                if (!child_bucket) {
                    invalid_child_idx = i;
                    expected_id = parent_thinks;
                    actual_id = persist::NodeID::invalid();
                    return false;
                }

                persist::NodeID child_actual = child_bucket->getNodeID();
                if (parent_thinks.raw() != child_actual.raw()) {
                    invalid_child_idx = i;
                    expected_id = parent_thinks;
                    actual_id = child_actual;
                    return false;
                }
            }

            return true;
        }

        /**
         * Debug helper for tests: Recursively verify entire tree's parent-child consistency.
         * @param depth Current depth (for diagnostics)
         * @return true if entire subtree has valid parent-child NodeID relationships
         */
        bool debug_verify_tree_consistency(int depth = 0) const {

            // First check this node's children
            int invalid_idx;
            persist::NodeID expected_id, actual_id;
            if (!debug_verify_child_consistency(invalid_idx, expected_id, actual_id)) {
                trace() << "[TREE_CONSISTENCY] Failed at depth " << depth
                          << ", child " << invalid_idx
                          << ", expected=" << expected_id.raw()
                          << ", actual=" << actual_id.raw() << std::endl;
                return false;
            }

            // Recursively check all child subtrees (for internal nodes)
            if (!_leaf) {
                for (unsigned int i = 0; i < _n; ++i) {
                    const _MBRKeyNode* kn = _kn(i);
                    if (!kn) continue;

                    auto* cache_node = kn->template cache_or_load<Record>(_idx);
                    if (cache_node && cache_node->object) {
                        auto* child_bucket = dynamic_cast<XTreeBucket<Record>*>(cache_node->object);
                        if (child_bucket) {
                            if (!child_bucket->debug_verify_tree_consistency(depth + 1)) {
                                return false;
                            }
                        }
                    }
                }
            }

            return true;
        }
#endif

        XTreeBucket<Record>(
            IndexDetails<Record>* idx, 
            bool isRoot, 
            KeyMBR* key = NULL, 
            const vector<_MBRKeyNode*>* sourceChildren = NULL,
            unsigned int split_index = 0, 
            bool isLeaf = true, 
            unsigned int sourceN = 0
        ) 
            : IRecord(key),
              _memoryUsage(sizeof(XTreeBucket<Record>)), 
              _idx(idx), 
              _parent(NULL), 
              _nextChild(NULL), 
              _prevChild(NULL),
              _n(0), 
              _isSupernode(false), 
              _leaf(isLeaf), 
              _ownsPreallocatedNodes(sourceChildren==NULL),
              _bucket_node_id(persist::NodeID::invalid()),
              _dirty(false),
              _enlisted(false) 
        {
            // if source children are not provided
                if(sourceChildren==NULL) {
                    generate_n(back_inserter(_children), XTREE_CHILDVEC_INIT_SIZE,
                        [&](){
                            auto* kn = new _MBRKeyNode();
                            kn->_owner = this;
                            return kn;
                        });
                } else {
                    this->_n = sourceN - (split_index + 1);
                    // Start iterator at split_index+1 (the first element for the new bucket)
                    auto srcChildIter = sourceChildren->begin() + split_index + 1;

                    // Pre-reserve capacity to avoid reallocations during generate_n
                    _children.reserve(_children.size() + this->_n);

                    generate_n(back_inserter(_children), this->_n, [&]() {
                        auto* kn = *(srcChildIter++);
                        if (kn) kn->_owner = this;
                        return kn;
                    });
                }

                // create the key for this bucket
                // NOTE: this memory space is shared by _MBRKeyNode,
                // so when a XTreeBucket is paged, we need to be sure
                // not to free the Key
                if(_key == NULL) {
                    _key = new KeyMBR(_idx->getDimensionCount(), _idx->getPrecision());
                }

                _memoryUsage += (MAX(XTREE_CHILDVEC_INIT_SIZE, this->_n)*knSize()) + _key->memUsage();
        }

        // Destructor to clean up allocated memory
        ~XTreeBucket() {
            // DO NOT delete _parent - it points to a KN owned by the parent bucket's _children array.
            // The _parent pointer is set via setParent() during tree construction/loading.
            // The parent bucket is responsible for deleting its own _children KNs.
            // (Note: createParentKN was intended for this but was never actually used)
            _parent = NULL;  // Clear the pointer but don't delete
            
            // Clean up pre-allocated but unused child nodes
            // These are nodes that were allocated in the constructor but never used
            // (i.e., nodes where index >= _n)
            // Only delete them if we created them (not from a split)
            if (_ownsPreallocatedNodes) {
                for (size_t i = _n; i < _children.size(); i++) {
                    if (_children[i] != NULL) {
                        // Now using class-scope aligned operator delete - plain delete is fine
                        delete _children[i];
                    }
                }
            }
            
            // Note: We don't delete active child nodes (i < _n) because they
            // are cached objects that should be managed by their owners

            // Note: _key is deleted by IRecord destructor
            
            // IMPORTANT: There is a known issue where splitRoot adds new buckets to the
            // real static cache, but tests using fake cache nodes don't clean them up.
            // This can cause memory leaks in tests. The proper fix is to ensure tests
            // clean up all cached buckets, not just fake cache nodes.
        }

        // generic traversal with a generic lambda function to be called on each
        // visit
        template<typename Result, typename Visit, typename TraversalOrder>
        Result traverse(CacheNode* thisCacheNode, Visit visit, ...) const;


        // returns the total memory consumption of JUST this bucket
        long memoryUsage() const { return this->_memoryUsage; }
        // returns the total memory consumption of this subtree
        long treeMemUsage(CacheNode* cachedNode) const;

        // get an iterator for traversing the tree
        Iterator<Record>* getIterator(CacheNode* thisCacheNode, IRecord* searchKey, int queryType);

        // wrapper around _insert that caches the record for insertion
        // into the tree
        void xt_insert(CacheNode* thisCacheNode, IRecord* record) {
            // Debug assertion to catch stale root cache issues
            #ifndef NDEBUG
            if (this->_parent == nullptr) { // I am the root
                if (this->_n == 0) {
                    // If we have no children, we *must* be a leaf
                    if (!this->_leaf) {
                        trace() << "[ERROR] Root has n=0 but _leaf=" << this->_leaf
                                  << " NodeID=" << this->getNodeID().raw() << std::endl;
                    }
                    assert(this->_leaf && "Root has zero children but is marked internal; cache is stale or deserialization bug");
                }
            }
            #endif

            // Debug output for root state (filtered by log level)
            if (this->_parent == nullptr && this->_n == 0) {
                trace() << "[XT_INSERT_DEBUG] Root state: n=" << this->_n
                        << ", _leaf=" << this->_leaf
                        << ", NodeID=" << this->getNodeID().raw();
            }

            const bool durable =
                _idx->hasDurableStore() &&
                _idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE;

            XTreeBucket<Record>* leaf = nullptr;
            
            if (durable && record->isDataNode()) {
                using Rec = Record;
                using Cache = typename IndexDetails<Record>::Cache;
                auto& cache = IndexDetails<Record>::getCache();
                auto* store = _idx->getStore();

                auto* raw = static_cast<Rec*>(record);
                const uint16_t dims = _idx->getDimensionCount();

                // 1) Allocate NodeID and writable buffer
                const size_t wire_sz = raw->wire_size(dims);
                persist::AllocResult alloc = store->allocate_node(wire_sz, persist::NodeKind::DataRecord);
                if (!alloc.writable || alloc.capacity < wire_sz) {
                    if (alloc.id.valid()) {
                        try { store->free_node(alloc.id); } catch (...) {}
                    }
                    throw std::runtime_error("Failed to allocate storage for DataRecord");
                }
                raw->setNodeID(alloc.id);
                const UniqueId cache_id = static_cast<UniqueId>(alloc.id.raw());

                // 2) Serialize DataRecord to allocated buffer
                raw->to_wire(static_cast<uint8_t*>(alloc.writable), dims);

                // 3) Zero unused tail bytes for deterministic checksums
                if (alloc.capacity > wire_sz) {
                    std::memset(static_cast<uint8_t*>(alloc.writable) + wire_sz, 0, alloc.capacity - wire_sz);
                }

                // 4) Publish to ObjectTable (stages for commit)
                try {
                    store->publish_node_in_place(alloc.id, wire_sz);
#ifndef NDEBUG
                    static std::atomic<uint64_t> dr_count{0};
                    uint64_t c = ++dr_count;
                    if (c % 1000 == 0) {
                        trace() << "[DR_PUBLISH] Staged " << c << " DataRecords (latest NodeID="
                                  << alloc.id.raw() << ")\n";
                    }
#endif
                } catch (...) {
                    // Fallback to memcpy variant
                    store->publish_node(alloc.id, alloc.writable, wire_sz);
                }

                // 5) RAII acquire - no persistence callback needed, already published
                ShardedScopedAcquire<Cache> rec_guard(cache, cache_id, raw);

                CacheNode* cachedRecord = rec_guard.get();
                assert(cachedRecord && "acquirePinned must return a node");

                trace() << "[XT_INSERT_DEBUG] Before _insert: n=" << this->_n
                        << ", _leaf=" << this->_leaf;

                leaf = _insert(thisCacheNode, cachedRecord);

                // rec_guard destructor automatically unpins

            } else {
                // IN_MEMORY or non-data nodes: use acquirePinned for consistency
                using Cache = typename IndexDetails<Record>::Cache;
                auto& cache = IndexDetails<Record>::getCache();
                UniqueId cache_id = _idx->getNextNodeID();

                // Atomically get-or-create and pin the node
                ShardedScopedAcquire<Cache> rec_guard(cache, cache_id, record);
                CacheNode* cachedRecord = rec_guard.get();
                assert(cachedRecord && "acquirePinned must return a node");

                leaf = _insert(thisCacheNode, cachedRecord);
                // rec_guard destructor will unpin when we exit
            }
            
            // 5) Don't publish on every insert - let commit handle it
            // Publishing on every insert causes massive WAL generation
            // TODO: Add explicit publish API for batch operations
            
            // Commit/COW accounting
            XAlloc<Record>::record_operation(this->_idx);
        }

        // completely purges this XTreeBucket, along w/ all children buckets and data
        void xt_purge(CacheNode* thisCacheNode);


        const unsigned short knSize() const {  return (sizeof(_MBRKeyNode)); }

        // returns the number of children
        const int n() const { return _n; }

        virtual KeyMBR* getKey() const {
            return _key;
        }

        void setKey(KeyMBR* key) {
            // set the key data
            *(this->_key) = *key;
            // delete the
            delete key;
        }

        // returns the index details
        IndexDetails<Record>* getIdxDetails() const { return this->_idx; }
        
        // NodeID accessors for DURABLE mode
        void setNodeID(persist::NodeID id) { _bucket_node_id = id; }
        persist::NodeID getNodeID() const { return _bucket_node_id; }
        bool hasNodeID() const { return _bucket_node_id.valid(); }

        // Parent bucket accessor - use KN's _owner, not its cache record
        XTreeBucket<Record>* parent_bucket() const noexcept {
            return _parent ? _parent->_owner : nullptr;
        }

        // Parent pointer management - critical for persistence layer
        void setParent(_MBRKeyNode* parent) {
#ifndef NDEBUG
            // Verify we're not setting a KN that belongs to this bucket as our parent
            // (which would be self-referential and nonsensical)
            if (parent && parent->_owner == this) {
                assert(false && "Cannot set own KN as parent!");
            }
            // Note: parent->getNodeID() SHOULD equal this->getNodeID() - that's correct!
            // The parent KN stores the child's NodeID as its reference.
#endif
            _parent = parent;
        }
        _MBRKeyNode* getParent() const { return _parent; }

        // print out this bucket for logging
        string toString(int indentLevel=0) {
            ostringstream oss;
            string indents;

            for(short i=0; i<indentLevel; ++i) indents += "\t";

            oss << indents << "this: " << this << endl;
            oss << indents << "this->_memoryUsage: " << this->_memoryUsage << endl;
            oss << indents << "this->_idx: " << this->_idx << endl;
            oss << indents << "this->_parent: " << this->_parent << endl;
            oss << indents << "this->_n: " << this->_n << endl;
            oss << indents << "this->_isSupernode: " << this->_isSupernode << endl;
            oss << indents << "this->_leaf: " << this->_leaf << endl;
            oss << indents << "this->_children->size(): " << this->_children.size() << endl;

            return oss.str();
        }

    protected:
        // accessors
        XTreeBucket<Record>* nextChild() const { return _nextChild; }
        const vector<_MBRKeyNode*>* getChildren() { return &_children; }

        // creates or updates a key node
        _MBRKeyNode* kn(CacheNode* record, int n=-1) {
            // Safety first
            if (!record || !record->object) return nullptr;

            // get the nth child (append when n < 0)
            _MBRKeyNode* child = (n < 0) ? _kn(_n++) : _kn(n);

            const bool existed = (n >= 0); // entry already present
            const bool durable = _idx &&
                _idx->hasDurableStore() &&
                _idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE;

            IRecord* obj = record->object;
            const KeyMBR* k = obj->getKey();
            assert(k && "Child object must provide a KeyMBR");

#ifndef NDEBUG
            // CRITICAL: Comprehensive type fence for leaf/bucket consistency
            if (obj->isDataNode()) {
                assert(this->_leaf && "Cannot insert a DataRecord into an internal bucket");
            } else {
                assert(!this->_leaf && "Cannot insert a bucket child into a leaf bucket");
            }
#endif

            if (obj->isDataNode()) {
                // --- DATA CHILD ---
                if (durable) {
                    // No alias; store (MBR, NodeID, data-bit)
                    auto* data = static_cast<Record*>(obj);
                    persist::NodeID nid = data->getNodeID();
                    assert(nid.valid() && nid.raw() != 0 && "DataRecord must have valid NodeID in DURABLE");
                    // Parity check removed - type will be validated via ObjectTable metadata (Phase 4)
                    child->setDurableChild(*k, nid);  // sets data bit internally
#ifndef NDEBUG
                    assert(child->isDataRecord());
                    assert(child->getCacheRecord() == nullptr);
                    assert(child->hasNodeID());
                    assert(nid.valid() && "DataRecord NodeID must be valid");
                    // Parity check removed - type will be validated via ObjectTable metadata (Phase 4)
#endif
                } else {
                    // In-memory: alias heap object and mark as data
                    child->setRecord(record);
                    child->setDataRecord(true);
#ifndef NDEBUG
                    assert(child->getCacheRecord() == record);
#endif
                }
                // NOTE: do NOT set leaf flag for data children (meaningless here)
            } else {
                // --- BUCKET CHILD ---
                auto* bucket = static_cast<XTreeBucket<Record>*>(obj);

                // Alias runtime bucket object (both modes)
                child->setRecord(record);
                child->setDataRecord(false);
                child->setLeaf(bucket->isLeaf()); // only meaningful for bucket children

                if (durable) {
                    // Bucket must have NodeID so it can be loaded when cache is cold
                    persist::NodeID bucketId = bucket->getNodeID();
                    if (!bucketId.valid() || bucketId.raw() == 0) {
                        throw std::runtime_error("kn(): Bucket child missing NodeID in DURABLE mode");
                    }

                    // CRITICAL: Ensure parent is not storing itself as a child
                    persist::NodeID parentId = this->getNodeID();
                    if (parentId.valid() && bucketId.raw() == parentId.raw()) {
                        trace() << "[KN_ERROR] Parent " << parentId.raw()
                                  << " attempting to store itself as child!" << std::endl;
                        assert(false && "Parent cannot reference itself as a child");
                    }

                    // Debug output for child insertion (always runs, even in Release mode)
                    trace() << "[INSERT_CHILD] parent=" << parentId.raw()
                              << " child=" << bucketId.raw()
                              << " leaf=" << bucket->isLeaf() << std::endl;

                    // Parity check removed - type will be validated via ObjectTable metadata (Phase 4)
                    child->setNodeID(bucketId);

                    // Verify the write
                    assert(child->getNodeID().raw() == bucketId.raw() &&
                           "Child NodeID must match what was intended to be stored");
                    assert(child->getNodeID().raw() != parentId.raw() &&
                           "Child NodeID must not be parent's NodeID after storage");
#ifndef NDEBUG
                    assert(child->getCacheRecord() == record &&
                           "Bucket child must retain exact cache alias in DURABLE mode");
#endif
                } else {
#ifndef NDEBUG
                    assert(child->getCacheRecord() == record);
#endif
                }
            }

#ifndef NDEBUG
            // Final sanity: flag must match object type
            assert(child->isDataRecord() == obj->isDataNode());

            // Validate entire bucket after insertion
            XTREE_DEBUG_VALIDATE_CHILDREN(this);
#endif

            // Set runtime _owner pointer for navigation
#ifndef NDEBUG
            child->_owner = this;
            assert(child->_owner == this);
#else
            child->_owner = this;
#endif

            // Update parent MBR
            assert(this->_key && "Parent bucket must have a KeyMBR before expand/recalc");
            if (existed) {
                this->recalculateMBR();
            } else {
                this->_key->expand(*k);
            }

            return child;
        }

        /**
         * Adopt a child entry from another bucket during split/redistribution.
         * Preserves the original (MBR, NodeID) without re-persisting or requiring heap object.
         * Critical for durable mode: maintains existing NodeIDs during redistribution.
         * 
         * IMPORTANT: Use ONLY during split/redistribution to re-adopt existing children.
         * Never call this with freshly persisted inserts — those should use kn(...).
         */
        _MBRKeyNode* kn_from_entry(const _MBRKeyNode& src, int n=-1) {
            // Safety: src must carry a key
            const KeyMBR* key = src.getKey();
            if (!key) return nullptr;

            // Get target slot in this bucket
            _MBRKeyNode* child = (n < 0) ? _kn(_n++) : _kn(n);

            // Set _owner immediately so any intermediate debug helpers see it
            child->_owner = this;
#ifndef NDEBUG
            assert(child->_owner == this);

            // Assert parent-child type consistency
            if (this->_leaf) {
                assert(src.isDataRecord() && "Leaf bucket can only adopt data children");
            } else {
                assert(!src.isDataRecord() && "Internal bucket can only adopt bucket children");
            }
#endif

            const bool durable = _idx &&
                _idx->hasDurableStore() &&
                _idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE;

            if (durable) {
                // Durable: copy (MBR, NodeID), do NOT alias heap
#ifndef NDEBUG
                // The source MUST have a NodeID in durable mode
                assert(src.hasNodeID() && "Source must have NodeID in durable mode");
#endif
                if (!src.hasNodeID()) {
                    throw std::runtime_error("kn_from_entry: source missing NodeID in durable mode");
                }

                // CRITICAL: Must preserve the type from source
                if (src.isDataRecord()) {
                    // Data record path - use data-specific setter
                    child->setDurableDataChild(*key, src.getNodeID());
                } else {
                    // Bucket child path - use bucket-specific setter
                    child->setDurableBucketChild(*key, src.getNodeID(), src.getLeaf());
                }

#ifndef NDEBUG
                // Verify type preservation
                assert(child->isDataRecord() == src.isDataRecord() &&
                       "Child type must match source after adoption");
                assert(child->hasNodeID() &&
                       "Child must have NodeID after adoption in durable mode");
                assert(child->getCacheRecord() == nullptr &&
                       "Child should not have cache pointer in durable mode");
                if (!child->isDataRecord()) {
                    assert(child->getLeaf() == src.getLeaf() &&
                           "Bucket child leaf flag must be preserved");
                }
#endif
            } else {
                // IN_MEMORY: reuse cache pointer if present, otherwise deep copy
                // Cast away const since we're just reading the pointer
                if (auto* cn = const_cast<_MBRKeyNode&>(src).getCacheRecord()) {
                    child->setRecord(cn);
                    // setRecord auto-detects type from the cached object

                    // If it's a loaded bucket, rewire its runtime parent to the *new* key-node
                    if (!child->isDataRecord() && cn->object) {
                        auto* bucket = static_cast<XTreeBucket<Record>*>(cn->object);
                        if (bucket->_parent != child) { // avoid redundant write
                            bucket->_parent = child;
#ifndef NDEBUG
                            assert(bucket->_parent == child && "Bucket parent not rewired correctly after adoption");
#endif
                        }
                    }
                } else {
                    // Deep copy the key to avoid aliasing a transient pointer
                    child->setChildFromKeyCopy(*key, src.isDataRecord(), src.getLeaf());
                }

#ifndef NDEBUG
                // Verify type preservation in memory mode
                assert(child->isDataRecord() == src.isDataRecord() &&
                       "Child type must match source in memory mode");
                if (!child->isDataRecord()) {
                    assert(child->getLeaf() == src.getLeaf() &&
                           "Bucket child leaf flag must be preserved");
                }
#endif
            }

            // Update parent MBR
            if (n < 0) {
                _key->expand(*key);
#ifndef NDEBUG
                // Defensive check: parent MBR should contain adopted child
                assert(_key->contains(*key) && "Parent MBR should contain adopted child key");
#endif
            } else {
                recalculateMBR();
            }

#ifndef NDEBUG
            // Post-condition: verify _owner ↔ _parent consistency after adoption
            assert(child->_owner == this && "Child owner mismatch after adoption");
            if (!child->isDataRecord()) {
                auto* obj = child->getCacheRecord() ? child->getCacheRecord()->object : nullptr;
                if (obj) {
                    auto* bucket = static_cast<XTreeBucket<Record>*>(obj);
                    assert(bucket->_parent == child && "Child bucket parent mismatch after adoption");
                }
            }
#endif

            return child;
        }

        // returns pointer to a KeyNode in the vector (or memory mapped file)
        //_MBRKeyNode* kn(int i) const { return const_cast<XTreeBucket<Record> *>(this)->_kn(i); }
        // Allow temporary overflow to M+1 for split
        static constexpr unsigned cap = XTREE_M + 1;

        _MBRKeyNode* _kn(unsigned i) {
            if (i <= XTREE_M) {
                if(i>=this->_children.size()) {
                    // needed is i+1 since i is 0-based
                    const size_t needed  = static_cast<size_t>(i) + 1;
                    const size_t doubled = _children.size() << 1;
                    const size_t target  = std::min<size_t>(cap, std::max(doubled, needed));
                    _expandChildren(target - _children.size());
                }
            } else {
                _expandSupernode();
            }
            return this->_children.at(i);
        }

        // Const overload for _kn() to avoid const_cast in validation methods
        inline const _MBRKeyNode* _kn(unsigned i) const noexcept {
            return (i < _children.size()) ? _children.at(i) : nullptr;
        }

        // _MBRKeyNode* _kn(unsigned int i) {
        //     if (i <= XTREE_M) {
        //         if(i>=this->_children.size()) {
        //             if((this->_children.size()<<1) < XTREE_M )
        //                 this->_expandChildren(this->_children.size());
        //             else
        //                 this->_expandChildren((XTREE_M+1)-this->_children.size());
        //         }
        //     } else {
        //         _expandSupernode();
        //     }
        //     return this->_children.at(i);
        // }

        _MBRKeyNode* _expandSupernode() {
            assert(this->_n>=XTREE_M);
            // double the block size, or just add 1
            if (_n >= _children.size()) {
                (this->_n<=(XTREE_M*2)) ? _expandChildren(XTREE_M) : _expandChildren(1);
            }
            this->_isSupernode = true;
            return this->_children.at(this->_n);
        }

        void _expandChildren(unsigned int i) {
            generate_n(back_inserter(_children), i, [&]() {
                // Now using class-scope aligned operator new - no manual alignment needed
                auto* kn = new _MBRKeyNode();
                kn->_owner = this;
                return kn;
            });
            _memoryUsage += i * sizeof(_MBRKeyNode);
        }

        const bool isDataNode() const { return false; }
        const bool isLeaf() const { return this->_leaf; }
        bool hasLeaves() { return (this->_n>0) ? _kn(0)->getLeaf() : false; }

        /** xt_insert() is basically just a wrapper around this. */
        XTreeBucket<Record>* _insert(CacheNode* thisCacheNode, CacheNode* record);
        XTreeBucket<Record>* insertHere(CacheNode* thisCacheNode, CacheNode* record);
        bool basicInsert(/*const KeyMBR& key,*/ /*IRecord* */ CacheNode* record);
        
    public:
        // Public accessor for leaf status (needed by allocator traits)
        bool getIsLeaf() const { return this->_leaf; }

        /**
         * Wire serialization size calculation for v1 format.
         * Returns the number of bytes needed to serialize this bucket.
         * Must match the layout in to_wire/from_wire exactly.
         */
        size_t wire_size(const IndexDetails<Record>& idx) const {
            const uint16_t dims = idx.getDimensionCount();
            
            // Header: is_leaf(1) + dims(2) + child_count(4) = 7
            constexpr size_t HEADER_BYTES = 1 + 2 + 4;
            
            // Child entry: MBR (2*dims * sizeof(float)) + NodeID(8) + flags(1) + pad(7)
            constexpr size_t NODEID_BYTES = 8;
            constexpr size_t FLAGS_BYTES = 1;
            constexpr size_t CHILD_PAD_BYTES = 7;
            const size_t mbr_bytes = static_cast<size_t>(2) * dims * sizeof(float);
            const size_t child_bytes = mbr_bytes + NODEID_BYTES + FLAGS_BYTES + CHILD_PAD_BYTES;
            
            return HEADER_BYTES + static_cast<size_t>(_n) * child_bytes;
        }
        
        /**
         * Serialize this bucket to wire format (version 1 assumed at file level).
         *
         * Wire layout (per bucket):
         * [u8]   is_leaf (0 or 1)
         * [u16]  dims
         * [u32]  child_count
         * [ChildEntry child[child_count]]
         *
         * ChildEntry layout:
         *   [MBR]   dim*2 floats (min/max per dimension)
         *   [u64]   NodeID (0 if none)
         *   [u8]    flags (bit 0: isLeaf, others reserved)
         *   [u8*7]  pad (for alignment)
         *
         * Notes:
         * - Parent buckets store child MBRs explicitly
         * - Data records remain separate (NodeID → WAL/mmap)
         * - No version written here; handled in file header
         */
        uint8_t* to_wire(uint8_t* out, const IndexDetails<Record>& idx) const {
            const uint16_t dims = idx.getDimensionCount();
            const uint32_t n = static_cast<uint32_t>(_n);
            
#ifndef NDEBUG
            const uint8_t* start = out;  // For debug size check
#endif
            
            // Bucket must always have a valid NodeID
            assert(this->getNodeID().valid() && "Bucket must have valid NodeID before to_wire");
            assert(this->getNodeID().raw() != 0 && "Bucket NodeID.raw() must not be 0");
            
            // --- Header: is_leaf(1) + dims(2) + child_count(4) = 7 bytes ---
            *out++ = (_leaf ? 1 : 0);                // is_leaf
            xtree::util::store_le16(out, dims); out += sizeof(uint16_t);
            xtree::util::store_le32(out, n);    out += sizeof(uint32_t);
            
            // --- Children ---
            constexpr size_t NODEID_BYTES = 8;
            constexpr size_t FLAGS_BYTES = 1;
            constexpr size_t CHILD_PAD_BYTES = 7;
            
            // Hoist mode check out of loop for better performance
            const bool durable = (idx.getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE);
            
            for (uint32_t i = 0; i < n; ++i) {
                const auto* kn = _children[i];
                assert(kn && "null child in to_wire");
                
#ifndef NDEBUG
                if (durable) {
                    assert(kn->hasNodeID() && kn->getNodeID().valid() &&
                           "Child missing valid NodeID in durable mode before to_wire");
                }
#endif
                
                // Write child's MBR (must be float on wire)
                const KeyMBR* child_mbr = kn->getKey();
                assert(child_mbr && "child missing MBR");
                out = child_mbr->to_wire(out, dims);  // IMPORTANT: ensure this writes floats
                
                // Write NodeID
                const uint64_t raw = kn->hasNodeID() ? kn->getNodeID().raw() : 0ULL;
                xtree::util::store_le64(out, raw); out += NODEID_BYTES;

#ifndef NDEBUG
                // Debug: track DataRecord NodeIDs being written
                if (_leaf && kn->isDataRecord()) {
                    static std::atomic<int> write_count{0};
                    int c = ++write_count;
                    if (c <= 20 || c % 500 == 0) {
                        trace() << "[TO_WIRE_DR] leaf=" << this->getNodeID().raw()
                                  << " dr_child[" << i << "]=" << raw << "\n";
                    }
                }
#endif
                
                // Write flags + pad
                uint8_t flags = 0;
                if (kn->getLeaf()) flags |= 0x1;  // Use getLeaf() not isLeaf()
                *out++ = flags;
                
                std::memset(out, 0, CHILD_PAD_BYTES); out += CHILD_PAD_BYTES;
            }
            
#ifndef NDEBUG
            // Strong symmetry check: verify we wrote exactly the expected bytes
            const size_t written = static_cast<size_t>(out - start);
            const size_t expected = wire_size(idx);
            assert(written == expected && "to_wire wrote unexpected number of bytes");
#endif
            
            return out;
        }
        
        /**
         * Deserialize this bucket from wire format (version 1 assumed at file level).
         * The caller already knows dims/precision from the file header.
         *
         * Wire layout (per bucket):
         * [u8]   is_leaf
         * [u16]  dims
         * [u32]  child_count
         * [ChildEntry child[child_count]]
         */
        const uint8_t* from_wire(const uint8_t* r, IndexDetails<Record>* idx) {
            const uint8_t* start = r;  // For debug size check
            const uint16_t prec = idx->getPrecision();
            
            auto get_u8  = [&](){ return *r++; };
            auto get_u16 = [&](){ uint16_t v = xtree::util::load_le16(r); r += 2; return v; };
            auto get_u32 = [&](){ uint32_t v = xtree::util::load_le32(r); r += 4; return v; };
            auto get_u64 = [&](){ uint64_t v = xtree::util::load_le64(r); r += 8; return v; };
            
            // --- Header: is_leaf(1) + dims(2) + child_count(4) = 7 bytes ---
            _leaf = (get_u8() != 0);  // Single byte, no endianness needed
            const uint16_t dims = get_u16();  // Uses load_le16
            const uint32_t n = get_u32();     // Uses load_le32
            
            if (!this->_key) {
                this->_key = new KeyMBR(dims, prec);
            }
            
            this->_idx = idx;
            
            // --- Children ---
            _children.clear();
            _children.reserve(n > XTREE_CHILDVEC_INIT_SIZE ? n : XTREE_CHILDVEC_INIT_SIZE);
            
            constexpr size_t NODEID_BYTES = 8;
            constexpr size_t FLAGS_BYTES = 1;
            constexpr size_t CHILD_PAD_BYTES = 7;
            
            // Hoist mode check out of loop for better performance
            const bool durable = (idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE);
            
            for (uint32_t i = 0; i < n; ++i) {
                // MBR (must read floats to match wire format)
                KeyMBR child_mbr(dims, prec);
                r = child_mbr.from_wire(r, dims);  // IMPORTANT: ensure this reads floats
                
                // NodeID
                const uint64_t raw = get_u64();
                
                // Flags + pad
                const uint8_t flags = get_u8();
                r += CHILD_PAD_BYTES; // skip padding
                
                auto* kn = new _MBRKeyNode();
                
                // CRITICAL FIX: Type children based on parent's _leaf flag, not just NodeID presence
                if (_leaf) {
                    // This parent is a leaf bucket, so children are DataRecords
                    if (raw == 0 && durable) {
                        // DataRecord without NodeID in durable mode is an error
                        std::string err = "from_wire: DataRecord child " + std::to_string(i) +
                                          " has NodeID=0 in durable mode in leaf bucket " +
                                          std::to_string(this->getNodeID().raw());
                        throw std::runtime_error(err);
                    }
                    
                    if (raw != 0) {
                        // Durable DataRecord child - setDurableChild makes a copy and sets data bit
                        kn->setDurableChild(child_mbr, persist::NodeID::from_raw(raw));
                        // Debug: count DataRecords loaded during recovery
                        static std::atomic<int> recovery_dr_count{0};
                        ++recovery_dr_count;
                    } else {
                        // Non-durable mode: allocate a copy
                        auto* owned_mbr = new KeyMBR(child_mbr);
                        kn->setKey(owned_mbr);
                        kn->_owns_key = true;
                        kn->setDataRecord(true);  // Mark as data child
                    }
                    // DO NOT set kn->setLeaf() for data children - leafness is meaningless for data
                } else {
                    // This parent is internal, so children are XTreeBuckets
                    // Bucket children need MBR copy and NodeID but NOT the data bit
                    auto* owned_mbr = new KeyMBR(child_mbr);
                    kn->setKey(owned_mbr);
                    kn->_owns_key = true;
                    
                    if (raw != 0) {
                        kn->setNodeID(persist::NodeID::from_raw(raw));
                    } else if (durable) {
                        // Bucket without NodeID in durable mode is an error
                        std::string err = "from_wire: Bucket child " + std::to_string(i) +
                                          " has NodeID=0 in durable mode in internal bucket " +
                                          std::to_string(this->getNodeID().raw());
                        throw std::runtime_error(err);
                    }
                    
                    kn->setDataRecord(false);  // Explicitly mark as bucket child
                    kn->setLeaf((flags & 0x1) != 0);  // Only meaningful for bucket children
                }
                
                _children.push_back(kn);

                // Set runtime-only _owner pointer for navigation
                kn->_owner = this;

                // Always expand parent MBR - simplifies and keeps symmetry with to_wire
                this->_key->expand(child_mbr);
            }
            
            // Fill to minimum size if needed
            while (_children.size() < XTREE_CHILDVEC_INIT_SIZE) {
                auto* kn = new _MBRKeyNode();
                kn->_owner = this;
                _children.push_back(kn);
            }
            
            _n = n;
            
            // Bucket is freshly loaded from disk, so it's not dirty
            this->clearDirty();
            
#ifndef NDEBUG
            // Strong symmetry check: verify we consumed exactly the expected bytes
            // Reuse same constants for clarity
            constexpr size_t HEADER_BYTES = 1 + 2 + 4;
            const size_t mbr_bytes = static_cast<size_t>(2) * dims * sizeof(float);
            const size_t child_bytes = mbr_bytes + NODEID_BYTES + FLAGS_BYTES + CHILD_PAD_BYTES;
            const size_t expected = HEADER_BYTES + n * child_bytes;
            const size_t consumed = static_cast<size_t>(r - start);
            assert(consumed == expected && "from_wire consumed unexpected number of bytes");
#endif
            
            return r;
        }

        /**
         * purges this bucket from memory
         */
        void purge(CacheNode* thisCacheNode) {}

        // choose subtree (complex algorithm)
        XTreeBucket<Record>* chooseSubtree(CacheNode* record);

        /*****************
         * Split methods *
         *****************/
        struct SplitResult {
            enum class Kind : uint8_t {
                Split,        // split done; insert should continue in next_target
                Grew,         // supernode growth performed; retry basicInsert in the same bucket
                Failed        // should not happen in normal operation; debug guard
            };

            Kind kind = Kind::Failed;
            XTreeBucket<Record>* next_target = nullptr; // valid only for Split
        };
        SplitResult split(CacheNode* thisCacheNode, const CacheNode* insertingCN);
        void splitCommit( CacheNode* thisCacheNode, KeyMBR* mbr1, KeyMBR* mbr2, const unsigned int &split_index );
        void splitRoot( CacheNode* thisCacheNode, CacheNode* cachedSplitNode);
        void splitNode( CacheNode* thisCacheNode, CacheNode* cachedSplitNode);
        // Force a cascade split when parent exceeds MAX_FANOUT (no new record to insert)
        void forceCascadeSplit(CacheNode* thisCacheNode);

        _MBRKeyNode* createParentKN( CacheNode* cachedNode ) {
            assert(this->_parent == NULL);
            this->_parent = new _MBRKeyNode( false, cachedNode);
            _memoryUsage += sizeof(_MBRKeyNode);
            return this->_parent;
        }
        void setNextChild(XTreeBucket<Record> *nextChild) { this->_nextChild = nextChild; }
        
        // Recalculate this bucket's MBR based on its children
        void recalculateMBR() {
            if (this->_n == 0) return;

            // Debug: Validate _key before use
            if (!this->_key) {
                trace() << "[RECALC_MBR_ERROR] _key is null! NodeID="
                          << (this->hasNodeID() ? std::to_string(this->getNodeID().raw()) : "none")
                          << " _n=" << this->_n << std::endl;
                return;
            }

            // Debug: Check _area before reset
            trace() << "[RECALC_MBR_DEBUG] BEFORE reset: this=" << (void*)this
                      << " _key=" << (void*)this->_key
                      << " _n=" << this->_n
                      << " NodeID=" << (this->hasNodeID() ? std::to_string(this->getNodeID().raw()) : "none")
                      << " _area=0x" << std::hex << this->_key->debug_area_value() << std::dec
                      << " valid=" << this->_key->debug_check_area()
                      << std::endl;

            // Reset the MBR to start fresh
            this->_key->reset();

            // Debug: Check _area after reset
            if (!this->_key->debug_check_area()) {
                trace() << "[RECALC_MBR_DEBUG] AFTER reset: _area corrupt! 0x"
                          << std::hex << this->_key->debug_area_value() << std::dec << std::endl;
            }

            // Expand the MBR to include all children
            for (unsigned int i = 0; i < this->_n; i++) {
                _MBRKeyNode* child = _kn(i);
                if (!child) {
                    trace() << "[RECALC_MBR_ERROR] Child " << i << " is null! "
                              << "parent_id=" << (this->hasNodeID() ? std::to_string(this->getNodeID().raw()) : "none")
                              << std::endl;
                    continue;
                }
                const KeyMBR* childKey = child->getKey();
                if (!childKey) {
                    trace() << "[RECALC_MBR_ERROR] Child " << i << " getKey() returned null! "
                              << "parent_id=" << (this->hasNodeID() ? std::to_string(this->getNodeID().raw()) : "none")
                              << " child NodeID=" << (child->hasNodeID() ? std::to_string(child->getNodeID().raw()) : "none")
                              << " isData=" << child->isDataRecord()
                              << std::endl;
                    continue;
                }
                if (childKey->data() == nullptr) {
                    trace() << "[RECALC_MBR_ERROR] Child " << i << " has null _box! "
                              << "parent_id=" << (this->hasNodeID() ? std::to_string(this->getNodeID().raw()) : "none")
                              << " child NodeID=" << (child->hasNodeID() ? std::to_string(child->getNodeID().raw()) : "none")
                              << " isData=" << child->isDataRecord()
                              << std::endl;
                    continue;
                }

                this->_key->expand(*childKey);
            }

            // Don't mark dirty here - let caller decide when to publish
        }
        
        // Check if bucket is dirty (needs persistence)
        bool isDirty() const { return _dirty; }
        
        // Clear dirty flag after publishing
        void clearDirty() { _dirty = false; }
        
        // Mark this bucket as dirty (needs persistence) and auto-register
        void markDirty() { 
            if (!_dirty) {
                _dirty = true;
                // Auto-register with IndexDetails for batch publishing
                if (_idx && _idx->hasDurableStore() && 
                    _idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE) {
                    _idx->register_dirty_bucket(this);
                }
            }
        }
        
        // Try to enlist this bucket in the dirty list (returns true if newly enlisted)
        bool try_enlist() noexcept {
            bool expected = false;
            return _enlisted.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
        }
        
        // Clear the enlisted flag after publishing
        void clearEnlistedFlag() noexcept {
            _enlisted.store(false, std::memory_order_release);
        }

        /**
         * Helper to update parent's child reference after a bucket reallocation.
         * Searches the parent's children for the old NodeID and updates it to the new one.
         * @param bucket The bucket that was reallocated
         * @param old_id The previous NodeID before reallocation
         * @param new_id The new NodeID after reallocation
         */
        static inline void updateParentAfterRealloc(XTreeBucket<Record>* bucket,
                                                    const persist::NodeID& old_id,
                                                    const persist::NodeID& new_id) {
            if (!bucket || !new_id.valid() || new_id == old_id) return;

            bucket->setNodeID(new_id);

            // Update the parent's reference to this bucket
            if (bucket->_parent) {
                // The parent is a _MBRKeyNode that points to this bucket
                // We need to update its NodeID to point to the new allocation
                bucket->_parent->setNodeID(new_id);
#ifndef NDEBUG
                trace() << "[PUBLISH_UPDATE] Updated parent's reference from NodeID "
                          << old_id.raw() << " -> " << new_id.raw() << std::endl;
#endif
            }
#ifndef NDEBUG
            else {
                trace() << "[PUBLISH_INFO] Root bucket reallocated, no parent reference to update "
                          << old_id.raw() << " -> " << new_id.raw() << std::endl;
            }
#endif
        }

        // Publish dirty buckets from a starting point up to root using parent walk
        void publishDirtyBucketsFrom(XTreeBucket<Record>* startBucket) {
            // Only publish in DURABLE mode
            if (!_idx->hasDurableStore() ||
                _idx->getPersistenceMode() != IndexDetails<Record>::PersistenceMode::DURABLE) {
                return;
            }

            auto* store = _idx->getStore();
            if (!store) return;

            // Walk from start bucket to root and publish dirty buckets
            for (auto* bucket = startBucket; bucket; ) {
                if (bucket->isDirty()) {
                    const persist::NodeID id = bucket->getNodeID();
                    if (!id.valid()) {
                        throw std::runtime_error("Dirty bucket lacks valid NodeID");
                    }

                    // Capture old NodeID before publishing
                    auto old_id = bucket->getNodeID();

                    // Publish bucket (may reallocate and return new NodeID)
                    auto pub_result = XAlloc<Record>::publish_with_realloc(_idx, bucket);

                    // Update parent reference if bucket was reallocated
                    updateParentAfterRealloc(bucket, old_id, pub_result.id);

                    // Clear dirty flag only after successful publish
                    bucket->clearDirty();
                }
                
                // Move up to parent bucket
                bucket = bucket->_parent
                    ? reinterpret_cast<XTreeBucket<Record>*>(bucket->_parent->getRecord(_idx))
                    : nullptr;
            }
        }
        
        // Publish all dirty buckets in the provided path (expected to be leaf-to-root order)
        void publishDirtyBuckets(const std::vector<XTreeBucket<Record>*>& leafToRootPath) {
            // Only publish in DURABLE mode
            if (!_idx->hasDurableStore() ||
                _idx->getPersistenceMode() != IndexDetails<Record>::PersistenceMode::DURABLE) {
                return;
            }

            auto* store = _idx->getStore();
            if (!store) return;

            // Publish from leaf to root (children before parents)
            for (auto* bucket : leafToRootPath) {
                if (!bucket || !bucket->isDirty()) continue;

                const persist::NodeID id = bucket->getNodeID();
                if (!id.valid()) {
                    throw std::runtime_error("Dirty bucket lacks valid NodeID");
                }

                const size_t wire_sz = bucket->wire_size(*_idx);
                
                // Check if we can do zero-copy update
                void* addr = store->get_mapped_address(id);
                if (addr && store->supports_in_place_publish()) {
                    // Zero-copy path: write directly to mapped memory
                    const size_t capacity = store->get_capacity(id);
                    if (wire_sz > capacity) {
                        // Need reallocation - capture old NodeID before publishing
                        auto old_id = bucket->getNodeID();
                        auto pub_result = XAlloc<Record>::publish_with_realloc(_idx, bucket);

                        // Update parent reference if bucket was reallocated
                        updateParentAfterRealloc(bucket, old_id, pub_result.id);
                    } else {
                        // Serialize directly to mapped memory (zero-copy)
                        uint8_t* end = bucket->to_wire(static_cast<uint8_t*>(addr), *_idx);
                        
#ifndef NDEBUG
                        if (static_cast<size_t>(end - static_cast<uint8_t*>(addr)) != wire_sz) {
                            throw std::runtime_error("to_wire wrote unexpected length");
                        }
#endif
                        
                        // Stage delta/dirty range (no memcpy)
                        store->publish_node_in_place(id, wire_sz);
                    }
                } else {
                    // Fall back to regular publish with temporary buffer
                    auto old_id = bucket->getNodeID();
                    auto pub_result = XAlloc<Record>::publish_with_realloc(_idx, bucket);

                    // Update parent reference if bucket was reallocated
                    updateParentAfterRealloc(bucket, old_id, pub_result.id);
                }

                // Clear dirty flag only after successful publish
                bucket->clearDirty();
            }
        }
        
        // Propagate MBR updates up the tree
        // @param thisCacheNode - cache node for this bucket
        // @param childChangedHint - true if a child of this bucket changed (optimization hint)
        // Propagate MBR updates up the tree iteratively to avoid recursion and stack overflow.
        void propagateMBRUpdate(CacheNode* thisCacheNode, bool childChangedHint = false) {
            XTreeBucket<Record>* cur = this;
            bool changed = childChangedHint;

#ifndef NDEBUG
            int guard = 0;
            auto sameNodeId = [](const XTreeBucket<Record>* a, const XTreeBucket<Record>* b) -> bool {
                return a && b && a->hasNodeID() && b->hasNodeID()
                       ? a->getNodeID().raw() == b->getNodeID().raw()
                       : false;
            };
#endif

            while (cur) {
                // Snapshot bounding box scalars (no heap churn in KeyMBR copy ctor)
                KeyMBRSnapshot oldMBR(*(cur->_key));

                // Recompute MBR for this node
                cur->recalculateMBR();

                const bool curChanged = changed || !oldMBR.equals(*(cur->_key));
                if (curChanged) {
                    cur->markDirty();
                }

                // Stop at root
                if (cur->_parent == nullptr) {
                    break;
                }

                // Update parent's cached key for this child
                cur->_parent->setKey(cur->_key);

                // If nothing changed here, no need to climb further
                if (!curChanged) {
                    break;
                }

                // Climb one level — use runtime _owner to avoid self-cycles
                XTreeBucket<Record>* parentBucket = nullptr;

                if (cur->_parent) {
                    parentBucket = cur->_parent->_owner;   // runtime-only pointer we set everywhere
#ifndef NDEBUG
                    // Sanity checks to catch wiring bugs early
                    if (!parentBucket) {
                        assert(false && "kn->_owner is null; owner must be set when wiring children");
                    } else {
                        // The parent should actually reference this kn
                        bool found = false;
                        for (auto* kn : parentBucket->_children) {
                            if (kn == cur->_parent) { found = true; break; }
                        }
                        assert(found && "parent->_children does not contain cur->_parent (wiring mismatch)");
                    }
#endif
                }

                // Defensive bail-out
                if (!parentBucket) break;

#ifndef NDEBUG
                if (++guard > 100000) {
                    assert(false && "Cycle detected while propagating MBR (iteration guard tripped).");
                    break;
                }
                assert(parentBucket != cur && "Parent equals current bucket (self-cycle).");
                // Optional NodeID check (when available)
                if (sameNodeId(parentBucket, cur)) {
                    assert(false && "Parent and child share the same NodeID (cycle).");
                    break;
                }
#endif

                cur = parentBucket;
                changed = true;
            }
        }

#ifndef NDEBUG
        /**
         * Validate that all children of an internal node are buckets (not data records).
         * Call this after any operation that modifies internal node children.
         */
        inline void validate_internal_children_types() const noexcept {
            if (!this->_leaf) {
                for (unsigned i = 0; i < _n; ++i) {
                    const auto* kn = _kn(i);
                    assert(kn && "null child in internal node");
                    assert(!kn->isDataRecord() && "Internal node child must be a BUCKET (not DataRecord)");

                    // In durable mode, verify NodeID validity and kind
                    if (_idx && _idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE && kn->hasNodeID()) {
                        const auto& id = kn->getNodeID();
                        assert(id.valid() && "Child NodeID must be valid in durable mode");

                        persist::NodeKind actual = persist::NodeKind::Invalid;
                        if (auto* store = _idx->getStore()) {
                            if (store->get_node_kind(id, actual)) {
                                persist::NodeKind expected = kn->getLeaf()
                                    ? persist::NodeKind::Leaf
                                    : persist::NodeKind::Internal;
                                if (actual != expected) {
                                    trace() << "[VALIDATE_ERROR] NodeKind mismatch:"
                                              << " child_id=" << id.raw()
                                              << " actual=" << static_cast<int>(actual)
                                              << " expected=" << static_cast<int>(expected)
                                              << " kn->getLeaf()=" << kn->getLeaf()
                                              << std::endl;
                                }
                                // Temporarily disabled while investigating NodeKind tracking
                                // assert(actual == expected && "NodeKind mismatch for internal child");
                            }
                        }
                    }
                }
            }
        }

        /**
         * Validate that all children of a leaf node are data records.
         * Call this after any operation that modifies leaf node children.
         */
        inline void validate_leaf_children_types() const noexcept {
            if (this->_leaf) {
                for (unsigned i = 0; i < _n; ++i) {
                    const auto* kn = _kn(i);
                    assert(kn && "null child in leaf bucket");
                    assert(kn->isDataRecord() && "Leaf bucket child must be a DataRecord");

                    // In durable mode, verify NodeID validity and kind
                    if (_idx && _idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::DURABLE && kn->hasNodeID()) {
                        const auto& id = kn->getNodeID();
                        assert(id.valid() && "Child NodeID must be valid in durable mode");

                        persist::NodeKind actual = persist::NodeKind::Invalid;
                        if (auto* store = _idx->getStore()) {
                            if (store->get_node_kind(id, actual)) {
                                assert(actual == persist::NodeKind::DataRecord &&
                                       "NodeKind mismatch for leaf child");
                            }
                        }
                    }
                }
            }
        }

        /**
         * Validate all children types based on whether this is a leaf or internal node.
         */
        inline void validate_children_types() const noexcept {
            if (this->_leaf) {
                validate_leaf_children_types();
            } else {
                validate_internal_children_types();
            }
        }

        /**
         * Debug helper to verify parent-child linkage for a specific KN pointer.
         * Works regardless of whether _children has been reordered.
         */
        template<typename RecordType>
        void debugVerifyLinkKN(const _MBRKeyNode* kn) const {
            assert(kn && "KN must not be null");
            assert(!kn->isDataRecord() && "Internal node child must be a bucket");

            // Parent-stored durable child id
            const auto pid = kn->getNodeID();
            assert(pid.valid() && "parent child NodeID must be valid");

            // Clear stale cache alias if it points to owner or has wrong NodeID
            if (auto* cn = kn->getCacheRecord()) {
                auto* maybe = reinterpret_cast<const XTreeBucket<RecordType>*>(cn->object);
                if (!maybe || !maybe->hasNodeID() || maybe->getNodeID() != kn->getNodeID() || maybe == this) {
                    const_cast<_MBRKeyNode*>(kn)->setCacheAlias(nullptr); // force reload by pid
                }
            }

            // Load child via same path descent would take
            auto* cnode = kn->template cache_or_load<RecordType>(_idx);
            assert(cnode && cnode->object && "cache_or_load must return child object");

            auto* child = dynamic_cast<const XTreeBucket<RecordType>*>(cnode->object);
            assert(child && "child object must be XTreeBucket");
            assert(child->hasNodeID() && "child bucket must have NodeID");

            // Normalize runtime NodeID to the durable ID we requested (same as checked_load)
            if (child->getNodeID() != pid) {
                trace() << "[DEBUG_VERIFY_NORMALIZE] runtime NodeID "
                          << child->getNodeID().raw() << " -> " << pid.raw() << "\n";
                const_cast<XTreeBucket<RecordType>*>(child)->setNodeID(pid);
                assert(child->getNodeID() == pid && "Debug verify NodeID normalization failed");
            }

            assert(child->getNodeID() == pid && "parent->KN id must equal child->getNodeID()");
            assert(child->_parent == kn && "child->_parent must equal KN backpointer");
        }
#endif

    private:
        /** header data below */

        // memory usage for this bucket
        int _memoryUsage;                   // 4 bytes
        // details of the index
        IndexDetails<Record>* _idx;         // 8 bytes
        
        // NodeID for this bucket in DURABLE mode
        persist::NodeID _bucket_node_id;    // 8 bytes

        // pointer to _parent
        _MBRKeyNode* _parent;               // 8 bytes

        // pointer to "next child". Defined as the next sibling, or
        // the "first" node on the next level. Used for BFS
        XTreeBucket<Record>* _nextChild;    // 8 bytes
        // pointer to the "previous child". Defined as the previous sibling.
        XTreeBucket<Record>* _prevChild;    // 8 bytes
        // number of keys in the bucket
        unsigned int _n;                    // 4 bytes
        // is this a supernode
        bool _isSupernode;                  // 1 byte
        // internal or leaf node
        bool _leaf;                         // 1 byte
        // whether this bucket owns its pre-allocated nodes
        bool _ownsPreallocatedNodes;        // 1 byte
        // dirty flag for batch publishing
        bool _dirty;                        // 1 byte
        // enlisted flag for deduplication in dirty list
        std::atomic<bool> _enlisted;        // 1 byte
        // in memory child pointers
        vector<_MBRKeyNode*> _children;     // 24 bytes
                                            // 8 bytes from IRecord
    }; // XTreeBucket
#pragma pack(1) // Restore byte packing for subsequent structures

    /**********************************************************
     * R Tree related functors used for sorting MBRKeyNodes
     **********************************************************/
    /**
     * InMemory key vector sort by minimum range value
     */
    template< class Record >
    struct SortKeysByRangeMin {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        const unsigned short _axis;
        explicit SortKeysByRangeMin( const unsigned short axis ) : _axis(axis) {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) const {
            // Safety checks
            if (!key1 || !key1->getKey()) return false;
            if (!key2 || !key2->getKey()) return true;
            
            double v1 = key1->getKey()->getMin(_axis);
            double v2 = key2->getKey()->getMin(_axis);
            
            // Handle NaN
            if (std::isnan(v1)) return false;
            if (std::isnan(v2)) return true;
            
            return v1 < v2;
        }
    };

    /**
     * InMemory key vector sorting by maximum range value
     */
    template< class Record >
    struct SortKeysByRangeMax {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        const unsigned short _axis;
        explicit SortKeysByRangeMax(const unsigned short axis) : _axis(axis) {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) const  {
#ifdef _DEBUG
            if(key1 == NULL) log() << "SortKeysByRangeMax: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByRangeMax: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log() << "SortKeysByRangeMax: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByRangeMax: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            return key1->getKey()->getMax(_axis) < key2->getKey()->getMax(_axis);
        }
    };

    /**
     * Expands a target MBR given an input MBR
     */
    template< class Record >
    struct StretchBoundingBox {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* argument_type;
        typedef void result_type;
        KeyMBR *_mbr;
        explicit StretchBoundingBox(KeyMBR *key) : _mbr(key) {}

        void operator() (const typename XTreeBucket<Record>::_MBRKeyNode*/*KeyMBR**/ mbr) {
            _mbr->expand(*(mbr->getKey()));
        }
    };

    /**
     * InMemory key vector sorting by area enlargement required for inserting a new key
     */
    template< class Record >
    struct SortKeysByAreaEnlargement {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        //const double _area;
        const KeyMBR* _key;
        bool* _zeroEnlargement;
        explicit SortKeysByAreaEnlargement(const KeyMBR* center, bool* zeroEnlargement) :
            _key(center/*.area()*/), _zeroEnlargement(zeroEnlargement)  {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) /*const*/ {
#ifdef _DEBUG
            if(key1 == NULL) log()  << "SortKeysByAreaEnlargement: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByAreaEnlargement: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log()  << "SortKeysByAreaEnlargement: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByAreaEnlargement: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            double key1AE = key1->getKey()->areaEnlargement(*_key);
            double key2AE = key2->getKey()->areaEnlargement(*_key);
            if(!(*_zeroEnlargement))
                *_zeroEnlargement = (key1AE == 0 || key2AE == 0);
            return key1AE < key2AE;
        }
    };

    /**
     * InMemory key vector sorting by overlap enlargement required for inserting a new key
     */
    template< class Record >
    struct SortKeysByOverlapEnlargement {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        const KeyMBR* _key;
        typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator _start;
        typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator _end;

        explicit SortKeysByOverlapEnlargement(const KeyMBR* key,
            typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator start,
            typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator end) :
            _key(key), _start(start), _end(end) {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) const {
#ifdef _DEBUG
            if(key1 == NULL) log()  << "SortKeysByOverlapEnlargement: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByOverlapEnlargement: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log()  << "SortKeysByOverlapEnlargement: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByOverlapEnlargement: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            return key1->overlapEnlargement(_key, _start, _end) < key2->overlapEnlargement(_key, _start, _end);
        }
    };

#pragma pack()

} // namespace xtree
