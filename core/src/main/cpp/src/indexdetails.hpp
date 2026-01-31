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

#include "pch.h"
#include "uniqueid.h"
#include "lru.hpp"
#include "lru_sharded.h"
#include "irecord.hpp"  // Need IRecord definition for getKey()

// New persistence layer includes
#include "persistence/durable_runtime.h"
#include "persistence/durable_store.h"
#include "persistence/memory_store.h"
#include "persistence/store_interface.h"
#include "xtree_allocator_traits.hpp"  // For XAlloc
#include <memory>
#include <mutex>
#include <atomic>
#include <cassert>

namespace xtree {

    // forward declarations
    class IRecord;
    
    template< class RecordType >
    class XTreeBucket;

    template< class RecordType >
    class Iterator;
    
    template< class Record >
    class CompactXTreeAllocatorMMAP;
    
    template< class Record >
    class CompactXTreeAllocator;
    
    template< class Record >
    class COWXTreeAllocator;

    template< class Record >
    class IndexDetails {
    public:
        // Cache type definitions - using sharded cache for scalability
        // 32 shards by default, with global object map for O(1) removeByObject
        using Cache = ShardedLRUCache<IRecord, UniqueId, LRUDeleteNone>;
        using CacheNode = typename Cache::Node;
        // Note: To switch back to unsharded, use:
        // using Cache = LRUCache<IRecord, UniqueId, LRUDeleteNone>;

        enum class PersistenceMode {
            IN_MEMORY,  // Pure in-memory, no persistence
            DURABLE     // Durable store with MVCC/COW (replaces MMAP)
        };
        
        IndexDetails( const unsigned short dimension, const unsigned short precision,
                      vector<const char*> *dimLabels, JNIEnv* env, jobject* xtPOJO,
                      const std::string& field_name,  // Required field name for this index
                      PersistenceMode mode = PersistenceMode::IN_MEMORY, 
                      const std::string& data_dir = "./xtree_data" ) :
            _xtPOJO(xtPOJO), _dimension(dimension), _dimensionLabels(dimLabels),
            _precision(precision), field_name_(field_name),
            // Start _nodeCount high to avoid collision with MemoryStore NodeIDs
            // MemoryStore allocates from 1 upward, so we use a different ID space
            _nodeCount(1ULL << 48), persistence_mode_(mode), _rootAddress(0),
            runtime_(nullptr), store_(nullptr) {
//            cout << "CACHE SIZE: " << _cache.getMaxMemory() << endl;

            // retain a handle to the jvm (used for java callbacks)
            if(!IndexDetails<Record>::jvm)
                IndexDetails<Record>::jvm = env;

            IndexDetails<Record>::indexes.push_back(this);

            // Initialize persistence based on mode
            switch (mode) {
                case PersistenceMode::IN_MEMORY:
                    // Create in-memory store
                    memory_store_ = std::make_unique<persist::MemoryStore>();
                    store_ = memory_store_.get();
                    break;
                    
                case PersistenceMode::DURABLE:
                    // Initialize the new durable store with MVCC and crash recovery
                    initializeDurableStore(data_dir);
                    break;
            }
            
            std::cout << "[IndexDetails] Constructor completed for " 
                      << (mode == PersistenceMode::DURABLE ? "DURABLE" : "IN_MEMORY") << " mode\n";

            // update cache size for each index
   //         for_each(IndexDetails<Record>::indexes.begin(),
   //                  IndexDetails<Record>::indexes.end(),
   //                  [&](IndexDetails<Record>* idx){ /*idx.updateMaxMemory();*/ });
            //_cache.updateMaxMemory(getAvailableSystemMemory);
        }

        /**
         * explicit copy constructor
         */
        IndexDetails( const IndexDetails& idx) :
            _dimension(idx.getDimensionCount()), _precision(idx.getPrecision()) {}

        /**
         * Close the index cleanly - should be called before destruction
         * This will:
         * - Commit any pending changes
         * - Fsync all data to disk
         * - Close all file handles
         * - Unmap memory regions
         * - Free all allocated memory
         */
        void close() {
            // If we have a durable store, close it properly
            if (store_ && persistence_mode_ == PersistenceMode::DURABLE) {
                // Flush any pending dirty buckets before final commit
                flush_dirty_buckets();
                
                // Final commit to ensure all data is persisted
                store_->commit(0);  // Use epoch 0 as final marker
                
                // TODO: Add close() to DurableStore to:
                // - Flush and sync WAL
                // - Close checkpoint files
                // - Unmap memory regions
                // - Close file handles via FileHandleRegistry
                // - Clean up segment allocator
            }
            
            // Clear the cache to free all cached nodes
            // This should help free DataRecords
            getCache().clear();
            
            // Clear root references
            root_cache_key_ = 0;
            
            // Reset store pointer (managed elsewhere)
            store_ = nullptr;
            
            // Note: We don't free the actual tree structure here
            // as it's complex to traverse and free all nodes safely.
            // This is a known limitation that needs future work.
        }
        
        /**
         * Entry point for deleting an XTree index
         */
        ~IndexDetails() {
            // Don't delete _dimensionLabels here as it's managed by the caller
            // The caller is responsible for the lifecycle of dimension labels
            
            // Clean up is handled by unique_ptrs automatically
            // Note: close() should be called before destruction for clean shutdown
        }

        unsigned short getDimensionCount() const {
            return this->_dimension;
        }

        unsigned short getDimensionIdx(string label ) const {
            // assert dimension label vector is not null
            assert(_dimensionLabels);
            // assert a label was passed in and not a null pointer
            assert(label.empty() == false);
            vector<const char*>::iterator it = find(_dimensionLabels->begin(), _dimensionLabels->end(), label.c_str());
            // assert the label exists in the iterator
            assert(it != _dimensionLabels->end());
            // return the index of the dimension label
            return (short) distance(_dimensionLabels->begin(), it);
        }

        unsigned short getPrecision() const {
            return this->_precision;
        }

        long getRootAddress() const {
            return this->_rootAddress;
        }

        void setRootAddress(const long rootAddress) {
            this->_rootAddress = rootAddress;
            
            // Also update the store's root tracking for persistence
            if (store_ && rootAddress != 0) {
                // TODO: Convert bucket address to NodeID and update store root
                // This will be handled when we integrate bucket allocation with store
            }
        }
        
        /**
         * Set the root identity for both cache and durable store
         * @param cache_key The cache key (NodeID.raw() for DURABLE, pointer for IN_MEMORY)
         * @param id The NodeID for durable tracking (invalid for IN_MEMORY)
         * @param cn The cache node pointer (direct reference, no lookups)
         * @param persist Whether to emit a WAL delta (false during recovery)
         */
        void setRootIdentity(uint64_t cache_key, persist::NodeID id,
                           LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>* cn,
                           bool persist = true) {
            root_cache_key_ = cache_key;   // still useful (debug/telemetry)
            root_node_id_   = id;          // durable identity
            root_cn_        = cn;          // direct pointer to MRU node
            
            // TODO: remove _rootAddress when all callers use root_cache_node()/root_bucket()
            this->_rootAddress = static_cast<long>(cache_key);
            
            if (persist && store_ && id.valid()) {
                // Try to extract MBR from the cached root bucket
                const float* mbr_data = nullptr;
                size_t mbr_size = 0;
                
                if (cn && cn->object) {
                    // cn->object is an IRecord*, need to cast to bucket to get MBR
                    // This is safe because roots are always buckets, not data records
                    auto* key = cn->object->getKey();
                    if (key) {
                        mbr_data = key->data();  // Get raw float array
                        mbr_size = getDimensionCount() * 2;  // dims * 2 (min/max)
                    }
                }
                
                // Only emit mapping during normal initialization, not recovery
                store_->set_root(id, /*epoch*/0, mbr_data, mbr_size, field_name_);
            }
        }
        
        // Root accessors with lazy cache rebuilding
        // Non-const because it may need to rebuild cache from persistence
        // Note: root_cn_ is always owned by the cache and must never be freed directly
        // Performance: root_version_ changes only on root splits, so steady-state cost
        //              is just one atomic load and a branch - essentially free
        // Throws: std::runtime_error if root reconstruction fails
        //         (e.g., corrupted persistence data)
        LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>* root_cache_node() noexcept(false) {
            // Always load the current root version
            uint64_t current_version = root_version_.load(std::memory_order_acquire);

            // Fast path: root already cached *and* version matches
            if (root_cn_ && cached_root_version_ == current_version) {
                return root_cn_;
            }

            // Slow path: version mismatch or no cache
            {
                std::lock_guard<std::mutex> lock(root_init_mutex_);

                // Re-check inside the lock (double-checked locking)
                if (!root_cn_ || cached_root_version_ != current_version) {
                    // Evict stale root, if any
                    if (root_cn_ && root_cn_->object) {
                        getCache().remove(root_cn_->object);
                        root_cn_ = nullptr;
                    }

                    // Rebuild only if we have a valid root persisted
                    if (root_node_id_.valid() && store_) {
                        rebuild_root_cache_from_persistence();
                        cached_root_version_ = current_version;  // mark cache as up-to-date
                    }
                }
            }

            return root_cn_;
        }

        // Const accessor that doesn't attempt rebuild (peek at current state only)
        // Returns const pointer to enforce read-only semantics
        const LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>* root_cache_node_peek() const {
            return root_cn_;
        }

        template <typename RecordType>
        XTreeBucket<RecordType>* root_bucket() {
            // Ensure cache is rebuilt if needed
            auto* cn = root_cache_node();
            return cn ? reinterpret_cast<XTreeBucket<RecordType>*>(cn->object) : nullptr;
        }

        // Const version for read-only access (no rebuild attempted)
        // Returns const pointer to prevent mutation through const accessor
        template <typename RecordType>
        const XTreeBucket<RecordType>* root_bucket_peek() const {
            return root_cn_ ? reinterpret_cast<const XTreeBucket<RecordType>*>(root_cn_->object) : nullptr;
        }
        
        // Accessors for durability/telemetry
        persist::NodeID root_node_id() const { return root_node_id_; }
        uint64_t root_cache_key() const { return root_cache_key_; }

        // Public: allow tests / callers to force a root reload on next access
        // Use this after external commits to ensure root cache is consistent with durable state
        void invalidate_root_cache() {
            std::lock_guard<std::mutex> lock(root_init_mutex_);
            if (root_cn_ && root_cn_->object) {
                getCache().remove(root_cn_->object);
                root_cn_ = nullptr;
            }
            cached_root_version_ = 0;  // force rebuild even if version matches
            // keep root_node_id_ so lazy rebuild knows what to load
        }

        // Called when splitRoot creates a new root
        // This increments the version to mark that the in-memory root has changed.
        // Note: We don't invalidate the cache here because the new root is already
        // in memory and registered via setRootIdentity(). We just update the version
        // so future accesses know they have the latest.
        void on_root_split(persist::NodeID new_root_id) {
            // Invariant: setRootIdentity() must have registered the new root already
            assert(root_cn_ && "on_root_split called before setRootIdentity registered the new root");

            // Increment version to mark the change
            uint64_t new_version = root_version_.fetch_add(1, std::memory_order_acq_rel) + 1;

            // Update the tracked root ID for future reloads
            root_node_id_ = new_root_id;

            // CRITICAL: Update cached version to match so we don't try to reload
            // The new root is already in memory and cached via setRootIdentity()
            cached_root_version_ = new_version;
        }

        // One-shot bootstrap for tests/simple flows (idempotent)
        template <typename RecordType>
        bool ensure_root_initialized() {
            // Thread-safe guard for concurrent initialization
            std::lock_guard<std::mutex> lock(root_init_mutex_);
            
            if (root_cn_) return true;  // Already initialized
            
            using Alloc = XAlloc<RecordType>;
            
            // CRITICAL: Initial root is a LEAF bucket that holds data records directly
            // It only becomes internal after splitRoot creates a new root above it
            auto ref = Alloc::allocate_bucket(this, persist::NodeKind::Leaf, /*isRoot*/true);
            
            const uint64_t key = Alloc::cache_key_for(ref.id, ref.ptr);
            auto* cn = getCache().add(key, static_cast<IRecord*>(ref.ptr));   // registers in MRU list
            assert(cn && "cache.add must return a valid node for root");
            
            setRootIdentity(key, ref.id, cn);                            // record identities
            Alloc::publish(this, ref.ptr);                               // WAL delta in DURABLE; no-op in memory
            return true;
        }
        
        // Recovery method: restore root from durable store on reopen
        template <typename RecordType>
        bool recover_root() {
            if (persistence_mode_ != PersistenceMode::DURABLE || !store_) {
                return false; // Nothing to do for in-memory mode
            }
            
            std::lock_guard<std::mutex> lock(root_init_mutex_);
            if (root_cn_) return true; // Already recovered
            
            // 1) Ask the store for the durable root for this index/field
            persist::NodeID stored_root = store_->get_root(field_name_);
            trace() << "[RECOVER_ROOT] field_name=" << field_name_
                      << " stored_root=" << (stored_root.valid() ? std::to_string(stored_root.raw()) : "INVALID")
                      << " handle=" << stored_root.handle_index()
                      << " tag=" << static_cast<int>(stored_root.tag())
                      << std::endl;
            if (!stored_root.valid()) return false; // No root persisted yet

            // 2) Resolve NodeID -> memory
            auto node_bytes = store_->read_node(stored_root);
            trace() << "[RECOVER_ROOT] read_node returned data=" << (node_bytes.data ? "valid" : "NULL")
                      << " size=" << node_bytes.size << std::endl;
            if (!node_bytes.data || node_bytes.size == 0) {
                return false; // Corrupt or missing root
            }
            
            // 3) CRITICAL: Do NOT treat read_node() memory as a live bucket!
            // We stored wire format, not raw struct bytes.
            // Create a new bucket on heap and deserialize from wire.
            auto* root_bucket = new XTreeBucket<RecordType>(this, /*isRoot*/true);
            root_bucket->setNodeID(stored_root);
            
            // Deserialize from wire format
            root_bucket->from_wire(reinterpret_cast<const uint8_t*>(node_bytes.data), this);

            trace() << "[RECOVER_ROOT] after from_wire: n=" << root_bucket->n()
                      << " isLeaf=" << root_bucket->getIsLeaf()
                      << " NodeID=" << root_bucket->getNodeID().raw()
                      << std::endl;

            // Debug: Print first 10 child NodeIDs and check if they exist in ObjectTable
            trace() << "[RECOVER_ROOT] First 10 child NodeIDs (children->size()=" << root_bucket->getChildren()->size() << "):" << std::endl;
            auto* children = root_bucket->getChildren();
            int in_ot_count = 0;
            int not_in_ot_count = 0;
            for (size_t i = 0; i < std::min(size_t(10), children->size()); ++i) {
                auto* kn = (*children)[i];
                if (kn) {
                    persist::NodeID nid = kn->getNodeID();
                    bool in_ot = store_->is_node_present(nid);
                    if (in_ot) in_ot_count++;
                    else not_in_ot_count++;
                    trace() << "[RECOVER_ROOT]   child[" << i << "] NodeID=" << nid.raw()
                              << " in_OT=" << in_ot << std::endl;
                }
            }
            trace() << "[RECOVER_ROOT] Summary: " << in_ot_count << " in OT, " << not_in_ot_count << " NOT in OT" << std::endl;

            // Verify invariants
            assert(!root_bucket->_leaf && "recovered root must be internal");
            
            // 4) Register in the MRU tracker and record identities
            const uint64_t key = XAlloc<RecordType>::cache_key_for(stored_root, root_bucket);
            auto* cn = getCache().add(key, reinterpret_cast<IRecord*>(root_bucket));
            if (!cn) return false; // must have an MRU node
            
            // Use recovery-safe identity setter that does NOT emit WAL delta
            setRootIdentity(key, stored_root, cn, /*persist=*/false);
            return true;
        }


        // Use leak-on-exit singleton to avoid static destruction order issues
        static Cache& getCache() {
            // Construct-on-first-use; intentionally leaked to avoid shutdown order issues
            static Cache* instance = []() {
                auto* cache = new Cache(32, true);  // 32 shards, global map enabled
                // Configure memory sizer for IRecord types
                cache->setMemorySizer([](const IRecord* obj) -> size_t {
                    if (!obj) return 0;
                    return static_cast<size_t>(obj->memoryUsage());
                });
                return cache;
            }();
            return *instance;
        }

        // Clear the cache - useful for test cleanup to prevent memory leaks
        // When using LRUDeleteNone, this will only delete cache nodes, not the cached objects
        static void clearCache() { getCache().clear(); }

        // Configure cache memory budget (0 = unlimited)
        // When budget is exceeded, LRU entries are automatically evicted
        static void setCacheMaxMemory(size_t bytes) {
            getCache().setMaxMemory(bytes);
        }

        static size_t getCacheMaxMemory() {
            return getCache().getMaxMemory();
        }

        static size_t getCacheCurrentMemory() {
            return getCache().getCurrentMemory();
        }

        // Explicitly evict cache entries to bring memory under budget
        // IMPORTANT: Only call this at safe points (e.g., after batch insert or commit)
        // when no tree traversal is in progress, as eviction may free unpinned nodes
        // Returns number of entries evicted
        static size_t evictCacheToMemoryBudget() {
            return getCache().evictToMemoryBudget();
        }

        void updateDetails(unsigned short precision,
                      vector<const char*> *dimLabels) {
        	// Set precision
        	_precision = precision;

        	// Update labels
        	// Note: We don't delete the old labels here because IndexDetails
        	// doesn't own the dimension labels - they're managed by the caller
        	_dimensionLabels = dimLabels;

        	// If # dimensions is same, do not modify tree
        	// Iff # dimensions has changed, rebuild tree somehow

        }

        const UniqueId getNextNodeID() { return ++_nodeCount; }
        
        PersistenceMode getPersistenceMode() const {
            return persistence_mode_;
        }
        
        const std::string& getFieldName() const {
            return field_name_;
        }

//        IRecord* getCachedNode( UniqueId recordAddress ) { return NULL; }

        // COW management methods
        bool hasCOWManager() const { return false; }
        
        COWXTreeAllocator<Record>* getCOWAllocator() { 
            // Always return nullptr since FILE_IO mode is removed
            return nullptr;
        }
        
        // Get the store interface for persistence operations
        persist::StoreInterface* getStore() {
            return store_;
        }
        
        // Check if we have a durable store
        bool hasDurableStore() const {
            return persistence_mode_ == PersistenceMode::DURABLE && store_ != nullptr;
        }
        
        // Helper method to record write operations for tracking
        void recordWrite(void* ptr) {
            // No-op for now, will be handled by store interface
        }
        
        // Helper method to record any operation for tracking  
        void recordOperation() {
            // No-op for now, will be handled by store interface
        }
        
        // Register a dirty bucket for later batch publishing (with deduplication)
        void register_dirty_bucket(XTreeBucket<Record>* bucket) {
            if (!bucket) return;
            if (!hasDurableStore() ||
                getPersistenceMode() != PersistenceMode::DURABLE) return;
            
            // Try to enlist - if already enlisted, skip
            if (!bucket->try_enlist()) return;
            
            // Add to dirty list
            std::lock_guard<std::mutex> lock(dirty_buckets_mutex_);
            dirty_buckets_.push_back(bucket);
        }
        
        // Flush all dirty buckets to storage with exception safety
        void flush_dirty_buckets() {
            if (!hasDurableStore() ||
                getPersistenceMode() != PersistenceMode::DURABLE) return;

            // Keep flushing until no more dirty buckets (handle cascading from reallocations)
            int iteration = 0;
            constexpr int MAX_ITERATIONS = 100; // Safety limit

            while (iteration < MAX_ITERATIONS) {
                // Swap out the list to minimize lock time
                std::vector<XTreeBucket<Record>*> buckets;
                {
                    std::lock_guard<std::mutex> lock(dirty_buckets_mutex_);
                    buckets.swap(dirty_buckets_);
                }

                if (buckets.empty()) break;

                ++iteration;

            // Publish each dirty bucket
#ifndef NDEBUG
            int flushed_count = 0;
            int leaf_count = 0;
            int internal_count = 0;
#endif
            for (auto* bucket : buckets) {
                if (!bucket) continue;
                
                // Extra safety: check if bucket still has valid index
                if (!bucket->getIdxDetails()) {
                    continue;  // Bucket was freed/destroyed
                }
                
                if (!bucket->isDirty()) {
                    // It was cleaned elsewhere
                    bucket->clearEnlistedFlag();
                    continue;
                }
                
                try {
                    // Use reallocation-aware publish path and update parent if needed
                    auto old_id = bucket->getNodeID();
                    auto pub_result = XAlloc<Record>::publish_with_realloc(this, bucket);

                    // If bucket was reallocated, update the parent's reference AND mark parent dirty
                    if (pub_result.id.valid() && pub_result.id != old_id) {
                        bucket->setNodeID(pub_result.id);
#ifndef NDEBUG
                        trace() << "[REALLOC_CASCADE] Bucket " << old_id.raw() << " -> " << pub_result.id.raw()
                                  << " (isLeaf=" << bucket->getIsLeaf() << ")"
                                  << " hasParent=" << (bucket->getParent() != nullptr)
                                  << " parentBucket=" << (bucket->parent_bucket() ? bucket->parent_bucket()->getNodeID().raw() : 0)
                                  << std::endl;
#endif
                        if (bucket->getParent()) {
                            bucket->getParent()->setNodeID(pub_result.id);

                            // CRITICAL FIX: Mark the parent bucket dirty since its child reference changed
                            // Without this, the parent's wire format would still have the old child NodeID
                            auto* parentBucket = bucket->parent_bucket();
                            if (parentBucket) {
                                // IMPORTANT: Clear enlisted flag first, then re-mark dirty.
                                // If parent was already in this batch (already enlisted), markDirty()
                                // would silently fail due to try_enlist(). By clearing first, we
                                // ensure parent gets added to the NEXT iteration's dirty list.
                                parentBucket->clearEnlistedFlag();
                                parentBucket->markDirty();
#ifndef NDEBUG
                                trace() << "[REALLOC_CASCADE] Marked parent " << parentBucket->getNodeID().raw()
                                          << " dirty (isRoot=" << (parentBucket->getParent() == nullptr) << ")"
                                          << std::endl;
#endif
                            }
                        } else {
                            // CRITICAL FIX: This bucket IS the root (no parent).
                            // When root is reallocated, we MUST update the superblock
                            // with the new root NodeID, otherwise recovery will use stale ID.
                            root_node_id_ = pub_result.id;
                            root_cache_key_ = pub_result.id.raw();

                            // Update superblock with new root ID
                            if (store_) {
                                // Extract MBR from the bucket
                                const float* mbr_data = nullptr;
                                size_t mbr_size = 0;
                                if (auto* key = bucket->getKey()) {
                                    mbr_data = key->data();
                                    mbr_size = getDimensionCount() * 2;
                                }
                                store_->set_root(pub_result.id, /*epoch*/0, mbr_data, mbr_size, field_name_);
#ifndef NDEBUG
                                trace() << "[REALLOC_ROOT] Updated superblock root: "
                                          << old_id.raw() << " -> " << pub_result.id.raw() << std::endl;
#endif
                            }
                        }
                        // Rekey the cache entry
                        getCache().rekey(old_id.raw(), pub_result.id.raw());
                    }

                    bucket->clearDirty();
                    bucket->clearEnlistedFlag();
#ifndef NDEBUG
                    ++flushed_count;
                    if (bucket->getIsLeaf()) ++leaf_count;
                    else ++internal_count;
#endif
                } catch (const std::exception& e) {
                    // Log the error but continue with other buckets
                    // Don't re-enqueue during close() to avoid use-after-free
                    if (persistence_mode_ != PersistenceMode::DURABLE) {
                        // Only re-enqueue if we're not shutting down
                        std::lock_guard<std::mutex> lock(dirty_buckets_mutex_);
                        dirty_buckets_.push_back(bucket);
                    }
                    // Continue with next bucket instead of throwing
                }
            }
#ifndef NDEBUG
            trace() << "[FLUSH_DIRTY] Flushed " << flushed_count << " buckets ("
                      << leaf_count << " leaf, " << internal_count << " internal)"
                      << " from dirty list of " << buckets.size() << " (iteration " << iteration << ")\n";
#endif
            } // end while loop
        }

        /**
         * Serializes an IndexDetail for persistence in HDFS
         **/
        template <class Archive>
        void serialize( Archive & ar ) const {
            ar( _dimension, _dimensionLabels, _rootAddress, _nodeCount );
        }

    protected:

    private:
        void initializeDurableStore(const std::string& data_dir);
        
        static JNIEnv *jvm;
        static vector<IndexDetails<Record>*> indexes;
        
        // Persistence layer
        PersistenceMode persistence_mode_;                 // Track which mode we're in
        std::string field_name_;                           // Field name for this index (required for multi-field support)
        std::unique_ptr<persist::DurableRuntime> runtime_; // Owns all persistence objects (DURABLE mode)
        std::unique_ptr<persist::MemoryStore> memory_store_; // For IN_MEMORY mode
        persist::StoreInterface* store_;                   // Points to either memory_store_ or durable_store_
        std::unique_ptr<persist::DurableContext> durable_context_; // Context for DurableStore (must outlive durable_store_)
        std::unique_ptr<persist::DurableStore> durable_store_; // For DURABLE mode

        jobject* _xtPOJO;
        unsigned short _dimension;                              // 2 bytes
        vector<const char*> *_dimensionLabels;
        unsigned short _precision;                              // 2 bytes
        long _rootAddress;                                      // 8 bytes (legacy - to be removed)
        
        // Root tracking - single source of truth
        uint64_t        root_cache_key_ = 0;                    // informational
        persist::NodeID root_node_id_   = persist::NodeID::invalid();
        LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>* root_cn_ = nullptr;  // authoritative pointer
        mutable std::mutex root_init_mutex_;                    // Thread-safety for root initialization

        // Root version tracking for automatic cache invalidation on splits
        std::atomic<uint64_t> root_version_{0};                 // Incremented on root split
        uint64_t cached_root_version_ = 0;                      // Version of cached root
        
        // Dirty bucket tracking for batched publishing
        std::vector<XTreeBucket<Record>*> dirty_buckets_;
        mutable std::mutex dirty_buckets_mutex_;                // Thread-safety for dirty list

        // Helper to rebuild root cache from persistence (after commit or reload)
        // Thread-safe via root_init_mutex_ (called from root_cache_node())
        void rebuild_root_cache_from_persistence() {
            if (!store_ || !root_node_id_.valid()) {
                throw std::runtime_error("Cannot rebuild root: no store or invalid NodeID");
            }

            // Verify root exists in ObjectTable
            persist::NodeKind kind;
            if (!store_->get_node_kind(root_node_id_, kind)) {
                throw std::runtime_error("Root NodeID not found in ObjectTable (NodeID=" +
                                       std::to_string(root_node_id_.raw()) + ")");
            }

            // Verify it's a valid bucket type (not data record)
            if (kind != persist::NodeKind::Leaf && kind != persist::NodeKind::Internal) {
                throw std::runtime_error("Root has invalid kind in ObjectTable: " +
                                       std::to_string(static_cast<int>(kind)));
            }

            // Read the root node bytes from persistence (returns mmap'd pointer - do NOT free)
            persist::NodeBytes bytes = store_->read_node(root_node_id_);
            if (!bytes.data || bytes.size == 0) {
                throw std::runtime_error("Failed to read root node from persistence (NodeID=" +
                                       std::to_string(root_node_id_.raw()) + ")");
            }

            // Fresh bucket, deserialize from wire
            auto* bucket = new XTreeBucket<Record>(this, /*isRoot*/true);
            bucket->setNodeID(root_node_id_);

            try {
                bucket->from_wire(static_cast<const uint8_t*>(bytes.data), this);
                #ifndef NDEBUG
                trace() << "[DEBUG] Root deserialized from wire: n=" << bucket->n()
                          << ", _leaf=" << (bucket->n() == 0 ? "should be true" : "depends on children")
                          << ", NodeID=" << root_node_id_.raw() << std::endl;
                #endif
            } catch (const std::exception& e) {
                delete bucket;
                throw std::runtime_error("Failed to deserialize root from wire: " + std::string(e.what()));
            }

            // NOTE: bytes.data points to mmap'd memory - do NOT free it

            // If root already in cache, evict it first to prevent leaks
            // cache.remove() will delete the old bucket via LRUDeleteObject policy
            if (root_cn_ && root_cn_->object) {
                getCache().remove(root_cn_->object);
                root_cn_ = nullptr;
            }

            // Add to cache with the committed NodeID as key
            root_cache_key_ = root_node_id_.raw();

            try {
                root_cn_ = getCache().add(root_cache_key_, reinterpret_cast<IRecord*>(bucket));
            } catch (...) {
                delete bucket;  // Avoid leak if cache.add throws
                throw;
            }

            if (!root_cn_) {
                delete bucket;
                throw std::runtime_error("Failed to add root to cache");
            }
        }
        
        // create the cache for the tree
//        LRUCache<IRecord, UniqueId, LRUDeleteObject> _cache;    // 48 bytes
        UniqueId _nodeCount;                                    // 8 bytes
        
        // Allow factory to initialize static members
        template<typename R> friend class MMapXTreeFactory;
        vector<Iterator<Record>*> *_iterators;
    }; // TOTAL: 68 bytes
}
