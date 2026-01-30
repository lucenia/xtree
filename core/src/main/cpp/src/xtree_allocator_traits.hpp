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

#include "persistence/node_id.hpp"
#include "persistence/store_interface.h"
#include "persistence/durable_store.h"
#include "datarecord.hpp"  // for DataRecordView
#include "config.h"    // for XTREE_M
#include <new>        // for std::launder
#include <memory>     // for std::construct_at/destroy_at (C++20)
#include <cstring>    // for std::memset
#include <string>     // for std::to_string

namespace xtree {

// Forward declarations
template<typename Record> class IndexDetails;
template<typename Record> class XTreeBucket;

/**
 * BucketRef holds both the durable NodeID and runtime pointer
 * This allows callers to work with both identities efficiently
 * [[nodiscard]] ensures callers don't ignore reallocation results
 */
template<typename Record>
struct [[nodiscard]] BucketRef {
    persist::NodeID id;              // Durable identity (for persistence/parent refs)
    XTreeBucket<Record>* ptr;        // Runtime pointer (for traversal/modification)
    
    // Convenience methods
    bool valid() const { return ptr != nullptr; }
    operator XTreeBucket<Record>*() const { return ptr; }
    XTreeBucket<Record>* operator->() const { return ptr; }
    XTreeBucket<Record>& operator*() const { return *ptr; }
};

/**
 * Allocation traits for XTree to enable compile-time selection
 * between standard allocation and COW-managed allocation
 */
template<typename Record, typename Enable = void>
struct XTreeAllocatorTraits {
    using allocator_type = std::nullptr_t;
    static constexpr bool has_cow = false;
    
    template<typename... Args>
    static XTreeBucket<Record>* allocate_bucket(Args&&... args) {
        // Simple allocator doesn't have IndexDetails, so just use standard allocation
        return new XTreeBucket<Record>(std::forward<Args>(args)...);
    }
    
    // Overload that accepts IndexDetails for consistency
    template<typename... Args>
    static XTreeBucket<Record>* allocate_bucket(IndexDetails<Record>* idx, Args&&... args) {
        // Check if we have a store for IN_MEMORY mode
        if (idx && idx->getPersistenceMode() == IndexDetails<Record>::PersistenceMode::IN_MEMORY) {
            if (auto* store = idx->getStore()) {
                // Use MemoryStore for allocation
                persist::AllocResult alloc = store->allocate_node(
                    sizeof(XTreeBucket<Record>),
                    persist::NodeKind::Internal
                );
                
                XTreeBucket<Record>* bucket = new (alloc.writable) XTreeBucket<Record>(idx, std::forward<Args>(args)...);
                store->publish_node(alloc.id, alloc.writable, sizeof(XTreeBucket<Record>));
                return bucket;
            }
        }
        // Fallback to standard allocation
        return new XTreeBucket<Record>(idx, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    static Record* allocate_record(Args&&... args) {
        return new Record(std::forward<Args>(args)...);
    }
    
    static void deallocate_bucket(XTreeBucket<Record>* bucket) {
        delete bucket;
    }
    
    static void deallocate_record(Record* record) {
        delete record;
    }
    
    static void record_write(IndexDetails<Record>* idx, void* ptr) {
        // No-op for standard allocation
    }
    
    static void record_operation(IndexDetails<Record>* idx) {
        // No-op for standard allocation
    }
};

/**
 * Store-enabled allocator traits specialization
 * This is selected when IndexDetails has a store (new persistence layer)
 */
template<typename Record>
struct XTreeAllocatorTraits<Record, 
    typename std::enable_if<
        std::is_same<
            decltype(std::declval<IndexDetails<Record>>().getStore()),
            persist::StoreInterface*
        >::value
    >::type> {
    
    using allocator_type = persist::StoreInterface;
    static constexpr bool has_store = true;

    // Helper overloads to create buckets with correct isLeaf flag based on NodeKind
    // Case 1: Only isRoot provided (common case from ensure_root_initialized)
    static XTreeBucket<Record>* make_bucket_for_kind(IndexDetails<Record>* idx,
                                                     persist::NodeKind kind,
                                                     bool isRoot) {
        const bool is_leaf = (kind == persist::NodeKind::Leaf);
        // Constructor: (idx, isRoot, key, sourceChildren, split_index, isLeaf, sourceN)
        return new XTreeBucket<Record>(idx, isRoot,
                                      nullptr,  // key
                                      nullptr,  // sourceChildren
                                      0,        // split_index
                                      is_leaf); // CRITICAL: correct leaf flag from NodeKind
    }

    // Case 2: Split form with key, sourceChildren, split_index
    // Using void* for sourceChildren to avoid forward declaration issues
    static XTreeBucket<Record>* make_bucket_for_kind(IndexDetails<Record>* idx,
                                                     persist::NodeKind kind,
                                                     bool isRoot,
                                                     KeyMBR* key,
                                                     const void* sourceChildren,
                                                     unsigned int split_index) {
        // Only Leaf or Internal kinds are valid for root creation
        assert((kind == persist::NodeKind::Leaf || kind == persist::NodeKind::Internal) &&
                "make_bucket_for_kind (isRoot) only supports Leaf or Internal kinds");

        const bool is_leaf = (kind == persist::NodeKind::Leaf);
        
        // Cast back to the expected type for the constructor
        using MBRKeyNodeVec = const std::vector<typename XTreeBucket<Record>::_MBRKeyNode*>*;
        return new XTreeBucket<Record>(idx, isRoot,
                                      key,
                                      static_cast<MBRKeyNodeVec>(sourceChildren),
                                      split_index,
                                      is_leaf); // CRITICAL: correct leaf flag
    }

    // Case 3: Fallback for any other parameter combinations
    template<typename... CtorArgs>
    static XTreeBucket<Record>* make_bucket_for_kind(IndexDetails<Record>* idx,
                                                     persist::NodeKind kind,
                                                     bool isRoot,
                                                     CtorArgs&&... rest) {
        const bool is_leaf = (kind == persist::NodeKind::Leaf);
        // For now, just construct with default isLeaf and log a warning
        // We can't access private _leaf member from here
        // The bucket will use its default isLeaf=true, which might be wrong for internal nodes
        // This is why the specialized overloads above are critical
        return new XTreeBucket<Record>(idx, isRoot, std::forward<CtorArgs>(rest)...);
    }

    /**
     * Allocate a bucket through the store with proper exception safety
     * Returns both NodeID and pointer for efficient parent/child management
     */
    template<typename... CtorArgs>
    static BucketRef<Record> allocate_bucket(IndexDetails<Record>* idx,
                                            persist::NodeKind kind,
                                            CtorArgs&&... args) {
        auto* store = idx ? idx->getStore() : nullptr;

        // Fallback path: no store configured
        if (!store) {
            // Use helper to ensure correct isLeaf flag
            auto* ptr = make_bucket_for_kind(idx, kind, std::forward<CtorArgs>(args)...);
            // Set invalid NodeID for uniformity
            ptr->setNodeID(persist::NodeID::invalid());
            return { persist::NodeID::invalid(), ptr };
        }

        // 1. Create bucket on heap (runtime structure) with correct isLeaf flag
        XTreeBucket<Record>* bucket = make_bucket_for_kind(idx, kind, std::forward<CtorArgs>(args)...);
        
        // 2. Calculate wire size for persistence
        size_t wire_sz = bucket->wire_size(*idx);
        
        // CRITICAL: Pre-allocate extra space for bucket growth
        // Buckets can grow significantly, especially supernodes (up to 3*XTREE_M children)
        // Strategy:
        // - For leaf buckets: allocate for at least XTREE_M children
        // - For internal buckets: allocate for at least XTREE_M*1.5 children
        // - This prevents reallocation issues when buckets grow
        
        const unsigned short dims = idx->getDimensionCount();
        const bool is_leaf = (kind == persist::NodeKind::Leaf);
        
        // Calculate minimum safe size based on bucket type
        // Wire format: 4 (header) + dims*8 (MBR) + n*16 (children) bytes
        size_t min_children = is_leaf ? XTREE_M : static_cast<size_t>(XTREE_M * 1.5);
        size_t min_wire_sz = 4 + (dims * 8) + (min_children * 16);
        
        // Use the larger of actual size or minimum safe size
        if (wire_sz < min_wire_sz) {
            wire_sz = min_wire_sz;
        }
        
        // 3. Allocate storage for wire format
        persist::AllocResult alloc = store->allocate_node(wire_sz, kind);
        
        // Record durable identity inside the bucket
        bucket->setNodeID(alloc.id);
        
        // Debug assertion: bucket must have valid NodeID before serialization
        assert(bucket->hasNodeID() && "Bucket must have NodeID before serialization");
        assert(bucket->getNodeID().valid() && "Bucket NodeID must be valid before serialization");
        assert(bucket->getNodeID().raw() != 0 && "Bucket NodeID.raw() must not be 0");
        
        try {
            // Serialize to wire format and publish
            uint8_t* wire_buf = static_cast<uint8_t*>(alloc.writable);
            uint8_t* end = bucket->to_wire(wire_buf, *idx);
            
            // Safety check: ensure we didn't overflow
            size_t bytes_written = end - wire_buf;
            if (bytes_written > wire_sz) {
                throw std::runtime_error("Buffer overflow in bucket serialization: wrote " + 
                    std::to_string(bytes_written) + " bytes but allocated only " + 
                    std::to_string(wire_sz) + " bytes");
            } else if (bytes_written > alloc.capacity) {
                throw std::runtime_error("Buffer overflow in bucket serialization: wrote " + 
                    std::to_string(bytes_written) + " bytes but allocated only " + 
                    std::to_string(alloc.capacity) + " bytes");
            }
            
            store->publish_node(alloc.id, alloc.writable, wire_sz);
        } catch (...) {
            // Clean up on failure
            delete bucket;
            throw;
        }
        
        // Return both durable ID and runtime pointer
        // Caller decides when to set_root() / commit()
        return { alloc.id, bucket };
    }
    
    /**
     * Convenience wrapper for allocating internal buckets
     */
    template<typename... CtorArgs>
    static BucketRef<Record> allocate_internal_bucket(IndexDetails<Record>* idx,
                                                     CtorArgs&&... args) {
        return allocate_bucket(idx, persist::NodeKind::Internal, std::forward<CtorArgs>(args)...);
    }
    
    /**
     * Convenience wrapper for allocating leaf buckets
     */
    template<typename... CtorArgs>
    static BucketRef<Record> allocate_leaf_bucket(IndexDetails<Record>* idx,
                                                 CtorArgs&&... args) {
        return allocate_bucket(idx, persist::NodeKind::Leaf, std::forward<CtorArgs>(args)...);
    }
    
    // Legacy interface for backward compatibility (returns just pointer)
    template<typename... Args>
    static XTreeBucket<Record>* allocate_bucket(IndexDetails<Record>* idx, Args&&... args) {
        // Default to internal bucket for legacy callers
        auto ref = allocate_internal_bucket(idx, std::forward<Args>(args)...);
        return ref.ptr;
    }
    
    // Return both NodeID and pointer for DataRecords
    struct RecordRef {
        persist::NodeID id;
        Record* ptr;
    };
    
    template<typename... Args>
    static RecordRef allocate_record_with_id(IndexDetails<Record>* idx, Args&&... args) {
        // Always construct the live object on the heap.
        // DataRecords aren't fully initialized yet (points come later).
        Record* rec = new Record(std::forward<Args>(args)...);
        
        // No persistent allocation *yet* — we don't know the final wire size.
        return { persist::NodeID::invalid(), rec };
    }
    
    // Legacy convenience - returns just pointer
    template<typename... Args>
    static Record* allocate_record(IndexDetails<Record>* idx, Args&&... args) {
        return allocate_record_with_id(idx, std::forward<Args>(args)...).ptr;
    }
    
    // Helper to detect if type has serialization methods
    template<typename T, typename = void>
    struct has_wire_methods : std::false_type {};
    
    template<typename T>
    struct has_wire_methods<T, std::void_t<
        // size_t T::wire_size(uint16_t) const
        decltype(std::declval<const T&>().wire_size(std::declval<uint16_t>())),
        // uint8_t* T::to_wire(uint8_t*, uint16_t) const (or non-const)
        decltype(std::declval<T&>().to_wire((uint8_t*)nullptr, std::declval<uint16_t>())),
        // void T::setNodeID(NodeID)
        decltype(std::declval<T&>().setNodeID(persist::NodeID::invalid())),
        // bool T::hasNodeID() const
        decltype(std::declval<const T&>().hasNodeID())
    >> : std::true_type {};

    // SFINAE overload pair for persist_data_record
    
    // Enabled when T has wire_size/to_wire methods
    template<typename T = Record,
             std::enable_if_t<has_wire_methods<T>::value, int> = 0>
    static void persist_data_record(IndexDetails<T>* idx, T* rec) {
        if (!idx || !rec) return;
        
        auto* store = idx->getStore();
        if (!store || 
            idx->getPersistenceMode() != IndexDetails<T>::PersistenceMode::DURABLE ||
            rec->hasNodeID()) {
            return; // no-op in IN_MEMORY, or already persisted
        }
        
        const uint16_t dims = idx->getDimensionCount();
        const size_t wire_sz = rec->wire_size(dims);
        if (!wire_sz) {
            throw std::runtime_error("DataRecord wire_size() returned 0");
        }
        
        persist::AllocResult alloc = store->allocate_node(wire_sz, persist::NodeKind::DataRecord);
        if (!alloc.writable || alloc.capacity < wire_sz) {
            // Optional: free_node(alloc.id) if allocation was partially successful
            if (alloc.id.valid()) {
                try { DS_FREE_IMMEDIATE(store, alloc.id, AbortRollback); } catch (...) {}
            }
            throw std::runtime_error("Allocator returned invalid slot for DataRecord");
        }
        
        // Serialize directly into destination
#ifndef NDEBUG
        // Debug mode: verify we write exactly wire_sz bytes
        {
            uint8_t* start = static_cast<uint8_t*>(alloc.writable);
            uint8_t* end = rec->to_wire(start, dims);
            if (size_t(end - start) != wire_sz) {
                // Clean up allocation before throwing
                try { DS_FREE_IMMEDIATE(store, alloc.id, AbortRollback); } catch (...) {}
                throw std::runtime_error("to_wire wrote unexpected size for DataRecord: expected " +
                    std::to_string(wire_sz) + " but wrote " + std::to_string(end - start));
            }
        }
#else
        rec->to_wire(static_cast<uint8_t*>(alloc.writable), dims);
#endif
        
        // Optional: Zero unused tail bytes for deterministic checksums
        if (alloc.capacity > wire_sz) {
            uint8_t* tail_start = static_cast<uint8_t*>(alloc.writable) + wire_sz;
            std::memset(tail_start, 0, alloc.capacity - wire_sz);
        }
        
        // Publish metadata WITHOUT copying (store computes CRC/dirty ranges internally)
        // Prefer zero-copy path; fallback to memcpy variant if not available
        try {
            if (store->supports_in_place_publish()) {
                store->publish_node_in_place(alloc.id, wire_sz);
            } else {
                store->publish_node(alloc.id, alloc.writable, wire_sz);
            }
        } catch (...) {
            // On any failure, free the allocated space to avoid orphans
            try { DS_FREE_IMMEDIATE(store, alloc.id, AbortRollback); } catch (...) {}
            throw; // propagate original exception
        }
        
        rec->setNodeID(alloc.id);
        // NOTE: After this point, the serialized bytes must not be modified until commit() completes
    }
    
    // Disabled / no-op when T lacks wire methods
    template<typename T = Record,
             std::enable_if_t<!has_wire_methods<T>::value, int> = 0>
    static void persist_data_record(IndexDetails<T>*, T*) {
        // no-op: type not persistable
    }

private:

public:
    
    static void deallocate_bucket(IndexDetails<Record>* idx, XTreeBucket<Record>* bucket) {
        if (bucket) {
            // Free the persistent storage for reuse
            if (idx && idx->getStore() && bucket->hasNodeID()) {
                DS_FREE_IMMEDIATE(idx->getStore(), bucket->getNodeID(), TreeDestroy);
            }
            // Delete the heap object
            delete bucket;
        }
    }
    
    static void deallocate_record(IndexDetails<Record>* idx, Record* record) {
        // Records are deallocated normally for now
        delete record;
    }
    
    static void record_write(IndexDetails<Record>* idx, void* ptr) {
        idx->recordWrite(ptr);
    }
    
    static void record_operation(IndexDetails<Record>* idx) {
        idx->recordOperation();
    }
    
    /**
     * Publish a bucket's changes (for mutations after initial allocation)
     * Handles reallocation if the bucket has grown beyond its capacity
     * [[nodiscard]] ensures callers handle potential reallocation
     */
    [[nodiscard]] static BucketRef<Record> publish_with_realloc(IndexDetails<Record>* idx, XTreeBucket<Record>* bucket) {
        if (!bucket || !idx) return { persist::NodeID::invalid(), bucket };
        
        auto* store = idx->getStore();
        if (!store || !bucket->hasNodeID()) {
            // IN_MEMORY mode or no NodeID: no-op
            return { bucket->getNodeID(), bucket };
        }
        
        // DURABLE mode: check if reallocation is needed
        const size_t wire_sz = bucket->wire_size(*idx);
        persist::NodeID current_id = bucket->getNodeID();
        
        // Try to publish to existing allocation
        try {
            std::vector<uint8_t> buf(wire_sz);
            bucket->to_wire(buf.data(), *idx);
            store->publish_node(current_id, buf.data(), wire_sz);
            return { current_id, bucket };  // Success, no reallocation needed
        } catch (const std::exception& e) {
            // Reallocation required - bucket has grown beyond its allocation
            // Strategy: Allocate with 2x growth factor to minimize future reallocations
            size_t new_capacity = wire_sz * 2;
            
            // Cap at next size class to avoid waste
            for (size_t sz : persist::size_class::kSizes) {
                if (sz >= new_capacity) {
                    new_capacity = sz;
                    break;
                }
            }
            
            // Get the original node's NodeID first
            persist::NodeID old_id = bucket->getNodeID();

            // Allocate new storage with growth room, preserving the original node's kind
            // CRITICAL FIX: Use the bucket's actual _leaf flag to determine NodeKind
            // Fallback to ObjectTable only if that fails
            persist::NodeKind nk = bucket->getIsLeaf() ? persist::NodeKind::Leaf
                                                     : persist::NodeKind::Internal;

            // Optionally verify against the current live entry
            persist::NodeKind existing_kind;
            if (store->get_node_kind(old_id, existing_kind)) {
#ifndef NDEBUG
                if (existing_kind != nk) {
                    trace() << "[REALLOC_KIND_WARN] NodeKind mismatch: bucket says "
                              << static_cast<int>(nk) << " but OT has "
                              << static_cast<int>(existing_kind)
                              << " for NodeID " << old_id.raw() << " - using bucket's flag\n";
                }
#endif
            }

            persist::AllocResult alloc = store->allocate_node(new_capacity, nk);

            // Update bucket's NodeID to the new allocation
            bucket->setNodeID(alloc.id);
            
            // Serialize and publish to new location
            uint8_t* wire_buf = static_cast<uint8_t*>(alloc.writable);
            bucket->to_wire(wire_buf, *idx);
            store->publish_node(alloc.id, alloc.writable, wire_sz);
            
            // Free the old allocation for reuse
            DS_FREE_IMMEDIATE(store, old_id, Reallocation);
            
            // Return new NodeID so caller can update parent references
            return { alloc.id, bucket };
        }
    }
    
    /**
     * Legacy publish interface (doesn't handle reallocation)
     */
    static void publish(IndexDetails<Record>* idx, XTreeBucket<Record>* bucket) {
        publish_with_realloc(idx, bucket);  // Ignore return value for backward compatibility
    }

    /**
     * Safe publish helper that updates parent's child reference after reallocation.
     * Ensures the parent always points to the correct (possibly new) NodeID.
     *
     * @param idx The IndexDetails containing the store
     * @param child_bucket The child bucket to publish
     * @param parent_kn The parent's key node that references this child (can be null)
     */
    template<typename KeyNode>
    static void publish_and_refresh_child(IndexDetails<Record>* idx,
                                         XTreeBucket<Record>* child_bucket,
                                         KeyNode* parent_kn) {
        if (!child_bucket || !idx) return;

        persist::NodeID old_id = (parent_kn && parent_kn->hasNodeID())
                                ? parent_kn->getNodeID()
                                : persist::NodeID::invalid();

        // Publish child (may reallocate, returns possibly new NodeID)
        auto pub = publish_with_realloc(idx, child_bucket);

        // Update parent if NodeID changed
        if (parent_kn && pub.id.valid() && pub.id != old_id) {
            parent_kn->setNodeID(pub.id);

#ifndef NDEBUG
            if (auto* store = idx->getStore()) {
                persist::NodeKind actual;
                if (store->get_node_kind(pub.id, actual)) {
                    persist::NodeKind expected = child_bucket->getLeaf()
                                               ? persist::NodeKind::Leaf
                                               : persist::NodeKind::Internal;
                    assert(actual == expected &&
                           "Parent-child kind mismatch at link time");
                }
            }

            if (old_id.valid()) {
                trace() << "[PUBLISH_REFRESH] Updated child NodeID "
                          << old_id.raw() << " -> " << pub.id.raw() << std::endl;
            }
#endif
        }
    }

    /**
     * Load a bucket from persistence given its NodeID
     * Production-ready with exception safety and dirty flag management
     * 
     * @param idx The IndexDetails containing the store
     * @param nid The NodeID to load
     * @return Heap-allocated bucket or nullptr on failure
     */
    static XTreeBucket<Record>* load_bucket(IndexDetails<Record>* idx, persist::NodeID nid) {
        if (!idx || !nid.valid()) return nullptr;
        
        // IN_MEMORY mode should never try to load from persistence
        if (idx->getPersistenceMode() != IndexDetails<Record>::PersistenceMode::DURABLE) {
            return nullptr;
        }
        
        auto* store = idx->getStore();
        if (!store) return nullptr;

#ifndef NDEBUG
        // Debug: Log attempt to load bucket from persistence
        log() << "DEBUG: load_bucket attempting to read NodeID " << nid.raw()
              << " - this should already be published by allocate_bucket()" << std::endl;
#endif

        // Read the bucket's wire format from persistence
        auto node_bytes = store->read_node(nid);
        if (!node_bytes.data || node_bytes.size == 0) {
            log() << "ERROR: Failed to read bucket NodeID " << nid.raw() << " from store" << std::endl;
#ifndef NDEBUG
            // Debug: Additional context for unpublished bucket detection
            log() << "DEBUG: Bucket NodeID " << nid.raw()
                  << " not found in store - likely never published or wrong NodeID type" << std::endl;
#endif
            return nullptr;
        }
        
        // Create a new bucket and deserialize from wire format
        auto* bucket = new XTreeBucket<Record>(idx, false); // isRoot=false
        bucket->setNodeID(nid);
        
        try {
            // Deserialize from wire format (populates _children with MBRs and NodeIDs)
            // This should NOT load grandchildren - just the child MBRs and NodeIDs
            bucket->from_wire(reinterpret_cast<const uint8_t*>(node_bytes.data), idx);
            
            // CRITICAL: Mark as clean after load from persistence
            // Otherwise checkpoint code may think it needs republishing
            bucket->clearDirty();
            
        } catch (const std::exception& e) {
            log() << "ERROR: Failed to deserialize bucket NodeID " << nid.raw() 
                  << ": " << e.what() << std::endl;
            delete bucket;
            return nullptr;
        } catch (...) {
            log() << "ERROR: Failed to deserialize bucket NodeID " << nid.raw() 
                  << " (unknown exception)" << std::endl;
            delete bucket;
            return nullptr;
        }
        
        return bucket;
    }
    
    /**
     * Load a data record from persistence given its NodeID
     * Production path: Returns DataRecordView for zero-copy mmap access
     * Fallback path: Returns heap DataRecord if pinning unavailable
     * 
     * IMPORTANT: This function returns an uncached object. The caller
     * (cache_or_load) is responsible for adding it to the cache.
     * 
     * @param idx The IndexDetails containing store and dimensions
     * @param nid The NodeID to load
     * @return IRecord* (DataRecordView or DataRecord) or nullptr on failure
     */
    static IRecord* load_data_record(IndexDetails<Record>* idx, persist::NodeID nid) {
        if (!idx || !nid.valid()) return nullptr;
        
        // IN_MEMORY mode should never try to load from persistence
        if (idx->getPersistenceMode() != IndexDetails<Record>::PersistenceMode::DURABLE) {
            return nullptr;
        }
        
        auto* store = idx->getStore();
        if (!store) return nullptr;
        
        // Production path: Zero-copy with pinned memory (Lucene-like performance)
        try {
            auto pinned = store->read_node_pinned(nid);
            if (pinned.data && pinned.size > 0) {
                // Create zero-copy view - Pin ownership transfers to view via std::move
                // View holds the pin RAII object, ensuring memory stays mapped
                auto* view = new DataRecordView(
                    std::move(pinned.pin),  // Transfer pin ownership for lifetime management
                    static_cast<const uint8_t*>(pinned.data),
                    pinned.size,
                    idx->getDimensionCount(),
                    idx->getPrecision(),
                    nid
                );
                
                // NOTE: NOT adding to cache here - cache_or_load does that
                // This keeps the function pure and lets caller manage caching
                return view;
            }
        } catch (const std::exception& e) {
            // Pinned read not available, fall back to heap allocation
            log() << "INFO: Pinned read failed for DataRecord NodeID " << nid.raw() 
                  << ", falling back to heap: " << e.what() << std::endl;
        }
        
        // Fallback path: Heap allocation when memory mapping unavailable
        auto node_bytes = store->read_node(nid);
        if (!node_bytes.data || node_bytes.size == 0) {
            log() << "ERROR: Failed to read DataRecord NodeID " << nid.raw() 
                  << " from store" << std::endl;
            return nullptr;
        }
        
        // Only compile this code for types that support persistence
        if constexpr (has_wire_methods<Record>::value) {
            try {
                // Create heap DataRecord - from_wire will populate all fields including rowid
                auto* record = new Record(
                    idx->getDimensionCount(),
                    idx->getPrecision(),
                    ""  // Placeholder - from_wire will set actual rowid
                );
                record->setNodeID(nid);
                
                // Deserialize - this populates rowid, points, and MBR from wire format
                record->from_wire(
                    reinterpret_cast<const uint8_t*>(node_bytes.data),
                    idx->getDimensionCount(),
                    idx->getPrecision()
                );
                
                // NOTE: NOT adding to cache here - cache_or_load does that
                return record;
                
            } catch (const std::exception& e) {
                log() << "ERROR: Failed to deserialize DataRecord NodeID " << nid.raw() 
                      << ": " << e.what() << std::endl;
                return nullptr;
            }
        } else {
            // Types without wire methods (like MockRecord) should never reach here
            log() << "ERROR: Attempted to load DataRecord for type without wire format support" << std::endl;
            return nullptr;
        }
    }
    
    /**
     * Generate a cache key transparently for both modes
     * DURABLE: use NodeID.raw() to avoid ABA issues
     * IN_MEMORY: use pointer value
     */
    static inline uint64_t cache_key_for(persist::NodeID id, void* ptr) {
        return id.valid() ? id.raw() : reinterpret_cast<uint64_t>(ptr);
    }
};

/**
 * Helper alias for cleaner code
 */
template<typename Record>
using XAlloc = XTreeAllocatorTraits<Record>;

} // namespace xtree

// Removed obsolete allocator includes - now using persistence layer