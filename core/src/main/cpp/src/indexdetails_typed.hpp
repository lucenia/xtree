/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Typed IndexDetails that enables compile-time persistence mode selection
 */

#pragma once

#include "indexdetails.hpp"
#include "xtree_persistence_traits.hpp"

namespace xtree {

/**
 * Typed IndexDetails that encodes persistence mode in the type system
 * This enables compile-time selection of bucket types
 */
template<typename Record, int PersistenceModeValue>
class IndexDetailsTyped : public IndexDetails<Record> {
public:
    static constexpr auto persistence_mode_value = PersistenceModeValue;
    using persistence_mode = std::integral_constant<int, PersistenceModeValue>;
    using persistence_traits = XTreePersistenceTraits<Record, persistence_mode>;
    using bucket_type = typename persistence_traits::bucket_type;
    using node_type = typename persistence_traits::node_type;
    
    // Constructor forwards to base class
    template<typename... Args>
    IndexDetailsTyped(Args&&... args) 
        : IndexDetails<Record>(std::forward<Args>(args)...) {}
    
    // Typed bucket allocation
    bucket_type* allocate_bucket(bool is_leaf) {
        return persistence_traits::create_bucket(this, is_leaf);
    }
    
    // Typed record allocation  
    auto allocate_record(uint16_t dims, uint16_t prec, const std::string& rowid) {
        if constexpr (persistence_traits::is_mmap) {
            return persistence_traits::create_record(this, dims, prec, rowid);
        } else {
            return persistence_traits::create_record(dims, prec, rowid);
        }
    }
};

// Convenience aliases
template<typename Record>
using IndexDetailsMemory = IndexDetailsTyped<Record, 
    static_cast<int>(IndexDetails<Record>::PersistenceMode::IN_MEMORY)>;

template<typename Record>
using IndexDetailsMMAP = IndexDetailsTyped<Record,
    static_cast<int>(IndexDetails<Record>::PersistenceMode::MMAP)>;

/**
 * Template-based XTree operations that work with both modes
 */
template<typename IndexType>
class XTreeOperations {
public:
    using index_type = IndexType;
    using bucket_type = typename IndexType::bucket_type;
    using node_type = typename IndexType::node_type;
    
    static void insert(IndexType* idx, typename IndexType::bucket_type* root, 
                      IRecord* record) {
        // Implementation depends on bucket type but interface is same
        if constexpr (IndexType::persistence_traits::is_mmap) {
            // MMAP-specific insertion logic
            insert_mmap(idx, root, record);
        } else {
            // Standard insertion (existing code)
            insert_memory(idx, root, record);
        }
    }
    
private:
    static void insert_memory(IndexType* idx, 
                             XTreeBucket<typename IndexType::RecordType>* root,
                             IRecord* record) {
        // Use existing XTreeBucket insertion
        auto* cached_record = idx->getCache().add(idx->getNextNodeID(), record);
        root->xt_insert(/* cachedRoot */ nullptr, cached_record);
    }
    
    static void insert_mmap(IndexType* idx,
                           XTreeBucketMMAP<typename IndexType::RecordType>* root,
                           IRecord* record) {
        // MMAP-specific insertion
        auto* allocator = idx->getCompactAllocator()->compact_allocator_;
        // Convert record to offset and insert
        // Implementation would go here
    }
};

/**
 * Factory function that returns typed index based on persistence mode
 */
template<typename Record>
auto create_typed_index(
    unsigned short dimensions,
    unsigned short precision,
    std::vector<const char*>* dimensionLabels,
    typename IndexDetails<Record>::PersistenceMode mode,
    const std::string& snapshotFile = "") {
    
    switch (mode) {
        case IndexDetails<Record>::PersistenceMode::IN_MEMORY:
            return std::make_unique<IndexDetailsMemory<Record>>(
                dimensions, precision, dimensionLabels, 
                nullptr, nullptr, mode, snapshotFile);
            
        case IndexDetails<Record>::PersistenceMode::MMAP:
            return std::make_unique<IndexDetailsMMAP<Record>>(
                dimensions, precision, dimensionLabels,
                nullptr, nullptr, mode, snapshotFile);
            
        default:
            return std::unique_ptr<IndexDetails<Record>>(nullptr);
    }
}

/**
 * Example of how algorithms can be written to work with both modes
 */
template<typename BucketType>
size_t count_nodes(BucketType* bucket) {
    size_t count = 1;  // Count this bucket
    
    if constexpr (std::is_same_v<BucketType, XTreeBucket<DataRecord>>) {
        // IN_MEMORY: Use vector iteration
        for (int i = 0; i < bucket->n(); ++i) {
            // Access children through vector
        }
    } else if constexpr (std::is_same_v<BucketType, XTreeBucketMMAP<DataRecord>>) {
        // MMAP: Use offset-based iteration
        auto* allocator = bucket->idx_ptr->getCompactAllocator()->compact_allocator_;
        uint32_t* offsets = bucket->children.get_offsets(allocator);
        for (uint32_t i = 0; i < bucket->children.count; ++i) {
            // Load child from offset
        }
    }
    
    return count;
}

} // namespace xtree