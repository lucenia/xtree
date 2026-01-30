/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Base allocator interface for XTree
 */

#pragma once

#include "xtree.h"

namespace xtree {

// Forward declaration
template<typename Record> class DirectMemoryCOWManager;

/**
 * Base allocator interface for XTree
 * This allows polymorphic use of different allocator implementations
 */
template<typename Record>
class XTreeAllocatorBase {
public:
    virtual ~XTreeAllocatorBase() = default;
    
    /**
     * Allocate a new XTreeBucket
     */
    virtual XTreeBucket<Record>* allocate_bucket(IndexDetails<Record>* idx, bool isRoot) = 0;
    
    /**
     * Allocate a new DataRecord
     */
    virtual Record* allocate_record(unsigned short dimension, unsigned short precision, const std::string& id) = 0;
    
    /**
     * Deallocate an object
     */
    template<typename T>
    virtual void deallocate(T* ptr) = 0;
    
    /**
     * Record a write operation to a bucket
     */
    virtual void record_bucket_write(XTreeBucket<Record>* bucket) = 0;
    
    /**
     * Record any tree operation
     */
    virtual void record_operation() = 0;
    
    /**
     * Get the COW manager (may return nullptr)
     */
    virtual DirectMemoryCOWManager<Record>* get_cow_manager() = 0;
};

} // namespace xtree