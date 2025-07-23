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

namespace xtree {

// Forward declarations
template<typename Record> class COWXTreeAllocator;
template<typename Record> class IndexDetails;

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
        return new XTreeBucket<Record>(std::forward<Args>(args)...);
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
 * COW-enabled allocator traits specialization
 * This is selected when IndexDetails has a COW allocator
 */
template<typename Record>
struct XTreeAllocatorTraits<Record, 
    typename std::enable_if<
        std::is_same<
            decltype(std::declval<IndexDetails<Record>>().getCOWAllocator()),
            COWXTreeAllocator<Record>*
        >::value
    >::type> {
    
    using allocator_type = COWXTreeAllocator<Record>;
    static constexpr bool has_cow = true;
    
    template<typename... Args>
    static XTreeBucket<Record>* allocate_bucket(IndexDetails<Record>* idx, Args&&... args) {
        if (auto* allocator = idx->getCOWAllocator()) {
            return allocator->allocate_bucket(idx, std::forward<Args>(args)...);
        }
        // Fallback to standard allocation
        return new XTreeBucket<Record>(idx, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    static Record* allocate_record(IndexDetails<Record>* idx, Args&&... args) {
        if (auto* allocator = idx->getCOWAllocator()) {
            return allocator->allocate_record(std::forward<Args>(args)...);
        }
        // Fallback to standard allocation
        return new Record(std::forward<Args>(args)...);
    }
    
    static void deallocate_bucket(IndexDetails<Record>* idx, XTreeBucket<Record>* bucket) {
        if (auto* allocator = idx->getCOWAllocator()) {
            allocator->deallocate(bucket);
        } else {
            delete bucket;
        }
    }
    
    static void deallocate_record(IndexDetails<Record>* idx, Record* record) {
        if (auto* allocator = idx->getCOWAllocator()) {
            allocator->deallocate(record);
        } else {
            delete record;
        }
    }
    
    static void record_write(IndexDetails<Record>* idx, void* ptr) {
        idx->recordWrite(ptr);
    }
    
    static void record_operation(IndexDetails<Record>* idx) {
        idx->recordOperation();
    }
};

/**
 * Helper alias for cleaner code
 */
template<typename Record>
using XAlloc = XTreeAllocatorTraits<Record>;

} // namespace xtree

// Include the actual allocator after traits are defined
#include "cow_allocator.hpp"