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

#include "memmgr/cow_memmgr.hpp"
#include "xtree.h"

namespace xtree {

/**
 * COW-aware memory allocator for XTree nodes
 * This replaces direct 'new' calls with COW-managed allocations
 */
template<typename Record>
class COWXTreeAllocator {
private:
    DirectMemoryCOWManager<Record>* cow_manager_;
    
public:
    explicit COWXTreeAllocator(DirectMemoryCOWManager<Record>* manager) 
        : cow_manager_(manager) {}
    
    /**
     * Allocate a new XTreeBucket with COW tracking
     */
    template<typename... Args>
    XTreeBucket<Record>* allocate_bucket(Args&&... args) {
        // Allocate page-aligned memory through COW manager
        void* mem = cow_manager_->allocate_and_register(sizeof(XTreeBucket<Record>));
        
        // Use placement new to construct in COW-managed memory
        return new (mem) XTreeBucket<Record>(std::forward<Args>(args)...);
    }
    
    /**
     * Allocate a new DataRecord with COW tracking
     */
    template<typename... Args>
    Record* allocate_record(Args&&... args) {
        // Allocate page-aligned memory through COW manager
        void* mem = cow_manager_->allocate_and_register(sizeof(Record));
        
        // Use placement new to construct in COW-managed memory
        return new (mem) Record(std::forward<Args>(args)...);
    }
    
    /**
     * Deallocate a COW-managed object
     * Note: We don't actually free memory here - just unregister from COW tracking
     * The memory will be reclaimed when the entire tree is destroyed
     */
    template<typename T>
    void deallocate(T* ptr) {
        if (ptr) {
            ptr->~T();  // Call destructor
            cow_manager_->get_memory_tracker().unregister_memory_region(ptr);
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
    
    /**
     * Record a write operation to a bucket (for hot page tracking)
     */
    void record_bucket_write(XTreeBucket<Record>* bucket) {
        cow_manager_->record_operation_with_write(bucket);
    }
    
    /**
     * Record any tree operation (for automatic snapshot triggers)
     */
    void record_operation() {
        cow_manager_->record_operation();
    }
    
    DirectMemoryCOWManager<Record>* get_cow_manager() {
        return cow_manager_;
    }
};

} // namespace xtree