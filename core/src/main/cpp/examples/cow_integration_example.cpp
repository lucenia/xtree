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
 * Example: Integrating COW Memory Manager with existing XTree
 * 
 * This example shows how to add COW persistence to your existing XTree
 * implementation with minimal changes.
 */

#include <iostream>
#include <memory>
#include <vector>
#include <chrono>
#include "../src/xtree.h"
#include "../src/indexdetails.hpp"
#include "../src/memmgr/cow_memmgr.hpp"

using namespace xtree;
using namespace std;

/**
 * Custom XTreeBucket allocator that uses COW page-aligned memory
 */
template<class Record>
class COWXTreeBucketAllocator {
private:
    DirectMemoryCOWManager<Record>* cow_manager_;
    
public:
    explicit COWXTreeBucketAllocator(DirectMemoryCOWManager<Record>* manager) 
        : cow_manager_(manager) {}
    
    XTreeBucket<Record>* allocate_bucket(IndexDetails<Record>* idx, 
                                        bool isRoot, 
                                        KeyMBR* key = nullptr,
                                        XTreeBucket<Record>* prevDB = nullptr,
                                        long myAddress = 0,
                                        bool isLeaf = true,
                                        unsigned int parentId = 0) {
        // Allocate page-aligned memory
        size_t bucket_size = sizeof(XTreeBucket<Record>);
        void* memory = PageAlignedMemoryTracker::allocate_aligned(bucket_size);
        
        // Construct bucket in-place
        auto* bucket = new (memory) XTreeBucket<Record>(idx, isRoot, key, prevDB, 
                                                        myAddress, isLeaf, parentId);
        
        // Register with COW manager
        if (cow_manager_) {
            cow_manager_->register_bucket_memory(bucket, bucket_size);
        }
        
        return bucket;
    }
};

int main() {
    cout << "=== XTree with COW Memory Persistence Example ===\n\n";
    
    // Step 1: Create your standard XTree setup
    vector<const char*> dimLabels = {"x", "y"};
    auto idx = make_unique<IndexDetails<DataRecord>>(
        2,                    // dimensions
        16,                   // precision
        &dimLabels,          // dimension labels
        256 * 1024 * 1024,   // 256MB cache
        nullptr,             // no factory
        nullptr              // no cache
    );
    
    // Step 2: Create COW memory manager
    auto cow_manager = make_unique<DirectMemoryCOWManager<DataRecord>>(
        idx.get(), 
        "my_xtree_data.snapshot"
    );
    
    cout << "COW Memory Manager initialized\n";
    
    // Step 3: Create root bucket with COW-aware allocation
    COWXTreeBucketAllocator<DataRecord> allocator(cow_manager.get());
    auto* root = allocator.allocate_bucket(idx.get(), true /* isRoot */);
    
    // Add to cache
    auto* cachedRoot = idx->getCache().add(idx->getNextNodeID(), root);
    idx->setRootAddress((long)cachedRoot);
    
    cout << "Root bucket created with COW tracking\n\n";
    
    // Step 4: Normal XTree operations - COW tracks everything automatically
    cout << "Inserting 10,000 spatial records...\n";
    
    auto insert_start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 10000; i++) {
        // Create spatial data
        auto* record = new DataRecord(2, 16, "record_" + to_string(i));
        vector<double> point = {
            (double)(i % 100) * 10.0,
            (double)(i / 100) * 10.0
        };
        record->putPoint(&point);
        
        // Insert into tree
        root->xt_insert(cachedRoot, record);
        
        // Track operation for COW
        cow_manager->record_operation();
        
        // Show progress
        if (i % 1000 == 0 && i > 0) {
            auto stats = cow_manager->get_stats();
            cout << "  Inserted " << i << " records"
                 << " | Memory tracked: " << stats.tracked_bytes / 1024 << " KB"
                 << " | Regions: " << stats.tracked_regions << "\n";
        }
    }
    
    auto insert_end = std::chrono::high_resolution_clock::now();
    auto insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start);
    
    cout << "\nInsertions complete in " << insert_time.count() << " ms\n";
    cout << "Average: " << (10000.0 / insert_time.count()) * 1000 << " inserts/second\n\n";
    
    // Step 5: Demonstrate COW snapshot
    cout << "Creating COW snapshot...\n";
    
    auto snapshot_start = std::chrono::high_resolution_clock::now();
    cow_manager->trigger_memory_snapshot();
    auto snapshot_end = std::chrono::high_resolution_clock::now();
    
    auto snapshot_time = std::chrono::duration_cast<std::chrono::microseconds>(snapshot_end - snapshot_start);
    cout << "Snapshot created in " << snapshot_time.count() << " microseconds!\n";
    
    // The snapshot is now being persisted in the background
    // Your application continues at full speed
    
    // Step 6: Continue operations while snapshot persists
    cout << "\nContinuing operations during background persistence...\n";
    
    for (int i = 10000; i < 11000; i++) {
        auto* record = new DataRecord(2, 16, "post_snapshot_" + to_string(i));
        vector<double> point = {(double)(i % 50), (double)(i / 50)};
        record->putPoint(&point);
        
        root->xt_insert(cachedRoot, record);
        cow_manager->record_operation();
    }
    
    cout << "Added 1,000 more records during snapshot persistence\n";
    
    // Step 7: Final statistics
    auto final_stats = cow_manager->get_stats();
    cout << "\nFinal Statistics:\n";
    cout << "  Total memory tracked: " << final_stats.tracked_bytes / (1024*1024) << " MB\n";
    cout << "  Memory regions: " << final_stats.tracked_regions << "\n";
    cout << "  Operations since last snapshot: " << final_stats.operations_since_snapshot << "\n";
    
    // Step 8: Search demonstration
    cout << "\nPerforming spatial search...\n";
    auto* searchQuery = new DataRecord(2, 16, "search");
    vector<double> minPoint = {200.0, 200.0};
    vector<double> maxPoint = {300.0, 300.0};
    searchQuery->putPoint(&minPoint);
    searchQuery->putPoint(&maxPoint);
    
    auto iter = root->getIterator(cachedRoot, searchQuery, INTERSECTS);
    int count = 0;
    while (iter->hasNext()) {
        auto* result = iter->next();
        if (result) count++;
    }
    
    cout << "Found " << count << " records in search region\n";
    
    delete iter;
    delete searchQuery;
    
    // Cleanup happens automatically
    cout << "\nExample complete. Snapshot saved to: my_xtree_data.snapshot\n";
    
    return 0;
}