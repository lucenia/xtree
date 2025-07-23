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

#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include "cow_xtree_factory.hpp"
#include "../src/xtree.h"

using namespace xtree;
using namespace std;

int main() {
    cout << "=== XTree with COW Memory Management Example ===\n\n";
    
    // Step 1: Create a COW-enabled 2D spatial index
    cout << "Creating 2D spatial index with COW snapshots...\n";
    auto index = COWXTreeFactory<DataRecord>::create_2d_spatial("example_spatial.snapshot");
    
    // Step 2: Create the root bucket
    auto* root = COWXTreeFactory<DataRecord>::create_root(index.get());
    auto* cachedRoot = reinterpret_cast<CacheNode*>(index->getRootAddress());
    
    cout << "Index created with dimensions: " << index->getDimensionCount() 
         << ", precision: " << index->getPrecision() << "\n\n";
    
    // Step 3: Insert some spatial data
    cout << "Inserting 10,000 points...\n";
    auto start = chrono::high_resolution_clock::now();
    
    random_device rd;
    mt19937 gen(rd());
    uniform_real_distribution<> lon_dist(-180.0, 180.0);
    uniform_real_distribution<> lat_dist(-90.0, 90.0);
    
    for (int i = 0; i < 10000; i++) {
        // Create a spatial point
        auto* record = new DataRecord(2, 32, "point_" + to_string(i));
        vector<double> point = {lon_dist(gen), lat_dist(gen)};
        record->putPoint(&point);
        
        // Insert into XTree
        root->xt_insert(cachedRoot, record);
        
        // Every 1000 insertions, show progress
        if ((i + 1) % 1000 == 0) {
            cout << "  Inserted " << (i + 1) << " points";
            
            // Get COW stats
            if (auto* cow_manager = index->getCOWManager()) {
                auto stats = cow_manager->get_stats();
                cout << " (Memory: " << stats.tracked_memory_bytes / 1024 << " KB, "
                     << "Ops since snapshot: " << stats.operations_since_snapshot << ")";
            }
            cout << "\n";
        }
    }
    
    auto insert_time = chrono::high_resolution_clock::now() - start;
    auto ms = chrono::duration_cast<chrono::milliseconds>(insert_time).count();
    cout << "\nInsertion completed in " << ms << " ms\n";
    cout << "Average: " << (ms / 10000.0) << " ms per insert\n\n";
    
    // Step 4: Perform a range query
    cout << "Performing range query (Western Europe: -10 to 20 lon, 40 to 60 lat)...\n";
    auto* query = new DataRecord(2, 32, "query");
    vector<double> minPoint = {-10.0, 40.0};
    vector<double> maxPoint = {20.0, 60.0};
    query->putPoint(&minPoint);
    query->putPoint(&maxPoint);
    
    start = chrono::high_resolution_clock::now();
    auto* iter = root->getIterator(cachedRoot, query, INTERSECTS);
    
    int resultCount = 0;
    while (iter->hasNext()) {
        iter->getNext();
        resultCount++;
    }
    
    auto query_time = chrono::high_resolution_clock::now() - start;
    auto query_ms = chrono::duration_cast<chrono::microseconds>(query_time).count();
    
    cout << "Found " << resultCount << " points in " << query_ms << " microseconds\n\n";
    
    delete iter;
    delete query;
    
    // Step 5: Trigger a manual snapshot
    cout << "Triggering manual snapshot...\n";
    if (auto* cow_manager = index->getCOWManager()) {
        cow_manager->trigger_memory_snapshot();
        
        // Wait a bit for snapshot to complete
        this_thread::sleep_for(chrono::milliseconds(100));
        
        auto stats = cow_manager->get_stats();
        cout << "Snapshot status:\n";
        cout << "  Total tracked memory: " << stats.tracked_memory_bytes / 1024 << " KB\n";
        cout << "  COW protection active: " << (stats.cow_protection_active ? "Yes" : "No") << "\n";
        cout << "  Commit in progress: " << (stats.commit_in_progress ? "Yes" : "No") << "\n";
    }
    
    // Step 6: Show how COW works during persistence
    cout << "\n=== COW Behavior Demonstration ===\n";
    cout << "The COW manager creates snapshots without blocking operations.\n";
    cout << "During snapshot:\n";
    cout << "  1. Memory pages are marked read-only (~100 microseconds)\n";
    cout << "  2. Data is copied to buffers while holding read lock\n";
    cout << "  3. Background thread writes to disk (non-blocking)\n";
    cout << "  4. Main operations continue without interruption\n\n";
    
    // Step 7: Benefits summary
    cout << "=== Benefits of COW-enabled XTree ===\n";
    cout << "✓ Automatic persistence with <2% overhead\n";
    cout << "✓ No serialization needed - raw memory snapshots\n";
    cout << "✓ Background snapshots don't block operations\n";
    cout << "✓ Fast recovery by loading entire snapshot\n";
    cout << "✓ Configurable snapshot triggers (ops/memory/time)\n";
    cout << "✓ Thread-safe with lock-free write tracking\n";
    
    cout << "\nSnapshot saved to: example_spatial.snapshot\n";
    
    return 0;
}