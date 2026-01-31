/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test XTree concurrent search with segmented allocator
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <random>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"
#include "../src/memmgr/concurrent_compact_allocator.hpp"


using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteObject>;

// Static member definitions are in test_globals.cpp

class XTreeConcurrentSearchTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing snapshot
        std::remove("/tmp/test_concurrent_xtree.dat");
        
        // Create index with MMAP persistence
        std::vector<const char*> dimLabels = {"x", "y"};
        
        // Create snapshot manager with concurrent allocator
        snapshot_manager = new CompactSnapshotManager("/tmp/test_concurrent_xtree.dat", 64 * 1024 * 1024);
        
        // Create concurrent allocator wrapper
        auto* base_allocator = snapshot_manager->get_allocator();
        concurrent_allocator = new ConcurrentCompactAllocator(
            base_allocator->get_arena_base(),
            base_allocator->get_arena_size(),
            base_allocator->get_used_size()
        );
        
        // Create XTree allocator
        xtree_allocator = new CompactXTreeAllocator<DataRecord>(snapshot_manager);
        
        index = new IndexDetails<DataRecord>(
            2,                          // dimensions
            32,                         // node size
            &dimLabels,
            nullptr,                    // JNIEnv
            nullptr,                    // jobject
            IndexDetails<DataRecord>::PersistenceMode::MMAP,
            "/tmp/test_concurrent_xtree.dat"
        );
        
        // Create and cache root
        root = XAlloc<DataRecord>::allocate_bucket(index, true);
        XAlloc<DataRecord>::record_write(index, root);
        cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress((long)cachedRoot);
    }
    
    void TearDown() override {
        IndexDetails<DataRecord>::clearCache();
        delete index;
        delete xtree_allocator;
        delete concurrent_allocator;
        delete snapshot_manager;
        std::remove("/tmp/test_concurrent_xtree.dat");
    }
    
    CompactSnapshotManager* snapshot_manager;
    ConcurrentCompactAllocator* concurrent_allocator;
    CompactXTreeAllocator<DataRecord>* xtree_allocator;
    IndexDetails<DataRecord>* index;
    XTreeBucket<DataRecord>* root;
    CacheNode* cachedRoot;
};

TEST_F(XTreeConcurrentSearchTest, DISABLED_ConcurrentSearchWhileInserting) {
    const int NUM_INITIAL_RECORDS = 10000;
    const int NUM_SEARCH_THREADS = 4;
    const int NUM_INSERT_THREADS = 2;
    const int INSERTS_PER_THREAD = 5000;
    
    std::cout << "\n=== XTree Concurrent Search Test ===\n";
    
    // Insert initial records
    std::cout << "Inserting " << NUM_INITIAL_RECORDS << " initial records...\n";
    for (int i = 0; i < NUM_INITIAL_RECORDS; i++) {
        DataRecord* record = new DataRecord(2, 32, std::to_string(i));
        std::vector<double> point = {(double)(i % 1000), (double)(i / 1000)};
        record->putPoint(&point);
        root->xt_insert(cachedRoot, record);
        
        if (i % 1000 == 0) {
            std::cout << "  Inserted " << i << " records\n";
        }
    }
    
    std::atomic<bool> stop_searching{false};
    std::atomic<int> search_count{0};
    std::atomic<int> insert_count{NUM_INITIAL_RECORDS};
    std::atomic<int> found_count{0};
    
    std::vector<std::thread> threads;
    
    // Launch search threads
    for (int t = 0; t < NUM_SEARCH_THREADS; t++) {
        threads.emplace_back([this, &stop_searching, &search_count, &found_count]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> x_dist(0, 1000);
            std::uniform_real_distribution<> y_dist(0, 20);
            
            while (!stop_searching.load()) {
                // Create random search rectangle
                float x = x_dist(gen);
                float y = y_dist(gen);
                DataRecord* searchKey = new DataRecord(2, 32, "search");
                std::vector<double> min_point = {x, y};
                std::vector<double> max_point = {x + 100, y + 5};
                searchKey->putPoint(&min_point);
                searchKey->putPoint(&max_point);
                
                // Perform search
                auto iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
                
                int local_found = 0;
                while (iter->hasNext()) {
                    DataRecord* result = iter->next();
                    local_found++;
                }
                
                delete iter;
                
                search_count.fetch_add(1);
                found_count.fetch_add(local_found);
            }
        });
    }
    
    // Launch insert threads
    for (int t = 0; t < NUM_INSERT_THREADS; t++) {
        threads.emplace_back([this, t, &insert_count, INSERTS_PER_THREAD]() {
            for (int i = 0; i < INSERTS_PER_THREAD; i++) {
                int id = NUM_INITIAL_RECORDS + t * INSERTS_PER_THREAD + i;
                DataRecord* record = new DataRecord(2, 32, std::to_string(id));
                std::vector<double> point = {(double)(id % 1000), (double)(id / 1000)};
                record->putPoint(&point);
                
                // Note: In production, would need proper locking here
                root->xt_insert(cachedRoot, record);
                insert_count.fetch_add(1);
                
                // Slow down inserts to allow searches
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }
    
    // Wait for inserts to complete
    auto start = high_resolution_clock::now();
    for (int t = NUM_SEARCH_THREADS; t < NUM_SEARCH_THREADS + NUM_INSERT_THREADS; t++) {
        threads[t].join();
    }
    auto insert_time = high_resolution_clock::now() - start;
    
    // Stop searches
    stop_searching.store(true);
    for (int t = 0; t < NUM_SEARCH_THREADS; t++) {
        threads[t].join();
    }
    
    std::cout << "\nResults:\n";
    std::cout << "  Initial records: " << NUM_INITIAL_RECORDS << "\n";
    std::cout << "  Records inserted during search: " << (NUM_INSERT_THREADS * INSERTS_PER_THREAD) << "\n";
    std::cout << "  Total records: " << insert_count.load() << "\n";
    std::cout << "  Search threads: " << NUM_SEARCH_THREADS << "\n";
    std::cout << "  Searches performed: " << search_count.load() << "\n";
    std::cout << "  Records found: " << found_count.load() << "\n";
    std::cout << "  Avg records per search: " << (double)found_count.load() / search_count.load() << "\n";
    std::cout << "  Insert time: " << duration_cast<milliseconds>(insert_time).count() << " ms\n";
    std::cout << "  Searches per second: " 
              << (search_count.load() * 1000.0 / duration_cast<milliseconds>(insert_time).count()) << "\n";
}

TEST_F(XTreeConcurrentSearchTest, DISABLED_SearchAcrossSegments) {
    // Force allocation across segments by inserting many records
    const int RECORDS_PER_BATCH = 50000;
    const int NUM_BATCHES = 3;
    
    std::cout << "\n=== Search Across Segments Test ===\n";
    
    for (int batch = 0; batch < NUM_BATCHES; batch++) {
        std::cout << "Inserting batch " << batch << " (" << RECORDS_PER_BATCH << " records)...\n";
        
        for (int i = 0; i < RECORDS_PER_BATCH; i++) {
            int id = batch * RECORDS_PER_BATCH + i;
            DataRecord* record = new DataRecord(2, 32, std::to_string(id));
            std::vector<double> point = {(double)(id % 1000), (double)(id / 1000)};
            record->putPoint(&point);
            root->xt_insert(cachedRoot, record);
        }
        
        std::cout << "  Memory used: " << (snapshot_manager->get_snapshot_size() / (1024.0 * 1024.0)) << " MB\n";
    }
    
    // Now search across all segments
    std::cout << "\nSearching across segments...\n";
    
    // Search for records in different ranges
    struct SearchRange {
        float x_min, x_max, y_min, y_max;
        const char* description;
    };
    
    std::vector<SearchRange> ranges = {
        {0, 100, 0, 10, "Small range (early segment)"},
        {450, 550, 45, 55, "Medium range (middle)"},
        {900, 1000, 90, 150, "Large range (across segments)"},
        {0, 1000, 0, 200, "Full range (all segments)"}
    };
    
    for (const auto& range : ranges) {
        DataRecord* searchKey = new DataRecord(2, 32, "search");
        std::vector<double> min_point = {range.x_min, range.y_min};
        std::vector<double> max_point = {range.x_max, range.y_max};
        searchKey->putPoint(&min_point);
        searchKey->putPoint(&max_point);
        
        auto start = high_resolution_clock::now();
        Iterator<DataRecord>* iter = root->getIterator(cachedRoot, searchKey, INTERSECTS);
        
        int count = 0;
        while (iter->hasNext()) {
            DataRecord* result = iter->next();
            count++;
        }
        
        auto end = high_resolution_clock::now();
        auto search_time = duration_cast<microseconds>(end - start).count();
        
        delete iter;
        
        std::cout << "  " << range.description << ":\n";
        std::cout << "    Found: " << count << " records\n";
        std::cout << "    Time: " << search_time << " μs\n";
        std::cout << "    Rate: " << (count * 1000000.0 / search_time) << " records/sec\n";
    }
    
    std::cout << "\nTotal memory used: " << (snapshot_manager->get_snapshot_size() / (1024.0 * 1024.0)) << " MB\n";
    std::cout << "Arena size: " << (concurrent_allocator->get_arena_size() / (1024.0 * 1024.0)) << " MB\n";
}