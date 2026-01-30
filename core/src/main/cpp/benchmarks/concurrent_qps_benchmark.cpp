/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Concurrent QPS benchmark for XTree with multi-segment allocator
 * Tests true concurrent reads and writes across multiple segments
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <iomanip>
#include <condition_variable>
#include <mutex>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"
#include "../src/xtree_allocator_traits.hpp"

using namespace xtree;
using namespace std::chrono;

TEST(ConcurrentQPSBenchmark, SimpleDebug) {
    std::cout << "Simple debug test started\n" << std::flush;
    std::cout << "Test completed\n" << std::flush;
}

TEST(ConcurrentQPSBenchmark, MixedReadWriteQPS) {
    std::cout << "\nTest started - about to remove file\n" << std::flush;
    // Clean up any existing files
    std::remove("/tmp/concurrent_qps_test.snapshot");
    std::cout << "File removed\n" << std::flush;
    std::cout << "\n=== Concurrent XTree QPS Benchmark ===\n" << std::flush;
    std::cout << "Testing concurrent reads and writes across multiple segments\n\n" << std::flush;
    
    std::cout << "Creating index...\n" << std::flush;
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 128, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, "/tmp/concurrent_qps_test.snapshot"
    );
    std::cout << "Index created.\n";
    
    // Initialize root bucket
    std::cout << "Initializing root bucket...\n";
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    std::cout << "Root allocated.\n";
    auto* cacheNode = index->getCache().add(index->getNextNodeID(), root);
    std::cout << "Root cached.\n";
    index->setRootAddress((long)cacheNode);
    std::cout << "Root address set.\n";
    
    // Pre-populate with data to create a meaningful tree
    std::cout << "Pre-populating XTree with initial data...\n";
    const int INITIAL_POINTS = 100; // Start with less data for debugging
    std::mt19937 gen(42);
    std::uniform_int_distribution<> dist(0, 10000);
    
    for (int i = 0; i < INITIAL_POINTS; i++) {
        float x = dist(gen);
        float y = dist(gen);
        
        auto* allocator = index->getCompactAllocator();
        auto* record = allocator ? 
            allocator->allocate_record(2, 32, std::to_string(i)) : 
            new DataRecord(2, 32, std::to_string(i));
        std::vector<double> minPt = {(double)x, (double)y};
        std::vector<double> maxPt = {(double)x, (double)y};
        record->putPoint(&minPt);
        record->putPoint(&maxPt);
        
        // Get fresh root for concurrent safety
        auto rootAddress = index->getRootAddress();
        cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
        root = (XTreeBucket<DataRecord>*)(cacheNode->object);
        root->xt_insert(cacheNode, record);
        
        if (i % 10000 == 0 && index->getCompactAllocator()) {
            std::cout << "  Inserted " << i << " points, segments: " 
                      << index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_segment_count() << "\n";
        }
    }
    
    std::cout << "\nInitial state:\n";
    std::cout << "  Points: " << INITIAL_POINTS << "\n";
    std::cout << "  Segments: " << index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_segment_count() << "\n";
    std::cout << "  Memory used: " << index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_used_size() / (1024.0 * 1024.0) << " MB\n\n";
    
    // Test configurations
    struct TestConfig {
        int reader_threads;
        int writer_threads;
        int queries_per_reader;
        int inserts_per_writer;
        double query_range_size;
    };
    
    std::vector<TestConfig> configs = {
        {1, 0, 10000, 0, 100.0},      // Read-only baseline
        {4, 0, 10000, 0, 100.0},      // Read-only parallel
        {8, 0, 10000, 0, 100.0},      // Read-only max parallel
        {0, 1, 0, 10000, 0.0},        // Write-only baseline
        {0, 4, 0, 2500, 0.0},         // Write-only parallel
        {4, 1, 10000, 2000, 100.0},   // Mixed: 4 readers, 1 writer
        {4, 2, 10000, 1000, 100.0},   // Mixed: 4 readers, 2 writers
        {8, 4, 5000, 500, 100.0},     // Mixed: 8 readers, 4 writers
    };
    
    std::cout << "Running concurrent QPS tests...\n";
    std::cout << "Readers | Writers | Queries | Inserts | Time(ms) | Read QPS | Write QPS | Segments | Memory(MB)\n";
    std::cout << "--------|---------|---------|---------|----------|----------|-----------|----------|----------\n";
    
    for (const auto& config : configs) {
        // Prepare for this test run
        std::atomic<int> total_queries{0};
        std::atomic<int> total_inserts{0};
        std::atomic<int> query_results{0};
        std::atomic<bool> stop_flag{false};
        
        auto start_segments = index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_segment_count();
        auto start_memory = index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_used_size();
        
        // Simple barrier implementation for C++17
        struct SimpleBarrier {
            std::mutex mutex;
            std::condition_variable cv;
            int count;
            int waiting = 0;
            
            SimpleBarrier(int n) : count(n) {}
            
            void wait() {
                std::unique_lock<std::mutex> lock(mutex);
                if (++waiting == count) {
                    cv.notify_all();
                } else {
                    cv.wait(lock, [this] { return waiting == count; });
                }
            }
        };
        
        int total_threads = config.reader_threads + config.writer_threads;
        auto sync_point = std::make_shared<SimpleBarrier>(total_threads);
        
        // Reader thread function
        auto reader_func = [&](int thread_id) {
            std::mt19937 local_gen(thread_id);
            std::uniform_real_distribution<float> pos_dist(0, 10000);
            
            sync_point->wait();
            
            for (int i = 0; i < config.queries_per_reader; i++) {
                float x = pos_dist(local_gen);
                float y = pos_dist(local_gen);
                
                // Create search record with bounding box using allocator
                auto* allocator = index->getCompactAllocator();
                DataRecord* searchRecord = allocator ? 
                    allocator->allocate_record(2, 32, "search") : 
                    new DataRecord(2, 32, "search");
                std::vector<double> minPoint = {x - config.query_range_size/2, y - config.query_range_size/2};
                std::vector<double> maxPoint = {x + config.query_range_size/2, y + config.query_range_size/2};
                searchRecord->putPoint(&minPoint);
                searchRecord->putPoint(&maxPoint);
                
                // Get root and perform search
                auto rootAddress = index->getRootAddress();
                auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                auto* root = (XTreeBucket<DataRecord>*)(cacheNode->object);
                
                auto iter = root->getIterator(cacheNode, searchRecord, INTERSECTS);
                int count = 0;
                while (iter->hasNext()) {
                    auto* result = iter->next();
                    if (result) count++;
                }
                query_results += count;
                
                delete iter;
                // Don't delete searchRecord if using allocator - it's managed by the allocator
                if (!allocator) {
                    delete searchRecord;
                }
                
                total_queries++;
            }
        };
        
        // Writer thread function
        auto writer_func = [&](int thread_id) {
            std::mt19937 local_gen(1000 + thread_id);
            std::uniform_int_distribution<> pos_dist(0, 10000);
            int base_id = INITIAL_POINTS + thread_id * 100000;
            
            sync_point->wait();
            
            for (int i = 0; i < config.inserts_per_writer; i++) {
                float x = pos_dist(local_gen);
                float y = pos_dist(local_gen);
                
                // Create record using allocator
                auto* allocator = index->getCompactAllocator();
                auto* record = allocator ?
                    allocator->allocate_record(2, 32, std::to_string(base_id + i)) :
                    new DataRecord(2, 32, std::to_string(base_id + i));
                
                std::vector<double> minPt = {(double)x, (double)y};
                std::vector<double> maxPt = {(double)x, (double)y};
                record->putPoint(&minPt);
                record->putPoint(&maxPt);
                
                // Get fresh root for each insert
                auto rootAddress = index->getRootAddress();
                auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                auto* root = (XTreeBucket<DataRecord>*)(cacheNode->object);
                root->xt_insert(cacheNode, record);
                
                total_inserts++;
            }
        };
        
        // Start timing
        auto start_time = high_resolution_clock::now();
        
        // Launch threads
        std::vector<std::thread> threads;
        
        // Launch readers
        for (int i = 0; i < config.reader_threads; i++) {
            threads.emplace_back(reader_func, i);
        }
        
        // Launch writers
        for (int i = 0; i < config.writer_threads; i++) {
            threads.emplace_back(writer_func, i);
        }
        
        // Wait for all threads
        for (auto& t : threads) {
            t.join();
        }
        
        auto end_time = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end_time - start_time);
        
        // Calculate metrics
        double time_sec = duration.count() / 1000.0;
        double read_qps = total_queries.load() / time_sec;
        double write_qps = total_inserts.load() / time_sec;
        
        auto end_segments = index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_segment_count();
        auto end_memory = index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_used_size();
        
        // Print results
        std::cout << std::setw(7) << config.reader_threads << " | "
                  << std::setw(7) << config.writer_threads << " | "
                  << std::setw(7) << total_queries.load() << " | "
                  << std::setw(7) << total_inserts.load() << " | "
                  << std::setw(8) << duration.count() << " | "
                  << std::setw(8) << std::fixed << std::setprecision(0) << read_qps << " | "
                  << std::setw(9) << std::fixed << std::setprecision(0) << write_qps << " | "
                  << std::setw(8) << end_segments << " | "
                  << std::setw(9) << std::fixed << std::setprecision(1) 
                  << (end_memory / (1024.0 * 1024.0)) << "\n";
    }
    
    // Final statistics
    std::cout << "\nFinal state:\n";
    std::cout << "  Total segments: " << index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_segment_count() << "\n";
    std::cout << "  Total memory: " << index->getCompactAllocator()->get_snapshot_manager()->get_allocator()->get_used_size() / (1024.0 * 1024.0) << " MB\n";
    
    delete index;
}

TEST(ConcurrentQPSBenchmark, ScalingAnalysis) {
    // Clean up any existing files
    std::remove("/tmp/concurrent_qps_test.snapshot");
    std::cout << "\n=== Concurrent Scaling Analysis ===\n";
    std::cout << "Testing how QPS scales with thread count\n\n";
    
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 128, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, "/tmp/concurrent_qps_test.snapshot"
    );
    
    // Insert 1M points for realistic testing
    std::cout << "Creating large dataset (1M points)...\n";
    const int DATASET_SIZE = 1000000;
    std::mt19937 gen(42);
    std::uniform_int_distribution<> dist(0, 100000);
    
    for (int i = 0; i < DATASET_SIZE; i++) {
        float x = dist(gen);
        float y = dist(gen);
        // Create record using allocator
        auto* allocator = index->getCompactAllocator();
        auto* record = allocator ?
            allocator->allocate_record(2, 32, std::to_string(i)) :
            new DataRecord(2, 32, std::to_string(i));
        
        std::vector<double> minPt = {(double)x, (double)y};
        std::vector<double> maxPt = {(double)x, (double)y};
        record->putPoint(&minPt);
        record->putPoint(&maxPt);
        
        // Get fresh root for each insert
        auto rootAddress = index->getRootAddress();
        auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
        auto* root = (XTreeBucket<DataRecord>*)(cacheNode->object);
        root->xt_insert(cacheNode, record);
        
        if (i % 100000 == 0) {
            std::cout << "  " << i/1000 << "K points inserted\n";
        }
    }
    
    std::cout << "\nDataset ready. Testing scaling...\n";
    std::cout << "Threads | Queries | Time(ms) | QPS     | Efficiency\n";
    std::cout << "--------|---------|----------|---------|----------\n";
    
    const int QUERIES_PER_THREAD = 10000;
    double baseline_qps = 0;
    
    for (int num_threads : {1, 2, 4, 8, 16, 32}) {
        std::atomic<int> total_queries{0};
        std::atomic<int> total_results{0};
        
        auto start_time = high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([&, i]() {
                std::mt19937 local_gen(i);
                std::uniform_real_distribution<float> pos_dist(0, 100000);
                
                for (int q = 0; q < QUERIES_PER_THREAD; q++) {
                    float x = pos_dist(local_gen);
                    float y = pos_dist(local_gen);
                    
                    // Create search record with bounding box using allocator
                    auto* allocator = index->getCompactAllocator();
                    DataRecord* searchRecord = allocator ?
                        allocator->allocate_record(2, 32, "search") :
                        new DataRecord(2, 32, "search");
                    std::vector<double> minPoint = {x - 500, y - 500};
                    std::vector<double> maxPoint = {x + 500, y + 500};
                    searchRecord->putPoint(&minPoint);
                    searchRecord->putPoint(&maxPoint);
                    
                    // Get root and perform search
                    auto rootAddress = index->getRootAddress();
                    auto* cacheNode = (LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>*)rootAddress;
                    auto* root = (XTreeBucket<DataRecord>*)(cacheNode->object);
                    
                    auto iter = root->getIterator(cacheNode, searchRecord, INTERSECTS);
                    int count = 0;
                    while (iter->hasNext()) {
                        auto* result = iter->next();
                        if (result) count++;
                    }
                    total_results += count;
                    
                    delete iter;
                    // Don't delete searchRecord if using allocator
                    if (!allocator) {
                        delete searchRecord;
                    }
                    
                    total_queries++;
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end_time = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end_time - start_time);
        
        double qps = (total_queries.load() * 1000.0) / duration.count();
        if (num_threads == 1) baseline_qps = qps;
        double efficiency = (qps / baseline_qps) / num_threads * 100.0;
        
        std::cout << std::setw(7) << num_threads << " | "
                  << std::setw(7) << total_queries.load() << " | "
                  << std::setw(8) << duration.count() << " | "
                  << std::setw(7) << std::fixed << std::setprecision(0) << qps << " | "
                  << std::setw(8) << std::fixed << std::setprecision(1) << efficiency << "%\n";
    }
    
    delete index;
}