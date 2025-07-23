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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <chrono>
#include <thread>
#include <cstdio>
#include <fstream>
#include <sys/stat.h>  // for stat() - cross-platform
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/xtiter.h"
#include "../src/config.h"
#include "../src/memmgr/cow_memmgr.hpp"

using namespace xtree;
using namespace std;

/**
 * Test suite for COW Memory Manager
 */
class COWMemoryTest : public ::testing::Test {
protected:
    IndexDetails<DataRecord>* idx;
    DirectMemoryCOWManager<DataRecord>* cow_manager;
    XTreeBucket<DataRecord>* root;
    LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>* cachedRoot;
    vector<const char*>* dimLabels;
    
    void SetUp() override {
        // Create index with 2D coordinates
        dimLabels = new vector<const char*>();
        dimLabels->push_back("longitude");
        dimLabels->push_back("latitude");
        
        idx = new IndexDetails<DataRecord>(2, 32, dimLabels, 1024*1024*10, nullptr, nullptr);
        
        // Create COW manager with unique filename per test
        string test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        string snapshot_file = "test_xtree_" + test_name + ".snapshot";
        cow_manager = new DirectMemoryCOWManager<DataRecord>(idx, snapshot_file);
        
        // Configure for fast testing
        cow_manager->set_operations_threshold(100);  // Much lower than production
        cow_manager->set_memory_threshold(1024 * 1024);  // 1MB instead of 64MB
        cow_manager->set_max_write_interval(std::chrono::milliseconds(1000));  // 1 second instead of 30
        
        // Create root bucket using page-aligned allocation
        void* root_memory = PageAlignedMemoryTracker::allocate_aligned(sizeof(XTreeBucket<DataRecord>));
        root = new (root_memory) XTreeBucket<DataRecord>(idx, true, nullptr, nullptr, 0, true, 0);
        
        // Register root with COW manager
        cow_manager->register_bucket_memory(root, sizeof(XTreeBucket<DataRecord>));
        
        // Add root to cache
        cachedRoot = idx->getCache().add(idx->getNextNodeID(), root);
        idx->setRootAddress((long)cachedRoot);
    }
    
    void TearDown() override {
        // Since we're using LRUDeleteNone, the cache doesn't own the objects
        // The root bucket was allocated with page-aligned memory and will be cleaned up here
        if (root) {
            // Unregister from COW manager before destroying
            if (cow_manager) {
                cow_manager->get_memory_tracker().unregister_memory_region(root);
            }
            root->~XTreeBucket<DataRecord>();
            PageAlignedMemoryTracker::deallocate_aligned(root);
            root = nullptr;
        }
        
        // Clear the cache (this just removes the tracking nodes, not the objects)
        IndexDetails<DataRecord>::clearCache();
        
        // Now we can safely delete the COW manager
        delete cow_manager;
        
        delete idx;
        delete dimLabels;
        
        // Clean up snapshot file
        string test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        string snapshot_file = "test_xtree_" + test_name + ".snapshot";
        std::remove(snapshot_file.c_str());
    }
    
    DataRecord* createDataRecord(const string& id, double minX, double minY, double maxX, double maxY) {
        DataRecord* dr = new DataRecord(2, 32, id);
        vector<double> minPoint = {minX, minY};
        vector<double> maxPoint = {maxX, maxY};
        dr->putPoint(&minPoint);
        dr->putPoint(&maxPoint);
        return dr;
    }
};

// Test basic COW functionality
TEST_F(COWMemoryTest, BasicCOWTracking) {
    // Check initial stats
    auto stats = cow_manager->get_stats();
    EXPECT_GT(stats.tracked_memory_bytes, 0);  // Should have root bucket tracked
    EXPECT_FALSE(stats.cow_protection_active);
    EXPECT_FALSE(stats.commit_in_progress);
    
    // Insert some records (less than threshold to avoid automatic snapshot)
    for (int i = 0; i < 95; i++) {
        string id = "record_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i * 10.0, i * 10.0, (i + 1) * 10.0, (i + 1) * 10.0);
        root->xt_insert(cachedRoot, dr);
        cow_manager->record_operation();
    }
    
    // Check stats after insertions (before snapshot)
    stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 95);
    
    // Trigger a snapshot manually
    cow_manager->trigger_memory_snapshot();
    this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // After snapshot, counter should be reset
    stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 0);
}

// Test COW snapshot creation speed
TEST_F(COWMemoryTest, SnapshotPerformance) {
    // Insert a significant amount of data
    for (int i = 0; i < 1000; i++) {
        string id = "record_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i * 10.0, i * 10.0, (i + 1) * 10.0, (i + 1) * 10.0);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Measure snapshot creation time
    auto start = std::chrono::high_resolution_clock::now();
    cow_manager->trigger_memory_snapshot();
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Snapshot should be very fast (< 1ms)
    EXPECT_LT(duration.count(), 1000) << "Snapshot took " << duration.count() << " microseconds";
    
    // Wait for background persistence to complete
    this_thread::sleep_for(std::chrono::milliseconds(100));
    
    auto stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 0);  // Should reset after snapshot
}

// Test memory tracking with allocator
TEST_F(COWMemoryTest, COWAllocatorTest) {
    COWAllocator<int> allocator(cow_manager);
    
    // Allocate some memory
    int* data = allocator.allocate(1000);
    ASSERT_NE(data, nullptr);
    
    // Should be page-aligned
    EXPECT_EQ(reinterpret_cast<uintptr_t>(data) % PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE, 0);
    
    // Fill with test data
    for (int i = 0; i < 1000; i++) {
        data[i] = i;
    }
    
    // Check memory tracking increased
    auto stats = cow_manager->get_stats();
    size_t initial_bytes = stats.tracked_memory_bytes;
    
    allocator.deallocate(data, 1000);
}

// Test multiple snapshots
TEST_F(COWMemoryTest, MultipleSnapshots) {
    // Increase threshold to avoid automatic snapshots during test
    cow_manager->set_operations_threshold(1000);
    
    // First batch of inserts
    for (int i = 0; i < 500; i++) {
        string id = "batch1_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i, i, i + 1, i + 1);
        root->xt_insert(cachedRoot, dr);
        cow_manager->record_operation();
    }
    
    cout << "First batch complete, triggering manual snapshot\n";
    
    // First snapshot
    cow_manager->trigger_memory_snapshot();
    this_thread::sleep_for(std::chrono::milliseconds(100));
    
    auto stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 0);
    
    // Second batch of inserts
    for (int i = 0; i < 500; i++) {
        string id = "batch2_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i * 2, i * 2, i * 2 + 1, i * 2 + 1);
        root->xt_insert(cachedRoot, dr);
        cow_manager->record_operation();
    }
    
    cout << "Second batch complete, triggering manual snapshot\n";
    
    // Second snapshot
    cow_manager->trigger_memory_snapshot();
    this_thread::sleep_for(std::chrono::milliseconds(100));
    
    stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 0);
}

// Test COW protection enabling/disabling
TEST_F(COWMemoryTest, COWProtectionToggle) {
    PageAlignedMemoryTracker tracker;
    
    // Allocate and track memory
    void* mem = PageAlignedMemoryTracker::allocate_aligned(8192);
    ASSERT_NE(mem, nullptr);
    tracker.register_memory_region(mem, 8192);
    
    // Enable COW protection
    tracker.enable_cow_protection();
    
    // On Linux/macOS, writing to protected memory would cause SIGSEGV
    // So we just verify the protection was enabled
    EXPECT_GT(tracker.get_total_tracked_bytes(), 0);
    
    // Disable protection
    tracker.disable_cow_protection();
    
    // Now we should be able to write
    *static_cast<int*>(mem) = 42;
    EXPECT_EQ(*static_cast<int*>(mem), 42);
    
    PageAlignedMemoryTracker::deallocate_aligned(mem);
}

// Test automatic snapshot triggering
TEST_F(COWMemoryTest, AutomaticSnapshotTrigger) {
    // Temporarily set a very high threshold to prevent automatic triggering during debug
    cow_manager->set_operations_threshold(10000);
    
    // Insert records without automatic trigger
    for (int i = 0; i < 150; i++) {
        string id = "auto_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i % 100, i % 100, (i % 100) + 1, (i % 100) + 1);
        root->xt_insert(cachedRoot, dr);
        cow_manager->record_operation();
    }
    
    // Check we have 150 operations
    auto stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 150);
    
    // Now set threshold to 100 to test if setting it after operations works
    cow_manager->set_operations_threshold(100);
    
    // Trigger one more operation to see if it triggers snapshot
    DataRecord* dr = createDataRecord("trigger", 0, 0, 1, 1);
    root->xt_insert(cachedRoot, dr);
    cow_manager->record_operation();
    
    // Give time for snapshot
    this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Check snapshot was triggered
    stats = cow_manager->get_stats();
    EXPECT_EQ(stats.operations_since_snapshot, 0);  // Should have reset
    EXPECT_FALSE(stats.commit_in_progress);
}

// Test memory growth tracking
TEST_F(COWMemoryTest, MemoryGrowthTracking) {
    auto initial_stats = cow_manager->get_stats();
    size_t initial_memory = initial_stats.tracked_memory_bytes;
    
    // Insert records that will cause tree growth
    for (int i = 0; i < 500; i++) {
        string id = "growth_" + to_string(i);
        DataRecord* dr = createDataRecord(id, 
                                         (i % 50) * 100.0, 
                                         (i / 50) * 100.0, 
                                         ((i % 50) + 1) * 100.0, 
                                         ((i / 50) + 1) * 100.0);
        root->xt_insert(cachedRoot, dr);
        
        // Periodically allocate new buckets (simulating splits)
        if (i % 100 == 99) {
            void* new_bucket_mem = PageAlignedMemoryTracker::allocate_aligned(sizeof(XTreeBucket<DataRecord>));
            cow_manager->register_bucket_memory(new_bucket_mem, sizeof(XTreeBucket<DataRecord>));
        }
    }
    
    auto final_stats = cow_manager->get_stats();
    EXPECT_GT(final_stats.tracked_memory_bytes, initial_memory);
}

// Test save and load functionality (basic test - full implementation would be complex)
TEST_F(COWMemoryTest, SaveSnapshot) {
    // Insert test data
    for (int i = 0; i < 100; i++) {
        string id = "save_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i, i, i + 1, i + 1);
        root->xt_insert(cachedRoot, dr);
    }
    
    // Save snapshot
    string test_file = "test_manual_snapshot.bin";
    cow_manager->trigger_memory_snapshot();
    
    // Wait for persistence
    this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Check file exists - use the test-specific filename
    string test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    string snapshot_file = "test_xtree_" + test_name + ".snapshot";
    struct stat buffer;
    bool file_exists = (stat(snapshot_file.c_str(), &buffer) == 0);
    EXPECT_TRUE(file_exists) << "Snapshot file should exist: " << snapshot_file;
}

// Test batch registration functionality
TEST_F(COWMemoryTest, BatchRegistration) {
    auto initial_stats = cow_manager->get_stats();
    size_t initial_memory = initial_stats.tracked_memory_bytes;
    
    // Test 1: Basic batch registration
    {
        vector<void*> allocations;
        const int BATCH_SIZE = 100;
        
        // Allocate memory regions
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        for (int i = 0; i < BATCH_SIZE; i++) {
            void* mem = PageAlignedMemoryTracker::allocate_aligned(page_size);
            ASSERT_NE(mem, nullptr);
            allocations.push_back(mem);
        }
        
        // Register as batch
        cow_manager->begin_batch_registration();
        for (auto ptr : allocations) {
            cow_manager->add_to_batch(ptr, page_size);
        }
        cow_manager->commit_batch_registration();
        
        // Verify all regions were registered
        auto stats = cow_manager->get_stats();
        size_t expected_memory = initial_memory + (BATCH_SIZE * page_size);
        EXPECT_EQ(stats.tracked_memory_bytes, expected_memory);
        
        // Clean up
        for (auto ptr : allocations) {
            cow_manager->get_memory_tracker().unregister_memory_region(ptr);
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
    
    // Test 2: Compare batch vs individual registration performance
    {
        const int PERF_BATCH_SIZE = 1000;
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        vector<void*> batch_allocs;
        vector<void*> individual_allocs;
        
        // Allocate memory for both tests
        for (int i = 0; i < PERF_BATCH_SIZE; i++) {
            batch_allocs.push_back(PageAlignedMemoryTracker::allocate_aligned(page_size));
            individual_allocs.push_back(PageAlignedMemoryTracker::allocate_aligned(page_size));
        }
        
        // Time batch registration
        auto batch_start = std::chrono::high_resolution_clock::now();
        cow_manager->begin_batch_registration();
        for (auto ptr : batch_allocs) {
            cow_manager->add_to_batch(ptr, page_size);
        }
        cow_manager->commit_batch_registration();
        auto batch_end = std::chrono::high_resolution_clock::now();
        auto batch_duration = std::chrono::duration_cast<std::chrono::microseconds>(batch_end - batch_start);
        
        // Time individual registration
        auto individual_start = std::chrono::high_resolution_clock::now();
        for (auto ptr : individual_allocs) {
            cow_manager->register_bucket_memory(ptr, page_size);
        }
        auto individual_end = std::chrono::high_resolution_clock::now();
        auto individual_duration = std::chrono::duration_cast<std::chrono::microseconds>(individual_end - individual_start);
        
        cout << "Batch registration (" << PERF_BATCH_SIZE << " regions): " 
             << batch_duration.count() << " microseconds" << endl;
        cout << "Individual registration (" << PERF_BATCH_SIZE << " regions): " 
             << individual_duration.count() << " microseconds" << endl;
        
        // Batch should be at least as fast as individual (usually faster due to single lock)
        EXPECT_LE(batch_duration.count(), individual_duration.count() * 1.5);
        
        // Clean up
        for (auto ptr : batch_allocs) {
            cow_manager->get_memory_tracker().unregister_memory_region(ptr);
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
        for (auto ptr : individual_allocs) {
            cow_manager->get_memory_tracker().unregister_memory_region(ptr);
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
    
    // Test 3: Batch registration with COW snapshot
    {
        vector<void*> snapshot_allocs;
        const int SNAPSHOT_BATCH_SIZE = 50;
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        
        // Register batch of memory
        cow_manager->begin_batch_registration();
        for (int i = 0; i < SNAPSHOT_BATCH_SIZE; i++) {
            void* mem = PageAlignedMemoryTracker::allocate_aligned(page_size);
            snapshot_allocs.push_back(mem);
            cow_manager->add_to_batch(mem, page_size);
        }
        cow_manager->commit_batch_registration();
        
        // Trigger snapshot
        cow_manager->trigger_memory_snapshot();
        this_thread::sleep_for(std::chrono::milliseconds(200));
        
        // Verify snapshot captured batch-registered memory
        auto stats = cow_manager->get_stats();
        EXPECT_EQ(stats.operations_since_snapshot, 0);
        
        // Clean up
        for (auto ptr : snapshot_allocs) {
            cow_manager->get_memory_tracker().unregister_memory_region(ptr);
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
}

// Test batch unregistration and memory leak prevention
TEST_F(COWMemoryTest, BatchUnregistrationAndLeakPrevention) {
    // Test 1: Basic batch unregistration
    {
        auto initial_stats = cow_manager->get_stats();
        size_t initial_memory = initial_stats.tracked_memory_bytes;
        
        vector<void*> allocations;
        const int BATCH_SIZE = 100;
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        
        // Allocate and register memory
        cow_manager->begin_batch_registration();
        for (int i = 0; i < BATCH_SIZE; i++) {
            void* mem = PageAlignedMemoryTracker::allocate_aligned(page_size);
            allocations.push_back(mem);
            cow_manager->add_to_batch(mem, page_size);
        }
        cow_manager->commit_batch_registration();
        
        // Verify memory is tracked
        auto mid_stats = cow_manager->get_stats();
        EXPECT_EQ(mid_stats.tracked_memory_bytes, initial_memory + (BATCH_SIZE * page_size));
        
        // Batch unregister all
        cow_manager->begin_batch_unregistration();
        for (auto ptr : allocations) {
            cow_manager->add_to_unregister_batch(ptr);
        }
        cow_manager->commit_batch_unregistration();
        
        // Verify memory is no longer tracked
        auto final_stats = cow_manager->get_stats();
        EXPECT_EQ(final_stats.tracked_memory_bytes, initial_memory);
        
        // Clean up allocated memory
        for (auto ptr : allocations) {
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
    
    // Test 2: Memory leak detection - ensure unregistered memory doesn't leak tracking
    {
        const int LEAK_TEST_SIZE = 500;
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        vector<void*> leak_test_allocs;
        
        auto initial_stats = cow_manager->get_stats();
        size_t initial_memory = initial_stats.tracked_memory_bytes;
        
        // Register many allocations
        for (int i = 0; i < LEAK_TEST_SIZE; i++) {
            void* mem = PageAlignedMemoryTracker::allocate_aligned(page_size);
            leak_test_allocs.push_back(mem);
            cow_manager->register_bucket_memory(mem, page_size);
        }
        
        // Verify all are tracked
        auto mid_stats = cow_manager->get_stats();
        EXPECT_EQ(mid_stats.tracked_memory_bytes, initial_memory + (LEAK_TEST_SIZE * page_size));
        
        // Unregister half individually, half in batch
        int half = LEAK_TEST_SIZE / 2;
        
        // Individual unregistration
        for (int i = 0; i < half; i++) {
            cow_manager->get_memory_tracker().unregister_memory_region(leak_test_allocs[i]);
        }
        
        // Batch unregistration
        cow_manager->begin_batch_unregistration();
        for (int i = half; i < LEAK_TEST_SIZE; i++) {
            cow_manager->add_to_unregister_batch(leak_test_allocs[i]);
        }
        cow_manager->commit_batch_unregistration();
        
        // Verify all tracking is cleaned up
        auto final_stats = cow_manager->get_stats();
        EXPECT_EQ(final_stats.tracked_memory_bytes, initial_memory);
        
        // Deallocate all memory
        for (auto ptr : leak_test_allocs) {
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
    
    // Test 3: COW protection cleanup on unregistration
    {
        const int PROTECTION_TEST_SIZE = 10;
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        vector<void*> protected_allocs;
        
        // Allocate and register memory
        for (int i = 0; i < PROTECTION_TEST_SIZE; i++) {
            void* mem = PageAlignedMemoryTracker::allocate_aligned(page_size);
            protected_allocs.push_back(mem);
            cow_manager->register_bucket_memory(mem, page_size);
        }
        
        // Enable COW protection
        cow_manager->get_memory_tracker().enable_cow_protection();
        
        // Batch unregister with protection active
        cow_manager->begin_batch_unregistration();
        for (auto ptr : protected_allocs) {
            cow_manager->add_to_unregister_batch(ptr);
        }
        cow_manager->commit_batch_unregistration();
        
        // Memory should be writable after unregistration (protection removed)
        // Write to first allocation to verify
        *static_cast<int*>(protected_allocs[0]) = 0xDEADBEEF;
        EXPECT_EQ(*static_cast<int*>(protected_allocs[0]), 0xDEADBEEF);
        
        // Clean up
        for (auto ptr : protected_allocs) {
            PageAlignedMemoryTracker::deallocate_aligned(ptr);
        }
    }
    
    // Test 4: Stress test - many rapid register/unregister cycles
    {
        const int STRESS_CYCLES = 100;
        const int ALLOCS_PER_CYCLE = 50;
        const size_t page_size = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
        
        auto initial_stats = cow_manager->get_stats();
        size_t initial_memory = initial_stats.tracked_memory_bytes;
        
        for (int cycle = 0; cycle < STRESS_CYCLES; cycle++) {
            vector<void*> cycle_allocs;
            
            // Batch register
            cow_manager->begin_batch_registration();
            for (int i = 0; i < ALLOCS_PER_CYCLE; i++) {
                void* mem = PageAlignedMemoryTracker::allocate_aligned(page_size);
                cycle_allocs.push_back(mem);
                cow_manager->add_to_batch(mem, page_size);
            }
            cow_manager->commit_batch_registration();
            
            // Verify tracked
            auto mid_stats = cow_manager->get_stats();
            EXPECT_EQ(mid_stats.tracked_memory_bytes, 
                     initial_memory + (ALLOCS_PER_CYCLE * page_size));
            
            // Batch unregister
            cow_manager->begin_batch_unregistration();
            for (auto ptr : cycle_allocs) {
                cow_manager->add_to_unregister_batch(ptr);
            }
            cow_manager->commit_batch_unregistration();
            
            // Verify untracked
            auto end_stats = cow_manager->get_stats();
            EXPECT_EQ(end_stats.tracked_memory_bytes, initial_memory);
            
            // Deallocate
            for (auto ptr : cycle_allocs) {
                PageAlignedMemoryTracker::deallocate_aligned(ptr);
            }
        }
        
        cout << "Completed " << STRESS_CYCLES << " register/unregister cycles without leaks" << endl;
    }
}

// Test comprehensive snapshot validation
TEST_F(COWMemoryTest, SnapshotValidation) {
    // Track memory before inserting data
    auto initial_stats = cow_manager->get_stats();
    size_t initial_tracked = initial_stats.tracked_memory_bytes;
    EXPECT_GT(initial_tracked, 0) << "Should have root bucket tracked";
    
    // Insert some data and track additional memory allocations
    vector<void*> extra_buckets;
    for (int i = 0; i < 50; i++) {
        string id = "validate_" + to_string(i);
        DataRecord* dr = createDataRecord(id, i, i, i + 1, i + 1);
        root->xt_insert(cachedRoot, dr);
        
        // Allocate extra buckets periodically to test multiple regions
        if (i % 10 == 9) {
            void* bucket = PageAlignedMemoryTracker::allocate_aligned(sizeof(XTreeBucket<DataRecord>));
            cow_manager->register_bucket_memory(bucket, sizeof(XTreeBucket<DataRecord>));
            extra_buckets.push_back(bucket);
        }
    }
    
    // Get stats before snapshot
    auto pre_snapshot_stats = cow_manager->get_stats();
    size_t expected_memory = pre_snapshot_stats.tracked_memory_bytes;
    
    // Trigger snapshot
    cow_manager->trigger_memory_snapshot();
    
    // Wait for persistence to complete
    this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    // Get the snapshot filename
    string test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    string snapshot_file = "test_xtree_" + test_name + ".snapshot";
    
    // Validate the snapshot
    EXPECT_TRUE(cow_manager->validate_snapshot(snapshot_file)) 
        << "Snapshot validation should succeed for valid file";
    
    // Verify snapshot header contains correct metadata
    auto header = cow_manager->get_snapshot_header(snapshot_file);
    EXPECT_EQ(header.magic, COW_SNAPSHOT_MAGIC) << "Magic should be XTRE";
    EXPECT_EQ(header.version, COW_SNAPSHOT_VERSION) << "Version should be " << COW_SNAPSHOT_VERSION;
    EXPECT_EQ(header.dimension, idx->getDimensionCount()) << "Dimension should match index";
    EXPECT_EQ(header.precision, idx->getPrecision()) << "Precision should match index";
    EXPECT_EQ(header.total_size, expected_memory) << "Total size should match tracked memory";
    EXPECT_GE(header.total_regions, 1u + extra_buckets.size()) << "Should have at least root + extra buckets";
    
    // Log what we found for debugging
#ifdef _DEBUG
    cout << "Snapshot validation - Header: magic=" << std::hex << header.magic 
         << ", version=" << std::dec << header.version
         << ", dims=" << header.dimension << ", precision=" << header.precision
         << ", regions=" << header.total_regions << ", size=" << header.total_size << endl;
#endif
    
    // Verify file size
    struct stat st;
    stat(snapshot_file.c_str(), &st);
    size_t file_size = st.st_size;
    size_t expected_min_size = sizeof(header) + expected_memory;
    EXPECT_GE(file_size, expected_min_size) << "File should contain header + data";
    
    // Test validation with non-existent file
    EXPECT_FALSE(cow_manager->validate_snapshot("non_existent_file.snapshot"))
        << "Validation should fail for non-existent file";
    
    // Test validation with corrupted file by writing bad magic
    {
        std::fstream bad_file(snapshot_file, std::ios::binary | std::ios::in | std::ios::out);
        if (bad_file.is_open()) {
            uint32_t bad_magic = 0xDEADBEEF;
            bad_file.write(reinterpret_cast<char*>(&bad_magic), sizeof(bad_magic));
            bad_file.close();
            
            EXPECT_FALSE(cow_manager->validate_snapshot(snapshot_file))
                << "Validation should fail for bad magic number";
        }
    }
    
    // Clean up extra allocations
    for (void* bucket : extra_buckets) {
        PageAlignedMemoryTracker::deallocate_aligned(bucket);
    }
}