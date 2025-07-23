/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test to compare snapshot sizes between current MMAP and Compact approaches
 */

#include <gtest/gtest.h>
#include <sys/stat.h>
#include "../src/indexdetails.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"

using namespace xtree;

TEST(SnapshotSizeComparison, DISABLED_CompareApproaches) {
    vector<const char*> dimLabels = {"x", "y"};
    const int NUM_RECORDS = 10000;
    
    // Test 1: FILE_IO approach (old COW-based)
    {
        IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::FILE_IO, "test_fileio.snapshot"
        );
        
        auto* allocator = index->getCOWAllocator();
        ASSERT_NE(allocator, nullptr);
        
        // Insert records
        for (int i = 0; i < NUM_RECORDS; i++) {
            DataRecord* record = allocator->allocate_record(2, 32, "test_" + std::to_string(i));
            vector<double> point = {static_cast<double>(i), static_cast<double>(i * 2)};
            record->putPoint(&point);
        }
        
        // Force snapshot
        index->getCOWManager()->trigger_memory_snapshot();
        
        delete index;
        
        // Check file size
        struct stat st;
        if (stat("test_fileio.snapshot", &st) == 0) {
            std::cout << "FILE_IO (COW) snapshot size: " << st.st_size << " bytes ("
                      << (st.st_size / 1024.0 / 1024.0) << " MB)" << std::endl;
        }
    }
    
    // Test 2: Compact approach
    {
        CompactSnapshotManager manager("test_compact.snapshot", 10 * 1024 * 1024);
        auto* allocator = manager.get_allocator();
        
        // Insert same records
        for (int i = 0; i < NUM_RECORDS; i++) {
            auto offset = allocator->allocate(sizeof(DataRecord));
            DataRecord* record = allocator->get_ptr<DataRecord>(offset);
            // Note: DataRecord constructor not called, but for size comparison it's OK
        }
        
        manager.save_snapshot();
        
        // Check file size
        struct stat st;
        if (stat("test_compact.snapshot", &st) == 0) {
            std::cout << "Compact snapshot size: " << st.st_size << " bytes ("
                      << (st.st_size / 1024.0 / 1024.0) << " MB)" << std::endl;
            std::cout << "Used allocator size: " << allocator->get_used_size() << " bytes" << std::endl;
        }
    }
    
    // Test 3: New MMAP approach (using CompactAllocator via IndexDetails)
    {
        IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
            2, 32, &dimLabels, nullptr, nullptr,
            IndexDetails<DataRecord>::PersistenceMode::MMAP, "test_mmap_compact.snapshot"
        );
        
        auto* allocator = index->getCompactAllocator();
        ASSERT_NE(allocator, nullptr);
        
        // Insert records
        for (int i = 0; i < NUM_RECORDS; i++) {
            DataRecord* record = allocator->allocate_record(2, 32, "test_" + std::to_string(i));
            vector<double> point = {static_cast<double>(i), static_cast<double>(i * 2)};
            record->putPoint(&point);
        }
        
        // Force snapshot
        allocator->save_snapshot();
        
        delete index;
        
        // Check file size
        struct stat st;
        if (stat("test_mmap_compact.snapshot", &st) == 0) {
            std::cout << "MMAP (Compact) snapshot size: " << st.st_size << " bytes ("
                      << (st.st_size / 1024.0 / 1024.0) << " MB)" << std::endl;
        }
    }
    
    // Cleanup
    std::remove("test_fileio.snapshot");
    std::remove("test_fileio.snapshot.tmp");
    std::remove("test_compact.snapshot");
    std::remove("test_mmap_compact.snapshot");
}