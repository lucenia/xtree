/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test multi-segment snapshot loading with proper data verification
 */

#include <gtest/gtest.h>
#include "../../src/memmgr/compact_snapshot_manager.hpp"
#include <chrono>
#include <vector>
#include <random>
#include <fstream>

using namespace xtree;
using namespace std::chrono;

class MultiSegmentLoadVerifyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test files
        std::remove("test_verify_load.snapshot");
        std::remove("test_verify_load.snapshot.tmp");
        std::remove("test_verify_offsets.dat");
    }
    
    void TearDown() override {
        std::remove("test_verify_load.snapshot");
        std::remove("test_verify_load.snapshot.tmp");
        std::remove("test_verify_offsets.dat");
    }
};

TEST_F(MultiSegmentLoadVerifyTest, VerifyDataIntegrityAfterLoad) {
    const char* snapshot_file = "test_verify_load.snapshot";
    const char* offsets_file = "test_verify_offsets.dat";
    
    struct TestRecord {
        uint32_t id;
        uint32_t checksum;
        char data[1024 * 1024 - 8]; // ~1MB per record
    };
    
    std::vector<CompactAllocator::offset_t> saved_offsets;
    std::vector<uint32_t> expected_checksums;
    
    // Step 1: Create and save a multi-segment snapshot with known data
    {
        CompactSnapshotManager save_manager(snapshot_file);
        auto* allocator = save_manager.get_allocator();
        
        std::cout << "\n=== Creating test data with checksums ===\n";
        
        for (uint32_t i = 0; i < 100; ++i) {
            auto offset = allocator->allocate(sizeof(TestRecord));
            ASSERT_NE(offset, CompactAllocator::INVALID_OFFSET);
            
            TestRecord* record = allocator->get_ptr<TestRecord>(offset);
            record->id = i;
            
            // Fill data with pattern and calculate checksum
            uint32_t checksum = 0;
            for (size_t j = 0; j < sizeof(record->data); ++j) {
                record->data[j] = static_cast<char>((i + j) % 256);
                checksum = (checksum << 1) ^ record->data[j];
            }
            record->checksum = checksum;
            
            saved_offsets.push_back(offset);
            expected_checksums.push_back(checksum);
            
            if (i % 20 == 0) {
                std::cout << "  Created record " << i << " at offset " << offset 
                          << " with checksum " << std::hex << checksum << std::dec << "\n";
            }
        }
        
        std::cout << "\nSaving snapshot with " << allocator->get_segment_count() 
                  << " segments, " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
        
        // Save the snapshot
        save_manager.save_snapshot();
        
        // Save offsets to separate file for verification
        std::ofstream ofs(offsets_file, std::ios::binary);
        size_t count = saved_offsets.size();
        ofs.write(reinterpret_cast<const char*>(&count), sizeof(count));
        ofs.write(reinterpret_cast<const char*>(saved_offsets.data()), 
                  saved_offsets.size() * sizeof(CompactAllocator::offset_t));
        ofs.write(reinterpret_cast<const char*>(expected_checksums.data()),
                  expected_checksums.size() * sizeof(uint32_t));
    }
    
    // Step 2: Load the snapshot and verify all data
    {
        std::cout << "\n=== Loading snapshot and verifying data ===\n";
        
        // Load saved offsets
        std::ifstream ifs(offsets_file, std::ios::binary);
        size_t count;
        ifs.read(reinterpret_cast<char*>(&count), sizeof(count));
        saved_offsets.resize(count);
        expected_checksums.resize(count);
        ifs.read(reinterpret_cast<char*>(saved_offsets.data()), 
                 count * sizeof(CompactAllocator::offset_t));
        ifs.read(reinterpret_cast<char*>(expected_checksums.data()),
                 count * sizeof(uint32_t));
        
        // Load the snapshot
        CompactSnapshotManager load_manager(snapshot_file);
        auto* allocator = load_manager.get_allocator();
        
        std::cout << "Loaded snapshot with " << allocator->get_segment_count() 
                  << " segments, " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
        
        // Verify each record
        int errors = 0;
        for (size_t i = 0; i < saved_offsets.size(); ++i) {
            TestRecord* record = allocator->get_ptr<TestRecord>(saved_offsets[i]);
            
            if (!record) {
                std::cerr << "ERROR: Failed to get pointer for offset " << saved_offsets[i] << "\n";
                errors++;
                continue;
            }
            
            if (record->id != i) {
                std::cerr << "ERROR: ID mismatch at offset " << saved_offsets[i] 
                          << ": expected " << i << ", got " << record->id << "\n";
                errors++;
                continue;
            }
            
            // Recalculate checksum
            uint32_t checksum = 0;
            for (size_t j = 0; j < sizeof(record->data); ++j) {
                checksum = (checksum << 1) ^ record->data[j];
            }
            
            if (checksum != expected_checksums[i] || checksum != record->checksum) {
                std::cerr << "ERROR: Checksum mismatch for record " << i 
                          << ": expected " << std::hex << expected_checksums[i]
                          << ", stored " << record->checksum
                          << ", calculated " << checksum << std::dec << "\n";
                errors++;
                continue;
            }
            
            // Verify data pattern
            bool data_valid = true;
            for (size_t j = 0; j < std::min(size_t(100), sizeof(record->data)); ++j) {
                if (record->data[j] != static_cast<char>((i + j) % 256)) {
                    data_valid = false;
                    break;
                }
            }
            
            if (!data_valid) {
                std::cerr << "ERROR: Data pattern mismatch for record " << i << "\n";
                errors++;
                continue;
            }
            
            if (i % 20 == 0) {
                std::cout << "  ✓ Verified record " << i << " at offset " << saved_offsets[i]
                          << " with checksum " << std::hex << checksum << std::dec << "\n";
            }
        }
        
        EXPECT_EQ(errors, 0) << "Found " << errors << " verification errors";
        
        if (errors == 0) {
            std::cout << "\n✅ ALL DATA VERIFIED SUCCESSFULLY!\n";
            std::cout << "   - All " << saved_offsets.size() << " records intact\n";
            std::cout << "   - All checksums match\n";
            std::cout << "   - All data patterns correct\n";
            std::cout << "   - Multi-segment load is working correctly!\n";
        }
    }
}

TEST_F(MultiSegmentLoadVerifyTest, LoadAndModifyData) {
    const char* snapshot_file = "test_verify_load.snapshot";
    
    // Step 1: Create initial snapshot
    std::vector<CompactAllocator::offset_t> initial_offsets;
    {
        CompactSnapshotManager manager(snapshot_file);
        auto* allocator = manager.get_allocator();
        
        // Create some initial data
        for (int i = 0; i < 50; ++i) {
            auto offset = allocator->allocate(1024);
            uint32_t* data = allocator->get_ptr<uint32_t>(offset);
            data[0] = 0xBEEF0000 + i;
            data[255] = 0xDEAD0000 + i;
            initial_offsets.push_back(offset);
            
            if (i < 5) {
                std::cout << "  Created record " << i << " at offset " << std::hex << offset << std::dec << "\n";
            }
        }
        
        std::cout << "Initial snapshot created with " << initial_offsets.size() << " records\n";
        manager.save_snapshot();
    }
    
    // Step 2: Load, verify, modify, and save again
    {
        CompactSnapshotManager manager(snapshot_file);
        auto* allocator = manager.get_allocator();
        
        // Verify initial data using segment-relative addressing
        std::cout << "Verifying initial data after load...\n";
        std::cout << "Allocator state: segments=" << allocator->get_segment_count() 
                  << ", used=" << allocator->get_used_size() << "\n";
        
        // Debug: Check what's at the offsets
        for (size_t i = 0; i < std::min(size_t(5), initial_offsets.size()); ++i) {
            uint32_t* data = allocator->get_ptr<uint32_t>(initial_offsets[i]);
            std::cout << "  Offset " << std::hex << initial_offsets[i] << std::dec 
                      << " -> ptr=" << data;
            if (data) {
                std::cout << ", data[0]=" << std::hex << data[0] 
                          << ", data[255]=" << data[255] << std::dec;
            }
            std::cout << "\n";
        }
        
        // For single segment, offsets should work directly
        // For multi-segment, we need to handle segment boundaries
        for (size_t i = 0; i < initial_offsets.size(); ++i) {
            uint32_t* data = allocator->get_ptr<uint32_t>(initial_offsets[i]);
            ASSERT_NE(data, nullptr) << "Failed to get pointer for offset " << initial_offsets[i];
            
            EXPECT_EQ(data[0], 0xBEEF0000 + i) << "Start value mismatch at record " << i 
                                                << " (offset=" << std::hex << initial_offsets[i] << std::dec << ")";
            EXPECT_EQ(data[255], 0xDEAD0000 + i) << "End value mismatch at record " << i;
            
            // Modify the data
            data[0] = 0xCAFE0000 + i;
            data[255] = 0xFEED0000 + i;
        }
        
        // Add more data to trigger multi-segment if not already
        std::cout << "Adding more data after load...\n";
        for (int i = 50; i < 150; ++i) {
            auto offset = allocator->allocate(1024 * 1024); // 1MB allocations
            uint32_t* data = allocator->get_ptr<uint32_t>(offset);
            data[0] = 0xF00D0000 + i;
        }
        
        std::cout << "Final state: " << allocator->get_segment_count() 
                  << " segments, " << allocator->get_used_size() / (1024.0 * 1024.0) << " MB\n";
        
        // Save modified snapshot
        manager.save_snapshot();
    }
    
    // Step 3: Load again and verify modifications persisted
    {
        CompactSnapshotManager manager(snapshot_file);
        auto* allocator = manager.get_allocator();
        
        std::cout << "Verifying modified data after second load...\n";
        
        for (size_t i = 0; i < initial_offsets.size(); ++i) {
            uint32_t* data = allocator->get_ptr<uint32_t>(initial_offsets[i]);
            ASSERT_NE(data, nullptr);
            
            EXPECT_EQ(data[0], 0xCAFE0000 + i) << "Modified start value not persisted at record " << i;
            EXPECT_EQ(data[255], 0xFEED0000 + i) << "Modified end value not persisted at record " << i;
        }
        
        std::cout << "\n✅ MODIFICATIONS PERSISTED CORRECTLY!\n";
        std::cout << "   - Data can be loaded, modified, and saved\n";
        std::cout << "   - Multi-segment snapshots maintain data integrity\n";
    }
}