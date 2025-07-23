/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test root offset persistence in CompactSnapshotManager
 */

#include <gtest/gtest.h>
#include "../src/memmgr/compact_snapshot_manager.hpp"
#include <cstdio>

using namespace xtree;

TEST(CompactRootPersistence, SaveAndLoadRootOffset) {
    const std::string snapshot_file = "test_root_offset.snapshot";
    
    // Clean up any existing file
    std::remove(snapshot_file.c_str());
    
    uint32_t saved_root_offset = 0;
    
    // Phase 1: Create allocator and save with root offset
    {
        CompactSnapshotManager manager(snapshot_file);
        auto* allocator = manager.get_allocator();
        
        // Allocate some data to simulate XTreeBucket
        auto offset1 = allocator->allocate(1024);
        auto offset2 = allocator->allocate(2048);
        auto root_offset = allocator->allocate(4096); // This will be our "root"
        
        std::cout << "Allocated offsets: " << offset1 << ", " << offset2 << ", " << root_offset << "\n";
        
        // Set the root offset
        manager.set_root_offset(root_offset);
        saved_root_offset = root_offset;
        
        // Save snapshot
        manager.save_snapshot();
        
        std::cout << "Saved snapshot with root offset: " << root_offset << "\n";
    }
    
    // Phase 2: Load snapshot and verify root offset
    {
        CompactSnapshotManager manager(snapshot_file);
        
        // The snapshot should be auto-loaded in constructor
        EXPECT_TRUE(manager.is_snapshot_loaded());
        
        // Get the root offset
        uint32_t loaded_root_offset = manager.get_root_offset();
        
        std::cout << "Loaded root offset: " << loaded_root_offset << "\n";
        
        // Verify it matches
        EXPECT_EQ(loaded_root_offset, saved_root_offset);
        
        // Verify we can access the memory at that offset
        auto* allocator = manager.get_allocator();
        void* root_ptr = allocator->get_ptr(loaded_root_offset);
        EXPECT_NE(root_ptr, nullptr);
        
        // Write some data to verify it's accessible
        *static_cast<int*>(root_ptr) = 42;
        EXPECT_EQ(*static_cast<int*>(root_ptr), 42);
    }
    
    // Clean up
    std::remove(snapshot_file.c_str());
}