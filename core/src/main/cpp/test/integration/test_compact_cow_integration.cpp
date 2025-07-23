/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Integration tests for Compact Allocator with COW-like functionality
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <cstdio>
#include "../src/memmgr/compact_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"

using namespace xtree;
using namespace std::chrono;

class CompactCOWIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up test files
        std::remove("test_compact_cow.snapshot");
        std::remove("test_compact_persist.snapshot");
    }
    
    void TearDown() override {
        std::remove("test_compact_cow.snapshot");
        std::remove("test_compact_persist.snapshot");
    }
};

// Test basic allocation with snapshot manager
TEST_F(CompactCOWIntegrationTest, BasicAllocationWithSnapshot) {
    CompactSnapshotManager manager("test_compact_cow.snapshot", 1024 * 1024);
    auto* allocator = manager.get_allocator();
    
    // Allocate some memory
    auto offset1 = allocator->allocate(1024);
    ASSERT_NE(offset1, CompactAllocator::INVALID_OFFSET);
    
    auto offset2 = allocator->allocate(2048);
    ASSERT_NE(offset2, CompactAllocator::INVALID_OFFSET);
    
    // Get pointers and write data
    int* ptr1 = allocator->get_ptr<int>(offset1);
    int* ptr2 = allocator->get_ptr<int>(offset2);
    
    *ptr1 = 42;
    *ptr2 = 84;
    
    // Save snapshot
    manager.save_snapshot();
}

// Test snapshot persistence and reload directly with CompactSnapshotManager
TEST_F(CompactCOWIntegrationTest, DISABLED_SnapshotPersistenceAndReload) {
    const char* snapshot_file = "test_compact_persist.snapshot";
    
    // Phase 1: Create data and save
    {
        CompactSnapshotManager manager(snapshot_file, 10 * 1024 * 1024);
        auto* allocator = manager.get_allocator();
        
        // Create test structure
        struct TestNode {
            int id;
            double value;
            char data[64];
        };
        
        std::vector<CompactAllocator::offset_t> offsets;
        
        // Allocate nodes
        for (int i = 0; i < 1000; i++) {
            auto offset = allocator->allocate(sizeof(TestNode));
            ASSERT_NE(offset, CompactAllocator::INVALID_OFFSET);
            
            TestNode* node = allocator->get_ptr<TestNode>(offset);
            node->id = i;
            node->value = i * 3.14159;
            snprintf(node->data, sizeof(node->data), "Node_%d", i);
            
            offsets.push_back(offset);
        }
        
        // Save snapshot
        manager.save_snapshot();
    }
    
    // Phase 2: Reload and verify
    {
        auto reload_start = high_resolution_clock::now();
        
        CompactSnapshotManager manager(snapshot_file);
        
        auto reload_time = high_resolution_clock::now() - reload_start;
        auto reload_ms = duration_cast<milliseconds>(reload_time).count();
        
        std::cout << "Snapshot reload time: " << reload_ms << "ms" << std::endl;
        
        // Should be very fast (< 10ms)
        EXPECT_LT(reload_ms, 10);
        
        // Verify snapshot was loaded
        EXPECT_TRUE(manager.is_snapshot_loaded());
        EXPECT_GT(manager.get_snapshot_size(), 0);
        
        // Verify we can read the data
        auto* allocator = manager.get_allocator();
        
        // Test structure must match what was saved
        struct TestNode {
            int id;
            double value;
            char data[64];
        };
        
        // Verify first few nodes
        size_t offset = sizeof(CompactAllocator::offset_t);
        for (int i = 0; i < 10; i++) {
            TestNode* node = allocator->get_ptr<TestNode>(offset);
            EXPECT_EQ(node->id, i);
            EXPECT_DOUBLE_EQ(node->value, i * 3.14159);
            
            char expected[64];
            snprintf(expected, sizeof(expected), "Node_%d", i);
            EXPECT_STREQ(node->data, expected);
            
            offset += (sizeof(TestNode) + 7) & ~7; // aligned size
        }
    }
}

// Test performance of compact allocator
TEST_F(CompactCOWIntegrationTest, CompactAllocatorPerformance) {
    const int NUM_ALLOCATIONS = 10000;
    const size_t ALLOC_SIZE = 128;
    
    // Test compact allocator
    {
        CompactSnapshotManager manager("test_compact_cow.snapshot", 50 * 1024 * 1024);
        auto* allocator = manager.get_allocator();
        
        auto start = high_resolution_clock::now();
        
        for (int i = 0; i < NUM_ALLOCATIONS; i++) {
            auto offset = allocator->allocate(ALLOC_SIZE);
            ASSERT_NE(offset, CompactAllocator::INVALID_OFFSET);
            
            int* ptr = allocator->get_ptr<int>(offset);
            *ptr = i;
        }
        
        auto alloc_time = high_resolution_clock::now() - start;
        auto alloc_us = duration_cast<microseconds>(alloc_time).count();
        
        double alloc_per_sec = (NUM_ALLOCATIONS * 1000000.0) / alloc_us;
        
        std::cout << "Compact allocator: " << NUM_ALLOCATIONS 
                  << " allocations in " << alloc_us << "us ("
                  << alloc_per_sec << " allocs/sec)" << std::endl;
        
        // Trigger and measure snapshot time
        start = high_resolution_clock::now();
        manager.save_snapshot();
        auto snapshot_time = high_resolution_clock::now() - start;
        
        std::cout << "Compact snapshot time: " 
                  << duration_cast<milliseconds>(snapshot_time).count() 
                  << "ms" << std::endl;
    }
}

// Test offset-based allocation pattern
TEST_F(CompactCOWIntegrationTest, OffsetBasedAllocation) {
    CompactSnapshotManager manager("test_compact_cow.snapshot", 10 * 1024 * 1024);
    auto* allocator = manager.get_allocator();
    
    // Create a linked list using offsets
    struct Node {
        int value;
        CompactAllocator::offset_t next;
    };
    
    CompactAllocator::offset_t head = CompactAllocator::INVALID_OFFSET;
    CompactAllocator::offset_t tail = CompactAllocator::INVALID_OFFSET;
    
    // Build linked list
    for (int i = 0; i < 100; i++) {
        auto offset = allocator->allocate(sizeof(Node));
        Node* node = allocator->get_ptr<Node>(offset);
        
        node->value = i;
        node->next = CompactAllocator::INVALID_OFFSET;
        
        if (head == CompactAllocator::INVALID_OFFSET) {
            head = offset;
            tail = offset;
        } else {
            Node* tail_node = allocator->get_ptr<Node>(tail);
            tail_node->next = offset;
            tail = offset;
        }
    }
    
    // Traverse and verify
    int count = 0;
    CompactAllocator::offset_t current = head;
    while (current != CompactAllocator::INVALID_OFFSET) {
        Node* node = allocator->get_ptr<Node>(current);
        EXPECT_EQ(node->value, count);
        current = node->next;
        count++;
    }
    
    EXPECT_EQ(count, 100);
}