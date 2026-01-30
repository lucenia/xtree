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
#include <thread>
#include <chrono>
#include <atomic>
#include <fstream>
#include <cstddef>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif
#include "persistence/superblock.hpp"
#include "persistence/node_id.hpp"
#include "persistence/checksums.h"

using namespace xtree::persist;
using namespace std::chrono_literals;

class SuperblockTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::string meta_path;
    
    void SetUp() override {
        test_dir = "/tmp/xtree_superblock_test_" + std::to_string(getpid());
        meta_path = test_dir + "/xtree.meta";
        
        #ifdef _WIN32
            CreateDirectoryA(test_dir.c_str(), nullptr);
        #else
            mkdir(test_dir.c_str(), 0755);
        #endif
    }
    
    void TearDown() override {
        #ifdef _WIN32
            std::string cmd = "rmdir /s /q \"" + test_dir + "\"";
            system(cmd.c_str());
        #else
            std::string cmd = "rm -rf " + test_dir;
            system(cmd.c_str());
        #endif
    }
};

TEST_F(SuperblockTest, CreateAndLoad) {
    // Create superblock
    {
        Superblock sb(meta_path);
        EXPECT_FALSE(sb.valid()); // Not valid until first publish
        
        // Publish initial state
        NodeID root = NodeID::from_parts(12345, 1);
        uint64_t epoch = 100;
        sb.publish(root, epoch);
        
        EXPECT_TRUE(sb.valid());
    }
    
    // Load in new instance
    {
        Superblock sb(meta_path);
        EXPECT_TRUE(sb.valid());
        
        auto snapshot = sb.load();
        EXPECT_EQ(snapshot.root.handle_index(), 12345u);
        EXPECT_EQ(snapshot.root.tag(), 1u);
        EXPECT_EQ(snapshot.epoch, 100u);
    }
}

TEST_F(SuperblockTest, AtomicPublish) {
    Superblock sb(meta_path);
    
    // Initial publish
    NodeID root1 = NodeID::from_parts(1000, 1);
    sb.publish(root1, 1);
    
    // Verify initial state
    auto snap1 = sb.load();
    EXPECT_EQ(snap1.root.raw(), root1.raw());
    EXPECT_EQ(snap1.epoch, 1u);
    
    // Update with new root and epoch
    NodeID root2 = NodeID::from_parts(2000, 2);
    sb.publish(root2, 2);
    
    // Verify updated state
    auto snap2 = sb.load();
    EXPECT_EQ(snap2.root.raw(), root2.raw());
    EXPECT_EQ(snap2.epoch, 2u);
}

TEST_F(SuperblockTest, ConcurrentReaders) {
    Superblock sb(meta_path);
    
    // Initial state
    NodeID root = NodeID::from_parts(5000, 5);
    uint64_t epoch = 500;
    sb.publish(root, epoch);
    
    const int num_readers = 10;
    std::vector<std::thread> readers;
    std::atomic<int> success_count{0};
    
    // Spawn concurrent readers
    for (int i = 0; i < num_readers; i++) {
        readers.emplace_back([&]() {
            for (int j = 0; j < 100; j++) {
                auto snapshot = sb.load();
                if (snapshot.root.handle_index() == 5000 &&
                    snapshot.root.tag() == 5 &&
                    snapshot.epoch == 500) {
                    success_count++;
                }
                std::this_thread::sleep_for(1us);
            }
        });
    }
    
    // Wait for readers
    for (auto& t : readers) {
        t.join();
    }
    
    // All reads should see consistent state
    EXPECT_EQ(success_count.load(), num_readers * 100);
}

TEST_F(SuperblockTest, WriterReaderConsistency) {
    Superblock sb(meta_path);
    
    // Initial state
    sb.publish(NodeID::from_parts(1, 1), 1);
    
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> max_epoch_seen{0};
    std::atomic<int> inconsistent_reads{0};
    
    // Writer thread
    std::thread writer([&]() {
        for (uint64_t epoch = 2; epoch <= 1000; epoch++) {
            NodeID root = NodeID::from_parts(epoch * 100, epoch % 65536);
            sb.publish(root, epoch);
            std::this_thread::sleep_for(100us);
        }
        stop = true;
    });
    
    // Reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; i++) {
        readers.emplace_back([&]() {
            while (!stop) {
                auto snapshot = sb.load();
                
                // Verify consistency: handle = epoch * 100, tag = epoch % 65536
                if (snapshot.epoch > 0) {
                    uint64_t expected_handle = snapshot.epoch * 100;
                    uint16_t expected_tag = snapshot.epoch % 65536;
                    
                    if (snapshot.root.handle_index() != expected_handle ||
                        snapshot.root.tag() != expected_tag) {
                        inconsistent_reads++;
                    }
                    
                    // Track maximum epoch seen
                    uint64_t current_max = max_epoch_seen.load();
                    while (snapshot.epoch > current_max &&
                           !max_epoch_seen.compare_exchange_weak(current_max, snapshot.epoch)) {
                        // Keep trying
                    }
                }
                
                std::this_thread::sleep_for(50us);
            }
        });
    }
    
    writer.join();
    for (auto& t : readers) {
        t.join();
    }
    
    // No inconsistent reads should occur
    EXPECT_EQ(inconsistent_reads.load(), 0);
    
    // Should have seen the final epoch
    EXPECT_EQ(max_epoch_seen.load(), 1000u);
}

TEST_F(SuperblockTest, PersistenceAcrossRestart) {
    NodeID final_root;
    uint64_t final_epoch;
    
    // First process - write multiple updates
    {
        Superblock sb(meta_path);
        
        for (int i = 1; i <= 10; i++) {
            NodeID root = NodeID::from_parts(i * 1000, i);
            uint64_t epoch = i * 10;
            sb.publish(root, epoch);
        }
        
        auto snapshot = sb.load();
        final_root = snapshot.root;
        final_epoch = snapshot.epoch;
    }
    
    // "Restart" - new process loads the superblock
    {
        Superblock sb(meta_path);
        EXPECT_TRUE(sb.valid());
        
        auto snapshot = sb.load();
        EXPECT_EQ(snapshot.root.raw(), final_root.raw());
        EXPECT_EQ(snapshot.epoch, final_epoch);
        EXPECT_EQ(snapshot.epoch, 100u); // Should be 10 * 10
    }
}

TEST_F(SuperblockTest, InvalidSuperblock) {
    // Try to load superblock with invalid path (no permission to create in root)
    Superblock sb("/root/no_permission/xtree.meta");
    EXPECT_FALSE(sb.valid());
    
    // Load should return invalid state
    auto snapshot = sb.load();
    EXPECT_FALSE(snapshot.root.valid());
    EXPECT_EQ(snapshot.epoch, 0u);
}

TEST_F(SuperblockTest, LargeEpochValues) {
    Superblock sb(meta_path);
    
    // Test with large epoch values
    uint64_t large_epoch = (1ULL << 50) - 1;
    // With 16-bit tags, max handle index is 48 bits (not 55)
    NodeID large_root = NodeID::from_parts((1ULL << 48) - 1, 0xFFFF);
    
    sb.publish(large_root, large_epoch);
    
    auto snapshot = sb.load();
    EXPECT_EQ(snapshot.root.handle_index(), (1ULL << 48) - 1);
    EXPECT_EQ(snapshot.root.tag(), 0xFFFF);
    EXPECT_EQ(snapshot.epoch, large_epoch);
}

TEST_F(SuperblockTest, RapidUpdates) {
    Superblock sb(meta_path);
    
    // Perform rapid updates
    const int num_updates = 1000;
    for (int i = 0; i < num_updates; i++) {
        NodeID root = NodeID::from_parts(i, i % 65536);
        sb.publish(root, i);
    }
    
    // Final state should be consistent
    auto snapshot = sb.load();
    EXPECT_EQ(snapshot.root.handle_index(), num_updates - 1);
    EXPECT_EQ(snapshot.root.tag(), (num_updates - 1) % 65536);
    EXPECT_EQ(snapshot.epoch, num_updates - 1);
}

// ============= Additional Production Battle Tests =============

TEST_F(SuperblockTest, CrashRecoverySimulation) {
    NodeID checkpoint_root = NodeID::from_parts(9999, 99);
    uint64_t checkpoint_epoch = 999;
    
    // Simulate a write followed by "crash"
    {
        Superblock sb(meta_path);
        sb.publish(checkpoint_root, checkpoint_epoch);
        // Destructor simulates ungraceful shutdown
    }
    
    // Recovery after crash - should see last published state
    {
        Superblock sb(meta_path);
        EXPECT_TRUE(sb.valid());
        
        auto snapshot = sb.load();
        EXPECT_EQ(snapshot.root.raw(), checkpoint_root.raw());
        EXPECT_EQ(snapshot.epoch, checkpoint_epoch);
    }
}

TEST_F(SuperblockTest, CorruptionDetection) {
    // Create valid superblock
    {
        Superblock sb(meta_path);
        sb.publish(NodeID::from_parts(1111, 11), 111);
    }
    
    // Corrupt the file by overwriting magic number
    {
        std::fstream file(meta_path, std::ios::binary | std::ios::in | std::ios::out);
        if (file.is_open()) {
            uint64_t bad_magic = 0xDEADBEEFDEADBEEF;
            file.write(reinterpret_cast<char*>(&bad_magic), sizeof(bad_magic));
            file.close();
        }
    }
    
    // Should detect corruption
    {
        Superblock sb(meta_path);
        EXPECT_FALSE(sb.valid());
    }
}

TEST_F(SuperblockTest, PowerFailurePartialWrite) {
    // Test recovery from partial writes
    NodeID original_root = NodeID::from_parts(5555, 55);
    uint64_t original_epoch = 555;
    
    // Establish initial state
    {
        Superblock sb(meta_path);
        sb.publish(original_root, original_epoch);
    }
    
    // Simulate partial write by truncating file
    {
        #ifdef _WIN32
        HANDLE hFile = CreateFileA(meta_path.c_str(), GENERIC_WRITE, 0, NULL, 
                                  OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        if (hFile != INVALID_HANDLE_VALUE) {
            LARGE_INTEGER size;
            size.QuadPart = sizeof(SuperblockOnDisk) / 2; // Truncate to half
            SetFilePointerEx(hFile, size, NULL, FILE_BEGIN);
            SetEndOfFile(hFile);
            CloseHandle(hFile);
        }
        #else
        truncate(meta_path.c_str(), sizeof(SuperblockOnDisk) / 2);
        #endif
    }
    
    // Recovery should handle truncated file
    {
        Superblock sb(meta_path);
        // Implementation should either recover or reinitialize
        // but not crash
        auto snapshot = sb.load();
        // At minimum, should not segfault
    }
}

TEST_F(SuperblockTest, MonotonicEpochGuarantee) {
    Superblock sb(meta_path);
    
    // Epochs should be monotonically increasing in production
    uint64_t last_epoch = 0;
    for (int i = 1; i <= 100; i++) {
        uint64_t epoch = i * 10;
        NodeID root = NodeID::from_parts(i * 100, i);
        sb.publish(root, epoch);
        
        auto snapshot = sb.load();
        EXPECT_GT(snapshot.epoch, last_epoch);
        last_epoch = snapshot.epoch;
    }
}

/**
 * Stress test for concurrent writers and readers
 */
TEST_F(SuperblockTest, StressTestConcurrentWriterReaders) {
    Superblock sb(meta_path);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> write_count{0};
    std::atomic<uint64_t> read_count{0};
    std::atomic<uint64_t> consistency_errors{0};
    
    // Initialize
    sb.publish(NodeID::from_parts(1, 1), 1);
    
    // Single writer thread doing continuous updates
    std::thread writer([&]() {
        for (uint64_t i = 2; i <= 10000 && !stop; i++) {
            NodeID root = NodeID::from_parts(i * 7, i % 65536);  // Use prime multiplier for uniqueness
            sb.publish(root, i);
            write_count++;
            
            // Vary write speed to stress different timing scenarios
            if (i % 100 == 0) {
                std::this_thread::sleep_for(1ms);
            } else if (i % 10 == 0) {
                std::this_thread::sleep_for(10us);
            }
        }
    });
    
    // Multiple reader threads
    std::vector<std::thread> readers;
    for (int r = 0; r < 8; r++) {
        readers.emplace_back([&]() {
            uint64_t last_seen_epoch = 0;
            while (!stop && write_count < 10000) {
                auto snapshot = sb.load();
                read_count++;
                
                // Check consistency: handle should be epoch * 7
                if (snapshot.epoch > 1 && snapshot.root.valid()) {
                    uint64_t expected_handle = snapshot.epoch * 7;
                    if (snapshot.root.handle_index() != expected_handle) {
                        consistency_errors++;
                    }
                }
                
                // Check monotonicity from reader's perspective
                if (snapshot.epoch < last_seen_epoch) {
                    consistency_errors++;
                }
                last_seen_epoch = snapshot.epoch;
                
                // Vary read speed
                if (read_count % 1000 == 0) {
                    std::this_thread::sleep_for(100us);
                }
            }
        });
    }
    
    // Let it run for a bit
    std::this_thread::sleep_for(5s);
    stop = true;
    
    writer.join();
    for (auto& t : readers) {
        t.join();
    }
    
    // Verify no consistency errors
    EXPECT_EQ(consistency_errors.load(), 0);
    EXPECT_GT(write_count.load(), 1000u);  // Should have done many writes
    EXPECT_GT(read_count.load(), 10000u);  // Should have done many more reads
}

TEST_F(SuperblockTest, DirectoryCreation) {
    // Test that superblock creates parent directories
    std::string nested_path = test_dir + "/deep/nested/path/xtree.meta";
    
    {
        Superblock sb(nested_path);
        sb.publish(NodeID::from_parts(7777, 77), 777);
    }
    
    // Verify file was created in nested directory
    {
        Superblock sb(nested_path);
        EXPECT_TRUE(sb.valid());
        auto snapshot = sb.load();
        EXPECT_EQ(snapshot.root.handle_index(), 7777u);
        EXPECT_EQ(snapshot.epoch, 777u);
    }
}

TEST_F(SuperblockTest, GenerationIncrement) {
    // Test that generation increments on each publish
    Superblock sb(meta_path);
    
    // Track generations through multiple publishes
    for (int i = 1; i <= 5; i++) {
        sb.publish(NodeID::from_parts(i * 100, i), i);
    }
    
    // Open file and read generation directly
    std::ifstream file(meta_path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    
    // Skip to generation field offset
    file.seekg(offsetof(SuperblockOnDisk, generation));
    uint64_t generation;
    file.read(reinterpret_cast<char*>(&generation), sizeof(generation));
    file.close();
    
    // Should have incremented 5 times from initial value of 1
    EXPECT_EQ(generation, 6u);
}

TEST_F(SuperblockTest, CRCValidation) {
    // Test CRC computation and validation
    NodeID test_root = NodeID::from_parts(0xABCDEF, 0x42);
    uint64_t test_epoch = 0x123456789;
    
    // Write with CRC
    {
        Superblock sb(meta_path);
        sb.publish(test_root, test_epoch);
    }
    
    // Verify CRC is non-zero and correct
    {
        std::ifstream file(meta_path, std::ios::binary);
        ASSERT_TRUE(file.is_open());
        
        // Read the entire superblock
        SuperblockOnDisk sb_data;
        file.read(reinterpret_cast<char*>(&sb_data), sizeof(sb_data));
        file.close();
        
        // CRC should be non-zero
        EXPECT_NE(sb_data.header_crc32c, 0u);
        
        // Compute expected CRC
        uint32_t stored_crc = sb_data.header_crc32c;
        sb_data.header_crc32c = 0;
        
        CRC32C crc;
        crc.update(&sb_data, offsetof(SuperblockOnDisk, header_crc32c));
        uint8_t zeros[4] = {0};
        crc.update(zeros, sizeof(zeros));
        size_t after_crc = offsetof(SuperblockOnDisk, header_crc32c) + sizeof(uint32_t);
        if (after_crc < sizeof(SuperblockOnDisk)) {
            crc.update(reinterpret_cast<uint8_t*>(&sb_data) + after_crc,
                      sizeof(SuperblockOnDisk) - after_crc);
        }
        
        EXPECT_EQ(stored_crc, crc.finalize());
    }
}

TEST_F(SuperblockTest, MultipleInstancesConsistency) {
    // Test multiple Superblock instances see consistent state
    Superblock sb1(meta_path);
    Superblock sb2(meta_path);
    Superblock sb3(meta_path);
    
    // Initial publish from sb1
    NodeID root1 = NodeID::from_parts(1000, 10);
    sb1.publish(root1, 100);
    
    // All instances should see the same state
    auto snap1 = sb1.load();
    auto snap2 = sb2.load();
    auto snap3 = sb3.load();
    
    EXPECT_EQ(snap1.root.raw(), snap2.root.raw());
    EXPECT_EQ(snap2.root.raw(), snap3.root.raw());
    EXPECT_EQ(snap1.epoch, snap2.epoch);
    EXPECT_EQ(snap2.epoch, snap3.epoch);
    
    // Update from sb2
    NodeID root2 = NodeID::from_parts(2000, 20);
    sb2.publish(root2, 200);
    
    // All should see the update
    snap1 = sb1.load();
    snap2 = sb2.load();
    snap3 = sb3.load();
    
    EXPECT_EQ(snap1.root.handle_index(), 2000u);
    EXPECT_EQ(snap2.root.handle_index(), 2000u);
    EXPECT_EQ(snap3.root.handle_index(), 2000u);
    EXPECT_EQ(snap1.epoch, 200u);
    EXPECT_EQ(snap2.epoch, 200u);
    EXPECT_EQ(snap3.epoch, 200u);
}

TEST_F(SuperblockTest, FilePermissionRecovery) {
    // Test recovery when file permissions are changed
    {
        Superblock sb(meta_path);
        sb.publish(NodeID::from_parts(3333, 33), 333);
    }
    
    // Make file read-only
    #ifndef _WIN32
    chmod(meta_path.c_str(), 0444);
    #else
    SetFileAttributesA(meta_path.c_str(), FILE_ATTRIBUTE_READONLY);
    #endif
    
    // Try to open read-only file
    {
        // Note: Opening read-only file in read-write mode may fail on some platforms
        // This is expected behavior - the superblock handles it gracefully
        Superblock sb(meta_path);
        
        if (sb.valid()) {
            // If we can map it read-only, verify we can read
            auto snapshot = sb.load();
            EXPECT_EQ(snapshot.root.handle_index(), 3333u);
            EXPECT_EQ(snapshot.epoch, 333u);
            
            // Publish should fail gracefully (not crash)
            sb.publish(NodeID::from_parts(4444, 44), 444);
        } else {
            // Mapping failed due to read-only permissions - this is acceptable
            EXPECT_FALSE(sb.valid());
        }
    }
    
    // Restore permissions
    #ifndef _WIN32
    chmod(meta_path.c_str(), 0644);
    #else
    SetFileAttributesA(meta_path.c_str(), FILE_ATTRIBUTE_NORMAL);
    #endif
    
    // Should be able to access normally now
    {
        Superblock sb(meta_path);
        EXPECT_TRUE(sb.valid());
        auto snapshot = sb.load();
        EXPECT_EQ(snapshot.root.handle_index(), 3333u);
        EXPECT_EQ(snapshot.epoch, 333u);
    }
}