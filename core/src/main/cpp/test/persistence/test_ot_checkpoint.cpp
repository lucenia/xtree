/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Tests for OT checkpoint binary format
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <atomic>
#include <chrono>
#include <set>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
#include "persistence/ot_checkpoint.h"
#include "persistence/object_table_sharded.hpp"
#include "persistence/segment_allocator.h"
#include "persistence/checksums.h"

using namespace xtree::persist;
namespace fs = std::filesystem;

class OTCheckpointTest : public ::testing::Test {
protected:
    std::string test_dir_;
    std::unique_ptr<SegmentAllocator> allocator_;
    std::unique_ptr<ObjectTableSharded> ot_;
    std::unique_ptr<OTCheckpoint> checkpoint_;
    
    void SetUp() override {
        // Create temp test directory (cross-platform)
        // Use timestamp and random number for concurrent test isolation
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(10000, 99999);
        
        auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto base = fs::temp_directory_path() / 
                   ("ot_ckpt_" + std::to_string(timestamp) + "_" + std::to_string(dis(gen)));
        test_dir_ = base.string();
        fs::create_directories(fs::path(test_dir_));
        
        // Initialize components
        allocator_ = std::make_unique<SegmentAllocator>(test_dir_);
        ot_ = std::make_unique<ObjectTableSharded>();
        // Note: ObjectTable doesn't need allocator set for basic testing
        // as it manages its own slabs for the handle table
        checkpoint_ = std::make_unique<OTCheckpoint>(test_dir_);
    }
    
    void TearDown() override {
        checkpoint_.reset();
        ot_.reset();
        allocator_.reset();
        
        // Clean up test directory
        try {
            fs::remove_all(test_dir_);
        } catch (...) {
            // Best effort cleanup
        }
    }
    
    // Helper to allocate test nodes
    NodeID allocate_test_node(uint64_t epoch, size_t size = 1024) {
        // For testing, we can use dummy addresses since we're not actually
        // storing data, just testing the checkpoint mechanism
        static uint32_t file_counter = 1;
        static uint64_t offset_counter = 0;
        
        OTAddr addr{file_counter++, 0, offset_counter, static_cast<uint32_t>(size)};
        offset_counter += size;
        
        uint8_t class_id = 0; // Dummy class
        return ot_->allocate(NodeKind::Leaf, class_id, addr, epoch);
    }
    
    // Helper to corrupt a file at specific offset
    void corrupt_file_at(const std::string& path, size_t offset, uint8_t xor_mask = 0xFF) {
        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file.good()) << "Failed to open file for corruption";
        
        file.seekg(offset);
        if (!file.good()) {
            // Handle EOF or seek error gracefully
            file.close();
            return;
        }
        
        char byte;
        file.read(&byte, 1);
        
        // Verify we actually read a byte (future format change protection)
        ASSERT_EQ(file.gcount(), 1) << "Failed to read byte at offset " << offset 
                                     << " - corruption would be no-op";
        
        if (!file.good()) {
            // Handle read error
            file.close();
            return;
        }
        
        byte ^= xor_mask;
        file.seekp(offset);
        file.write(&byte, 1);
        file.close();
    }
    
    // Helper to truncate file
    void truncate_file(const std::string& path, size_t new_size) {
        fs::resize_file(path, new_size);
    }
};

TEST_F(OTCheckpointTest, EmptyCheckpoint) {
    // Write empty checkpoint
    uint64_t epoch = 100;
    ASSERT_TRUE(checkpoint_->write(ot_.get(), epoch));
    
    // Find the checkpoint file
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    ASSERT_FALSE(ckpt_path.empty());
    EXPECT_TRUE(ckpt_path.find("epoch-100") != std::string::npos);
    
    // Map and verify
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
    EXPECT_EQ(read_epoch, epoch);
    EXPECT_EQ(entry_count, 0u);
}

TEST_F(OTCheckpointTest, BasicWriteAndRead) {
    // Allocate some nodes
    std::vector<NodeID> nodes;
    for (int i = 0; i < 10; i++) {
        nodes.push_back(allocate_test_node(i * 10, 1024 + i * 512));
    }
    
    // Write checkpoint
    uint64_t epoch = 500;
    ASSERT_TRUE(checkpoint_->write(ot_.get(), epoch));
    
    // Find and read checkpoint
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    ASSERT_FALSE(ckpt_path.empty());
    
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
    EXPECT_EQ(read_epoch, epoch);
    EXPECT_EQ(entry_count, nodes.size());
    
    // Verify entries match
    for (size_t i = 0; i < entry_count; i++) {
        const auto& pe = entries[i];
        EXPECT_LT(pe.handle_idx, 1000u) << "Handle index seems too large";
        EXPECT_EQ(pe.retire_epoch, ~uint64_t{0}) << "Entry should be live";
        EXPECT_LE(pe.birth_epoch, epoch) << "Birth epoch should be <= checkpoint epoch";
    }
}

TEST_F(OTCheckpointTest, LargeCheckpoint) {
    // Allocate many nodes to test streaming
    const size_t num_nodes = 10000;
    std::vector<NodeID> nodes;
    nodes.reserve(num_nodes);
    
    for (size_t i = 0; i < num_nodes; i++) {
        nodes.push_back(allocate_test_node(i, 1024));
    }
    
    // Write checkpoint
    uint64_t epoch = 50000;
    ASSERT_TRUE(checkpoint_->write(ot_.get(), epoch));
    
    // Read back
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
    EXPECT_EQ(entry_count, num_nodes);
    
    // Verify file size is as expected
    size_t expected_size = sizeof(OTCheckpoint::Header) + 
                          (num_nodes * sizeof(OTCheckpoint::PersistentEntry)) +
                          sizeof(OTCheckpoint::Footer);
    EXPECT_EQ(fs::file_size(ckpt_path), expected_size);
}

TEST_F(OTCheckpointTest, RetiredNodesExcluded) {
    // Allocate nodes
    std::vector<NodeID> live_nodes;
    std::vector<NodeID> retired_nodes;
    
    for (int i = 0; i < 20; i++) {
        NodeID id = allocate_test_node(i * 10);
        if (i % 3 == 0) {
            // Retire some nodes
            ot_->retire(id, i * 10 + 5);
            retired_nodes.push_back(id);
        } else {
            live_nodes.push_back(id);
        }
    }
    
    // Write checkpoint
    uint64_t epoch = 1000;
    ASSERT_TRUE(checkpoint_->write(ot_.get(), epoch));
    
    // Read back and verify only live nodes included
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
    EXPECT_EQ(entry_count, live_nodes.size());
    
    // All entries should be live
    for (size_t i = 0; i < entry_count; i++) {
        EXPECT_EQ(entries[i].retire_epoch, ~uint64_t{0});
    }
}

TEST_F(OTCheckpointTest, CorruptHeaderCRC) {
    // Write valid checkpoint
    uint64_t epoch = 100;
    allocate_test_node(50);
    ASSERT_TRUE(checkpoint_->write(ot_.get(), epoch));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    
    // Corrupt header CRC field (last 4 bytes of 4KB header)
    corrupt_file_at(ckpt_path, 4096 - 4);
    
    // Should fail to map
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, CorruptMagic) {
    // Write valid checkpoint
    allocate_test_node(50);
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    
    // Corrupt magic bytes
    corrupt_file_at(ckpt_path, 0);
    
    // Should fail to map
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, CorruptFooterCRC) {
    // Write checkpoint with some entries
    for (int i = 0; i < 5; i++) {
        allocate_test_node(i * 10);
    }
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    size_t file_size = fs::file_size(ckpt_path);
    
    // Corrupt footer CRC (last 4 bytes of file)
    corrupt_file_at(ckpt_path, file_size - 4);
    
    // Should fail to map
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, CorruptEntriesCRC) {
    // Write checkpoint with entries
    for (int i = 0; i < 5; i++) {
        allocate_test_node(i * 10);
    }
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    
    // Corrupt an entry (somewhere in the middle of entries)
    size_t entry_offset = sizeof(OTCheckpoint::Header) + sizeof(OTCheckpoint::PersistentEntry);
    corrupt_file_at(ckpt_path, entry_offset);
    
    // Should fail due to entries CRC mismatch
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, PartialFooterWrite) {
    // Test detection of partially written footer (missing footer_crc32c)
    for (int i = 0; i < 5; i++) {
        allocate_test_node(i * 10);
    }
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    size_t original_size = fs::file_size(ckpt_path);
    
    // Truncate just the footer_crc32c field (last 4 bytes)
    truncate_file(ckpt_path, original_size - sizeof(uint32_t));
    
    // Should fail due to incomplete footer
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, TruncatedFile) {
    // Write checkpoint
    for (int i = 0; i < 5; i++) {
        allocate_test_node(i * 10);
    }
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    size_t original_size = fs::file_size(ckpt_path);
    
    // Truncate by 2 bytes
    truncate_file(ckpt_path, original_size - 2);
    
    // Should fail due to size mismatch
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, SizeNotCongruent) {
    // Test that file size must be congruent with row_size
    // (filesize - header - footer) must be divisible by row_size
    
    // Write checkpoint with multiple entries
    for (int i = 0; i < 5; i++) {
        allocate_test_node(i * 10);
    }
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    size_t original_size = fs::file_size(ckpt_path);
    
    // Add a partial row worth of bytes (not divisible by sizeof(PersistentEntry))
    // This simulates a partial row tail
    std::fstream file(ckpt_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);
    ASSERT_TRUE(file.good());
    
    // Add 7 bytes (not divisible by 48-byte row size)
    char garbage[7] = {0};
    file.write(garbage, sizeof(garbage));
    file.close();
    
    // Should fail due to size not being congruent with row_size
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries))
        << "Should reject file with partial row tail";
}

TEST_F(OTCheckpointTest, WrongRowSize) {
    // Test that wrong row_size is rejected even with valid CRC
    allocate_test_node(50);
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    
    // Read header, modify row_size, recompute CRC
    std::fstream file(ckpt_path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file.good());
    
    OTCheckpoint::Header header{};
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    ASSERT_TRUE(file.good());
    
    // Change row_size to invalid value
    header.row_size = 999;  // Not 48 bytes
    header.header_crc32c = 0;
    
    // Recompute CRC with wrong row_size
    CRC32C crc;
    crc.update(&header, offsetof(OTCheckpoint::Header, header_crc32c));
    uint8_t zeros[4] = {0};
    crc.update(zeros, 4);
    header.header_crc32c = crc.finalize();
    
    // Write back header with valid CRC but wrong row_size
    file.seekp(0);
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.close();
    
    // Should fail due to row_size mismatch
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, WrongVersion) {
    // Write valid checkpoint
    allocate_test_node(50);
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    
    // Read header, modify version, recompute CRC
    std::fstream file(ckpt_path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file.good());
    
    OTCheckpoint::Header header{};
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    ASSERT_TRUE(file.good());
    
    // Change version to invalid value
    header.version = 999;
    header.header_crc32c = 0;
    
    // Recompute CRC with wrong version
    CRC32C crc;
    crc.update(&header, offsetof(OTCheckpoint::Header, header_crc32c));
    uint8_t zeros[4] = {0};
    crc.update(zeros, 4);
    header.header_crc32c = crc.finalize();
    
    // Write back entire header with correct CRC but wrong version
    file.seekp(0);
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.close();
    
    // Should fail due to version mismatch
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    EXPECT_FALSE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
}

TEST_F(OTCheckpointTest, IgnoreTempFiles) {
    // Test that .tmp files are ignored in discovery and cleanup
    
    // Create valid checkpoint
    allocate_test_node(100);
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 100));
    
    // Create a stray .tmp file with higher epoch
    std::string temp_file = test_dir_ + "/ot_checkpoint_epoch-999.bin.tmp";
    std::ofstream tmp(temp_file, std::ios::binary);
    tmp << "garbage";
    tmp.close();
    
    // find_latest should ignore the .tmp and return epoch 100
    std::string latest = OTCheckpoint::find_latest_checkpoint(test_dir_);
    ASSERT_FALSE(latest.empty());
    EXPECT_TRUE(latest.find("epoch-100") != std::string::npos);
    EXPECT_TRUE(latest.find(".tmp") == std::string::npos);
    
    // Cleanup should also ignore .tmp files
    OTCheckpoint::cleanup_old_checkpoints(test_dir_, 1);
    
    // The .tmp should still exist (not cleaned up)
    EXPECT_TRUE(fs::exists(temp_file));
    
    // Clean up manually
    fs::remove(temp_file);
}

TEST_F(OTCheckpointTest, MultipleCheckpoints) {
    // Write multiple checkpoints at different epochs
    std::vector<uint64_t> epochs = {100, 200, 150, 300, 250};
    
    for (uint64_t epoch : epochs) {
        allocate_test_node(epoch);
        ASSERT_TRUE(checkpoint_->write(ot_.get(), epoch));
    }
    
    // Find latest should return epoch 300
    std::string latest = OTCheckpoint::find_latest_checkpoint(test_dir_);
    ASSERT_FALSE(latest.empty());
    EXPECT_TRUE(latest.find("epoch-300") != std::string::npos);
    
    // Verify we can read it
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(latest, &read_epoch, &entry_count, &entries));
    EXPECT_EQ(read_epoch, 300u);
    
    // Also verify we can still read earlier checkpoints (not clobbered)
    std::string earlier_path;
    for (const auto& entry : fs::directory_iterator(test_dir_)) {
        std::string filename = entry.path().filename().string();
        if (filename.find("epoch-250.bin") != std::string::npos) {
            earlier_path = entry.path().string();
            break;
        }
    }
    
    ASSERT_FALSE(earlier_path.empty()) << "Earlier checkpoint should still exist";
    uint64_t earlier_epoch = 0;
    ASSERT_TRUE(checkpoint_->map_for_read(earlier_path, &earlier_epoch, &entry_count, &entries));
    EXPECT_EQ(earlier_epoch, 250u) << "Earlier checkpoint should be readable";
}

TEST_F(OTCheckpointTest, CleanupOldCheckpoints) {
    // Create 10 checkpoints
    for (int i = 1; i <= 10; i++) {
        allocate_test_node(i * 10);
        ASSERT_TRUE(checkpoint_->write(ot_.get(), i * 100));
    }
    
    // Count checkpoint files
    auto count_checkpoints = [this]() {
        size_t count = 0;
        for (const auto& entry : fs::directory_iterator(test_dir_)) {
            if (entry.path().filename().string().find("ot_checkpoint_epoch-") == 0) {
                count++;
            }
        }
        return count;
    };
    
    EXPECT_EQ(count_checkpoints(), 10u);
    
    // Keep only 3 most recent
    OTCheckpoint::cleanup_old_checkpoints(test_dir_, 3);
    
    EXPECT_EQ(count_checkpoints(), 3u);
    
    // Verify the remaining are the most recent (epochs 800, 900, 1000)
    // Parse epochs properly to avoid brittle string matching
    std::set<uint64_t> remaining_epochs;
    for (const auto& entry : fs::directory_iterator(test_dir_)) {
        std::string filename = entry.path().filename().string();
        if (filename.find("ot_checkpoint_epoch-") == 0) {
            // Parse epoch number from filename
            size_t start = strlen("ot_checkpoint_epoch-");
            size_t end = filename.find('.', start);
            if (end != std::string::npos) {
                try {
                    uint64_t epoch = std::stoull(filename.substr(start, end - start));
                    remaining_epochs.insert(epoch);
                } catch (...) {
                    // Ignore malformed filenames
                }
            }
        }
    }
    
    // Should have exactly epochs 800, 900, 1000
    EXPECT_EQ(remaining_epochs.size(), 3u);
    EXPECT_TRUE(remaining_epochs.count(800) > 0);
    EXPECT_TRUE(remaining_epochs.count(900) > 0);
    EXPECT_TRUE(remaining_epochs.count(1000) > 0);
}

TEST_F(OTCheckpointTest, ConcurrentSnapshot) {
    // This test verifies snapshot consistency under concurrent modifications
    // The iterate_live_snapshot should provide a consistent view
    
    const size_t num_initial = 100;
    std::vector<NodeID> initial_nodes;
    
    // Create initial nodes
    for (size_t i = 0; i < num_initial; i++) {
        initial_nodes.push_back(allocate_test_node(i));
    }
    
    // Use barriers for deterministic test timing
    std::atomic<bool> modifier_ready{false};
    std::atomic<bool> start_modifications{false};
    std::atomic<size_t> retired_count{0};
    
    std::thread modifier([&]() {
        // Signal we're ready
        modifier_ready = true;
        
        // Wait for start signal
        while (!start_modifications.load()) {
            std::this_thread::yield();
        }
        
        // Retire every 3rd node
        for (size_t i = 0; i < initial_nodes.size(); i += 3) {
            ot_->retire(initial_nodes[i], 999999);
            retired_count++;
        }
    });
    
    // Wait for modifier thread to be ready
    while (!modifier_ready.load()) {
        std::this_thread::yield();
    }
    
    // Signal modifier to start, then immediately checkpoint
    start_modifications = true;
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 1000));
    
    modifier.join();
    
    // Read checkpoint
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
    
    // The snapshot should have captured a consistent state
    // Either all initial nodes or some retired (but consistent)
    EXPECT_GE(entry_count, num_initial - retired_count.load());
    EXPECT_LE(entry_count, num_initial);
    
    // All captured entries should be self-consistent
    for (size_t i = 0; i < entry_count; i++) {
        const auto& pe = entries[i];
        if (pe.retire_epoch != ~uint64_t{0}) {
            EXPECT_LE(pe.retire_epoch, 999999u);
        }
    }
}

TEST_F(OTCheckpointTest, CheckpointAfterReclaim) {
    // Test that reclaimed handles are properly handled in checkpoint
    std::vector<NodeID> nodes;
    
    // Allocate and immediately retire some nodes
    for (int i = 0; i < 10; i++) {
        NodeID id = allocate_test_node(i * 10);
        nodes.push_back(id);
        if (i < 5) {
            ot_->retire(id, i * 10 + 1);
        }
    }
    
    // Reclaim retired nodes (with safe epoch)
    size_t reclaimed = ot_->reclaim_before_epoch(100);
    EXPECT_EQ(reclaimed, 5u);
    
    // Write checkpoint
    ASSERT_TRUE(checkpoint_->write(ot_.get(), 200));
    
    // Read and verify
    std::string ckpt_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
    uint64_t read_epoch = 0;
    size_t entry_count = 0;
    const OTCheckpoint::PersistentEntry* entries = nullptr;
    
    ASSERT_TRUE(checkpoint_->map_for_read(ckpt_path, &read_epoch, &entry_count, &entries));
    
    // After reclaim, the slots might still exist but should be marked as retired
    // or the slots might be reused. The important thing is that truly live nodes
    // are captured correctly. Let's count actual live nodes.
    size_t live_count = 0;
    for (size_t i = 0; i < entry_count; i++) {
        if (entries[i].retire_epoch == ~uint64_t{0}) {
            live_count++;
        }
    }
    
    // Should have at least the 5 nodes we didn't retire
    EXPECT_GE(live_count, 5u);
    EXPECT_LE(live_count, 10u);  // Should not exceed original allocation
}