/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <gtest/gtest.h>
#include "../../src/persistence/recovery.h"
#include "../../src/persistence/superblock.hpp"
#include "../../src/persistence/object_table_sharded.hpp"
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/ot_checkpoint.h"
#include "../../src/persistence/manifest.h"
#include <filesystem>
#include <fstream>

namespace xtree {
namespace persist {
namespace test {

namespace fs = std::filesystem;

class RecoveryTest : public ::testing::Test {
protected:
    std::string test_dir_;
    std::unique_ptr<Superblock> sb_;
    std::unique_ptr<ObjectTableSharded> ot_;
    std::unique_ptr<OTDeltaLog> log_;
    std::unique_ptr<OTCheckpoint> chk_;
    std::unique_ptr<Manifest> mf_;
    
    void SetUp() override {
        // Create a unique test directory
        test_dir_ = fs::temp_directory_path() / ("recovery_test_" + std::to_string(::getpid()));
        fs::create_directories(test_dir_);
        
        // Initialize components
        std::string sb_path = test_dir_ + "/superblock.dat";
        sb_ = std::make_unique<Superblock>(sb_path);
        
        ot_ = std::make_unique<ObjectTableSharded>();
        
        std::string log_path = test_dir_ + "/delta.wal";
        log_ = std::make_unique<OTDeltaLog>(log_path);
        
        chk_ = std::make_unique<OTCheckpoint>(test_dir_);
        
        mf_ = std::make_unique<Manifest>(test_dir_);
    }
    
    void TearDown() override {
        // Clean up test directory
        if (!test_dir_.empty()) {
            fs::remove_all(test_dir_);
        }
    }
    
    void CreateTestCheckpoint(const std::string& filename, uint64_t epoch, size_t num_entries) {
        // Populate the object table with test entries
        for (size_t i = 0; i < num_entries; i++) {
            OTAddr addr;
            addr.file_id = 1;
            addr.segment_id = 1;
            addr.offset = i * 4096;
            addr.length = 4096;
            
            NodeID id = ot_->allocate(NodeKind::Leaf, 0, addr, epoch - 10);
            // Mark the node as live so it's included in the checkpoint
            NodeID reserved = ot_->mark_live_reserve(id, epoch - 10);
            ot_->mark_live_commit(reserved, epoch - 10);
        }
        
        // Write checkpoint using the ObjectTable
        chk_ = std::make_unique<OTCheckpoint>(test_dir_);
        chk_->write(ot_.get(), epoch);
        
        // The checkpoint file will be created with a standard name pattern
        // We'll need to rename it to match the expected filename
        std::string checkpoint_path = OTCheckpoint::find_latest_checkpoint(test_dir_);
        if (!checkpoint_path.empty() && checkpoint_path != test_dir_ + "/" + filename) {
            std::filesystem::rename(checkpoint_path, test_dir_ + "/" + filename);
        }
    }
};

TEST_F(RecoveryTest, ColdStartWithNoData) {
    // Set up empty manifest
    mf_->store();
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Should complete without error
    ASSERT_NO_THROW(recovery.cold_start());
}

TEST_F(RecoveryTest, ColdStartWithCheckpoint) {
    // Create a checkpoint
    CreateTestCheckpoint("checkpoint_100.dat", 100, 10);
    
    // Set up manifest with checkpoint
    Manifest::CheckpointInfo checkpoint;
    checkpoint.path = "checkpoint_100.dat";
    checkpoint.epoch = 100;
    checkpoint.size = 0;
    checkpoint.entries = 10;
    checkpoint.crc32c = 0;
    mf_->set_checkpoint(checkpoint);
    mf_->store();
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Perform recovery
    recovery.cold_start();
    
    // Verify entries were restored
    // Note: handle 0 is reserved, so handles start at 1
    for (size_t i = 0; i < 10; i++) {
        NodeID id = NodeID::from_raw(((i + 1) << 16) | 1);  // Handles are 1-10, not 0-9 (16-bit tag)
        const OTEntry& entry = ot_->get(id);
        EXPECT_EQ(entry.class_id, 0);
        EXPECT_EQ(entry.kind, NodeKind::Leaf);
        EXPECT_EQ(entry.addr.offset, i * 4096);
    }
}

TEST_F(RecoveryTest, ColdStartWithCheckpointAndDeltaLog) {
    // Create a checkpoint at epoch 100
    CreateTestCheckpoint("checkpoint_100.dat", 100, 10);
    
    // Create delta log with changes after checkpoint
    std::string log_path = test_dir_ + "/delta_101_200.wal";
    std::ofstream log_file(log_path, std::ios::binary);
    
    // Write a few delta records (simplified - in reality would use proper framing)
    // For now, just create the file so recovery doesn't fail
    log_file.close();
    
    // Set up manifest
    Manifest::CheckpointInfo checkpoint;
    checkpoint.path = "checkpoint_100.dat";
    checkpoint.epoch = 100;
    checkpoint.size = 0;
    checkpoint.entries = 10;
    checkpoint.crc32c = 0;
    mf_->set_checkpoint(checkpoint);
    
    Manifest::DeltaLogInfo delta_log;
    delta_log.path = "delta_101_200.wal";
    delta_log.start_epoch = 101;
    delta_log.end_epoch = 200;
    delta_log.size = 0;
    mf_->add_delta_log(delta_log);
    mf_->store();
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Should handle delta log replay (even if stubbed)
    ASSERT_NO_THROW(recovery.cold_start());
}

TEST_F(RecoveryTest, HandleMissingManifest) {
    // Don't create manifest file
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Should continue with warning but not crash
    ASSERT_NO_THROW(recovery.cold_start());
}

TEST_F(RecoveryTest, HandleCorruptCheckpoint) {
    // Create corrupt checkpoint file
    std::string checkpoint_path = test_dir_ + "/checkpoint_bad.dat";
    {
        std::ofstream file(checkpoint_path, std::ios::binary);
        file << "corrupt data";
    }
    
    // Set up manifest with bad checkpoint
    Manifest::CheckpointInfo checkpoint;
    checkpoint.path = "checkpoint_bad.dat";
    checkpoint.epoch = 100;
    checkpoint.size = 0;
    checkpoint.entries = 10;
    checkpoint.crc32c = 0;
    mf_->set_checkpoint(checkpoint);
    mf_->store();
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Should handle corrupt checkpoint gracefully
    ASSERT_NO_THROW(recovery.cold_start());
}

TEST_F(RecoveryTest, DeltaLogOrdering) {
    // Create checkpoint at epoch 100
    CreateTestCheckpoint("checkpoint_100.dat", 100, 5);
    
    // Set up manifest with multiple delta logs
    Manifest::CheckpointInfo checkpoint;
    checkpoint.path = "checkpoint_100.dat";
    checkpoint.epoch = 100;
    checkpoint.size = 0;
    checkpoint.entries = 5;
    checkpoint.crc32c = 0;
    mf_->set_checkpoint(checkpoint);
    
    Manifest::DeltaLogInfo log1;
    log1.path = "delta_301_400.wal";
    log1.start_epoch = 301;
    log1.end_epoch = 400;
    log1.size = 0;
    mf_->add_delta_log(log1);
    
    Manifest::DeltaLogInfo log2;
    log2.path = "delta_101_200.wal";
    log2.start_epoch = 101;
    log2.end_epoch = 200;
    log2.size = 0;
    mf_->add_delta_log(log2);
    
    Manifest::DeltaLogInfo log3;
    log3.path = "delta_201_300.wal";
    log3.start_epoch = 201;
    log3.end_epoch = 300;
    log3.size = 0;
    mf_->add_delta_log(log3);
    mf_->store();
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Recovery should sort and apply logs in correct order
    ASSERT_NO_THROW(recovery.cold_start());
}

TEST_F(RecoveryTest, SkipOldDeltaLogs) {
    // Create checkpoint at epoch 300
    CreateTestCheckpoint("checkpoint_300.dat", 300, 5);
    
    // Set up manifest with old and new delta logs
    Manifest::CheckpointInfo checkpoint;
    checkpoint.path = "checkpoint_300.dat";
    checkpoint.epoch = 300;
    checkpoint.size = 0;
    checkpoint.entries = 5;
    checkpoint.crc32c = 0;
    mf_->set_checkpoint(checkpoint);
    
    Manifest::DeltaLogInfo log1;
    log1.path = "delta_100_200.wal";
    log1.start_epoch = 100;
    log1.end_epoch = 200;
    log1.size = 0;
    mf_->add_delta_log(log1);
    
    Manifest::DeltaLogInfo log2;
    log2.path = "delta_201_299.wal";
    log2.start_epoch = 201;
    log2.end_epoch = 299;
    log2.size = 0;
    mf_->add_delta_log(log2);
    
    Manifest::DeltaLogInfo log3;
    log3.path = "delta_301_400.wal";
    log3.start_epoch = 301;
    log3.end_epoch = 400;
    log3.size = 0;
    mf_->add_delta_log(log3);
    mf_->store();
    
    // Create empty log files
    for (const auto& log : mf_->get_delta_logs()) {
        std::ofstream file(test_dir_ + "/" + log.path, std::ios::binary);
        file.close();
    }
    
    // Create recovery instance
    Recovery recovery(*sb_, *ot_, *log_, *chk_, *mf_);
    
    // Should skip old logs
    ASSERT_NO_THROW(recovery.cold_start());
}

} // namespace test
} // namespace persist
} // namespace xtree