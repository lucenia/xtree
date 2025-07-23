/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <gtest/gtest.h>
#include "../../src/persistence/manifest.h"
#include <filesystem>
#include <fstream>

namespace xtree {
namespace persist {
namespace test {

namespace fs = std::filesystem;

class ManifestTest : public ::testing::Test {
protected:
    std::string test_dir_;
    
    void SetUp() override {
        // Create a unique test directory
        test_dir_ = fs::temp_directory_path() / ("manifest_test_" + std::to_string(::getpid()));
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        // Clean up test directory
        if (!test_dir_.empty()) {
            fs::remove_all(test_dir_);
        }
    }
};

TEST_F(ManifestTest, CreateAndStore) {
    Manifest manifest(test_dir_);
    
    // Set some test data
    manifest.set_superblock_path("superblock.dat");
    
    // Add data file
    Manifest::DataFileInfo data_file;
    data_file.class_id = 1;
    data_file.seq = 1;
    data_file.file = "segment_001.dat";
    data_file.bytes = 1024;
    manifest.add_data_file(data_file);
    
    // Add delta log
    Manifest::DeltaLogInfo delta_log;
    delta_log.path = "delta_001.wal";
    delta_log.start_epoch = 100;
    delta_log.end_epoch = 200;
    delta_log.size = 0;
    manifest.add_delta_log(delta_log);
    
    // Store the manifest
    ASSERT_TRUE(manifest.store());
    
    // Verify file exists
    std::string manifest_path = test_dir_ + "/manifest.json";
    ASSERT_TRUE(fs::exists(manifest_path));
}

TEST_F(ManifestTest, StoreAndLoad) {
    // Create and store a manifest
    {
        Manifest manifest(test_dir_);
        manifest.set_superblock_path("superblock.dat");
        
        // Add data files
        Manifest::DataFileInfo file1;
        file1.class_id = 1;
        file1.seq = 1;
        file1.file = "segment_001.dat";
        file1.bytes = 1024;
        manifest.add_data_file(file1);
        
        Manifest::DataFileInfo file2;
        file2.class_id = 2;
        file2.seq = 2;
        file2.file = "segment_002.dat";
        file2.bytes = 2048;
        manifest.add_data_file(file2);
        
        // Add delta logs
        Manifest::DeltaLogInfo log1;
        log1.path = "delta_001.wal";
        log1.start_epoch = 100;
        log1.end_epoch = 200;
        log1.size = 0;
        manifest.add_delta_log(log1);
        
        Manifest::DeltaLogInfo log2;
        log2.path = "delta_002.wal";
        log2.start_epoch = 201;
        log2.end_epoch = 300;
        log2.size = 0;
        manifest.add_delta_log(log2);
        
        // Set checkpoint
        Manifest::CheckpointInfo checkpoint;
        checkpoint.path = "checkpoint_300.dat";
        checkpoint.epoch = 300;
        checkpoint.size = 0;
        checkpoint.entries = 50000;
        checkpoint.crc32c = 0;
        manifest.set_checkpoint(checkpoint);
        
        ASSERT_TRUE(manifest.store());
    }
    
    // Load and verify
    {
        Manifest loaded_manifest(test_dir_);
        ASSERT_TRUE(loaded_manifest.load());
        
        EXPECT_EQ(loaded_manifest.get_superblock_path(), "superblock.dat");
        
        auto data_files = loaded_manifest.get_data_files();
        ASSERT_EQ(data_files.size(), 2);
        EXPECT_EQ(data_files[0].class_id, 1);
        EXPECT_EQ(data_files[0].file, "segment_001.dat");
        EXPECT_EQ(data_files[0].bytes, 1024);
        
        auto logs = loaded_manifest.get_delta_logs();
        ASSERT_EQ(logs.size(), 2);
        EXPECT_EQ(logs[0].path, "delta_001.wal");
        EXPECT_EQ(logs[0].start_epoch, 100);
        EXPECT_EQ(logs[0].end_epoch, 200);
        
        auto checkpoint = loaded_manifest.get_checkpoint();
        EXPECT_EQ(checkpoint.path, "checkpoint_300.dat");
        EXPECT_EQ(checkpoint.epoch, 300);
        EXPECT_EQ(checkpoint.entries, 50000);
    }
}

TEST_F(ManifestTest, PruneOldDeltaLogs) {
    Manifest manifest(test_dir_);
    
    // Add several delta logs
    Manifest::DeltaLogInfo log1;
    log1.path = "delta_001.wal";
    log1.start_epoch = 100;
    log1.end_epoch = 200;
    log1.size = 0;
    manifest.add_delta_log(log1);
    
    Manifest::DeltaLogInfo log2;
    log2.path = "delta_002.wal";
    log2.start_epoch = 201;
    log2.end_epoch = 300;
    log2.size = 0;
    manifest.add_delta_log(log2);
    
    Manifest::DeltaLogInfo log3;
    log3.path = "delta_003.wal";
    log3.start_epoch = 301;
    log3.end_epoch = 400;
    log3.size = 0;
    manifest.add_delta_log(log3);
    
    Manifest::DeltaLogInfo log4;
    log4.path = "delta_004.wal";
    log4.start_epoch = 401;
    log4.end_epoch = 0; // Current log
    log4.size = 0;
    manifest.add_delta_log(log4);
    
    // Prune logs before epoch 350
    manifest.prune_old_delta_logs(350);
    
    auto logs = manifest.get_delta_logs();
    ASSERT_EQ(logs.size(), 2);
    EXPECT_EQ(logs[0].path, "delta_003.wal"); // Spans checkpoint
    EXPECT_EQ(logs[1].path, "delta_004.wal"); // Current
}

TEST_F(ManifestTest, AtomicReplace) {
    // Create initial manifest
    {
        Manifest manifest(test_dir_);
        manifest.set_superblock_path("superblock_v1.dat");
        ASSERT_TRUE(manifest.store());
    }
    
    // Update manifest atomically
    {
        Manifest manifest(test_dir_);
        ASSERT_TRUE(manifest.load());
        manifest.set_superblock_path("superblock_v2.dat");
        ASSERT_TRUE(manifest.store());
    }
    
    // Verify update was atomic
    {
        Manifest manifest(test_dir_);
        ASSERT_TRUE(manifest.load());
        EXPECT_EQ(manifest.get_superblock_path(), "superblock_v2.dat");
    }
    
    // Verify no temp files left behind
    for (const auto& entry : fs::directory_iterator(test_dir_)) {
        EXPECT_FALSE(entry.path().string().find(".tmp") != std::string::npos);
    }
}

TEST_F(ManifestTest, HandleMissingFile) {
    Manifest manifest(test_dir_);
    
    // Should return false when no manifest exists
    EXPECT_FALSE(manifest.load());
}

TEST_F(ManifestTest, HandleCorruptedFile) {
    // Write invalid JSON
    std::string manifest_path = test_dir_ + "/manifest.json";
    {
        std::ofstream file(manifest_path);
        file << "{ invalid json ][";
    }
    
    Manifest manifest(test_dir_);
    EXPECT_FALSE(manifest.load());
}

} // namespace test
} // namespace persist
} // namespace xtree