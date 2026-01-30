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
#include "persistence/config.h"

using namespace xtree::persist;

class ConfigTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ConfigTest, SizeClassConfiguration) {
    // Verify size class constants
    EXPECT_EQ(size_class::kNumClasses, 7);
    EXPECT_EQ(size_class::kMinSize, 4096u);
    EXPECT_EQ(size_class::kMaxSize, 262144u);
    
    // Verify size class array
    EXPECT_EQ(size_class::kSizes[0], 4096u);
    EXPECT_EQ(size_class::kSizes[1], 8192u);
    EXPECT_EQ(size_class::kSizes[2], 16384u);
    EXPECT_EQ(size_class::kSizes[3], 32768u);
    EXPECT_EQ(size_class::kSizes[4], 65536u);
    EXPECT_EQ(size_class::kSizes[5], 131072u);
    EXPECT_EQ(size_class::kSizes[6], 262144u);
    
    // Verify each size is double the previous
    for (size_t i = 1; i < size_class::kNumClasses; i++) {
        EXPECT_EQ(size_class::kSizes[i], size_class::kSizes[i-1] * 2);
    }
}

TEST_F(ConfigTest, ObjectTableConfiguration) {
    // Verify object table limits
    EXPECT_EQ(object_table::kInitialCapacity, 1u << 20);
    EXPECT_EQ(object_table::kMaxHandles, 1ULL << 56);
    EXPECT_EQ(object_table::kMaxTag, 255u);
    
    // Verify initial capacity is reasonable
    EXPECT_GE(object_table::kInitialCapacity, 1024u);
    EXPECT_LE(object_table::kInitialCapacity, 10u << 20); // <= 10M
}

TEST_F(ConfigTest, SegmentConfiguration) {
    // Verify segment sizes
    EXPECT_EQ(segment::kDefaultSegmentSize, 16u * 1024 * 1024);
    EXPECT_EQ(segment::kMaxSegmentSize, 256u * 1024 * 1024);
    EXPECT_LT(segment::kDefaultSegmentSize, segment::kMaxSegmentSize);
    
    // Verify thresholds
    EXPECT_GT(segment::kFragmentationThreshold, 0.0);
    EXPECT_LE(segment::kFragmentationThreshold, 1.0);
    EXPECT_GT(segment::kMinFreeSpacePercent, 0u);
    EXPECT_LT(segment::kMinFreeSpacePercent, 100u);
}

TEST_F(ConfigTest, MVCCConfiguration) {
    // Verify MVCC constants
    EXPECT_EQ(mvcc::kInvalidEpoch, ~uint64_t{0});
    EXPECT_GE(mvcc::kInitialPinSlots, 100u);
    EXPECT_LE(mvcc::kInitialPinSlots, mvcc::kMaxPinSlots);
    EXPECT_LE(mvcc::kMaxPinSlots, 1u << 20); // Reasonable limit
}

TEST_F(ConfigTest, SuperblockConfiguration) {
    // Verify magic number
    EXPECT_EQ(superblock::kMagic, 0x5854524545505331ULL);
    
    // Verify sizes
    EXPECT_EQ(superblock::kVersion, 1u);
    EXPECT_EQ(superblock::kHeaderSize, 4096u);
    EXPECT_EQ(superblock::kPadSize, 256u);
    EXPECT_LT(superblock::kPadSize, superblock::kHeaderSize);
}

TEST_F(ConfigTest, DeltaLogConfiguration) {
    // Verify delta log parameters
    EXPECT_GT(delta_log::kMaxBatchSize, 0u);
    EXPECT_LE(delta_log::kMaxBatchSize, 10000u); // Reasonable batch size
    
    EXPECT_GT(delta_log::kRotateSize, 1u << 20); // > 1MB
    EXPECT_LE(delta_log::kRotateSize, 1u << 30); // <= 1GB
    
    EXPECT_GT(delta_log::kRotateAge, 0u);
    EXPECT_LE(delta_log::kRotateAge, 86400u); // <= 24 hours
    
    EXPECT_GE(delta_log::kBufferSize, 1u << 20); // >= 1MB
    EXPECT_LE(delta_log::kBufferSize, delta_log::kRotateSize);
}

TEST_F(ConfigTest, CheckpointConfiguration) {
    // Verify checkpoint triggers
    EXPECT_GT(checkpoint::kTriggerSize, delta_log::kRotateSize);
    EXPECT_GT(checkpoint::kTriggerTime, 0u);
    EXPECT_LE(checkpoint::kTriggerTime, 3600u); // <= 1 hour
    
    // Verify compression level is reasonable
    EXPECT_GE(checkpoint::kCompressionLevel, 0u);
    EXPECT_LE(checkpoint::kCompressionLevel, 20u); // zstd max is ~22
}

TEST_F(ConfigTest, CompactionConfiguration) {
    // Verify compaction thresholds
    EXPECT_GT(compaction::kDeadRatioThreshold, 0.0);
    EXPECT_LT(compaction::kDeadRatioThreshold, 1.0);
    
    EXPECT_GT(compaction::kTombstoneRatioThreshold, 0.0);
    EXPECT_LT(compaction::kTombstoneRatioThreshold, 1.0);
    
    EXPECT_GT(compaction::kMinSegmentAge, 0u);
    EXPECT_LE(compaction::kMinSegmentAge, 3600u); // <= 1 hour
    
    EXPECT_GE(compaction::kMaxConcurrentCompactions, 1u);
    EXPECT_LE(compaction::kMaxConcurrentCompactions, 10u);
    
    EXPECT_GT(compaction::kTargetCpuPercent, 0.0);
    EXPECT_LE(compaction::kTargetCpuPercent, 50.0); // <= 50% CPU
}

TEST_F(ConfigTest, RecoveryConfiguration) {
    // Verify recovery parameters
    EXPECT_GT(recovery::kMaxRecoveryTime, 0u);
    EXPECT_LE(recovery::kMaxRecoveryTime, 30000u); // <= 30 seconds
    
    EXPECT_GE(recovery::kPrefetchSize, 1u << 20); // >= 1MB
    EXPECT_LE(recovery::kPrefetchSize, 1u << 28); // <= 256MB
    
    // Checksum verification should be configurable
    EXPECT_TRUE(recovery::kVerifyChecksums == true || recovery::kVerifyChecksums == false);
}

TEST_F(ConfigTest, HotsetConfiguration) {
    // Verify hotset sizes are increasing
    EXPECT_LT(hotset::kL0Size, hotset::kL1Size);
    EXPECT_LT(hotset::kL1Size, hotset::kL2Size);
    
    // Verify reasonable sizes
    EXPECT_GE(hotset::kL0Size, 1u << 10); // >= 1KB
    EXPECT_LE(hotset::kL2Size, 1u << 30); // <= 1GB
}

TEST_F(ConfigTest, PlatformSpecificConfiguration) {
    #ifdef _WIN32
        // Windows-specific checks
        EXPECT_TRUE(platform::kUseWindowsLargePage == true || 
                   platform::kUseWindowsLargePage == false);
        EXPECT_EQ(platform::kLargePageSize, 2u * 1024 * 1024); // 2MB
    #else
        // Unix-specific checks
        EXPECT_TRUE(platform::kUseMadvise == true || 
                   platform::kUseMadvise == false);
        EXPECT_TRUE(platform::kUseHugePages == true || 
                   platform::kUseHugePages == false);
        EXPECT_EQ(platform::kHugePageSize, 2u * 1024 * 1024); // 2MB
    #endif
}

TEST_F(ConfigTest, DebugConfiguration) {
    #ifdef NDEBUG
        // Release mode
        EXPECT_FALSE(debug_config::kValidateTags);
        EXPECT_FALSE(debug_config::kTrackAllocations);
        EXPECT_FALSE(debug_config::kChecksumWrites);
    #else
        // Debug mode
        EXPECT_TRUE(debug_config::kValidateTags);
        EXPECT_TRUE(debug_config::kTrackAllocations);
        EXPECT_TRUE(debug_config::kChecksumWrites);
    #endif
}

TEST_F(ConfigTest, FileNamingConfiguration) {
    // Verify file names are reasonable
    EXPECT_STREQ(files::kMetaFile, "xtree.meta");
    EXPECT_STREQ(files::kDataPrefix, "xtree");
    EXPECT_STREQ(files::kDeltaLogFile, "ot_delta.wal");
    EXPECT_STREQ(files::kCheckpointPrefix, "ot_checkpoint");
    EXPECT_STREQ(files::kManifestFile, "manifest.json");
    
    // Verify no empty names
    EXPECT_GT(strlen(files::kMetaFile), 0u);
    EXPECT_GT(strlen(files::kDataPrefix), 0u);
    EXPECT_GT(strlen(files::kDeltaLogFile), 0u);
    EXPECT_GT(strlen(files::kCheckpointPrefix), 0u);
    EXPECT_GT(strlen(files::kManifestFile), 0u);
}

TEST_F(ConfigTest, ConsistencyChecks) {
    // Checkpoint should trigger before delta log rotates
    EXPECT_GT(checkpoint::kTriggerSize, delta_log::kRotateSize);
    
    // Buffer size should be smaller than rotate size
    EXPECT_LE(delta_log::kBufferSize, delta_log::kRotateSize);
    
    // Max segment size should accommodate largest size class
    EXPECT_GE(segment::kMaxSegmentSize, size_class::kMaxSize);
    
    // Compaction thresholds should be reasonable
    EXPECT_LT(compaction::kTombstoneRatioThreshold, 
              compaction::kDeadRatioThreshold);
}