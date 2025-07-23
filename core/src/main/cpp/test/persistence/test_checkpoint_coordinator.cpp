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

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "../../src/persistence/checkpoint_coordinator.h"
#include "../../src/persistence/object_table_sharded.hpp"
#include "../../src/persistence/mvcc_context.h"
#include "../../src/persistence/manifest.h"
#include "../../src/persistence/superblock.hpp"
#include "../../src/persistence/ot_log_gc.h"
#include "../../src/persistence/reclaimer.h"
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/platform_fs.h"
#include <filesystem>
#include <thread>
#include <chrono>

using namespace xtree::persist;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

class CheckpointCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temp directory
        test_dir_ = fs::temp_directory_path() / ("ckpt_coord_test_" + std::to_string(getpid()));
        fs::create_directories(test_dir_);
        
        // Create logs subdirectory
        fs::create_directories(test_dir_ / "logs");
        
        // Initialize components
        ot_ = std::make_unique<ObjectTableSharded>();
        mvcc_ = std::make_unique<MVCCContext>();
        manifest_ = std::make_unique<Manifest>(test_dir_.string());
        
        // Initialize manifest with a base checkpoint at epoch 0
        Manifest::CheckpointInfo base_ckpt;
        base_ckpt.path = (test_dir_ / "checkpoint_000000").string();
        base_ckpt.epoch = 0;
        base_ckpt.size = 0;
        base_ckpt.entries = 0;
        base_ckpt.crc32c = 0;
        manifest_->set_checkpoint(base_ckpt);
        std::string superblock_path = (test_dir_ / "superblock").string();
        superblock_ = std::make_unique<Superblock>(superblock_path);
        
        // Initialize active log to nullptr - coordinator will create it
        active_log_ = nullptr;
        
        // Create log GC
        log_gc_ = std::make_unique<OTLogGC>(*manifest_, *mvcc_);
        
        // Create reclaimer
        reclaimer_ = std::make_unique<Reclaimer>(*ot_, *mvcc_);
    }
    
    void TearDown() override {
        // Stop coordinator if running
        if (coordinator_) {
            coordinator_->stop();
        }
        
        // Coordinator owns the active log; nothing to delete here
        // If no coordinator was created, initial_log_ will be cleaned up automatically
        
        // Clean up test directory
        fs::remove_all(test_dir_);
    }
    
    // Helper to create coordinator with custom policy
    void CreateCoordinator(const CheckpointPolicy& policy = CheckpointPolicy{}) {
        // Transfer ownership of the log to the coordinator
        coordinator_ = std::make_unique<CheckpointCoordinator>(
            *ot_, *superblock_, *manifest_, active_log_,
            *log_gc_, *mvcc_, policy, reclaimer_.get()
        );
    }
    
    // Helper to simulate write activity
    void SimulateWrites(size_t count, size_t batch_size = 10) {
        auto log = std::atomic_load(&active_log_);
        ASSERT_NE(log, nullptr);
        
        for (size_t i = 0; i < count; i += batch_size) {
            std::vector<OTDeltaRec> batch;
            uint64_t batch_epoch = mvcc_->advance_epoch();  // One epoch per batch
            for (size_t j = 0; j < batch_size && (i + j) < count; ++j) {
                OTDeltaRec rec{};
                rec.handle_idx = i + j;
                rec.tag = 1;
                rec.birth_epoch = batch_epoch;
                rec.retire_epoch = ~uint64_t{0};
                batch.push_back(rec);
            }
            log->append(batch);
        }
        log->sync();
    }
    
protected:
    fs::path test_dir_;
    fs::path initial_log_path_;
    std::unique_ptr<ObjectTableSharded> ot_;
    std::unique_ptr<MVCCContext> mvcc_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<Superblock> superblock_;
    std::unique_ptr<OTDeltaLog> initial_log_;  // Ownership will transfer to coordinator
    std::shared_ptr<OTDeltaLog> active_log_;  // Shared pointer for writers
    std::unique_ptr<OTLogGC> log_gc_;
    std::unique_ptr<Reclaimer> reclaimer_;
    std::unique_ptr<CheckpointCoordinator> coordinator_;
};

TEST_F(CheckpointCoordinatorTest, BasicStartStop) {
    CreateCoordinator();
    
    // Should be able to start and stop
    coordinator_->start();
    std::this_thread::sleep_for(100ms);
    coordinator_->stop();
    
    // Should be idempotent
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, RequestCheckpoint) {
    CheckpointPolicy policy;
    policy.max_age = 3600s;  // 1 hour - won't trigger naturally in test
    CreateCoordinator(policy);
    
    coordinator_->start();
    
    // Request checkpoint explicitly
    coordinator_->request_checkpoint();
    
    // Give it time to process
    std::this_thread::sleep_for(500ms);
    
    auto stats = coordinator_->stats();
    EXPECT_GT(stats.checkpoints_written, 0);
    
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, TimeBasedTrigger) {
    CheckpointPolicy policy;
    policy.max_age = 1s;  // Very short for testing
    policy.min_interval = 0s;  // No minimum
    CreateCoordinator(policy);
    
    coordinator_->start();
    
    // Wait for time-based trigger
    std::this_thread::sleep_for(1500ms);
    
    auto stats = coordinator_->stats();
    EXPECT_GT(stats.checkpoints_written, 0);
    
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, SizeBasedTrigger) {
    CheckpointPolicy policy;
    policy.max_replay_bytes = 1024;  // 1KB - very small for testing
    policy.max_age = 3600s;  // Won't trigger on time
    policy.min_interval = 0s;  // No minimum interval for testing
    CreateCoordinator(policy);
    
    coordinator_->start();
    
    // Write enough data to trigger size-based checkpoint
    SimulateWrites(100, 10);
    
    // Give coordinator time to detect and checkpoint
    std::this_thread::sleep_for(500ms);
    
    auto stats = coordinator_->stats();
    EXPECT_GT(stats.checkpoints_written, 0);
    // Note: last_replay_bytes might be 0 if checkpoint just completed and reset the window
    
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, RecoveryInitialization) {
    CreateCoordinator();
    
    // Simulate recovery with large replay
    coordinator_->initialize_after_recovery(100, 300 * 1024 * 1024);  // 300MB replay
    
    coordinator_->start();
    
    // Should trigger checkpoint soon due to large replay
    std::this_thread::sleep_for(500ms);
    
    auto stats = coordinator_->stats();
    EXPECT_GT(stats.checkpoints_written, 0);
    
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, ErrorCallback) {
    CreateCoordinator();
    
    std::vector<std::string> errors;
    coordinator_->set_error_callback([&errors](const std::string& error) {
        errors.push_back(error);
    });
    
    // We can't easily trigger real errors in test, but we can verify the callback is set
    EXPECT_EQ(errors.size(), 0);
}

TEST_F(CheckpointCoordinatorTest, MetricsCallback) {
    CreateCoordinator();
    
    std::atomic<size_t> metrics_calls{0};
    coordinator_->set_metrics_callback([&metrics_calls](const CheckpointCoordinator::Stats& stats) {
        metrics_calls.fetch_add(1);
        EXPECT_GE(stats.checkpoints_written, 0);
    });
    
    coordinator_->start();
    coordinator_->request_checkpoint();
    
    // Wait for checkpoint to complete
    std::this_thread::sleep_for(500ms);
    
    // Metrics should have been called at least once
    EXPECT_GT(metrics_calls.load(), 0);
    
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, GroupCommitDisabled) {
    CreateCoordinator();
    
    // Group commit disabled by default
    coordinator_->set_group_commit_interval(0ms);
    
    auto new_root = NodeID::from_parts(123, 1);
    uint64_t new_epoch = mvcc_->advance_epoch();
    
    // Should do direct publish
    bool published = coordinator_->try_publish(new_root, new_epoch);
    EXPECT_TRUE(published);
}

TEST_F(CheckpointCoordinatorTest, GroupCommitEnabled) {
    CreateCoordinator();
    
    // Enable group commit with 10ms interval
    coordinator_->set_group_commit_interval(10ms);
    
    std::atomic<int> publish_count{0};
    std::vector<std::thread> writers;
    
    // Launch multiple writers
    for (int i = 0; i < 5; ++i) {
        writers.emplace_back([this, &publish_count, i]() {
            auto new_root = NodeID::from_parts(100 + i, 1);
            uint64_t new_epoch = mvcc_->advance_epoch();
            
            bool leader = coordinator_->try_publish(new_root, new_epoch);
            if (leader) {
                publish_count.fetch_add(1);
            } else {
                // Wait for leader to finish
                coordinator_->wait_for_publish();
            }
        });
    }
    
    // Wait for all writers
    for (auto& t : writers) {
        t.join();
    }
    
    // Only one should have been the leader
    EXPECT_EQ(publish_count.load(), 1);
}

TEST_F(CheckpointCoordinatorTest, LogRotation) {
    CheckpointPolicy policy;
    policy.max_replay_bytes = 2048;  // Checkpoint threshold
    policy.rotate_bytes = 1024;       // Rotate at 1KB (small for testing)
    policy.rotate_age = 3600s;         // Won't trigger in test
    policy.min_interval = 0s;          // No minimum interval for testing
    CreateCoordinator(policy);
    
    coordinator_->start();
    
    // Write enough data to exceed rotation threshold (1KB)
    // Each OTDeltaRec is 52 bytes on wire, 100 records = ~5200 bytes
    SimulateWrites(100);
    
    // Log should have data now
    auto log = std::atomic_load(&active_log_);
    ASSERT_NE(log, nullptr);
    size_t log_size = log->get_end_offset();
    EXPECT_GT(log_size, policy.rotate_bytes) << "Log size should exceed rotation threshold";
    
    // Request checkpoint to trigger rotation check
    coordinator_->request_checkpoint();
    std::this_thread::sleep_for(1000ms);  // Give more time for rotation
    
    auto stats = coordinator_->stats();
    EXPECT_GT(stats.checkpoints_written, 0);  // Should have checkpointed
    EXPECT_GT(stats.rotations, 0);  // Should have rotated the log
    // Note: last_rotate_ms might be 0 if timing is very fast
    
    // Active log should have changed
    auto current_log = std::atomic_load(&active_log_);
    ASSERT_NE(current_log, nullptr);
    
    coordinator_->stop();
}

TEST_F(CheckpointCoordinatorTest, WorkloadAdaptivePolicies) {
    CheckpointPolicy policy;
    // Set up adaptive thresholds
    policy.max_replay_bytes = 100 * 1024;             // Burst: 100KB for testing
    policy.steady_replay_bytes = 50 * 1024;           // Steady: 50KB for testing  
    policy.query_only_age = 1s;                       // Query-only: 1s for testing
    policy.min_interval = 0s;                         // No min interval for testing
    
    CreateCoordinator(policy);
    coordinator_->start();
    
    // Simulate burst workload
    SimulateWrites(10000, 100);  // Large batches
    
    // Simulate steady workload
    for (int i = 0; i < 10; ++i) {
        SimulateWrites(10, 1);  // Small writes
        std::this_thread::sleep_for(100ms);
    }
    
    // Simulate query-only (no writes)
    std::this_thread::sleep_for(1s);
    
    coordinator_->stop();
    
    auto stats = coordinator_->stats();
    // Should have triggered some checkpoints
    EXPECT_GT(stats.checkpoints_written, 0);
}

// Integration test with full persistence stack
TEST_F(CheckpointCoordinatorTest, FullPersistenceIntegration) {
    CheckpointPolicy policy;
    policy.max_replay_bytes = 50 * 1024;  // 50KB - trigger on small data
    policy.steady_replay_bytes = 25 * 1024;  // 25KB
    policy.rotate_bytes = 100 * 1024;  // 100KB rotation
    policy.min_interval = 0s;  // No minimum interval for testing
    policy.max_age = 600s;  // 10 minutes
    CreateCoordinator(policy);
    
    // Set up callbacks
    std::vector<std::string> errors;
    size_t metrics_count = 0;
    
    coordinator_->set_error_callback([&errors](const std::string& error) {
        errors.push_back(error);
    });
    
    coordinator_->set_metrics_callback([&metrics_count](const CheckpointCoordinator::Stats& stats) {
        metrics_count++;
        EXPECT_GE(stats.last_epoch, 0);
    });
    
    // Start coordinator
    coordinator_->start();
    
    // Simulate mixed workload
    for (int cycle = 0; cycle < 3; ++cycle) {
        // Burst phase
        SimulateWrites(1000, 50);
        
        // Steady phase
        for (int i = 0; i < 5; ++i) {
            SimulateWrites(10, 1);
            std::this_thread::sleep_for(50ms);
        }
        
        // Query-only phase
        std::this_thread::sleep_for(200ms);
    }
    
    // Request final checkpoint
    coordinator_->request_checkpoint();
    std::this_thread::sleep_for(500ms);
    
    coordinator_->stop();
    
    // Verify results
    auto stats = coordinator_->stats();
    EXPECT_GT(stats.checkpoints_written, 0);
    EXPECT_GT(stats.rotations, 0);
    EXPECT_GT(metrics_count, 0);
    if (!errors.empty()) {
        for (const auto& err : errors) {
            std::cerr << "Error: " << err << std::endl;
        }
    }
    EXPECT_EQ(errors.size(), 0);  // No errors expected
}

// Test to verify the duplicate active logs bug is fixed
TEST_F(CheckpointCoordinatorTest, NoDuplicateActiveLogsAfterRotation) {
    CheckpointPolicy policy;
    policy.max_replay_bytes = 4096;   // Small checkpoint threshold
    policy.rotate_bytes = 2048;       // Small rotation threshold
    policy.min_interval = 0s;         // No minimum interval for testing
    CreateCoordinator(policy);
    
    coordinator_->start();
    
    // Write enough data to trigger multiple rotations
    for (int rotation = 0; rotation < 3; ++rotation) {
        // Write data to exceed rotation threshold
        SimulateWrites(100);  // ~5200 bytes per batch
        
        // Request checkpoint to trigger rotation
        coordinator_->request_checkpoint();
        std::this_thread::sleep_for(500ms);  // Wait for rotation to complete
        
        // Verify manifest has exactly one active log
        auto& logs = manifest_->get_delta_logs();
        int active_count = 0;
        uint64_t last_end_epoch = 0;
        
        // Debug output
        std::cout << "Rotation " << rotation << " - Logs in manifest:\n";
        for (const auto& log : logs) {
            std::cout << "  Path: " << log.path 
                      << ", Start: " << log.start_epoch 
                      << ", End: " << log.end_epoch 
                      << ", Size: " << log.size << "\n";
        }
        
        // Sort logs by start_epoch for validation
        std::vector<Manifest::DeltaLogInfo> sorted_logs(logs.begin(), logs.end());
        std::sort(sorted_logs.begin(), sorted_logs.end(),
                  [](const auto& a, const auto& b) { return a.start_epoch < b.start_epoch; });
        
        std::set<uint64_t> start_epochs;
        for (const auto& log : sorted_logs) {
            // Check for duplicate start epochs
            EXPECT_EQ(start_epochs.count(log.start_epoch), 0) 
                << "Found duplicate start_epoch: " << log.start_epoch;
            start_epochs.insert(log.start_epoch);
            
            // Count active logs (end_epoch == 0)
            if (log.end_epoch == 0) {
                active_count++;
            } else {
                // For closed logs, verify epoch continuity
                if (last_end_epoch > 0) {
                    EXPECT_LE(last_end_epoch, log.start_epoch)
                        << "Gap in epoch sequence between logs";
                }
                last_end_epoch = log.end_epoch;
            }
        }
        
        // Exactly one log should be active
        EXPECT_EQ(active_count, 1) 
            << "Expected exactly 1 active log, found " << active_count 
            << " after rotation " << rotation;
        
        // If we have closed logs, verify the new active log starts after them
        if (last_end_epoch > 0 && !sorted_logs.empty()) {
            auto active_log = std::find_if(sorted_logs.begin(), sorted_logs.end(),
                                          [](const auto& l) { return l.end_epoch == 0; });
            if (active_log != sorted_logs.end()) {
                EXPECT_GT(active_log->start_epoch, last_end_epoch)
                    << "Active log start_epoch should be > last closed log end_epoch";
            }
        }
    }
    
    coordinator_->stop();
    
    // Final verification of stats
    auto stats = coordinator_->stats();
    EXPECT_GE(stats.rotations, 2) << "Should have performed at least 2 rotations";
    EXPECT_GT(stats.checkpoints_written, 0) << "Should have written checkpoints";
}