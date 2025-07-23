/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Multi-threaded rotation stress tests for the checkpoint coordinator.
 * Verifies size threshold handling, epoch monotonicity, and replay bounds.
 */

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <unordered_set>

#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/durable_store.h"
#include "../../src/persistence/object_table_sharded.hpp"
#include "../../src/persistence/checkpoint_coordinator.h"
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/platform_fs.h"

using namespace xtree::persist;
namespace fs = std::filesystem;

class RotationStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / 
                    ("rotation_test_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(test_dir_);
        fs::create_directories(test_dir_ / "logs");
    }
    
    void TearDown() override {
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }
    
    struct TestSetup {
        std::unique_ptr<ObjectTableSharded> ot;
        std::unique_ptr<Superblock> sb;
        std::unique_ptr<Manifest> manifest;
        std::unique_ptr<OTLogGC> log_gc;
        std::unique_ptr<MVCCContext> mvcc;
        std::shared_ptr<OTDeltaLog> active_log;
        std::unique_ptr<CheckpointCoordinator> coordinator;
        
        void initialize(const fs::path& dir, size_t rotate_bytes) {
            // Create components
            ot = std::make_unique<ObjectTableSharded>();
            
            sb = std::make_unique<Superblock>((dir / "superblock").string());
            
            manifest = std::make_unique<Manifest>(dir.string());
            manifest->load();
            
            mvcc = std::make_unique<MVCCContext>();
            log_gc = std::make_unique<OTLogGC>(*manifest, *mvcc);
            
            // Initialize active log to nullptr
            active_log = nullptr;
            
            // Create coordinator with rotation policy
            CheckpointPolicy policy;
            policy.rotate_bytes = rotate_bytes;  // Rotate at this size
            policy.rotate_age = std::chrono::seconds{3600};  // 1 hour (won't trigger in test)
            policy.max_replay_bytes = rotate_bytes * 2;  // Checkpoint at 2x rotation size
            policy.min_interval = std::chrono::seconds{0};  // No minimum interval for testing
            policy.gc_on_rotate = false;  // Don't GC on rotation - let logs accumulate
            policy.gc_on_checkpoint = true;  // GC on checkpoint
            policy.gc_min_keep_logs = 1;  // Only keep active log (allow aggressive GC for test)
            
            coordinator = std::make_unique<CheckpointCoordinator>(
                *ot, *sb, *manifest, active_log,
                *log_gc, *mvcc, policy);
            
            // After coordinator constructor, it will have created the initial log
            // We need to get a reference to it
            active_log = coordinator->get_active_log();
            
            coordinator->start();
        }
        
        void shutdown() {
            if (coordinator) {
                coordinator->stop();
            }
        }
    };
    
    fs::path test_dir_;
};

// Test rotation triggered by size threshold with multiple threads
TEST_F(RotationStressTest, MultiThreadedSizeThreshold) {
    const size_t rotate_bytes = 50 * 1024;  // 50KB rotation threshold
    const int num_threads = 8;
    const int writes_per_thread = 1000;
    const size_t record_size = 1024;  // 1KB records (for length field only)
    
    TestSetup setup;
    setup.initialize(test_dir_, rotate_bytes);
    
    std::atomic<int> total_writes{0};
    std::atomic<uint64_t> max_epoch_seen{0};
    std::vector<std::thread> threads;
    
    // Track initial log sequence - get from coordinator
    auto initial_log = setup.coordinator->get_active_log();
    ASSERT_NE(initial_log, nullptr);
    uint64_t initial_sequence = initial_log->sequence();
    
    // Mutex and CV to synchronize thread start (C++17 compatible)
    std::mutex start_mutex;
    std::condition_variable start_cv;
    bool start_flag = false;
    std::atomic<int> threads_ready{0};
    
    // Launch writer threads
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            std::vector<uint8_t> data(record_size);
            std::mt19937 rng(t);
            
            // Synchronize start (C++17 compatible)
            {
                threads_ready.fetch_add(1);
                std::unique_lock<std::mutex> lock(start_mutex);
                start_cv.wait(lock, [&] { return start_flag; });
            }
            
            for (int i = 0; i < writes_per_thread; i++) {
                // Generate random data
                std::generate(data.begin(), data.end(), [&]() { return rng() & 0xFF; });
                
                // Create delta record
                OTDeltaRec rec;
                rec.handle_idx = t * writes_per_thread + i;
                rec.birth_epoch = setup.mvcc->get_global_epoch();
                rec.retire_epoch = UINT64_MAX;
                rec.file_id = 0;
                rec.offset = t * writes_per_thread * record_size + i * record_size;
                rec.length = record_size;
                
                // Append to log - load fresh shared_ptr from coordinator
                if (auto log = setup.coordinator->get_active_log()) {
                    try {
                        log->append({rec});
                        total_writes.fetch_add(1);
                        
                        // Track max epoch
                        uint64_t epoch = log->end_epoch_relaxed();
                        uint64_t current_max = max_epoch_seen.load();
                        while (epoch > current_max) {
                            max_epoch_seen.compare_exchange_weak(current_max, epoch);
                        }
                    } catch (const std::exception& e) {
                        // Log might be closing during rotation
                    }
                }
                
                // Small delay to spread writes
                if (i % 10 == 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            }
        });
    }
    
    // Wait for all threads to be ready, then start them
    while (threads_ready.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        std::lock_guard<std::mutex> lock(start_mutex);
        start_flag = true;
    }
    start_cv.notify_all();
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    // Give coordinator time to process final rotation if needed
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Check that rotation occurred - get from coordinator
    auto final_log = setup.coordinator->get_active_log();
    ASSERT_NE(final_log, nullptr);
    uint64_t final_sequence = final_log->sequence();
    
    // We should have rotated at least once (probably multiple times)
    EXPECT_GT(final_sequence, initial_sequence) 
        << "Expected rotation but log sequence didn't increase";
    
    // Verify manifest has multiple logs
    auto logs = setup.manifest->get_delta_logs();
    EXPECT_GT(logs.size(), 1) << "Expected multiple logs after rotation";
    
    // Verify epoch monotonicity across logs
    uint64_t prev_end = 0;
    for (const auto& log_info : logs) {
        if (log_info.end_epoch > 0) {  // Closed log
            EXPECT_GT(log_info.start_epoch, prev_end) 
                << "Epochs should be strictly increasing across logs";
            prev_end = log_info.end_epoch;
        }
    }
    
    // Verify replay window is bounded
    size_t replay_bytes = 0;
    for (const auto& log_info : logs) {
        if (log_info.end_epoch == 0) {  // Active log
            // Estimate size from current offset
            replay_bytes += final_log->get_end_offset();
        } else if (log_info.start_epoch > setup.manifest->get_checkpoint().epoch) {
            // Log is after checkpoint, contributes to replay
            auto [result, size] = PlatformFS::file_size(log_info.path);
            if (result.ok) {
                replay_bytes += size;
            }
        }
    }
    
    // Replay should be bounded by policy
    EXPECT_LE(replay_bytes, rotate_bytes * 6)  // More slack for smaller rotation size
        << "Replay window exceeded expected bounds";
    
    setup.shutdown();
}

// Test multiple rotations maintain epoch monotonicity
TEST_F(RotationStressTest, MultipleRotationsEpochMonotonicity) {
    const size_t rotate_bytes = 100 * 1024;  // 100KB - small for quick rotations
    const int num_rotations = 5;
    
    TestSetup setup;
    setup.initialize(test_dir_, rotate_bytes);
    
    std::vector<uint64_t> rotation_epochs;
    std::vector<uint64_t> log_sequences;
    
    for (int r = 0; r < num_rotations; r++) {
        // Fill log to trigger rotation
        std::vector<uint8_t> data(1024);  // 1KB chunks
        
        while (true) {
            auto log = setup.coordinator->get_active_log();
            if (!log) break;
            
            uint64_t start_seq = log->sequence();
            
            // Write until rotation occurs
            for (int i = 0; i < 150; i++) {  // 150KB should trigger rotation
                OTDeltaRec rec;
                rec.handle_idx = r * 1000 + i;
                rec.birth_epoch = setup.mvcc->advance_epoch();
                rec.retire_epoch = UINT64_MAX;
                rec.length = data.size();
                
                try {
                    // Get fresh log for each write
                    auto write_log = setup.coordinator->get_active_log();
                    if (write_log) {
                        write_log->append({rec});
                    }
                } catch (...) {
                    // Might fail during rotation
                }
            }
            
            // Check if rotation occurred
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto new_log = setup.coordinator->get_active_log();
            if (new_log && new_log->sequence() > start_seq) {
                rotation_epochs.push_back(new_log->end_epoch_relaxed());
                log_sequences.push_back(new_log->sequence());
                break;
            }
        }
    }
    
    // Verify sequences are strictly increasing
    for (size_t i = 1; i < log_sequences.size(); i++) {
        EXPECT_GT(log_sequences[i], log_sequences[i-1])
            << "Log sequences should strictly increase";
    }
    
    // Verify no epoch gaps or overlaps in manifest
    auto logs = setup.manifest->get_delta_logs();
    uint64_t last_end = 0;
    for (const auto& log_info : logs) {
        if (log_info.end_epoch > 0) {  // Closed log
            if (last_end > 0) {
                EXPECT_EQ(log_info.start_epoch, last_end + 1)
                    << "No gaps or overlaps between log epochs";
            }
            last_end = log_info.end_epoch;
        }
    }
    
    setup.shutdown();
}

// Test log GC after checkpoint - ensure old logs are cleaned up properly
TEST_F(RotationStressTest, LogGCAfterCheckpoint) {
    const size_t rotate_bytes = 5 * 1024;  // tiny to force rotations
    TestSetup setup;
    setup.initialize(test_dir_, rotate_bytes);

    auto wait_until = [&](auto pred, std::chrono::milliseconds timeout) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    };

    // write helper (payload-in-WAL so file size grows immediately)
    auto write_k_items = [&](int start, int count, size_t bytes_each) {
        std::vector<uint8_t> payload(bytes_each, 0xAB);
        for (int i = 0; i < count; ++i) {
            auto log = setup.coordinator->get_active_log();
            if (!log) continue;
            OTDeltaRec rec{};
            rec.handle_idx   = start + i;
            rec.birth_epoch  = setup.mvcc->advance_epoch();
            rec.retire_epoch = UINT64_MAX;
            rec.file_id      = 1;
            rec.segment_id   = 0;
            rec.offset       = static_cast<uint64_t>(start + i) * bytes_each;
            rec.length       = bytes_each;
            OTDeltaLog::DeltaWithPayload dwp{rec, payload.data(), payload.size()};
            log->append_with_payloads({dwp});
            // make size/epoch visible promptly
            log->sync();
        }
    };

    // 1) create >= 2 logs
    write_k_items(/*start*/0, /*count*/6, /*1KB*/1024);
    // Coordinator should rotate automatically when size threshold is hit

    ASSERT_TRUE(wait_until([&]{
        return setup.manifest->get_delta_logs().size() >= 2;
    }, std::chrono::milliseconds(2000))) << "Expected at least 2 logs after first rotation";


    // 2) create a 3rd log
    write_k_items(/*start*/6, /*count*/8, /*1KB*/1024);
    // Coordinator should rotate automatically when size threshold is hit
    

    ASSERT_TRUE(wait_until([&]{
        return setup.manifest->get_delta_logs().size() >= 3;
    }, std::chrono::milliseconds(3000))) << "Expected at least 3 logs after second rotation";

    // 3) snapshot manifest and choose a checkpoint **from the manifest**, not a guessed epoch
    auto logs_before = setup.manifest->get_delta_logs();
    // collect closed logs and sort by end_epoch
    std::vector<Manifest::DeltaLogInfo> closed;
    Manifest::DeltaLogInfo active{};
    for (const auto& li : logs_before) {
        if (li.end_epoch == 0) active = li; else closed.push_back(li);
    }
    ASSERT_GE(closed.size(), 2u) << "Need at least 2 closed logs to exercise GC.";
    std::sort(closed.begin(), closed.end(),
              [](const auto& a, const auto& b){ return a.end_epoch < b.end_epoch; });

    // pick the checkpoint at the boundary of the second closed log
    const uint64_t checkpoint_epoch = closed[1].end_epoch;
    ASSERT_GT(checkpoint_epoch, 0u);

    // 4) request checkpoint; coordinator should run GC afterwards
    // Get initial checkpoint epoch before requesting
    auto initial_stats = setup.coordinator->stats();
    uint64_t initial_checkpoint_epoch = initial_stats.last_checkpoint_epoch;
    uint64_t initial_checkpoints_written = initial_stats.checkpoints_written;
    
    std::cout << "Initial checkpoint epoch: " << initial_checkpoint_epoch 
              << " checkpoints_written: " << initial_checkpoints_written << std::endl;
    
    setup.coordinator->request_checkpoint();
    
    // Wait for checkpoint to complete (any new checkpoint)
    uint64_t actual_checkpoint_epoch = 0;
    ASSERT_TRUE(wait_until([&]{
        auto stats = setup.coordinator->stats();
        // Simple check: has a new checkpoint been written?
        if (stats.checkpoints_written > initial_checkpoints_written) {
            actual_checkpoint_epoch = stats.last_checkpoint_epoch;
            std::cout << "Checkpoint completed at epoch: " << actual_checkpoint_epoch 
                      << " checkpoints_written: " << stats.checkpoints_written << std::endl;
            return true;
        }
        return false;
    }, std::chrono::milliseconds(5000))) << "Checkpoint did not complete in time";
    
    // Wait for GC to complete (should happen right after checkpoint if gc_on_checkpoint is true)
    ASSERT_TRUE(wait_until([&]{
        auto stats = setup.coordinator->stats();
        return stats.last_gc_epoch >= actual_checkpoint_epoch;
    }, std::chrono::milliseconds(1000))) << "GC did not complete in time";

    // Reload manifest to see the latest state on disk
    setup.manifest->reload();
    
    // Now verify GC has removed the expected logs
    auto ls = setup.manifest->get_delta_logs();
    for (const auto& li : ls) {
        EXPECT_FALSE(li.end_epoch > 0 && li.end_epoch <= actual_checkpoint_epoch)
            << "Log should have been pruned: " << li.path 
            << " end_epoch=" << li.end_epoch << " checkpoint_epoch=" << actual_checkpoint_epoch;
    }

    // 5) verify only allowed logs remain and disk matches manifest
    auto logs_after = setup.manifest->get_delta_logs();

    // active must remain; no closed log fully covered by checkpoint
    bool found_active = false;
    for (const auto& li : logs_after) {
        if (li.end_epoch == 0) found_active = true;
        EXPECT_FALSE(li.end_epoch > 0 && li.end_epoch <= actual_checkpoint_epoch)
            << "Covered log not pruned: " << li.path
            << " end_epoch=" << li.end_epoch << " ckpt=" << actual_checkpoint_epoch;
    }
    EXPECT_TRUE(found_active) << "Active log should not be deleted by GC.";

    // directory matches manifest
    std::unordered_set<std::string> expected;
    for (const auto& li : logs_after) {
        expected.insert(std::filesystem::path(li.path).filename().string());
    }

    std::unordered_set<std::string> actual;
    std::filesystem::path logs_dir = test_dir_ / "logs";
    for (auto& p : std::filesystem::directory_iterator(logs_dir)) {
        if (!std::filesystem::is_regular_file(p.path())) continue;
        actual.insert(p.path().filename().string());
    }
    EXPECT_EQ(actual, expected) << "On-disk files must match manifest after GC.";
    
    setup.shutdown();
}

// Test concurrent reads during rotation
TEST_F(RotationStressTest, ConcurrentReadsduringRotation) {
    const size_t rotate_bytes = 10 * 1024;  // 10KB - small for quick rotation
    
    TestSetup setup;
    setup.initialize(test_dir_, rotate_bytes);
    
    std::atomic<bool> stop_readers{false};
    std::atomic<int> read_failures{0};
    std::atomic<int> successful_reads{0};
    
    // Start reader threads
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; t++) {
        readers.emplace_back([&]() {
            while (!stop_readers.load()) {
                // Load fresh shared_ptr from coordinator each time
                auto log = setup.coordinator->get_active_log();
                if (log) {
                    try {
                        // Try to read log state
                        size_t offset = log->get_end_offset();
                        uint64_t epoch = log->end_epoch_relaxed();
                        uint64_t seq = log->sequence();
                        
                        // Verify consistency - only count if we got real data
                        if (offset > 0 || epoch > 0 || seq > 0) {
                            successful_reads.fetch_add(1);
                        }
                    } catch (...) {
                        read_failures.fetch_add(1);
                    }
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }
    
    // Writer thread to trigger rotations
    std::thread writer([&]() {
        std::vector<uint8_t> data(1024);  // 1KB payload data
        for (int i = 0; i < 100; i++) {  // Write 100 records with payload
            // Load fresh shared_ptr from coordinator each time
            auto log = setup.coordinator->get_active_log();
            if (log) {
                OTDeltaRec rec;
                rec.handle_idx = i;
                rec.birth_epoch = setup.mvcc->advance_epoch();
                rec.retire_epoch = UINT64_MAX;
                rec.file_id = 1;
                rec.offset = i * 1024;
                rec.length = data.size();
                
                try {
                    // Write with actual payload data to increase file size
                    OTDeltaLog::DeltaWithPayload dwp{rec, data.data(), data.size()};
                    log->append_with_payloads({dwp});
                } catch (...) {
                    // Expected during rotation
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });
    
    writer.join();
    stop_readers.store(true);
    
    for (auto& t : readers) {
        t.join();
    }
    
    // Verify reads succeeded during rotation
    EXPECT_GT(successful_reads.load(), 0) << "Should have successful reads";
    EXPECT_EQ(read_failures.load(), 0) << "Reads should not fail during rotation";
    
    // Verify multiple rotations occurred
    auto logs = setup.manifest->get_delta_logs();
    EXPECT_GE(logs.size(), 3) << "Expected multiple rotations";
    
    setup.shutdown();
}