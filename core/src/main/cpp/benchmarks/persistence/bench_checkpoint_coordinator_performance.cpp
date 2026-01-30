/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Checkpoint Coordinator Performance Benchmarks
 * Tests coordinator efficiency, group commit, rotation, and GC
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <filesystem>
#include <atomic>
#include <thread>
#include <cstring>
#include "../../src/persistence/checkpoint_coordinator.h"
#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/durable_store.h"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class CheckpointCoordinatorBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    Paths paths_;
    std::unique_ptr<DurableRuntime> runtime_;
    
    void SetUp() override {
        test_dir_ = "/tmp/coordinator_bench_" + std::to_string(getpid());
        fs::create_directories(test_dir_);
        
        paths_ = {
            .data_dir = test_dir_,
            .manifest = test_dir_ + "/manifest.json",
            .superblock = test_dir_ + "/superblock.bin",
            .active_log = test_dir_ + "/ot_delta.wal"
        };
    }
    
    void TearDown() override {
        runtime_.reset();
        try {
            fs::remove_all(test_dir_);
        } catch (...) {}
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
    
    std::vector<uint8_t> generatePayload(size_t size) {
        std::vector<uint8_t> payload(size);
        std::mt19937 rng(42);
        std::uniform_int_distribution<uint8_t> dist(0, 255);
        for (auto& byte : payload) {
            byte = dist(rng);
        }
        return payload;
    }
};

TEST_F(CheckpointCoordinatorBenchmark, GroupCommitPerformance) {
    printSeparator("Group Commit Performance");
    
    std::cout << "\nMeasuring group commit batching efficiency:\n\n";
    std::cout << "Writers | Interval | Commits/sec | Latency p50 | Latency p99 | Status\n";
    std::cout << "--------|----------|-------------|-------------|-------------|--------\n";
    
    const int WRITER_COUNTS[] = {1, 2, 4, 8, 16};
    const size_t GROUP_COMMIT_MS[] = {0, 1, 5, 10};  // 0 = disabled
    const size_t COMMITS_PER_WRITER = 100;
    const size_t NODE_SIZE = 4096;
    
    for (size_t interval_ms : GROUP_COMMIT_MS) {
        for (int num_writers : WRITER_COUNTS) {
            // Create runtime with group commit policy
            CheckpointPolicy policy;
            policy.group_commit_interval_ms = interval_ms;
            policy.max_replay_bytes = 1024 * 1024 * 1024;  // 1GB to avoid checkpoints
            policy.min_interval = std::chrono::seconds(3600);  // 1hr to avoid interference
            
            runtime_ = DurableRuntime::open(paths_, policy);
            ASSERT_NE(runtime_, nullptr);
            
            DurableContext ctx{
                .ot = runtime_->ot(),
                .alloc = runtime_->allocator(),
                .coord = runtime_->coordinator(),
                .mvcc = runtime_->mvcc(),
                .runtime = *runtime_
            };
            
            std::vector<std::thread> writers;
            std::vector<std::vector<double>> latencies(num_writers);
            std::atomic<size_t> total_commits(0);
            auto payload = generatePayload(NODE_SIZE);
            
            auto start = high_resolution_clock::now();
            
            for (int w = 0; w < num_writers; ++w) {
                writers.emplace_back([&, w]() {
                    DurableStore store(ctx, "writer_" + std::to_string(w));
                    
                    for (size_t i = 0; i < COMMITS_PER_WRITER; ++i) {
                        auto commit_start = high_resolution_clock::now();
                        
                        // Allocate and write a node
                        auto alloc = store.allocate_node(NODE_SIZE, NodeKind::Leaf);
                        memcpy(alloc.writable, payload.data(), NODE_SIZE);
                        store.publish_node(alloc.id, alloc.writable, NODE_SIZE);
                        
                        // Commit
                        store.commit(w * 1000 + i + 1);
                        
                        auto commit_end = high_resolution_clock::now();
                        auto duration = duration_cast<microseconds>(commit_end - commit_start);
                        latencies[w].push_back(duration.count() / 1000.0);  // ms
                        total_commits++;
                    }
                });
            }
            
            for (auto& t : writers) {
                t.join();
            }
            
            auto end = high_resolution_clock::now();
            auto total_duration = duration_cast<milliseconds>(end - start);
            
            // Calculate statistics
            std::vector<double> all_latencies;
            for (const auto& writer_lats : latencies) {
                all_latencies.insert(all_latencies.end(), writer_lats.begin(), writer_lats.end());
            }
            std::sort(all_latencies.begin(), all_latencies.end());
            
            double p50 = all_latencies[all_latencies.size() / 2];
            double p99 = all_latencies[size_t(all_latencies.size() * 0.99)];
            double commits_per_sec = (total_commits * 1000.0) / total_duration.count();
            
            // Group commit should improve throughput with multiple writers
            bool efficient = (interval_ms > 0 && num_writers > 1) ? 
                            (commits_per_sec > num_writers * 100) : true;
            const char* status = efficient ? "✓ GOOD" : "⚠ SLOW";
            
            std::cout << std::setw(7) << num_writers << " | "
                      << std::setw(8) << (interval_ms == 0 ? "disabled" : std::to_string(interval_ms) + "ms") << " | "
                      << std::fixed << std::setprecision(0)
                      << std::setw(11) << commits_per_sec << " | "
                      << std::setprecision(2)
                      << std::setw(11) << p50 << " | "
                      << std::setw(11) << p99 << " | "
                      << status << "\n";
            
            runtime_.reset();
        }
    }
    
    std::cout << "\n💡 Group commit should batch multiple writers into single fsync\n";
}

TEST_F(CheckpointCoordinatorBenchmark, CheckpointTriggerPerformance) {
    printSeparator("Checkpoint Trigger Performance");
    
    std::cout << "\nMeasuring checkpoint trigger responsiveness:\n\n";
    std::cout << "Trigger    | Threshold | Time to CP | CP Duration | Replay MB | Status\n";
    std::cout << "-----------|-----------|------------|-------------|-----------|--------\n";
    
    struct TriggerTest {
        const char* name;
        CheckpointPolicy policy;
        std::function<void(DurableStore&, size_t&)> workload;
    };
    
    TriggerTest tests[] = {
        {"Bytes", 
         {.max_replay_bytes = 10 * 1024 * 1024,  // 10MB trigger
          .min_interval = std::chrono::seconds(0)},
         [](DurableStore& store, size_t& epoch) {
             // Write 12MB to trigger checkpoint
             const size_t NODE_SIZE = 4096;
             const size_t NUM_NODES = 3072;  // 12MB total
             std::vector<uint8_t> data(NODE_SIZE, 0x42);
             
             for (size_t i = 0; i < NUM_NODES; ++i) {
                 auto alloc = store.allocate_node(NODE_SIZE, NodeKind::Leaf);
                 memcpy(alloc.writable, data.data(), NODE_SIZE);
                 store.publish_node(alloc.id, alloc.writable, NODE_SIZE);
                 if (i % 10 == 0) {
                     store.commit(++epoch);
                 }
             }
         }},
        
        {"Epochs",
         {.max_replay_epochs = 100,
          .max_replay_bytes = 1024 * 1024 * 1024,  // 1GB (won't hit)
          .min_interval = std::chrono::seconds(0)},
         [](DurableStore& store, size_t& epoch) {
             // Create 120 epochs to trigger checkpoint
             const size_t NODE_SIZE = 1024;
             std::vector<uint8_t> data(NODE_SIZE, 0x43);
             
             for (size_t i = 0; i < 120; ++i) {
                 auto alloc = store.allocate_node(NODE_SIZE, NodeKind::Leaf);
                 memcpy(alloc.writable, data.data(), NODE_SIZE);
                 store.publish_node(alloc.id, alloc.writable, NODE_SIZE);
                 store.commit(++epoch);
             }
         }},
        
        {"Age",
         {.max_age = std::chrono::seconds(2),  // 2 second trigger
          .max_replay_bytes = 1024 * 1024 * 1024,
          .max_replay_epochs = 100000,
          .min_interval = std::chrono::seconds(0)},
         [](DurableStore& store, size_t& epoch) {
             // Write some data then wait for age trigger
             const size_t NODE_SIZE = 4096;
             std::vector<uint8_t> data(NODE_SIZE, 0x44);
             
             for (size_t i = 0; i < 10; ++i) {
                 auto alloc = store.allocate_node(NODE_SIZE, NodeKind::Leaf);
                 memcpy(alloc.writable, data.data(), NODE_SIZE);
                 store.publish_node(alloc.id, alloc.writable, NODE_SIZE);
                 store.commit(++epoch);
             }
             
             // Wait for age trigger
             std::this_thread::sleep_for(std::chrono::seconds(3));
         }}
    };
    
    for (const auto& test : tests) {
        runtime_ = DurableRuntime::open(paths_, test.policy);
        ASSERT_NE(runtime_, nullptr);
        
        DurableContext ctx{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        };
        
        DurableStore store(ctx, "test", DurabilityPolicy{});
        
        auto initial_stats = runtime_->coordinator().stats();
        auto start = high_resolution_clock::now();
        
        size_t epoch = 1;
        test.workload(store, epoch);
        
        // Wait for checkpoint to happen
        auto deadline = start + std::chrono::seconds(10);
        while (high_resolution_clock::now() < deadline) {
            auto stats = runtime_->coordinator().stats();
            if (stats.checkpoints_written > initial_stats.checkpoints_written) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        auto end = high_resolution_clock::now();
        auto final_stats = runtime_->coordinator().stats();
        
        bool checkpoint_triggered = (final_stats.checkpoints_written > initial_stats.checkpoints_written);
        auto trigger_time = duration_cast<milliseconds>(end - start);
        
        const char* status = checkpoint_triggered ? "✓ PASS" : "✗ FAIL";
        
        std::cout << std::setw(10) << test.name << " | "
                  << std::setw(9) << 
                     (strcmp(test.name, "Bytes") == 0 ? "10MB" :
                      strcmp(test.name, "Epochs") == 0 ? "100" : "2s") << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(10) << trigger_time.count() << "ms | "
                  << std::setw(11) << final_stats.last_ckpt_ms.count() << "ms | "
                  << std::setprecision(1)
                  << std::setw(9) << (final_stats.last_replay_bytes / (1024.0 * 1024.0)) << " | "
                  << status << "\n";
        
        runtime_.reset();
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }
    
    std::cout << "\n💡 Checkpoints should trigger promptly when thresholds are exceeded\n";
}

TEST_F(CheckpointCoordinatorBenchmark, LogRotationPerformance) {
    printSeparator("Log Rotation Performance");
    
    std::cout << "\nMeasuring log rotation efficiency:\n\n";
    std::cout << "Log Size | Rotation Time | New Log Ready | GC Cleanup | Status\n";
    std::cout << "---------|---------------|---------------|------------|--------\n";
    
    const size_t LOG_SIZES[] = {10, 50, 100, 256};  // MB
    
    for (size_t log_mb : LOG_SIZES) {
        CheckpointPolicy policy;
        policy.rotate_bytes = log_mb * 1024 * 1024;
        policy.gc_on_rotate = true;
        policy.max_replay_bytes = 10ULL * 1024 * 1024 * 1024;  // 10GB (won't checkpoint)
        policy.min_interval = std::chrono::seconds(3600);
        
        runtime_ = DurableRuntime::open(paths_, policy);
        ASSERT_NE(runtime_, nullptr);
        
        DurableContext ctx{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        };
        
        DurableStore store(ctx, "test");
        
        // Write data to exceed rotation threshold
        const size_t NODE_SIZE = 4096;
        const size_t NODES_NEEDED = (log_mb * 1024 * 1024) / NODE_SIZE + 100;
        std::vector<uint8_t> data(NODE_SIZE, 0x55);
        
        auto initial_stats = runtime_->coordinator().stats();
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NODES_NEEDED; ++i) {
            auto alloc = store.allocate_node(NODE_SIZE, NodeKind::Leaf);
            memcpy(alloc.writable, data.data(), NODE_SIZE);
            store.publish_node(alloc.id, alloc.writable, NODE_SIZE);
            
            if (i % 10 == 0) {
                store.commit(i / 10 + 1);
            }
        }
        
        // Wait for rotation
        auto deadline = start + std::chrono::seconds(10);
        while (high_resolution_clock::now() < deadline) {
            auto stats = runtime_->coordinator().stats();
            if (stats.rotations > initial_stats.rotations) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        
        auto end = high_resolution_clock::now();
        auto final_stats = runtime_->coordinator().stats();
        
        bool rotated = (final_stats.rotations > initial_stats.rotations);
        auto rotation_time = duration_cast<milliseconds>(end - start);
        
        // Verify new log is ready for writes
        bool new_log_ready = runtime_->coordinator().get_active_log() != nullptr;
        
        // Check if GC cleaned up old logs
        bool gc_ran = (final_stats.pruned_logs > initial_stats.pruned_logs);
        
        const char* status = (rotated && new_log_ready) ? "✓ PASS" : "✗ FAIL";
        
        std::cout << std::setw(7) << log_mb << "MB | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(13) << rotation_time.count() << "ms | "
                  << std::setw(13) << (new_log_ready ? "Yes" : "No") << " | "
                  << std::setw(10) << (gc_ran ? "Yes" : "No") << " | "
                  << status << "\n";
        
        runtime_.reset();
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }
    
    std::cout << "\n💡 Log rotation should be seamless with minimal write interruption\n";
}

TEST_F(CheckpointCoordinatorBenchmark, Summary) {
    printSeparator("Checkpoint Coordinator Performance Summary");
    
    std::cout << "\n📊 Validating critical coordinator operations...\n\n";
    
    // Test group commit efficiency
    {
        CheckpointPolicy policy;
        policy.group_commit_interval_ms = 5;  // 5ms batching
        policy.max_replay_bytes = 1024 * 1024 * 1024;
        policy.min_interval = std::chrono::seconds(3600);
        
        runtime_ = DurableRuntime::open(paths_, policy);
        ASSERT_NE(runtime_, nullptr);
        
        DurableContext ctx{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        };
        
        const int NUM_WRITERS = 4;
        const size_t COMMITS_PER_WRITER = 100;
        std::atomic<size_t> total_commits(0);
        
        auto start = high_resolution_clock::now();
        
        std::vector<std::thread> writers;
        for (int w = 0; w < NUM_WRITERS; ++w) {
            writers.emplace_back([&, w]() {
                DurableStore store(ctx, "writer_" + std::to_string(w));
                std::vector<uint8_t> data(1024, w);
                
                for (size_t i = 0; i < COMMITS_PER_WRITER; ++i) {
                    auto alloc = store.allocate_node(1024, NodeKind::Leaf);
                    memcpy(alloc.writable, data.data(), 1024);
                    store.publish_node(alloc.id, alloc.writable, 1024);
                    store.commit(w * 1000 + i + 1);
                    total_commits++;
                }
            });
        }
        
        for (auto& t : writers) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        double commits_per_sec = (total_commits * 1000.0) / duration.count();
        
        std::cout << "Group Commit:\n";
        std::cout << "  • " << NUM_WRITERS << " writers, " << COMMITS_PER_WRITER << " commits each\n";
        std::cout << "  • " << std::fixed << std::setprecision(0)
                  << commits_per_sec << " commits/sec\n";
        std::cout << "  • Target >1000 commits/sec: "
                  << (commits_per_sec > 1000 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    runtime_.reset();
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    
    // Test checkpoint trigger responsiveness
    {
        CheckpointPolicy policy;
        policy.max_replay_bytes = 10 * 1024 * 1024;  // 10MB
        policy.min_interval = std::chrono::seconds(0);
        
        runtime_ = DurableRuntime::open(paths_, policy);
        ASSERT_NE(runtime_, nullptr);
        
        DurableContext ctx{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        };
        
        DurableStore store(ctx, "test");
        
        auto initial_stats = runtime_->coordinator().stats();
        auto start = high_resolution_clock::now();
        
        // Write 12MB to trigger checkpoint
        std::vector<uint8_t> data(4096, 0x42);
        for (size_t i = 0; i < 3072; ++i) {
            auto alloc = store.allocate_node(4096, NodeKind::Leaf);
            memcpy(alloc.writable, data.data(), 4096);
            store.publish_node(alloc.id, alloc.writable, 4096);
            if (i % 10 == 0) {
                store.commit(i / 10 + 1);
            }
        }
        
        // Wait for checkpoint
        auto deadline = start + std::chrono::seconds(5);
        while (high_resolution_clock::now() < deadline) {
            auto stats = runtime_->coordinator().stats();
            if (stats.checkpoints_written > initial_stats.checkpoints_written) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        
        auto end = high_resolution_clock::now();
        auto trigger_time = duration_cast<milliseconds>(end - start);
        auto final_stats = runtime_->coordinator().stats();
        
        bool triggered = (final_stats.checkpoints_written > initial_stats.checkpoints_written);
        
        std::cout << "\nCheckpoint Triggering:\n";
        std::cout << "  • Wrote 12MB with 10MB trigger threshold\n";
        std::cout << "  • Trigger time: " << trigger_time.count() << "ms\n";
        std::cout << "  • Target <1000ms: "
                  << (trigger_time.count() < 1000 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    runtime_.reset();
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    
    // Test log rotation
    {
        CheckpointPolicy policy;
        policy.rotate_bytes = 50 * 1024 * 1024;  // 50MB
        policy.max_replay_bytes = 10 * 1024 * 1024 * 1024;
        policy.min_interval = std::chrono::seconds(3600);
        
        runtime_ = DurableRuntime::open(paths_, policy);
        ASSERT_NE(runtime_, nullptr);
        
        DurableContext ctx{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        };
        
        DurableStore store(ctx, "test");
        
        auto initial_stats = runtime_->coordinator().stats();
        auto start = high_resolution_clock::now();
        
        // Write 55MB to trigger rotation
        std::vector<uint8_t> data(4096, 0x55);
        for (size_t i = 0; i < 14080; ++i) {  // ~55MB
            auto alloc = store.allocate_node(4096, NodeKind::Leaf);
            memcpy(alloc.writable, data.data(), 4096);
            store.publish_node(alloc.id, alloc.writable, 4096);
            if (i % 10 == 0) {
                store.commit(i / 10 + 1);
            }
        }
        
        // Wait for rotation
        auto deadline = start + std::chrono::seconds(5);
        while (high_resolution_clock::now() < deadline) {
            auto stats = runtime_->coordinator().stats();
            if (stats.rotations > initial_stats.rotations) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        
        auto end = high_resolution_clock::now();
        auto rotation_time = duration_cast<milliseconds>(end - start);
        auto final_stats = runtime_->coordinator().stats();
        
        bool rotated = (final_stats.rotations > initial_stats.rotations);
        
        std::cout << "\nLog Rotation:\n";
        std::cout << "  • Wrote 55MB with 50MB rotation threshold\n";
        std::cout << "  • Rotation time: " << rotation_time.count() << "ms\n";
        std::cout << "  • Target <500ms: "
                  << (rotation_time.count() < 500 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    std::cout << "\n🎯 Coordinator Performance Targets:\n";
    std::cout << "  ✓ Group Commit: >1000 commits/sec with batching\n";
    std::cout << "  ✓ Checkpoint Trigger: <1s response time\n";
    std::cout << "  ✓ Log Rotation: <500ms seamless rotation\n";
    std::cout << "  ✓ GC: Automatic cleanup of old logs\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}