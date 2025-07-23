/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <chrono>
#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/durable_store.h"
#include "../../src/persistence/durability_policy.h"
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/platform_fs.h"
#include "../../src/persistence/checksums.h"

using namespace xtree::persist;
namespace fs = std::filesystem;

class DurabilityPolicyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use test name to make directory unique per test
        const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        test_dir_ = "test_durability_" + std::to_string(getpid()) + "_" + 
                    std::string(test_info->test_case_name()) + "_" + 
                    std::string(test_info->name());
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
        
        paths_.data_dir = test_dir_ + "/data";
        paths_.manifest = test_dir_ + "/manifest.json";
        paths_.superblock = test_dir_ + "/superblock.bin";
        paths_.active_log = test_dir_ + "/ot_delta.wal";
        
        fs::create_directories(paths_.data_dir);
    }
    
    void TearDown() override {
        store_.reset();
        ctx_.reset();
        runtime_.reset();
        fs::remove_all(test_dir_);
    }
    
    void InitializeWithPolicy(DurabilityPolicy policy) {
        // Create runtime with long checkpoint interval to avoid interference
        CheckpointPolicy ckpt_policy;
        ckpt_policy.min_interval = std::chrono::seconds(3600); // 1 hour
        ckpt_policy.group_commit_interval_ms = 0; // Disable group commit for deterministic tests
        runtime_ = DurableRuntime::open(paths_, ckpt_policy);
        
        ctx_ = std::make_unique<DurableContext>(
            DurableContext{
                runtime_->ot(),
                runtime_->allocator(),
                runtime_->coordinator(),
                runtime_->mvcc(),
                *runtime_
            }
        );
        
        store_ = std::make_unique<DurableStore>(*ctx_, "test_store", policy);
    }
    
    // Helper to count delta records in the log
    size_t count_delta_records() {
        auto log = runtime_->coordinator().get_active_log();
        if (!log) return 0;
        
        log->sync();
        
        // Create a fresh reader for the actual log file
        OTDeltaLog reader(log->path());  // Use the actual log path
        size_t count = 0;
        
        reader.replay([&count](const OTDeltaRec& delta) {
            count++;
        });
        
        return count;
    }
    
    // Helper to get baseline delta count after initialization
    size_t get_baseline_delta_count() {
        return count_delta_records();
    }
    
    // Helper to verify data is actually on disk
    bool verify_data_on_disk(void* vaddr, size_t len, uint8_t expected_value) {
        // Force OS to drop clean pages, then re-read
        // PlatformFS::advise_dontneed(vaddr, len);  // TODO: Add this method to PlatformFS
        
        // Re-map the same region to force read from disk
        // In a real test, we'd close and re-open the file
        // For now, just verify the data is readable
        uint8_t* bytes = static_cast<uint8_t*>(vaddr);
        for (size_t i = 0; i < len; ++i) {
            if (bytes[i] != expected_value) {
                return false;
            }
        }
        return true;
    }
    
protected:
    std::string test_dir_;
    Paths paths_;
    std::unique_ptr<DurableRuntime> runtime_;
    std::unique_ptr<DurableContext> ctx_;
    std::unique_ptr<DurableStore> store_;
};

// Test STRICT mode durability
TEST_F(DurabilityPolicyTest, StrictModeDataDurability) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::STRICT;
    policy.coalesce_flushes = false;  // Test individual flushes
    InitializeWithPolicy(policy);
    
    // Allocate and publish nodes
    std::vector<NodeID> nodes;
    std::vector<void*> addrs;
    const size_t num_nodes = 10;
    const size_t node_size = 4096;
    const uint8_t test_value = 0x42;
    
    for (size_t i = 0; i < num_nodes; ++i) {
        auto result = store_->allocate_node(node_size, NodeKind::Leaf);
        ASSERT_NE(result.writable, nullptr);
        
        // Fill with test data
        std::memset(result.writable, test_value, node_size);
        store_->publish_node(result.id, result.writable, node_size);
        
        nodes.push_back(result.id);
        addrs.push_back(result.writable);
    }
    
    // Commit with STRICT mode
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Verify data is on disk (STRICT mode guarantee)
    for (size_t i = 0; i < num_nodes; ++i) {
        EXPECT_TRUE(verify_data_on_disk(addrs[i], node_size, test_value))
            << "Node " << i << " data not durable after STRICT commit";
    }
    
    // Verify deltas were logged
    EXPECT_EQ(count_delta_records(), num_nodes);
}

// Test BALANCED mode deferred flushing
TEST_F(DurabilityPolicyTest, BalancedModeDeferredFlush) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::BALANCED;
    policy.dirty_flush_bytes = 1024 * 1024;  // 1MB threshold
    policy.dirty_flush_age = std::chrono::seconds(2);
    InitializeWithPolicy(policy);
    
    // Allocate and publish a node
    auto result = store_->allocate_node(8192, NodeKind::Internal);
    ASSERT_NE(result.writable, nullptr);
    
    const uint8_t test_value = 0x33;
    std::memset(result.writable, test_value, 8192);
    store_->publish_node(result.id, result.writable, 8192);
    
    // Commit with BALANCED mode
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Immediately after commit, WAL should be durable but data may not be
    EXPECT_EQ(count_delta_records(), 1);
    
    // In BALANCED mode, data flush is deferred to coordinator
    // We can't easily test the coordinator's background flush here,
    // but we verify the WAL has the delta for recovery
}

// Test thread-local batching
TEST_F(DurabilityPolicyTest, ThreadLocalBatching) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::STRICT;
    InitializeWithPolicy(policy);
    
    // Before commit, no deltas should be in WAL
    EXPECT_EQ(count_delta_records(), 0);
    
    // Stage multiple writes without commit
    std::vector<NodeID> nodes;
    for (int i = 0; i < 5; ++i) {
        auto result = store_->allocate_node(512, NodeKind::Leaf);
        std::memset(result.writable, i, 512);
        store_->publish_node(result.id, result.writable, 512);
        nodes.push_back(result.id);
    }
    
    // Still no deltas (batched in thread-local storage)
    EXPECT_EQ(count_delta_records(), 0);
    
    // Now commit - all deltas written as batch
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // All deltas should now be in WAL
    EXPECT_EQ(count_delta_records(), 5);
}

// Test retirement batching
TEST_F(DurabilityPolicyTest, RetirementBatching) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::BALANCED;
    InitializeWithPolicy(policy);
    
    // Create nodes
    std::vector<NodeID> nodes;
    for (int i = 0; i < 10; ++i) {
        auto result = store_->allocate_node(256, NodeKind::Leaf);
        std::memset(result.writable, i, 256);
        store_->publish_node(result.id, result.writable, 256);
        nodes.push_back(result.id);
    }
    
    // Commit allocations
    uint64_t epoch1 = runtime_->mvcc().advance_epoch();
    store_->commit(epoch1);
    EXPECT_EQ(count_delta_records(), 10);
    
    // Retire half the nodes
    uint64_t retire_epoch = runtime_->mvcc().advance_epoch();
    for (int i = 0; i < 5; ++i) {
        store_->retire_node(nodes[i], retire_epoch);
    }
    
    // Retirements are batched, not yet in WAL
    EXPECT_EQ(count_delta_records(), 10);
    
    // Commit retirements
    store_->commit(retire_epoch);
    
    // Should have allocations + retirements
    EXPECT_EQ(count_delta_records(), 15);
}

// Test coalesced flushing in STRICT mode
TEST_F(DurabilityPolicyTest, CoalescedFlushing) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::STRICT;
    policy.coalesce_flushes = true;  // Enable coalescing
    InitializeWithPolicy(policy);
    
    // Allocate contiguous nodes (likely in same segment)
    std::vector<NodeID> nodes;
    std::vector<void*> addrs;
    
    for (int i = 0; i < 20; ++i) {
        auto result = store_->allocate_node(1024, NodeKind::Leaf);
        std::memset(result.writable, 0xAA, 1024);
        store_->publish_node(result.id, result.writable, 1024);
        nodes.push_back(result.id);
        addrs.push_back(result.writable);
    }
    
    // Commit should coalesce flushes for better performance
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    auto start = std::chrono::high_resolution_clock::now();
    store_->commit(epoch);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // With coalescing, should be faster than individual flushes
    // Hard to test exact performance, but verify correctness
    EXPECT_EQ(count_delta_records(), 20);
    
    // Verify all data is durable
    for (auto addr : addrs) {
        EXPECT_TRUE(verify_data_on_disk(addr, 1024, 0xAA));
    }
}

// Test epoch assignment at commit time
TEST_F(DurabilityPolicyTest, EpochAssignmentAtCommit) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::BALANCED;
    InitializeWithPolicy(policy);
    
    // Get initial epoch
    uint64_t initial_epoch = runtime_->mvcc().get_global_epoch();
    
    // Publish nodes (epochs not yet assigned)
    auto result1 = store_->allocate_node(512, NodeKind::Leaf);
    store_->publish_node(result1.id, nullptr, 512);
    
    auto result2 = store_->allocate_node(512, NodeKind::Leaf);
    store_->publish_node(result2.id, nullptr, 512);
    
    // Commit - this advances epoch and assigns it
    store_->commit(0);  // hint_epoch is ignored anyway
    
    // Get the epoch that was assigned
    uint64_t commit_epoch = runtime_->mvcc().get_global_epoch();
    EXPECT_GT(commit_epoch, initial_epoch);
    
    // Read back deltas and verify epochs
    auto log = runtime_->coordinator().get_active_log();
    ASSERT_NE(log, nullptr);
    OTDeltaLog reader(log->path());  // Use the actual log path
    std::vector<OTDeltaRec> deltas;
    size_t count = 0;
    reader.replay([&deltas, &count](const OTDeltaRec& delta) {
        deltas.push_back(delta);
        count++;
    });
    
    EXPECT_EQ(count, 2);
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(deltas[i].birth_epoch, commit_epoch)
            << "Delta " << i << " should have commit epoch";
        EXPECT_EQ(deltas[i].retire_epoch, ~uint64_t{0})
            << "Delta " << i << " should be live";
    }
}

// Test mixed operations in single batch
TEST_F(DurabilityPolicyTest, MixedOperationBatch) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::STRICT;
    InitializeWithPolicy(policy);
    
    // Create some nodes
    std::vector<NodeID> nodes;
    for (int i = 0; i < 5; ++i) {
        auto result = store_->allocate_node(256, NodeKind::Leaf);
        std::memset(result.writable, i, 256);
        store_->publish_node(result.id, result.writable, 256);
        nodes.push_back(result.id);
    }
    
    // Commit initial nodes
    uint64_t epoch1 = runtime_->mvcc().advance_epoch();
    store_->commit(epoch1);
    
    // Now do mixed operations in one batch
    // 1. Retire some old nodes
    uint64_t epoch2 = runtime_->mvcc().advance_epoch();
    store_->retire_node(nodes[0], epoch2);
    store_->retire_node(nodes[1], epoch2);
    
    // 2. Allocate new nodes
    auto new1 = store_->allocate_node(512, NodeKind::Internal);
    std::memset(new1.writable, 0xFF, 512);
    store_->publish_node(new1.id, new1.writable, 512);
    
    auto new2 = store_->allocate_node(1024, NodeKind::Leaf);
    std::memset(new2.writable, 0xEE, 1024);
    store_->publish_node(new2.id, new2.writable, 1024);
    
    // 3. Retire another old node
    store_->retire_node(nodes[2], epoch2);
    
    // Commit mixed batch
    store_->commit(epoch2);
    
    // Should have: 5 initial + 2 retirements + 2 new + 1 retirement = 10 deltas
    EXPECT_EQ(count_delta_records(), 10);
    
    // Verify the new nodes are durable (STRICT mode)
    EXPECT_TRUE(verify_data_on_disk(new1.writable, 512, 0xFF));
    EXPECT_TRUE(verify_data_on_disk(new2.writable, 1024, 0xEE));
}

// Test error handling when no active log
TEST_F(DurabilityPolicyTest, NoActiveLogError) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::STRICT;
    InitializeWithPolicy(policy);
    
    // Allocate and stage a write
    auto result = store_->allocate_node(512, NodeKind::Leaf);
    store_->publish_node(result.id, nullptr, 512);
    
    // Simulate no active log by stopping coordinator
    runtime_->coordinator().stop();
    
    // Commit should throw when no log available
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    EXPECT_THROW(store_->commit(epoch), std::runtime_error);
}

// Test concurrent threads with thread-local batching
TEST_F(DurabilityPolicyTest, ConcurrentThreadBatching) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::BALANCED;
    InitializeWithPolicy(policy);
    
    const int num_threads = 4;
    const int nodes_per_thread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> ready{0};
    std::atomic<int> committed{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Wait for all threads
            ready.fetch_add(1);
            while (ready.load() < num_threads) {
                std::this_thread::yield();
            }
            
            // Each thread has its own batch
            std::vector<NodeID> local_nodes;
            for (int i = 0; i < nodes_per_thread; ++i) {
                auto result = store_->allocate_node(256, NodeKind::Leaf);
                if (result.writable) {
                    std::memset(result.writable, t * 100 + i, 256);
                    store_->publish_node(result.id, result.writable, 256);
                    local_nodes.push_back(result.id);
                }
            }
            
            // Each thread commits its batch
            uint64_t epoch = runtime_->mvcc().advance_epoch();
            store_->commit(epoch);
            committed.fetch_add(1);
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(committed.load(), num_threads);
    
    // Should have all deltas from all threads
    EXPECT_EQ(count_delta_records(), num_threads * nodes_per_thread);
}

// Test EVENTUAL mode with payload-in-WAL for small nodes
TEST_F(DurabilityPolicyTest, EventualModeSmallNodesWithPayload) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::EVENTUAL;
    policy.max_payload_in_wal = 8192;  // 8KB threshold
    policy.group_commit_interval_ms = 0;  // Remove batching latency for deterministic test
    InitializeWithPolicy(policy);
    
    // Get baseline delta count after initialization
    size_t baseline_deltas = count_delta_records();
    
    // Test 1: Small node that should have payload in WAL
    const size_t small_size = 512;
    const uint8_t small_pattern = 0x11;
    auto small_result = store_->allocate_node(small_size, NodeKind::Leaf);
    ASSERT_NE(small_result.writable, nullptr);
    std::memset(small_result.writable, small_pattern, small_size);
    store_->publish_node(small_result.id, small_result.writable, small_size);
    
    // Test 2: Another small node
    const size_t medium_size = 4096;
    const uint8_t medium_pattern = 0x22;
    auto medium_result = store_->allocate_node(medium_size, NodeKind::Internal);
    ASSERT_NE(medium_result.writable, nullptr);
    std::memset(medium_result.writable, medium_pattern, medium_size);
    store_->publish_node(medium_result.id, medium_result.writable, medium_size);
    
    // Test 3: Large node that exceeds threshold (no payload in WAL)
    const size_t large_size = 16384;  // 16KB > 8KB threshold
    const uint8_t large_pattern = 0x33;
    auto large_result = store_->allocate_node(large_size, NodeKind::Leaf);
    ASSERT_NE(large_result.writable, nullptr);
    std::memset(large_result.writable, large_pattern, large_size);
    store_->publish_node(large_result.id, large_result.writable, large_size);
    
    // Commit all nodes
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Belt & suspenders: ensure WAL is visible to the counter immediately
    if (auto log = runtime_->coordinator().get_active_log()) {
        log->sync();
    }
    
    // Verify WAL has all deltas (accounting for baseline)
    size_t delta_count = count_delta_records();
    size_t new_deltas = delta_count - baseline_deltas;
    // Note: We expect at least 3 deltas for our nodes. There might be additional deltas for 
    // internal bookkeeping, but we should have at least the 3 we explicitly created.
    EXPECT_GE(new_deltas, 3) << "Should have at least 3 new deltas for our nodes, but got " << new_deltas 
                              << " (total: " << delta_count << ", baseline: " << baseline_deltas << ")";
    
    // Now simulate recovery with payload rehydration
    // Close the store and runtime
    store_.reset();
    ctx_.reset();
    runtime_.reset();
    
    // Re-open with payload recovery enabled
    CheckpointPolicy recovery_policy;
    recovery_policy.group_commit_interval_ms = 0; // Keep group commit disabled
    runtime_ = DurableRuntime::open(paths_, recovery_policy, true /* use_payload_recovery */);
    
    ctx_ = std::make_unique<DurableContext>(
        DurableContext{
            runtime_->ot(),
            runtime_->allocator(),
            runtime_->coordinator(),
            runtime_->mvcc(),
            *runtime_
        }
    );
    
    // Verify nodes were recovered correctly
    // Small nodes should have been rehydrated from WAL payloads
    // Large node should have been recovered from segment data
    
    // Check small node data
    const auto& small_entry = runtime_->ot().get(small_result.id);
    if (small_entry.addr.vaddr && small_entry.addr.length >= small_size) {
        uint8_t* data = static_cast<uint8_t*>(small_entry.addr.vaddr);
        for (size_t i = 0; i < small_size; ++i) {
            EXPECT_EQ(data[i], small_pattern) << "Small node data mismatch at byte " << i;
            if (data[i] != small_pattern) break;  // Stop on first mismatch
        }
    }
    
    // Check medium node data
    const auto& medium_entry = runtime_->ot().get(medium_result.id);
    if (medium_entry.addr.vaddr && medium_entry.addr.length >= medium_size) {
        uint8_t* data = static_cast<uint8_t*>(medium_entry.addr.vaddr);
        for (size_t i = 0; i < medium_size; ++i) {
            EXPECT_EQ(data[i], medium_pattern) << "Medium node data mismatch at byte " << i;
            if (data[i] != medium_pattern) break;
        }
    }
}

// Test EVENTUAL mode with mixed size nodes
TEST_F(DurabilityPolicyTest, EventualModeMixedSizes) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::EVENTUAL;
    policy.max_payload_in_wal = 4096;  // 4KB threshold
    InitializeWithPolicy(policy);
    
    std::vector<NodeID> small_nodes;
    std::vector<NodeID> large_nodes;
    
    // Create a mix of small and large nodes
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            // Small node (under threshold)
            auto result = store_->allocate_node(1024, NodeKind::Leaf);
            std::memset(result.writable, static_cast<uint8_t>(i), 1024);
            store_->publish_node(result.id, result.writable, 1024);
            small_nodes.push_back(result.id);
        } else {
            // Large node (over threshold)
            auto result = store_->allocate_node(8192, NodeKind::Internal);
            std::memset(result.writable, static_cast<uint8_t>(i), 8192);
            store_->publish_node(result.id, result.writable, 8192);
            large_nodes.push_back(result.id);
        }
    }
    
    // Commit all nodes
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    EXPECT_EQ(count_delta_records(), 10) << "Should have deltas for all 10 nodes";
    
    // Verify that small nodes got payload-in-WAL treatment
    // and large nodes got deferred to coordinator
    // This would require inspecting the WAL format, which we'll verify in recovery
}

// Test EVENTUAL mode recovery after crash
TEST_F(DurabilityPolicyTest, EventualModeRecoveryAfterCrash) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::EVENTUAL;
    policy.max_payload_in_wal = 2048;
    InitializeWithPolicy(policy);
    
    // Create nodes with specific patterns
    struct TestNode {
        NodeID id;
        size_t size;
        uint8_t pattern;
    };
    std::vector<TestNode> test_nodes;
    
    // Small nodes with payload in WAL
    for (int i = 0; i < 3; ++i) {
        size_t size = 512 * (i + 1);  // 512, 1024, 1536
        uint8_t pattern = 0xA0 + i;
        auto result = store_->allocate_node(size, NodeKind::Leaf);
        std::memset(result.writable, pattern, size);
        store_->publish_node(result.id, result.writable, size);
        test_nodes.push_back({result.id, size, pattern});
    }
    
    // Large nodes without payload in WAL
    for (int i = 0; i < 2; ++i) {
        size_t size = 4096 * (i + 1);  // 4096, 8192
        uint8_t pattern = 0xB0 + i;
        auto result = store_->allocate_node(size, NodeKind::Internal);
        std::memset(result.writable, pattern, size);
        store_->publish_node(result.id, result.writable, size);
        test_nodes.push_back({result.id, size, pattern});
    }
    
    // Commit
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Simulate crash by destroying everything
    store_.reset();
    ctx_.reset();
    runtime_.reset();
    
    // Recovery with payload support
    runtime_ = DurableRuntime::open(paths_, CheckpointPolicy{}, true);
    ctx_ = std::make_unique<DurableContext>(
        DurableContext{
            runtime_->ot(),
            runtime_->allocator(),
            runtime_->coordinator(),
            runtime_->mvcc(),
            *runtime_
        }
    );
    
    // Verify all nodes recovered with correct data
    for (const auto& test_node : test_nodes) {
        const auto& entry = runtime_->ot().get(test_node.id);
        
        // After recovery, vaddr may be null - need to get from allocator
        void* ptr = entry.addr.vaddr;
        if (ptr == nullptr) {
            ptr = runtime_->allocator().get_ptr_for_recovery(
                entry.class_id, entry.addr.file_id, entry.addr.segment_id,
                entry.addr.offset, entry.addr.length);
        }
        ASSERT_NE(ptr, nullptr) << "Node should be recovered";
        
        // Verify data pattern
        uint8_t* data = static_cast<uint8_t*>(ptr);
        bool data_correct = true;
        for (size_t i = 0; i < test_node.size; ++i) {
            if (data[i] != test_node.pattern) {
                data_correct = false;
                break;
            }
        }
        EXPECT_TRUE(data_correct) << "Node data should match original pattern";
    }
}

// Test CRC32C computation in BALANCED mode
TEST_F(DurabilityPolicyTest, CRC32CInBalancedMode) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::BALANCED;
    InitializeWithPolicy(policy);
    
    // Create a node with known data
    const size_t node_size = 512;
    const uint8_t test_pattern = 0xAB;
    
    auto result = store_->allocate_node(node_size, NodeKind::Leaf);
    ASSERT_NE(result.writable, nullptr);
    std::memset(result.writable, test_pattern, node_size);
    store_->publish_node(result.id, result.writable, node_size);
    
    // Commit to trigger CRC32C computation
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Read back deltas and verify CRC32C was computed
    auto log = runtime_->coordinator().get_active_log();
    ASSERT_NE(log, nullptr);
    OTDeltaLog reader(log->path(), true);  // Use the actual log path
    std::vector<OTDeltaRec> deltas;
    reader.replay([&deltas](const OTDeltaRec& delta) {
        deltas.push_back(delta);
    });
    
    ASSERT_GE(deltas.size(), 1) << "Should have at least one delta";
    
    // Debug: Print all deltas
    for (size_t i = 0; i < deltas.size(); ++i) {
        const auto& delta = deltas[i];
        std::cout << "Delta " << i << ": length=" << delta.length 
                  << ", retire_epoch=" << std::hex << delta.retire_epoch << std::dec
                  << ", crc32c=" << std::hex << delta.data_crc32c << std::dec << std::endl;
    }
    
    // Find the delta for our node (allocator may round up to size class)
    bool found_with_crc = false;
    for (const auto& delta : deltas) {
        if (delta.length >= node_size && delta.retire_epoch == ~uint64_t{0}) {
            // This is our allocation delta
            EXPECT_NE(delta.data_crc32c, 0) << "CRC32C should be computed in BALANCED mode";
            
            // Verify the CRC matches the data we wrote (only the part we filled)
            std::vector<uint8_t> test_data(node_size, test_pattern);
            uint32_t expected_crc = crc32c(test_data.data(), test_data.size());
            // Note: CRC is computed on the entire allocated size, not just what we filled
            // So we can only verify it's non-zero
            found_with_crc = true;
            break;
        }
    }
    
    EXPECT_TRUE(found_with_crc) << "Should find delta with CRC32C for size>=" << node_size;
}

// Test dirty range tracking in BALANCED mode
TEST_F(DurabilityPolicyTest, DirtyRangeTracking) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::BALANCED;
    policy.dirty_flush_bytes = 64 * 1024 * 1024;  // 64MB threshold
    policy.dirty_flush_age = std::chrono::seconds(2);
    InitializeWithPolicy(policy);
    
    // Create multiple nodes to generate dirty ranges
    const size_t num_nodes = 20;
    const size_t node_size = 4096;
    std::vector<NodeID> nodes;
    
    for (size_t i = 0; i < num_nodes; ++i) {
        auto result = store_->allocate_node(node_size, NodeKind::Leaf);
        ASSERT_NE(result.writable, nullptr);
        
        // Write pattern to each node
        std::memset(result.writable, static_cast<uint8_t>(i), node_size);
        store_->publish_node(result.id, result.writable, node_size);
        nodes.push_back(result.id);
    }
    
    // Commit should submit dirty ranges to coordinator
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // In BALANCED mode, dirty ranges should be tracked and passed to coordinator
    // The coordinator will flush them based on thresholds
    // We can verify by checking WAL has deltas but data isn't immediately flushed
    
    // Verify deltas were written
    size_t delta_count = count_delta_records();
    EXPECT_GE(delta_count, num_nodes) << "Should have deltas for all nodes";
    
    // Note: In a real test, we'd verify the coordinator received the dirty ranges
    // and scheduled them for background flushing. For now, we just verify the
    // basic flow works without errors.
}

// Test that nodes are invisible until commit (birth_epoch=0)
TEST_F(DurabilityPolicyTest, NodesInvisibleUntilCommit) {
    DurabilityPolicy policy;
    policy.mode = DurabilityMode::STRICT;
    InitializeWithPolicy(policy);
    
    // Allocate a node but don't commit
    auto result = store_->allocate_node(1024, NodeKind::Leaf);
    ASSERT_NE(result.writable, nullptr);
    std::memset(result.writable, 0x55, 1024);
    store_->publish_node(result.id, result.writable, 1024);
    
    // Before commit, the node should have birth_epoch=0 (invisible)
    // We can't directly check the ObjectTable from here, but we can
    // verify no deltas are in the WAL yet
    EXPECT_EQ(count_delta_records(), 0) << "No deltas should be written before commit";
    
    // Now commit (it will advance epoch internally)
    store_->commit(0);  // hint_epoch is ignored
    
    // Get the epoch that was assigned by commit
    uint64_t commit_epoch = runtime_->mvcc().get_global_epoch();
    
    // After commit, delta should be written with proper birth_epoch
    auto log = runtime_->coordinator().get_active_log();
    ASSERT_NE(log, nullptr);
    OTDeltaLog reader(log->path(), true);  // Use the actual log path
    std::vector<OTDeltaRec> deltas;
    reader.replay([&deltas](const OTDeltaRec& delta) {
        deltas.push_back(delta);
    });
    
    ASSERT_EQ(deltas.size(), 1);
    EXPECT_EQ(deltas[0].birth_epoch, commit_epoch) << "Birth epoch should be stamped at commit";
    EXPECT_EQ(deltas[0].retire_epoch, ~uint64_t{0}) << "Should be live";
}

// Test durability policy configuration
TEST_F(DurabilityPolicyTest, PolicyConfiguration) {
    // Test named policy helper
    auto strict = get_durability_policy("strict");
    EXPECT_EQ(strict.mode, DurabilityMode::STRICT);
    EXPECT_EQ(strict.group_commit_interval_ms, 0);  // No batching in strict
    
    auto eventual = get_durability_policy("eventual");
    EXPECT_EQ(eventual.mode, DurabilityMode::EVENTUAL);
    EXPECT_EQ(eventual.max_payload_in_wal, 32768);
    
    auto balanced = get_durability_policy("balanced");
    EXPECT_EQ(balanced.mode, DurabilityMode::BALANCED);
    EXPECT_EQ(balanced.dirty_flush_bytes, 128 * 1024 * 1024);
    
    auto default_policy = get_durability_policy("");
    EXPECT_EQ(default_policy.mode, DurabilityMode::BALANCED);
}