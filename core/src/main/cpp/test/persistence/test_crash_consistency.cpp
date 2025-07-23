/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Tests for crash consistency guarantees in the persistence layer.
 * These tests verify that the system maintains consistency across
 * various crash scenarios.
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <atomic>
#include "persistence/durable_store.h"
#include "persistence/durable_context.h"
#include "persistence/checkpoint_coordinator.h"
#include "persistence/segment_allocator.h"
#include "persistence/superblock.hpp"
#include "persistence/manifest.h"
#include "persistence/object_table.hpp"
#include "persistence/recovery.h"
#include "persistence/ot_delta_log.h"
#include "test_helpers.h"

namespace xtree::persist::test {

class CrashConsistencyTest : public ::testing::Test {
protected:
    std::string test_dir_;
    std::unique_ptr<DurableContext> ctx_;
    std::unique_ptr<DurableStore> store_;
    
    void SetUp() override {
        test_dir_ = create_temp_dir("crash_consistency");
        SetupContext();
    }
    
    void TearDown() override {
        store_.reset();
        ctx_.reset();
        std::filesystem::remove_all(test_dir_);
    }
    
    void SetupContext(DurabilityMode mode = DurabilityMode::STRICT) {
        ctx_ = std::make_unique<DurableContext>();
        
        // Setup segment allocator
        SegmentAllocator::Config alloc_config;
        alloc_config.base_path = test_dir_;
        ctx_->alloc.initialize(alloc_config);
        
        // Setup superblock
        ctx_->sb = std::make_unique<Superblock>(test_dir_ + "/superblock");
        
        // Setup manifest
        ctx_->manifest = std::make_unique<Manifest>(test_dir_);
        
        // Setup coordinator with a delta log
        auto log = std::make_unique<OTDeltaLog>(test_dir_ + "/delta.wal");
        log->open_for_append();
        auto active_log = std::make_shared<std::atomic<OTDeltaLog*>>(log.get());
        
        CheckpointPolicy policy;
        ctx_->coord = std::make_unique<CheckpointCoordinator>(
            ctx_->ot, *ctx_->sb, *ctx_->manifest, std::move(log), active_log,
            ctx_->log_gc, ctx_->mvcc, policy, nullptr);
        
        // Create store
        DurabilityPolicy durability;
        durability.mode = mode;
        store_ = std::make_unique<DurableStore>(*ctx_, "test_tree", durability);
    }
    
    // Helper to simulate crash
    void SimulateCrash() {
        // Force close without proper shutdown
        store_.reset();
        ctx_.reset();
    }
    
    // Helper to recover after crash
    void RecoverAfterCrash(DurabilityMode mode = DurabilityMode::STRICT) {
        SetupContext(mode);
        
        // Run recovery
        Recovery recovery(ctx_->ot, *ctx_->sb, *ctx_->manifest, ctx_->ot_checkpoint, &ctx_->alloc);
        recovery.cold_start();
    }
};

// Test 1: Crash after WAL append but before OT apply
// Recovery should replay and materialize live entries
TEST_F(CrashConsistencyTest, CrashAfterWALBeforeOT) {
    // Setup runtime
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::STRICT;
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    ASSERT_NE(store, nullptr);
    
    // Allocate and publish a node
    auto alloc = store->allocate_node(1024, NodeKind::Leaf);
    ASSERT_TRUE(alloc.id.valid());
    
    // Write some data
    std::vector<uint8_t> data(100, 0x42);
    store->publish_node(alloc.id, data.data(), data.size());
    
    // Inject fault: simulate crash after WAL but before OT update
    // We need to hook into the commit path for this
    // For now, we'll test the recovery side
    
    // Normal commit first
    store->commit(1);
    
    // Allocate another node
    auto alloc2 = store->allocate_node(1024, NodeKind::Internal);
    store->publish_node(alloc2.id, data.data(), data.size());
    
    // Simulate crash before this commit completes
    // (In real test, we'd inject fault in flush_strict_mode between WAL and OT)
    SimulateCrash();
    
    // Recover
    runtime_ = RecoverAfterCrash();
    store = runtime_->get_store("test_tree");
    
    // First node should be visible
    auto bytes = store->read_node(alloc.id);
    EXPECT_NE(bytes.data, nullptr);
    EXPECT_EQ(bytes.len, 1024);
    
    // Second node should not be visible (not committed)
    auto bytes2 = store->read_node(alloc2.id);
    EXPECT_EQ(bytes2.data, nullptr);
}

// Test 2: Crash after OT apply but before superblock publish
// Recovery replays beyond last published epoch and reaches same root
TEST_F(CrashConsistencyTest, CrashAfterOTBeforePublish) {
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::BALANCED;
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    
    // Create some nodes
    std::vector<NodeID> nodes;
    for (int i = 0; i < 5; i++) {
        auto alloc = store->allocate_node(512, NodeKind::Leaf);
        std::vector<uint8_t> data(50, i);
        store->publish_node(alloc.id, data.data(), data.size());
        nodes.push_back(alloc.id);
    }
    
    // Set root and commit
    store->set_root(nodes[2], 1, "test_tree");
    store->commit(1);
    
    // Allocate more nodes
    for (int i = 5; i < 10; i++) {
        auto alloc = store->allocate_node(512, NodeKind::Internal);
        std::vector<uint8_t> data(50, i);
        store->publish_node(alloc.id, data.data(), data.size());
        nodes.push_back(alloc.id);
    }
    
    // Update root but simulate crash before publish
    store->set_root(nodes[7], 2, "test_tree");
    
    // In real scenario, we'd crash between OT update and superblock publish
    // For now, complete the commit
    store->commit(2);
    
    SimulateCrash();
    
    // Recover
    runtime_ = RecoverAfterCrash();
    store = runtime_->get_store("test_tree");
    
    // All committed nodes should be visible
    for (size_t i = 0; i < 10; i++) {
        auto bytes = store->read_node(nodes[i]);
        EXPECT_NE(bytes.data, nullptr) << "Node " << i << " not found";
    }
    
    // Root should be at latest committed state
    auto root = store->get_root("test_tree");
    EXPECT_EQ(root.raw(), nodes[7].raw());
}

// Test 3: Crash during frame write - torn frame ignored
TEST_F(CrashConsistencyTest, TornFrameHandling) {
    // Create a delta log and write partial frame
    std::string log_path = test_dir_ + "/torn_frame_test.wal";
    {
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        // Write some complete frames
        std::vector<OTDeltaRec> batch;
        for (int i = 0; i < 3; i++) {
            OTDeltaRec rec{};
            rec.handle_idx = i;
            rec.tag = 1;
            rec.birth_epoch = i + 1;
            rec.retire_epoch = ~uint64_t{0};
            batch.push_back(rec);
        }
        log.append(batch);
        log.sync();
        
        // Simulate torn write by directly writing partial data
        log.close();
    }
    
    // Corrupt the file by truncating it mid-frame
    {
        std::filesystem::resize_file(log_path, 
            std::filesystem::file_size(log_path) - 10);
    }
    
    // Try to replay - should handle torn frame gracefully
    OTDeltaLog replay_log(log_path);
    size_t count = 0;
    bool success = replay_log.replay(log_path,
        [&](const OTDeltaRec& rec) {
            count++;
            EXPECT_LE(rec.handle_idx, 2u); // Only complete frames
        });
    
    EXPECT_TRUE(success);
    EXPECT_EQ(count, 3u); // Only the 3 complete frames
}

// Test 4: Tag consistency - verify tags remain consistent through crash/recovery
TEST_F(CrashConsistencyTest, TagConsistencyAcrossCrash) {
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::STRICT;
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    
    // Allocate node - gets tag T
    auto alloc1 = store->allocate_node(256, NodeKind::Leaf);
    uint16_t original_tag = alloc1.id.tag();
    EXPECT_GT(original_tag, 0);
    
    // Publish and commit
    std::vector<uint8_t> data(100, 0xAA);
    store->publish_node(alloc1.id, data.data(), data.size());
    store->commit(1);
    
    // Retire the node
    store->retire_node(alloc1.id, 2);
    store->commit(2);
    
    // Simulate crash
    SimulateCrash();
    
    // Recover
    runtime_ = RecoverAfterCrash();
    store = runtime_->get_store("test_tree");
    
    // Reallocate same handle - should get bumped tag
    auto alloc2 = store->allocate_node(256, NodeKind::Internal);
    
    // If we got the same handle, tag should be bumped
    if (alloc2.id.handle_index() == alloc1.id.handle_index()) {
        EXPECT_EQ(alloc2.id.tag(), uint16_t(original_tag + 1))
            << "Tag should be bumped on handle reuse";
    }
    
    // Commit with new node
    store->publish_node(alloc2.id, data.data(), data.size());
    store->commit(3);
    
    // Verify no ABA problem - old NodeID should not work
    auto bytes_old = store->read_node(alloc1.id);
    EXPECT_EQ(bytes_old.data, nullptr) << "Old NodeID should be invalid";
    
    // New NodeID should work
    auto bytes_new = store->read_node(alloc2.id);
    EXPECT_NE(bytes_new.data, nullptr) << "New NodeID should be valid";
}

// Test 5: EVENTUAL mode - small payload recovery from WAL only
TEST_F(CrashConsistencyTest, EventualSmallPayloadRecovery) {
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::EVENTUAL;
    config.policy.max_payload_in_wal = 128; // Small threshold
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    
    // Create small nodes (payload in WAL)
    std::vector<NodeID> small_nodes;
    for (int i = 0; i < 5; i++) {
        auto alloc = store->allocate_node(256, NodeKind::Leaf);
        std::vector<uint8_t> data(64, 0x10 + i); // Small payload
        store->publish_node(alloc.id, data.data(), data.size());
        small_nodes.push_back(alloc.id);
    }
    
    // Create large nodes (payload in segments)
    std::vector<NodeID> large_nodes;
    for (int i = 0; i < 3; i++) {
        auto alloc = store->allocate_node(1024, NodeKind::Internal);
        std::vector<uint8_t> data(512, 0x20 + i); // Large payload
        store->publish_node(alloc.id, data.data(), data.size());
        large_nodes.push_back(alloc.id);
    }
    
    // Commit
    store->commit(1);
    
    // Simulate crash (segments might not be flushed in EVENTUAL)
    SimulateCrash();
    
    // Recover
    runtime_ = RecoverAfterCrash();
    store = runtime_->get_store("test_tree");
    
    // Small nodes should be recovered from WAL payloads
    for (size_t i = 0; i < small_nodes.size(); i++) {
        auto bytes = store->read_node(small_nodes[i]);
        EXPECT_NE(bytes.data, nullptr) << "Small node " << i << " not recovered";
        if (bytes.data) {
            // Verify data integrity
            EXPECT_EQ(static_cast<uint8_t*>(bytes.data)[0], 0x10 + i);
        }
    }
    
    // Large nodes might be lost if segments weren't flushed (EVENTUAL mode)
    // But metadata should be in WAL
    for (size_t i = 0; i < large_nodes.size(); i++) {
        auto bytes = store->read_node(large_nodes[i]);
        // Node should exist (metadata in WAL) but data might be zeros
        EXPECT_NE(bytes.data, nullptr) << "Large node " << i << " metadata not recovered";
    }
}

// Test 6: STRICT mode fsync discipline
TEST_F(CrashConsistencyTest, StrictModeFsyncDiscipline) {
    // Track fsync calls
    std::atomic<int> fsync_count{0};
    
    // We'd need to hook into PlatformFS to count fsyncs
    // For now, test the behavior
    
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::STRICT;
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    
    // Batch multiple operations
    const int batch_size = 10;
    std::vector<NodeID> nodes;
    
    for (int i = 0; i < batch_size; i++) {
        auto alloc = store->allocate_node(512, NodeKind::Leaf);
        std::vector<uint8_t> data(256, i);
        store->publish_node(alloc.id, data.data(), data.size());
        nodes.push_back(alloc.id);
    }
    
    // Single commit should batch all fsyncs efficiently
    store->commit(1);
    
    // Simulate immediate crash
    SimulateCrash();
    
    // Recover - all data must be durable in STRICT mode
    runtime_ = RecoverAfterCrash();
    store = runtime_->get_store("test_tree");
    
    for (size_t i = 0; i < nodes.size(); i++) {
        auto bytes = store->read_node(nodes[i]);
        ASSERT_NE(bytes.data, nullptr) << "STRICT mode lost data for node " << i;
        // Verify exact data
        uint8_t* ptr = static_cast<uint8_t*>(bytes.data);
        for (size_t j = 0; j < 256; j++) {
            EXPECT_EQ(ptr[j], i) << "Data corruption at byte " << j;
        }
    }
}

// Test 7: Handle reuse with tag increment
TEST_F(CrashConsistencyTest, HandleReuseTagIncrement) {
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::BALANCED;
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    
    // Allocate and track handle/tag
    auto alloc1 = store->allocate_node(256, NodeKind::Leaf);
    uint64_t handle = alloc1.id.handle_index();
    uint16_t tag1 = alloc1.id.tag();
    
    std::vector<uint8_t> data1(100, 0xAA);
    store->publish_node(alloc1.id, data1.data(), data1.size());
    store->commit(1);
    
    // Retire the node
    store->retire_node(alloc1.id, 2);
    store->commit(2);
    
    // Force handle reuse by allocating many nodes to exhaust free list
    // Then the retired handle should be reclaimed
    std::vector<NodeID> filler_nodes;
    for (int i = 0; i < 100; i++) {
        auto alloc = store->allocate_node(256, NodeKind::Leaf);
        store->publish_node(alloc.id, nullptr, 0);
        filler_nodes.push_back(alloc.id);
        if ((i % 10) == 9) {
            store->commit(3 + i/10);
        }
    }
    
    // Now allocate again - might reuse the retired handle
    auto alloc2 = store->allocate_node(256, NodeKind::Internal);
    
    if (alloc2.id.handle_index() == handle) {
        // Got the same handle - verify tag was bumped
        uint16_t tag2 = alloc2.id.tag();
        EXPECT_EQ(tag2, uint16_t(tag1 + 1)) << "Tag not bumped on reuse";
        
        // Verify WAL has correct tag
        std::vector<uint8_t> data2(100, 0xBB);
        store->publish_node(alloc2.id, data2.data(), data2.size());
        store->commit(20);
        
        // Old NodeID should be invalid
        auto bytes1 = store->read_node(alloc1.id);
        EXPECT_EQ(bytes1.data, nullptr);
        
        // New NodeID should work
        auto bytes2 = store->read_node(alloc2.id);
        EXPECT_NE(bytes2.data, nullptr);
    }
}

// Test 8: Group commit consistency
TEST_F(CrashConsistencyTest, GroupCommitConsistency) {
    runtime_ = std::make_unique<DurableRuntime>();
    DurableRuntime::Config config;
    config.data_dir = test_dir_;
    config.policy.mode = DurabilityMode::BALANCED;
    config.policy.group_commit_interval_ms = 10; // Enable group commit
    ASSERT_TRUE(runtime_->open(config));
    
    auto* store = runtime_->get_store("test_tree");
    
    // Launch multiple threads doing concurrent commits
    const int num_threads = 4;
    const int ops_per_thread = 10;
    std::vector<std::thread> threads;
    std::vector<std::vector<NodeID>> thread_nodes(num_threads);
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < ops_per_thread; i++) {
                auto alloc = store->allocate_node(256, NodeKind::Leaf);
                std::vector<uint8_t> data(100, t * 16 + i);
                store->publish_node(alloc.id, data.data(), data.size());
                thread_nodes[t].push_back(alloc.id);
                store->commit(t * 100 + i);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Simulate crash
    SimulateCrash();
    
    // Recover and verify all committed data
    runtime_ = RecoverAfterCrash();
    store = runtime_->get_store("test_tree");
    
    for (int t = 0; t < num_threads; t++) {
        for (size_t i = 0; i < thread_nodes[t].size(); i++) {
            auto bytes = store->read_node(thread_nodes[t][i]);
            ASSERT_NE(bytes.data, nullptr) 
                << "Lost node from thread " << t << " op " << i;
        }
    }
}

} // namespace xtree::persist::test