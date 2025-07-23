/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test group commit functionality
 */

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <filesystem>
#include <cstring>
#include "persistence/durable_runtime.h"
#include "persistence/durable_store.h"
#include "persistence/durability_policy.h"

using namespace xtree::persist;
using namespace std::chrono_literals;

class GroupCommitTest : public ::testing::Test {
protected:
    std::string test_dir_;
    
    void SetUp() override {
        test_dir_ = "/tmp/xtree_group_commit_test_" + std::to_string(getpid());
    }
    
    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }
};

TEST_F(GroupCommitTest, BasicGroupCommit) {
    // Create runtime with group commit enabled
    Paths paths{
        .data_dir = test_dir_,
        .manifest = test_dir_ + "/manifest.json",
        .superblock = test_dir_ + "/xtree.meta",
        .active_log = test_dir_ + "/delta_0000.wal"
    };
    
    CheckpointPolicy ckpt_policy;
    ckpt_policy.group_commit_interval_ms = 10;  // 10ms batch window
    
    auto runtime = DurableRuntime::open(paths, ckpt_policy, false);
    ASSERT_TRUE(runtime);
    
    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };
    
    // Create store with group commit durability policy
    DurabilityPolicy dur_policy;
    dur_policy.mode = DurabilityMode::BALANCED;
    dur_policy.group_commit_interval_ms = 10;  // Should match checkpoint policy
    
    DurableStore store(ctx, "test_store", dur_policy);
    
    // Track sync count
    std::atomic<int> sync_count{0};
    std::atomic<int> commit_count{0};
    
    // Launch multiple writer threads
    const int num_writers = 4;
    const int commits_per_writer = 10;
    std::vector<std::thread> writers;
    
    auto start_time = std::chrono::steady_clock::now();
    
    for (int w = 0; w < num_writers; w++) {
        writers.emplace_back([&, w]() {
            for (int i = 0; i < commits_per_writer; i++) {
                // Allocate and write a node
                auto result = store.allocate_node(256, NodeKind::Leaf);
                ASSERT_TRUE(result.id.valid());
                NodeID id = result.id;
                void* ptr = result.writable;
                
                // Write some data
                std::memset(ptr, w * 10 + i, 256);
                store.publish_node(id, ptr, 256);
                
                // Commit
                uint64_t epoch = ctx.mvcc.advance_epoch();
                store.commit(epoch);
                commit_count++;
                
                // Small delay between commits
                std::this_thread::sleep_for(2ms);
            }
        });
    }
    
    // Wait for all writers
    for (auto& t : writers) {
        t.join();
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    
    // Verify all commits succeeded
    EXPECT_EQ(commit_count.load(), num_writers * commits_per_writer);
    
    // With group commit, we should have fewer syncs than commits
    // In ideal batching with 10ms window and 2ms between commits,
    // we should batch ~5 commits per sync
    // But this is timing-dependent, so just verify we got some batching
    
    // For now, just verify the test runs without crashing
    // A more sophisticated test would instrument the actual sync calls
    
    std::cout << "Total commits: " << commit_count.load() << std::endl;
    std::cout << "Total time: " << duration_ms << "ms" << std::endl;
    std::cout << "Average commit rate: " << (commit_count.load() * 1000.0 / duration_ms) << " commits/sec" << std::endl;
}

TEST_F(GroupCommitTest, DisabledGroupCommit) {
    // Create runtime with group commit disabled
    Paths paths{
        .data_dir = test_dir_,
        .manifest = test_dir_ + "/manifest.json",
        .superblock = test_dir_ + "/xtree.meta",
        .active_log = test_dir_ + "/delta_0000.wal"
    };
    
    CheckpointPolicy ckpt_policy;
    ckpt_policy.group_commit_interval_ms = 0;  // Disabled
    
    auto runtime = DurableRuntime::open(paths, ckpt_policy, false);
    ASSERT_TRUE(runtime);
    
    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };
    
    // Create store with group commit disabled
    DurabilityPolicy dur_policy;
    dur_policy.mode = DurabilityMode::BALANCED;
    dur_policy.group_commit_interval_ms = 0;  // Disabled
    
    DurableStore store(ctx, "test_store", dur_policy);
    
    // Single writer test - should sync immediately
    auto result = store.allocate_node(256, NodeKind::Leaf);
    ASSERT_TRUE(result.id.valid());
    NodeID id = result.id;
    void* ptr = result.writable;
    
    std::memset(ptr, 42, 256);
    store.publish_node(id, ptr, 256);
    
    uint64_t epoch = ctx.mvcc.advance_epoch();
    store.commit(epoch);
    
    // Verify commit succeeded
    NodeID root = store.get_root("");
    EXPECT_EQ(root.raw(), id.raw());
}

TEST_F(GroupCommitTest, MixedPolicies) {
    // Test that STRICT mode ignores group commit (always syncs immediately)
    Paths paths{
        .data_dir = test_dir_,
        .manifest = test_dir_ + "/manifest.json",
        .superblock = test_dir_ + "/xtree.meta",
        .active_log = test_dir_ + "/delta_0000.wal"
    };
    
    CheckpointPolicy ckpt_policy;
    ckpt_policy.group_commit_interval_ms = 10;  // Enable in coordinator
    
    auto runtime = DurableRuntime::open(paths, ckpt_policy, false);
    ASSERT_TRUE(runtime);
    
    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };
    
    // Create store with STRICT mode (should ignore group commit)
    DurabilityPolicy dur_policy;
    dur_policy.mode = DurabilityMode::STRICT;
    dur_policy.group_commit_interval_ms = 10;  // Should be ignored in STRICT mode
    
    DurableStore store(ctx, "test_store", dur_policy);
    
    // Write and commit
    auto result = store.allocate_node(256, NodeKind::Leaf);
    ASSERT_TRUE(result.id.valid());
    NodeID id = result.id;
    void* ptr = result.writable;
    
    std::memset(ptr, 99, 256);
    store.publish_node(id, ptr, 256);
    
    uint64_t epoch = ctx.mvcc.advance_epoch();
    
    // In STRICT mode, this should sync immediately despite group commit setting
    store.commit(epoch);
    
    // Verify commit succeeded
    NodeID root = store.get_root("");
    EXPECT_EQ(root.raw(), id.raw());
}