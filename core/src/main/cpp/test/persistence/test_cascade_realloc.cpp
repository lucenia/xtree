/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Deterministic test for multi-level cascade reallocation during splits.
 * Tests the specific scenario where splits cascade up multiple tree levels
 * and require coordinated dirty tracking and reallocation.
 */

#include <gtest/gtest.h>
#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/durable_store.h"
#include "../../src/persistence/memory_store.h"
#include <filesystem>
#include <cstring>
#include <unistd.h>
#include <vector>
#include <set>
#include <iostream>

namespace xtree {
namespace persist {

class CascadeReallocTest : public ::testing::Test {
protected:
    void SetUp() override {
        namespace fs = std::filesystem;
        test_dir_ = "/tmp/cascade_realloc_test_" + std::to_string(getpid());
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);

        // Setup paths
        paths_ = {
            .data_dir = test_dir_,
            .manifest = test_dir_ + "/manifest.json",
            .superblock = test_dir_ + "/superblock.bin",
            .active_log = test_dir_ + "/ot_delta.wal"
        };

        // Setup policy
        policy_ = {
            .max_replay_bytes = 100 * 1024 * 1024,  // 100MB
            .max_replay_epochs = 100000,
            .max_age = std::chrono::seconds(600),
            .min_interval = std::chrono::seconds(30)
        };
    }

    void TearDown() override {
        namespace fs = std::filesystem;
        fs::remove_all(test_dir_);
    }

    std::string test_dir_;
    Paths paths_;
    CheckpointPolicy policy_;
};

// Test: Simulate parent-child relationship with cascading reallocations
TEST_F(CascadeReallocTest, ParentChildCascade) {
    std::cerr << "\n=== ParentChildCascade Test ===" << std::endl;

    auto runtime = DurableRuntime::open(paths_, policy_);
    ASSERT_NE(runtime, nullptr);

    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };

    DurableStore store(ctx, "test");

    // Allocate a parent (internal) node
    auto parent_alloc = store.allocate_node(128, NodeKind::Internal);
    ASSERT_TRUE(parent_alloc.id.valid());
    NodeID parent_id = parent_alloc.id;
    std::cerr << "[TEST] Parent allocated: " << parent_id.raw() << std::endl;

    // Allocate 3 child (leaf) nodes
    std::vector<NodeID> child_ids;
    for (int i = 0; i < 3; i++) {
        auto child_alloc = store.allocate_node(64, NodeKind::Leaf);
        ASSERT_TRUE(child_alloc.id.valid());
        child_ids.push_back(child_alloc.id);
        std::cerr << "[TEST] Child " << i << " allocated: " << child_alloc.id.raw() << std::endl;
    }

    // Simulate parent's wire format containing child NodeIDs
    std::vector<uint8_t> parent_payload(100);
    uint64_t* child_refs = reinterpret_cast<uint64_t*>(parent_payload.data() + 8);
    for (size_t i = 0; i < child_ids.size(); i++) {
        child_refs[i] = child_ids[i].raw();
    }

    // Publish parent
    store.publish_node(parent_id, parent_payload.data(), parent_payload.size());

    // Publish children
    for (auto& cid : child_ids) {
        std::vector<uint8_t> child_payload(50, 0x42);
        store.publish_node(cid, child_payload.data(), child_payload.size());
    }

    // Commit epoch 1
    store.commit(1);
    std::cerr << "[TEST] Epoch 1 committed" << std::endl;

    // Verify all nodes are LIVE
    NodeKind kind;
    ASSERT_TRUE(store.get_node_kind(parent_id, kind));
    EXPECT_EQ(kind, NodeKind::Internal);

    for (auto& cid : child_ids) {
        ASSERT_TRUE(store.get_node_kind(cid, kind));
        EXPECT_EQ(kind, NodeKind::Leaf);
    }

    std::cerr << "[TEST] All nodes verified LIVE" << std::endl;

    // Now simulate child reallocation (child grows beyond its allocation)
    NodeID old_child0 = child_ids[0];
    auto new_child_alloc = store.allocate_node(200, NodeKind::Leaf);
    ASSERT_TRUE(new_child_alloc.id.valid());
    NodeID new_child0 = new_child_alloc.id;

    std::cerr << "[TEST] Child 0 reallocated: " << old_child0.raw()
              << " -> " << new_child0.raw() << std::endl;

    // Publish larger payload to new child
    std::vector<uint8_t> large_child_payload(180, 0x43);
    store.publish_node(new_child0, large_child_payload.data(), large_child_payload.size());

    // Update child_ids with new ID
    child_ids[0] = new_child0;

    // CASCADE: Parent must be updated to reference new child ID
    child_refs[0] = new_child0.raw();
    store.publish_node(parent_id, parent_payload.data(), parent_payload.size());

    // Commit epoch 2
    store.commit(2);
    std::cerr << "[TEST] Epoch 2 committed (after cascade)" << std::endl;

    // Verify new child is LIVE
    ASSERT_TRUE(store.get_node_kind(new_child0, kind));
    EXPECT_EQ(kind, NodeKind::Leaf);

    // Old child should be RETIRED (or FREE if garbage collected)
    bool old_still_present = store.is_node_present(old_child0);
    std::cerr << "[TEST] Old child 0 still present: " << old_still_present << std::endl;

    std::cerr << "[TEST] ParentChildCascade PASSED" << std::endl;
}

// Test: Three-level cascade (root -> internal -> leaf)
TEST_F(CascadeReallocTest, ThreeLevelCascade) {
    std::cerr << "\n=== ThreeLevelCascade Test ===" << std::endl;

    auto runtime = DurableRuntime::open(paths_, policy_);
    ASSERT_NE(runtime, nullptr);

    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };

    DurableStore store(ctx, "test");

    // Level 0: Root (internal)
    auto root_alloc = store.allocate_node(256, NodeKind::Internal);
    NodeID root_id = root_alloc.id;
    std::cerr << "[TEST] Root: " << root_id.raw() << std::endl;

    // Level 1: 2 internal nodes
    std::vector<NodeID> level1_ids;
    for (int i = 0; i < 2; i++) {
        auto alloc = store.allocate_node(128, NodeKind::Internal);
        level1_ids.push_back(alloc.id);
        std::cerr << "[TEST] Level1[" << i << "]: " << alloc.id.raw() << std::endl;
    }

    // Level 2: 4 leaf nodes (2 per internal)
    std::vector<NodeID> level2_ids;
    for (int i = 0; i < 4; i++) {
        auto alloc = store.allocate_node(64, NodeKind::Leaf);
        level2_ids.push_back(alloc.id);
        std::cerr << "[TEST] Level2[" << i << "]: " << alloc.id.raw() << std::endl;
    }

    // Wire up tree structure
    std::vector<uint8_t> root_payload(200);
    uint64_t* root_children = reinterpret_cast<uint64_t*>(root_payload.data() + 8);
    root_children[0] = level1_ids[0].raw();
    root_children[1] = level1_ids[1].raw();
    store.publish_node(root_id, root_payload.data(), root_payload.size());

    std::vector<uint8_t> level1_payload(100);
    uint64_t* level1_children = reinterpret_cast<uint64_t*>(level1_payload.data() + 8);

    level1_children[0] = level2_ids[0].raw();
    level1_children[1] = level2_ids[1].raw();
    store.publish_node(level1_ids[0], level1_payload.data(), level1_payload.size());

    level1_children[0] = level2_ids[2].raw();
    level1_children[1] = level2_ids[3].raw();
    store.publish_node(level1_ids[1], level1_payload.data(), level1_payload.size());

    for (auto& lid : level2_ids) {
        std::vector<uint8_t> leaf_payload(50, 0x42);
        store.publish_node(lid, leaf_payload.data(), leaf_payload.size());
    }

    // Commit epoch 1
    store.commit(1);
    store.set_root(root_id, 1, nullptr, 0, "test_field");
    std::cerr << "[TEST] Epoch 1: All levels committed" << std::endl;

    // === Cascade: leaf[0] grows ===
    NodeID old_leaf0 = level2_ids[0];
    auto new_leaf_alloc = store.allocate_node(200, NodeKind::Leaf);
    NodeID new_leaf0 = new_leaf_alloc.id;
    std::cerr << "[TEST] Leaf[0] reallocated: " << old_leaf0.raw()
              << " -> " << new_leaf0.raw() << std::endl;

    std::vector<uint8_t> large_leaf(180, 0x43);
    store.publish_node(new_leaf0, large_leaf.data(), large_leaf.size());
    level2_ids[0] = new_leaf0;

    // Update level1[0]
    level1_children[0] = new_leaf0.raw();
    level1_children[1] = level2_ids[1].raw();
    store.publish_node(level1_ids[0], level1_payload.data(), level1_payload.size());

    store.commit(2);
    std::cerr << "[TEST] Epoch 2: Cascade committed" << std::endl;

    // Verify tree is still intact
    NodeKind kind;
    ASSERT_TRUE(store.get_node_kind(root_id, kind));
    EXPECT_EQ(kind, NodeKind::Internal);

    ASSERT_TRUE(store.get_node_kind(level1_ids[0], kind));
    EXPECT_EQ(kind, NodeKind::Internal);

    ASSERT_TRUE(store.get_node_kind(new_leaf0, kind));
    EXPECT_EQ(kind, NodeKind::Leaf);

    NodeID stored_root = store.get_root("test_field");
    EXPECT_EQ(stored_root.raw(), root_id.raw());

    std::cerr << "[TEST] ThreeLevelCascade PASSED" << std::endl;
}

// Test: What happens when a RESERVED node is freed before commit?
TEST_F(CascadeReallocTest, AbortedReservation) {
    std::cerr << "\n=== AbortedReservation Test ===" << std::endl;

    auto runtime = DurableRuntime::open(paths_, policy_);
    ASSERT_NE(runtime, nullptr);

    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };

    DurableStore store(ctx, "test");

    // Allocate a node (RESERVED state)
    auto alloc1 = store.allocate_node(64, NodeKind::Leaf);
    NodeID reserved_id = alloc1.id;
    std::cerr << "[TEST] Allocated (RESERVED): " << reserved_id.raw() << std::endl;

    // Before committing, allocate a replacement
    auto alloc2 = store.allocate_node(128, NodeKind::Leaf);
    NodeID replacement_id = alloc2.id;
    std::cerr << "[TEST] Replacement allocated: " << replacement_id.raw() << std::endl;

    // Free the original RESERVED node
    DS_FREE_IMMEDIATE(&store, reserved_id, Reallocation);
    std::cerr << "[TEST] Original RESERVED node freed" << std::endl;

    // Commit only the replacement
    std::vector<uint8_t> payload(100, 0x42);
    store.publish_node(replacement_id, payload.data(), payload.size());
    store.commit(1);

    // Verify replacement is LIVE
    NodeKind kind;
    ASSERT_TRUE(store.get_node_kind(replacement_id, kind));
    EXPECT_EQ(kind, NodeKind::Leaf);

    // Original should not be accessible
    bool original_present = store.is_node_present(reserved_id);
    std::cerr << "[TEST] Original still present: " << original_present << std::endl;
    EXPECT_FALSE(original_present);

    std::cerr << "[TEST] AbortedReservation PASSED" << std::endl;
}

// Test: Multiple rapid reallocations of the same logical node
TEST_F(CascadeReallocTest, MultipleRapidReallocations) {
    std::cerr << "\n=== MultipleRapidReallocations Test ===" << std::endl;

    auto runtime = DurableRuntime::open(paths_, policy_);
    ASSERT_NE(runtime, nullptr);

    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };

    DurableStore store(ctx, "test");

    NodeID current_id;
    std::vector<NodeID> all_ids;

    // Allocate initial node
    auto alloc = store.allocate_node(64, NodeKind::Leaf);
    current_id = alloc.id;
    all_ids.push_back(current_id);
    std::cerr << "[TEST] Initial: " << current_id.raw() << std::endl;

    // Simulate 5 rapid reallocations
    for (int i = 0; i < 5; i++) {
        auto new_alloc = store.allocate_node(64 + (i * 32), NodeKind::Leaf);
        NodeID new_id = new_alloc.id;
        all_ids.push_back(new_id);

        DS_FREE_IMMEDIATE(&store, current_id, Reallocation);

        std::cerr << "[TEST] Realloc " << i << ": " << current_id.raw()
                  << " -> " << new_id.raw() << std::endl;

        current_id = new_id;
    }

    // Publish and commit only the final one
    std::vector<uint8_t> payload(200, 0x42);
    store.publish_node(current_id, payload.data(), payload.size());
    store.commit(1);

    // Only the final ID should be LIVE
    NodeKind kind;
    ASSERT_TRUE(store.get_node_kind(current_id, kind));
    EXPECT_EQ(kind, NodeKind::Leaf);

    // All intermediate IDs should be gone
    for (size_t i = 0; i < all_ids.size() - 1; i++) {
        bool present = store.is_node_present(all_ids[i]);
        std::cerr << "[TEST] ID " << all_ids[i].raw() << " present: " << present << std::endl;
        EXPECT_FALSE(present);
    }

    std::cerr << "[TEST] MultipleRapidReallocations PASSED" << std::endl;
}

// Test: Recovery after cascade - close and reopen, verify structure
TEST_F(CascadeReallocTest, RecoveryAfterCascade) {
    std::cerr << "\n=== RecoveryAfterCascade Test ===" << std::endl;

    NodeID root_id, leaf0_id, leaf1_id;

    // Phase 1: Create tree and do cascade
    {
        auto runtime = DurableRuntime::open(paths_, policy_);
        ASSERT_NE(runtime, nullptr);

        DurableContext ctx{
            .ot = runtime->ot(),
            .alloc = runtime->allocator(),
            .coord = runtime->coordinator(),
            .mvcc = runtime->mvcc(),
            .runtime = *runtime
        };

        DurableStore store(ctx, "test");

        // Create simple 2-level tree
        auto root_alloc = store.allocate_node(128, NodeKind::Internal);
        root_id = root_alloc.id;

        auto leaf0_alloc = store.allocate_node(64, NodeKind::Leaf);
        leaf0_id = leaf0_alloc.id;

        auto leaf1_alloc = store.allocate_node(64, NodeKind::Leaf);
        leaf1_id = leaf1_alloc.id;

        std::cerr << "[TEST] Initial tree: root=" << root_id.raw()
                  << " leaf0=" << leaf0_id.raw()
                  << " leaf1=" << leaf1_id.raw() << std::endl;

        // Wire up tree structure
        std::vector<uint8_t> root_payload(100);
        uint64_t* root_children = reinterpret_cast<uint64_t*>(root_payload.data() + 8);
        root_children[0] = leaf0_id.raw();
        root_children[1] = leaf1_id.raw();

        store.publish_node(root_id, root_payload.data(), root_payload.size());

        std::vector<uint8_t> leaf_payload(50, 0x42);
        store.publish_node(leaf0_id, leaf_payload.data(), leaf_payload.size());
        store.publish_node(leaf1_id, leaf_payload.data(), leaf_payload.size());

        store.set_root(root_id, 1, nullptr, 0, "test_field");
        store.commit(1);

        // Now cascade: reallocate leaf0
        NodeID old_leaf0 = leaf0_id;
        auto new_leaf_alloc = store.allocate_node(200, NodeKind::Leaf);
        leaf0_id = new_leaf_alloc.id;

        std::cerr << "[TEST] Leaf0 reallocated: " << old_leaf0.raw()
                  << " -> " << leaf0_id.raw() << std::endl;

        std::vector<uint8_t> large_leaf(180, 0x43);
        store.publish_node(leaf0_id, large_leaf.data(), large_leaf.size());

        // Update root with new leaf reference
        root_children[0] = leaf0_id.raw();
        store.publish_node(root_id, root_payload.data(), root_payload.size());

        store.commit(2);
        std::cerr << "[TEST] Phase 1 complete - tree committed with cascade" << std::endl;
    }

    // Phase 2: Reopen and verify
    {
        auto runtime = DurableRuntime::open(paths_, policy_);
        ASSERT_NE(runtime, nullptr);

        DurableContext ctx{
            .ot = runtime->ot(),
            .alloc = runtime->allocator(),
            .coord = runtime->coordinator(),
            .mvcc = runtime->mvcc(),
            .runtime = *runtime
        };

        DurableStore store(ctx, "test");

        // Verify root is still accessible
        NodeID recovered_root = store.get_root("test_field");
        std::cerr << "[TEST] Recovered root: " << recovered_root.raw()
                  << " (expected " << root_id.raw() << ")" << std::endl;
        EXPECT_EQ(recovered_root.raw(), root_id.raw());

        // Verify root can be read
        NodeBytes root_bytes = store.read_node(root_id);
        ASSERT_GT(root_bytes.size, 0u);
        std::cerr << "[TEST] Root readable, size=" << root_bytes.size << std::endl;

        // Verify children in root payload
        const uint64_t* children = reinterpret_cast<const uint64_t*>(
            static_cast<const uint8_t*>(root_bytes.data) + 8);
        EXPECT_EQ(children[0], leaf0_id.raw());
        EXPECT_EQ(children[1], leaf1_id.raw());

        // Verify both leaves are accessible
        NodeBytes leaf0_bytes = store.read_node(leaf0_id);
        ASSERT_GT(leaf0_bytes.size, 0u);
        std::cerr << "[TEST] Leaf0 readable, size=" << leaf0_bytes.size << std::endl;

        NodeBytes leaf1_bytes = store.read_node(leaf1_id);
        ASSERT_GT(leaf1_bytes.size, 0u);
        std::cerr << "[TEST] Leaf1 readable, size=" << leaf1_bytes.size << std::endl;

        std::cerr << "[TEST] RecoveryAfterCascade PASSED" << std::endl;
    }
}

} // namespace persist
} // namespace xtree
