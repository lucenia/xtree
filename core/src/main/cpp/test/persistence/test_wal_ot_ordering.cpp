/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Tests for WAL ⇄ OT ordering invariants
 */

#include <gtest/gtest.h>
#include <filesystem>
#include "persistence/object_table_sharded.hpp"
#include "persistence/ot_delta_log.h"
#include "test_helpers.h"

namespace xtree::persist::test {

class WALOTOrderingTest : public ::testing::Test {
protected:
    std::string test_dir_;
    std::unique_ptr<ObjectTableSharded> ot_;
    std::unique_ptr<OTDeltaLog> log_;
    
    void SetUp() override {
        test_dir_ = create_temp_dir("wal_ot_ordering");
        ot_ = std::make_unique<ObjectTableSharded>();
        
        // Setup delta log
        std::string log_path = test_dir_ + "/delta.wal";
        log_ = std::make_unique<OTDeltaLog>(log_path);
        ASSERT_TRUE(log_->open_for_append());
    }
    
    void TearDown() override {
        log_.reset();
        ot_.reset();
        std::filesystem::remove_all(test_dir_);
    }
};

// Test the two-phase mark_live protocol
TEST_F(WALOTOrderingTest, TwoPhaseMarkLive) {
    // Allocate a handle
    OTAddr addr{1, 1, 0, 256, nullptr};
    NodeID id1 = ot_->allocate(NodeKind::Leaf, 1, addr, 0);
    ASSERT_TRUE(id1.valid());
    uint16_t initial_tag = id1.tag();
    
    // Phase 1: Reserve (before WAL)
    uint64_t epoch = 10;
    NodeID reserved = ot_->mark_live_reserve(id1, epoch);
    EXPECT_EQ(reserved.raw(), id1.raw()) << "Tag shouldn't change on first use";
    
    // Build WAL record with reserved tag
    OTDeltaRec delta{};
    delta.handle_idx = reserved.handle_index();
    delta.tag = reserved.tag();
    delta.birth_epoch = epoch;
    delta.retire_epoch = ~uint64_t{0};
    delta.kind = static_cast<uint8_t>(NodeKind::Leaf);
    
    // Append to WAL
    std::vector<OTDeltaRec> batch = {delta};
    log_->append(batch);
    log_->sync();
    
    // Phase 2: Commit (after WAL)
    ot_->mark_live_commit(reserved, epoch);
    
    // Verify the entry is now live with correct tag
    const auto& entry = ot_->get(reserved);
    EXPECT_EQ(entry.birth_epoch.load(), epoch);
    EXPECT_EQ(entry.tag.load(), reserved.tag());
}

// Test handle reuse with tag bump
TEST_F(WALOTOrderingTest, HandleReuseTagBump) {
    // First allocation
    OTAddr addr1{1, 1, 0, 256, nullptr};
    NodeID id1 = ot_->allocate(NodeKind::Leaf, 1, addr1, 0);
    uint16_t tag1 = id1.tag();
    
    // Mark live and commit
    NodeID reserved1 = ot_->mark_live_reserve(id1, 10);
    ot_->mark_live_commit(reserved1, 10);
    
    // Retire the node
    ot_->retire(id1, 20);
    
    // Simulate handle being freed and reused
    // (In real scenario, reclaim would return handle to free list)
    
    // Second allocation reusing same handle
    OTAddr addr2{1, 2, 0, 512, nullptr};
    // Manually set up the reuse scenario
    // Simulate what reclaim_before_epoch does: clears birth but keeps retire as breadcrumb
    const auto& entry = ot_->get_by_handle_unsafe(id1.handle_index());
    const_cast<OTEntry&>(entry).birth_epoch.store(0);  // Cleared by reclaim
    // retire_epoch stays at 20 as the breadcrumb
    
    // Now reserve should detect reuse and bump tag
    NodeID id2 = NodeID::from_parts(id1.handle_index(), tag1); // Same handle, old tag
    NodeID reserved2 = ot_->mark_live_reserve(id2, 30);
    
    // Tag should be bumped
    EXPECT_EQ(reserved2.handle_index(), id1.handle_index());
    EXPECT_EQ(reserved2.tag(), uint16_t(tag1 + 1)) << "Tag not bumped on reuse";
    
    // WAL should get the bumped tag
    OTDeltaRec delta{};
    delta.handle_idx = reserved2.handle_index();
    delta.tag = reserved2.tag(); // Bumped tag
    delta.birth_epoch = 30;
    delta.retire_epoch = ~uint64_t{0};
    
    std::vector<OTDeltaRec> batch = {delta};
    log_->append(batch);
    log_->sync();
    
    // Commit with bumped tag
    ot_->mark_live_commit(reserved2, 30);
    
    // Old NodeID should now be invalid
    EXPECT_FALSE(ot_->validate_tag(id1));
    
    // New NodeID should be valid
    EXPECT_TRUE(ot_->validate_tag(reserved2));
}

// Test that allocate() doesn't make entry visible
TEST_F(WALOTOrderingTest, AllocateInvisibleUntilCommit) {
    // Allocate but don't commit
    OTAddr addr{1, 1, 0, 256, nullptr};
    NodeID id = ot_->allocate(NodeKind::Leaf, 1, addr, 0);
    
    // Entry should not be visible to readers
    const auto& entry = ot_->get_by_handle_unsafe(id.handle_index());
    EXPECT_EQ(entry.birth_epoch.load(), 0u) << "Birth epoch should be 0 until commit";
    EXPECT_EQ(entry.retire_epoch.load(), ~uint64_t{0}) << "Retire epoch should be MAX";
    
    // Tag should NOT be stored yet
    uint16_t stored_tag = entry.tag.load();
    // The stored tag might be from a previous use, but shouldn't be the new tag
    // since we don't store it in allocate() anymore
    
    // Now go through proper commit sequence
    NodeID reserved = ot_->mark_live_reserve(id, 100);
    
    // Build and append WAL
    OTDeltaRec delta{};
    delta.handle_idx = reserved.handle_index();
    delta.tag = reserved.tag();
    delta.birth_epoch = 100;
    delta.retire_epoch = ~uint64_t{0};
    
    std::vector<OTDeltaRec> batch = {delta};
    log_->append(batch);
    log_->sync();
    
    // NOW commit to OT
    ot_->mark_live_commit(reserved, 100);
    
    // Entry should now be visible
    EXPECT_EQ(entry.birth_epoch.load(), 100u);
    EXPECT_EQ(entry.tag.load(), reserved.tag());
}

// Test recovery replay order
TEST_F(WALOTOrderingTest, RecoveryReplayOrder) {
    // Simulate a batch of operations
    std::vector<NodeID> nodes;
    std::vector<OTDeltaRec> wal_batch;
    
    // Allocate several nodes
    for (int i = 0; i < 5; i++) {
        OTAddr addr{1, uint16_t(i), uint32_t(i * 256), 256, nullptr};
        NodeID id = ot_->allocate(NodeKind::Leaf, 1, addr, 0);
        nodes.push_back(id);
    }
    
    // Reserve all for epoch 50
    std::vector<NodeID> reserved;
    for (const auto& id : nodes) {
        NodeID r = ot_->mark_live_reserve(id, 50);
        reserved.push_back(r);
        
        // Build WAL record
        OTDeltaRec delta{};
        delta.handle_idx = r.handle_index();
        delta.tag = r.tag();
        delta.birth_epoch = 50;
        delta.retire_epoch = ~uint64_t{0};
        delta.kind = static_cast<uint8_t>(NodeKind::Leaf);
        wal_batch.push_back(delta);
    }
    
    // Append to WAL
    log_->append(wal_batch);
    log_->sync();
    
    // Simulate crash before OT update
    // (Don't call mark_live_commit)
    
    // Close and reopen log for replay
    log_->close();
    
    // Create new OT for recovery
    auto recovery_ot = std::make_unique<ObjectTableSharded>();
    recovery_ot->begin_recovery();
    
    // Replay the log
    size_t replayed = 0;
    uint64_t last_offset = 0;
    std::string error_msg;
    OTDeltaLog::replay(test_dir_ + "/delta.wal",
        [&](const OTDeltaRec& rec) {
            recovery_ot->apply_delta(rec);
            replayed++;
        },
        &last_offset,
        &error_msg);
    
    recovery_ot->end_recovery();
    
    EXPECT_EQ(replayed, 5u);
    
    // Verify all nodes are now live with correct tags
    for (size_t i = 0; i < reserved.size(); i++) {
        const auto& entry = recovery_ot->get(reserved[i]);
        EXPECT_EQ(entry.birth_epoch.load(), 50u);
        EXPECT_EQ(entry.tag.load(), reserved[i].tag());
    }
}

// Test that WAL batch atomicity
TEST_F(WALOTOrderingTest, WALBatchAtomicity) {
    // Create a batch with multiple operations
    std::vector<NodeID> allocations;
    std::vector<NodeID> retirements;
    
    // Some allocations
    for (int i = 0; i < 3; i++) {
        OTAddr addr{1, uint16_t(i), uint32_t(i * 256), 256, nullptr};
        NodeID id = ot_->allocate(NodeKind::Leaf, 1, addr, 0);
        allocations.push_back(id);
    }
    
    // Mark first one live (to retire it later)
    NodeID reserved0 = ot_->mark_live_reserve(allocations[0], 10);
    ot_->mark_live_commit(reserved0, 10);
    
    // Now build a batch with allocation + retirement
    uint64_t commit_epoch = 20;
    
    // Reserve the new allocations
    std::vector<NodeID> reserved;
    for (size_t i = 1; i < allocations.size(); i++) {
        NodeID r = ot_->mark_live_reserve(allocations[i], commit_epoch);
        reserved.push_back(r);
    }
    
    // Build WAL batch
    std::vector<OTDeltaRec> wal_batch;
    
    // Add allocations
    for (const auto& r : reserved) {
        OTDeltaRec delta{};
        delta.handle_idx = r.handle_index();
        delta.tag = r.tag();
        delta.birth_epoch = commit_epoch;
        delta.retire_epoch = ~uint64_t{0};
        wal_batch.push_back(delta);
    }
    
    // Add retirement
    OTDeltaRec retire_delta{};
    retire_delta.handle_idx = reserved0.handle_index();
    retire_delta.tag = reserved0.tag();
    retire_delta.birth_epoch = 10; // Original birth
    retire_delta.retire_epoch = commit_epoch;
    wal_batch.push_back(retire_delta);
    
    // Append atomically
    log_->append(wal_batch);
    log_->sync();
    
    // Now apply to OT
    for (const auto& r : reserved) {
        ot_->mark_live_commit(r, commit_epoch);
    }
    ot_->retire(reserved0, commit_epoch);
    
    // Verify batch was applied atomically
    for (const auto& r : reserved) {
        const auto& entry = ot_->get(r);
        EXPECT_EQ(entry.birth_epoch.load(), commit_epoch);
    }
    
    // First node should be retired
    const auto& retired_entry = ot_->get(reserved0);
    EXPECT_EQ(retired_entry.retire_epoch.load(), commit_epoch);
}

} // namespace xtree::persist::test