/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Simplified crash consistency tests focusing on core invariants
 */

#include <gtest/gtest.h>
#include <filesystem>
#include "persistence/object_table.hpp"
#include "persistence/ot_delta_log.h"
#include "test_helpers.h"

namespace xtree::persist::test {

// Test torn frame handling in delta log
TEST(CrashConsistency, TornFrameHandling) {
    std::string test_dir = create_temp_dir("torn_frame");
    std::string log_path = test_dir + "/torn_frame.wal";
    
    // Write some complete frames
    {
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
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
        log.close();
    }
    
    // Get file size before corruption
    size_t original_size = std::filesystem::file_size(log_path);
    
    // Each frame is kFrameHeaderSize (16) + kWireRecSize (52) = 68 bytes
    // We wrote 3 frames, so we have 3 * 68 = 204 bytes
    // To create a torn frame, truncate into the last frame (leave 2 complete + partial 3rd)
    size_t two_frames = 2 * (16 + 52);  // 136 bytes
    std::filesystem::resize_file(log_path, two_frames + 30);  // Partial 3rd frame
    
    // Try to replay - should handle torn frame gracefully
    size_t count = 0;
    uint64_t last_offset = 0;
    std::string error_msg;
    
    bool success = OTDeltaLog::replay(log_path,
        [&](const OTDeltaRec& rec) {
            count++;
            EXPECT_LE(rec.handle_idx, 2u);
        },
        &last_offset,
        &error_msg);
    
    if (!success) {
        std::cerr << "Replay failed with error: " << error_msg << std::endl;
        std::cerr << "Count: " << count << ", Last offset: " << last_offset << std::endl;
    }
    EXPECT_TRUE(success);
    EXPECT_EQ(count, 2u); // Only 2 complete frames replayed (3rd is torn)
    
    std::filesystem::remove_all(test_dir);
}

// Test that allocation doesn't make entry visible
TEST(CrashConsistency, AllocateInvisibleUntilCommit) {
    ObjectTable ot;
    
    // Allocate but don't commit
    OTAddr addr{1, 1, 0, 256, nullptr};
    NodeID id = ot.allocate(NodeKind::Leaf, 1, addr, 0);
    ASSERT_TRUE(id.valid());
    
    // Entry should not be visible (birth_epoch = 0)
    const auto& entry = ot.get_by_handle_unsafe(id.handle_index());
    EXPECT_EQ(entry.birth_epoch.load(), 0u) << "Birth epoch should be 0 until commit";
    EXPECT_EQ(entry.retire_epoch.load(), ~uint64_t{0}) << "Retire epoch should be MAX";
    
    // Now mark live properly
    NodeID reserved = ot.mark_live_reserve(id, 100);
    ot.mark_live_commit(reserved, 100);
    
    // Now should be visible
    EXPECT_EQ(entry.birth_epoch.load(), 100u);
    EXPECT_EQ(entry.tag.load(), reserved.tag());
}

// Test tag management on handle reuse
TEST(CrashConsistency, HandleReuseTagBump) {
    ObjectTable ot;

    // First allocation (fresh handle, tag not bumped yet)
    OTAddr addr1{1, 1, 0, 256, nullptr};
    NodeID id1 = ot.allocate(NodeKind::Leaf, 1, addr1, 0);
    const uint16_t tag1 = id1.tag();

    // First make-live (no bump on first use)
    NodeID reserved1 = ot.mark_live_reserve(id1, 10);
    ot.mark_live_commit(reserved1, 10);
    EXPECT_EQ(reserved1.tag(), tag1);

    // Retire and reclaim so the handle returns to the free list
    ot.retire(reserved1, 20);
    size_t reclaimed = ot.reclaim_before_epoch(30);
    EXPECT_GT(reclaimed, 0u);

    // Re-allocate: should reuse the same handle (only one available)
    OTAddr addr2{1, 2, 0, 512, nullptr};
    NodeID id2 = ot.allocate(NodeKind::Leaf, 1, addr2, 0);
    EXPECT_EQ(id2.handle_index(), id1.handle_index()) << "Allocator should reuse the only freed handle";
    EXPECT_EQ(id2.tag(), tag1) << "Allocate returns current tag; bump happens at reserve";

    // Reuse detect & bump at reserve (skip-0 wrapping)
    NodeID reserved2 = ot.mark_live_reserve(id2, 40);
    const uint16_t expected_tag = (uint16_t)(tag1 + 1) == 0 ? 1 : (uint16_t)(tag1 + 1);
    EXPECT_EQ(reserved2.handle_index(), id1.handle_index());
    EXPECT_EQ(reserved2.tag(), expected_tag) << "Tag not bumped on reuse";

    // Commit and verify the entry's stored tag matches
    ot.mark_live_commit(reserved2, 40);
    const auto& e = ot.get_by_handle_unsafe(reserved2.handle_index());
    EXPECT_EQ(e.tag.load(std::memory_order_relaxed), reserved2.tag());

    // Optional: no double-bump without another retire/reclaim
    NodeID reserved_again = ot.mark_live_reserve(reserved2, 41);
    EXPECT_EQ(reserved_again.tag(), reserved2.tag()) << "No bump when already live";
}

// Test WAL replay with payloads
TEST(CrashConsistency, PayloadReplay) {
    std::string test_dir = create_temp_dir("payload_replay");
    std::string log_path = test_dir + "/payload.wal";
    
    // Write frames with payloads
    {
        OTDeltaLog log(log_path);
        ASSERT_TRUE(log.open_for_append());
        
        std::vector<OTDeltaLog::DeltaWithPayload> batch;
        
        // Small node with payload
        OTDeltaRec rec1{};
        rec1.handle_idx = 1;
        rec1.tag = 1;
        rec1.birth_epoch = 10;
        rec1.retire_epoch = ~uint64_t{0};
        rec1.data_crc32c = 0x12345678;
        
        std::vector<uint8_t> payload1(64, 0xAA);
        batch.push_back({rec1, payload1.data(), payload1.size()});
        
        // Large node without payload
        OTDeltaRec rec2{};
        rec2.handle_idx = 2;
        rec2.tag = 1;
        rec2.birth_epoch = 10;
        rec2.retire_epoch = ~uint64_t{0};
        batch.push_back({rec2, nullptr, 0});
        
        log.append_with_payloads(batch);
        log.sync();
        log.close();
    }
    
    // Replay with payloads
    {
        OTDeltaLog log(log_path);
        size_t count = 0;
        bool got_payload = false;
        
        log.replay_with_payloads(
            [&](const OTDeltaRec& rec, const void* payload, size_t payload_size) {
                count++;
                if (rec.handle_idx == 1) {
                    EXPECT_NE(payload, nullptr);
                    EXPECT_EQ(payload_size, 64u);
                    if (payload) {
                        EXPECT_EQ(static_cast<const uint8_t*>(payload)[0], 0xAA);
                        got_payload = true;
                    }
                } else if (rec.handle_idx == 2) {
                    EXPECT_EQ(payload, nullptr);
                    EXPECT_EQ(payload_size, 0u);
                }
            });
        
        EXPECT_EQ(count, 2u);
        EXPECT_TRUE(got_payload);
    }
    
    std::filesystem::remove_all(test_dir);
}

// Test two-phase mark_live protocol
TEST(CrashConsistency, TwoPhaseMarkLive) {
    std::string test_dir = create_temp_dir("two_phase");
    std::string log_path = test_dir + "/delta.wal";
    
    ObjectTable ot;
    OTDeltaLog log(log_path);
    ASSERT_TRUE(log.open_for_append());
    
    // Allocate
    OTAddr addr{1, 1, 0, 256, nullptr};
    NodeID id = ot.allocate(NodeKind::Leaf, 1, addr, 0);
    
    // Phase 1: Reserve
    uint64_t epoch = 50;
    NodeID reserved = ot.mark_live_reserve(id, epoch);
    
    // Build WAL with reserved tag
    OTDeltaRec delta{};
    delta.handle_idx = reserved.handle_index();
    delta.tag = reserved.tag();
    delta.birth_epoch = epoch;
    delta.retire_epoch = ~uint64_t{0};
    
    // Append to WAL
    std::vector<OTDeltaRec> batch = {delta};
    log.append(batch);
    log.sync();
    
    // Phase 2: Commit
    ot.mark_live_commit(reserved, epoch);
    
    // Verify state
    const auto& entry = ot.get(reserved);
    EXPECT_EQ(entry.birth_epoch.load(), epoch);
    EXPECT_EQ(entry.tag.load(), reserved.tag());
    
    log.close();
    std::filesystem::remove_all(test_dir);
}

} // namespace xtree::persist::test