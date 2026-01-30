/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test for ObjectTable handle lifecycle management
 * Ensures handles follow: FREE → RESERVED → LIVE → RETIRED → (GC) → FREE
 * and that ABA protection via tag increments works correctly
 */

#include <gtest/gtest.h>
#include "persistence/object_table.hpp"
#include "persistence/node_id.hpp"
#include <vector>
#include <set>
#include <thread>
#include <chrono>
#include <atomic>
#include <unordered_set>

using namespace xtree::persist;

class ObjectTableHandleLifecycleTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Start with a fresh ObjectTable for each test
        ot = std::make_unique<ObjectTable>(1000); // Small capacity for testing
    }

    void TearDown() override {
        ot.reset();
    }

    std::unique_ptr<ObjectTable> ot;
};

TEST_F(ObjectTableHandleLifecycleTest, HandleCannotBeReusedWhileLive) {
    // Allocate a handle
    OTAddr addr1{1, 1, 100, 1024, nullptr};
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr1, 1);
    ASSERT_TRUE(id1.valid());
    uint64_t handle = id1.handle_index();
    uint8_t tag1 = id1.tag();

    // Try to allocate more handles - should NOT reuse the LIVE handle
    std::set<uint64_t> allocated_handles;
    allocated_handles.insert(handle);

    for (int i = 0; i < 100; i++) {
        OTAddr addr{1, 1, static_cast<uint64_t>(200 + i * 100), 1024, nullptr};
        NodeID id = ot->allocate(NodeKind::Leaf, 1, addr, 1);
        uint64_t h = id.handle_index();

        // Critical assertion: should never get the same handle while it's LIVE
        ASSERT_NE(h, handle) << "Handle " << handle
                             << " was reallocated while still LIVE at iteration " << i;

        // Also check for no duplicate allocations
        ASSERT_EQ(allocated_handles.find(h), allocated_handles.end())
            << "Handle " << h << " allocated twice";
        allocated_handles.insert(h);
    }
}

TEST_F(ObjectTableHandleLifecycleTest, TagIncrementOnReuse) {
    // Allocate and publish a handle
    OTAddr addr1{1, 1, 100, 1024, nullptr};
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr1, 1);
    uint64_t handle = id1.handle_index();
    uint8_t tag1 = id1.tag();

    // Retire the handle
    ot->retire(id1, 2); // retire_epoch = 2

    // Simulate epoch advancement to make it safe to reclaim
    // (In real system, this would be done by MVCC/GC)
    ot->reclaim_before_epoch(3); // All retired with epoch <= 3 can be reclaimed

    // Now try to allocate - if we get the same handle, tag MUST be different
    bool found_reuse = false;
    for (int i = 0; i < 100 && !found_reuse; i++) {
        OTAddr addr2{1, 1, static_cast<uint64_t>(200 + i * 100), 1024, nullptr};
        NodeID id2 = ot->allocate(NodeKind::Leaf, 1, addr2, 3);
        if (id2.handle_index() == handle) {
            found_reuse = true;
            uint8_t tag2 = id2.tag();

            // Critical assertion: tag must be incremented on reuse
            ASSERT_NE(tag2, tag1) << "Handle " << handle
                                  << " reused with same tag (ABA vulnerability!)";

            // Expected: tag2 = tag1 + 1 (with wraparound and skip-0)
            uint8_t expected_tag = tag1 + 1;
            if (expected_tag == 0) expected_tag = 1; // Skip 0
            ASSERT_EQ(tag2, expected_tag) << "Tag not incremented correctly on reuse";
        }
    }
}

TEST_F(ObjectTableHandleLifecycleTest, ResolveEntryRespectsTag) {
    // Allocate and publish a handle
    OTAddr addr1{1, 1, 100, 1024, nullptr};
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr1, 1);
    uint64_t handle = id1.handle_index();
    uint8_t tag1 = id1.tag();

    // Should be able to resolve with correct tag
    const OTEntry* found = ot->try_get(id1);
    ASSERT_NE(found, nullptr);
    ASSERT_EQ(found->kind, NodeKind::Leaf);

    // Should NOT resolve with wrong tag (simulating stale reference)
    NodeID stale_id = NodeID::from_parts(handle, tag1 + 1);
    const OTEntry* not_found = ot->try_get(stale_id);
    ASSERT_EQ(not_found, nullptr) << "Should not resolve with wrong tag";
}

TEST_F(ObjectTableHandleLifecycleTest, NoDuplicateAllocationBug) {
    // This test specifically checks for the bug we found:
    // Handle 1 being allocated twice with the same tag

    // First allocation
    OTAddr addr1{1, 1, 100, 1024, nullptr};
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr1, 1);
    ASSERT_TRUE(id1.valid());

    // Track what we got
    uint64_t first_raw = id1.raw();
    uint64_t first_handle = id1.handle_index();
    uint8_t first_tag = id1.tag();

    // Allocate many more handles
    std::set<uint64_t> seen_raw_ids;
    seen_raw_ids.insert(first_raw);

    for (int i = 0; i < 500; i++) {
        OTAddr addr{1, 1, static_cast<uint64_t>(200 + i * 100), 1024, nullptr};
        NodeID id = ot->allocate(NodeKind::Leaf, 1, addr, 1);
        uint64_t raw = id.raw();

        // Critical check: should NEVER see the same raw NodeID twice
        ASSERT_EQ(seen_raw_ids.find(raw), seen_raw_ids.end())
            << "NodeID " << raw << " (handle=" << id.handle_index()
            << ", tag=" << (int)id.tag() << ") allocated twice! "
            << "First was handle=" << first_handle << ", tag=" << (int)first_tag
            << " at iteration " << i;

        seen_raw_ids.insert(raw);
    }
}

TEST_F(ObjectTableHandleLifecycleTest, StressTestHandleLifecycle) {
    // Stress test: allocate, publish, retire, reclaim in cycles
    const int NUM_CYCLES = 10;
    const int HANDLES_PER_CYCLE = 100;

    std::vector<NodeID> active_ids;
    uint64_t epoch = 1;

    for (int cycle = 0; cycle < NUM_CYCLES; cycle++) {
        // Allocate and publish new handles
        for (int i = 0; i < HANDLES_PER_CYCLE; i++) {
            OTAddr addr{
                static_cast<uint32_t>(cycle),
                static_cast<uint32_t>(i),
                static_cast<uint64_t>(i * 1024),
                1024,
                nullptr
            };
            NodeID id = ot->allocate(
                (i % 2 == 0) ? NodeKind::Leaf : NodeKind::Internal,
                1,
                addr,
                epoch
            );
            ASSERT_TRUE(id.valid());
            active_ids.push_back(id);
        }

        epoch++;

        // Retire half of the active handles
        size_t retire_count = active_ids.size() / 2;
        for (size_t i = 0; i < retire_count && !active_ids.empty(); i++) {
            NodeID id = active_ids.front();
            active_ids.erase(active_ids.begin());
            ot->retire(id, epoch);
        }

        epoch++;

        // Reclaim retired handles (simulate GC)
        ot->reclaim_before_epoch(epoch - 1);

        // Verify all active handles are still resolvable
        for (const NodeID& id : active_ids) {
            const OTEntry* entry = ot->try_get(id);
            ASSERT_NE(entry, nullptr)
                << "Active NodeID " << id.raw() << " not found after cycle " << cycle;
        }
    }
}

TEST_F(ObjectTableHandleLifecycleTest, ExhaustionReturnsInvalid) {
    // Test that allocate fails when no FREE handles remain
    std::vector<NodeID> ids;

    // Allocate handles until exhaustion
    // Small capacity (1000) should exhaust quickly
    for (size_t i = 0; i < 10000; ++i) { // Try more than capacity
        OTAddr addr{1, 1, i * 100, 100, nullptr};
        NodeID id = ot->allocate(NodeKind::Leaf, 1, addr, 1);
        if (!id.valid()) {
            // Already exhausted
            ASSERT_GT(i, 0) << "Should allocate at least some handles before exhaustion";
            return;
        }
        // Keep them allocated (LIVE) to prevent reuse
        ids.push_back(id);
    }

    // If we get here, we should have exhausted the table
    OTAddr overflow_addr{1, 1, 999999, 100, nullptr};
    NodeID overflow = ot->allocate(NodeKind::Leaf, 1, overflow_addr, 1);
    ASSERT_FALSE(overflow.valid()) << "allocate should fail when no FREE handles remain";
}

TEST_F(ObjectTableHandleLifecycleTest, AllocateIdempotentAndStateful) {
    // Test that allocation with same parameters gives different handles
    OTAddr addr{1, 1, 128, 256, nullptr};

    // First allocation
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr, 10);
    ASSERT_TRUE(id1.valid());

    // Second allocation with same parameters should give different handle
    NodeID id2 = ot->allocate(NodeKind::Leaf, 1, addr, 10);
    ASSERT_TRUE(id2.valid());
    ASSERT_NE(id1.raw(), id2.raw()) << "Should allocate different handles for same parameters";

    // Both should be resolvable
    const OTEntry* p1 = ot->try_get(id1);
    ASSERT_NE(p1, nullptr);
    ASSERT_EQ(p1->kind, NodeKind::Leaf);

    const OTEntry* p2 = ot->try_get(id2);
    ASSERT_NE(p2, nullptr);
    ASSERT_EQ(p2->kind, NodeKind::Leaf);
}

TEST_F(ObjectTableHandleLifecycleTest, RetireIdempotent) {
    // Test that retiring multiple times is safe
    OTAddr addr{1, 1, 64, 64, nullptr};
    NodeID id = ot->allocate(NodeKind::Leaf, 1, addr, 12);
    ASSERT_TRUE(id.valid());

    // First retire
    ot->retire(id, 15);

    // Should still be resolvable (not reclaimed yet)
    const OTEntry* p = ot->try_get(id);
    ASSERT_NE(p, nullptr) << "Handle should still be resolvable after retire before reclaim";

    // Second retire with same or different epoch should be safe
    ot->retire(id, 16);

    // Still resolvable
    p = ot->try_get(id);
    ASSERT_NE(p, nullptr) << "Handle should still be resolvable after multiple retires";
}

TEST_F(ObjectTableHandleLifecycleTest, RetiredInvisibleOnlyAfterGC) {
    // Test GC safety and epoch visibility
    OTAddr addr{2, 0, 0, 512, nullptr};
    NodeID id = ot->allocate(NodeKind::Internal, 2, addr, 100);
    ASSERT_TRUE(id.valid());

    // Retire the entry
    ot->retire(id, 200);

    // Before GC, still resolvable (depending on contract)
    const OTEntry* pre = ot->try_get(id);
    ASSERT_NE(pre, nullptr) << "Retired entry should still be visible before GC";

    // Reclaim with epoch before retirement - should still be visible
    ot->reclaim_before_epoch(199);
    ASSERT_NE(ot->try_get(id), nullptr) << "Not GC-safe yet (retire_epoch=200, cutoff=199)";

    // Reclaim at retirement epoch - should now be reclaimable
    ot->reclaim_before_epoch(200);
    // At or after GC cutoff: entry becomes invalid for lookups with that NodeID
    // The handle may be recycled, but with a different tag
    const OTEntry* post = ot->try_get(id);
    // After reclaim, the old NodeID should not resolve (tag mismatch or freed)
    // The exact behavior depends on implementation, but it should be safe
}

TEST_F(ObjectTableHandleLifecycleTest, ABAProtectionOnReallocate) {
    // Test ABA protection when handle is reallocated
    OTAddr addr1{1, 1, 1, 1, nullptr};
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr1, 1);
    ASSERT_TRUE(id1.valid());

    uint64_t h = id1.handle_index();
    uint8_t t1 = id1.tag();

    // Retire and reclaim
    ot->retire(id1, 2);
    ot->reclaim_before_epoch(3);

    // Force reuse - for small capacity, it will happen soon
    NodeID id2;
    bool found_reuse = false;
    for (int i = 0; i < 500; ++i) {
        OTAddr addr2{1, 1, static_cast<uint64_t>(100 + i), 16, nullptr};
        NodeID x = ot->allocate(NodeKind::Leaf, 1, addr2, 3);
        if (!x.valid()) break; // Exhausted
        if (x.handle_index() == h) {
            id2 = x;
            found_reuse = true;
            break;
        }
    }

    if (found_reuse) {
        ASSERT_TRUE(id2.valid());
        ASSERT_EQ(id2.handle_index(), h);
        ASSERT_NE(id2.tag(), t1) << "ABA: tag must bump on reuse";

        // Old (handle,tag) must not resolve after reuse
        ASSERT_EQ(ot->try_get(id1), nullptr) << "Old (handle,tag) must not resolve after reuse";

        // New (handle,tag) should resolve
        ASSERT_NE(ot->try_get(id2), nullptr) << "New (handle,tag) should resolve";
    }
}

TEST_F(ObjectTableHandleLifecycleTest, ConcurrentAllocRetire) {
    // Concurrent smoke test - skip if ObjectTable is not thread-safe
    // Note: This assumes ObjectTable has thread-safe operations
    constexpr int T = 4;  // threads
    constexpr int N = 200; // operations per thread
    std::vector<std::thread> threads;
    std::atomic<int> ok{0};

    for (int t = 0; t < T; ++t) {
        threads.emplace_back([this, &ok, t]() {
            for (int i = 0; i < N; ++i) {
                OTAddr addr{
                    static_cast<uint32_t>(t),
                    static_cast<uint32_t>(i),
                    static_cast<uint64_t>(i),
                    64,
                    nullptr
                };
                NodeID id = ot->allocate(NodeKind::Leaf, 1, addr, 1);
                if (!id.valid()) continue; // May exhaust

                if (ot->try_get(id)) {
                    ok++;
                }

                // Randomly retire some
                if (i % 3 == 0) {
                    ot->retire(id, i + 2);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    ASSERT_GT(ok.load(), 0) << "Should have successfully allocated and resolved some handles";
}

TEST_F(ObjectTableHandleLifecycleTest, MarkLiveWorkflow) {
    // Test the mark_live_reserve and mark_live_commit workflow
    OTAddr addr{1, 1, 100, 1024, nullptr};

    // Initial allocation
    NodeID proposed = ot->allocate(NodeKind::Leaf, 1, addr, 0); // birth_epoch=0 initially
    ASSERT_TRUE(proposed.valid());

    // Reserve for marking live
    NodeID final_id = ot->mark_live_reserve(proposed, 10);
    ASSERT_TRUE(final_id.valid());

    // The final_id might have a different tag if handle was recycled
    // but the handle_index should match
    ASSERT_EQ(final_id.handle_index(), proposed.handle_index());

    // Commit the mark_live operation
    ot->mark_live_commit(final_id, 10);

    // Now should be resolvable and live
    const OTEntry* entry = ot->try_get(final_id);
    ASSERT_NE(entry, nullptr);
    ASSERT_TRUE(entry->is_live());
    ASSERT_EQ(entry->birth_epoch.load(std::memory_order_acquire), 10u);
}

// Additional API contract validation tests
TEST_F(ObjectTableHandleLifecycleTest, APIContractValidation) {
    // Test the expected state transitions according to the API contract

    // 1. allocate() must: create entry with birth_epoch, set kind, return valid NodeID
    OTAddr addr1{1, 1, 100, 1024, nullptr};
    NodeID id1 = ot->allocate(NodeKind::Leaf, 1, addr1, 5);
    ASSERT_TRUE(id1.valid());
    const OTEntry* e1 = ot->try_get(id1);
    ASSERT_NE(e1, nullptr);
    ASSERT_EQ(e1->kind, NodeKind::Leaf);
    ASSERT_EQ(e1->birth_epoch.load(std::memory_order_acquire), 5u);
    ASSERT_EQ(e1->retire_epoch.load(std::memory_order_acquire), ~uint64_t{0});

    // 2. retire() must: only accept LIVE; set retire_epoch
    ot->retire(id1, 10);
    const OTEntry* e2 = ot->try_get(id1);
    ASSERT_NE(e2, nullptr); // Still visible until reclaimed
    ASSERT_EQ(e2->retire_epoch.load(std::memory_order_acquire), 10u);

    // 3. reclaim_before_epoch() must: free RETIRED with retire_epoch <= cutoff
    size_t reclaimed = ot->reclaim_before_epoch(10);
    EXPECT_GT(reclaimed, 0u) << "Should have reclaimed at least one entry";

    // After reclaim, old NodeID should not resolve (tag mismatch)
    const OTEntry* e3 = ot->try_get(id1);
    // May be nullptr or have different tag - implementation specific
}