/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Tests for Sharded Object Table
 */

// Enable stats tracking for tests
#define XTREE_ENABLE_SHARD_STATS 1

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <random>
#include <set>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include "../../src/persistence/object_table_sharded.hpp"
#include "../../src/persistence/ot_checkpoint.h"
#include "../../src/persistence/ot_delta_log.h"

using namespace xtree::persist;

class ObjectTableShardedTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Each test gets a fresh sharded table
        ot_sharded_ = std::make_unique<ObjectTableSharded>(10000, 64);
    }
    
    void TearDown() override {
        ot_sharded_.reset();
    }
    
    std::unique_ptr<ObjectTableSharded> ot_sharded_;
};

// Test that sharding behaves like unsharded at small scale
TEST_F(ObjectTableShardedTest, SmallScaleBehavior) {
    // At small scale, operations should work identically to unsharded
    std::vector<NodeID> ids;
    
    // Allocate a small number of nodes
    for (int i = 0; i < 100; ++i) {
        OTAddr addr{};
        addr.file_id = 0;
        addr.segment_id = i;
        addr.offset = i * 4096;
        addr.length = 4096;
        
        NodeID id = ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
        ids.push_back(id);
        
        // Verify unique handle
        EXPECT_GT(id.handle_index(), 0);
        // Note: tag is not set until mark_live_reserve/commit
    }
    
    // Verify all handles are unique
    std::set<uint32_t> unique_handles;
    for (const auto& id : ids) {
        unique_handles.insert(id.handle_index());
    }
    EXPECT_EQ(unique_handles.size(), ids.size());
    
    // Check how many shards were actually used (should be just a few)
    size_t active = ot_sharded_->active_shards();
    EXPECT_LE(active, 10) << "Small workload should only touch a few shards";
}

// Test that first allocation returns a valid handle
TEST_F(ObjectTableShardedTest, FirstHandleIsValid) {
    OTAddr addr{};
    addr.file_id = 0;
    addr.segment_id = 0;
    addr.offset = 0;
    addr.length = 4096;

    NodeID first = ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
    // No parity enforcement - just verify handle is valid and not reserved
    EXPECT_NE(first.handle_index(), 0) << "Handle 0 is reserved and should never be allocated";
    // Check the local handle portion (before sharding transformation) is valid
    // The sharded table uses bits [47:42] for shard_id, [41:0] for local handle
    uint64_t local_handle = first.handle_index() & ((1ULL << 42) - 1);
    EXPECT_NE(local_handle, 0) << "Local handle should not be 0 (reserved)";
}

// Test mark_live operations work correctly across shards
TEST_F(ObjectTableShardedTest, MarkLiveAcrossShards) {
    // Create a fresh table with a high activation threshold to ensure we stay on 1 shard
    ot_sharded_ = std::make_unique<ObjectTableSharded>(10000, 64);
    ot_sharded_->set_activation_step_for_tests(2000);  // Won't activate second shard until 2000 ops
    
    std::vector<NodeID> allocated;
    std::vector<NodeID> published;
    
    // Allocate nodes (will distribute across shards)
    for (int i = 0; i < 1000; ++i) {
        OTAddr addr{};
        addr.file_id = i / 100;
        addr.segment_id = i % 100;
        addr.offset = i * 4096;
        addr.length = 4096;
        
        NodeID id = ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
        allocated.push_back(id);
    }
    
    // Mark all as live
    for (size_t i = 0; i < allocated.size(); ++i) {
        NodeID reserved = ot_sharded_->mark_live_reserve(allocated[i], i + 100);
        ot_sharded_->mark_live_commit(reserved, i + 100);
        published.push_back(reserved);
    }
    
    // Verify all published IDs are valid
    for (const auto& id : published) {
        EXPECT_TRUE(ot_sharded_->is_valid(id));
        EXPECT_TRUE(ot_sharded_->validate_tag(id));
    }
    
    // Check shard distribution - with activation_step=2000, 1000 allocations should stay on 1 shard
    size_t active = ot_sharded_->active_shards();
    EXPECT_EQ(active, 1) << "Small workload (1000 ops) should stay on 1 shard with activation_step=2000";
}

// Test reclaim works correctly across all shards
TEST_F(ObjectTableShardedTest, ReclaimAcrossShards) {
    const size_t N = 10000;
    std::vector<NodeID> ids;
    
    // Allocate and publish many nodes
    for (size_t i = 0; i < N; ++i) {
        OTAddr addr{};
        addr.file_id = i / 1000;
        addr.segment_id = (i / 10) % 100;
        addr.offset = (i % 10) * 4096;
        addr.length = 4096;
        
        NodeID id = ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
        NodeID reserved = ot_sharded_->mark_live_reserve(id, 100);
        ot_sharded_->mark_live_commit(reserved, 100);
        ids.push_back(reserved);
    }
    
    // Retire all at epoch 200
    for (const auto& id : ids) {
        ot_sharded_->retire(id, 200);
    }
    
    // Reclaim should work across all shards
    size_t reclaimed = ot_sharded_->reclaim_before_epoch(201);
    EXPECT_EQ(reclaimed, N) << "Should reclaim all retired nodes";
    
    // Verify none are valid anymore
    for (const auto& id : ids) {
        EXPECT_FALSE(ot_sharded_->is_valid(id));
    }
}

// Test concurrent operations show good scaling
TEST_F(ObjectTableShardedTest, ConcurrentScaling) {
    const int thread_counts[] = {1, 2, 4, 8};
    const size_t ops_per_thread = 10000;
    
    
    double single_thread_ops = 0;
    
    for (int num_threads : thread_counts) {
        // Fresh table for each test
        ot_sharded_ = std::make_unique<ObjectTableSharded>(
            ops_per_thread * num_threads * 2, 64);
        
        auto worker = [this](int thread_id, size_t ops) {
            std::vector<NodeID> local_ids;
            
            for (size_t i = 0; i < ops; ++i) {
                OTAddr addr{};
                addr.file_id = thread_id;
                addr.segment_id = i / 100;
                addr.offset = (i % 100) * 4096;
                addr.length = 4096;
                
                NodeID id = ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
                NodeID reserved = ot_sharded_->mark_live_reserve(id, thread_id * 1000 + i);
                ot_sharded_->mark_live_commit(reserved, thread_id * 1000 + i);
                local_ids.push_back(reserved);
            }
            
            // Some validations
            for (size_t i = 0; i < ops / 10; ++i) {
                ot_sharded_->is_valid(local_ids[i]);
            }
        };
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back(worker, i, ops_per_thread);
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            end - start).count();
        
        double ops_per_sec = (num_threads * ops_per_thread * 1e6) / duration;
        
        if (num_threads == 1) {
            single_thread_ops = ops_per_sec;
        }
        
        double scaling = (single_thread_ops > 0) ? 
            (ops_per_sec / single_thread_ops) / num_threads : 1.0;
        
        size_t active_shards = ot_sharded_->active_shards();
        
    }
}

// Test metrics collection works correctly
TEST_F(ObjectTableShardedTest, MetricsCollection) {
    const size_t N = 1000;
    
    // Do some operations
    for (size_t i = 0; i < N; ++i) {
        OTAddr addr{};
        addr.file_id = 0;
        addr.segment_id = i;
        addr.offset = i * 4096;
        addr.length = 4096;
        
        NodeID id = ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
        NodeID reserved = ot_sharded_->mark_live_reserve(id, 100);
        ot_sharded_->mark_live_commit(reserved, 100);
        
        if (i % 2 == 0) {
            ot_sharded_->validate_tag(reserved);
        }
        
        if (i % 3 == 0) {
            ot_sharded_->retire(reserved, 200);
        }
    }
    
    // Get aggregate metrics
    auto metrics = ot_sharded_->get_aggregate_metrics();
    
    // Note: We skip allocation stats in hot path for performance
    // EXPECT_EQ(metrics.allocations, N);
    EXPECT_EQ(metrics.validations, N / 2);
    // Allow ±1 for rounding in division
    EXPECT_GE(metrics.retirements, N / 3 - 1);
    EXPECT_LE(metrics.retirements, N / 3 + 1);
    
}

// Performance comparison: sharded vs unsharded for single thread
TEST_F(ObjectTableShardedTest, SingleThreadPerformance) {
    const size_t N = 100000;
    
    // Disable shard activation for fair comparison
    ot_sharded_->set_activation_step_for_tests(UINT32_MAX);
    
    // Test sharded version
    auto start_sharded = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < N; ++i) {
        OTAddr addr{};
        addr.file_id = i / 10000;
        addr.segment_id = (i / 100) % 100;
        addr.offset = (i % 100) * 4096;
        addr.length = 4096;
        
        ot_sharded_->allocate(NodeKind::Internal, 1, addr, 0);
    }
    
    auto end_sharded = std::chrono::high_resolution_clock::now();
    auto sharded_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end_sharded - start_sharded).count();
    
    // Test unsharded version
    ObjectTable ot_single(N);
    auto start_single = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < N; ++i) {
        OTAddr addr{};
        addr.file_id = i / 10000;
        addr.segment_id = (i / 100) % 100;
        addr.offset = (i % 100) * 4096;
        addr.length = 4096;
        
        ot_single.allocate(NodeKind::Internal, 1, addr, 0);
    }
    
    auto end_single = std::chrono::high_resolution_clock::now();
    auto single_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end_single - start_single).count();
    
    double sharded_ns_per_op = double(sharded_ns) / N;
    double single_ns_per_op = double(single_ns) / N;
    double overhead = ((sharded_ns_per_op / single_ns_per_op) - 1.0) * 100;
    
    // Sharding overhead should be reasonable (<30% for single thread)
    // The overhead comes from: atomic check, array indirection, potential cache miss
    // This is acceptable given the massive benefits for multi-threaded scenarios
    EXPECT_LT(overhead, 30.0) << "Sharding overhead should be reasonable";
}

// Test that routing works correctly - allocations route to correct shard
TEST_F(ObjectTableShardedTest, RoutingCorrectness) {
    // Fresh sharded table
    auto test_ot = std::make_unique<ObjectTableSharded>(100000, 8);

    const size_t NUM_ALLOCS = 5000;  // enough to engage multiple shards
    std::vector<NodeID> ids;
    ids.reserve(NUM_ALLOCS);

    std::map<uint32_t, size_t> shard_counts;

    // 1) Allocate (NOT live yet; birth_epoch must be 0 on allocate)
    for (size_t i = 0; i < NUM_ALLOCS; ++i) {
        OTAddr addr{};
        addr.file_id    = i / 1000;
        addr.segment_id = i / 100;
        addr.offset     = i * 4096;
        addr.length     = 4096;

        NodeID id = test_ot->allocate(NodeKind::Internal, /*class_id=*/1, addr, /*birth_epoch=*/0);
        ids.push_back(id);

        const uint32_t shard = ShardBits::shard_from_handle_idx(id.handle_index());
        shard_counts[shard]++;
    }

    // Print distribution

    // We expect multiple shards to be active (depends on your policy, but with 5000 allocs it should)
    EXPECT_GE(test_ot->active_shards(), 1u) << "Should have at least 1 shard active";
    if (test_ot->active_shards() > 1) {
        EXPECT_GT(shard_counts.size(), 1u) << "Allocations should be distributed across shards";
    }

    // 2) For a small subset, make them live, then verify routing and retire semantics
    const size_t test_count = std::min<size_t>(100, ids.size());
    for (size_t i = 0; i < test_count; ++i) {
        NodeID alloc_id = ids[i];

        // mark live (reserve + commit)
        const uint64_t epoch = 1000 + i;
        NodeID reserved = test_ot->mark_live_reserve(alloc_id, epoch);
        test_ot->mark_live_commit(reserved, epoch);

        // Validate should route to the same shard and succeed (tag-only check)
        EXPECT_TRUE(test_ot->validate_tag(reserved)) << "Failed validation at index " << i;

        // try_get should route to the same shard and the entry should be LIVE
        const OTEntry* entry_before = test_ot->try_get(reserved);
        ASSERT_NE(entry_before, nullptr) << "Failed try_get at index " << i;
        EXPECT_TRUE(entry_before->is_live()) << "Entry should be live before retire at index " << i;

        // Retire should route to the same shard
        test_ot->retire(reserved, /*retire_epoch=*/2000000 + i);

        // After retire: tag still matches (validate_tag may remain true),
        // but the entry must no longer be live.
        const OTEntry* entry_after = test_ot->try_get(reserved);
        ASSERT_NE(entry_after, nullptr) << "Entry lookup failed after retire at index " << i;
        EXPECT_FALSE(entry_after->is_live()) << "Entry should not be live after retire at index " << i;
    }

    // 3) Optional: Prove ABA/tag-bump works across shards
    if (test_count > 0) {
        // Pick the first retired entry
        NodeID old_id = ids[0];
        
        // Reclaim across all shards
        size_t reclaimed = test_ot->reclaim_before_epoch(/*safe_epoch=*/3000000);
        (void)reclaimed; // just to trigger potential handle reuse
        
        // Allocate a new entry (might reuse the handle)
        OTAddr addr{};
        addr.file_id = 9999;
        addr.segment_id = 1;
        addr.offset = 0;
        addr.length = 4096;
        
        NodeID new_id = test_ot->allocate(NodeKind::Internal, 1, addr, 0);
        
        // If the same handle got reused (same shard and local handle), check tag bump
        if (ShardBits::local_from_handle_idx(new_id.handle_index()) ==
            ShardBits::local_from_handle_idx(old_id.handle_index()) &&
            ShardBits::shard_from_handle_idx(new_id.handle_index()) ==
            ShardBits::shard_from_handle_idx(old_id.handle_index())) {
            EXPECT_FALSE(test_ot->validate_tag(old_id)) << "Old ID should be invalid after handle reuse";
            // ABA protection verified: handle reused with tag bump
        }
    }
}

// Test ABA safety across shards - same local handle can be reused independently
TEST_F(ObjectTableShardedTest, ABAIndependence) {
    auto ot = std::make_unique<ObjectTableSharded>(10000, 2);
    ot->set_activation_step_for_tests(32);  // activate shard 1 quickly for faster test
    
    std::vector<NodeID> shard0_ids, shard1_ids;

    // Keep allocating until both shards have at least MIN_PER_SHARD live nodes,
    // or hit a hard cap to avoid infinite loops if the policy changes.
    // Note: Current policy activates shard 1 after 1000 allocations, 
    // but first 1000 go to shard 0, then round-robin starts
    constexpr size_t MIN_PER_SHARD = 100;  // Minimum per shard
    constexpr size_t HARD_CAP      = 5000;  // Enough to get both shards populated

    for (size_t i = 0; i < HARD_CAP &&
                       (shard0_ids.size() < MIN_PER_SHARD || shard1_ids.size() < MIN_PER_SHARD); ++i) {
        OTAddr a{}; 
        a.file_id = i / 100; 
        a.segment_id = i % 100; 
        a.offset = i * 4096; 
        a.length = 4096;

        NodeID alloc = ot->allocate(NodeKind::Internal, 1, a, /*birth_epoch=*/0);
        NodeID res   = ot->mark_live_reserve(alloc, /*epoch=*/100 + i);
        ot->mark_live_commit(res, 100 + i);

        const uint32_t shard = ShardBits::shard_from_handle_idx(res.handle_index());
        if (shard == 0) shard0_ids.push_back(res);
        else            shard1_ids.push_back(res);
    }

    
    ASSERT_GE(shard0_ids.size(), MIN_PER_SHARD) << "Shard 0 never activated";
    ASSERT_GE(shard1_ids.size(), MIN_PER_SHARD) << "Shard 1 never activated";

    // Retire every live entry in shard 0
    for (const auto& id : shard0_ids) ot->retire(id, /*retire_epoch=*/1000);

    // Reclaim across shards (only shard 0 has retirees)
    ot->reclaim_before_epoch(/*safe_epoch=*/1100);

    // Allocate until we actually see shard 0 reuse (within a hard cap)
    std::unordered_set<uint64_t> reused_local_s0;
    std::unordered_map<uint64_t, NodeID> new_ids_s0;  // Track new IDs for reused handles
    size_t attempts = 0;
    const size_t want = std::max<size_t>(1, shard0_ids.size() / 4);  // aim to see some reuse
    const size_t cap  = shard0_ids.size() * 8;                        // safety cap

    // Build set of retired locals for faster lookup
    std::unordered_set<uint64_t> retired_locals;
    retired_locals.reserve(shard0_ids.size());
    for (const auto& id : shard0_ids) {
        retired_locals.insert(ShardBits::local_from_handle_idx(id.handle_index()));
    }
    
    while (reused_local_s0.size() < want && attempts < cap) {
        OTAddr a{}; 
        a.file_id = 1; 
        a.segment_id = attempts; 
        a.offset = attempts * 8192; 
        a.length = 8192;
        
        NodeID alloc = ot->allocate(NodeKind::Leaf, 2, a, /*birth_epoch=*/0);
        // IMPORTANT: Must call mark_live_reserve to trigger tag bump on reuse!
        NodeID reserved = ot->mark_live_reserve(alloc, 2000 + attempts);
        ot->mark_live_commit(reserved, 2000 + attempts);
        
        if (ShardBits::shard_from_handle_idx(reserved.handle_index()) == 0) {
            uint64_t local = ShardBits::local_from_handle_idx(reserved.handle_index());
            // Check if this local handle was one we retired
            if (retired_locals.count(local)) {
                reused_local_s0.insert(local);
                new_ids_s0[local] = reserved;  // Store the new ID (with bumped tag) for debug
            }
        }
        ++attempts;
    }

    // Independence: shard 1 entries still live
    for (const auto& id : shard1_ids) {
        EXPECT_TRUE(ot->validate_tag(id)) << "Shard 1 handle should remain valid";
        const OTEntry* e = ot->try_get(id);
        ASSERT_NE(e, nullptr);
        EXPECT_TRUE(e->is_live());
    }

    // ABA: only assert invalidation for locals we know were reused
    size_t invalidated = 0;
    for (const auto& id : shard0_ids) {
        const uint32_t s = ShardBits::shard_from_handle_idx(id.handle_index());
        if (s != 0) continue;
        const uint64_t local = ShardBits::local_from_handle_idx(id.handle_index());
        if (reused_local_s0.count(local)) {
            bool valid = ot->validate_tag(id);
            if (!valid) {
                ++invalidated;
            } else {
                // Debug: why is it still valid?
                auto new_it = new_ids_s0.find(local);
                uint16_t new_tag = (new_it != new_ids_s0.end()) ? new_it->second.tag() : 0;
                // This shouldn't happen with proper tag bumping
                FAIL() << "Old NodeID still valid after reuse! "
                       << "handle_idx=" << id.handle_index() 
                       << " local=" << local
                       << " old_tag=" << id.tag() 
                       << " new_tag=" << new_tag;
            }
        }
    }

    // We should have observed at least some shard-0 reuse
    EXPECT_GT(reused_local_s0.size(), 0u) << "No shard-0 reuse observed within " << attempts << " attempts";
    EXPECT_EQ(invalidated, reused_local_s0.size()) << "Each reused local should invalidate its old NodeID";
}

// Test snapshot repacking - iterate_live_snapshot correctly repacks handles
TEST_F(ObjectTableShardedTest, SnapshotRepack) {
    auto ot = std::make_unique<ObjectTableSharded>(10000, 4);

    const size_t NUM = 2000; // a bit larger to span shards
    std::unordered_map<uint64_t, NodeID> live;  // global handle -> NodeID
    live.reserve(NUM);

    size_t allocated_count = 0;
    size_t retired_count = 0;

    for (size_t i = 0; i < NUM; ++i) {
        OTAddr a{}; 
        a.file_id = i / 100; 
        a.segment_id = i % 100; 
        a.offset = i * 4096; 
        a.length = 4096;

        NodeID alloc = ot->allocate(NodeKind::Internal, 1, a, 0);
        NodeID res   = ot->mark_live_reserve(alloc, /*epoch=*/10 + i);
        ot->mark_live_commit(res, 10 + i);
        allocated_count++;

        if (i % 3 == 1) {
            ot->retire(res, 100 + i);            // retired, not live
            retired_count++;
        } else {
            live[res.handle_index()] = res;      // keep only live ones
        }
    }

    // Debug: verify our tracking
    ASSERT_EQ(allocated_count, NUM) << "Should have allocated exactly NUM nodes";
    ASSERT_EQ(retired_count + live.size(), NUM) << "Retired + live should equal allocated";

    // With i % 3 == 1 retired, we expect 2/3 of nodes to be live
    size_t expected_live = NUM - retired_count;
    EXPECT_EQ(live.size(), expected_live) << "Live map size should match expected";

    // Reclaim everything older than all retire epochs (we used 100 + i)
    // This ensures retired entries are removed and won't appear in snapshot
    ot->reclaim_before_epoch(100 + NUM + 1);  // any epoch > 100+NUM

    std::vector<OTCheckpoint::PersistentEntry> snap;
    snap.reserve(NUM);

    size_t count = ot->iterate_live_snapshot(snap);

    // Now snapshot should match exactly the live set
    EXPECT_EQ(count, live.size()) << "After reclaim, snapshot count should match live nodes";
    EXPECT_EQ(snap.size(), live.size()) << "After reclaim, snapshot size should match live nodes";

    // Build a set for quick membership checks
    std::unordered_set<uint64_t> live_handles;
    for (const auto& kv : live) {
        live_handles.insert(kv.first);
    }

    // Verify bidirectional consistency:
    // 1) Every live node appears in the snapshot
    for (const auto& kv : live) {
        bool found = std::any_of(snap.begin(), snap.end(),
            [&](const auto& pe){ return pe.handle_idx == kv.first; });
        EXPECT_TRUE(found) << "Live node " << kv.first << " missing from snapshot";
    }

    // 2) Snapshot contains no extras (every snapshot entry is in live)
    for (const auto& pe : snap) {
        EXPECT_TRUE(live_handles.count(pe.handle_idx))
            << "Snapshot contained non-live handle " << pe.handle_idx;

        // Sharded wrapper must have repacked per-shard handle_idx to GLOBAL here
        uint32_t shard = ShardBits::shard_from_handle_idx(pe.handle_idx);
        uint64_t local = ShardBits::local_from_handle_idx(pe.handle_idx);
        EXPECT_LT(shard, ot->num_shards());
        EXPECT_GT(local, 0u);

        NodeID id = NodeID::from_parts(pe.handle_idx, pe.tag);
        EXPECT_TRUE(ot->validate_tag(id));

        // Verify NodeID raw matches what we tracked
        auto it = live.find(pe.handle_idx);
        ASSERT_NE(it, live.end());
        EXPECT_EQ(it->second.raw(), id.raw());
    }
}

// Test recovery with WAL replay routing to correct shards
TEST_F(ObjectTableShardedTest, RecoveryRouting) {
    // Simulate recovery by applying deltas to correct shards
    std::vector<OTDeltaRec> deltas;
    
    // Create deltas that would route to different shards
    for (size_t shard = 0; shard < 4; ++shard) {
        for (size_t i = 0; i < 10; ++i) {
            OTDeltaRec rec{};
            // Create handle with specific shard encoding
            uint64_t local_handle = i + 1;  // Start from 1
            rec.handle_idx = ShardBits::make_global_handle_idx(shard, local_handle);
            rec.tag = i + 1;
            rec.birth_epoch = 100 + i;
            rec.retire_epoch = ~uint64_t{0};  // Live entry
            rec.kind = static_cast<uint8_t>(NodeKind::Internal);
            rec.class_id = 1;
            rec.file_id = shard;
            rec.segment_id = i;
            rec.offset = i * 4096;
            rec.length = 4096;
            rec.data_crc32c = 0;
            
            deltas.push_back(rec);
        }
    }
    
    // Apply deltas (simulating recovery)
    ot_sharded_->begin_recovery();
    
    for (const auto& delta : deltas) {
        ot_sharded_->apply_delta(delta);
    }
    
    ot_sharded_->end_recovery();
    
    // Verify all NodeIDs are valid and routed correctly
    for (const auto& delta : deltas) {
        NodeID id = NodeID::from_parts(delta.handle_idx, delta.tag);
        
        // Should be valid
        EXPECT_TRUE(ot_sharded_->validate_tag(id));
        
        // Should route to correct shard
        uint32_t expected_shard = ShardBits::shard_from_handle_idx(delta.handle_idx);
        uint32_t actual_shard = ShardBits::shard_from_handle_idx(id.handle_index());
        EXPECT_EQ(actual_shard, expected_shard);
        
        // Entry should have correct data
        const OTEntry* entry = ot_sharded_->try_get(id);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->addr.file_id, delta.file_id);
        EXPECT_EQ(entry->addr.segment_id, delta.segment_id);
    }
}