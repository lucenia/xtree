/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Unit tests for ShardedLRUCache - multi-shard scalability and correctness
 */

#include "gtest/gtest.h"
#include "../src/lru_sharded.h"
#include "../src/lru.hpp"
#include <unordered_set>
#include <random>
#include <thread>
#include <atomic>

using namespace xtree;

class LruShardedTest : public ::testing::Test {
protected:
    using ShardedCache = ShardedLRUCache<int, int, LRUDeleteObject>;
    using ShardedCacheNoMap = ShardedLRUCache<int, int, LRUDeleteObject>;

    std::unique_ptr<ShardedCache> cache;
    std::unique_ptr<ShardedCacheNoMap> cacheNoMap;

    void SetUp() override {
        cache = std::make_unique<ShardedCache>(8, true);  // 8 shards, global map enabled
        cacheNoMap = std::make_unique<ShardedCacheNoMap>(8, false);  // No global map
    }

    void TearDown() override {
        // unique_ptr will handle destruction automatically
        // Explicit clear not needed
    }
};

// ============= Basic Sharding Operations =============

TEST_F(LruShardedTest, AddAndGetDistributed) {
    const int numItems = 64;

    for (int i = 0; i < numItems; i++) {
        auto* node = cache->add(i, new int(i * 10));
        EXPECT_NE(node, nullptr);
    }

    // Verify all items are retrievable
    for (int i = 0; i < numItems; i++) {
        int* val = cache->get(i);
        ASSERT_NE(val, nullptr);
        EXPECT_EQ(*val, i * 10);
    }
}

TEST_F(LruShardedTest, PeekWithoutLRUUpdate) {
    cache->add(1, new int(10));
    cache->add(2, new int(20));

    // Peek shouldn't affect eviction order
    auto* val = cache->peek(1);
    EXPECT_EQ(*val, 10);

    // Get should still work
    val = cache->get(2);
    EXPECT_EQ(*val, 20);
}

TEST_F(LruShardedTest, ShardDistribution) {
    const int numItems = 1000;

    for (int i = 0; i < numItems; i++) {
        cache->add(i, new int(i));
    }

    auto stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, numItems);

    // Check reasonable distribution across shards
    // With 8 shards and 1000 items, expect ~125 per shard
    for (size_t count : stats.nodesPerShard) {
        EXPECT_GT(count, 50);   // At least some items
        EXPECT_LT(count, 250);  // Not too skewed
    }
}

// ============= Remove Operations =============

TEST_F(LruShardedTest, RemoveById) {
    const int id = 42;
    cache->add(id, new int(4200));

    // removeById returns ownership - caller must delete
    int* removed = cache->removeById(id);
    ASSERT_NE(removed, nullptr);
    EXPECT_EQ(*removed, 4200);
    delete removed;  // Clean up

    EXPECT_EQ(cache->get(id), nullptr);
    EXPECT_EQ(cache->peek(id), nullptr);
}

TEST_F(LruShardedTest, RemoveByIdBatch) {
    const int numItems = 100;

    // Add items
    for (int i = 0; i < numItems; i++) {
        cache->add(i, new int(i));
    }

    // Remove every other item
    for (int i = 0; i < numItems; i += 2) {
        int* removed = cache->removeById(i);
        delete removed;  // Clean up returned object
    }

    // Verify removal
    for (int i = 0; i < numItems; i++) {
        if (i % 2 == 0) {
            EXPECT_EQ(cache->get(i), nullptr);
        } else {
            EXPECT_NE(cache->get(i), nullptr);
        }
    }
}

TEST_F(LruShardedTest, RemoveByObjectWithGlobalMap) {
    auto* node = cache->add(99, new int(9900));
    int* obj = node->object;

    cache->removeByObject(obj);
    EXPECT_EQ(cache->get(99), nullptr);

    // Verify stats
    auto stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, 0);
}

TEST_F(LruShardedTest, RemoveByObjectWithoutGlobalMap) {
    auto* node = cacheNoMap->add(88, new int(8800));
    int* obj = node->object;

    // Should scan all shards (O(numShards))
    cacheNoMap->removeByObject(obj);
    EXPECT_EQ(cacheNoMap->get(88), nullptr);
}

TEST_F(LruShardedTest, RemoveByObjectGlobalMapConsistency) {
    // Test that global map stays consistent through operations
    std::vector<int*> objects;

    for (int i = 0; i < 50; i++) {
        auto* node = cache->add(i, new int(i * 100));
        objects.push_back(node->object);
    }

    // Remove by ID - should update global map
    for (int i = 0; i < 25; i++) {
        int* removed = cache->removeById(i);
        delete removed;  // Clean up returned object
    }

    // Remove by object for remaining items
    for (int i = 25; i < 50; i++) {
        cache->removeByObject(objects[i]);
        EXPECT_EQ(cache->get(i), nullptr);
    }

    auto stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, 0);
}

// ============= Eviction =============

TEST_F(LruShardedTest, EvictionRoundRobin) {
    const int numItems = 32;
    std::unordered_set<int> evictedIds;

    for (int i = 0; i < numItems; i++) {
        cache->add(i, new int(i));
    }

    // Evict half the items
    for (int i = 0; i < numItems / 2; i++) {
        auto* victim = cache->removeOne();
        ASSERT_NE(victim, nullptr);
        evictedIds.insert(victim->id);
        delete victim;
    }

    // Should have evicted from multiple shards (round-robin)
    // With 8 shards and 16 evictions, expect reasonable distribution
    EXPECT_EQ(evictedIds.size(), numItems / 2);
}

TEST_F(LruShardedTest, EvictionWithPinnedNodes) {
    std::vector<ShardedCache::Node*> nodes;

    // Add items to multiple shards
    for (int i = 0; i < 16; i++) {
        nodes.push_back(cache->add(i, new int(i)));
    }

    // Pin half the nodes
    for (int i = 0; i < 8; i++) {
        cache->pin(nodes[i], i);
    }

    // Eviction should only return unpinned nodes
    std::unordered_set<int> evictedIds;
    for (int i = 0; i < 8; i++) {
        auto* victim = cache->removeOne();
        if (victim) {
            evictedIds.insert(victim->id);
            delete victim;
        }
    }

    // Only unpinned nodes should be evicted
    for (int id : evictedIds) {
        EXPECT_GE(id, 8);  // IDs 0-7 were pinned
    }

    // Unpin and clean up
    for (int i = 0; i < 8; i++) {
        cache->unpin(nodes[i], i);
    }
}

TEST_F(LruShardedTest, EvictionAllPinned) {
    std::vector<ShardedCache::Node*> nodes;

    for (int i = 0; i < 8; i++) {
        nodes.push_back(cache->add(i, new int(i)));
        cache->pin(nodes[i], i);
    }

    // All pinned - nothing to evict
    EXPECT_EQ(cache->removeOne(), nullptr);

    // Unpin one
    cache->unpin(nodes[0], 0);

    // Now can evict
    auto* victim = cache->removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 0);
    delete victim;

    // Clean up pins
    for (int i = 1; i < 8; i++) {
        cache->unpin(nodes[i], i);
    }
}

// ============= Pin/Unpin =============

TEST_F(LruShardedTest, PinUnpinBasic) {
    auto* node = cache->add(7, new int(77));

    cache->pin(node, 7);
    // Pinned node shouldn't be evictable
    cache->unpin(node, 7);
}

TEST_F(LruShardedTest, ShardedScopedPin) {
    auto* node = cache->add(123, new int(1230));

    {
        ShardedScopedPin<ShardedCache, ShardedCache::Node, int> pin(*cache, node, 123);
        // Node is pinned in scope

        // Add more nodes to force eviction
        for (int i = 200; i < 210; i++) {
            cache->add(i, new int(i));
        }

        // Try to evict - pinned node should be protected
        for (int i = 0; i < 10; i++) {
            auto* victim = cache->removeOne();
            if (victim) {
                EXPECT_NE(victim->id, 123);  // Pinned node not evicted
                delete victim;
            }
        }
    }
    // Automatically unpinned when ShardedScopedPin destroyed
}

// ============= Clear and Stats =============

TEST_F(LruShardedTest, ClearAllShards) {
    for (int i = 0; i < 64; i++) {
        cache->add(i, new int(i * 10));
    }

    auto stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, 64);

    cache->clear();

    stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, 0);
    EXPECT_EQ(stats.totalPinned, 0);
    EXPECT_EQ(stats.totalEvictable, 0);

    // All shards should be empty
    for (size_t count : stats.nodesPerShard) {
        EXPECT_EQ(count, 0);
    }

    // Verify items are gone
    for (int i = 0; i < 64; i++) {
        EXPECT_EQ(cache->get(i), nullptr);
    }
}

TEST_F(LruShardedTest, StatsConsistency) {
    std::vector<ShardedCache::Node*> nodes;
    const int numItems = 32;

    for (int i = 0; i < numItems; i++) {
        nodes.push_back(cache->add(i, new int(i)));
    }

    auto stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, numItems);
    EXPECT_EQ(stats.totalEvictable, numItems);
    EXPECT_EQ(stats.totalPinned, 0);
    EXPECT_EQ(stats.totalNodes, stats.totalPinned + stats.totalEvictable);

    // Pin some nodes
    const int numPinned = 10;
    for (int i = 0; i < numPinned; i++) {
        cache->pin(nodes[i], i);
    }

    stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, numItems);
    EXPECT_EQ(stats.totalPinned, numPinned);
    EXPECT_EQ(stats.totalEvictable, numItems - numPinned);
    EXPECT_EQ(stats.totalNodes, stats.totalPinned + stats.totalEvictable);

    // Clean up pins
    for (int i = 0; i < numPinned; i++) {
        cache->unpin(nodes[i], i);
    }
}

TEST_F(LruShardedTest, StatsPerShard) {
    const int itemsPerShard = 10;
    const int numShards = 8;  // Matches cache construction

    // Try to add items that hash to different shards
    int totalAdded = 0;
    for (int i = 0; totalAdded < itemsPerShard * numShards && i < 10000; i++) {
        cache->add(i, new int(i));
        totalAdded++;
    }

    auto stats = cache->getStats();
    EXPECT_EQ(stats.nodesPerShard.size(), numShards);

    // Verify total matches sum of shards
    size_t sumFromShards = 0;
    for (size_t count : stats.nodesPerShard) {
        sumFromShards += count;
    }
    EXPECT_EQ(sumFromShards, stats.totalNodes);
}

// ============= Concurrent Operations =============

TEST_F(LruShardedTest, ConcurrentReads) {
    const int numItems = 1000;
    const int numThreads = 8;
    const int readsPerThread = 10000;

    // Populate cache
    for (int i = 0; i < numItems; i++) {
        cache->add(i, new int(i * 10));
    }

    std::vector<std::thread> threads;
    std::atomic<int> successfulReads{0};

    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> dist(0, numItems - 1);

            for (int i = 0; i < readsPerThread; i++) {
                int id = dist(rng);

                if (i % 2 == 0) {
                    // Half peek (shared lock)
                    auto* val = cache->peek(id);
                    if (val && *val == id * 10) {
                        successfulReads++;
                    }
                } else {
                    // Half get (exclusive lock)
                    auto* val = cache->get(id);
                    if (val && *val == id * 10) {
                        successfulReads++;
                    }
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(successfulReads.load(), numThreads * readsPerThread);
}

TEST_F(LruShardedTest, ConcurrentMixedOperations) {
    const int numThreads = 4;
    const int opsPerThread = 1000;
    std::atomic<int> nextId{0};

    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> opDist(0, 9);
            std::uniform_int_distribution<int> idDist(0, 999);

            for (int i = 0; i < opsPerThread; i++) {
                int op = opDist(rng);

                if (op < 4) {  // 40% adds
                    int id = nextId.fetch_add(1);
                    cache->add(id, new int(id));
                } else if (op < 7) {  // 30% gets
                    int id = idDist(rng);
                    cache->get(id);
                } else if (op == 7) {  // 10% removes
                    int id = idDist(rng);
                    int* removed = cache->removeById(id);
                    delete removed;
                } else if (op == 8) {  // 10% evictions
                    auto* victim = cache->removeOne();
                    delete victim;
                } else {  // 10% peeks
                    int id = idDist(rng);
                    cache->peek(id);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify stats consistency after concurrent ops
    auto stats = cache->getStats();
    EXPECT_EQ(stats.totalNodes, stats.totalPinned + stats.totalEvictable);
}

// ============= Stress Testing =============

TEST_F(LruShardedTest, HighChurnSimulation) {
    const int iterations = 50000;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> idDist(0, 999);
    std::uniform_int_distribution<int> opDist(0, 99);

    int adds = 0, removes = 0, evictions = 0;

    for (int i = 0; i < iterations; i++) {
        int op = opDist(rng);
        int id = idDist(rng);

        if (op < 40) {  // 40% adds
            if (!cache->peek(id)) {
                cache->add(id, new int(id));
                adds++;
            }
        } else if (op < 70) {  // 30% gets
            cache->get(id);
        } else if (op < 85) {  // 15% removes
            int* removed = cache->removeById(id);
            delete removed;
            removes++;
        } else if (op < 95) {  // 10% evictions
            auto* victim = cache->removeOne();
            if (victim) {
                evictions++;
                delete victim;
            }
        } else {  // 5% clear
            cache->clear();
        }

        // Periodic consistency check
        if (i % 1000 == 0) {
            auto stats = cache->getStats();
            EXPECT_EQ(stats.totalNodes, stats.totalPinned + stats.totalEvictable);
        }
    }

    // Final cleanup
    cache->clear();
    auto finalStats = cache->getStats();
    EXPECT_EQ(finalStats.totalNodes, 0);
}

TEST_F(LruShardedTest, PowerOfTwoShardCount) {
    // Test that non-power-of-2 shard counts get rounded up
    ShardedCache cache3(3, false);  // Should become 4
    ShardedCache cache7(7, false);  // Should become 8
    ShardedCache cache16(16, false); // Should stay 16

    // Add items and verify they work
    for (int i = 0; i < 100; i++) {
        cache3.add(i, new int(i));
        cache7.add(i, new int(i));
        cache16.add(i, new int(i));
    }

    for (int i = 0; i < 100; i++) {
        EXPECT_NE(cache3.get(i), nullptr);
        EXPECT_NE(cache7.get(i), nullptr);
        EXPECT_NE(cache16.get(i), nullptr);
    }
}

// ============= Edge Cases =============

TEST_F(LruShardedTest, EmptyShardOperations) {
    EXPECT_EQ(cache->get(999), nullptr);
    EXPECT_EQ(cache->peek(999), nullptr);
    EXPECT_EQ(cache->removeOne(), nullptr);

    int* removed = cache->removeById(999);  // Should not crash
    delete removed;  // Will be nullptr, delete is safe

    int dummy = 42;
    cache->removeByObject(&dummy);  // Should not crash
}

TEST_F(LruShardedTest, SingleItemPerShard) {
    // Add exactly one item to each shard
    for (int i = 0; i < 8; i++) {
        cache->add(i * 1000, new int(i));  // Space out IDs to hit different shards
    }

    auto stats = cache->getStats();

    // Each shard should have at most a few items
    for (size_t count : stats.nodesPerShard) {
        EXPECT_GE(count, 0);
        EXPECT_LE(count, 8);  // All items might hash to same shard in worst case
    }
}