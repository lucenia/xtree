/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Unit tests for LRUCache - single-threaded core functionality
 */

#include "gtest/gtest.h"
#include "../src/lru.h"
#include "../src/lru.hpp"
#include <memory>
#include <random>
#include <thread>
#include <chrono>

using namespace xtree;

class LruUnitTest : public ::testing::Test {
protected:
    using Cache = LRUCache<int, int, LRUDeleteObject>;
    using CacheNoDel = LRUCache<int, int, LRUDeleteNone>;
    Cache cache;

    void SetUp() override {
        // Start fresh for each test
    }

    void TearDown() override {
        // Cache destructor will handle cleanup
    }
};

// ============= Core Operations =============

TEST_F(LruUnitTest, AddAndGet) {
    auto* node1 = cache.add(1, new int(10));
    EXPECT_NE(node1, nullptr);
    EXPECT_EQ(*cache.get(1), 10);

    auto* node2 = cache.add(2, new int(20));
    EXPECT_NE(node2, nullptr);
    EXPECT_EQ(*cache.get(2), 20);
}

TEST_F(LruUnitTest, PeekDoesNotPromote) {
    cache.add(1, new int(10));
    cache.add(2, new int(20));
    cache.add(3, new int(30));

    // Peek at 1 shouldn't affect LRU order
    auto* val = cache.peek(1);
    EXPECT_EQ(*val, 10);

    // 1 should still be LRU (evicted first)
    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 1);
    delete victim;
}

TEST_F(LruUnitTest, GetPromotesToMRU) {
    cache.add(1, new int(10));
    cache.add(2, new int(20));
    cache.add(3, new int(30));

    // Get 1 should promote it to MRU
    cache.get(1);

    // Now 2 should be LRU
    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 2);
    delete victim;
}

TEST_F(LruUnitTest, DuplicateIdPrevented) {
    cache.add(1, new int(10));

    // Adding same ID should assert in debug
    EXPECT_DEATH(cache.add(1, new int(20)), "Duplicate id");
}

// ============= Eviction Order =============

TEST_F(LruUnitTest, EvictionOrderLRU) {
    cache.add(1, new int(10));
    cache.add(2, new int(20));
    cache.add(3, new int(30));

    // Access order: 2, 3 (1 remains LRU)
    cache.get(2);
    cache.get(3);

    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 1);
    delete victim;

    // Next eviction should be 2
    victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 2);
    delete victim;
}

TEST_F(LruUnitTest, EvictionSkipsPinned) {
    auto* n1 = cache.add(1, new int(10));
    auto* n2 = cache.add(2, new int(20));
    auto* n3 = cache.add(3, new int(30));

    cache.pin(n1);  // Pin the LRU

    // Should evict 2, not 1
    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 2);
    delete victim;

    cache.unpin(n1);
}

// ============= Remove Operations =============

TEST_F(LruUnitTest, RemoveById) {
    cache.add(1, new int(42));
    cache.add(2, new int(84));

    // removeById returns the object (caller must delete)
    auto* removed = cache.removeById(1);
    ASSERT_NE(removed, nullptr);
    EXPECT_EQ(*removed, 42);
    delete removed;  // Caller owns the object now

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.size(), 1);
}

TEST_F(LruUnitTest, RemoveByObject) {
    auto* node = cache.add(1, new int(99));
    auto* obj = node->object;

    cache.removeByObject(obj);
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.size(), 0);
}

TEST_F(LruUnitTest, RemoveNonExistent) {
    cache.add(1, new int(10));

    // Remove non-existent ID
    auto* removed = cache.removeById(999);
    EXPECT_EQ(removed, nullptr);
    EXPECT_EQ(cache.size(), 1);

    // Remove non-existent object
    int dummy = 42;
    cache.removeByObject(&dummy);
    EXPECT_EQ(cache.size(), 1);
}

// ============= Pin/Unpin Semantics =============

TEST_F(LruUnitTest, PinUnpinBasic) {
    auto* node = cache.add(1, new int(11));
    EXPECT_FALSE(Cache::is_pinned(node));

    cache.pin(node);
    EXPECT_TRUE(Cache::is_pinned(node));

    cache.unpin(node);
    EXPECT_FALSE(Cache::is_pinned(node));
}

TEST_F(LruUnitTest, MultiplePinUnpin) {
    auto* node = cache.add(1, new int(11));

    // Multiple pins
    cache.pin(node);
    cache.pin(node);
    EXPECT_TRUE(Cache::is_pinned(node));

    // First unpin - still pinned
    cache.unpin(node);
    EXPECT_TRUE(Cache::is_pinned(node));

    // Second unpin - now unpinned
    cache.unpin(node);
    EXPECT_FALSE(Cache::is_pinned(node));
}

TEST_F(LruUnitTest, PinnedNotEvictable) {
    auto* n1 = cache.add(1, new int(10));
    auto* n2 = cache.add(2, new int(20));

    cache.pin(n1);
    cache.pin(n2);

    // All pinned - nothing evictable
    EXPECT_EQ(cache.removeOne(), nullptr);
    EXPECT_EQ(cache.evictableCount(), 0);
    EXPECT_EQ(cache.pinnedCount(), 2);

    cache.unpin(n1);
    EXPECT_EQ(cache.evictableCount(), 1);
    EXPECT_EQ(cache.pinnedCount(), 1);

    // Now can evict n1
    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    EXPECT_EQ(victim->id, 1);
    delete victim;
}

TEST_F(LruUnitTest, ScopedPinRAII) {
    auto* node = cache.add(1, new int(123));

    {
        ScopedPin<Cache, Cache::Node> pin(cache, node);
        EXPECT_TRUE(Cache::is_pinned(node));

        // Should not be evictable while pinned
        EXPECT_EQ(cache.removeOne(), nullptr);
    }
    // Automatically unpinned when ScopedPin destroyed
    EXPECT_FALSE(Cache::is_pinned(node));

    // Now evictable
    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    delete victim;
}

// ============= Clear and State Management =============

TEST_F(LruUnitTest, ClearEmpty) {
    cache.clear();  // Clear empty cache shouldn't crash
    EXPECT_EQ(cache.size(), 0);
}

TEST_F(LruUnitTest, ClearWithNodes) {
    cache.add(1, new int(5));
    cache.add(2, new int(6));
    cache.add(3, new int(7));

    EXPECT_EQ(cache.size(), 3);

    cache.clear();
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(cache.get(3), nullptr);
}

TEST_F(LruUnitTest, ClearWithPinnedNodes) {
    auto* n1 = cache.add(1, new int(10));
    auto* n2 = cache.add(2, new int(20));

    cache.pin(n1);
    cache.pin(n2);

    cache.clear();  // Should handle pinned nodes correctly
    EXPECT_EQ(cache.size(), 0);
}

TEST_F(LruUnitTest, ReuseAfterClear) {
    cache.add(1, new int(10));
    cache.clear();

    // Should be able to add again
    auto* node = cache.add(1, new int(20));
    EXPECT_NE(node, nullptr);
    EXPECT_EQ(*cache.get(1), 20);
}

// ============= Delete Policies =============

TEST_F(LruUnitTest, DeleteObjectPolicy) {
    // Already tested with Cache (LRUDeleteObject)
    cache.add(1, new int(42));
    cache.clear();
    // Memory freed by destructor
}

TEST_F(LruUnitTest, DeleteNonePolicy) {
    CacheNoDel cacheNoDel;
    int value = 42;
    cacheNoDel.add(1, &value);

    cacheNoDel.clear();
    // value should still be valid (not deleted)
    EXPECT_EQ(value, 42);
}

TEST_F(LruUnitTest, DeleteArrayPolicy) {
    LRUCache<int, int, LRUDeleteArray> cacheArr;
    cacheArr.add(1, new int[10]);
    cacheArr.clear();
    // Array deleted by destructor
}

// ============= Stats =============

TEST_F(LruUnitTest, StatsAccuracy) {
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.evictableCount(), 0);
    EXPECT_EQ(cache.pinnedCount(), 0);

    auto* n1 = cache.add(1, new int(10));
    auto* n2 = cache.add(2, new int(20));
    auto* n3 = cache.add(3, new int(30));

    EXPECT_EQ(cache.size(), 3);
    EXPECT_EQ(cache.evictableCount(), 3);
    EXPECT_EQ(cache.pinnedCount(), 0);

    cache.pin(n1);
    cache.pin(n2);

    EXPECT_EQ(cache.size(), 3);
    EXPECT_EQ(cache.evictableCount(), 1);
    EXPECT_EQ(cache.pinnedCount(), 2);

    cache.unpin(n1);
    EXPECT_EQ(cache.evictableCount(), 2);
    EXPECT_EQ(cache.pinnedCount(), 1);
}

// ============= Stress Testing =============

TEST_F(LruUnitTest, ChurnSimulation) {
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> dist(1, 1000);

    const int iterations = 10000;
    int adds = 0, removes = 0, gets = 0;

    for (int i = 0; i < iterations; i++) {
        int op = dist(rng) % 10;
        int id = dist(rng) % 100;  // Limited ID range for collisions

        if (op < 5) {  // 50% adds
            if (cache.peek(id) == nullptr) {
                cache.add(id, new int(id * 10));
                adds++;
            }
        } else if (op < 8) {  // 30% gets
            cache.get(id);
            gets++;
        } else if (op == 8) {  // 10% removes
            cache.removeById(id);
            removes++;
        } else {  // 10% evictions
            auto* victim = cache.removeOne();
            if (victim) {
                removes++;
                delete victim;
            }
        }

        // Verify consistency
        size_t total = cache.size();
        size_t evictable = cache.evictableCount();
        size_t pinned = cache.pinnedCount();
        EXPECT_EQ(total, evictable + pinned);
    }

    // Final cleanup
    cache.clear();
    EXPECT_EQ(cache.size(), 0);
}

TEST_F(LruUnitTest, ConcurrentPinUnpin) {
    // Test thread-safety of pin/unpin
    auto* node = cache.add(1, new int(42));

    const int numThreads = 10;
    const int opsPerThread = 1000;
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < opsPerThread; i++) {
                if (i % 2 == 0) {
                    cache.pin(node);
                } else {
                    cache.unpin(node);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Should be unpinned (even number of ops)
    EXPECT_FALSE(Cache::is_pinned(node));
}

// ============= Edge Cases =============

TEST_F(LruUnitTest, EmptyCacheOperations) {
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.peek(1), nullptr);
    EXPECT_EQ(cache.removeOne(), nullptr);
    EXPECT_EQ(cache.removeById(1), nullptr);

    int dummy = 42;
    cache.removeByObject(&dummy);  // Should not crash
}

TEST_F(LruUnitTest, SingleNodeAllOperations) {
    auto* node = cache.add(1, new int(42));

    // Get and peek
    EXPECT_EQ(*cache.get(1), 42);
    EXPECT_EQ(*cache.peek(1), 42);

    // Pin/unpin
    cache.pin(node);
    EXPECT_EQ(cache.removeOne(), nullptr);  // Can't evict pinned
    cache.unpin(node);

    // Remove
    auto* victim = cache.removeOne();
    ASSERT_NE(victim, nullptr);
    delete victim;

    EXPECT_EQ(cache.size(), 0);
}