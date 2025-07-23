/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Stress tests for ShardedLRUCache - High concurrency and load testing
 *
 * These tests verify the sharded cache's correctness under extreme conditions:
 * - Millions of operations
 * - High thread contention
 * - Rapid add/remove/get cycles
 * - Concurrent pin/unpin operations
 * - Eviction under memory pressure
 */

#include "gtest/gtest.h"
#include "../src/lru.hpp"  // Need implementation for template instantiation
#include "../src/lru_sharded.h"
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <iostream>

using namespace xtree;

class ShardedLruStressTest : public ::testing::Test {
protected:
    static constexpr size_t NUM_SHARDS = 64;
    static constexpr size_t NUM_KEYS = 1'000'000;
    static constexpr size_t NUM_THREADS = 8;
    static constexpr size_t RUNTIME_SECONDS = 5;

    using Cache = ShardedLRUCache<int, uint64_t, LRUDeleteObject>;
    std::unique_ptr<Cache> cache;

    void SetUp() override {
        // Create cache with global object map for O(1) removeByObject
        cache = std::make_unique<Cache>(NUM_SHARDS, true);
    }

    void TearDown() override {
        cache.reset();
    }
};

TEST_F(ShardedLruStressTest, HighChurnAddRemoveGet) {
    std::atomic<bool> running{true};
    std::atomic<size_t> totalOps{0};
    std::atomic<size_t> addOps{0};
    std::atomic<size_t> removeOps{0};
    std::atomic<size_t> getOps{0};
    std::atomic<size_t> hitCount{0};

    auto worker = [&](int tid) {
        std::mt19937_64 rng(tid + 12345);
        std::uniform_int_distribution<uint64_t> dist(0, NUM_KEYS - 1);
        size_t localOps = 0;

        while (running.load(std::memory_order_relaxed)) {
            uint64_t key = dist(rng);

            // Random operation: 40% add, 30% remove, 30% get
            int op = dist(rng) % 10;
            if (op < 4) {  // 40% add
                // Use acquirePinned for atomic get-or-create
                auto result = cache->acquirePinned(key, new int(static_cast<int>(key)));
                if (result.created) {
                    addOps.fetch_add(1, std::memory_order_relaxed);
                }
                // acquirePinned handles cleanup of unused object automatically
                // Unpin immediately since we're just adding
                cache->unpin(result.node, key);
            } else if (op < 7) {  // 30% remove
                int* removed = cache->removeById(key);
                if (removed) {
                    ASSERT_EQ(*removed, static_cast<int>(key)) << "Removed value mismatch";
                    delete removed;  // We own it after removeById
                    removeOps.fetch_add(1, std::memory_order_relaxed);
                }
            } else {  // 30% get
                int* val = cache->get(key);
                if (val) {
                    ASSERT_EQ(*val, static_cast<int>(key)) << "Get value mismatch";
                    hitCount.fetch_add(1, std::memory_order_relaxed);
                }
                getOps.fetch_add(1, std::memory_order_relaxed);
            }
            localOps++;
        }
        totalOps.fetch_add(localOps, std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    auto startTime = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(worker, static_cast<int>(i));
    }

    std::this_thread::sleep_for(std::chrono::seconds(RUNTIME_SECONDS));
    running.store(false, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Print statistics
    std::cout << "\n=== High Churn Test Results ===" << std::endl;
    std::cout << "Runtime: " << duration.count() << "ms" << std::endl;
    std::cout << "Total operations: " << totalOps.load() << std::endl;
    std::cout << "  Adds: " << addOps.load() << std::endl;
    std::cout << "  Removes: " << removeOps.load() << std::endl;
    std::cout << "  Gets: " << getOps.load() << std::endl;
    std::cout << "  Hit rate: " << (hitCount.load() * 100.0 / getOps.load()) << "%" << std::endl;
    std::cout << "Ops/sec: " << (totalOps.load() * 1000 / duration.count()) << std::endl;

    // Verify cache invariants
    auto stats = cache->getStats();
    std::cout << "Final cache state:" << std::endl;
    std::cout << "  Total nodes: " << stats.totalNodes << std::endl;
    std::cout << "  Pinned: " << stats.totalPinned << std::endl;
    std::cout << "  Evictable: " << stats.totalEvictable << std::endl;

    // Basic sanity checks
    ASSERT_EQ(stats.totalNodes, stats.totalPinned + stats.totalEvictable)
        << "Node count mismatch";
    ASSERT_LE(stats.totalNodes, NUM_KEYS)
        << "More nodes than possible keys";
}

TEST_F(ShardedLruStressTest, ConcurrentPinUnpin) {
    std::atomic<bool> running{true};
    std::atomic<size_t> totalPinOps{0};
    std::atomic<size_t> createdCount{0};
    std::atomic<size_t> conflicts{0};

    auto worker = [&](int tid) {
        std::mt19937_64 rng(tid + 98765);
        std::uniform_int_distribution<uint64_t> dist(0, 1000);  // Smaller key range for more contention
        size_t localPins = 0;

        while (running.load(std::memory_order_relaxed)) {
            uint64_t key = dist(rng);

            // Use ShardedScopedAcquire for atomic get-or-create with automatic unpin
            {
                ShardedScopedAcquire<Cache> acquire(*cache, key, new int(static_cast<int>(key)));
                ASSERT_NE(acquire.get(), nullptr) << "acquirePinned should always return a node";

                if (acquire.wasCreated()) {
                    createdCount.fetch_add(1, std::memory_order_relaxed);
                } else {
                    conflicts.fetch_add(1, std::memory_order_relaxed);
                }

                // Node is pinned for the scope of acquire

                // While pinned, removal must fail:
                int* shouldBeNull = cache->removeById(key);
                ASSERT_EQ(shouldBeNull, nullptr) << "removeById succeeded on a pinned node!";

                // Access while pinned - should always succeed
                int* val = cache->get(key);
                ASSERT_NE(val, nullptr) << "Pinned node disappeared!";
                ASSERT_EQ(*val, static_cast<int>(key)) << "Value corruption detected";

                // Try to evict while pinned - must NOT evict our pinned node
                auto* evicted = cache->removeOne();
                if (evicted) {
                    // Verify we didn't evict the pinned node
                    ASSERT_NE(evicted, acquire.get()) << "Evicted a pinned node!";
                    ASSERT_NE(evicted->id, key) << "Evicted node has same ID as pinned node!";

                    // OK to delete: Node destructor frees object per delete policy
                    delete evicted;
                }

                // Simulate some work while pinned
                for (int i = 0; i < 10; ++i) {
                    int* peeked = cache->peek(key);
                    ASSERT_NE(peeked, nullptr) << "Pinned node not peekable!";
                    ASSERT_EQ(*peeked, static_cast<int>(key)) << "Peek value mismatch";
                }

                localPins++;
            } // auto unpin here via ~ShardedScopedAcquire()

            // After unpin, optionally remove
            if (dist(rng) % 2 == 0) {  // 50% chance to remove
                int* removed = cache->removeById(key);
                if (removed) {
                    ASSERT_EQ(*removed, static_cast<int>(key)) << "Post-unpin value mismatch";
                    delete removed;
                }
            }
        }
        totalPinOps.fetch_add(localPins, std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    auto startTime = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(worker, static_cast<int>(i));
    }

    std::this_thread::sleep_for(std::chrono::seconds(RUNTIME_SECONDS));
    running.store(false, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    std::cout << "\n=== Concurrent Pin/Unpin Test Results ===" << std::endl;
    std::cout << "Runtime: " << duration.count() << "ms" << std::endl;
    std::cout << "Total pin operations: " << totalPinOps.load() << std::endl;
    std::cout << "Created nodes: " << createdCount.load() << std::endl;
    std::cout << "Key reuse conflicts: " << conflicts.load() << std::endl;
    std::cout << "Pin ops/sec: " << (totalPinOps.load() * 1000 / duration.count()) << std::endl;

    auto stats = cache->getStats();
    std::cout << "Final cache state:" << std::endl;
    std::cout << "  Total nodes: " << stats.totalNodes << std::endl;
    std::cout << "  Currently pinned: " << stats.totalPinned << std::endl;

    // All should be unpinned at the end
    ASSERT_EQ(stats.totalPinned, 0) << "Nodes still pinned after test completion";
}

TEST_F(ShardedLruStressTest, EvictionUnderPressure) {
    std::atomic<bool> running{true};
    std::atomic<size_t> totalEvictions{0};
    std::atomic<size_t> failedEvictions{0};

    // Pre-fill cache with many entries
    std::cout << "\nPre-filling cache with entries..." << std::endl;
    for (size_t i = 0; i < 10000; ++i) {
        cache->add(i, new int(static_cast<int>(i)));
    }

    auto stats = cache->getStats();
    std::cout << "Initial cache size: " << stats.totalNodes << std::endl;

    auto adder = [&](int tid) {
        std::mt19937_64 rng(tid + 55555);
        std::uniform_int_distribution<uint64_t> dist(10000, NUM_KEYS - 1);

        while (running.load(std::memory_order_relaxed)) {
            uint64_t key = dist(rng);
            if (!cache->peek(key)) {
                cache->add(key, new int(static_cast<int>(key)));
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    };

    auto evictor = [&]() {
        while (running.load(std::memory_order_relaxed)) {
            auto* node = cache->removeOne();
            if (node) {
                delete node;  // Clean up the evicted node
                totalEvictions.fetch_add(1, std::memory_order_relaxed);
            } else {
                failedEvictions.fetch_add(1, std::memory_order_relaxed);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    };

    std::vector<std::thread> threads;

    // Start adder threads
    for (size_t i = 0; i < NUM_THREADS / 2; ++i) {
        threads.emplace_back(adder, static_cast<int>(i));
    }

    // Start evictor threads
    for (size_t i = 0; i < NUM_THREADS / 2; ++i) {
        threads.emplace_back(evictor);
    }

    std::this_thread::sleep_for(std::chrono::seconds(RUNTIME_SECONDS));
    running.store(false, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    stats = cache->getStats();
    std::cout << "\n=== Eviction Under Pressure Results ===" << std::endl;
    std::cout << "Total evictions: " << totalEvictions.load() << std::endl;
    std::cout << "Failed eviction attempts: " << failedEvictions.load() << std::endl;
    std::cout << "Final cache size: " << stats.totalNodes << std::endl;
    std::cout << "Eviction rate: " << (totalEvictions.load() * 100.0 /
        (totalEvictions.load() + failedEvictions.load())) << "%" << std::endl;

    // Verify all remaining nodes are valid
    for (size_t i = 0; i < NUM_KEYS; ++i) {
        int* val = cache->peek(i);
        if (val) {
            ASSERT_EQ(*val, static_cast<int>(i)) << "Value corruption at key " << i;
        }
    }
}

TEST_F(ShardedLruStressTest, ShardDistribution) {
    // Test that keys are well distributed across shards
    std::cout << "\n=== Testing Shard Distribution ===" << std::endl;

    // Insert many keys
    for (size_t i = 0; i < 100000; ++i) {
        cache->add(i, new int(static_cast<int>(i)));
    }

    auto stats = cache->getStats();

    // Calculate distribution statistics
    double mean = static_cast<double>(stats.totalNodes) / stats.nodesPerShard.size();
    double variance = 0;
    size_t minNodes = stats.totalNodes;
    size_t maxNodes = 0;

    for (size_t count : stats.nodesPerShard) {
        variance += (count - mean) * (count - mean);
        minNodes = std::min(minNodes, count);
        maxNodes = std::max(maxNodes, count);
    }
    variance /= stats.nodesPerShard.size();
    double stddev = std::sqrt(variance);

    std::cout << "Shard distribution:" << std::endl;
    std::cout << "  Mean nodes per shard: " << mean << std::endl;
    std::cout << "  Std deviation: " << stddev << std::endl;
    std::cout << "  Min nodes in shard: " << minNodes << std::endl;
    std::cout << "  Max nodes in shard: " << maxNodes << std::endl;
    std::cout << "  Coefficient of variation: " << (stddev / mean * 100) << "%" << std::endl;

    // Verify reasonable distribution (within 20% of mean)
    ASSERT_LT(stddev / mean, 0.2) << "Poor shard distribution";
    ASSERT_GT(minNodes, mean * 0.5) << "Some shards severely underutilized";
    ASSERT_LT(maxNodes, mean * 1.5) << "Some shards severely overloaded";
}