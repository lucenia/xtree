/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <random>
#include <climits>
#include "persistence/mvcc_context.h"

using namespace xtree::persist;
using namespace std::chrono_literals;

class MVCCContextTest : public ::testing::Test {
protected:
    MVCCContext mvcc;
    
    void SetUp() override {
        // Clear any thread-local state from previous tests
        mvcc.deregister_thread();
    }
    void TearDown() override {
        // Clean up thread-local state
        mvcc.deregister_thread();
    }
};

TEST_F(MVCCContextTest, PinAndUnpin) {
    // Register thread and get pin
    auto* pin = mvcc.register_thread();
    EXPECT_NE(pin, nullptr);
    
    // Pin an epoch using static method
    uint64_t epoch = 100;
    MVCCContext::pin_epoch(pin, epoch);
    EXPECT_EQ(pin->epoch.load(), epoch);
    
    // Unpin using static method
    MVCCContext::unpin(pin);
    
    // After unpin, epoch should be UINT64_MAX
    EXPECT_EQ(pin->epoch.load(), UINT64_MAX);
}

TEST_F(MVCCContextTest, MinActiveEpochSinglePin) {
    // With no pins, min should be 0 (or current global epoch)
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
    
    // Register and pin an epoch
    auto* pin = mvcc.register_thread();
    MVCCContext::pin_epoch(pin, 150);
    
    // Min should now be the pinned epoch
    EXPECT_EQ(mvcc.min_active_epoch(), 150u);
    
    // Unpin
    MVCCContext::unpin(pin);
    
    // Min should return to 0
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}

TEST_F(MVCCContextTest, MinActiveEpochMultiplePins) {
    // Use actual threads to get different pins
    std::atomic<uint64_t> min_epoch{UINT64_MAX};
    std::vector<std::thread> threads;
    std::vector<std::atomic<bool>> should_unpin(3);
    
    // Create 3 threads that pin different epochs
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        EXPECT_NE(pin, nullptr);
        MVCCContext::pin_epoch(pin, 300);
        while (!should_unpin[0].load()) {
            std::this_thread::sleep_for(1ms);
        }
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        EXPECT_NE(pin, nullptr);
        MVCCContext::pin_epoch(pin, 100);
        while (!should_unpin[1].load()) {
            std::this_thread::sleep_for(1ms);
        }
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        EXPECT_NE(pin, nullptr);
        MVCCContext::pin_epoch(pin, 200);
        while (!should_unpin[2].load()) {
            std::this_thread::sleep_for(1ms);
        }
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    // Let threads start and pin
    std::this_thread::sleep_for(10ms);
    
    // Min should be the smallest pinned epoch
    EXPECT_EQ(mvcc.min_active_epoch(), 100u);
    
    // Unpin the minimum
    should_unpin[1] = true;
    std::this_thread::sleep_for(10ms);
    
    // Min should update to next smallest
    EXPECT_EQ(mvcc.min_active_epoch(), 200u);
    
    // Unpin remaining
    should_unpin[2] = true;
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(mvcc.min_active_epoch(), 300u);
    
    should_unpin[0] = true;
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}

TEST_F(MVCCContextTest, ConcurrentPinning) {
    const int num_threads = 8;
    const int epochs_per_thread = 10;  // Reduced for faster test
    std::vector<std::thread> threads;
    std::atomic<uint64_t> min_epoch{UINT64_MAX};
    
    // Each thread registers once then pins/unpins many times
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            // Register once per thread
            auto* pin = mvcc.register_thread();
            EXPECT_NE(pin, nullptr);
            
            for (int i = 0; i < epochs_per_thread; i++) {
                uint64_t epoch = t * 1000 + i;
                
                // Use RAII guard for automatic unpin
                {
                    MVCCContext::Guard guard(pin, epoch);
                    
                    // Track minimum epoch we've used
                    uint64_t current_min = min_epoch.load();
                    while (epoch < current_min && 
                           !min_epoch.compare_exchange_weak(current_min, epoch)) {
                        // Retry
                    }
                    
                    std::this_thread::sleep_for(10us);
                }
                // Guard destructor unpins automatically
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // All threads done, all should be unpinned
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}

TEST_F(MVCCContextTest, SlowReaderScenario) {
    // Simulate a slow reader with one thread and fast readers with another
    std::thread slow_thread([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 10);
        
        // Hold for the duration of the test
        std::this_thread::sleep_for(100ms);
        
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    // Give slow reader time to pin
    std::this_thread::sleep_for(10ms);
    
    // Fast reader in another thread
    std::thread fast_thread([&]() {
        auto* pin = mvcc.register_thread();
        
        // Simulate multiple fast reads
        for (int i = 0; i < 10; i++) {
            MVCCContext::pin_epoch(pin, 100 + i * 10);
            std::this_thread::sleep_for(1ms);
            MVCCContext::unpin(pin);
        }
        
        mvcc.deregister_thread();
    });
    
    // Give fast reader time to start
    std::this_thread::sleep_for(20ms);
    
    // Min should still be the slow reader's epoch
    EXPECT_EQ(mvcc.min_active_epoch(), 10u);
    
    // Wait for both threads
    fast_thread.join();
    
    // Slow reader still holds minimum
    EXPECT_EQ(mvcc.min_active_epoch(), 10u);
    
    slow_thread.join();
    
    // All done
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}

TEST_F(MVCCContextTest, EpochUpdateWhilePinned) {
    // Register and pin an epoch
    auto* pin = mvcc.register_thread();
    MVCCContext::pin_epoch(pin, 100);
    
    // Update the pinned epoch (simulating epoch advancement)
    pin->epoch.store(200, std::memory_order_release);
    
    // Min active should reflect the update
    EXPECT_EQ(mvcc.min_active_epoch(), 200u);
    
    MVCCContext::unpin(pin);
}

TEST_F(MVCCContextTest, ManyReaderStress) {
    const int num_readers = 100;
    std::atomic<bool> stop{false};
    std::vector<std::thread> readers;
    std::atomic<uint64_t> min_seen{UINT64_MAX};
    
    // Start many reader threads
    for (int i = 0; i < num_readers; i++) {
        readers.emplace_back([&, i]() {
            // Register once per thread
            auto* pin = mvcc.register_thread();
            if (!pin) return;  // Too many threads
            
            std::mt19937 rng(i);
            std::uniform_int_distribution<uint64_t> epoch_dist(1, 10000);
            
            while (!stop) {
                uint64_t epoch = epoch_dist(rng);
                
                // Use RAII guard for automatic pin/unpin
                {
                    MVCCContext::Guard guard(pin, epoch);
                    
                    // Simulate some work
                    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 100));
                    
                    // Track minimum epoch we've seen
                    uint64_t current_min = mvcc.min_active_epoch();
                    uint64_t prev_min = min_seen.load();
                    while (current_min < prev_min && 
                           !min_seen.compare_exchange_weak(prev_min, current_min)) {
                        // Keep trying
                    }
                }
                // Guard destructor unpins automatically
            }
        });
    }
    
    // Let it run for a bit
    std::this_thread::sleep_for(100ms);
    stop = true;
    
    for (auto& t : readers) {
        t.join();
    }
    
    // Should have seen some reasonable minimum
    EXPECT_LT(min_seen.load(), 10000u);
    
    // After all readers done, min should be max
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}

TEST_F(MVCCContextTest, PinReuse) {
    // Test that the same pin can be reused many times
    auto* pin = mvcc.register_thread();
    EXPECT_NE(pin, nullptr);
    
    // Pin and unpin many times with the same pin
    for (int round = 0; round < 100; round++) {
        uint64_t epoch = round * 100;
        
        // Pin
        MVCCContext::pin_epoch(pin, epoch);
        EXPECT_EQ(pin->epoch.load(), epoch);
        
        // Verify min active
        EXPECT_EQ(mvcc.min_active_epoch(), epoch);
        
        // Unpin
        MVCCContext::unpin(pin);
        EXPECT_EQ(pin->epoch.load(), UINT64_MAX);
    }
    
    // Pin one more time to verify still works
    MVCCContext::pin_epoch(pin, 9999);
    EXPECT_EQ(mvcc.min_active_epoch(), 9999u);
    MVCCContext::unpin(pin);
}

TEST_F(MVCCContextTest, MinActiveWithGaps) {
    // Use actual threads to test gaps in epochs
    std::vector<std::thread> threads;
    std::vector<std::atomic<bool>> should_unpin(4);
    
    // Create threads with non-contiguous epochs
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 100);
        while (!should_unpin[0]) std::this_thread::sleep_for(1ms);
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 500);
        while (!should_unpin[1]) std::this_thread::sleep_for(1ms);
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 1000);
        while (!should_unpin[2]) std::this_thread::sleep_for(1ms);
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(mvcc.min_active_epoch(), 100u);
    
    // Add thread with epoch between existing
    threads.emplace_back([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 300);
        while (!should_unpin[3]) std::this_thread::sleep_for(1ms);
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    std::this_thread::sleep_for(10ms);
    
    // Min shouldn't change
    EXPECT_EQ(mvcc.min_active_epoch(), 100u);
    
    // Remove min
    should_unpin[0] = true;
    std::this_thread::sleep_for(10ms);
    
    // New min should be 300
    EXPECT_EQ(mvcc.min_active_epoch(), 300u);
    
    // Cleanup
    should_unpin[1] = true;
    should_unpin[2] = true;
    should_unpin[3] = true;
    
    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(MVCCContextTest, ZeroEpoch) {
    // Test with epoch 0 from one thread
    std::thread t1([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 0);
        std::this_thread::sleep_for(50ms);
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
    
    // Add another thread with non-zero epoch
    std::thread t2([&]() {
        auto* pin = mvcc.register_thread();
        MVCCContext::pin_epoch(pin, 100);
        std::this_thread::sleep_for(20ms);
        MVCCContext::unpin(pin);
        mvcc.deregister_thread();
    });
    
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
    
    t2.join();
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);  // t1 still has epoch 0
    
    t1.join();
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);  // All unpinned
}

TEST_F(MVCCContextTest, RAIIGuard) {
    // Test RAII guard automatic pin/unpin
    auto* pin = mvcc.register_thread();
    EXPECT_NE(pin, nullptr);
    
    // Initially unpinned
    EXPECT_EQ(pin->epoch.load(), UINT64_MAX);
    
    {
        // Create guard - should pin automatically
        MVCCContext::Guard guard(pin, 42);
        EXPECT_EQ(pin->epoch.load(), 42u);
        EXPECT_EQ(mvcc.min_active_epoch(), 42u);
    }
    // Guard destroyed - should unpin automatically
    
    EXPECT_EQ(pin->epoch.load(), UINT64_MAX);
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
    
    // Test nested scopes with simulated other thread
    {
        MVCCContext::Guard guard1(pin, 100);
        EXPECT_EQ(mvcc.min_active_epoch(), 100u);
        
        // Start another thread to test nested behavior
        std::thread other([&]() {
            auto* pin2 = mvcc.register_thread();
            {
                MVCCContext::Guard guard2(pin2, 50);
                std::this_thread::sleep_for(10ms);
            }
            mvcc.deregister_thread();
        });
        
        // Let other thread pin its epoch
        std::this_thread::sleep_for(5ms);
        EXPECT_EQ(mvcc.min_active_epoch(), 50u);
        
        // Wait for other thread to unpin
        other.join();
        
        // guard2 destroyed, min should go back to 100
        EXPECT_EQ(mvcc.min_active_epoch(), 100u);
    }
    // guard1 destroyed
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}

TEST_F(MVCCContextTest, GuardMoveSemantics) {
    // Test that Guard supports move semantics
    auto* pin = mvcc.register_thread();
    EXPECT_NE(pin, nullptr);
    
    // Test move constructor
    {
        MVCCContext::Guard guard1(pin, 100);
        EXPECT_EQ(mvcc.min_active_epoch(), 100u);
        
        // Move construct
        MVCCContext::Guard guard2(std::move(guard1));
        EXPECT_EQ(mvcc.min_active_epoch(), 100u);
        
        // guard1 should no longer unpin when destroyed
    }
    // guard2 destroyed, should unpin
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
    
    // Test move assignment (skip because we can't have 2 guards on same pin)
    // The issue is that creating guard2 with the same pin overwrites guard1's epoch
    // In production, different threads would have different pins
    
    // Test returning Guard from function
    auto make_guard = [&]() -> MVCCContext::Guard {
        return MVCCContext::Guard(pin, 400);
    };
    
    {
        auto guard = make_guard();
        EXPECT_EQ(mvcc.min_active_epoch(), 400u);
    }
    EXPECT_EQ(mvcc.min_active_epoch(), 0u);
}