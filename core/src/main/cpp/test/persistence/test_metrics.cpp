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
#include <chrono>
#include "persistence/metrics.h"

using namespace xtree::persist;
using namespace std::chrono_literals;

class MetricsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MetricsTest, CounterBasics) {
    Counter counter("test_counter");
    
    // Initial value should be 0
    EXPECT_EQ(counter.value(), 0u);
    EXPECT_EQ(counter.name(), "test_counter");
    EXPECT_EQ(counter.type(), MetricType::Counter);
    
    // Increment by 1
    counter.increment();
    EXPECT_EQ(counter.value(), 1u);
    
    // Increment by custom amount
    counter.increment(10);
    EXPECT_EQ(counter.value(), 11u);
    
    // Reset
    counter.reset();
    EXPECT_EQ(counter.value(), 0u);
}

TEST_F(MetricsTest, GaugeBasics) {
    Gauge gauge("test_gauge");
    
    // Initial value should be 0
    EXPECT_EQ(gauge.value(), 0);
    EXPECT_EQ(gauge.name(), "test_gauge");
    EXPECT_EQ(gauge.type(), MetricType::Gauge);
    
    // Set value
    gauge.set(42);
    EXPECT_EQ(gauge.value(), 42);
    
    // Increment
    gauge.increment(8);
    EXPECT_EQ(gauge.value(), 50);
    
    // Decrement
    gauge.decrement(20);
    EXPECT_EQ(gauge.value(), 30);
    
    // Set negative
    gauge.set(-10);
    EXPECT_EQ(gauge.value(), -10);
    
    // Reset
    gauge.reset();
    EXPECT_EQ(gauge.value(), 0);
}

TEST_F(MetricsTest, CounterConcurrency) {
    Counter counter("concurrent_counter");
    const int num_threads = 8;
    const int increments_per_thread = 10000;
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&counter, increments_per_thread]() {
            for (int j = 0; j < increments_per_thread; j++) {
                counter.increment();
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // All increments should be accounted for
    EXPECT_EQ(counter.value(), num_threads * increments_per_thread);
}

TEST_F(MetricsTest, GaugeConcurrency) {
    Gauge gauge("concurrent_gauge");
    const int num_threads = 8;
    const int operations_per_thread = 10000;
    
    std::vector<std::thread> threads;
    
    // Half threads increment, half decrement
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&gauge, i, operations_per_thread]() {
            if (i % 2 == 0) {
                for (int j = 0; j < operations_per_thread; j++) {
                    gauge.increment();
                }
            } else {
                for (int j = 0; j < operations_per_thread; j++) {
                    gauge.decrement();
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should balance out to 0
    EXPECT_EQ(gauge.value(), 0);
}

TEST_F(MetricsTest, TimerBasics) {
    Timer timer;
    
    // Sleep for a known duration
    std::this_thread::sleep_for(10ms);
    
    // Check elapsed time is reasonable
    auto elapsed_ms = timer.elapsed_ms();
    EXPECT_GE(elapsed_ms, 10u);
    EXPECT_LE(elapsed_ms, 20u); // Allow some overhead
    
    // Check conversions
    auto elapsed_us = timer.elapsed_us();
    EXPECT_GE(elapsed_us, 10000u);
    
    auto elapsed_ns = timer.elapsed_ns();
    EXPECT_GE(elapsed_ns, 10000000u);
    
    // Check consistency (with tolerance for rounding)
    EXPECT_NEAR(elapsed_ms, elapsed_us / 1000, 1);  // Allow 1ms difference
    EXPECT_NEAR(elapsed_us, elapsed_ns / 1000, 1);  // Allow 1us difference
}

TEST_F(MetricsTest, MetricMacros) {
    // Test counter macros
    Counter test_counter("macro_counter");
    MetricsCollector::instance().register_counter(test_counter);
    
    #define test_counter_inc() test_counter.increment()
    #define test_counter_add(n) test_counter.increment(n)
    
    test_counter_inc();
    EXPECT_EQ(test_counter.value(), 1u);
    
    test_counter_add(5);
    EXPECT_EQ(test_counter.value(), 6u);
    
    #undef test_counter_inc
    #undef test_counter_add
    
    // Test gauge macros
    Gauge test_gauge("macro_gauge");
    MetricsCollector::instance().register_gauge(test_gauge);
    
    #define test_gauge_set(v) test_gauge.set(v)
    #define test_gauge_inc() test_gauge.increment()
    #define test_gauge_dec() test_gauge.decrement()
    
    test_gauge_set(100);
    EXPECT_EQ(test_gauge.value(), 100);
    
    test_gauge_inc();
    EXPECT_EQ(test_gauge.value(), 101);
    
    test_gauge_dec();
    EXPECT_EQ(test_gauge.value(), 100);
    
    #undef test_gauge_set
    #undef test_gauge_inc
    #undef test_gauge_dec
}

TEST_F(MetricsTest, TimerResolution) {
    // Test that timer has microsecond resolution
    Timer timer;
    
    // Busy wait for ~100 microseconds
    auto start = std::chrono::high_resolution_clock::now();
    while (std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::high_resolution_clock::now() - start).count() < 100) {
        // Busy wait
    }
    
    auto elapsed_us = timer.elapsed_us();
    EXPECT_GE(elapsed_us, 100u);
    EXPECT_LE(elapsed_us, 1000u); // Should be less than 1ms
}

TEST_F(MetricsTest, CounterOverflow) {
    Counter counter("overflow_test");
    
    // Set to near max value in one large increment
    uint64_t near_max = UINT64_MAX - 10;
    counter.increment(near_max);
    
    uint64_t before = counter.value();
    EXPECT_EQ(before, near_max);
    EXPECT_GT(before, UINT64_MAX - 1000);
    
    // Increment should wrap around (implementation dependent)
    counter.increment(100);
    
    // Value should have increased (with possible wrap)
    // This test is mainly to ensure no crash
    uint64_t after = counter.value();
    // Either wrapped to 89 or saturated at UINT64_MAX
    EXPECT_TRUE(after == 89 || after == UINT64_MAX);
}

TEST_F(MetricsTest, GaugeNegativeValues) {
    Gauge gauge("negative_test");
    
    // Test full range of values
    gauge.set(INT64_MAX);
    EXPECT_EQ(gauge.value(), INT64_MAX);
    
    gauge.set(INT64_MIN);
    EXPECT_EQ(gauge.value(), INT64_MIN);
    
    // Test transitions
    gauge.set(0);
    gauge.decrement(INT64_MAX);
    EXPECT_LT(gauge.value(), 0);
    
    gauge.set(0);
    gauge.increment(INT64_MAX);
    EXPECT_GT(gauge.value(), 0);
}