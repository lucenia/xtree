/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Sharded Object Table Overhead Benchmark
 * Measures the overhead of sharding vs unsharded ObjectTable
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include <algorithm>
#include <thread>

using namespace std::chrono;

// Minimal simulation focusing on the actual overhead
class IsolatedTest {
public:
    IsolatedTest() : active_shards_(1) {
        shards_ = std::make_unique<Shard[]>(64);
    }
    
    struct Shard {
        mutable std::mutex mu;
        size_t free_list[1000];
        size_t free_count = 1000;
        size_t counter = 0;
        
        Shard() {
            for (size_t i = 0; i < 1000; ++i) {
                free_list[i] = 1000 - i;
            }
        }
        
        size_t allocate() {
            if (free_count > 0) {
                return free_list[--free_count];
            }
            return ++counter + 1000;
        }
    };
    
    // Baseline: what unsharded ObjectTable does
    size_t baseline() {
        std::lock_guard<std::mutex> lock(direct_.mu);
        return direct_.allocate();
    }
    
    // What current sharded does
    size_t sharded() {
        size_t active = active_shards_.load(std::memory_order_relaxed);
        if (__builtin_expect(active == 1, 1)) {
            std::lock_guard<std::mutex> lock(shards_[0].mu);
            return shards_[0].allocate();
        }
        return 0;
    }
    
    // Theoretical minimum - no atomic check
    size_t sharded_no_check() {
        std::lock_guard<std::mutex> lock(shards_[0].mu);
        return shards_[0].allocate();
    }
    
private:
    Shard direct_;
    std::atomic<size_t> active_shards_;
    std::unique_ptr<Shard[]> shards_;
};

class ShardedObjectTableOverheadBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        // Stabilize CPU frequency
        volatile double x = 1.0;
        for (int i = 0; i < 10000000; ++i) {
            x = x * 1.000001;
        }
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(60, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(60, '=') << "\n\n";
    }
};

TEST_F(ShardedObjectTableOverheadBenchmark, IsolatedOverhead) {
    printSeparator("Isolated Sharding Overhead Measurement");
    const size_t N = 10000000;
    const int warmup_iterations = 5;
    const int test_iterations = 20;
    
    std::cout << "Running warmup...";
    IsolatedTest warmup_test;
    for (int i = 0; i < warmup_iterations; ++i) {
        for (size_t j = 0; j < N/10; ++j) {
            volatile size_t r1 = warmup_test.baseline();
            volatile size_t r2 = warmup_test.sharded();
        }
    }
    std::cout << " done\n\n";
    
    std::vector<double> baseline_times;
    std::vector<double> sharded_times;
    std::vector<double> no_check_times;
    
    for (int iter = 0; iter < test_iterations; ++iter) {
        IsolatedTest test;
        
        // Test baseline
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < N; ++i) {
            volatile size_t r = test.baseline();
        }
        auto end = high_resolution_clock::now();
        baseline_times.push_back(duration_cast<nanoseconds>(end - start).count() / double(N));
        
        // Test sharded
        start = high_resolution_clock::now();
        for (size_t i = 0; i < N; ++i) {
            volatile size_t r = test.sharded();
        }
        end = high_resolution_clock::now();
        sharded_times.push_back(duration_cast<nanoseconds>(end - start).count() / double(N));
        
        // Test without atomic check
        start = high_resolution_clock::now();
        for (size_t i = 0; i < N; ++i) {
            volatile size_t r = test.sharded_no_check();
        }
        end = high_resolution_clock::now();
        no_check_times.push_back(duration_cast<nanoseconds>(end - start).count() / double(N));
    }
    
    // Calculate median (robust against outliers)
    std::sort(baseline_times.begin(), baseline_times.end());
    std::sort(sharded_times.begin(), sharded_times.end());
    std::sort(no_check_times.begin(), no_check_times.end());
    
    double median_baseline = baseline_times[test_iterations/2];
    double median_sharded = sharded_times[test_iterations/2];
    double median_no_check = no_check_times[test_iterations/2];
    
    // Calculate trimmed mean (exclude top/bottom 25%)
    int trim = test_iterations / 4;
    double trimmed_baseline = 0, trimmed_sharded = 0;
    for (int i = trim; i < test_iterations - trim; ++i) {
        trimmed_baseline += baseline_times[i];
        trimmed_sharded += sharded_times[i];
    }
    trimmed_baseline /= (test_iterations - 2 * trim);
    trimmed_sharded /= (test_iterations - 2 * trim);
    
    std::cout << "=== Results (Median - Most Stable) ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Baseline (unsharded):        " << median_baseline << " ns/op\n";
    std::cout << "Sharded (with atomic check): " << median_sharded << " ns/op\n";
    std::cout << "Sharded (no atomic check):   " << median_no_check << " ns/op\n";
    
    std::cout << "\n=== Overhead Analysis ===\n";
    double total_overhead = median_sharded - median_baseline;
    double check_overhead = median_sharded - median_no_check;
    double other_overhead = median_no_check - median_baseline;
    
    std::cout << "Total overhead:              " << total_overhead << " ns (+"
              << (total_overhead / median_baseline * 100) << "%)\n";
    std::cout << "From atomic check + branch:  " << check_overhead << " ns\n";
    std::cout << "From cache/memory layout:    " << other_overhead << " ns\n";
    
    std::cout << "\n=== System Noise Check ===\n";
    double baseline_range = baseline_times.back() - baseline_times.front();
    double sharded_range = sharded_times.back() - sharded_times.front();
    std::cout << "Baseline range: " << baseline_times.front() << " - " 
              << baseline_times.back() << " ns (variance: " << baseline_range << ")\n";
    std::cout << "Sharded range:  " << sharded_times.front() << " - " 
              << sharded_times.back() << " ns (variance: " << sharded_range << ")\n";
    
    if (baseline_range > 2.0 || sharded_range > 2.0) {
        std::cout << "\n⚠️  HIGH SYSTEM NOISE DETECTED!\n";
        std::cout << "Results may be unreliable. Close other applications and retry.\n";
    } else {
        std::cout << "\n✓ Low system noise - results are reliable\n";
    }
    
    std::cout << "\n=== Bottom Line ===\n";
    double overhead_pct = total_overhead / median_baseline * 100;
    if (overhead_pct < 10.0) {
        std::cout << "✓ EXCELLENT: Overhead is under 10%\n";
    } else if (overhead_pct < 20.0) {
        std::cout << "✓ GOOD: Overhead is under 20%\n";
    } else if (overhead_pct < 30.0) {
        std::cout << "⚠️  ACCEPTABLE: Overhead is under 30%\n";
    } else {
        std::cout << "✗ HIGH: Overhead exceeds 30%\n";
    }
    
    // Assert that overhead is reasonable
    EXPECT_LT(overhead_pct, 30.0) << "Sharding overhead should be under 30%";
}