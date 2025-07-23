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

#pragma once
#include <cstdint>
#include <string>
#include <functional>
#include <chrono>
#include <atomic>
#include <mutex>
#include <vector>

namespace xtree {
namespace persist {

// Forward declarations
class MetricsCollector;
class Timer;

// Metric types
enum class MetricType {
    Counter,
    Gauge,
    Histogram,
    Timer
};

// Base metric interface
class Metric {
public:
    virtual ~Metric() = default;
    virtual MetricType type() const = 0;
    virtual std::string name() const = 0;
    virtual void reset() = 0;
};

// Counter - monotonically increasing value
class Counter : public Metric {
public:
    explicit Counter(const std::string& name) : name_(name), value_(0) {}
    
    MetricType type() const override { return MetricType::Counter; }
    std::string name() const override { return name_; }
    void reset() override { value_.store(0); }
    
    void increment(uint64_t delta = 1) {
        value_.fetch_add(delta, std::memory_order_relaxed);
    }
    
    uint64_t value() const {
        return value_.load(std::memory_order_relaxed);
    }
    
private:
    std::string name_;
    std::atomic<uint64_t> value_;
};

// Gauge - value that can go up or down
class Gauge : public Metric {
public:
    explicit Gauge(const std::string& name) : name_(name), value_(0) {}
    
    MetricType type() const override { return MetricType::Gauge; }
    std::string name() const override { return name_; }
    void reset() override { value_.store(0); }
    
    void set(int64_t value) {
        value_.store(value, std::memory_order_relaxed);
    }
    
    void increment(int64_t delta = 1) {
        value_.fetch_add(delta, std::memory_order_relaxed);
    }
    
    void decrement(int64_t delta = 1) {
        value_.fetch_sub(delta, std::memory_order_relaxed);
    }
    
    int64_t value() const {
        return value_.load(std::memory_order_relaxed);
    }
    
private:
    std::string name_;
    std::atomic<int64_t> value_;
};

// Histogram - distribution of values
class Histogram : public Metric {
public:
    explicit Histogram(const std::string& name) : name_(name) {}
    
    MetricType type() const override { return MetricType::Histogram; }
    std::string name() const override { return name_; }
    void reset() override;
    
    void record(uint64_t value);
    
    struct Stats {
        uint64_t count;
        uint64_t sum;
        uint64_t min;
        uint64_t max;
        double mean;
        uint64_t p50;
        uint64_t p95;
        uint64_t p99;
    };
    
    Stats get_stats() const;
    
private:
    std::string name_;
    mutable std::mutex mutex_;
    std::vector<uint64_t> values_;
};

// Timer - convenience wrapper for timing operations
class Timer {
public:
    Timer() : start_(std::chrono::steady_clock::now()) {}
    
    uint64_t elapsed_ns() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_).count();
    }
    
    uint64_t elapsed_us() const {
        return elapsed_ns() / 1000;
    }
    
    uint64_t elapsed_ms() const {
        return elapsed_ns() / 1000000;
    }
    
private:
    std::chrono::steady_clock::time_point start_;
};

// Scoped timer that records to a histogram on destruction
class ScopedTimer {
public:
    explicit ScopedTimer(Histogram& histogram) 
        : histogram_(histogram), timer_() {}
    
    ~ScopedTimer() {
        histogram_.record(timer_.elapsed_ns());
    }
    
private:
    Histogram& histogram_;
    Timer timer_;
};

// Global metrics collector
class MetricsCollector {
public:
    static MetricsCollector& instance();
    
    // Register metrics
    void register_counter(Counter& counter);
    void register_gauge(Gauge& gauge);
    void register_histogram(Histogram& histogram);
    
    // Export metrics
    using ExportFunc = std::function<void(const std::string& name, 
                                          MetricType type, 
                                          const std::string& value)>;
    void export_metrics(ExportFunc func) const;
    
    // Reset all metrics
    void reset_all();
    
private:
    MetricsCollector() = default;
    
    mutable std::mutex mutex_;
    std::vector<Counter*> counters_;
    std::vector<Gauge*> gauges_;
    std::vector<Histogram*> histograms_;
};

// Predefined metrics for persistence layer
namespace metrics {
    
    // Object Table metrics
    extern Counter ot_allocations;
    extern Counter ot_retirements;
    extern Counter ot_reclamations;
    extern Gauge ot_live_entries;
    extern Histogram ot_allocation_latency_ns;
    
    // Segment allocator metrics
    extern Counter segment_allocations;
    extern Counter segment_frees;
    extern Gauge segment_fragmentation_pct;
    extern Histogram segment_allocation_size;
    
    // MVCC metrics
    extern Gauge mvcc_active_readers;
    extern Gauge mvcc_min_active_epoch;
    extern Counter mvcc_epoch_advances;
    extern Histogram mvcc_epoch_lag;
    
    // Compaction metrics
    extern Counter compaction_runs;
    extern Counter compaction_bytes_moved;
    extern Gauge compaction_active;
    extern Histogram compaction_duration_ms;
    
    // I/O metrics
    extern Counter io_reads;
    extern Counter io_writes;
    extern Counter io_bytes_read;
    extern Counter io_bytes_written;
    extern Histogram io_read_latency_us;
    extern Histogram io_write_latency_us;
    
    // Recovery metrics
    extern Counter recovery_attempts;
    extern Histogram recovery_duration_ms;
    extern Counter recovery_records_replayed;
    
    // Initialize all metrics
    void initialize();
}

// Convenience macros
#define METRIC_COUNTER_INC(name) xtree::persist::metrics::name.increment()
#define METRIC_COUNTER_ADD(name, delta) xtree::persist::metrics::name.increment(delta)
#define METRIC_GAUGE_SET(name, value) xtree::persist::metrics::name.set(value)
#define METRIC_GAUGE_INC(name) xtree::persist::metrics::name.increment()
#define METRIC_GAUGE_DEC(name) xtree::persist::metrics::name.decrement()
#define METRIC_HISTOGRAM_RECORD(name, value) xtree::persist::metrics::name.record(value)
#define METRIC_SCOPED_TIMER(name) xtree::persist::ScopedTimer _timer(xtree::persist::metrics::name)

} // namespace persist
} // namespace xtree