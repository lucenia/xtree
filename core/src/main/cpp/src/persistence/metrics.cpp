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

#include "metrics.h"
#include <algorithm>
#include <numeric>
#include <sstream>

namespace xtree {
namespace persist {

// Histogram implementation
void Histogram::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    values_.clear();
}

void Histogram::record(uint64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    values_.push_back(value);
}

Histogram::Stats Histogram::get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    Stats stats{};
    if (values_.empty()) {
        return stats;
    }
    
    // Make a copy for sorting
    std::vector<uint64_t> sorted = values_;
    std::sort(sorted.begin(), sorted.end());
    
    stats.count = sorted.size();
    stats.sum = std::accumulate(sorted.begin(), sorted.end(), uint64_t(0));
    stats.min = sorted.front();
    stats.max = sorted.back();
    stats.mean = static_cast<double>(stats.sum) / stats.count;
    
    // Calculate percentiles
    auto percentile = [&sorted](double p) -> uint64_t {
        size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
        return sorted[idx];
    };
    
    stats.p50 = percentile(0.50);
    stats.p95 = percentile(0.95);
    stats.p99 = percentile(0.99);
    
    return stats;
}


// MetricsCollector implementation
static MetricsCollector* g_instance = nullptr;
static std::mutex g_instance_mutex;

MetricsCollector& MetricsCollector::instance() {
    std::lock_guard<std::mutex> lock(g_instance_mutex);
    if (!g_instance) {
        g_instance = new MetricsCollector();
    }
    return *g_instance;
}

void MetricsCollector::register_counter(Counter& counter) {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_.push_back(&counter);
}

void MetricsCollector::register_gauge(Gauge& gauge) {
    std::lock_guard<std::mutex> lock(mutex_);
    gauges_.push_back(&gauge);
}

void MetricsCollector::register_histogram(Histogram& histogram) {
    std::lock_guard<std::mutex> lock(mutex_);
    histograms_.push_back(&histogram);
}

void MetricsCollector::export_metrics(ExportFunc func) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Export counters
    for (const auto* counter : counters_) {
        func(counter->name(), MetricType::Counter, std::to_string(counter->value()));
    }
    
    // Export gauges
    for (const auto* gauge : gauges_) {
        func(gauge->name(), MetricType::Gauge, std::to_string(gauge->value()));
    }
    
    // Export histograms
    for (const auto* histogram : histograms_) {
        auto stats = histogram->get_stats();
        std::ostringstream oss;
        oss << "count=" << stats.count 
            << ",sum=" << stats.sum
            << ",mean=" << stats.mean
            << ",p50=" << stats.p50
            << ",p95=" << stats.p95
            << ",p99=" << stats.p99;
        func(histogram->name(), MetricType::Histogram, oss.str());
    }
}

void MetricsCollector::reset_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto* counter : counters_) {
        counter->reset();
    }
    
    for (auto* gauge : gauges_) {
        gauge->reset();
    }
    
    for (auto* histogram : histograms_) {
        histogram->reset();
    }
}

} // namespace persist
} // namespace xtree