/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * MemoryCoordinator implementation.
 */

#include "memory_coordinator.h"
#include "mapping_manager.h"
#include "index_registry.h"
#include "../indexdetails.hpp"
#include "../irecord.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>

namespace xtree {
namespace persist {

// ============================================================================
// Singleton
// ============================================================================

MemoryCoordinator& MemoryCoordinator::global() {
    static MemoryCoordinator instance;
    return instance;
}

// ============================================================================
// Constructor
// ============================================================================

MemoryCoordinator::MemoryCoordinator()
    : last_tick_(std::chrono::steady_clock::now())
    , last_rebalance_(std::chrono::steady_clock::now()) {

    // Check for environment variable configuration
    if (const char* env_budget = std::getenv("XTREE_MEMORY_BUDGET")) {
        // Parse budget with suffixes: KB, MB, GB
        std::string spec(env_budget);
        size_t multiplier = 1;
        std::string numPart = spec;

        if (spec.size() >= 2) {
            std::string suffix = spec.substr(spec.size() - 2);
            if (suffix == "KB" || suffix == "kb") {
                multiplier = 1024ULL;
                numPart = spec.substr(0, spec.size() - 2);
            } else if (suffix == "MB" || suffix == "mb") {
                multiplier = 1024ULL * 1024;
                numPart = spec.substr(0, spec.size() - 2);
            } else if (suffix == "GB" || suffix == "gb") {
                multiplier = 1024ULL * 1024 * 1024;
                numPart = spec.substr(0, spec.size() - 2);
            }
        }

        try {
            size_t bytes = std::stoull(numPart) * multiplier;
            total_budget_ = bytes;
        } catch (...) {
            // Invalid format - ignore
        }
    }

    // Check for cache ratio override
    if (const char* env_ratio = std::getenv("XTREE_CACHE_RATIO")) {
        try {
            float ratio = std::stof(env_ratio);
            if (ratio >= MIN_RATIO && ratio <= MAX_RATIO) {
                cache_ratio_ = ratio;
                mmap_ratio_ = 1.0f - ratio;
            }
        } catch (...) {
            // Invalid format - ignore
        }
    }

    // Apply initial budgets if configured
    if (total_budget_ > 0) {
        apply_budgets();
    }
}

// ============================================================================
// Configuration
// ============================================================================

void MemoryCoordinator::set_total_budget(size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    total_budget_ = bytes;
    if (bytes > 0) {
        apply_budgets();
    }
}

size_t MemoryCoordinator::get_total_budget() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_budget_;
}

void MemoryCoordinator::set_rebalance_interval(std::chrono::seconds interval) {
    std::lock_guard<std::mutex> lock(mutex_);
    rebalance_interval_ = interval;
}

void MemoryCoordinator::set_initial_ratios(float cache_ratio, float mmap_ratio) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Normalize if they don't sum to 1.0
    float total = cache_ratio + mmap_ratio;
    if (total > 0.0f) {
        cache_ratio_ = std::clamp(cache_ratio / total, MIN_RATIO, MAX_RATIO);
        mmap_ratio_ = 1.0f - cache_ratio_;
    }

    if (total_budget_ > 0) {
        apply_budgets();
    }
}

void MemoryCoordinator::set_workload_hint(WorkloadHint hint) {
    std::lock_guard<std::mutex> lock(mutex_);
    workload_hint_ = hint;

    if (hint != WorkloadHint::Auto) {
        apply_workload_preset(hint);
    }
}

WorkloadHint MemoryCoordinator::get_workload_hint() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return workload_hint_;
}

// ============================================================================
// Periodic Update
// ============================================================================

void MemoryCoordinator::tick() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Skip if no budget configured
    if (total_budget_ == 0) {
        return;
    }

    auto now = std::chrono::steady_clock::now();

    // Always collect metrics for observability
    collect_metrics();

    // Check if enough time has passed for rebalancing
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - last_rebalance_);

    if (elapsed >= rebalance_interval_) {
        detect_pressure();

        // Only auto-rebalance if no workload hint is set
        if (workload_hint_ == WorkloadHint::Auto) {
            rebalance_if_needed();
        }

        last_rebalance_ = now;
    }

    last_tick_ = now;
}

void MemoryCoordinator::force_rebalance() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (total_budget_ == 0) {
        return;
    }

    collect_metrics();
    detect_pressure();
    rebalance_if_needed();
    last_rebalance_ = std::chrono::steady_clock::now();
}

// ============================================================================
// Internal Methods
// ============================================================================

void MemoryCoordinator::collect_metrics() {
    // Collect cache metrics from IndexDetails' global cache
    // Using IRecord as the base type for XTree buckets
    using Cache = IndexDetails<IRecord>::Cache;
    auto& cache = IndexDetails<IRecord>::getCache();
    auto cache_stats = cache.getStats();

    current_metrics_.cache_memory_used = cache_stats.currentMemory;
    current_metrics_.cache_memory_budget = cache_stats.maxMemory;
    current_metrics_.cache_entries = cache_stats.totalNodes;
    current_metrics_.cache_evictable = cache_stats.totalEvictable;

    // Calculate eviction delta (using round-robin counter as proxy)
    // Note: Actual eviction count would need to be tracked in ShardedLRUCache
    // For now, we use utilization as a proxy for pressure

    // Collect mmap metrics from MappingManager
    auto mmap_stats = MappingManager::global().getStats();

    current_metrics_.mmap_memory_used = mmap_stats.total_memory_mapped;
    current_metrics_.mmap_memory_budget = mmap_stats.max_memory_budget;
    current_metrics_.mmap_extents = mmap_stats.total_extents;
    current_metrics_.mmap_evictions_since_last =
        mmap_stats.evictions_count > prev_mmap_evictions_
            ? mmap_stats.evictions_count - prev_mmap_evictions_
            : 0;

    prev_mmap_evictions_ = mmap_stats.evictions_count;
}

void MemoryCoordinator::detect_pressure() {
    // Calculate utilization ratios
    if (current_metrics_.cache_memory_budget > 0) {
        current_metrics_.cache_utilization =
            static_cast<double>(current_metrics_.cache_memory_used) /
            static_cast<double>(current_metrics_.cache_memory_budget);
    } else {
        current_metrics_.cache_utilization = 0.0;
    }

    if (current_metrics_.mmap_memory_budget > 0) {
        current_metrics_.mmap_utilization =
            static_cast<double>(current_metrics_.mmap_memory_used) /
            static_cast<double>(current_metrics_.mmap_memory_budget);
    } else {
        current_metrics_.mmap_utilization = 0.0;
    }

    // Calculate pressure based on eviction rate and utilization
    // High utilization + high evictions = high pressure
    current_metrics_.cache_pressure = current_metrics_.cache_utilization;
    if (current_metrics_.cache_evictions_since_last > HIGH_EVICTION_RATE) {
        current_metrics_.cache_pressure = std::min(1.0,
            current_metrics_.cache_pressure +
            (current_metrics_.cache_evictions_since_last / (HIGH_EVICTION_RATE * 10)));
    }

    current_metrics_.mmap_pressure = current_metrics_.mmap_utilization;
    if (current_metrics_.mmap_evictions_since_last > HIGH_EVICTION_RATE) {
        current_metrics_.mmap_pressure = std::min(1.0,
            current_metrics_.mmap_pressure +
            (static_cast<double>(current_metrics_.mmap_evictions_since_last) /
             (HIGH_EVICTION_RATE * 10)));
    }
}

void MemoryCoordinator::rebalance_if_needed() {
    // Determine if rebalancing is warranted
    bool cache_under_pressure =
        current_metrics_.cache_pressure > PRESSURE_THRESHOLD;
    bool mmap_under_pressure =
        current_metrics_.mmap_pressure > PRESSURE_THRESHOLD;

    // If both are under pressure, try to unload cold indexes first
    if (cache_under_pressure && mmap_under_pressure) {
        // Both pressured - try to free memory by unloading cold indexes
        // Target: free 10% of total budget
        size_t target_free = total_budget_ / 10;
        size_t freed = IndexRegistry::global().unload_cold_indexes(target_free);
        if (freed > 0) {
            std::cout << "[MemoryCoordinator] Unloaded cold indexes, freed "
                      << (freed / (1024*1024)) << " MB\n";
        }
        return;
    }

    // If neither is under pressure, no need to rebalance
    if (!cache_under_pressure && !mmap_under_pressure) {
        return;
    }

    // Calculate new ratios
    float new_cache_ratio = cache_ratio_;
    float new_mmap_ratio = mmap_ratio_;

    if (cache_under_pressure && !mmap_under_pressure) {
        // Shift from mmap to cache
        new_cache_ratio = cache_ratio_ + REBALANCE_STEP;
        new_mmap_ratio = mmap_ratio_ - REBALANCE_STEP;
    } else if (mmap_under_pressure && !cache_under_pressure) {
        // Shift from cache to mmap
        new_cache_ratio = cache_ratio_ - REBALANCE_STEP;
        new_mmap_ratio = mmap_ratio_ + REBALANCE_STEP;
    }

    // Clamp to min/max bounds
    new_cache_ratio = std::clamp(new_cache_ratio, MIN_RATIO, MAX_RATIO);
    new_mmap_ratio = std::clamp(new_mmap_ratio, MIN_RATIO, MAX_RATIO);

    // Ensure they sum to 1.0
    float total = new_cache_ratio + new_mmap_ratio;
    if (total > 0.0f) {
        new_cache_ratio /= total;
        new_mmap_ratio /= total;
    }

    // Only apply if ratios actually changed significantly (avoid thrashing)
    const float EPSILON = 0.001f;
    if (std::abs(new_cache_ratio - cache_ratio_) > EPSILON ||
        std::abs(new_mmap_ratio - mmap_ratio_) > EPSILON) {

        cache_ratio_ = new_cache_ratio;
        mmap_ratio_ = new_mmap_ratio;
        apply_budgets();
        rebalance_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void MemoryCoordinator::apply_budgets() {
    if (total_budget_ == 0) {
        return;
    }

    // Calculate individual budgets
    size_t cache_budget = static_cast<size_t>(total_budget_ * cache_ratio_);
    size_t mmap_budget = static_cast<size_t>(total_budget_ * mmap_ratio_);

    // Apply to cache
    IndexDetails<IRecord>::setCacheMaxMemory(cache_budget);

    // Apply to mmap
    MappingManager::global().set_memory_budget(mmap_budget);
}

void MemoryCoordinator::apply_workload_preset(WorkloadHint hint) {
    switch (hint) {
        case WorkloadHint::BulkIngestion:
            // Write-heavy: favor mmap for persistence writes
            cache_ratio_ = 0.30f;
            mmap_ratio_ = 0.70f;
            break;

        case WorkloadHint::QueryHeavy:
            // Read-heavy: favor cache for bucket traversal
            cache_ratio_ = 0.60f;
            mmap_ratio_ = 0.40f;
            break;

        case WorkloadHint::Mixed:
            // Balanced workload
            cache_ratio_ = 0.50f;
            mmap_ratio_ = 0.50f;
            break;

        case WorkloadHint::MemoryConstrained:
            // Minimal footprint: favor mmap (can evict to disk)
            cache_ratio_ = 0.25f;
            mmap_ratio_ = 0.75f;
            break;

        case WorkloadHint::Auto:
        default:
            // Use default ratios
            cache_ratio_ = 0.40f;
            mmap_ratio_ = 0.60f;
            break;
    }

    if (total_budget_ > 0) {
        apply_budgets();
    }
}

// ============================================================================
// Metrics
// ============================================================================

MemoryMetrics MemoryCoordinator::get_metrics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_metrics_;
}

float MemoryCoordinator::get_cache_ratio() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_ratio_;
}

float MemoryCoordinator::get_mmap_ratio() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return mmap_ratio_;
}

size_t MemoryCoordinator::get_rebalance_count() const {
    return rebalance_count_.load(std::memory_order_relaxed);
}

// ============================================================================
// Testing Support
// ============================================================================

void MemoryCoordinator::reset() {
    std::lock_guard<std::mutex> lock(mutex_);

    total_budget_ = 0;
    cache_ratio_ = 0.40f;
    mmap_ratio_ = 0.60f;
    rebalance_interval_ = std::chrono::seconds{5};
    workload_hint_ = WorkloadHint::Auto;
    current_metrics_ = MemoryMetrics{};
    prev_cache_evictions_ = 0;
    prev_mmap_evictions_ = 0;
    last_tick_ = std::chrono::steady_clock::now();
    last_rebalance_ = std::chrono::steady_clock::now();
    rebalance_count_.store(0, std::memory_order_relaxed);
}

} // namespace persist
} // namespace xtree
