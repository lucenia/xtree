/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * MemoryCoordinator: Adaptive memory budget balancer between cache and mmap.
 *
 * This class dynamically balances the total memory budget between the
 * ShardedLRUCache and MappingManager based on workload characteristics
 * and eviction pressure.
 *
 * Problem:
 * Both systems (cache and mmap) have independent memory budgets. If both
 * are set to 4GB on an 8GB machine, OOM can occur.
 *
 * Solution:
 * A unified coordinator that owns the total memory budget and splits it
 * between the two systems based on observed pressure metrics.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <mutex>

namespace xtree {

// Forward declarations are not needed - we include indexdetails.hpp in the .cpp

namespace persist {

class MappingManager;

/**
 * Memory metrics snapshot from both cache and mmap systems.
 */
struct MemoryMetrics {
    // Cache metrics (from ShardedLRUCache)
    size_t cache_memory_used = 0;
    size_t cache_memory_budget = 0;
    size_t cache_entries = 0;
    size_t cache_evictable = 0;
    size_t cache_evictions_since_last = 0;  // Delta since last tick

    // MMap metrics (from MappingManager)
    size_t mmap_memory_used = 0;
    size_t mmap_memory_budget = 0;
    size_t mmap_extents = 0;
    size_t mmap_evictions_since_last = 0;  // Delta since last tick

    // Derived metrics (calculated in tick())
    double cache_utilization = 0.0;   // used / budget (0-1)
    double mmap_utilization = 0.0;    // used / budget (0-1)
    double cache_pressure = 0.0;      // evictions / tick (normalized)
    double mmap_pressure = 0.0;       // evictions / tick (normalized)
};

/**
 * Workload type hints for manual override.
 */
enum class WorkloadHint {
    Auto,           // Let coordinator detect and adapt
    BulkIngestion,  // Write-heavy - favor mmap
    QueryHeavy,     // Read-heavy - favor cache
    Mixed,          // Balanced workload
    MemoryConstrained  // Minimal footprint
};

/**
 * MemoryCoordinator: Unified memory controller for cache and mmap.
 *
 * Usage:
 *   // At startup, set total budget
 *   MemoryCoordinator::global().set_total_budget(4ULL * 1024 * 1024 * 1024);  // 4GB
 *
 *   // Periodically call tick() during active workloads
 *   MemoryCoordinator::global().tick();
 *
 *   // Or set a workload hint for manual control
 *   MemoryCoordinator::global().set_workload_hint(WorkloadHint::QueryHeavy);
 *
 * Thread-safety:
 *   All public methods are thread-safe. tick() should be called from
 *   a single thread or with appropriate external synchronization.
 */
class MemoryCoordinator {
public:
    // Global singleton accessor (Meyers' singleton - thread-safe, lazy init)
    static MemoryCoordinator& global();

    // Disable copy/move
    MemoryCoordinator(const MemoryCoordinator&) = delete;
    MemoryCoordinator& operator=(const MemoryCoordinator&) = delete;
    MemoryCoordinator(MemoryCoordinator&&) = delete;
    MemoryCoordinator& operator=(MemoryCoordinator&&) = delete;

    // ========== Configuration ==========

    /**
     * Set the total memory budget in bytes.
     * This is split between cache and mmap according to current ratios.
     * @param bytes Total budget (0 = unlimited, disables coordination)
     */
    void set_total_budget(size_t bytes);

    /**
     * Get the current total budget.
     */
    size_t get_total_budget() const;

    /**
     * Set the rebalance interval.
     * tick() will only perform rebalancing after this interval has passed.
     * @param interval Minimum time between rebalances (default: 5s)
     */
    void set_rebalance_interval(std::chrono::seconds interval);

    /**
     * Set initial memory ratios.
     * @param cache_ratio Fraction for cache (0.0-1.0)
     * @param mmap_ratio Fraction for mmap (0.0-1.0)
     * Note: cache_ratio + mmap_ratio should equal 1.0
     */
    void set_initial_ratios(float cache_ratio, float mmap_ratio);

    /**
     * Set workload hint for manual ratio override.
     * When set to Auto, the coordinator adapts based on observed metrics.
     * @param hint Workload type
     */
    void set_workload_hint(WorkloadHint hint);

    /**
     * Get current workload hint.
     */
    WorkloadHint get_workload_hint() const;

    // ========== Periodic Update ==========

    /**
     * Called periodically to collect metrics and rebalance if needed.
     * This is the main entry point for adaptive memory management.
     *
     * Can be called frequently - internally throttled by rebalance_interval.
     * Returns immediately if interval hasn't elapsed.
     *
     * Typical call patterns:
     *   - Every N operations (e.g., every 10000 inserts)
     *   - From a background timer thread
     *   - At commit/checkpoint boundaries
     */
    void tick();

    /**
     * Force a rebalance regardless of interval.
     * Use sparingly - primarily for testing or after major workload changes.
     */
    void force_rebalance();

    // ========== Metrics ==========

    /**
     * Get current memory metrics snapshot.
     */
    MemoryMetrics get_metrics() const;

    /**
     * Get current cache ratio (0.0-1.0).
     */
    float get_cache_ratio() const;

    /**
     * Get current mmap ratio (0.0-1.0).
     */
    float get_mmap_ratio() const;

    /**
     * Get number of rebalances performed since startup.
     */
    size_t get_rebalance_count() const;

    // ========== Testing Support ==========

    /**
     * Reset coordinator to default state (for testing).
     */
    void reset();

private:
    MemoryCoordinator();
    ~MemoryCoordinator() = default;

    // Internal methods
    void collect_metrics();
    void detect_pressure();
    void rebalance_if_needed();
    void apply_budgets();
    void apply_workload_preset(WorkloadHint hint);

    // Configuration
    size_t total_budget_ = 0;  // 0 = unlimited (no coordination)
    float cache_ratio_ = 0.40f;  // 40% to cache
    float mmap_ratio_ = 0.60f;   // 60% to mmap
    std::chrono::seconds rebalance_interval_{5};
    WorkloadHint workload_hint_ = WorkloadHint::Auto;

    // Current metrics snapshot
    MemoryMetrics current_metrics_;

    // Previous tick metrics for delta calculation
    size_t prev_cache_evictions_ = 0;
    size_t prev_mmap_evictions_ = 0;

    // Timing
    std::chrono::steady_clock::time_point last_tick_;
    std::chrono::steady_clock::time_point last_rebalance_;

    // Statistics
    std::atomic<size_t> rebalance_count_{0};

    // Thread safety
    mutable std::mutex mutex_;

    // Thresholds
    static constexpr float PRESSURE_THRESHOLD = 0.8f;    // 80% utilization
    static constexpr float HIGH_EVICTION_RATE = 100.0f;  // evictions per tick
    static constexpr float REBALANCE_STEP = 0.05f;       // 5% shift per tick
    static constexpr float MIN_RATIO = 0.20f;            // 20% minimum
    static constexpr float MAX_RATIO = 0.80f;            // 80% maximum
};

} // namespace persist
} // namespace xtree
