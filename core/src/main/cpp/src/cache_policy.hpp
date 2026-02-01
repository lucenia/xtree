/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Cache memory policies for XTree LRU cache management.
 *
 * Policies control how the cache budget is determined and when eviction occurs.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <algorithm>
#include <string>

#ifdef _WIN32
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/types.h>
#include <sys/sysctl.h>
#else
#include <sys/sysinfo.h>
#endif

namespace xtree {

// Forward declaration
namespace detail {
    inline size_t getTotalSystemMemory();
}

/**
 * Abstract base class for cache memory policies.
 *
 * Policies determine the memory budget for the LRU cache based on
 * various strategies (fixed size, percentage of RAM, adaptive, etc.)
 */
class CachePolicy {
public:
    virtual ~CachePolicy() = default;

    /**
     * Get the maximum memory budget in bytes.
     * @return Budget in bytes, or 0 for unlimited
     */
    virtual size_t getMaxMemory() const = 0;

    /**
     * Get the policy name for logging/debugging.
     */
    virtual const char* name() const = 0;

    /**
     * Called periodically to allow adaptive policies to adjust.
     * @param currentMemory Current cache memory usage
     * @param hitRate Recent cache hit rate (0.0 - 1.0)
     */
    virtual void onTick(size_t currentMemory, double hitRate) {
        (void)currentMemory;
        (void)hitRate;
    }
};

// ============================================================================
// System Memory Detection
// ============================================================================

namespace detail {

inline size_t getTotalSystemMemory() {
#ifdef _WIN32
    MEMORYSTATUSEX memInfo;
    memInfo.dwLength = sizeof(MEMORYSTATUSEX);
    if (GlobalMemoryStatusEx(&memInfo)) {
        return static_cast<size_t>(memInfo.ullTotalPhys);
    }
    return 4ULL * 1024 * 1024 * 1024;  // Default 4GB
#elif defined(__APPLE__)
    // macOS
    int64_t memsize;
    size_t len = sizeof(memsize);
    if (sysctlbyname("hw.memsize", &memsize, &len, nullptr, 0) == 0) {
        return static_cast<size_t>(memsize);
    }
    return 4ULL * 1024 * 1024 * 1024;  // Default 4GB
#else
    // Linux
    struct sysinfo info;
    if (sysinfo(&info) == 0) {
        return info.totalram * info.mem_unit;
    }
    return 4ULL * 1024 * 1024 * 1024;  // Default 4GB
#endif
}

} // namespace detail

// ============================================================================
// Policy Implementations
// ============================================================================

/**
 * Unlimited cache - no memory budget, never evicts.
 * Best for: Maximum performance when memory is not constrained.
 */
class UnlimitedCachePolicy : public CachePolicy {
public:
    size_t getMaxMemory() const override { return 0; }
    const char* name() const override { return "Unlimited"; }
};

/**
 * Fixed memory budget in bytes.
 * Best for: Predictable memory usage, containerized environments.
 */
class FixedMemoryCachePolicy : public CachePolicy {
public:
    explicit FixedMemoryCachePolicy(size_t bytes) : budget_(bytes) {}

    size_t getMaxMemory() const override { return budget_; }
    const char* name() const override { return "FixedMemory"; }

    void setBudget(size_t bytes) { budget_ = bytes; }

private:
    size_t budget_;
};

/**
 * Percentage of total system memory.
 * Best for: Automatic scaling across different machines.
 */
class PercentageMemoryCachePolicy : public CachePolicy {
public:
    /**
     * @param percentage Percentage of system RAM (1-100)
     */
    explicit PercentageMemoryCachePolicy(unsigned percentage)
        : percentage_(std::min(100u, std::max(1u, percentage)))
        , budget_((detail::getTotalSystemMemory() * percentage_) / 100) {}

    size_t getMaxMemory() const override { return budget_; }
    const char* name() const override { return "PercentageMemory"; }

    unsigned getPercentage() const { return percentage_; }

private:
    unsigned percentage_;
    size_t budget_;
};

/**
 * Budget based on expected record count.
 * Best for: When you know the expected dataset size upfront.
 */
class PerRecordCachePolicy : public CachePolicy {
public:
    /**
     * @param expectedRecords Expected number of records
     * @param bytesPerRecord Memory budget per record (default ~50 bytes for minimal cache)
     */
    PerRecordCachePolicy(size_t expectedRecords, size_t bytesPerRecord = 50)
        : budget_(expectedRecords * bytesPerRecord) {}

    size_t getMaxMemory() const override { return budget_; }
    const char* name() const override { return "PerRecord"; }

private:
    size_t budget_;
};

/**
 * Tiered policy based on workload hints.
 */
enum class WorkloadType {
    BulkIngestion,    // Write-heavy, aggressive eviction OK
    QueryHeavy,       // Read-heavy, keep more in cache
    Mixed,            // Balanced
    MemoryConstrained // Minimal footprint
};

class WorkloadCachePolicy : public CachePolicy {
public:
    explicit WorkloadCachePolicy(WorkloadType workload)
        : workload_(workload)
        , budget_(calculateBudget(workload)) {}

    size_t getMaxMemory() const override { return budget_; }
    const char* name() const override { return "Workload"; }

    WorkloadType getWorkload() const { return workload_; }

private:
    static size_t calculateBudget(WorkloadType workload) {
        size_t totalRam = detail::getTotalSystemMemory();

        switch (workload) {
            case WorkloadType::BulkIngestion:
                return totalRam / 16;     // ~6% of RAM (256MB on 4GB)
            case WorkloadType::QueryHeavy:
                return totalRam / 4;      // ~25% of RAM (1GB on 4GB)
            case WorkloadType::Mixed:
                return totalRam / 8;      // ~12% of RAM (512MB on 4GB)
            case WorkloadType::MemoryConstrained:
                return totalRam / 32;     // ~3% of RAM (128MB on 4GB)
        }
        return totalRam / 8;  // Default fallback
    }

    WorkloadType workload_;
    size_t budget_;
};

/**
 * Adaptive policy that adjusts based on cache hit rate.
 * Increases budget when hit rate is low, decreases when high.
 * Best for: Dynamic workloads where optimal cache size isn't known.
 */
class AdaptiveCachePolicy : public CachePolicy {
public:
    /**
     * @param minBudget Minimum memory budget
     * @param maxBudget Maximum memory budget
     * @param targetHitRate Target cache hit rate (0.0-1.0)
     */
    AdaptiveCachePolicy(size_t minBudget, size_t maxBudget, double targetHitRate = 0.90)
        : minBudget_(minBudget)
        , maxBudget_(maxBudget)
        , targetHitRate_(targetHitRate)
        , currentBudget_((minBudget + maxBudget) / 2) {}

    size_t getMaxMemory() const override {
        return currentBudget_.load(std::memory_order_relaxed);
    }

    const char* name() const override { return "Adaptive"; }

    void onTick(size_t currentMemory, double hitRate) override {
        (void)currentMemory;

        size_t budget = currentBudget_.load(std::memory_order_relaxed);

        if (hitRate < targetHitRate_ - 0.05) {
            // Hit rate too low - increase budget by 10%
            size_t newBudget = std::min(maxBudget_, budget + budget / 10);
            currentBudget_.store(newBudget, std::memory_order_relaxed);
        } else if (hitRate > targetHitRate_ + 0.05) {
            // Hit rate exceeds target - can decrease budget by 5%
            size_t newBudget = std::max(minBudget_, budget - budget / 20);
            currentBudget_.store(newBudget, std::memory_order_relaxed);
        }
    }

private:
    size_t minBudget_;
    size_t maxBudget_;
    double targetHitRate_;
    std::atomic<size_t> currentBudget_;
};

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a policy from a simple string specification.
 *
 * Formats:
 *   "unlimited"           - No limit
 *   "512MB" or "1GB"      - Fixed size
 *   "25%"                 - Percentage of RAM
 *   "bulk" / "query" / "mixed" / "minimal" - Workload presets
 *
 * @param spec Policy specification string
 * @return Shared pointer to the policy, or nullptr if invalid
 */
inline std::shared_ptr<CachePolicy> createCachePolicy(const std::string& spec) {
    if (spec.empty() || spec == "unlimited" || spec == "0") {
        return std::make_shared<UnlimitedCachePolicy>();
    }

    // Check for percentage (e.g., "25%")
    if (spec.back() == '%') {
        try {
            unsigned pct = static_cast<unsigned>(std::stoul(spec.substr(0, spec.size() - 1)));
            return std::make_shared<PercentageMemoryCachePolicy>(pct);
        } catch (...) {
            return nullptr;
        }
    }

    // Check for size suffixes (e.g., "512MB", "1GB")
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

    // Try parsing as a number
    try {
        size_t bytes = std::stoull(numPart) * multiplier;
        return std::make_shared<FixedMemoryCachePolicy>(bytes);
    } catch (...) {
        // Not a number - check workload presets
    }

    // Workload presets
    if (spec == "bulk" || spec == "ingestion") {
        return std::make_shared<WorkloadCachePolicy>(WorkloadType::BulkIngestion);
    } else if (spec == "query" || spec == "read") {
        return std::make_shared<WorkloadCachePolicy>(WorkloadType::QueryHeavy);
    } else if (spec == "mixed" || spec == "balanced") {
        return std::make_shared<WorkloadCachePolicy>(WorkloadType::Mixed);
    } else if (spec == "minimal" || spec == "constrained") {
        return std::make_shared<WorkloadCachePolicy>(WorkloadType::MemoryConstrained);
    }

    return nullptr;  // Unknown spec
}

/**
 * Get the default cache policy from environment variable XTREE_CACHE_POLICY.
 * Falls back to Unlimited if not set or invalid.
 */
inline std::shared_ptr<CachePolicy> getDefaultCachePolicy() {
    const char* envPolicy = std::getenv("XTREE_CACHE_POLICY");
    if (envPolicy) {
        auto policy = createCachePolicy(envPolicy);
        if (policy) return policy;
    }
    return std::make_shared<UnlimitedCachePolicy>();
}

} // namespace xtree
