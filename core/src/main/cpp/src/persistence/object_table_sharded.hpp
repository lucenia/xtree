/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Sharded Object Table Implementation
 * 
 * Provides scalable persistence substrate by sharding the ObjectTable
 * to eliminate lock contention. Works efficiently at both small and large scales.
 */

#pragma once

#include "object_table.hpp"
#include "ot_checkpoint.h"
#include "ot_delta_log.h"
#include <array>
#include <atomic>
#include <memory>
#include <vector>
#include <future>
#include <thread>

namespace xtree {
namespace persist {

// Stats tracking can be enabled at compile time via -DXTREE_ENABLE_SHARD_STATS
// Default: disabled in Release builds for zero overhead
#ifndef XTREE_ENABLE_SHARD_STATS
  #ifdef NDEBUG
    #define XTREE_ENABLE_SHARD_STATS 0
  #else
    #define XTREE_ENABLE_SHARD_STATS 1
  #endif
#endif

/**
 * Statistics per shard for monitoring (copyable snapshot)
 */
#ifndef XTREE_SHARD_STATS_DEFINED
#define XTREE_SHARD_STATS_DEFINED
struct ShardStats {
    uint64_t allocations = 0;
    uint64_t retirements = 0;
    uint64_t reclaims = 0;
    uint64_t validations = 0;
    uint64_t active_handles = 0;
    uint64_t free_handles = 0;
};

/**
 * Internal atomic statistics for thread-safe updates
 */
struct ShardStatsAtomic {
    std::atomic<uint64_t> allocations{0};
    std::atomic<uint64_t> retirements{0};
    std::atomic<uint64_t> reclaims{0};
    std::atomic<uint64_t> validations{0};
    std::atomic<uint64_t> active_handles{0};
    std::atomic<uint64_t> free_handles{0};
};
#endif

// ShardBits is now defined in object_table.hpp (included above)
// It provides the bit layout for global handle encoding

/**
 * Sharded Object Table
 * 
 * Distributes handles across shards to eliminate lock contention.
 * - At small scale: behaves like single ObjectTable (most ops hit one shard)
 * - At large scale: provides linear scaling with concurrent operations
 * - Memory efficient: same total overhead as single large table
 * - Cache friendly: each shard has smaller working set
 */
class ObjectTableSharded {
private:
    struct Shard {
        std::unique_ptr<ObjectTable> table;
        mutable ShardStatsAtomic stats;  // mutable to allow stats updates in const methods
    };
    
    // Tunable activation step (default 1024, power-of-two ideal)
    std::atomic<uint32_t> activation_step_{1024};
    
    // Per-instance epoch for TLS reset (prevents test bleed-through)
    static std::atomic<uint64_t> g_epoch_counter_;
    uint64_t epoch_;
    
    // Helper functions to convert between global and local NodeIDs
    static inline NodeID to_global(uint32_t shard, NodeID local) {
        uint64_t g = ShardBits::make_global_handle_idx(shard, local.handle_index());
        return NodeID::from_parts(g, local.tag());
    }
    
    static inline NodeID to_local(NodeID global) {
        uint64_t l = ShardBits::local_from_handle_idx(global.handle_index());
        return NodeID::from_parts(l, global.tag());
    }
    
public:
    // 64 shards is optimal for most deployments:
    // - Power of 2 for efficient masking
    // - Good for up to 64 concurrent writers  
    // - Still manageable at small scale
    static constexpr size_t DEFAULT_NUM_SHARDS = 64;
    
    /**
     * Set activation step for testing (default 1024)
     * Use UINT_MAX to prevent activation in single-thread benchmarks
     */
    void set_activation_step_for_tests(uint32_t step) {
        activation_step_.store(step ? step : 1, std::memory_order_relaxed);
    }
    
    /**
     * Construct sharded object table
     * @param initial_capacity Total capacity across all shards
     * @param num_shards Number of shards (must be power of 2, max 64)
     */
    explicit ObjectTableSharded(size_t initial_capacity = 100000,
                               size_t num_shards = DEFAULT_NUM_SHARDS)
        : num_shards_(num_shards),
          epoch_(1 + g_epoch_counter_.fetch_add(1, std::memory_order_relaxed)) {
        
        // Guard against zero shards
        num_shards_ = std::max<size_t>(1, num_shards_);
        
        // Ensure power of 2 for efficient masking
        if ((num_shards_ & (num_shards_ - 1)) != 0) {
            // Round up to next power of 2 (C++17 compatible)
            size_t n = 1;
            while (n < num_shards_) n <<= 1;
            num_shards_ = n;
        }
        
        // Limit to 64 shards (6 bits in encoding)
        if (num_shards_ > 64) {
            num_shards_ = 64;
        }
        
        shard_mask_ = num_shards_ - 1;
        
        // Initialize atomics with explicit stores for clarity
        active_shards_.store(1, std::memory_order_relaxed);
        round_robin_.store(0, std::memory_order_relaxed);
        activation_step_.store(1024, std::memory_order_relaxed);
        
        // Initialize shards with capacity divided among them
        size_t capacity_per_shard = (initial_capacity + num_shards_ - 1) / num_shards_;
        if (capacity_per_shard < 1000) capacity_per_shard = 1000;  // Minimum reasonable size
        
        // Allocate array of shards (avoids vector reallocation issues with unique_ptr)
        shards_.reset(new Shard[num_shards_]);
        for (size_t i = 0; i < num_shards_; ++i) {
            shards_[i].table = std::make_unique<ObjectTable>(capacity_per_shard);
        }
    }
    
    /**
     * Select shard for new allocation (tenant-aware in future)
     */
    inline size_t select_shard_for_allocation(uint32_t /*tenant_id*/ = 0) {
        const size_t ticket = round_robin_.fetch_add(1, std::memory_order_relaxed);

        const uint32_t step = activation_step_.load(std::memory_order_relaxed);
        if (step > 0 && (ticket % step) == 0 && active_shards_.load(std::memory_order_relaxed) < num_shards_) {
            const size_t desired = std::min<size_t>(num_shards_, 1 + (ticket / step));
            size_t cur = active_shards_.load(std::memory_order_relaxed);
            while (cur < desired &&
                   !active_shards_.compare_exchange_weak(
                        cur, desired, std::memory_order_release, std::memory_order_relaxed)) { 
                /* cur updated */ 
            }
        }

        size_t active = active_shards_.load(std::memory_order_acquire);
        if (active == 0) active = 1;
        return (active == 1) ? 0 : (ticket % active);
    }
    
    /**
     * Allocate a new NodeID
     * Uses progressive shard activation - starts with 1 shard, grows as needed
     * Encodes shard_id in handle bits [63:58] for direct routing
     */
    inline NodeID allocate(NodeKind kind, uint8_t class_id, const OTAddr& addr,
                           uint64_t birth_epoch, uint32_t /*tenant_id*/ = 0) {
        // Ultra-fast path while a==1: no selector, no modulo, no per-op atomics.
        size_t a = active_shards_.load(std::memory_order_relaxed);
        if (__builtin_expect(a == 1, 1)) {
            // Reset TLS state for new instances (prevents test bleed-through)
            thread_local uint64_t tls_epoch = 0;
            thread_local uint32_t tls_tick = 0;
            if (tls_epoch != epoch_) {
                tls_epoch = epoch_;
                tls_tick = 0;
            }
            
            // Opportunistic activation gate with zero per-op atomics
            const uint32_t step = activation_step_.load(std::memory_order_relaxed);
            
            // Check if we should fire activation
            bool fire = false;
            if (step > 0) {
                const bool pow2 = (step & (step - 1)) == 0;
                const uint32_t t = ++tls_tick;
                fire = pow2 ? ((t & (step - 1)) == 0) : ((t % step) == 0);
            }
            
            if (fire && a < num_shards_) {
                // Bump by one shard (monotonic). CAS is rare, amortized.
                size_t cur = a;
                const size_t desired = std::min<size_t>(num_shards_, cur + 1);
                while (cur < desired &&
                       !active_shards_.compare_exchange_weak(
                            cur, desired, std::memory_order_release, std::memory_order_relaxed)) {
                    /* cur updated */
                }
                a = active_shards_.load(std::memory_order_acquire);
                if (a > 1) {
                    // Fall through to multi-shard path
                    const size_t s = select_shard_for_allocation();
                    NodeID local = shards_[s].table->allocate(kind, class_id, addr, birth_epoch);
#if XTREE_ENABLE_SHARD_STATS
                    shards_[s].stats.allocations.fetch_add(1, std::memory_order_relaxed);
#endif
                    return to_global(uint32_t(s), local);
                }
            }
            
            // Single-shard hot path
            NodeID local = shards_[0].table->allocate(kind, class_id, addr, birth_epoch);
#if XTREE_ENABLE_SHARD_STATS
            shards_[0].stats.allocations.fetch_add(1, std::memory_order_relaxed);
#endif
            // Always return GLOBAL (even shard 0) to keep handles consistent
            return to_global(0, local);
        }

        // Multi-shard path (only when a > 1): selector owns the round_robin_ ticket.
        const size_t s = select_shard_for_allocation();
        NodeID local = shards_[s].table->allocate(kind, class_id, addr, birth_epoch);
#if XTREE_ENABLE_SHARD_STATS
        shards_[s].stats.allocations.fetch_add(1, std::memory_order_relaxed);
#endif
        return to_global(uint32_t(s), local);
    }
    
    /**
     * Retire a NodeID
     * Uses handle-based sharding for operations on existing NodeIDs
     */
    void retire(NodeID id, uint64_t retire_epoch) {
        const size_t s = ShardBits::shard_from_handle_idx(id.handle_index());
        const NodeID local = to_local(id);
        
        // No outer lock needed
        shards_[s].table->retire(local, retire_epoch);
        
#if XTREE_ENABLE_SHARD_STATS
        shards_[s].stats.retirements.fetch_add(1, std::memory_order_relaxed);
        shards_[s].stats.active_handles.fetch_sub(1, std::memory_order_relaxed);
#endif
    }
    
    /**
     * Mark live - Reserve phase
     * IMPORTANT: Returns new NodeID with potentially bumped tag for reused handles
     */
    NodeID mark_live_reserve(NodeID global, uint64_t birth_epoch) {
        const uint32_t shard = ShardBits::shard_from_handle_idx(global.handle_index());
        const uint64_t local = ShardBits::local_from_handle_idx(global.handle_index());
        
        // Build local NodeID from global's local handle + current tag
        NodeID local_in = NodeID::from_parts(local, global.tag());
        
        // IMPORTANT: This may bump the tag for reused handles
        NodeID local_out = shards_[shard].table->mark_live_reserve(local_in, birth_epoch);
        
        // Rewrap with the (potentially bumped) tag from the underlying table
        return to_global(shard, local_out);
    }
    
    /**
     * Mark live - Commit phase
     */
    void mark_live_commit(NodeID global_final, uint64_t birth_epoch) {
        const uint32_t shard = ShardBits::shard_from_handle_idx(global_final.handle_index());
        const uint64_t local = ShardBits::local_from_handle_idx(global_final.handle_index());

        // Build local NodeID with the final tag
        NodeID local_final = NodeID::from_parts(local, global_final.tag());

        // No outer lock needed
        shards_[shard].table->mark_live_commit(local_final, birth_epoch);
    }

    /**
     * Abort a RESERVED entry (never published).
     * Delegates to the underlying shard's ObjectTable::abort_reservation.
     *
     * @param global_id Global NodeID to abort (must be RESERVED with matching tag).
     * @return true if aborted successfully; false otherwise.
     */
    bool abort_reservation(NodeID global_id) {
        const uint32_t shard = ShardBits::shard_from_handle_idx(global_id.handle_index());
        const uint64_t local = ShardBits::local_from_handle_idx(global_id.handle_index());

        assert(shard < num_shards_ && "abort_reservation: shard index out of bounds");

        // Construct local NodeID with same tag
        NodeID local_id = NodeID::from_parts(local, global_id.tag());

        bool ok = shards_[shard].table->abort_reservation(local_id);

#ifndef NDEBUG
        if (!ok) {
            trace() << "[OT_ABORT_FAIL] shard=" << shard
                      << " local=" << local
                      << " global=" << global_id.raw()
                      << " tag=" << static_cast<int>(global_id.tag())
                      << " (abort_reservation returned false)" << std::endl;
        }
#endif
        return ok;
    }

    /**
     * Validate a NodeID tag (lock-free in base ObjectTable)
     */
    bool validate_tag(NodeID id) const {
        const size_t s = ShardBits::shard_from_handle_idx(id.handle_index());
        const NodeID local = to_local(id);
        
#if XTREE_ENABLE_SHARD_STATS
        shards_[s].stats.validations.fetch_add(1, std::memory_order_relaxed);
#endif
        
        // No lock needed - validate_tag is lock-free in base ObjectTable
        return shards_[s].table->validate_tag(local);
    }
    
    /**
     * Check if a NodeID is valid (not retired)
     */
    bool is_valid(NodeID id) const {
        const size_t s = ShardBits::shard_from_handle_idx(id.handle_index());
        const NodeID local = to_local(id);
        
        // No outer lock needed
        return shards_[s].table->is_valid(local);
    }
    
    /**
     * Try to get entry with tag validation
     * Returns nullptr if tag doesn't match or handle is invalid
     */
    const OTEntry* try_get(NodeID id) const {
        const size_t s = ShardBits::shard_from_handle_idx(id.handle_index());
        const NodeID local = to_local(id);
        
        // No outer lock needed - try_get is thread-safe in base ObjectTable
        return shards_[s].table->try_get(local);
    }
    
    /**
     * Get entry by handle without tag validation (unsafe - for internal use)
     * Use only when you know the handle is valid (e.g., during publish)
     */
    const OTEntry& get_by_handle_unsafe(uint64_t handle_idx) const {
        const uint32_t shard = ShardBits::shard_from_handle_idx(handle_idx);
        const uint64_t local = ShardBits::local_from_handle_idx(handle_idx);

        // No outer lock needed - call base OT's unsafe accessor
        return shards_[shard].table->get_by_handle_unsafe(local);
    }

    /**
     * Try to get entry by handle without tag validation.
     * Safe version that returns nullptr for invalid handles.
     */
    const OTEntry* try_get_by_handle(uint64_t handle_idx) const noexcept {
        const uint32_t shard = ShardBits::shard_from_handle_idx(handle_idx);
        const uint64_t local = ShardBits::local_from_handle_idx(handle_idx);

        if (shard >= num_shards_ || !shards_[shard].table) {
            return nullptr;
        }

        // Delegate to base OT's safe accessor
        return shards_[shard].table->try_get_by_handle(local);
    }

    /**
     * Get entry with tag validation (throws if invalid)
     */
    const OTEntry& get(NodeID id) const {
        const OTEntry* entry = try_get(id);
        if (!entry) {
            throw std::runtime_error("Invalid NodeID in get()");
        }
        return *entry;
    }
    
    /**
     * Collect a stable set of live entries across shards for checkpointing.
     * Each shard is snapped under its own lock; aggregation is best-effort global snapshot.
     * If you need a crash-consistent checkpoint, pair this with WAL fencing.
     * 
     * @param out Vector to fill with persistent entries (handles will be global)
     * @return Total number of live entries captured
     */
    template<typename PersistentEntry>
    size_t iterate_live_snapshot(std::vector<PersistentEntry>& out) {
        size_t total = 0;
        out.clear();
        
        // Optional: Pre-estimate total capacity to reduce reallocs
        size_t estimated_total = 0;
        for (size_t s = 0; s < num_shards_; ++s) {
            if (shards_[s].table) {
                auto stats = shards_[s].table->get_stats();
                size_t live_estimate = 0;
                if (stats.max_handle_allocated > stats.free_handles_count + stats.retired_handles_count) {
                    live_estimate = stats.max_handle_allocated - stats.free_handles_count - stats.retired_handles_count;
                }
                estimated_total += live_estimate;
            }
        }
        if (estimated_total > 0) {
            out.reserve(estimated_total);
        }
        
        // Take per-shard snapshots and aggregate
        for (size_t s = 0; s < num_shards_; ++s) {
            if (!shards_[s].table) continue;  // Skip uninitialized shards
            
            std::vector<PersistentEntry> shard_buf;
            shard_buf.reserve(1024);  // Start with reasonable size
            
            // Take a per-shard stable view
            size_t n = shards_[s].table->iterate_live_snapshot(shard_buf);
            
            // Repack handle_idx from local to GLOBAL before appending
            for (auto& pe : shard_buf) {
                pe.handle_idx = ShardBits::make_global_handle_idx(uint32_t(s), pe.handle_idx);
                out.push_back(std::move(pe));
            }
            total += n;
        }
        
        return total;
    }
    
    /**
     * Reclaim handles retired before the safe epoch
     * Runs in parallel across all shards for efficiency
     */
    size_t reclaim_before_epoch(uint64_t safe_epoch) {
        size_t total_reclaimed = 0;
        
        // Parallel reclaim without outer locks
        std::vector<std::future<size_t>> futures;
        futures.reserve(num_shards_);
        
        for (size_t i = 0; i < num_shards_; ++i) {
            futures.push_back(std::async(std::launch::async, 
                [this, i, safe_epoch]() {
                    // No outer lock needed
                    size_t reclaimed = shards_[i].table->reclaim_before_epoch(safe_epoch);
                    
#if XTREE_ENABLE_SHARD_STATS
                    shards_[i].stats.reclaims.fetch_add(reclaimed, std::memory_order_relaxed);
#endif
                    // Note: free_handles and active_handles updates should come from 
                    // the underlying ObjectTable's truth, not manual tracking
                    
                    return reclaimed;
                }));
        }
        
        for (auto& f : futures) {
            total_reclaimed += f.get();
        }
        
        return total_reclaimed;
    }
    
    /**
     * Get metrics for a specific shard
     * Returns a snapshot of the atomic values plus truth from underlying table
     */
    ShardStats get_shard_metrics(size_t shard_idx) const {
        if (shard_idx >= num_shards_) {
            throw std::out_of_range("Invalid shard index");
        }
        
        ShardStats snapshot;
        snapshot.allocations = shards_[shard_idx].stats.allocations.load();
        snapshot.retirements = shards_[shard_idx].stats.retirements.load();
        snapshot.reclaims = shards_[shard_idx].stats.reclaims.load();
        snapshot.validations = shards_[shard_idx].stats.validations.load();
        
        // Get truth from underlying ObjectTable
        auto ot_stats = shards_[shard_idx].table->get_stats();
        snapshot.active_handles = ot_stats.max_handle_allocated - ot_stats.free_handles_count - ot_stats.retired_handles_count;
        snapshot.free_handles = ot_stats.free_handles_count;
        
        return snapshot;
    }
    
    /**
     * Get aggregated metrics across all shards
     */
    ShardStats get_aggregate_metrics() const {
        ShardStats aggregate;
        
        for (size_t i = 0; i < num_shards_; ++i) {
            auto shard_stats = get_shard_metrics(i);
            aggregate.allocations += shard_stats.allocations;
            aggregate.retirements += shard_stats.retirements;
            aggregate.reclaims += shard_stats.reclaims;
            aggregate.validations += shard_stats.validations;
            aggregate.active_handles += shard_stats.active_handles;
            aggregate.free_handles += shard_stats.free_handles;
        }
        
        return aggregate;
    }
    
    /**
     * Get number of shards
     */
    size_t num_shards() const { return num_shards_; }
    
    /**
     * Check if sharding is actually being utilized
     * Returns the number of currently active shards
     */
    size_t active_shards() const {
        return active_shards_.load(std::memory_order_acquire);
    }
    
    /**
     * Restore a handle with specific index and properties (for recovery).
     * Used during checkpoint recovery to preserve NodeID references.
     * Routes to the correct shard based on handle encoding.
     */
    void restore_handle(uint64_t handle_idx, const OTCheckpoint::PersistentEntry& entry) {
        const uint32_t shard = ShardBits::shard_from_handle_idx(handle_idx);
        const uint64_t local = ShardBits::local_from_handle_idx(handle_idx);
        
        // No outer lock needed
        shards_[shard].table->restore_handle(local, entry);
    }
    
    /**
     * Apply a delta record during recovery replay.
     * Routes to the correct shard based on handle in the delta.
     */
    void apply_delta(const OTDeltaRec& rec) {
        const uint32_t shard = ShardBits::shard_from_handle_idx(rec.handle_idx);
        const uint64_t local = ShardBits::local_from_handle_idx(rec.handle_idx);
        
        // Create a modified delta with local handle
        OTDeltaRec local_rec = rec;
        local_rec.handle_idx = local;
        
        // No outer lock needed
        shards_[shard].table->apply_delta(local_rec);
    }
    
    /**
     * Begin recovery mode across all shards.
     */
    void begin_recovery() {
        for (size_t i = 0; i < num_shards_; ++i) {
            // No outer lock needed
            shards_[i].table->begin_recovery();
        }
    }
    
    /**
     * End recovery mode across all shards.
     */
    void end_recovery() {
        for (size_t i = 0; i < num_shards_; ++i) {
            // No outer lock needed
            shards_[i].table->end_recovery();
        }
    }
    
    /**
     * Get entry by handle index without tag validation (for checkpointing).
     * Routes to the correct shard based on handle encoding.
     */
    const OTEntry* get_by_handle_unchecked(uint64_t handle_idx) const {
        const uint32_t shard = ShardBits::shard_from_handle_idx(handle_idx);
        const uint64_t local = ShardBits::local_from_handle_idx(handle_idx);
        
        // No outer lock needed
        return shards_[shard].table->get_by_handle_unchecked(local);
    }
    
    /**
     * Get configuration info for debugging/monitoring.
     */
    size_t get_entries_per_slab() const {
        // All shards have the same configuration
        return shards_[0].table->get_entries_per_slab();
    }
    
    /**
     * Get total slab count across all shards.
     */
    size_t get_slab_count() const {
        size_t total = 0;
        for (size_t i = 0; i < num_shards_; ++i) {
            total += shards_[i].table->get_slab_count();
        }
        return total;
    }

#ifndef NDEBUG
    /**
     * Debug-only semantic validation of a NodeID against expected kind.
     * Ensures that the ObjectTable entry for this handle exists and matches
     * the expected NodeKind. Replaces old NodeID parity checks.
     * Routes to the correct shard based on the NodeID's encoded shard_id.
     *
     * @param id           The NodeID to validate
     * @param expectedKind Expected kind (Leaf, Internal, DataRecord)
     */
    inline void assert_kind(const NodeID& id, NodeKind expectedKind) const {
        if (!id.valid()) {
            throw std::runtime_error("assert_kind: invalid NodeID");
        }

        // Extract shard from global NodeID and convert to local
        const uint32_t shard = ShardBits::shard_from_handle_idx(id.handle_index());
        if (shard >= num_shards_) {
            throw std::runtime_error("assert_kind: invalid shard id in NodeID");
        }

        const NodeID local = to_local(id);

        // Delegate to the appropriate shard's assert_kind
        shards_[shard].table->assert_kind(local, expectedKind);
    }
#endif

private:
    size_t num_shards_;
    size_t shard_mask_;  // For efficient modulo via bitwise AND
    std::unique_ptr<Shard[]> shards_;  // Array of shards
    std::atomic<size_t> round_robin_;  // For allocation distribution
    std::atomic<size_t> active_shards_;  // Number of currently active shards
};

/**
 * Helper to lookup NodeKind from ObjectTableSharded.
 * Returns true if found, false otherwise.
 * Writes to 'out' only if true is returned.
 * Used by cache_or_load to determine correct loader.
 *
 * Note: This uses the public try_get() method which already
 * handles sharding internally, so we don't need friend access.
 */
inline bool try_lookup_kind(const ObjectTableSharded* ot,
                            const NodeID& id,
                            NodeKind& out) noexcept {
    if (!ot || !id.valid()) return false;
    // try_get() already routes to the correct shard internally
    if (const auto* e = ot->try_get(id)) {
        out = e->kind;
        return true;
    }
    return false;
}

// Static member definition
inline std::atomic<uint64_t> ObjectTableSharded::g_epoch_counter_{0};

} // namespace persist
} // namespace xtree