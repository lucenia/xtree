/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <cstddef>

namespace xtree {
namespace persist {

enum class DurabilityMode {
    STRICT,    // Synchronous data + WAL flush
    EVENTUAL,  // Payload-in-WAL for small nodes  
    BALANCED   // WAL-only with coalesced flush (default)
};

struct DurabilityPolicy {
    DurabilityMode mode = DurabilityMode::BALANCED;
    
    // EVENTUAL mode settings
    size_t max_payload_in_wal = 8192;  // Max node size to embed in WAL
    
    // BALANCED mode settings
    size_t dirty_flush_bytes = 128 * 1024 * 1024;  // 128MB
    std::chrono::seconds dirty_flush_age{3};        // 3 seconds
    bool validate_checksums_on_recovery = true;
    
    // Optimization flags (apply to all modes)
    bool coalesce_flushes = true;      // Group contiguous ranges
    bool use_fdatasync = true;          // Use fdatasync vs fsync where possible
    size_t group_commit_interval_ms = 5;  // Group commit window in milliseconds
    bool sync_on_commit = false;        // EVENTUAL mode: whether to sync on commit
};

// Helper to get a named policy
inline DurabilityPolicy get_durability_policy(const std::string& name) {
    DurabilityPolicy policy;
    
    if (name == "strict") {
        policy.mode = DurabilityMode::STRICT;
        policy.group_commit_interval_ms = 0;  // No batching in strict mode
    } else if (name == "eventual") {
        policy.mode = DurabilityMode::EVENTUAL;
        policy.max_payload_in_wal = 32768;  // 32KB payloads in WAL
    } else if (name == "balanced" || name.empty()) {
        // Default settings already set above
    }
    
    return policy;
}

} // namespace persist
} // namespace xtree