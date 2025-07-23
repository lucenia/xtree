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

#include "indexdetails.hpp"
#include "xtree.h"  // For DataRecord
#include <filesystem>
#include <iostream>

namespace xtree {

template<class Record>
void IndexDetails<Record>::initializeDurableStore(const std::string& data_dir) {
    namespace fs = std::filesystem;
    
    // Create data directory if it doesn't exist
    fs::create_directories(data_dir);
    
    // Setup paths for persistence components
    persist::Paths paths{
        .data_dir = data_dir,
        .manifest = data_dir + "/manifest.json",
        .superblock = data_dir + "/superblock.bin",
        .active_log = data_dir + "/ot_delta.wal"
    };
    
    // Setup checkpoint policy with production-safe defaults
    // These values balance throughput, recovery time, and disk usage
    // For heavy ingest workloads, use initializeDurableStore() overload with custom policy
    persist::CheckpointPolicy policy{
        .max_replay_bytes = 256ULL * 1024 * 1024,     // 256MB - reasonable replay time
        .max_replay_epochs = 200000,                   // 200K epochs
        .max_age = std::chrono::seconds(300),          // 5 minutes - bounded recovery
        .min_interval = std::chrono::seconds(30),      // 30 seconds minimum between checkpoints
        .rotate_bytes = 512ULL * 1024 * 1024,          // 512MB - balance between churn and size
        .rotate_age = std::chrono::seconds(1800)       // 30 minutes
    };
    
    // Check for environment variable override for stress testing or heavy ingest
    const char* ingest_mode = std::getenv("XTREE_INGEST_MODE");
    if (ingest_mode && std::string(ingest_mode) == "HEAVY") {
        // Heavy ingest mode: optimize for throughput over recovery time
        policy.max_replay_bytes = 1024ULL * 1024 * 1024;  // 1GB
        policy.max_replay_epochs = 1000000;                // 1M epochs
        policy.max_age = std::chrono::seconds(1800);       // 30 minutes
        policy.rotate_bytes = 1024ULL * 1024 * 1024;       // 1GB
        policy.rotate_age = std::chrono::seconds(3600);    // 1 hour
        std::cout << "[IndexDetails] Using HEAVY ingest mode checkpoint policy\n";
    }
    
    try {
        // Create the durable runtime which owns all persistence components
        runtime_ = persist::DurableRuntime::open(paths, policy);
        
        // Create the durable store context (must outlive DurableStore)
        durable_context_ = std::make_unique<persist::DurableContext>(persist::DurableContext{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        });
        
        // Create the durable store for this xtree instance using the field name
        durable_store_ = std::make_unique<persist::DurableStore>(*durable_context_, field_name_);
        store_ = durable_store_.get();
        
        // Recover the root if it exists
        persist::NodeID root_id = store_->get_root();
        if (root_id.raw() != 0) {
            // Set the recovered root NodeID so it can be lazily loaded on first access
            root_node_id_ = root_id;
            root_cache_key_ = root_id.raw();
            // root_cn_ stays nullptr - will be lazily loaded by root_cache_node()

            std::cout << "[IndexDetails::initializeDurableStore] Recovered root NodeID: "
                      << root_id.raw() << "\n";
        }
        
        std::cout << "[IndexDetails::initializeDurableStore] Durable store initialized successfully\n";
    } catch (const std::exception& e) {
        trace() << "[IndexDetails::initializeDurableStore] Failed to initialize durable store: " 
                  << e.what() << "\n";
        throw;
    }
}

// Static member definitions
template<class Record>
JNIEnv* IndexDetails<Record>::jvm = nullptr;

template<class Record>
std::vector<IndexDetails<Record>*> IndexDetails<Record>::indexes;

// Note: cache is now a function-local static (getCache()) to avoid static destruction order issues

// Explicit instantiations for common record types
template class IndexDetails<IRecord>;
template class IndexDetails<DataRecord>;

} // namespace xtree