/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * IndexRegistry implementation.
 */

#include "index_registry.h"
#include "../xtree.h"  // Full XTree definitions
#include "memory_coordinator.h"
#include "mapping_manager.h"
#include "manifest.h"

#include <algorithm>
#include <iostream>

namespace xtree {
namespace persist {

// ============================================================================
// Singleton
// ============================================================================

IndexRegistry& IndexRegistry::global() {
    static IndexRegistry instance;
    return instance;
}

IndexRegistry::~IndexRegistry() {
    // Clean up all loaded indexes
    std::lock_guard<std::mutex> lock(registry_mutex_);
    for (auto& [name, entry] : entries_) {
        if (entry->index_ptr && entry->destructor) {
            entry->destructor(entry->index_ptr);
            entry->index_ptr = nullptr;
        }
    }
    entries_.clear();
}

// ============================================================================
// Registration
// ============================================================================

bool IndexRegistry::register_index(const std::string& field_name,
                                   const IndexConfig& config) {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    if (entries_.find(field_name) != entries_.end()) {
        return false;  // Already registered
    }

    auto entry = std::make_unique<IndexEntry>();
    entry->metadata.config = config;
    entry->metadata.config.field_name = field_name;  // Ensure consistency
    entry->metadata.state = IndexLoadState::REGISTERED;
    entry->metadata.last_access = std::chrono::steady_clock::now();
    entry->index_ptr = nullptr;

    entries_[field_name] = std::move(entry);
    return true;
}

bool IndexRegistry::is_registered(const std::string& field_name) const {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return entries_.find(field_name) != entries_.end();
}

size_t IndexRegistry::register_from_manifest(const Manifest& manifest,
                                              const IndexConfig& defaults) {
    const auto& roots = manifest.get_roots();
    if (roots.empty()) {
        return 0;
    }

    size_t registered = 0;
    const std::string& data_dir = manifest.get_data_dir();

    for (const auto& root : roots) {
        // Build config for this field
        IndexConfig config = defaults;
        config.field_name = root.name;
        config.data_dir = data_dir;  // All fields share the same data directory

        // Infer dimension from MBR if available and not explicitly set
        if (config.dimension == 0 && !root.mbr.empty()) {
            config.dimension = static_cast<unsigned short>(root.mbr.size() / 2);
        }

        // Use default dimension if still not set
        if (config.dimension == 0) {
            config.dimension = 2;
        }

        if (register_index(root.name, config)) {
            registered++;
        }
    }

    if (registered > 0) {
        std::cout << "[IndexRegistry] Registered " << registered
                  << " fields from manifest (data_dir: " << data_dir << ")\n";
    }

    return registered;
}

size_t IndexRegistry::register_from_data_dir(const std::string& data_dir,
                                              const IndexConfig& defaults) {
    // Create and load manifest
    Manifest manifest(data_dir);
    if (!manifest.load()) {
        std::cout << "[IndexRegistry] No manifest found in " << data_dir << "\n";
        return 0;
    }

    // Delegate to register_from_manifest
    return register_from_manifest(manifest, defaults);
}

IndexLoadState IndexRegistry::get_state(const std::string& field_name) const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    auto it = entries_.find(field_name);
    if (it == entries_.end()) {
        return IndexLoadState::REGISTERED;  // Not found
    }
    return it->second->metadata.state;
}

const IndexMetadata* IndexRegistry::get_metadata(const std::string& field_name) const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    auto it = entries_.find(field_name);
    if (it == entries_.end()) {
        return nullptr;
    }
    return &it->second->metadata;
}

// ============================================================================
// Loading state queries
// ============================================================================

bool IndexRegistry::is_loaded(const std::string& field_name) const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    auto it = entries_.find(field_name);
    if (it == entries_.end()) {
        return false;
    }
    return it->second->metadata.state == IndexLoadState::LOADED &&
           it->second->index_ptr != nullptr;
}

// ============================================================================
// Unloading
// ============================================================================

size_t IndexRegistry::unload_index(const std::string& field_name) {
    std::unique_lock<std::mutex> lock(registry_mutex_);

    auto it = entries_.find(field_name);
    if (it == entries_.end()) {
        return 0;
    }

    auto& entry = *it->second;
    auto& meta = entry.metadata;

    // Not loaded - nothing to do
    if (meta.state != IndexLoadState::LOADED || !entry.index_ptr) {
        return 0;
    }

    // Release registry lock, acquire per-index lock
    lock.unlock();

    std::lock_guard<std::mutex> load_lock(entry.load_mutex);

    // Double-check after acquiring lock
    if (meta.state != IndexLoadState::LOADED || !entry.index_ptr) {
        return 0;
    }

    return unload_index_impl(field_name, meta);
}

size_t IndexRegistry::unload_index_impl(const std::string& field_name,
                                        IndexMetadata& meta) {
    // Must hold entry's load_mutex when calling this

    meta.state = IndexLoadState::UNLOADING;

    // Find the entry again to access the index_ptr
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = entries_.find(field_name);
    if (it == entries_.end()) {
        meta.state = IndexLoadState::REGISTERED;
        return 0;
    }

    auto& entry = *it->second;
    size_t bytes_freed = meta.estimated_memory;

    // Get memory stats before unloading
    if (bytes_freed == 0) {
        // Try to get actual memory from per-field stats
        auto mmap_stats = MappingManager::global().getPerFieldStats();
        auto mmap_it = mmap_stats.find(field_name);
        if (mmap_it != mmap_stats.end()) {
            bytes_freed = mmap_it->second.mmap_bytes;
        }
    }

    // Delete the index
    if (entry.index_ptr && entry.destructor) {
        entry.destructor(entry.index_ptr);
        entry.index_ptr = nullptr;
    }

    meta.state = IndexLoadState::REGISTERED;
    meta.estimated_memory = 0;

    // Callback
    if (on_unload_callback_) {
        on_unload_callback_(field_name);
    }

    return bytes_freed;
}

size_t IndexRegistry::unload_cold_indexes(size_t target_bytes) {
    // Build list of loaded indexes sorted by last access time (oldest first)
    std::vector<std::pair<std::string, std::chrono::steady_clock::time_point>> candidates;

    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        for (const auto& [name, entry] : entries_) {
            if (entry->metadata.state == IndexLoadState::LOADED &&
                entry->index_ptr != nullptr) {
                candidates.emplace_back(name, entry->metadata.last_access);
            }
        }
    }

    // Sort by last access time (oldest first)
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) {
                  return a.second < b.second;
              });

    // Unload until we've freed enough memory
    size_t total_freed = 0;
    for (const auto& [name, _] : candidates) {
        if (total_freed >= target_bytes) {
            break;
        }

        size_t freed = unload_index(name);
        total_freed += freed;

        std::cout << "[IndexRegistry] Unloaded cold index '" << name
                  << "', freed " << (freed / (1024*1024)) << " MB\n";
    }

    return total_freed;
}

size_t IndexRegistry::unload_idle_indexes(std::chrono::seconds max_idle) {
    auto now = std::chrono::steady_clock::now();
    size_t unloaded_count = 0;

    // Build list of idle indexes
    std::vector<std::string> idle_indexes;

    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        for (const auto& [name, entry] : entries_) {
            if (entry->metadata.state == IndexLoadState::LOADED &&
                entry->index_ptr != nullptr) {

                auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
                    now - entry->metadata.last_access);

                if (idle_time >= max_idle) {
                    idle_indexes.push_back(name);
                }
            }
        }
    }

    // Unload each idle index
    for (const auto& name : idle_indexes) {
        size_t freed = unload_index(name);
        if (freed > 0) {
            unloaded_count++;
            std::cout << "[IndexRegistry] Unloaded idle index '" << name
                      << "' (idle " << max_idle.count() << "s)\n";
        }
    }

    return unloaded_count;
}

// ============================================================================
// Metrics
// ============================================================================

size_t IndexRegistry::registered_count() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return entries_.size();
}

size_t IndexRegistry::loaded_count() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    size_t count = 0;
    for (const auto& [_, entry] : entries_) {
        if (entry->metadata.state == IndexLoadState::LOADED &&
            entry->index_ptr != nullptr) {
            count++;
        }
    }
    return count;
}

size_t IndexRegistry::total_loaded_memory() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    size_t total = 0;
    for (const auto& [name, entry] : entries_) {
        if (entry->metadata.state == IndexLoadState::LOADED &&
            entry->index_ptr != nullptr) {
            total += entry->metadata.estimated_memory;
        }
    }

    // If estimated_memory wasn't set, try to get from MappingManager
    if (total == 0) {
        auto mmap_stats = MappingManager::global().getPerFieldStats();
        for (const auto& [name, entry] : entries_) {
            if (entry->metadata.state == IndexLoadState::LOADED &&
                entry->index_ptr != nullptr) {
                auto it = mmap_stats.find(name);
                if (it != mmap_stats.end()) {
                    total += it->second.mmap_bytes;
                }
            }
        }
    }

    return total;
}

std::vector<std::string> IndexRegistry::get_registered_fields() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    std::vector<std::string> fields;
    fields.reserve(entries_.size());
    for (const auto& [name, _] : entries_) {
        fields.push_back(name);
    }
    return fields;
}

std::vector<std::string> IndexRegistry::get_loaded_fields() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    std::vector<std::string> fields;
    for (const auto& [name, entry] : entries_) {
        if (entry->metadata.state == IndexLoadState::LOADED &&
            entry->index_ptr != nullptr) {
            fields.push_back(name);
        }
    }
    return fields;
}

// ============================================================================
// Callbacks
// ============================================================================

void IndexRegistry::set_on_load_callback(std::function<void(const std::string&)> callback) {
    on_load_callback_ = std::move(callback);
}

void IndexRegistry::set_on_unload_callback(std::function<void(const std::string&)> callback) {
    on_unload_callback_ = std::move(callback);
}

// ============================================================================
// Internal helpers
// ============================================================================

void IndexRegistry::touch(IndexMetadata& meta) {
    meta.last_access = std::chrono::steady_clock::now();
    meta.access_count.fetch_add(1, std::memory_order_relaxed);
}

// ============================================================================
// Testing
// ============================================================================

void IndexRegistry::remove_index(const std::string& field_name) {
    // First unload if loaded
    unload_index(field_name);

    // Then remove from registry
    std::lock_guard<std::mutex> lock(registry_mutex_);
    entries_.erase(field_name);
}

void IndexRegistry::reset() {
    std::lock_guard<std::mutex> lock(registry_mutex_);

    // Unload all indexes
    for (auto& [name, entry] : entries_) {
        if (entry->index_ptr && entry->destructor) {
            entry->destructor(entry->index_ptr);
            entry->index_ptr = nullptr;
        }
    }

    entries_.clear();
    on_load_callback_ = nullptr;
    on_unload_callback_ = nullptr;
}

} // namespace persist
} // namespace xtree
