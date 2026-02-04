/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * IndexRegistry: Manages lazy loading and lifecycle of field indexes.
 *
 * This class provides:
 * - Catalog of all known indexes (from manifest or registration)
 * - Lazy loading of indexes on first access
 * - Unloading of cold indexes under memory pressure
 * - Integration with MemoryCoordinator for adaptive memory management
 *
 * Usage:
 *   // Register an index (doesn't load it yet)
 *   IndexRegistry::global().register_index("user_locations", "/data/users", config);
 *
 *   // Get or load an index (loads on first access)
 *   auto* idx = IndexRegistry::global().get_or_load<DataRecord>("user_locations");
 *
 *   // Under memory pressure, unload cold indexes
 *   IndexRegistry::global().unload_cold_indexes(target_memory_to_free);
 */

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace xtree {

namespace persist {
// Forward declaration for manifest integration
class Manifest;
} // namespace persist

// Forward declarations
template<class Record> class IndexDetails;
class IRecord;
class DataRecord;

namespace persist {

/**
 * Load state for lazy index management.
 */
enum class IndexLoadState {
    REGISTERED,  // Known but not loaded
    LOADING,     // Currently being loaded
    LOADED,      // Fully loaded and ready
    UNLOADING,   // Currently being unloaded
    FAILED       // Load failed (can retry)
};

/**
 * Configuration for a registered index.
 */
struct IndexConfig {
    std::string field_name;
    std::string data_dir;
    unsigned short dimension = 2;
    unsigned short precision = 32;
    bool read_only = false;
    std::vector<std::string> dimension_labels;

    // Optional: pre-configured dimension labels pointer (for compatibility)
    std::vector<const char*>* dim_labels_ptr = nullptr;
};

/**
 * Metadata about a registered index.
 */
struct IndexMetadata {
    IndexConfig config;
    IndexLoadState state = IndexLoadState::REGISTERED;

    // Timing for LRU-based unloading
    std::chrono::steady_clock::time_point last_access;
    std::chrono::steady_clock::time_point loaded_at;

    // Memory tracking
    size_t estimated_memory = 0;  // Bytes when loaded

    // Access statistics
    std::atomic<uint64_t> access_count{0};
    std::atomic<uint64_t> load_count{0};
};

/**
 * IndexRegistry: Global registry for lazy index management.
 *
 * Thread-safety:
 *   All public methods are thread-safe. Loading/unloading operations
 *   are serialized per-index to prevent races.
 */
class IndexRegistry {
public:
    // Singleton accessor
    static IndexRegistry& global();

    // Disable copy/move
    IndexRegistry(const IndexRegistry&) = delete;
    IndexRegistry& operator=(const IndexRegistry&) = delete;
    IndexRegistry(IndexRegistry&&) = delete;
    IndexRegistry& operator=(IndexRegistry&&) = delete;

    // ========== Registration ==========

    /**
     * Register an index without loading it.
     * @param field_name Unique identifier for this index
     * @param config Index configuration
     * @return true if registered, false if already exists
     */
    bool register_index(const std::string& field_name, const IndexConfig& config);

    /**
     * Register all fields from a manifest.
     * This is the primary serverless initialization method.
     *
     * @param manifest Loaded manifest with root entries
     * @param defaults Default configuration (dimension, precision, read_only)
     * @return Number of fields registered
     */
    size_t register_from_manifest(const Manifest& manifest, const IndexConfig& defaults);

    /**
     * Register all fields from a data directory.
     * Convenience method that loads manifest and registers all fields.
     *
     * @param data_dir Path to persistent data directory
     * @param defaults Default configuration (dimension, precision, read_only)
     * @return Number of fields registered, or 0 if manifest not found
     */
    size_t register_from_data_dir(const std::string& data_dir, const IndexConfig& defaults);

    /**
     * Check if an index is registered.
     */
    bool is_registered(const std::string& field_name) const;

    /**
     * Get the load state of an index.
     */
    IndexLoadState get_state(const std::string& field_name) const;

    /**
     * Get metadata for an index.
     */
    const IndexMetadata* get_metadata(const std::string& field_name) const;

    // ========== Loading ==========

    /**
     * Get an index, loading it if necessary.
     * This is the primary access method - handles lazy loading automatically.
     *
     * @param field_name Index to get
     * @return Pointer to loaded index, or nullptr if failed
     */
    template<class Record>
    IndexDetails<Record>* get_or_load(const std::string& field_name);

    /**
     * Explicitly load an index (if not already loaded).
     * @param field_name Index to load
     * @return true if loaded successfully
     */
    template<class Record>
    bool load_index(const std::string& field_name);

    /**
     * Check if an index is currently loaded.
     */
    bool is_loaded(const std::string& field_name) const;

    // ========== Unloading ==========

    /**
     * Unload an index to free memory.
     * The index remains registered and can be reloaded on next access.
     *
     * @param field_name Index to unload
     * @return Bytes freed, or 0 if not loaded
     */
    size_t unload_index(const std::string& field_name);

    /**
     * Unload cold indexes to free target amount of memory.
     * Uses LRU ordering - least recently accessed indexes are unloaded first.
     *
     * @param target_bytes Amount of memory to free
     * @return Actual bytes freed
     */
    size_t unload_cold_indexes(size_t target_bytes);

    /**
     * Unload indexes that haven't been accessed for a given duration.
     *
     * @param max_idle Maximum idle time before unloading
     * @return Number of indexes unloaded
     */
    size_t unload_idle_indexes(std::chrono::seconds max_idle);

    // ========== Metrics ==========

    /**
     * Get number of registered indexes.
     */
    size_t registered_count() const;

    /**
     * Get number of currently loaded indexes.
     */
    size_t loaded_count() const;

    /**
     * Get total estimated memory of loaded indexes.
     */
    size_t total_loaded_memory() const;

    /**
     * Get list of all registered field names.
     */
    std::vector<std::string> get_registered_fields() const;

    /**
     * Get list of currently loaded field names.
     */
    std::vector<std::string> get_loaded_fields() const;

    // ========== Callbacks ==========

    /**
     * Set callback for when an index is loaded.
     */
    void set_on_load_callback(std::function<void(const std::string&)> callback);

    /**
     * Set callback for when an index is unloaded.
     */
    void set_on_unload_callback(std::function<void(const std::string&)> callback);

    // ========== Testing ==========

    /**
     * Remove an index from the registry entirely.
     * Only use for testing - in production, use unload_index().
     */
    void remove_index(const std::string& field_name);

    /**
     * Reset registry to empty state (for testing).
     */
    void reset();

private:
    IndexRegistry() = default;
    ~IndexRegistry();

    // Internal loading implementation
    template<class Record>
    IndexDetails<Record>* load_index_impl(const std::string& field_name,
                                          IndexMetadata& meta);

    // Internal unloading implementation
    size_t unload_index_impl(const std::string& field_name, IndexMetadata& meta);

    // Update last access time
    void touch(IndexMetadata& meta);

    // Per-index state
    struct IndexEntry {
        IndexMetadata metadata;
        void* index_ptr = nullptr;  // Type-erased IndexDetails<Record>*
        std::mutex load_mutex;      // Serializes load/unload for this index

        // Destructor function for type-erased cleanup
        std::function<void(void*)> destructor;
    };

    // Registry state
    mutable std::mutex registry_mutex_;
    std::unordered_map<std::string, std::unique_ptr<IndexEntry>> entries_;

    // Callbacks
    std::function<void(const std::string&)> on_load_callback_;
    std::function<void(const std::string&)> on_unload_callback_;
};

// ============================================================================
// Template implementations (must be in header)
// ============================================================================

template<class Record>
IndexDetails<Record>* IndexRegistry::get_or_load(const std::string& field_name) {
    std::unique_lock<std::mutex> lock(registry_mutex_);

    auto it = entries_.find(field_name);
    if (it == entries_.end()) {
        return nullptr;  // Not registered
    }

    auto& entry = *it->second;
    auto& meta = entry.metadata;

    // Fast path: already loaded
    if (meta.state == IndexLoadState::LOADED && entry.index_ptr) {
        touch(meta);
        lock.unlock();
        return static_cast<IndexDetails<Record>*>(entry.index_ptr);
    }

    // Need to load - drop registry lock, take per-index lock
    lock.unlock();

    std::lock_guard<std::mutex> load_lock(entry.load_mutex);

    // Double-check after acquiring load lock
    if (meta.state == IndexLoadState::LOADED && entry.index_ptr) {
        touch(meta);
        return static_cast<IndexDetails<Record>*>(entry.index_ptr);
    }

    // Actually load
    return load_index_impl<Record>(field_name, meta);
}

template<class Record>
bool IndexRegistry::load_index(const std::string& field_name) {
    return get_or_load<Record>(field_name) != nullptr;
}

template<class Record>
IndexDetails<Record>* IndexRegistry::load_index_impl(const std::string& field_name,
                                                      IndexMetadata& meta) {
    // Must hold entry's load_mutex when calling this

    if (meta.state == IndexLoadState::LOADING) {
        return nullptr;  // Already loading (shouldn't happen with proper locking)
    }

    meta.state = IndexLoadState::LOADING;

    try {
        const auto& config = meta.config;

        // Build dimension labels pointer
        std::vector<const char*>* dim_labels = config.dim_labels_ptr;
        std::vector<const char*> temp_labels;
        if (!dim_labels && !config.dimension_labels.empty()) {
            temp_labels.reserve(config.dimension_labels.size());
            for (const auto& label : config.dimension_labels) {
                temp_labels.push_back(label.c_str());
            }
            dim_labels = &temp_labels;
        }

        // Create the IndexDetails
        auto* idx = new IndexDetails<Record>(
            config.dimension,
            config.precision,
            dim_labels,
            nullptr,  // jvm
            nullptr,  // xtPOJO
            config.field_name,
            IndexDetails<Record>::PersistenceMode::DURABLE,
            config.data_dir,
            config.read_only
        );

        // Store in registry
        {
            std::lock_guard<std::mutex> lock(registry_mutex_);
            auto it = entries_.find(field_name);
            if (it != entries_.end()) {
                it->second->index_ptr = idx;
                it->second->destructor = [](void* p) {
                    delete static_cast<IndexDetails<Record>*>(p);
                };
            }
        }

        // Update metadata
        meta.state = IndexLoadState::LOADED;
        meta.loaded_at = std::chrono::steady_clock::now();
        meta.last_access = meta.loaded_at;
        meta.load_count.fetch_add(1, std::memory_order_relaxed);

        // Callback
        if (on_load_callback_) {
            on_load_callback_(field_name);
        }

        return idx;

    } catch (const std::exception& e) {
        meta.state = IndexLoadState::FAILED;
        return nullptr;
    }
}

} // namespace persist
} // namespace xtree
