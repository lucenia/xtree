# Lazy Index Loading Implementation

**Status**: Implemented and Tested
**Date**: 2026-02-01
**Goal**: Enable scalable serverless deployments with 1000+ indexes by loading indexes on-demand

## Executive Summary

The IndexRegistry provides lazy loading of field indexes to support large-scale serverless deployments. Instead of loading all indexes at startup (which would cause OOM with 1000+ indexes), indexes are registered as lightweight metadata and loaded on-demand when first accessed. Cold indexes can be automatically unloaded under memory pressure.

## Problem Statement

### Original Issue
- With 1000 indexes × 1000 fields = 1,000,000 field indexes
- Each loaded IndexDetails requires ~4-10 KB of overhead
- Loading all at startup: 10-50+ GB of memory
- Serverless environments (Lambda, Cloud Run) have memory limits (10-32 GB max)

### Solution
- Register indexes as lightweight metadata (~100 bytes each)
- Load DurableRuntime only on first access
- Unload cold indexes automatically under memory pressure
- Support reload on subsequent access

## Architecture

```
                        IndexRegistry (Singleton)
    ┌─────────────────────────────────────────────────────────────┐
    │                                                             │
    │   entries_: map<field_name, IndexEntry>                     │
    │                                                             │
    │   ┌─────────────────────────────────────────────────────┐   │
    │   │  "user_locations"                                   │   │
    │   │    state: LOADED                                    │   │
    │   │    last_access: 2026-02-01 12:00:00                 │   │
    │   │    index_ptr: 0x7f8b4c000000                        │   │
    │   └─────────────────────────────────────────────────────┘   │
    │                                                             │
    │   ┌─────────────────────────────────────────────────────┐   │
    │   │  "product_embeddings"                               │   │
    │   │    state: REGISTERED  (not loaded)                  │   │
    │   │    last_access: N/A                                 │   │
    │   │    index_ptr: nullptr                               │   │
    │   └─────────────────────────────────────────────────────┘   │
    │                                                             │
    │   ┌─────────────────────────────────────────────────────┐   │
    │   │  "order_history"                                    │   │
    │   │    state: REGISTERED  (was loaded, then unloaded)   │   │
    │   │    last_access: 2026-02-01 11:30:00                 │   │
    │   │    index_ptr: nullptr                               │   │
    │   └─────────────────────────────────────────────────────┘   │
    │                                                             │
    └─────────────────────────────────────────────────────────────┘
                            │
                            │ get_or_load("product_embeddings")
                            ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    Load Process                             │
    │                                                             │
    │  1. Acquire per-index load_mutex (prevents races)           │
    │  2. Create IndexDetails with full DurableRuntime            │
    │  3. Load checkpoint, replay WAL (unless read_only)          │
    │  4. Update state: REGISTERED → LOADED                       │
    │  5. Store index_ptr, update last_access                     │
    │  6. Fire on_load_callback if registered                     │
    │  7. Return IndexDetails pointer                             │
    │                                                             │
    └─────────────────────────────────────────────────────────────┘
```

## API Reference

### IndexConfig

Configuration for registering an index:

```cpp
struct IndexConfig {
    std::string field_name;           // Unique identifier
    std::string data_dir;             // Path to persistent data
    unsigned short dimension = 2;     // Number of dimensions
    unsigned short precision = 32;    // Bucket capacity
    bool read_only = false;           // Serverless reader mode
    std::vector<std::string> dimension_labels;  // Optional labels
};
```

### IndexLoadState

```cpp
enum class IndexLoadState {
    REGISTERED,  // Known but not loaded (lightweight)
    LOADING,     // Currently being loaded (transient)
    LOADED,      // Fully loaded and ready for queries
    UNLOADING,   // Currently being unloaded (transient)
    FAILED       // Load failed (can retry)
};
```

### IndexRegistry Methods

#### Registration

```cpp
// Register an index without loading it
bool register_index(const std::string& field_name, const IndexConfig& config);

// Register all fields from a manifest (primary serverless initialization)
size_t register_from_manifest(const Manifest& manifest, const IndexConfig& defaults);

// Register all fields from a data directory (convenience method)
size_t register_from_data_dir(const std::string& data_dir, const IndexConfig& defaults);

// Check if an index is registered
bool is_registered(const std::string& field_name) const;

// Get current state
IndexLoadState get_state(const std::string& field_name) const;
```

#### Loading (Lazy)

```cpp
// Get or load an index - primary access method
// Loads on first access, returns cached pointer on subsequent calls
template<class Record>
IndexDetails<Record>* get_or_load(const std::string& field_name);

// Check if currently loaded
bool is_loaded(const std::string& field_name) const;
```

#### Unloading

```cpp
// Unload a specific index to free memory
// Index stays registered and can be reloaded
size_t unload_index(const std::string& field_name);

// Unload cold indexes using LRU ordering
// Unloads least-recently-accessed first until target_bytes freed
size_t unload_cold_indexes(size_t target_bytes);

// Unload indexes idle for more than max_idle seconds
size_t unload_idle_indexes(std::chrono::seconds max_idle);
```

#### Metrics

```cpp
size_t registered_count() const;      // Total registered
size_t loaded_count() const;          // Currently loaded
size_t total_loaded_memory() const;   // Estimated memory of loaded indexes

std::vector<std::string> get_registered_fields() const;
std::vector<std::string> get_loaded_fields() const;
```

#### Callbacks

```cpp
// Called when an index is loaded
void set_on_load_callback(std::function<void(const std::string&)> callback);

// Called when an index is unloaded
void set_on_unload_callback(std::function<void(const std::string&)> callback);
```

## Usage Examples

### Basic Usage

```cpp
#include "persistence/index_registry.h"
#include "persistence/manifest.h"

using namespace xtree::persist;

// At startup: register all indexes from data directory (recommended)
void serverless_startup(const std::string& data_dir) {
    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = true;  // Serverless = read-only

    size_t count = IndexRegistry::global().register_from_data_dir(data_dir, defaults);
    std::cout << "Registered " << count << " indexes (none loaded yet)\n";
}

// Alternative: register from already-loaded manifest
void register_from_manifest(const Manifest& manifest) {
    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = true;

    IndexRegistry::global().register_from_manifest(manifest, defaults);
}

// When a query arrives: load on demand
IndexDetails<DataRecord>* get_index(const std::string& field_name) {
    auto* idx = IndexRegistry::global().get_or_load<DataRecord>(field_name);
    if (!idx) {
        throw std::runtime_error("Unknown field: " + field_name);
    }
    return idx;
}

// Query a field (lazy loads if needed)
void handle_query(const std::string& field, const Query& q) {
    auto* idx = get_index(field);  // Loads on first access
    idx->root_bucket<DataRecord>()->xt_search(...);
}
```

### Integration with MemoryCoordinator

The MemoryCoordinator automatically triggers cold index unloading when both cache and mmap are under pressure:

```cpp
// In MemoryCoordinator::rebalance_if_needed()
if (cache_under_pressure && mmap_under_pressure) {
    // Both pressured - try to free memory by unloading cold indexes
    size_t target_free = total_budget_ / 10;  // Free 10% of budget
    size_t freed = IndexRegistry::global().unload_cold_indexes(target_free);
    if (freed > 0) {
        std::cout << "[MemoryCoordinator] Unloaded cold indexes, freed "
                  << (freed / (1024*1024)) << " MB\n";
    }
}
```

### Serverless Deployment Pattern

```cpp
// Lambda/Cloud Run initialization (cold start)
void on_cold_start(const std::string& data_dir) {
    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = true;

    // Register all fields from manifest - fast, O(n) metadata only
    size_t count = IndexRegistry::global().register_from_data_dir(data_dir, defaults);
    std::cout << "Cold start: registered " << count << " fields\n";
}

// Lambda/Cloud Run handler
Response handle_request(const Request& req) {
    // First request to this field will load the index
    // Subsequent requests use cached index
    auto* idx = IndexRegistry::global().get_or_load<DataRecord>(req.field);

    if (!idx) {
        return Response::not_found("Unknown field");
    }

    // Perform query
    auto results = idx->root_bucket<DataRecord>()->xt_search(req.query);

    return Response::ok(results);
}

// Periodic cleanup (e.g., between requests or on timer)
void cleanup_cold_indexes() {
    // Unload indexes not accessed in last 5 minutes
    IndexRegistry::global().unload_idle_indexes(std::chrono::seconds{300});
}
```

## Memory Impact

### Before Lazy Loading

| Scenario | Indexes | Memory at Startup |
|----------|---------|-------------------|
| Small | 100 | ~500 MB |
| Medium | 1,000 | ~5 GB |
| Large | 10,000 | ~50 GB (OOM) |
| Target | 1,000,000 | ~500 GB (impossible) |

### After Lazy Loading

| Scenario | Indexes | Memory at Startup | Memory per Load |
|----------|---------|-------------------|-----------------|
| Small | 100 | ~10 KB | ~5 MB per active |
| Medium | 1,000 | ~100 KB | ~5 MB per active |
| Large | 10,000 | ~1 MB | ~5 MB per active |
| Target | 1,000,000 | ~100 MB | ~5 MB per active |

**Key insight**: Memory usage is now proportional to **active** indexes, not **total** indexes.

## Test Results

### Unit Tests (26 tests, all passing)

```
# Core Registration & Loading
IndexRegistryTest.RegisterIndex
IndexRegistryTest.RegisterDuplicateFails
IndexRegistryTest.IsRegisteredForUnknown
IndexRegistryTest.GetStateForRegistered
IndexRegistryTest.GetOrLoadCreatesIndex
IndexRegistryTest.GetOrLoadReturnsSameInstance
IndexRegistryTest.GetOrLoadForUnregisteredReturnsNull
IndexRegistryTest.LoadedIndexIsUsable

# Unloading & Lifecycle
IndexRegistryTest.UnloadIndex
IndexRegistryTest.UnloadAndReload
IndexRegistryTest.UnloadColdIndexes
IndexRegistryTest.MetadataTracksAccess
IndexRegistryTest.LoadCallback
IndexRegistryTest.UnloadCallback
IndexRegistryTest.GetRegisteredFields
IndexRegistryTest.GetLoadedFields

# Thread Safety
IndexRegistryTest.ConcurrentLoads
IndexRegistryTest.ConcurrentRegisterAndLoad
IndexRegistryTest.Reset
IndexRegistryTest.MemoryPressureTriggersUnload

# Manifest Integration (NEW)
IndexRegistryTest.RegisterFromManifest
IndexRegistryTest.RegisterFromManifestInfersDimension
IndexRegistryTest.RegisterFromDataDir
IndexRegistryTest.RegisterFromDataDirNoManifest
IndexRegistryTest.ManifestRegisteredFieldsCanLoad
IndexRegistryTest.ServerlessPatternEndToEnd
```

### Serverless Scaling Test (100 fields)

```
| Fields | RSS (MB) | MMap (MB) | Throughput |
|--------|----------|-----------|------------|
| 10     | 14       | ~2 GB     | 100K/s     |
| 50     | 40       | ~4 GB     | 100K/s     |
| 100    | 81       | ~4 GB     | 100K/s     |
```

Key observations:
- RSS grows sub-linearly (~0.8 MB per field)
- MMap stays bounded (lazy remapping works)
- Throughput remains high

## Thread Safety

The IndexRegistry is fully thread-safe:

1. **Global registry mutex**: Protects the entries map for registration and lookup
2. **Per-index load mutex**: Serializes load/unload operations per index
3. **Double-checked locking**: Avoids unnecessary locking on hot path

```cpp
template<class Record>
IndexDetails<Record>* IndexRegistry::get_or_load(const std::string& field_name) {
    std::unique_lock<std::mutex> lock(registry_mutex_);

    auto it = entries_.find(field_name);
    if (it == entries_.end()) return nullptr;

    auto& entry = *it->second;

    // Fast path: already loaded
    if (entry.metadata.state == IndexLoadState::LOADED && entry.index_ptr) {
        touch(entry.metadata);  // Update LRU
        lock.unlock();
        return static_cast<IndexDetails<Record>*>(entry.index_ptr);
    }

    // Slow path: need to load
    lock.unlock();
    std::lock_guard<std::mutex> load_lock(entry.load_mutex);

    // Double-check after acquiring load lock
    if (entry.metadata.state == IndexLoadState::LOADED && entry.index_ptr) {
        return static_cast<IndexDetails<Record>*>(entry.index_ptr);
    }

    return load_index_impl<Record>(field_name, entry.metadata);
}
```

## Integration Points

### Files Modified

| File | Changes |
|------|---------|
| `memory_coordinator.cpp` | Added `#include "index_registry.h"`, calls `unload_cold_indexes()` under pressure |
| `CMakeLists.txt` | Added `test/test_index_registry.cpp` to test sources |

### Files Created

| File | Purpose |
|------|---------|
| `persistence/index_registry.h` | IndexRegistry class definition and templates |
| `persistence/index_registry.cpp` | IndexRegistry implementation |
| `test/test_index_registry.cpp` | Unit tests |

## Future Enhancements

1. ~~**Manifest Integration**: Auto-register indexes from manifest~~ ✅ DONE
2. **Preloading Hints**: Allow marking certain fields as "preload" for faster first access
3. **Memory Quotas**: Per-index memory limits (from SHARDED_SUBSTRATE_DESIGN.md)
4. **Statistics Export**: Prometheus/StatsD integration for monitoring
5. **Warm Pool**: Keep N most-recently-used indexes always loaded

## Conclusion

The IndexRegistry provides the foundation for scaling to 1000+ indexes in serverless environments. By deferring index loading until first access and automatically unloading cold indexes under memory pressure, memory usage becomes proportional to active workload rather than total index count.

Combined with the MemoryCoordinator (adaptive cache/mmap budget) and existing lazy segment remapping, the system can now handle large-scale multi-tenant deployments within serverless memory constraints.
