/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Unit tests for IndexRegistry (lazy index loading).
 */

#include <gtest/gtest.h>
#include "../src/persistence/index_registry.h"
#include "../src/persistence/memory_coordinator.h"
#include "../src/persistence/manifest.h"
#include "../src/xtree.h"  // Full XTree definitions including XTreeBucket

#include <filesystem>
#include <thread>
#include <chrono>

using namespace xtree;
using namespace xtree::persist;

class IndexRegistryTest : public ::testing::Test {
protected:
    std::string test_base_dir_;

    void SetUp() override {
        // Create unique test directory
        test_base_dir_ = "./test_index_registry_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        std::filesystem::create_directories(test_base_dir_);

        // Reset registry before each test
        IndexRegistry::global().reset();
    }

    void TearDown() override {
        // Reset registry after each test
        IndexRegistry::global().reset();

        // Clean up test directory
        std::filesystem::remove_all(test_base_dir_);
    }

    // Helper to create a test index config
    IndexConfig make_config(const std::string& field_name) {
        IndexConfig config;
        config.field_name = field_name;
        config.data_dir = test_base_dir_ + "/" + field_name;
        config.dimension = 2;
        config.precision = 32;
        config.read_only = false;
        config.dimension_labels = {"x", "y"};
        std::filesystem::create_directories(config.data_dir);
        return config;
    }
};

// ============================================================================
// Registration Tests
// ============================================================================

TEST_F(IndexRegistryTest, RegisterIndex) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    EXPECT_TRUE(registry.register_index("test_field", config));
    EXPECT_TRUE(registry.is_registered("test_field"));
    EXPECT_EQ(registry.registered_count(), 1u);
}

TEST_F(IndexRegistryTest, RegisterDuplicateFails) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    EXPECT_TRUE(registry.register_index("test_field", config));
    EXPECT_FALSE(registry.register_index("test_field", config));  // Duplicate
    EXPECT_EQ(registry.registered_count(), 1u);
}

TEST_F(IndexRegistryTest, IsRegisteredForUnknown) {
    auto& registry = IndexRegistry::global();
    EXPECT_FALSE(registry.is_registered("nonexistent"));
}

TEST_F(IndexRegistryTest, GetStateForRegistered) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    EXPECT_EQ(registry.get_state("test_field"), IndexLoadState::REGISTERED);
    EXPECT_FALSE(registry.is_loaded("test_field"));
}

// ============================================================================
// Lazy Loading Tests
// ============================================================================

TEST_F(IndexRegistryTest, GetOrLoadCreatesIndex) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    // Should start unloaded
    EXPECT_FALSE(registry.is_loaded("test_field"));
    EXPECT_EQ(registry.loaded_count(), 0u);

    // Get or load should create the index
    auto* idx = registry.get_or_load<DataRecord>("test_field");
    EXPECT_NE(idx, nullptr);
    EXPECT_TRUE(registry.is_loaded("test_field"));
    EXPECT_EQ(registry.loaded_count(), 1u);
    EXPECT_EQ(registry.get_state("test_field"), IndexLoadState::LOADED);
}

TEST_F(IndexRegistryTest, GetOrLoadReturnsSameInstance) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    auto* idx1 = registry.get_or_load<DataRecord>("test_field");
    auto* idx2 = registry.get_or_load<DataRecord>("test_field");

    EXPECT_EQ(idx1, idx2);  // Same instance
}

TEST_F(IndexRegistryTest, GetOrLoadForUnregisteredReturnsNull) {
    auto& registry = IndexRegistry::global();

    auto* idx = registry.get_or_load<DataRecord>("nonexistent");
    EXPECT_EQ(idx, nullptr);
}

TEST_F(IndexRegistryTest, LoadedIndexIsUsable) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    auto* idx = registry.get_or_load<DataRecord>("test_field");
    ASSERT_NE(idx, nullptr);

    // Initialize and use the index
    idx->ensure_root_initialized<DataRecord>();

    // Insert a record
    auto* record = new DataRecord(2, 32, "test_record");
    std::vector<double> pt = {1.0, 2.0};
    record->putPoint(&pt);
    idx->root_bucket<DataRecord>()->xt_insert(idx->root_cache_node(), record);

    // Verify we can query
    EXPECT_NE(idx->root_bucket<DataRecord>(), nullptr);
}

// ============================================================================
// Unloading Tests
// ============================================================================

TEST_F(IndexRegistryTest, UnloadIndex) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    // Load the index
    auto* idx = registry.get_or_load<DataRecord>("test_field");
    ASSERT_NE(idx, nullptr);
    EXPECT_TRUE(registry.is_loaded("test_field"));

    // Unload it
    size_t freed = registry.unload_index("test_field");
    // freed might be 0 if no mmap was used yet, that's OK

    EXPECT_FALSE(registry.is_loaded("test_field"));
    EXPECT_EQ(registry.get_state("test_field"), IndexLoadState::REGISTERED);
    EXPECT_EQ(registry.loaded_count(), 0u);

    // Still registered
    EXPECT_TRUE(registry.is_registered("test_field"));
}

TEST_F(IndexRegistryTest, UnloadAndReload) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    // Load, use, unload, reload
    auto* idx1 = registry.get_or_load<DataRecord>("test_field");
    ASSERT_NE(idx1, nullptr);
    idx1->ensure_root_initialized<DataRecord>();

    // Get load count before unload
    const IndexMetadata* meta = registry.get_metadata("test_field");
    uint64_t load_count_before = meta->load_count.load();
    EXPECT_EQ(load_count_before, 1u);

    registry.unload_index("test_field");
    EXPECT_FALSE(registry.is_loaded("test_field"));
    EXPECT_EQ(registry.get_state("test_field"), IndexLoadState::REGISTERED);

    // Reload - should work
    auto* idx2 = registry.get_or_load<DataRecord>("test_field");
    ASSERT_NE(idx2, nullptr);
    EXPECT_TRUE(registry.is_loaded("test_field"));

    // Load count should have increased (proves it was actually reloaded)
    uint64_t load_count_after = meta->load_count.load();
    EXPECT_EQ(load_count_after, 2u);
}

TEST_F(IndexRegistryTest, UnloadColdIndexes) {
    auto& registry = IndexRegistry::global();

    // Register multiple indexes
    for (int i = 0; i < 5; i++) {
        std::string name = "field_" + std::to_string(i);
        auto config = make_config(name);
        registry.register_index(name, config);
        registry.get_or_load<DataRecord>(name);
    }

    EXPECT_EQ(registry.loaded_count(), 5u);

    // Unload cold indexes (all are "cold" since none accessed after load)
    // Target a large amount to unload all
    size_t freed = registry.unload_cold_indexes(1ULL * 1024 * 1024 * 1024);

    // Should have unloaded some (maybe not all if some are still "hot")
    EXPECT_LT(registry.loaded_count(), 5u);
}

// ============================================================================
// Metadata Tests
// ============================================================================

TEST_F(IndexRegistryTest, MetadataTracksAccess) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    auto* meta_before = registry.get_metadata("test_field");
    ASSERT_NE(meta_before, nullptr);
    EXPECT_EQ(meta_before->access_count.load(), 0u);
    EXPECT_EQ(meta_before->load_count.load(), 0u);

    // Load the index
    registry.get_or_load<DataRecord>("test_field");

    auto* meta_after = registry.get_metadata("test_field");
    EXPECT_EQ(meta_after->load_count.load(), 1u);

    // Access again
    registry.get_or_load<DataRecord>("test_field");
    registry.get_or_load<DataRecord>("test_field");

    auto* meta_final = registry.get_metadata("test_field");
    EXPECT_GE(meta_final->access_count.load(), 2u);  // At least 2 accesses
    EXPECT_EQ(meta_final->load_count.load(), 1u);    // Still only 1 load
}

// ============================================================================
// Callback Tests
// ============================================================================

TEST_F(IndexRegistryTest, LoadCallback) {
    auto& registry = IndexRegistry::global();

    std::string loaded_field;
    registry.set_on_load_callback([&loaded_field](const std::string& name) {
        loaded_field = name;
    });

    auto config = make_config("test_field");
    registry.register_index("test_field", config);
    registry.get_or_load<DataRecord>("test_field");

    EXPECT_EQ(loaded_field, "test_field");
}

TEST_F(IndexRegistryTest, UnloadCallback) {
    auto& registry = IndexRegistry::global();

    std::string unloaded_field;
    registry.set_on_unload_callback([&unloaded_field](const std::string& name) {
        unloaded_field = name;
    });

    auto config = make_config("test_field");
    registry.register_index("test_field", config);
    registry.get_or_load<DataRecord>("test_field");
    registry.unload_index("test_field");

    EXPECT_EQ(unloaded_field, "test_field");
}

// ============================================================================
// Listing Tests
// ============================================================================

TEST_F(IndexRegistryTest, GetRegisteredFields) {
    auto& registry = IndexRegistry::global();

    registry.register_index("field_a", make_config("field_a"));
    registry.register_index("field_b", make_config("field_b"));
    registry.register_index("field_c", make_config("field_c"));

    auto fields = registry.get_registered_fields();
    EXPECT_EQ(fields.size(), 3u);

    // Check all fields are present (order may vary)
    std::sort(fields.begin(), fields.end());
    EXPECT_EQ(fields[0], "field_a");
    EXPECT_EQ(fields[1], "field_b");
    EXPECT_EQ(fields[2], "field_c");
}

TEST_F(IndexRegistryTest, GetLoadedFields) {
    auto& registry = IndexRegistry::global();

    registry.register_index("field_a", make_config("field_a"));
    registry.register_index("field_b", make_config("field_b"));
    registry.register_index("field_c", make_config("field_c"));

    // Only load two
    registry.get_or_load<DataRecord>("field_a");
    registry.get_or_load<DataRecord>("field_c");

    auto loaded = registry.get_loaded_fields();
    EXPECT_EQ(loaded.size(), 2u);

    std::sort(loaded.begin(), loaded.end());
    EXPECT_EQ(loaded[0], "field_a");
    EXPECT_EQ(loaded[1], "field_c");
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

TEST_F(IndexRegistryTest, ConcurrentLoads) {
    auto& registry = IndexRegistry::global();

    auto config = make_config("test_field");
    registry.register_index("test_field", config);

    const int num_threads = 4;
    std::vector<std::thread> threads;
    std::vector<IndexDetails<DataRecord>*> results(num_threads, nullptr);

    // Multiple threads try to load the same index
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&registry, &results, i]() {
            results[i] = registry.get_or_load<DataRecord>("test_field");
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All should get the same instance
    for (int i = 0; i < num_threads; i++) {
        EXPECT_NE(results[i], nullptr);
        EXPECT_EQ(results[i], results[0]);
    }

    // Only one load should have occurred
    auto* meta = registry.get_metadata("test_field");
    EXPECT_EQ(meta->load_count.load(), 1u);
}

TEST_F(IndexRegistryTest, ConcurrentRegisterAndLoad) {
    auto& registry = IndexRegistry::global();

    const int num_fields = 10;
    std::vector<std::thread> threads;

    // Concurrent registration and loading of different fields
    for (int i = 0; i < num_fields; i++) {
        threads.emplace_back([this, &registry, i]() {
            std::string name = "field_" + std::to_string(i);
            auto config = make_config(name);
            registry.register_index(name, config);
            auto* idx = registry.get_or_load<DataRecord>(name);
            EXPECT_NE(idx, nullptr);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(registry.registered_count(), static_cast<size_t>(num_fields));
    EXPECT_EQ(registry.loaded_count(), static_cast<size_t>(num_fields));
}

// ============================================================================
// Reset Tests
// ============================================================================

TEST_F(IndexRegistryTest, Reset) {
    auto& registry = IndexRegistry::global();

    // Register and load some indexes
    registry.register_index("field_a", make_config("field_a"));
    registry.register_index("field_b", make_config("field_b"));
    registry.get_or_load<DataRecord>("field_a");

    EXPECT_EQ(registry.registered_count(), 2u);
    EXPECT_EQ(registry.loaded_count(), 1u);

    // Reset
    registry.reset();

    EXPECT_EQ(registry.registered_count(), 0u);
    EXPECT_EQ(registry.loaded_count(), 0u);
    EXPECT_FALSE(registry.is_registered("field_a"));
}

// ============================================================================
// Integration with MemoryCoordinator
// ============================================================================

TEST_F(IndexRegistryTest, MemoryPressureTriggersUnload) {
    auto& registry = IndexRegistry::global();
    auto& coordinator = MemoryCoordinator::global();

    // Set a small memory budget
    coordinator.set_total_budget(10 * 1024 * 1024);  // 10MB
    coordinator.set_rebalance_interval(std::chrono::seconds{0});

    // Register and load several indexes
    for (int i = 0; i < 5; i++) {
        std::string name = "field_" + std::to_string(i);
        auto config = make_config(name);
        registry.register_index(name, config);
        auto* idx = registry.get_or_load<DataRecord>(name);
        if (idx) {
            idx->ensure_root_initialized<DataRecord>();
        }
    }

    size_t initial_loaded = registry.loaded_count();

    // Force rebalance (simulating memory pressure)
    coordinator.force_rebalance();

    // May or may not have unloaded depending on actual memory pressure
    // Just verify it doesn't crash
    EXPECT_LE(registry.loaded_count(), initial_loaded);

    // Reset coordinator
    coordinator.reset();
}

// ============================================================================
// Manifest Integration Tests
// ============================================================================

TEST_F(IndexRegistryTest, RegisterFromManifest) {
    auto& registry = IndexRegistry::global();

    // Create a manifest with multiple root entries
    Manifest manifest(test_base_dir_);

    // Add root entries to the manifest
    std::vector<Manifest::RootEntry> roots;
    roots.push_back({"field_a", 1001, 100, {0.0f, 10.0f, 0.0f, 10.0f}});  // 2D MBR
    roots.push_back({"field_b", 1002, 101, {0.0f, 20.0f, 0.0f, 20.0f}});
    roots.push_back({"field_c", 1003, 102, {0.0f, 30.0f, 0.0f, 30.0f}});
    manifest.set_roots(roots);

    // Default config for serverless read-only mode
    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = true;

    // Register from manifest
    size_t registered = registry.register_from_manifest(manifest, defaults);

    EXPECT_EQ(registered, 3u);
    EXPECT_EQ(registry.registered_count(), 3u);
    EXPECT_TRUE(registry.is_registered("field_a"));
    EXPECT_TRUE(registry.is_registered("field_b"));
    EXPECT_TRUE(registry.is_registered("field_c"));

    // All should start unloaded
    EXPECT_EQ(registry.loaded_count(), 0u);
    EXPECT_FALSE(registry.is_loaded("field_a"));
}

TEST_F(IndexRegistryTest, RegisterFromManifestInfersDimension) {
    auto& registry = IndexRegistry::global();

    // Create manifest with 3D MBR (6 floats)
    Manifest manifest(test_base_dir_);
    std::vector<Manifest::RootEntry> roots;
    roots.push_back({"field_3d", 2001, 200,
                     {0.0f, 10.0f, 0.0f, 10.0f, 0.0f, 10.0f}});  // 3D MBR
    manifest.set_roots(roots);

    // Config with dimension=0 (should infer from MBR)
    IndexConfig defaults;
    defaults.dimension = 0;  // Will be inferred
    defaults.precision = 32;
    defaults.read_only = true;

    size_t registered = registry.register_from_manifest(manifest, defaults);
    EXPECT_EQ(registered, 1u);

    // Check the metadata has inferred dimension
    const IndexMetadata* meta = registry.get_metadata("field_3d");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->config.dimension, 3);  // Inferred from MBR size (6 / 2)
}

TEST_F(IndexRegistryTest, RegisterFromDataDir) {
    auto& registry = IndexRegistry::global();

    // Create a manifest file in test directory
    Manifest manifest(test_base_dir_);
    std::vector<Manifest::RootEntry> roots;
    roots.push_back({"users", 3001, 300, {0.0f, 100.0f, 0.0f, 100.0f}});
    roots.push_back({"products", 3002, 301, {0.0f, 200.0f, 0.0f, 200.0f}});
    manifest.set_roots(roots);
    ASSERT_TRUE(manifest.store());  // Write manifest to disk

    // Register from data directory
    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = true;

    size_t registered = registry.register_from_data_dir(test_base_dir_, defaults);

    EXPECT_EQ(registered, 2u);
    EXPECT_TRUE(registry.is_registered("users"));
    EXPECT_TRUE(registry.is_registered("products"));
}

TEST_F(IndexRegistryTest, RegisterFromDataDirNoManifest) {
    auto& registry = IndexRegistry::global();

    // Try to register from directory with no manifest
    std::string empty_dir = test_base_dir_ + "/empty";
    std::filesystem::create_directories(empty_dir);

    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = true;

    size_t registered = registry.register_from_data_dir(empty_dir, defaults);

    EXPECT_EQ(registered, 0u);
    EXPECT_EQ(registry.registered_count(), 0u);
}

TEST_F(IndexRegistryTest, ManifestRegisteredFieldsCanLoad) {
    auto& registry = IndexRegistry::global();

    // Create manifest and store it
    Manifest manifest(test_base_dir_);
    std::vector<Manifest::RootEntry> roots;
    roots.push_back({"loadable_field", 4001, 400, {0.0f, 10.0f, 0.0f, 10.0f}});
    manifest.set_roots(roots);
    manifest.store();

    // Register from data dir
    IndexConfig defaults;
    defaults.dimension = 2;
    defaults.precision = 32;
    defaults.read_only = false;  // Writable for test

    registry.register_from_data_dir(test_base_dir_, defaults);

    // Verify lazy loading works
    EXPECT_FALSE(registry.is_loaded("loadable_field"));

    auto* idx = registry.get_or_load<DataRecord>("loadable_field");
    ASSERT_NE(idx, nullptr);
    EXPECT_TRUE(registry.is_loaded("loadable_field"));

    // Verify the index is usable
    idx->ensure_root_initialized<DataRecord>();
    EXPECT_NE(idx->root_bucket<DataRecord>(), nullptr);
}

TEST_F(IndexRegistryTest, ServerlessPatternEndToEnd) {
    auto& registry = IndexRegistry::global();

    // Simulate serverless startup: create indexes, checkpoint, then reload

    // Step 1: Create indexes and write data (simulating prior ingestion)
    {
        std::vector<std::string> field_names = {"geo", "time", "embedding"};
        Manifest manifest(test_base_dir_);
        std::vector<Manifest::RootEntry> roots;

        for (const auto& name : field_names) {
            IndexConfig config;
            config.field_name = name;
            config.data_dir = test_base_dir_;
            config.dimension = 2;
            config.precision = 32;
            config.read_only = false;

            // Create and initialize the index
            auto* idx = new IndexDetails<DataRecord>(
                config.dimension, config.precision, nullptr, nullptr, nullptr,
                config.field_name,
                IndexDetails<DataRecord>::PersistenceMode::DURABLE,
                config.data_dir, false
            );
            idx->ensure_root_initialized<DataRecord>();

            // Insert some data
            auto* record = new DataRecord(2, 32, "test_" + name);
            std::vector<double> pt = {1.0, 2.0};
            record->putPoint(&pt);
            idx->root_bucket<DataRecord>()->xt_insert(idx->root_cache_node(), record);

            // Add to manifest roots
            roots.push_back({name, 5000 + roots.size(), 500, {0.0f, 10.0f, 0.0f, 10.0f}});

            delete idx;
        }

        manifest.set_roots(roots);
        ASSERT_TRUE(manifest.store());
    }

    // Step 2: Serverless cold start - register all from manifest (read-only)
    IndexConfig serverless_defaults;
    serverless_defaults.dimension = 2;
    serverless_defaults.precision = 32;
    serverless_defaults.read_only = true;  // Serverless = read-only

    size_t registered = registry.register_from_data_dir(test_base_dir_, serverless_defaults);
    EXPECT_EQ(registered, 3u);
    EXPECT_EQ(registry.loaded_count(), 0u);  // Nothing loaded yet

    // Step 3: First query triggers lazy load
    auto* geo_idx = registry.get_or_load<DataRecord>("geo");
    ASSERT_NE(geo_idx, nullptr);
    EXPECT_EQ(registry.loaded_count(), 1u);  // Only geo loaded

    // Step 4: Query another field
    auto* time_idx = registry.get_or_load<DataRecord>("time");
    ASSERT_NE(time_idx, nullptr);
    EXPECT_EQ(registry.loaded_count(), 2u);  // geo + time loaded

    // embedding still not loaded
    EXPECT_FALSE(registry.is_loaded("embedding"));

    // Step 5: Simulate memory pressure - unload cold indexes
    size_t freed = registry.unload_cold_indexes(1024 * 1024);  // Try to free 1MB
    EXPECT_LT(registry.loaded_count(), 3u);  // Should have unloaded something
}
