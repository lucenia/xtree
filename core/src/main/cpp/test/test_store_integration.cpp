/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <gtest/gtest.h>
#include "../src/indexdetails.hpp"
#include "../src/persistence/memory_store.h"
#include "../src/persistence/durable_runtime.h"
#include "../src/persistence/durable_store.h"
#include <filesystem>
#include <cstring>
#include <unistd.h>

namespace xtree {
namespace persist {

class StoreIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any previous test data
        namespace fs = std::filesystem;
        test_dir_ = "/tmp/xtree_store_test_" + std::to_string(getpid());
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        namespace fs = std::filesystem;
        fs::remove_all(test_dir_);
    }
    
    std::string test_dir_;
};

TEST_F(StoreIntegrationTest, MemoryStoreBasicOperations) {
    MemoryStore store;
    
    // Test allocation
    auto alloc = store.allocate_node(1024, NodeKind::Leaf);
    EXPECT_NE(alloc.id.raw(), 0);
    EXPECT_NE(alloc.writable, nullptr);
    EXPECT_GE(alloc.capacity, 1024);
    
    // Write some data
    const char* test_data = "Hello, Store!";
    memcpy(alloc.writable, test_data, strlen(test_data) + 1);
    
    // Publish the node
    store.publish_node(alloc.id, alloc.writable, strlen(test_data) + 1);
    
    // Read it back
    auto bytes = store.read_node(alloc.id);
    EXPECT_NE(bytes.data, nullptr);
    EXPECT_EQ(bytes.size, strlen(test_data) + 1);
    EXPECT_STREQ(static_cast<const char*>(bytes.data), test_data);
    
    // Test root management
    EXPECT_EQ(store.get_root("test").raw(), NodeID::INVALID_RAW);
    store.set_root(alloc.id, 1, nullptr, 0, "test");
    EXPECT_EQ(store.get_root("test").raw(), alloc.id.raw());
}

TEST_F(StoreIntegrationTest, DurableStoreCreation) {
    // Setup paths
    Paths paths{
        .data_dir = test_dir_,
        .manifest = test_dir_ + "/manifest.json",
        .superblock = test_dir_ + "/superblock.bin",
        .active_log = test_dir_ + "/ot_delta.wal"
    };
    
    CheckpointPolicy policy{
        .max_replay_bytes = 100 * 1024 * 1024,  // 100MB
        .max_replay_epochs = 100000,
        .max_age = std::chrono::seconds(600),
        .min_interval = std::chrono::seconds(30)
    };
    
    // Create runtime
    auto runtime = DurableRuntime::open(paths, policy);
    ASSERT_NE(runtime, nullptr);
    
    // Create durable store context
    DurableContext ctx{
        .ot = runtime->ot(),
        .alloc = runtime->allocator(),
        .coord = runtime->coordinator(),
        .mvcc = runtime->mvcc(),
        .runtime = *runtime
    };
    
    // Create durable store
    DurableStore store(ctx, "test_store");
    
    // Test basic allocation
    auto alloc = store.allocate_node(2048, NodeKind::Internal);
    EXPECT_NE(alloc.id.raw(), 0);
    EXPECT_NE(alloc.writable, nullptr);
    EXPECT_GE(alloc.capacity, 2048);
    
    // Write and publish
    const char* test_data = "Durable Data!";
    memcpy(alloc.writable, test_data, strlen(test_data) + 1);
    store.publish_node(alloc.id, alloc.writable, strlen(test_data) + 1);
    
    // Need to commit to make the node visible (birth_epoch > 0)
    store.commit(1);
    
    // Read back
    auto bytes = store.read_node(alloc.id);
    EXPECT_NE(bytes.data, nullptr);
    // Note: bytes.size is the allocated size (from size class), not data size
    EXPECT_GE(bytes.size, strlen(test_data) + 1);
    // Verify the actual data content
    EXPECT_STREQ(static_cast<const char*>(bytes.data), test_data);
    
    // Set and get root
    store.set_root(alloc.id, 2, nullptr, 0, "");
    store.commit(2);
    EXPECT_EQ(store.get_root("").raw(), alloc.id.raw());
}

TEST_F(StoreIntegrationTest, IndexDetailsInMemoryMode) {
    // Create IndexDetails with IN_MEMORY mode
    std::vector<const char*> dims = {"x", "y", "z"};
    IndexDetails<IRecord> index(3, 2, &dims, nullptr, nullptr,
                                "memory_test_field",  // field name
                                IndexDetails<IRecord>::PersistenceMode::IN_MEMORY);
    
    // Verify we have a store
    auto* store = index.getStore();
    ASSERT_NE(store, nullptr);
    
    // Test allocation through the store
    auto alloc = store->allocate_node(512, NodeKind::Leaf);
    EXPECT_NE(alloc.id.raw(), 0);
    EXPECT_NE(alloc.writable, nullptr);
    
    // Write some test data
    const char* test_data = "test";
    memcpy(alloc.writable, test_data, strlen(test_data) + 1);
    store->publish_node(alloc.id, alloc.writable, strlen(test_data) + 1);
    
    // Read it back
    auto bytes = store->read_node(alloc.id);
    EXPECT_STREQ(static_cast<const char*>(bytes.data), test_data);
}

TEST_F(StoreIntegrationTest, IndexDetailsDurableMode) {
    // Create IndexDetails with DURABLE mode
    std::vector<const char*> dims = {"x", "y", "z"};
    IndexDetails<IRecord> index(3, 2, &dims, nullptr, nullptr,
                                "durable_test_field",  // field name
                                IndexDetails<IRecord>::PersistenceMode::DURABLE,
                                test_dir_);
    
    // Verify we have a durable store
    EXPECT_TRUE(index.hasDurableStore());
    auto* store = index.getStore();
    ASSERT_NE(store, nullptr);
    
    // Test allocation
    auto alloc = store->allocate_node(1024, NodeKind::Internal);
    EXPECT_NE(alloc.id.raw(), 0);
    EXPECT_NE(alloc.writable, nullptr);
    EXPECT_GE(alloc.capacity, 1024);
    
    // Write data
    const char* data = "Persistent xtree node";
    memcpy(alloc.writable, data, strlen(data) + 1);
    store->publish_node(alloc.id, alloc.writable, strlen(data) + 1);
    
    // Set as root
    store->set_root(alloc.id, 1, nullptr, 0, "");
    store->commit(1);
    
    // Verify root was set
    auto root_id = store->get_root("");
    EXPECT_EQ(root_id.handle_index(), alloc.id.handle_index());
    // The committed root may have the same tag (first use) or bumped tag (reuse)
    // We can't assume it's always bumped - that only happens on handle reuse
    // Just verify we got a valid root with the same handle
    EXPECT_NE(root_id.raw(), 0);
}

TEST_F(StoreIntegrationTest, MultipleNodesAllocation) {
    MemoryStore store;
    std::vector<NodeID> nodes;
    
    // Allocate multiple nodes
    for (int i = 0; i < 10; ++i) {
        auto alloc = store.allocate_node(256 * (i + 1), NodeKind::Leaf);
        EXPECT_NE(alloc.id.raw(), 0);
        
        // Write unique data
        std::string data = "Node " + std::to_string(i);
        memcpy(alloc.writable, data.c_str(), data.size() + 1);
        store.publish_node(alloc.id, alloc.writable, data.size() + 1);
        
        nodes.push_back(alloc.id);
    }
    
    // Verify all nodes
    for (int i = 0; i < 10; ++i) {
        auto bytes = store.read_node(nodes[i]);
        std::string expected = "Node " + std::to_string(i);
        EXPECT_STREQ(static_cast<const char*>(bytes.data), expected.c_str());
    }
}

TEST_F(StoreIntegrationTest, NodeRetirement) {
    MemoryStore store;
    
    // Allocate and publish a node
    auto alloc = store.allocate_node(128, NodeKind::Leaf);
    const char* data = "To be retired";
    memcpy(alloc.writable, data, strlen(data) + 1);
    store.publish_node(alloc.id, alloc.writable, strlen(data) + 1);
    
    // Retire the node
    store.retire_node(alloc.id, 2);
    
    // Should still be readable (MemoryStore doesn't implement reclamation)
    auto bytes = store.read_node(alloc.id);
    EXPECT_NE(bytes.data, nullptr);
}

} // namespace persist
} // namespace xtree