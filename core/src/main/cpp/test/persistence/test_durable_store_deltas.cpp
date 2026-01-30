/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <atomic>
#include <cstring>
#include "persistence/durable_runtime.h"
#include "persistence/durable_store.h"
#include "persistence/ot_delta_log.h"

using namespace xtree::persist;
namespace fs = std::filesystem;

class DurableStoreDeltaTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "test_durable_deltas_" + std::to_string(getpid());
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
        
        paths_.data_dir = test_dir_ + "/data";
        paths_.manifest = test_dir_ + "/manifest.json";
        paths_.superblock = test_dir_ + "/superblock.bin";
        paths_.active_log = test_dir_ + "/ot_delta.wal";
        
        fs::create_directories(paths_.data_dir);
        
        // Create runtime with default policy
        CheckpointPolicy policy;
        policy.min_interval = std::chrono::seconds(60); // Don't checkpoint during test
        runtime_ = DurableRuntime::open(paths_, policy);
        
        // Create context for store
        ctx_ = std::make_unique<DurableContext>(
            DurableContext{
                runtime_->ot(),
                runtime_->allocator(),
                runtime_->coordinator(),
                runtime_->mvcc(),
                *runtime_
            }
        );
        
        store_ = std::make_unique<DurableStore>(*ctx_, "test_store");
    }
    
    void TearDown() override {
        store_.reset();
        ctx_.reset();
        runtime_.reset();
        fs::remove_all(test_dir_);
    }
    
    // Helper to read the delta log and count records
    size_t count_delta_records() {
        // Get the active log from coordinator
        auto log = runtime_->coordinator().get_active_log();
        if (!log) return 0;
        
        // Sync to ensure writes are visible
        log->sync();
        
        // Count records by replaying from the actual log path
        size_t count = 0;
        OTDeltaLog reader(log->path());  // Use the actual log path
        
        reader.replay([&count](const OTDeltaRec& rec) {
            count++;
        });
        
        return count;
    }
    
protected:
    std::string test_dir_;
    Paths paths_;
    std::unique_ptr<DurableRuntime> runtime_;
    std::unique_ptr<DurableContext> ctx_;
    std::unique_ptr<DurableStore> store_;
};

TEST_F(DurableStoreDeltaTest, PublishNodeAppendsDelta) {
    // Allocate a node
    auto result = store_->allocate_node(1024, NodeKind::Leaf);
    ASSERT_NE(result.writable, nullptr);
    ASSERT_TRUE(result.id.valid());
    
    // Initially should have no deltas
    EXPECT_EQ(count_delta_records(), 0);
    
    // Fill with test data
    std::vector<uint8_t> data(1024, 0x42);
    std::memcpy(result.writable, data.data(), data.size());
    
    // Publish the node - this stages the delta
    store_->publish_node(result.id, data.data(), data.size());
    
    // Commit to actually write the delta to WAL
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Should now have 1 delta record
    EXPECT_EQ(count_delta_records(), 1);
}

TEST_F(DurableStoreDeltaTest, RetireNodeAppendsDelta) {
    // Allocate and publish a node
    auto result = store_->allocate_node(512, NodeKind::Internal);
    ASSERT_NE(result.writable, nullptr);
    
    std::vector<uint8_t> data(512, 0x33);
    std::memcpy(result.writable, data.data(), data.size());
    store_->publish_node(result.id, data.data(), data.size());
    
    // Commit the publish
    uint64_t epoch1 = runtime_->mvcc().advance_epoch();
    store_->commit(epoch1);
    
    EXPECT_EQ(count_delta_records(), 1);
    
    // Retire the node - stages another delta
    uint64_t retire_epoch = runtime_->mvcc().advance_epoch();
    store_->retire_node(result.id, retire_epoch);
    
    // Commit the retirement
    uint64_t epoch2 = runtime_->mvcc().advance_epoch();
    store_->commit(epoch2);
    
    // Should now have 2 delta records (allocate + retire)
    EXPECT_EQ(count_delta_records(), 2);
}

TEST_F(DurableStoreDeltaTest, MultipleDeltasAppended) {
    std::vector<NodeID> nodes;
    
    // Allocate and publish multiple nodes
    for (int i = 0; i < 10; ++i) {
        auto result = store_->allocate_node(256, NodeKind::Leaf);
        ASSERT_NE(result.writable, nullptr);
        
        std::vector<uint8_t> data(256, static_cast<uint8_t>(i));
        std::memcpy(result.writable, data.data(), data.size());
        store_->publish_node(result.id, data.data(), data.size());
        
        nodes.push_back(result.id);
    }
    
    // Commit all publishes
    uint64_t epoch1 = runtime_->mvcc().advance_epoch();
    store_->commit(epoch1);
    
    // Should have 10 allocation deltas
    EXPECT_EQ(count_delta_records(), 10);
    
    // Retire half of them
    uint64_t retire_epoch = runtime_->mvcc().advance_epoch();
    for (int i = 0; i < 5; ++i) {
        store_->retire_node(nodes[i], retire_epoch);
    }
    
    // Commit retirements
    uint64_t epoch2 = runtime_->mvcc().advance_epoch();
    store_->commit(epoch2);
    
    // Should have 10 allocations + 5 retirements = 15 deltas
    EXPECT_EQ(count_delta_records(), 15);
}

TEST_F(DurableStoreDeltaTest, DeltasPersistedAfterCommit) {
    // Allocate several nodes
    std::vector<NodeID> nodes;
    for (int i = 0; i < 5; ++i) {
        auto result = store_->allocate_node(128, NodeKind::Leaf);
        std::memset(result.writable, i, result.capacity);
        store_->publish_node(result.id, result.writable, result.capacity);
        nodes.push_back(result.id);
    }
    
    // Set root
    uint64_t root_epoch = runtime_->mvcc().get_global_epoch();
    store_->set_root(nodes[0], root_epoch, nullptr, 0, "");
    
    // Commit - this should fsync the log
    uint64_t epoch = runtime_->mvcc().advance_epoch();
    store_->commit(epoch);
    
    // Verify deltas are persisted by creating a new reader
    std::vector<OTDeltaRec> deltas;
    auto log = runtime_->coordinator().get_active_log();
    ASSERT_NE(log, nullptr);
    OTDeltaLog reader(log->path());
    reader.replay([&deltas](const OTDeltaRec& rec) {
        deltas.push_back(rec);
    });
    
    EXPECT_EQ(deltas.size(), 5); // Should have 5 allocation deltas
    
    // Verify the deltas have correct information
    for (const auto& delta : deltas) {
        EXPECT_EQ(delta.retire_epoch, ~uint64_t{0}); // Not retired
        EXPECT_GT(delta.birth_epoch, 0); // Valid epoch
        // Note: length is the allocated size (size class), not requested size
        EXPECT_GE(delta.length, 128); // At least the requested size
    }
}

TEST_F(DurableStoreDeltaTest, ConcurrentDeltaAppends) {
    // Test that concurrent appends work correctly
    const int num_threads = 4;
    const int nodes_per_thread = 25;
    std::vector<std::thread> threads;
    std::atomic<int> ready{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Wait for all threads to be ready
            ready.fetch_add(1);
            while (ready.load() < num_threads) {
                std::this_thread::yield();
            }
            
            // Each thread allocates and publishes nodes
            for (int i = 0; i < nodes_per_thread; ++i) {
                auto result = store_->allocate_node(64, NodeKind::Leaf);
                if (result.writable) {
                    std::memset(result.writable, t * 100 + i, result.capacity);
                    store_->publish_node(result.id, result.writable, result.capacity);
                }
            }
            
            // Each thread commits its batch
            uint64_t epoch = runtime_->mvcc().advance_epoch();
            store_->commit(epoch);
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should have all the deltas
    EXPECT_EQ(count_delta_records(), num_threads * nodes_per_thread);
}