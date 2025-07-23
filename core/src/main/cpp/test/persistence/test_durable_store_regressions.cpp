#include <gtest/gtest.h>
#include "../../src/persistence/durable_store.h"
#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/checkpoint_coordinator.h"
#include "../../src/persistence/segment_allocator.h"
#include <filesystem>
#include <fstream>

namespace xtree::persist {

class DurableStoreRegressionTest : public ::testing::Test {
protected:
    Paths paths_;
    CheckpointPolicy policy_;
    
    void SetUp() override {
        // Create test directory
        auto base_dir = std::filesystem::temp_directory_path() / "xtree_regression_test";
        std::filesystem::remove_all(base_dir);
        std::filesystem::create_directories(base_dir);
        
        paths_.data_dir = base_dir.string();
        paths_.superblock = (base_dir / "meta.super").string();
        paths_.manifest = (base_dir / "manifest.json").string();
        paths_.active_log = (base_dir / "ot.wal").string();
        
        // Use default checkpoint policy
        policy_ = CheckpointPolicy{};
    }
    
    void TearDown() override {
        // Clean up test directory
        std::filesystem::remove_all(paths_.data_dir);
    }
};

// Test for the reopen hang regression
// This test validates that runtimes can be properly closed and reopened
// after commits without hanging
TEST_F(DurableStoreRegressionTest, OpenCloseOpen) {
    // First runtime - create and commit a node
    {
        auto rt1 = DurableRuntime::open(paths_, policy_);
        ASSERT_NE(rt1, nullptr);
        
        DurableContext ctx{
            .ot = rt1->ot(),
            .alloc = rt1->allocator(),
            .coord = rt1->coordinator(),
            .mvcc = rt1->mvcc(),
            .runtime = *rt1
        };
        
        DurableStore s(ctx, "primary");
        auto a = s.allocate_node(4096, NodeKind::Leaf);
        std::array<uint8_t, 4096> buf{};
        memcpy(buf.data(), "Test data", 10);
        s.publish_node(a.id, buf.data(), buf.size());
        s.set_root(a.id, 1, nullptr, 0, "");
        s.commit(1);
        
        // Runtime will be destroyed here
    }
    
    // Second runtime - should not hang
    {
        auto rt2 = DurableRuntime::open(paths_, policy_);
        ASSERT_NE(rt2, nullptr);
        
        DurableContext ctx{
            .ot = rt2->ot(),
            .alloc = rt2->allocator(),
            .coord = rt2->coordinator(),
            .mvcc = rt2->mvcc(),
            .runtime = *rt2
        };
        
        DurableStore s2(ctx, "primary");
        
        // Verify we can read the root
        auto root = s2.get_root("");
        EXPECT_TRUE(root.valid());
        
        // Create another node to ensure everything works
        auto b = s2.allocate_node(1024, NodeKind::Internal);
        std::array<uint8_t, 1024> buf2{};
        memcpy(buf2.data(), "Second test", 12);
        s2.publish_node(b.id, buf2.data(), buf2.size());
        s2.commit(2);
    }
    
    // Third runtime - ensure multiple reopens work
    {
        auto rt3 = DurableRuntime::open(paths_, policy_);
        ASSERT_NE(rt3, nullptr);
        
        DurableContext ctx{
            .ot = rt3->ot(),
            .alloc = rt3->allocator(),
            .coord = rt3->coordinator(),
            .mvcc = rt3->mvcc(),
            .runtime = *rt3
        };
        
        DurableStore s3(ctx, "primary");
        auto root = s3.get_root("");
        EXPECT_TRUE(root.valid());
    }
}

// Test that we can close and reopen multiple times with different operations
TEST_F(DurableStoreRegressionTest, MultipleOpenCloseWithVariousOps) {
    NodeID saved_root;
    
    // First: Just allocate, no commit
    {
        auto rt = DurableRuntime::open(paths_, policy_);
        DurableContext ctx{
            .ot = rt->ot(),
            .alloc = rt->allocator(),
            .coord = rt->coordinator(),
            .mvcc = rt->mvcc(),
            .runtime = *rt
        };
        DurableStore s(ctx, "test");
        auto node = s.allocate_node(512, NodeKind::Leaf);
        // No commit - just close
    }
    
    // Second: Allocate and commit
    {
        auto rt = DurableRuntime::open(paths_, policy_);
        DurableContext ctx{
            .ot = rt->ot(),
            .alloc = rt->allocator(),
            .coord = rt->coordinator(),
            .mvcc = rt->mvcc(),
            .runtime = *rt
        };
        DurableStore s(ctx, "test");
        auto node = s.allocate_node(1024, NodeKind::Internal);
        std::vector<uint8_t> data(1024, 0x42);
        s.publish_node(node.id, data.data(), data.size());
        s.set_root(node.id, 1, nullptr, 0, "");
        s.commit(1);
        saved_root = s.get_root("");  // Get committed ID with correct tag
    }
    
    // Third: Read the committed data
    {
        auto rt = DurableRuntime::open(paths_, policy_);
        DurableContext ctx{
            .ot = rt->ot(),
            .alloc = rt->allocator(),
            .coord = rt->coordinator(),
            .mvcc = rt->mvcc(),
            .runtime = *rt
        };
        DurableStore s(ctx, "test");
        auto root = s.get_root("");
        EXPECT_EQ(root.raw(), saved_root.raw());
    }
}

} // namespace xtree::persist