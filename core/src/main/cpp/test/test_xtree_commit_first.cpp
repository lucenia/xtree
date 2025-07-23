/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test that reproduces the exact issue from the stress test:
 * Create root, commit, then try to insert
 */

#include <gtest/gtest.h>
#include "indexdetails.hpp"
#include "persistence/durable_store.h"
#include "xtree.h"
#include "xtree.hpp"
#include "datarecord.hpp"
#include <filesystem>
#include <unistd.h>

namespace xtree {

TEST(CommitFirstTest, InsertAfterRootCommit) {
    std::cout << "\n=== Test: Insert After Root Commit ===" << std::endl;

    std::string test_dir = "/tmp/xtree_commit_first_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    std::vector<std::string> dims = {"x", "y"};
    std::vector<const char*> dim_ptrs;
    for (const auto& dim : dims) {
        dim_ptrs.push_back(dim.c_str());
    }

    IndexDetails<DataRecord> index(
        2, 32, &dim_ptrs, nullptr, nullptr,
        "commit_first_test",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );

    ASSERT_TRUE(index.hasDurableStore());
    auto* store = index.getStore();
    ASSERT_NE(store, nullptr);

    // Initialize root
    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());
    auto* cachedRoot = index.root_cache_node();
    auto* root = index.root_bucket<DataRecord>();

    std::cout << "Root created: NodeID=" << root->getNodeID().raw()
              << ", n=" << root->n() << std::endl;

    // CRITICAL: Commit the root (this is what the stress test does)
    std::cout << "Committing root..." << std::endl;
    store->commit(0);

    // CRITICAL: After external commit, invalidate cache to force reload
    std::cout << "Invalidating root cache after commit..." << std::endl;
    index.invalidate_root_cache();

    // Check if root is still in ObjectTable after commit
    persist::NodeKind kind;
    bool found = store->get_node_kind(root->getNodeID(), kind);
    std::cout << "Root in OT after commit: " << found
              << " (kind=" << static_cast<int>(kind) << ")" << std::endl;

    // Now try to insert a single record (this is where stress test fails)
    std::cout << "Attempting first insert after commit..." << std::endl;

    DataRecord* dr = new DataRecord(2, 32, "rec_0");
    std::vector<double> point = {0.0, 0.0};
    dr->putPoint(&point);
    dr->putPoint(&point);

    // Get fresh root from durable state
    std::cout << "Getting fresh root from durable state..." << std::endl;
    cachedRoot = index.root_cache_node();
    root = index.root_bucket<DataRecord>();

    // Debug: Check root state before insert
    std::cout << "Before insert: root n=" << root->n()
              << ", NodeID=" << root->getNodeID().raw() << std::endl;

    try {
        root->xt_insert(cachedRoot, dr);
        std::cout << "✓ Insert succeeded!" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "✗ Insert failed: " << e.what() << std::endl;
        FAIL() << "Insert after commit failed: " << e.what();
    }

    // Clean up
    std::filesystem::remove_all(test_dir);
}

} // namespace xtree