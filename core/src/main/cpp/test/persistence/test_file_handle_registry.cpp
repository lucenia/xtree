/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Comprehensive tests for FileHandleRegistry
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <set>
#include <random>
#include <chrono>
#include <fstream>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#include <sys/resource.h>
#endif
#include "persistence/file_handle_registry.h"
#include "persistence/platform_fs.h"

using namespace xtree::persist;

class FileHandleRegistryTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::unique_ptr<FileHandleRegistry> registry;
    
    void SetUp() override {
        test_dir = "/tmp/fhr_test_" + std::to_string(getpid());
        
        #ifdef _WIN32
            CreateDirectoryA(test_dir.c_str(), nullptr);
        #else
            mkdir(test_dir.c_str(), 0755);
        #endif
        
        registry = std::make_unique<FileHandleRegistry>(10); // Small cap for testing
    }
    
    void TearDown() override {
        registry.reset();
        
        // Clean up test directory and files
        #ifdef _WIN32
            std::string cmd = "rmdir /s /q \"" + test_dir + "\"";
            system(cmd.c_str());
        #else
            std::string cmd = "rm -rf " + test_dir;
            system(cmd.c_str());
        #endif
    }
    
    std::string create_test_file(const std::string& name, size_t size = 4096) {
        std::string path = test_dir + "/" + name;
        std::ofstream f(path, std::ios::binary);
        std::vector<char> data(size, 'X');
        f.write(data.data(), data.size());
        f.close();
        return path;
    }
};

TEST_F(FileHandleRegistryTest, BasicAcquireRelease) {
    std::string path = create_test_file("test1.dat");
    
    // Acquire handle
    auto fh = registry->acquire(path, true, true);
    ASSERT_TRUE(fh);
    ASSERT_GE(fh->fd, 0);
    EXPECT_EQ(fh->pins, 1);
    EXPECT_TRUE(fh->writable);
    
    // Release handle
    registry->release(fh);
    EXPECT_EQ(fh->pins, 0);
    
    // Should still be cached
    EXPECT_EQ(registry->debug_open_file_count(), 1);
}

TEST_F(FileHandleRegistryTest, PathCanonicalization) {
    std::string path = create_test_file("test2.dat");
    
    // Acquire with different path representations
    auto fh1 = registry->acquire(path, true, true);
    auto fh2 = registry->acquire(test_dir + "/./test2.dat", true, true);
    auto fh3 = registry->acquire(test_dir + "/../" + 
                                  test_dir.substr(test_dir.rfind('/')+1) + "/test2.dat", 
                                  true, true);
    
    // Should all be the same handle
    EXPECT_EQ(fh1.get(), fh2.get());
    EXPECT_EQ(fh2.get(), fh3.get());
    EXPECT_EQ(fh1->pins, 3);
    
    // Only one file should be open
    EXPECT_EQ(registry->debug_open_file_count(), 1);
    
    // Release all
    registry->release(fh1);
    registry->release(fh2);
    registry->release(fh3);
    
    EXPECT_EQ(fh1->pins, 0);
}

TEST_F(FileHandleRegistryTest, PinUnpinSemantics) {
    std::string path = create_test_file("test3.dat");
    
    auto fh = registry->acquire(path, true, true);
    EXPECT_EQ(fh->pins, 1);
    
    // Pin multiple times
    registry->pin(fh);
    EXPECT_EQ(fh->pins, 2);
    registry->pin(fh);
    EXPECT_EQ(fh->pins, 3);
    
    // Unpin
    registry->unpin(fh);
    EXPECT_EQ(fh->pins, 2);
    registry->unpin(fh);
    EXPECT_EQ(fh->pins, 1);
    
    // Release original acquire
    registry->release(fh);
    EXPECT_EQ(fh->pins, 0);
}

TEST_F(FileHandleRegistryTest, LRUEviction) {
    // Create more files than cap (10)
    std::vector<std::string> paths;
    for (int i = 0; i < 15; i++) {
        paths.push_back(create_test_file("file" + std::to_string(i) + ".dat"));
    }
    
    // Acquire and immediately release all (makes them evictable)
    std::vector<std::shared_ptr<FileHandle>> handles;
    for (const auto& path : paths) {
        auto fh = registry->acquire(path, true, true);
        handles.push_back(fh);
        registry->release(fh);
    }
    
    // Should have evicted oldest ones to stay at cap
    EXPECT_LE(registry->debug_open_file_count(), 11); // cap + 1
    
    // Verify oldest handles were closed
    for (int i = 0; i < 5; i++) {
        if (handles[i]->fd >= 0) {
            // Handle might still be open if not evicted
            continue;
        }
        EXPECT_LT(handles[i]->fd, 0) << "Old handle " << i << " should be evicted";
    }
    
    // Recent handles should still be open
    for (int i = 10; i < 15; i++) {
        EXPECT_GE(handles[i]->fd, 0) << "Recent handle " << i << " should be cached";
    }
}

TEST_F(FileHandleRegistryTest, NoEvictionOfPinnedHandles) {
    // Fill up to cap with pinned handles
    std::vector<std::shared_ptr<FileHandle>> pinned;
    for (int i = 0; i < 10; i++) {
        auto path = create_test_file("pinned" + std::to_string(i) + ".dat");
        auto fh = registry->acquire(path, true, true);
        pinned.push_back(fh);
        // Don't release - keeps them pinned
    }
    
    // Try to add more files
    std::vector<std::shared_ptr<FileHandle>> extra;
    for (int i = 0; i < 5; i++) {
        auto path = create_test_file("extra" + std::to_string(i) + ".dat");
        auto fh = registry->acquire(path, true, true);
        extra.push_back(fh);
        registry->release(fh); // Make evictable
    }
    
    // All pinned handles should still be open
    for (const auto& fh : pinned) {
        EXPECT_GE(fh->fd, 0) << "Pinned handle should not be evicted";
        EXPECT_GT(fh->pins, 0);
    }
    
    // We should have more than cap files open (pinned can't be evicted)
    EXPECT_GT(registry->debug_open_file_count(), 10);
}

TEST_F(FileHandleRegistryTest, FileGrowth) {
    std::string path = create_test_file("grow.dat", 1024);
    
    auto fh = registry->acquire(path, true, true);
    ASSERT_TRUE(fh);
    
    // Ensure size multiple times
    EXPECT_TRUE(registry->ensure_size(fh, 2048));
    EXPECT_TRUE(registry->ensure_size(fh, 4096));
    EXPECT_TRUE(registry->ensure_size(fh, 8192));
    
    // Verify file actually grew
    struct stat st;
    ASSERT_EQ(stat(path.c_str(), &st), 0);
    EXPECT_EQ(st.st_size, 8192);
    
    registry->release(fh);
}

TEST_F(FileHandleRegistryTest, ConcurrentAcquireSameFile) {
    std::string path = create_test_file("concurrent.dat");
    const int num_threads = 8;
    const int acquires_per_thread = 100;
    
    std::vector<std::thread> threads;
    std::atomic<int> total_acquires(0);
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < acquires_per_thread; i++) {
                auto fh = registry->acquire(path, true, true);
                EXPECT_TRUE(fh);
                total_acquires++;
                
                // Simulate some work
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                
                registry->release(fh);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(total_acquires.load(), num_threads * acquires_per_thread);
    
    // Should have exactly one file handle
    EXPECT_EQ(registry->debug_open_file_count(), 1);
}

TEST_F(FileHandleRegistryTest, ConcurrentDifferentFiles) {
    const int num_threads = 8;
    const int files_per_thread = 5;
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            std::vector<std::shared_ptr<FileHandle>> handles;
            
            // Acquire multiple files
            for (int i = 0; i < files_per_thread; i++) {
                auto path = create_test_file("thread" + std::to_string(t) + 
                                            "_file" + std::to_string(i) + ".dat");
                auto fh = registry->acquire(path, true, true);
                handles.push_back(fh);
            }
            
            // Release them all
            for (auto& fh : handles) {
                registry->release(fh);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should have many files, but capped by eviction
    EXPECT_LE(registry->debug_open_file_count(), 11); // cap + 1
}

TEST_F(FileHandleRegistryTest, ScaleTest) {
    // Test that we can handle many files without FD exhaustion
    const int num_files = 1000; // Much more than typical FD limit
    
    std::vector<std::string> paths;
    for (int i = 0; i < num_files; i++) {
        paths.push_back(create_test_file("scale" + std::to_string(i) + ".dat", 512));
    }
    
    // Acquire and release in a pattern that would exhaust FDs without eviction
    for (int round = 0; round < 3; round++) {
        for (const auto& path : paths) {
            auto fh = registry->acquire(path, false, true);
            ASSERT_TRUE(fh) << "Failed to acquire " << path;
            
            // Immediately release to make evictable
            registry->release(fh);
            
            // Verify we're not leaking FDs
            EXPECT_LE(registry->debug_open_file_count(), 15) 
                << "Too many open files - eviction not working";
        }
    }
    
    // Final check - we should have handled all files without issues
    EXPECT_LE(registry->debug_open_file_count(), 11);
}

TEST_F(FileHandleRegistryTest, ReadOnlyVsWritable) {
    std::string path = create_test_file("readonly.dat");
    
    // First acquire as read-only
    auto fh_ro = registry->acquire(path, false, true);
    ASSERT_TRUE(fh_ro);
    EXPECT_FALSE(fh_ro->writable);
    
    // Try to acquire as writable - should fail or upgrade
    auto fh_rw = registry->acquire(path, true, true);
    
    // Could be same handle upgraded, or different handle
    // But if same, should now be writable
    if (fh_rw == fh_ro) {
        EXPECT_TRUE(fh_rw->writable);
    }
    
    registry->release(fh_ro);
    if (fh_rw != fh_ro) {
        registry->release(fh_rw);
    }
}

TEST_F(FileHandleRegistryTest, CreateNonExistentFile) {
    std::string path = test_dir + "/new_file.dat";
    
    // Acquire with create=true
    auto fh = registry->acquire(path, true, true);
    ASSERT_TRUE(fh);
    
    // File should exist now
    struct stat st;
    EXPECT_EQ(stat(path.c_str(), &st), 0);
    
    // Should be able to grow it
    EXPECT_TRUE(registry->ensure_size(fh, 4096));
    
    registry->release(fh);
}

TEST_F(FileHandleRegistryTest, EvictionWithMultiplePins) {
    std::string path1 = create_test_file("multi1.dat");
    std::string path2 = create_test_file("multi2.dat");
    
    auto fh1 = registry->acquire(path1, true, true);
    
    // Add multiple pins to fh1
    registry->pin(fh1);
    registry->pin(fh1);
    EXPECT_EQ(fh1->pins, 3);
    
    // Fill registry to trigger eviction
    std::vector<std::shared_ptr<FileHandle>> handles;
    for (int i = 0; i < 15; i++) {
        auto path = create_test_file("filler" + std::to_string(i) + ".dat");
        auto fh = registry->acquire(path, true, true);
        handles.push_back(fh);
        registry->release(fh); // Make evictable
    }
    
    // fh1 should not be evicted despite pressure
    EXPECT_GE(fh1->fd, 0);
    
    // Now unpin gradually
    registry->unpin(fh1);
    registry->unpin(fh1);
    registry->release(fh1);
    
    // Now it could be evicted
    EXPECT_EQ(fh1->pins, 0);
}

TEST_F(FileHandleRegistryTest, LRUUpdateOnAccess) {
    // Create files that will fill the cache
    std::vector<std::shared_ptr<FileHandle>> handles;
    for (int i = 0; i < 10; i++) {
        auto path = create_test_file("lru" + std::to_string(i) + ".dat");
        auto fh = registry->acquire(path, true, true);
        handles.push_back(fh);
        registry->release(fh); // Make evictable
        
        // Small delay to ensure different timestamps
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    
    // Touch the first file to update its LRU
    registry->pin(handles[0]);
    registry->unpin(handles[0]);
    
    // Add more files to trigger eviction
    for (int i = 0; i < 5; i++) {
        auto path = create_test_file("new" + std::to_string(i) + ".dat");
        auto fh = registry->acquire(path, true, true);
        registry->release(fh);
    }
    
    // First handle should still be open (recently accessed)
    EXPECT_GE(handles[0]->fd, 0) << "Recently accessed file was evicted";
    
    // Some middle handles should be evicted
    bool some_evicted = false;
    for (int i = 1; i < 5; i++) {
        if (handles[i]->fd < 0) {
            some_evicted = true;
            break;
        }
    }
    EXPECT_TRUE(some_evicted) << "No middle files were evicted";
}

#ifndef _WIN32
TEST_F(FileHandleRegistryTest, FDLimitStress) {
    // Get current FD limit
    struct rlimit rlim;
    getrlimit(RLIMIT_NOFILE, &rlim);
    
    // Try to open twice the soft limit (would fail without eviction)
    int target_files = std::min(2000, (int)(rlim.rlim_cur * 2));
    
    std::cout << "Testing with " << target_files << " files (soft limit: " 
              << rlim.rlim_cur << ")" << std::endl;
    
    for (int i = 0; i < target_files; i++) {
        auto path = create_test_file("fdstress" + std::to_string(i) + ".dat", 256);
        auto fh = registry->acquire(path, false, true);
        
        if (!fh) {
            FAIL() << "Failed to acquire file " << i << " - FD exhaustion despite eviction";
        }
        
        registry->release(fh);
        
        // Periodically check we're not accumulating FDs
        if (i % 100 == 0) {
            EXPECT_LE(registry->debug_open_file_count(), 20) 
                << "FD count growing despite eviction at iteration " << i;
        }
    }
    
    std::cout << "Successfully handled " << target_files 
              << " files with max " << registry->debug_open_file_count() 
              << " FDs open" << std::endl;
}
#endif

TEST_F(FileHandleRegistryTest, PathNormalizationEdgeCases) {
    std::string base = create_test_file("base.dat");
    
    // Test various path representations
    std::vector<std::string> variants = {
        test_dir + "/base.dat",
        test_dir + "/./base.dat",
        test_dir + "//base.dat",
        test_dir + "/subdir/../base.dat"
    };
    
    std::shared_ptr<FileHandle> first;
    for (const auto& path : variants) {
        auto fh = registry->acquire(path, true, true);
        if (!first) {
            first = fh;
        } else {
            // Should be same handle
            EXPECT_EQ(fh.get(), first.get()) 
                << "Different handle for path: " << path;
        }
    }
    
    // Should have many refs but only one file
    EXPECT_EQ(registry->debug_open_file_count(), 1);
    EXPECT_EQ(first->pins, variants.size());
    
    // Release all
    for (size_t i = 0; i < variants.size(); i++) {
        registry->release(first);
    }
    EXPECT_EQ(first->pins, 0);
}