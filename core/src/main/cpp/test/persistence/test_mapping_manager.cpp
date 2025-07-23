/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test suite for MappingManager - windowed mmap with pin/unpin
 */

#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include <random>
#include <chrono>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>
#include "persistence/file_handle_registry.h"
#include "persistence/mapping_manager.h"

using namespace xtree::persist;

class MappingManagerTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::unique_ptr<FileHandleRegistry> fhr;
    std::unique_ptr<MappingManager> mm;
    
    void SetUp() override {
        test_dir = "/tmp/mapping_manager_test_" + std::to_string(getpid());
        mkdir(test_dir.c_str(), 0755);
        
        // Create registries with reasonable limits
        fhr = std::make_unique<FileHandleRegistry>(16);  // Small limit to test eviction
        mm = std::make_unique<MappingManager>(*fhr, 
                                             1024 * 1024,  // 1MB windows (small for testing)
                                             32);          // Max 32 extents
    }
    
    void TearDown() override {
        mm.reset();
        fhr.reset();
        
        // Clean up test directory
        std::string cmd = "rm -rf " + test_dir;
        system(cmd.c_str());
    }
    
    std::string get_test_file(int id) {
        return test_dir + "/test_" + std::to_string(id) + ".dat";
    }
};

TEST_F(MappingManagerTest, BasicPinUnpin) {
    std::string file = get_test_file(1);
    
    // Pin a region
    auto pin = mm->pin(file, 0, 4096, true);
    ASSERT_TRUE(pin);
    ASSERT_NE(pin.get(), nullptr);
    
    // Write to the pinned memory
    std::memset(pin.get(), 0x42, 4096);
    
    // Pin should be automatically released when it goes out of scope
}

TEST_F(MappingManagerTest, MultiplePinsInSameWindow) {
    std::string file = get_test_file(2);
    
    // Pin multiple regions within the same window
    auto pin1 = mm->pin(file, 0, 4096, true);
    auto pin2 = mm->pin(file, 8192, 4096, true);
    auto pin3 = mm->pin(file, 16384, 4096, true);
    
    ASSERT_TRUE(pin1);
    ASSERT_TRUE(pin2);
    ASSERT_TRUE(pin3);
    
    // All should be valid pointers
    ASSERT_NE(pin1.get(), nullptr);
    ASSERT_NE(pin2.get(), nullptr);
    ASSERT_NE(pin3.get(), nullptr);
    
    // Write different patterns
    std::memset(pin1.get(), 0x11, 4096);
    std::memset(pin2.get(), 0x22, 4096);
    std::memset(pin3.get(), 0x33, 4096);
    
    // Should still have only one extent since they're in the same window
    EXPECT_EQ(mm->extent_count(), 1);
}

TEST_F(MappingManagerTest, WindowEviction) {
    // Create many pins to trigger eviction
    std::vector<MappingManager::Pin> pins;
    
    // Each file will create a new window
    for (int i = 0; i < 40; ++i) {
        std::string file = get_test_file(i);
        auto pin = mm->pin(file, 0, 4096, true);
        ASSERT_TRUE(pin);
        
        // Write a pattern
        std::memset(pin.get(), i & 0xFF, 4096);
        
        // Keep only the last 10 pins alive
        if (i >= 10) {
            pins.erase(pins.begin());
        }
        pins.push_back(std::move(pin));
    }
    
    // Should have evicted some extents
    EXPECT_LE(mm->extent_count(), 32);  // Our max_extents limit
}

TEST_F(MappingManagerTest, MemoryOrderingGuarantees) {
    // Test that memory writes through pins are visible across threads
    std::string file = get_test_file(3);
    std::atomic<bool> ready{false};
    std::atomic<bool> success{true};
    
    // Writer thread
    std::thread writer([&]() {
        auto pin = mm->pin(file, 0, 4096, true);
        ASSERT_TRUE(pin);
        
        // Write a pattern
        uint32_t* data = reinterpret_cast<uint32_t*>(pin.get());
        for (int i = 0; i < 1024; ++i) {
            data[i] = i * i;
        }
        
        // Memory barrier to ensure writes are visible
        std::atomic_thread_fence(std::memory_order_release);
        ready.store(true, std::memory_order_release);
        
        // Keep pin alive until reader is done
        while (ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });
    
    // Reader thread
    std::thread reader([&]() {
        // Wait for writer
        while (!ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        
        // Pin the same region for reading
        auto pin = mm->pin(file, 0, 4096, false);
        ASSERT_TRUE(pin);
        
        // Verify the pattern
        uint32_t* data = reinterpret_cast<uint32_t*>(pin.get());
        for (int i = 0; i < 1024; ++i) {
            if (data[i] != static_cast<uint32_t>(i * i)) {
                success.store(false);
                break;
            }
        }
        
        // Signal done
        ready.store(false, std::memory_order_release);
    });
    
    writer.join();
    reader.join();
    
    EXPECT_TRUE(success.load());
}

TEST_F(MappingManagerTest, ConcurrentPinUnpin) {
    // Test concurrent access to the same file
    std::string file = get_test_file(4);
    std::atomic<int> counter{0};
    std::atomic<bool> go{false};
    
    auto worker = [&](int id) {
        // Wait for signal
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        
        // Rapidly pin/unpin
        for (int i = 0; i < 100; ++i) {
            size_t offset = (id * 4096) % (1024 * 1024);
            auto pin = mm->pin(file, offset, 4096, true);
            ASSERT_TRUE(pin);
            
            // Do some work
            std::memset(pin.get(), id & 0xFF, 4096);
            counter.fetch_add(1);
            
            // Pin automatically released
        }
    };
    
    // Start threads
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back(worker, i);
    }
    
    // Start them all at once
    go.store(true);
    
    // Wait for completion
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(counter.load(), 800);
}

TEST_F(MappingManagerTest, FileGrowth) {
    // Test that files grow correctly when pinning beyond current size
    std::string file = get_test_file(5);
    
    // Pin at offset 0
    auto pin1 = mm->pin(file, 0, 4096, true);
    ASSERT_TRUE(pin1);
    
    // Pin way beyond current size (should trigger growth)
    auto pin2 = mm->pin(file, 10 * 1024 * 1024, 4096, true);
    ASSERT_TRUE(pin2);
    
    // Write to both regions
    std::memset(pin1.get(), 0xAA, 4096);
    std::memset(pin2.get(), 0xBB, 4096);
    
    // Verify file size
    struct stat st;
    ASSERT_EQ(stat(file.c_str(), &st), 0);
    EXPECT_GE(st.st_size, 10 * 1024 * 1024 + 4096);
}

TEST_F(MappingManagerTest, WindowReuse) {
    // Test that windows are reused when pinning nearby regions
    std::string file = get_test_file(6);
    
    // Pin a region
    auto pin1 = mm->pin(file, 0, 4096, true);
    ASSERT_TRUE(pin1);
    size_t initial_extents = mm->extent_count();
    
    // Pin nearby region (should reuse window)
    auto pin2 = mm->pin(file, 8192, 4096, true);
    ASSERT_TRUE(pin2);
    EXPECT_EQ(mm->extent_count(), initial_extents);
    
    // Pin far region (should create new window)
    auto pin3 = mm->pin(file, 2 * 1024 * 1024, 4096, true);
    ASSERT_TRUE(pin3);
    EXPECT_GT(mm->extent_count(), initial_extents);
}

TEST_F(MappingManagerTest, ZeroLengthPin) {
    // Test that zero-length pins are handled gracefully
    std::string file = get_test_file(7);
    
    auto pin = mm->pin(file, 0, 0, true);
    EXPECT_FALSE(pin);  // Should return null pin
}

TEST_F(MappingManagerTest, FDEviction) {
    // Test that file descriptors are properly evicted
    
    // Create more files than our FD limit (16)
    std::vector<std::string> files;
    for (int i = 0; i < 20; ++i) {
        files.push_back(get_test_file(100 + i));
    }
    
    // Pin each file briefly
    for (const auto& file : files) {
        auto pin = mm->pin(file, 0, 4096, true);
        ASSERT_TRUE(pin);
        std::memset(pin.get(), 0x55, 4096);
        // Pin released here
    }
    
    // Force eviction of unpinned extents
    mm->debug_evict_all_unpinned();
    
    // FHR should have evicted some files
    EXPECT_LE(fhr->open_file_count(), 16);
}

TEST_F(MappingManagerTest, PathCanonicalization) {
    // Test that relative and absolute paths to the same file work
    std::string abs_file = get_test_file(8);
    std::string rel_file = "./" + test_dir + "/test_8.dat";
    
    // Pin via absolute path
    auto pin1 = mm->pin(abs_file, 0, 4096, true);
    ASSERT_TRUE(pin1);
    std::memset(pin1.get(), 0x77, 4096);
    pin1.reset();
    
    // Pin via relative path - should see the same data
    auto pin2 = mm->pin(rel_file, 0, 4096, false);
    ASSERT_TRUE(pin2);
    uint8_t* data = reinterpret_cast<uint8_t*>(pin2.get());
    EXPECT_EQ(data[0], 0x77);
    EXPECT_EQ(data[4095], 0x77);
}

TEST_F(MappingManagerTest, Prefetch) {
    // Test prefetch functionality
    std::string file = get_test_file(9);
    
    // Prefetch multiple ranges
    std::vector<std::pair<size_t, size_t>> ranges = {
        {0, 4096},
        {8192, 4096},
        {16384, 4096}
    };
    
    mm->prefetch(file, ranges);
    
    // Now pin them - should be faster since prefetched
    for (const auto& [offset, length] : ranges) {
        auto pin = mm->pin(file, offset, length, true);
        ASSERT_TRUE(pin);
        std::memset(pin.get(), 0x99, length);
    }
}

// Stress test
TEST_F(MappingManagerTest, StressTest) {
    std::atomic<bool> stop{false};
    std::atomic<int> operations{0};
    
    auto stress_worker = [&](int id) {
        std::mt19937 rng(id);
        std::uniform_int_distribution<int> file_dist(0, 9);
        std::uniform_int_distribution<size_t> offset_dist(0, 10 * 1024 * 1024);
        
        while (!stop.load(std::memory_order_acquire)) {
            std::string file = get_test_file(file_dist(rng));
            size_t offset = offset_dist(rng) & ~4095;  // Align to page
            
            auto pin = mm->pin(file, offset, 4096, true);
            if (pin) {
                // Write thread ID
                *reinterpret_cast<int*>(pin.get()) = id;
                operations.fetch_add(1);
            }
        }
    };
    
    // Run stress test for 2 seconds
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back(stress_worker, i);
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true);
    
    for (auto& t : workers) {
        t.join();
    }
    
    std::cout << "Stress test completed " << operations.load() << " operations\n";
    EXPECT_GT(operations.load(), 0);
}