/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <cstdlib>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

#ifdef __APPLE__
#include <sys/mman.h>
#include <unistd.h>
#endif

#ifdef _WIN32
#include <windows.h>
#endif

#include "page_write_tracker.hpp"

// Direct Memory COW - works with your existing packed structures
namespace xtree {

// Forward declarations
template<class Record> class IndexDetails;
template<class Record> class XTreeBucket;
class DataRecord;

// COW Memory Manager Constants
// Magic number: 'XTRE' = 0x58 0x54 0x52 0x45 = X T R E in ASCII
constexpr uint32_t COW_SNAPSHOT_MAGIC = 0x58545245; // 'XTRE' in hex
constexpr uint32_t COW_SNAPSHOT_VERSION = 1;

/**
 * Page-aligned memory tracker for COW optimization
 */
class PageAlignedMemoryTracker {
public:
    struct MemoryRegion {
        void* start_addr;
        size_t size;
        bool is_cow_protected;
        std::chrono::steady_clock::time_point last_modified;
        bool is_huge_page;
    };
    
    // Get page size at compile time where possible
    // Users can override by defining XTREE_PAGE_SIZE
#ifdef XTREE_PAGE_SIZE
    static constexpr size_t PAGE_SIZE = XTREE_PAGE_SIZE;
#elif defined(__APPLE__) && defined(__arm64__)
    // Apple Silicon uses 16KB pages
    static constexpr size_t PAGE_SIZE = 16384;
#elif defined(_WIN32)
    // Windows x86/x64 uses 4KB pages (Large pages are opt-in)
    static constexpr size_t PAGE_SIZE = 4096;
#elif defined(__linux__) && defined(__aarch64__)
    // ARM64 Linux can use 4KB, 16KB, or 64KB
    // Default to 4KB - safest for alignment
    static constexpr size_t PAGE_SIZE = 4096;
#else
    // Default to 4KB - most common page size
    static constexpr size_t PAGE_SIZE = 4096;
#endif
    
    // Use unordered_map for O(1) lookups by address
    std::unordered_map<void*, MemoryRegion> tracked_regions_;
    mutable std::shared_mutex regions_lock_;
    
    // Batch registration support
    struct BatchRegistration {
        std::vector<std::pair<void*, size_t>> pending_regions;
        std::mutex batch_mutex;
    };
    BatchRegistration batch_registration_;

private:
    std::atomic<size_t> total_tracked_bytes_{0};
    std::unique_ptr<PageWriteTracker> write_tracker_;

public:
    PageAlignedMemoryTracker() 
        : write_tracker_(std::make_unique<PageWriteTracker>(PAGE_SIZE)) {}
    
    ~PageAlignedMemoryTracker() {
        // Disable COW protection on all regions before destruction
        disable_cow_protection();
        
        // Clear all tracked regions
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        tracked_regions_.clear();
        total_tracked_bytes_ = 0;
        
        // write_tracker_ will be automatically destroyed
    }
    
    // Register a memory region for COW tracking
    void register_memory_region(void* ptr, size_t size) {
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        // Align to page boundaries
        uintptr_t start = reinterpret_cast<uintptr_t>(ptr);
        uintptr_t aligned_start = start & ~(PAGE_SIZE - 1);  // Round down
        uintptr_t end = start + size;
        uintptr_t aligned_end = (end + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);  // Round up
        
        MemoryRegion region{};
        region.start_addr = reinterpret_cast<void*>(aligned_start);
        region.size = aligned_end - aligned_start;
        region.is_cow_protected = false;
        region.last_modified = std::chrono::steady_clock::now();
        region.is_huge_page = false;
        
        // Use the aligned address as the key for O(1) lookups
        tracked_regions_[region.start_addr] = region;
        
        // Track total memory
        total_tracked_bytes_ += region.size;
    }
    
    // Batch register multiple regions (more efficient for bulk operations)
    void batch_register_begin() {
        batch_registration_.batch_mutex.lock();
        batch_registration_.pending_regions.clear();
    }
    
    void batch_register_add(void* ptr, size_t size) {
        // No lock needed here, protected by batch_mutex
        batch_registration_.pending_regions.emplace_back(ptr, size);
    }
    
    void batch_register_commit() {
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        for (const auto& [ptr, size] : batch_registration_.pending_regions) {
            // Align to page boundaries
            uintptr_t start = reinterpret_cast<uintptr_t>(ptr);
            uintptr_t aligned_start = start & ~(PAGE_SIZE - 1);
            uintptr_t end = start + size;
            uintptr_t aligned_end = (end + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
            
            MemoryRegion region{};
            region.start_addr = reinterpret_cast<void*>(aligned_start);
            region.size = aligned_end - aligned_start;
            region.is_cow_protected = false;
            region.last_modified = std::chrono::steady_clock::now();
            
            tracked_regions_[region.start_addr] = region;
            total_tracked_bytes_ += region.size;
        }
        
        batch_registration_.pending_regions.clear();
        batch_registration_.batch_mutex.unlock();
    }
    
    // Create COW protection for all tracked regions with hot page prefaulting
    void enable_cow_protection() {
        // First, prefault hot pages to minimize COW overhead
        if (write_tracker_) {
            write_tracker_->prefault_hot_pages();
        }
        
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        for (auto& [addr, region] : tracked_regions_) {
            if (!region.is_cow_protected) {
#if defined(__linux__) || defined(__APPLE__)
                // Mark pages as read-only to trigger COW on write
                if (mprotect(region.start_addr, region.size, PROT_READ) == 0) {
                    region.is_cow_protected = true;
                }
#elif defined(_WIN32)
                DWORD old_protect;
                if (VirtualProtect(region.start_addr, region.size, PAGE_READONLY, &old_protect)) {
                    region.is_cow_protected = true;
                }
#endif
            }
        }
    }
    
    // Remove COW protection (allow writes again)
    void disable_cow_protection() {
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        for (auto& [addr, region] : tracked_regions_) {
            if (region.is_cow_protected) {
#if defined(__linux__) || defined(__APPLE__)
                mprotect(region.start_addr, region.size, PROT_READ | PROT_WRITE);
#elif defined(_WIN32)
                DWORD old_protect;
                VirtualProtect(region.start_addr, region.size, PAGE_READWRITE, &old_protect);
#endif
                region.is_cow_protected = false;
            }
        }
    }
    
    // Get total tracked memory
    size_t get_total_tracked_bytes() const {
        return total_tracked_bytes_.load();
    }
    
    // Unregister a memory region when it's freed
    void unregister_memory_region(void* ptr) {
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        // Align the pointer to find the key
        uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
        uintptr_t aligned_addr = addr & ~(PAGE_SIZE - 1);
        void* key = reinterpret_cast<void*>(aligned_addr);
        
        auto it = tracked_regions_.find(key);
        if (it != tracked_regions_.end()) {
            // Update total tracked bytes
            total_tracked_bytes_ -= it->second.size;
            
            // Remove COW protection if active
            if (it->second.is_cow_protected) {
#if defined(__linux__) || defined(__APPLE__)
                mprotect(it->second.start_addr, it->second.size, PROT_READ | PROT_WRITE);
#elif defined(_WIN32)
                DWORD old_protect;
                VirtualProtect(it->second.start_addr, it->second.size, PAGE_READWRITE, &old_protect);
#endif
            }
            
            // Remove from tracked regions
            tracked_regions_.erase(it);
        }
    }
    
    // Batch unregister multiple regions (more efficient for bulk cleanup)
    void batch_unregister_begin() {
        batch_registration_.batch_mutex.lock();
        batch_registration_.pending_regions.clear();
    }
    
    void batch_unregister_add(void* ptr) {
        // Store with size 0 to indicate unregistration
        batch_registration_.pending_regions.emplace_back(ptr, 0);
    }
    
    void batch_unregister_commit() {
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        for (const auto& [ptr, _] : batch_registration_.pending_regions) {
            // Align the pointer to find the key
            uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
            uintptr_t aligned_addr = addr & ~(PAGE_SIZE - 1);
            void* key = reinterpret_cast<void*>(aligned_addr);
            
            auto it = tracked_regions_.find(key);
            if (it != tracked_regions_.end()) {
                // Update total tracked bytes
                total_tracked_bytes_ -= it->second.size;
                
                // Remove COW protection if active
                if (it->second.is_cow_protected) {
#if defined(__linux__) || defined(__APPLE__)
                    mprotect(it->second.start_addr, it->second.size, PROT_READ | PROT_WRITE);
#elif defined(_WIN32)
                    DWORD old_protect;
                    VirtualProtect(it->second.start_addr, it->second.size, PAGE_READWRITE, &old_protect);
#endif
                }
                
                // Remove from tracked regions
                tracked_regions_.erase(it);
            }
        }
        
        batch_registration_.pending_regions.clear();
        batch_registration_.batch_mutex.unlock();
    }
    
    // Record write access for tracking hot pages
    void record_write(void* ptr) {
        if (write_tracker_) {
            write_tracker_->record_write(ptr);
        }
    }
    
    // Record read access for tracking access patterns
    void record_access(void* ptr) {
        if (write_tracker_) {
            write_tracker_->record_access(ptr);
        }
    }
    
    // Get write tracker for advanced operations
    PageWriteTracker* get_write_tracker() {
        return write_tracker_.get();
    }
    
    // Allocate page-aligned memory
    static void* allocate_aligned(size_t size) {
        size_t aligned_size = (size + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
#ifdef _WIN32
        return _aligned_malloc(aligned_size, PAGE_SIZE);
#else
        return std::aligned_alloc(PAGE_SIZE, aligned_size);
#endif
    }
    
    // Allocate huge page-aligned memory (2MB on Linux)
    static void* allocate_aligned_huge(size_t size, bool& is_huge) {
        is_huge = false;
#ifdef __linux__
        if (HugePageAllocator::is_huge_page_available()) {
            void* ptr = HugePageAllocator::allocate_huge_aligned(size);
            if (ptr) {
                is_huge = true;
                return ptr;
            }
        }
#endif
        // Fallback to regular page allocation
        return allocate_aligned(size);
    }
    
    // Deallocate aligned memory
    static void deallocate_aligned(void* ptr) {
#ifdef _WIN32
        _aligned_free(ptr);
#else
        std::free(ptr);
#endif
    }
};

/**
 * Direct Memory COW Manager - no serialization needed!
 */
template<class Record>
class DirectMemoryCOWManager {
public:
    // Memory snapshot header
    struct MemorySnapshotHeader {
        uint32_t magic = COW_SNAPSHOT_MAGIC;
        uint32_t version = COW_SNAPSHOT_VERSION;
        size_t total_regions;
        size_t total_size;
        unsigned short dimension;
        unsigned short precision;
        long root_address;
        std::chrono::system_clock::time_point snapshot_time;
    };
    
    // Statistics
    struct MemoryCOWStats {
        size_t tracked_memory_bytes;
        size_t operations_since_snapshot;
        bool cow_protection_active;
        bool commit_in_progress;
    };

    DirectMemoryCOWManager(IndexDetails<Record>* index_details,
                          const std::string& persist_file = "xtree_memory.snapshot")
        : index_details_(index_details), persist_file_(persist_file),
          batch_coordinator_(std::make_unique<BatchUpdateCoordinator<Record>>(
              PageAlignedMemoryTracker::PAGE_SIZE)) {
        // Start persistent background thread for periodic snapshots
        start_background_thread();
    }
    
    ~DirectMemoryCOWManager() {
        shutdown_ = true;
        
        // Execute any pending batch updates before shutdown
        if (batch_coordinator_ && batch_coordinator_->pending_update_count() > 0) {
            batch_coordinator_->execute_updates();
        }
        
        // Notify background thread to exit
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            snapshot_cv_.notify_all();
        }
        
        // Wait for background thread to exit
        if (background_thread_.joinable()) {
            background_thread_.join();
        }
        
        // Wait for any in-progress commits to complete
        int wait_count = 0;
        while (commit_in_progress_.load() && wait_count < 100) { // Max 10 seconds
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            wait_count++;
        }
        
        // Disable COW protection before destroying tracker
        memory_tracker_.disable_cow_protection();
        
        // Note: memory_tracker_ and batch_coordinator_ will be automatically destroyed
        // write_tracker_ inside memory_tracker_ will also be automatically destroyed
    }
    
    // Call this after XTree operations - optimized for hot path
    void record_operation() {
        // Fast path - just increment counter
        size_t ops = operations_since_snapshot_.fetch_add(1, std::memory_order_relaxed) + 1;
        
        // Update epoch counter periodically (every 64 ops) instead of clock
        if ((ops & 63) == 0) {
            last_write_epoch_.fetch_add(1, std::memory_order_relaxed);
        }
        
        // Check if we should trigger snapshot based on operations count
        if (ops >= operations_threshold_) {
            // Try to claim the snapshot atomically
            if (!snapshot_requested_.exchange(true, std::memory_order_acq_rel)) {
                // We won the race, background thread will handle it
                snapshot_cv_.notify_one();
            }
        }
    }
    
    // Record operation with write tracking
    void record_operation_with_write(void* modified_ptr) {
        memory_tracker_.record_write(modified_ptr);
        record_operation();
    }
    
    // Batch update support
    void add_batch_update(Record* target, std::function<void()> update) {
        if (batch_coordinator_) {
            batch_coordinator_->add_update(target, std::move(update));
        }
    }
    
    size_t execute_batch_updates() {
        if (batch_coordinator_) {
            return batch_coordinator_->execute_updates();
        }
        return 0;
    }
    
    size_t pending_batch_update_count() const {
        if (batch_coordinator_) {
            return batch_coordinator_->pending_update_count();
        }
        return 0;
    }
    
    // Ultra-fast memory snapshot using COW
    void trigger_memory_snapshot() {
        if (commit_in_progress_.exchange(true)) {
            // Snapshot already in progress, skip this request
            return;
        }
        
        // Reset counter immediately to prevent triggering multiple snapshots
        operations_since_snapshot_ = 0;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Enable COW protection on all tracked memory
        memory_tracker_.enable_cow_protection();
        cow_snapshot_active_ = true;
        
        auto cow_time = std::chrono::high_resolution_clock::now() - start;
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(cow_time);
        
#ifdef _DEBUG
        std::cout << "Memory COW snapshot enabled in " << microseconds.count() << " microseconds\n";
#endif
        
        // Background persistence
        std::thread([this]() {
            persist_memory_snapshot();
            
            // Re-enable writes
            memory_tracker_.disable_cow_protection();
            cow_snapshot_active_ = false;
            commit_in_progress_ = false;
        }).detach();
    }
    
    MemoryCOWStats get_stats() const {
        MemoryCOWStats stats{};
        stats.tracked_memory_bytes = memory_tracker_.get_total_tracked_bytes();
        stats.operations_since_snapshot = operations_since_snapshot_.load();
        stats.cow_protection_active = cow_snapshot_active_.load();
        stats.commit_in_progress = commit_in_progress_.load();
        return stats;
    }

    // Register XTree bucket memory for tracking
    void register_bucket_memory(void* bucket_ptr, size_t bucket_size) {
        memory_tracker_.register_memory_region(bucket_ptr, bucket_size);
    }
    
    // Allocate and register memory with optional huge page support
    void* allocate_and_register(size_t size, bool prefer_huge_page = false) {
        void* ptr = nullptr;
        bool is_huge = false;
        
        if (prefer_huge_page) {
            ptr = PageAlignedMemoryTracker::allocate_aligned_huge(size, is_huge);
        } else {
            ptr = PageAlignedMemoryTracker::allocate_aligned(size);
        }
        
        if (ptr) {
            register_bucket_memory(ptr, size);
            if (is_huge) {
                // Mark the region as huge page in tracker
                std::unique_lock<std::shared_mutex> lock(memory_tracker_.regions_lock_);
                auto it = memory_tracker_.tracked_regions_.find(ptr);
                if (it != memory_tracker_.tracked_regions_.end()) {
                    it->second.is_huge_page = true;
                }
            }
        }
        
        return ptr;
    }
    
    // Batch registration methods for bulk operations
    void begin_batch_registration() {
        memory_tracker_.batch_register_begin();
    }
    
    void add_to_batch(void* ptr, size_t size) {
        memory_tracker_.batch_register_add(ptr, size);
    }
    
    void commit_batch_registration() {
        memory_tracker_.batch_register_commit();
    }
    
    // Batch unregistration methods for bulk cleanup
    void begin_batch_unregistration() {
        memory_tracker_.batch_unregister_begin();
    }
    
    void add_to_unregister_batch(void* ptr) {
        memory_tracker_.batch_unregister_add(ptr);
    }
    
    void commit_batch_unregistration() {
        memory_tracker_.batch_unregister_commit();
    }
    
    // Load a snapshot back into memory
    void* load_snapshot(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open snapshot file: " + filename);
        }
        
        // Read and validate header
        MemorySnapshotHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(header));
        
        if (header.magic != COW_SNAPSHOT_MAGIC || header.version != COW_SNAPSHOT_VERSION) {
            throw std::runtime_error("Invalid snapshot file format");
        }
        
        // TODO: Implement full snapshot loading with pointer fixup
        // This is complex and requires knowledge of XTree internal structure
        
        return nullptr;
    }
    
    // Validate a snapshot file for integrity
    bool validate_snapshot(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            return false;
        }
        
        try {
            // Read and validate header
            MemorySnapshotHeader header;
            file.read(reinterpret_cast<char*>(&header), sizeof(header));
            
            if (!file.good() || header.magic != COW_SNAPSHOT_MAGIC || header.version != COW_SNAPSHOT_VERSION) {
                return false;
            }
            
            // Validate header fields
            if (header.total_regions == 0 || header.total_size == 0) {
                return false;
            }
            
            // Validate dimension and precision match current index (if we have index_details)
            if (index_details_) {
                if (header.dimension != index_details_->getDimensionCount() ||
                    header.precision != index_details_->getPrecision()) {
                    return false;
                }
            }
            
            // Check for timestamp drift (snapshot too old)
            auto age = std::chrono::system_clock::now() - header.snapshot_time;
            auto age_hours = std::chrono::duration_cast<std::chrono::hours>(age).count();
            if (age_hours > 24) {
#ifdef _DEBUG
                std::cerr << "Warning: Snapshot is " << age_hours << " hours old\n";
#endif
                // Note: We still allow old snapshots, just warn about them
                // Uncomment below to reject snapshots older than 24 hours:
                // return false;
            }
            
            // Validate root_address is reasonable (should be at least one page)
            if (header.root_address != 0 && header.root_address < PageAlignedMemoryTracker::PAGE_SIZE) {
                return false; // Suspicious low address - likely corrupted
            }
            
            // Read region headers
            struct RegionHeader {
                void* original_addr;
                size_t size;
                size_t offset_in_file;
            };
            
            size_t expected_data_start = sizeof(MemorySnapshotHeader) + 
                                       (sizeof(RegionHeader) * header.total_regions);
            size_t total_data_size = 0;
            
            for (size_t i = 0; i < header.total_regions; i++) {
                RegionHeader rh;
                file.read(reinterpret_cast<char*>(&rh), sizeof(rh));
                
                if (!file.good() || rh.size == 0) {
                    return false;
                }
                
                // Validate offset is reasonable
                if (rh.offset_in_file < expected_data_start) {
                    return false;
                }
                
                total_data_size += rh.size;
            }
            
            // Verify total size matches
            if (total_data_size != header.total_size) {
                return false;
            }
            
            // Check file size matches expected size
            file.seekg(0, std::ios::end);
            size_t file_size = file.tellg();
            size_t expected_size = expected_data_start + total_data_size;
            
            if (file_size != expected_size) {
                return false;
            }
            
            return true;
            
        } catch (...) {
            return false;
        }
    }
    
    // Public access to memory tracker
    PageAlignedMemoryTracker& get_memory_tracker() {
        return memory_tracker_;
    }
    
    // Configuration methods (useful for testing)
    void set_operations_threshold(size_t threshold) {
        operations_threshold_ = threshold;
    }
    
    void set_memory_threshold(size_t bytes) {
        memory_threshold_ = bytes;
    }
    
    void set_max_write_interval(std::chrono::milliseconds interval) {
        max_write_interval_ = interval;
    }
    
    // Get snapshot metadata for testing
    MemorySnapshotHeader get_snapshot_header(const std::string& filename) {
        MemorySnapshotHeader header;
        std::ifstream file(filename, std::ios::binary);
        if (file) {
            file.read(reinterpret_cast<char*>(&header), sizeof(header));
        }
        return header;
    }

private:
    IndexDetails<Record>* index_details_;
    PageAlignedMemoryTracker memory_tracker_;
    std::unique_ptr<BatchUpdateCoordinator<Record>> batch_coordinator_;
    
    // COW state
    std::atomic<bool> cow_snapshot_active_{false};
    std::atomic<bool> commit_in_progress_{false};
    
    // Background persistence
    std::atomic<bool> shutdown_{false};
    std::string persist_file_;
    
    // Operation counting and thresholds
    std::atomic<size_t> operations_since_snapshot_{0};
    size_t operations_threshold_ = 10000;
    size_t memory_threshold_ = 64 * 1024 * 1024; // 64MB default
    
    // Optimized time tracking using epochs instead of clock calls
    std::atomic<uint64_t> last_write_epoch_{0};
    std::chrono::milliseconds max_write_interval_{30000}; // 30 seconds
    
    // Background thread for snapshots
    std::thread background_thread_;
    std::mutex snapshot_mutex_;
    std::condition_variable snapshot_cv_;
    std::atomic<bool> snapshot_requested_{false};
    
    void persist_memory_snapshot() {
        std::string temp_file = persist_file_ + ".tmp";
        
        std::ofstream file(temp_file, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Failed to create memory snapshot file");
        }
        
        // Write memory snapshot header
        MemorySnapshotHeader header;
        
        {
            std::shared_lock<std::shared_mutex> lock(memory_tracker_.regions_lock_);
            header.total_regions = memory_tracker_.tracked_regions_.size();
            header.total_size = memory_tracker_.get_total_tracked_bytes();
        }
        // Only set if index_details is available
        if (index_details_) {
            header.dimension = index_details_->getDimensionCount();
            header.precision = index_details_->getPrecision();
            header.root_address = index_details_->getRootAddress();
        } else {
            header.dimension = 0;
            header.precision = 0;
            header.root_address = 0;
        }
        header.snapshot_time = std::chrono::system_clock::now();
        
        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        
        // Write each memory region
        write_memory_regions_to_file(file);
        
        if (!file.good()) {
            throw std::runtime_error("Failed to write memory snapshot");
        }
        
        file.close();
        
        // Atomic rename
        if (rename(temp_file.c_str(), persist_file_.c_str()) != 0) {
            throw std::runtime_error("Failed to commit memory snapshot");
        }
        
#ifdef _DEBUG
        std::cout << "Memory snapshot persisted to " << persist_file_ << "\n";
#endif
    }
    
    void write_memory_regions_to_file(std::ofstream& file) {
        struct RegionHeader {
            void* original_addr;
            size_t size;
            size_t offset_in_file;
        };
        
        // Copy region data while holding the lock to avoid deadlock during I/O
        std::vector<std::pair<PageAlignedMemoryTracker::MemoryRegion, std::vector<char>>> region_copies;
        {
            std::shared_lock<std::shared_mutex> lock(memory_tracker_.regions_lock_);
            
            for (const auto& [addr, region] : memory_tracker_.tracked_regions_) {
                // Copy the memory contents
                std::vector<char> data(region.size);
                std::memcpy(data.data(), region.start_addr, region.size);
                region_copies.emplace_back(region, std::move(data));
            }
        }
        // Lock released here - no deadlock during I/O
        
        // First pass: write region headers
        size_t current_offset = sizeof(MemorySnapshotHeader) + 
                               (sizeof(RegionHeader) * region_copies.size());
        
        for (const auto& [region, data] : region_copies) {
            RegionHeader rh{region.start_addr, region.size, current_offset};
            file.write(reinterpret_cast<const char*>(&rh), sizeof(rh));
            current_offset += region.size;
        }
        
        // Second pass: write actual memory contents
        for (const auto& [region, data] : region_copies) {
            file.write(data.data(), data.size());
        }
    }
    
    void start_background_thread() {
        background_thread_ = std::thread([this]() {
            uint64_t last_snapshot_epoch = 0;
            
            while (!shutdown_) {
                std::unique_lock<std::mutex> lock(snapshot_mutex_);
                
                // Wait for snapshot request or timeout
                auto timeout = snapshot_cv_.wait_for(lock, std::chrono::seconds(5),
                    [this] { return shutdown_.load() || snapshot_requested_.load(); });
                
                if (shutdown_) break;
                
                // Check if we need a snapshot
                bool need_snapshot = false;
                
                if (snapshot_requested_.load()) {
                    need_snapshot = true;
                    snapshot_requested_ = false;
                } else {
                    // Check time-based trigger using epochs
                    uint64_t current_epoch = last_write_epoch_.load(std::memory_order_relaxed);
                    if (current_epoch > last_snapshot_epoch && 
                        (current_epoch - last_snapshot_epoch) > 300 && // ~300 * 64 ops = ~19k ops
                        operations_since_snapshot_.load() > 0) {
                        need_snapshot = true;
                    }
                    
                    // Check memory threshold
                    if (memory_tracker_.get_total_tracked_bytes() >= memory_threshold_ && 
                        operations_since_snapshot_.load() > 0) {
                        need_snapshot = true;
                    }
                }
                
                if (need_snapshot && !commit_in_progress_.load()) {
                    lock.unlock();
                    trigger_memory_snapshot();
                    last_snapshot_epoch = last_write_epoch_.load(std::memory_order_relaxed);
                }
            }
        });
    }
};

/**
 * Smart Memory Allocator with COW awareness
 */
template<typename T>
class COWAllocator {
public:
    typedef T value_type;
    
    COWAllocator(DirectMemoryCOWManager<DataRecord>* cow_manager = nullptr) 
        : cow_manager_(cow_manager) {}
    
    template<typename U>
    COWAllocator(const COWAllocator<U>& other) : cow_manager_(other.cow_manager_) {}
    
    T* allocate(size_t n) {
        // Allocate page-aligned memory for COW efficiency
        size_t size = n * sizeof(T);
        size_t aligned_size = (size + 4095) & ~4095;  // Round up to page boundary
        
        void* ptr = PageAlignedMemoryTracker::allocate_aligned(aligned_size);
        if (!ptr) {
            throw std::bad_alloc();
        }
        
        // Zero initialize
        std::memset(ptr, 0, aligned_size);
        
        // Register with COW manager's memory tracker
        if (cow_manager_) {
            cow_manager_->get_memory_tracker().register_memory_region(ptr, aligned_size);
        }
        
        return static_cast<T*>(ptr);
    }
    
    void deallocate(T* ptr, size_t n) {
        // Unregister from COW tracking before freeing
        if (cow_manager_) {
            cow_manager_->get_memory_tracker().unregister_memory_region(ptr);
        }
        PageAlignedMemoryTracker::deallocate_aligned(ptr);
    }
    
    template<typename U>
    bool operator==(const COWAllocator<U>& other) const {
        return cow_manager_ == other.cow_manager_;
    }
    
    template<typename U>
    bool operator!=(const COWAllocator<U>& other) const {
        return !(*this == other);
    }
    
private:
    DirectMemoryCOWManager<DataRecord>* cow_manager_;
    
    template<typename U>
    friend class COWAllocator;
};

// Helper function to integrate COW with existing XTree
template<class Record>
void setup_cow_for_xtree(IndexDetails<Record>* index_details, 
                        DirectMemoryCOWManager<Record>* cow_manager) {
    // This would be called when creating new XTreeBuckets
    // to register their memory with the COW manager
}

} // namespace xtree

