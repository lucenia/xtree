#pragma once

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

// Direct Memory COW - works with your existing packed structures
namespace xtree {

// Forward declarations
template<class Record> class IndexDetails;
template<class Record> class XTreeBucket;
class DataRecord;

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
    
    std::vector<MemoryRegion> tracked_regions_;
    mutable std::shared_mutex regions_lock_;

private:
    std::atomic<size_t> total_tracked_bytes_{0};

public:
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
        
        tracked_regions_.push_back(region);
        
        // Track total memory
        total_tracked_bytes_ += region.size;
    }
    
    // Create COW protection for all tracked regions
    void enable_cow_protection() {
        std::unique_lock<std::shared_mutex> lock(regions_lock_);
        
        for (auto& region : tracked_regions_) {
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
        
        for (auto& region : tracked_regions_) {
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
    
    // Allocate page-aligned memory
    static void* allocate_aligned(size_t size) {
        size_t aligned_size = (size + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
#ifdef _WIN32
        return _aligned_malloc(aligned_size, PAGE_SIZE);
#else
        return std::aligned_alloc(PAGE_SIZE, aligned_size);
#endif
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
        uint32_t magic = 0x58545245; // 'XTRE'
        uint32_t version = 1;
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
          last_write_time_(std::chrono::steady_clock::now()) {
        // Background timer starts only when writes occur
    }
    
    ~DirectMemoryCOWManager() {
        shutdown_ = true;
        
        // Wait for any in-progress commits to complete
        int wait_count = 0;
        while (commit_in_progress_.load() && wait_count < 100) { // Max 10 seconds
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            wait_count++;
        }
        
        // Give background thread time to exit
        if (background_timer_active_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
        
        memory_tracker_.disable_cow_protection();
    }
    
    // Call this after XTree operations
    void record_operation() {
        size_t ops = operations_since_snapshot_.fetch_add(1) + 1;  // Get the NEW value
        last_write_time_ = std::chrono::steady_clock::now();
        
        // Check if we should trigger snapshot based on operations count
        if (ops >= operations_threshold_ && !commit_in_progress_.load()) {
            trigger_memory_snapshot();
            return;
        }
        
        // Check if we should trigger based on memory size
        if (memory_tracker_.get_total_tracked_bytes() >= memory_threshold_ && !commit_in_progress_.load()) {
            trigger_memory_snapshot();
            return;
        }
        
        // Start background timer if not already running (for time-based safety trigger)
        if (!background_timer_active_.load()) {
            start_background_timer();
        }
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
    
    // Load a snapshot back into memory
    void* load_snapshot(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open snapshot file: " + filename);
        }
        
        // Read and validate header
        MemorySnapshotHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(header));
        
        if (header.magic != 0x58545245 || header.version != 1) {
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
            
            if (!file.good() || header.magic != 0x58545245 || header.version != 1) {
                return false;
            }
            
            // Validate header fields
            if (header.total_regions == 0 || header.total_size == 0) {
                return false;
            }
            
            // Validate dimension and precision match current index
            if (header.dimension != index_details_->getDimensionCount() ||
                header.precision != index_details_->getPrecision()) {
                return false;
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
    
    // COW state
    std::atomic<bool> cow_snapshot_active_{false};
    std::atomic<bool> commit_in_progress_{false};
    
    // Background persistence
    std::thread background_persister_;
    std::atomic<bool> shutdown_{false};
    std::string persist_file_;
    
    // Operation counting and thresholds
    std::atomic<size_t> operations_since_snapshot_{0};
    size_t operations_threshold_ = 10000;
    size_t memory_threshold_ = 64 * 1024 * 1024; // 64MB default
    
    // Time-based triggering (only during active writes)
    std::chrono::steady_clock::time_point last_write_time_;
    std::chrono::milliseconds max_write_interval_{30000}; // 30 seconds
    std::atomic<bool> background_timer_active_{false};
    
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
        header.dimension = index_details_->getDimensionCount();
        header.precision = index_details_->getPrecision();
        header.root_address = index_details_->getRootAddress();
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
        
        std::shared_lock<std::shared_mutex> lock(memory_tracker_.regions_lock_);
        
        // First pass: write region headers
        size_t current_offset = sizeof(MemorySnapshotHeader) + 
                               (sizeof(RegionHeader) * memory_tracker_.tracked_regions_.size());
        
        for (const auto& region : memory_tracker_.tracked_regions_) {
            RegionHeader rh{region.start_addr, region.size, current_offset};
            file.write(reinterpret_cast<const char*>(&rh), sizeof(rh));
            current_offset += region.size;
        }
        
        // Second pass: write actual memory contents
        for (const auto& region : memory_tracker_.tracked_regions_) {
            file.write(static_cast<const char*>(region.start_addr), region.size);
        }
    }
    
    void start_background_timer() {
        bool expected = false;
        if (!background_timer_active_.compare_exchange_strong(expected, true)) {
            return; // Timer already running
        }
        
        background_persister_ = std::thread([this]() {
            auto last_check = std::chrono::steady_clock::now();
            
            while (!shutdown_) {
                // Check every second
                std::this_thread::sleep_for(std::chrono::seconds(1));
                
                auto now = std::chrono::steady_clock::now();
                auto time_since_write = now - last_write_time_;
                
                // If no writes for 60 seconds, exit this thread
                if (time_since_write > std::chrono::seconds(60)) {
                    background_timer_active_ = false;
                    return; // Thread exits, will restart on next write
                }
                
                // If writes are happening and it's been too long since snapshot
                if (time_since_write < max_write_interval_ &&
                    operations_since_snapshot_.load() > 0 &&
                    !commit_in_progress_.load()) {
                    
                    auto time_since_check = now - last_check;
                    if (time_since_check >= max_write_interval_) {
                        trigger_memory_snapshot();
                        last_check = now;
                    }
                }
            }
            
            background_timer_active_ = false;
        });
        
        // Detach the thread so it can clean itself up
        background_persister_.detach();
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

