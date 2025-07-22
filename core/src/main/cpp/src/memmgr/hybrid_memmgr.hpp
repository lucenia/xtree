#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <stdexcept>
#include <iostream>
#include <cstring>
#include <thread>
#include <chrono>

#ifdef _WIN32
    #include <windows.h>
    #include <malloc.h>  // For _aligned_malloc
#else
    #include <sys/mman.h>
    #include <fcntl.h>
    #include <unistd.h>
    #include <stdlib.h>  // For aligned_alloc
#endif

using namespace std;

// Size constants
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

// Ultra-optimized hybrid memory manager for 500K QPS
class HybridMemoryManager {
private:
    // Memory regions - cache-aligned
    alignas(64) char* hot_region_;
    alignas(64) char* cold_region_;
    
    // Fast allocation pointers - separate cache lines to avoid false sharing
    alignas(64) std::atomic<char*> hot_current_;
    alignas(64) std::atomic<char*> cold_current_;
    
    // Region boundaries
    char* hot_end_;
    char* cold_end_;
    
    // Hot region optimization: pre-fault all pages
    size_t hot_size_;
    size_t cold_size_;
    
#ifdef _WIN32
    HANDLE file_handle_;
    HANDLE mapping_handle_;
    void* mapped_memory_;
#else
    int fd_;
    void* mapped_memory_;
#endif

public:
    HybridMemoryManager(size_t hot_size = 1*GB, size_t cold_size = 1*GB) 
        : hot_size_(hot_size), cold_size_(cold_size) {
        
        // Hot region: regular allocation + pre-fault + huge pages
#ifdef _WIN32
        hot_region_ = static_cast<char*>(_aligned_malloc(hot_size, 64));
#else
        hot_region_ = static_cast<char*>(aligned_alloc(64, hot_size));
#endif
        if (!hot_region_) throw std::runtime_error("Hot region allocation failed");
        
        // Pre-fault all hot pages to avoid page faults during queries
        prefault_memory(hot_region_, hot_size);
        
        // Request huge pages for hot region (Linux)
#ifdef __linux__
        madvise(hot_region_, hot_size, MADV_HUGEPAGE);
        madvise(hot_region_, hot_size, MADV_WILLNEED);
#endif
        
        hot_current_ = hot_region_;
        hot_end_ = hot_region_ + hot_size;
        
        // Cold region: memory-mapped
        setup_cold_region();
        
        cold_current_ = static_cast<char*>(mapped_memory_);
        cold_end_ = static_cast<char*>(mapped_memory_) + cold_size;
    }
    
    ~HybridMemoryManager() {
#ifdef _WIN32
        _aligned_free(hot_region_);
#else
        free(hot_region_);
#endif
        cleanup_cold_region();
    }
    
    // ULTRA-FAST PATH: Zero overhead hot allocation
    // This should compile to just a few instructions
#ifdef _MSC_VER
    __forceinline
#else
    __attribute__((always_inline))
#endif
    inline void* allocate_hot_fast(size_t size) noexcept {
        // Assume size is already aligned to 8-byte boundary
        char* current = hot_current_.load(std::memory_order_relaxed);
        char* new_current = current + size;
        
        // Fast path: assume we have space (true 99.9% of the time)
        if (__builtin_expect(new_current <= hot_end_, 1)) {
            // Use relaxed ordering for maximum speed
            while (!hot_current_.compare_exchange_weak(
                current, new_current, 
                std::memory_order_relaxed, std::memory_order_relaxed)) {
                new_current = current + size;
                if (__builtin_expect(new_current > hot_end_, 0)) {
                    return nullptr; // Out of hot memory
                }
            }
            return current;
        }
        return nullptr; // Out of hot memory
    }
    
    // FAST PATH: Cold allocation (for background operations)
    void* allocate_cold(size_t size) noexcept {
        char* current = cold_current_.load(std::memory_order_relaxed);
        char* new_current = current + size;
        
        if (new_current <= cold_end_) {
            while (!cold_current_.compare_exchange_weak(
                current, new_current,
                std::memory_order_relaxed, std::memory_order_relaxed)) {
                new_current = current + size;
                if (new_current > cold_end_) {
                    return nullptr;
                }
            }
            return current;
        }
        return nullptr;
    }
    
    // ZERO-OVERHEAD query interface
    // No branching, no function calls, no overhead
    template<typename NodeType>
#ifdef _MSC_VER
    __forceinline
#else
    __attribute__((always_inline))
#endif
    inline NodeType* get_node(void* ptr) noexcept {
        // Just cast - no hot/cold checking during queries
        return static_cast<NodeType*>(ptr);
    }
    
    // Background thread interface for LRU management
    struct BackgroundOps {
        std::atomic<bool> should_promote{false};
        std::atomic<void*> promote_candidate{nullptr};
        std::atomic<size_t> promote_size{0};
    };
    
    BackgroundOps background_ops_;
    
    // Called by background thread only - zero query overhead
    void background_maintenance() {
        // Check if promotion needed
        if (background_ops_.should_promote.load()) {
            void* candidate = background_ops_.promote_candidate.load();
            size_t size = background_ops_.promote_size.load();
            
            if (candidate && size > 0) {
                promote_to_hot(candidate, size);
                background_ops_.should_promote = false;
            }
        }
    }
    
    // Statistics for monitoring (background thread only)
    struct UltraStats {
        size_t hot_used;
        size_t hot_total;
        size_t cold_used; 
        size_t cold_total;
        double hot_utilization;
        double cold_utilization;
    };
    
    UltraStats get_stats() const noexcept {
        UltraStats stats{};
        stats.hot_used = hot_current_.load() - hot_region_;
        stats.hot_total = hot_size_;
        stats.cold_used = cold_current_.load() - static_cast<char*>(mapped_memory_);
        stats.cold_total = cold_size_;
        stats.hot_utilization = static_cast<double>(stats.hot_used) / stats.hot_total;
        stats.cold_utilization = static_cast<double>(stats.cold_used) / stats.cold_total;
        return stats;
    }
    
    // Memory region accessors for smart allocation
    char* get_hot_base() const noexcept { return hot_region_; }
    char* get_cold_base() const noexcept { return static_cast<char*>(mapped_memory_); }
    size_t get_hot_size() const noexcept { return hot_size_; }
    size_t get_cold_size() const noexcept { return cold_size_; }
    
    // Check if pointer is in hot region
    bool is_in_hot_region(void* ptr) const noexcept {
        char* p = static_cast<char*>(ptr);
        return p >= hot_region_ && p < hot_region_ + hot_size_;
    }
    
    // Check if pointer is in cold region  
    bool is_in_cold_region(void* ptr) const noexcept {
        char* p = static_cast<char*>(ptr);
        char* cold_base = static_cast<char*>(mapped_memory_);
        return p >= cold_base && p < cold_base + cold_size_;
    }

private:
    void prefault_memory(void* ptr, size_t size) {
        // Touch every page to ensure it's mapped
        char* p = static_cast<char*>(ptr);
        const size_t page_size = 4096;
        
        for (size_t i = 0; i < size; i += page_size) {
            p[i] = 0; // Touch the page
        }
    }
    
    void setup_cold_region() {
#ifdef _WIN32
        file_handle_ = CreateFileA("cold_storage.dat",
                                  GENERIC_READ | GENERIC_WRITE,
                                  0, nullptr, CREATE_ALWAYS,
                                  FILE_ATTRIBUTE_NORMAL, nullptr);
        
        mapping_handle_ = CreateFileMapping(file_handle_, nullptr,
                                          PAGE_READWRITE, 0,
                                          static_cast<DWORD>(cold_size_),
                                          nullptr);
        
        mapped_memory_ = MapViewOfFile(mapping_handle_,
                                     FILE_MAP_ALL_ACCESS,
                                     0, 0, cold_size_);
#else
        fd_ = open("cold_storage.dat", O_RDWR | O_CREAT, 0644);
        if (ftruncate(fd_, cold_size_) == -1) {
            throw std::runtime_error("Failed to resize cold file");
        }
        
        mapped_memory_ = mmap(nullptr, cold_size_,
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd_, 0);
        
        if (mapped_memory_ == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }
        
#ifdef __linux__
        // Optimize for sequential access in cold region
        madvise(mapped_memory_, cold_size_, MADV_SEQUENTIAL);
#endif
#endif
    }
    
    void cleanup_cold_region() {
#ifdef _WIN32
        if (mapped_memory_) UnmapViewOfFile(mapped_memory_);
        if (mapping_handle_) CloseHandle(mapping_handle_);
        if (file_handle_) CloseHandle(file_handle_);
#else
        if (mapped_memory_ != MAP_FAILED) {
            munmap(mapped_memory_, cold_size_);
        }
        if (fd_ != -1) close(fd_);
#endif
    }
    
    void promote_to_hot(void* cold_ptr, size_t size) {
        // Copy data from cold to hot region
        void* hot_ptr = allocate_hot_fast(size);
        if (hot_ptr) {
            memcpy(hot_ptr, cold_ptr, size);
            // Update tree pointers in background
        }
    }
};
// Example tree implementations removed - see integration with actual XTree