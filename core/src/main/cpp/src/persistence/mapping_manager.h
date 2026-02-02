/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * MappingManager: Manages windowed memory mappings with pin/unpin
 * Part of the windowed mmap redesign to prevent VMA explosion
 */

#pragma once

#include "file_handle_registry.h"
#include <string>
#include <memory>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <span>
#include <utility>

namespace xtree {
namespace persist {

struct MappingExtent {
    uint8_t* base = nullptr;      // mmap address
    size_t   length = 0;          // bytes (typically 1GB)
    size_t   file_off = 0;        // offset in file this window starts at
    uint32_t pins = 0;            // segments using this extent
    uint64_t last_use_ns = 0;     // for LRU
    
    MappingExtent() = default;
    MappingExtent(uint8_t* base_, size_t length_, size_t file_off_)
        : base(base_), length(length_), file_off(file_off_) {
        update_last_use();
    }
    
    void update_last_use() {
        using namespace std::chrono;
        last_use_ns = duration_cast<nanoseconds>(
            high_resolution_clock::now().time_since_epoch()).count();
    }
    
    // Check if a range [off, off+len) is within this extent
    bool contains(size_t off, size_t len) const {
        if (len == 0) return off >= file_off && off <= file_off + length;
        if (off < file_off) return false;
        size_t end;
        if (__builtin_add_overflow(off, len, &end)) return false;
        return end <= file_off + length;
    }
    
    // Get pointer to offset within this extent
    uint8_t* ptr_at(size_t off) const {
        if (!contains(off, 1)) return nullptr;
        return base + (off - file_off);
    }
    
    // Unmap this extent
    void unmap();
};

struct FileMapping {
    std::string path;                      // Canonical path for this file
    std::shared_ptr<FileHandle> fh;        // Shared FD from registry
    std::vector<std::unique_ptr<MappingExtent>> extents;  // Sorted by file_off
    
    // Find extent containing [off, off+len)
    MappingExtent* find_extent(size_t off, size_t len);
    
    // Insert a new extent (maintains sort order)
    void insert_extent(std::unique_ptr<MappingExtent> ext);
};

class MappingManager {
public:
    // Global singleton accessor - lazy initialization, thread-safe
    static MappingManager& global();

    // Per-field memory statistics
    struct FieldMemoryStats {
        size_t mmap_bytes = 0;      // Total bytes mapped for this field
        size_t pin_count = 0;       // Total pins active for this field
        size_t extent_count = 0;    // Number of extents for this field
    };

    // Register a file as belonging to a specific field/index
    // Thread-safe, can be called multiple times (idempotent)
    void register_file_for_field(const std::string& path, const std::string& field_name);

    // Unregister a file when it's no longer needed
    void unregister_file(const std::string& path);

    // Get memory breakdown by field
    std::unordered_map<std::string, FieldMemoryStats> getPerFieldStats() const;

    // Pin is a RAII handle for mapped memory
    class Pin {
    public:
        Pin() = default;
        Pin(MappingManager* mgr, FileMapping* fmap, MappingExtent* ext, uint8_t* ptr, size_t size)
            : mgr_(mgr), fmap_(fmap), ext_(ext), ptr_(ptr), size_(size) {}

        // Move-only
        Pin(Pin&& other) noexcept
            : mgr_(other.mgr_), fmap_(other.fmap_), ext_(other.ext_),
              ptr_(other.ptr_), size_(other.size_) {
            other.mgr_ = nullptr;
            other.fmap_ = nullptr;
            other.ext_ = nullptr;
            other.ptr_ = nullptr;
            other.size_ = 0;
        }

        Pin& operator=(Pin&& other) noexcept {
            if (this != &other) {
                release();
                mgr_ = other.mgr_;
                fmap_ = other.fmap_;
                ext_ = other.ext_;
                ptr_ = other.ptr_;
                size_ = other.size_;
                other.mgr_ = nullptr;
                other.fmap_ = nullptr;
                other.ext_ = nullptr;
                other.ptr_ = nullptr;
                other.size_ = 0;
            }
            return *this;
        }

        ~Pin() {
            release();
        }

        // No copy
        Pin(const Pin&) = delete;
        Pin& operator=(const Pin&) = delete;

        // Access the pinned memory
        uint8_t* get() const { return ptr_; }
        uint8_t* operator->() const { return ptr_; }
        explicit operator bool() const { return ptr_ != nullptr; }
        size_t size() const { return size_; }

        // Release the pin
        void reset() {
            release();
            mgr_ = nullptr;
            fmap_ = nullptr;
            ext_ = nullptr;
            ptr_ = nullptr;
            size_ = 0;
        }

    private:
        void release();

        MappingManager* mgr_ = nullptr;
        FileMapping* fmap_ = nullptr;
        MappingExtent* ext_ = nullptr;
        uint8_t* ptr_ = nullptr;
        size_t size_ = 0;  // Size of pinned region for MADV_DONTNEED on release

        friend class MappingManager;
    };
    
    MappingManager(FileHandleRegistry& fhr,
                   size_t window_size = 1ULL << 30,     // 1 GiB default
                   size_t max_extents_global = 8192);   // cap VMA count

    ~MappingManager();

    // Memory budget configuration
    void set_memory_budget(size_t max_bytes, float eviction_headroom = 0.1f);
    size_t get_memory_budget() const { return max_memory_budget_; }
    size_t get_total_memory_mapped() const;
    float get_eviction_headroom() const { return eviction_headroom_; }
    
    // Pin ensures segment [off, off+len) is mapped, returns pointer
    Pin pin(const std::string& path, size_t off, size_t len, bool writable);
    
    // Unpin (called by Pin destructor)
    void unpin(Pin&& p);
    
    // Bulk prefetch for sequential access (optional optimization)
    void prefetch(const std::string& path,
                  const std::vector<std::pair<size_t, size_t>>& ranges);
    
    // Debug/monitoring
    size_t extent_count() const;
    size_t debug_total_extents() const;
    void debug_evict_all_unpinned();

    // Statistics for observability
    struct MappingStats {
        size_t total_extents = 0;
        size_t total_memory_mapped = 0;
        size_t max_memory_budget = 0;
        size_t total_pins_active = 0;
        size_t evictions_count = 0;
        size_t evictions_bytes = 0;
        double memory_utilization = 0.0;  // mapped / budget (0 if unlimited)
    };
    MappingStats getStats() const;

private:
    FileHandleRegistry& fhr_;
    const size_t window_size_;
    const size_t max_extents_global_;

    // Memory budget tracking
    size_t max_memory_budget_ = 0;       // 0 = unlimited
    size_t total_memory_mapped_ = 0;     // Current total bytes mapped
    float eviction_headroom_ = 0.1f;     // 10% hysteresis
    size_t evictions_bytes_ = 0;         // Total bytes evicted

    mutable std::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<FileMapping>> by_file_;
    size_t total_extents_ = 0;
    size_t total_pins_ = 0;
    size_t total_evictions_ = 0;
    
    // Ensure an extent exists for the range, return it
    MappingExtent* ensure_extent(FileMapping& fm, bool writable, size_t off, size_t len);
    
    // Evict idle extents if needed (memory-based or count-based)
    void evict_extents_if_needed();

    // Evict until memory usage is at or below target
    void evict_to_memory_target(size_t target_bytes);

    // Find eviction candidates sorted by LRU
    std::vector<std::pair<std::string, size_t>> find_eviction_candidates(size_t count);
    
    // Create a new mmap window
    std::unique_ptr<MappingExtent> create_extent(const FileHandle& fh,
                                                  size_t file_off,
                                                  size_t len,
                                                  bool writable);

    // Per-field tracking (path -> field_name)
    mutable std::mutex field_map_mutex_;
    std::unordered_map<std::string, std::string> path_to_field_;
};

} // namespace persist
} // namespace xtree