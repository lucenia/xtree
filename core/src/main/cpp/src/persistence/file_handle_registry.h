/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * FileHandleRegistry: Manages file descriptors with LRU eviction
 * Part of the windowed mmap redesign to prevent FD exhaustion
 */

#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include <algorithm>

namespace xtree {
namespace persist {

struct FileHandle {
    int fd = -1;
    std::string path;
    size_t size_bytes = 0;          // Current file size (fstat/ftruncate tracked)
    uint64_t last_use_ns = 0;       // For LRU tracking
    uint32_t pins = 0;              // Reference count (how many mappings use this)
    bool writable = false;          // Whether opened for writing
    
    FileHandle() = default;
    FileHandle(int fd_, const std::string& path_, size_t size_, bool writable_ = false)
        : fd(fd_), path(path_), size_bytes(size_), writable(writable_) {
        update_last_use();
    }
    
    void update_last_use() {
        using namespace std::chrono;
        last_use_ns = duration_cast<nanoseconds>(
            high_resolution_clock::now().time_since_epoch()).count();
    }
    
    // Close the file descriptor if open
    void close();
};

class FileHandleRegistry {
public:
    explicit FileHandleRegistry(size_t max_open_files = 256);
    ~FileHandleRegistry();
    
    // Acquire a file handle, opening if needed
    // writable: whether to open for writing
    // create: whether to create if doesn't exist
    // Returns shared_ptr that keeps the FD alive while held
    std::shared_ptr<FileHandle> acquire(const std::string& path, bool writable, bool create);
    
    // Release a file handle (decrements pin count, may trigger eviction)
    void release(const std::shared_ptr<FileHandle>& fh);
    
    // Pin/unpin for explicit reference counting (used by MappingManager)
    void pin(const std::shared_ptr<FileHandle>& fh);
    void unpin(const std::shared_ptr<FileHandle>& fh);
    
    // Ensure file is at least the given size (grows if needed)
    bool ensure_size(const std::shared_ptr<FileHandle>& fh, size_t min_size);
    
    // Ensure file handle is writable (upgrade if needed)
    void ensure_writable(const std::shared_ptr<FileHandle>& fh, bool create);
    
    // Get current number of open files
    size_t open_file_count() const;
    
    // Get total number of open files (for testing)
    size_t debug_open_file_count() const;
    
    // Get number of open files for a specific path (for testing)
    size_t debug_open_file_count_for_path(const std::string& path) const;
    
    // Force eviction of all unpinned files (for testing)
    void debug_evict_all_unpinned();
    
    // Canonicalize a path (resolve symlinks, make absolute) - public for MappingManager
    std::string canonicalize_path(const std::string& path) const;
    
private:
    // Evict least recently used files if we're at capacity
    void evict_if_needed();
    
    // Find LRU candidate with pins == 0
    std::vector<std::string> find_eviction_candidates(size_t count);
    
    // Open or grow a file
    std::shared_ptr<FileHandle> open_or_grow(const std::string& path, bool writable, bool create);
    
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<FileHandle>> table_;
    size_t max_open_files_;
    size_t total_opens_ = 0;
    size_t total_evictions_ = 0;
};

} // namespace persist
} // namespace xtree