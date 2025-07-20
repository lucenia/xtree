/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#pragma once

#include <string>
#include <cstddef>
#include <memory>

namespace xtree {

/**
 * Memory-mapped file manager for XTree storage
 * 
 * Provides a virtual address space for XTree buckets that's backed by disk.
 * The OS kernel handles paging between memory and disk automatically.
 */
class MMapFile {
public:
    /**
     * Open or create a memory-mapped file
     * @param filename Path to the file
     * @param initial_size Initial size if creating new file
     * @param read_only Whether to open in read-only mode
     */
    MMapFile(const std::string& filename, size_t initial_size = 0, bool read_only = false);
    
    /**
     * Destructor - unmaps memory and closes file
     */
    virtual ~MMapFile();
    
    // Delete copy constructor and assignment operator
    MMapFile(const MMapFile&) = delete;
    MMapFile& operator=(const MMapFile&) = delete;
    
    /**
     * Get pointer to mapped memory at given offset
     * @param offset Byte offset from start of file
     * @return Pointer to memory at that offset
     */
    virtual void* getPointer(size_t offset) const;
    
    /**
     * Allocate space in the file for a new object
     * @param size Size in bytes to allocate
     * @return Offset of allocated space, or 0 if allocation failed
     */
    virtual size_t allocate(size_t size);
    
    /**
     * Get current file size
     */
    virtual size_t size() const { return file_size_; }
    
    /**
     * Get mapped memory size
     */
    size_t mapped_size() const { return mapped_size_; }
    
    /**
     * Expand the file and remapping if needed
     * @param new_size New minimum size
     * @return true if successful
     */
    bool expand(size_t new_size);
    
    /**
     * Force sync to disk
     */
    void sync();
    
    /**
     * Lock a memory region to prevent swapping
     * @param offset Offset of region to lock
     * @param size Size of region to lock
     * @return true if successful
     */
    virtual bool mlock_region(size_t offset, size_t size);
    
    /**
     * Unlock a memory region
     * @param offset Offset of region to unlock
     * @param size Size of region to unlock
     * @return true if successful
     */
    virtual bool munlock_region(size_t offset, size_t size);
    
    /**
     * Get allocation offset for root node
     */
    size_t get_root_offset() const;
    
    /**
     * Set allocation offset for root node
     */
    void set_root_offset(size_t offset);
    
    // Public constants for header
    static constexpr uint32_t FILE_MAGIC = 0x58545245; // "XTRE"
    static constexpr uint32_t FILE_VERSION = 1;
    static constexpr size_t HEADER_SIZE = 64; // Fixed header size
    
private:
    std::string filename_;
    int fd_;
    void* mapped_memory_;
    size_t file_size_;
    size_t mapped_size_;
    size_t next_allocation_offset_;
    bool read_only_;
    
    // File header structure
    struct FileHeader {
        uint32_t magic;           // Magic number for file format validation
        uint32_t version;         // File format version
        size_t root_offset;       // Offset of root bucket
        size_t next_free_offset;  // Next allocation offset
        char reserved[48];        // Reserved for future use
    };
    
    bool create_new_file(size_t initial_size);
    bool open_existing_file();
    bool map_memory();
    void unmap_memory();
    FileHeader* get_header() const;
};

/**
 * Simple pointer wrapper for memory-mapped objects
 * Access tracking will be handled separately to avoid circular dependencies
 */
template<typename T>
class MMapPtr {
public:
    MMapPtr(T* ptr, size_t offset)
        : ptr_(ptr), offset_(offset) {}
    
    T* operator->() const { return ptr_; }
    T& operator*() const { return *ptr_; }
    T* get() const { return ptr_; }
    size_t offset() const { return offset_; }
    
    bool is_valid() const { return ptr_ != nullptr; }
    
private:
    T* ptr_;
    size_t offset_;
};

} // namespace xtree