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

#include <memory>
#include <string>
#include <unistd.h>
#include <jni.h>  // For JNIEnv

namespace xtree {

// Forward declarations
template<typename Record> class MMapXTree;
template<typename Record> class IndexDetails;

/**
 * Constants for page-cache friendly operations
 */
class PageCacheConstants {
public:
    static constexpr size_t PAGE_SIZE = 4096;  // Standard page size (4KB)
    static constexpr size_t BUCKET_ALIGNMENT = PAGE_SIZE;  // Align buckets to page boundaries
    static constexpr size_t MIN_MMAP_SIZE = 16 * PAGE_SIZE;  // Minimum mmap size (64KB)
    static constexpr size_t DEFAULT_MMAP_SIZE = 256 * PAGE_SIZE;  // Default mmap size (1MB)
    static constexpr size_t PREFETCH_PAGES = 4;  // Number of pages to prefetch
    
    /**
     * Get the actual system page size at runtime
     */
    static size_t getSystemPageSize() {
        static size_t pageSize = 0;
        if (pageSize == 0) {
            pageSize = sysconf(_SC_PAGESIZE);
            if (pageSize == 0) {
                pageSize = PAGE_SIZE;  // Fallback to default
            }
        }
        return pageSize;
    }
    
    /**
     * Round up size to next page boundary
     */
    static size_t alignToPage(size_t size) {
        size_t pageSize = getSystemPageSize();
        return ((size + pageSize - 1) / pageSize) * pageSize;
    }
    
    /**
     * Calculate offset aligned to page boundary
     */
    static size_t alignOffsetToPage(size_t offset) {
        size_t pageSize = getSystemPageSize();
        return (offset / pageSize) * pageSize;
    }
};

/**
 * Factory for creating MMapXTree instances with proper initialization
 * 
 * This factory ensures that IndexDetails static members are properly
 * initialized before use, solving the library linking issues.
 */
template<typename Record>
class MMapXTreeFactory {
public:
    /**
     * Initialize static members for the given Record type
     * This must be called once before creating any MMapXTree instances
     * 
     * @param cache_size Size of the LRU cache in bytes
     * @param jvm JNI environment (can be null for non-JNI usage)
     */
    static void initialize(size_t cache_size = 10 * 1024 * 1024, JNIEnv* jvm = nullptr);
    
    /**
     * Create a new MMapXTree with page-cache friendly settings
     * 
     * @param base_filename Base filename (without extension)
     * @param dimension Number of dimensions
     * @param precision Precision bits
     * @param dimLabels Dimension labels
     * @param initial_size Initial file size (will be rounded to page boundary)
     * @return Unique pointer to the new MMapXTree
     */
    static std::unique_ptr<MMapXTree<Record>> createNew(
        const std::string& base_filename,
        unsigned short dimension,
        unsigned short precision,
        std::vector<const char*>* dimLabels,
        size_t initial_size = PageCacheConstants::DEFAULT_MMAP_SIZE);
    
    /**
     * Open an existing MMapXTree
     * 
     * @param base_filename Base filename (without extension)
     * @param prefetch_root Whether to prefetch root pages on open
     * @return Unique pointer to the opened MMapXTree
     */
    static std::unique_ptr<MMapXTree<Record>> openExisting(
        const std::string& base_filename,
        bool prefetch_root = true);
    
    /**
     * Check if static members are initialized
     */
    static bool isInitialized();
    
private:
    static bool initialized_;
};

} // namespace xtree