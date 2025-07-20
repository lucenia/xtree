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

#include "xtree_mmap_factory.h"
#include "xtree_mmap.h"
#include "indexdetails.h"
#include "xtree.h"
#include "util/log.h"
#include <mutex>

namespace xtree {

// Static member definitions for the factory
template<typename Record>
bool MMapXTreeFactory<Record>::initialized_ = false;

// Static initialization mutex to ensure thread-safe initialization
static std::mutex init_mutex;

// Define static members for IndexDetails<DataRecord>
// These definitions are only compiled into the library once
template<>
void MMapXTreeFactory<DataRecord>::initialize(size_t cache_size, JNIEnv* jvm) {
    std::lock_guard<std::mutex> lock(init_mutex);
    
    if (initialized_) {
        return;  // Already initialized
    }
    
    // TODO: Initialize IndexDetails static members
    // This requires resolving the library linking issues with static template members
    
#ifdef _DEBUG
    log() << "[MMapXTreeFactory] Factory initialized" << endl;
    log() << "[MMapXTreeFactory] Page size: " << PageCacheConstants::getSystemPageSize() << " bytes" << endl;
#endif
    
    initialized_ = true;
}

template<typename Record>
bool MMapXTreeFactory<Record>::isInitialized() {
    std::lock_guard<std::mutex> lock(init_mutex);
    return initialized_;
}

template<typename Record>
std::unique_ptr<MMapXTree<Record>> MMapXTreeFactory<Record>::createNew(
    const std::string& base_filename,
    unsigned short dimension,
    unsigned short precision,
    std::vector<const char*>* dimLabels,
    size_t initial_size) {
    
    // Ensure static members are initialized
    if (!isInitialized()) {
        initialize();
    }
    
    // Round initial size to page boundary for optimal performance
    size_t aligned_size = PageCacheConstants::alignToPage(initial_size);
    
#ifdef _DEBUG
    log() << "[MMapXTreeFactory] Creating new tree with page-aligned size: " 
          << aligned_size << " bytes (" << (aligned_size / PageCacheConstants::getSystemPageSize()) 
          << " pages)" << endl;
#endif
    
    // Create the MMapXTree with page-aligned settings
    auto tree = std::make_unique<MMapXTree<Record>>(
        base_filename, dimension, precision, dimLabels, true);
    
    // The MMapXTree constructor will handle creating page-aligned files
    
    return tree;
}

template<typename Record>
std::unique_ptr<MMapXTree<Record>> MMapXTreeFactory<Record>::openExisting(
    const std::string& base_filename,
    bool prefetch_root) {
    
    // Ensure static members are initialized
    if (!isInitialized()) {
        initialize();
    }
    
#ifdef _DEBUG
    log() << "[MMapXTreeFactory] Opening existing tree: " << base_filename << endl;
#endif
    
    // Open the existing tree
    auto tree = std::make_unique<MMapXTree<Record>>(
        base_filename, 0, 0, nullptr, false);
    
    if (prefetch_root) {
        // TODO: Implement root prefetching using madvise
#ifdef _DEBUG
        log() << "[MMapXTreeFactory] Root prefetching requested (implementation pending)" << endl;
#endif
    }
    
    return tree;
}

// Explicit template instantiations
template class MMapXTreeFactory<DataRecord>;

} // namespace xtree