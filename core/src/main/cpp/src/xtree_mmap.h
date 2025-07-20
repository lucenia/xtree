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

#include "xtree.h"
#include "mmapfile.h"
#include "lru_tracker.h"
#include <memory>

// Forward declaration to avoid circular dependency
namespace xtree {
    template<typename Record> class XTreeSerializer;
}

namespace xtree {

/**
 * Memory-mapped XTree implementation
 * 
 * Combines the existing XTree logic with memory-mapped storage and LRU tracking.
 * The tree structure is stored in a memory-mapped file, while the LRU tracker
 * monitors access patterns for optimization decisions.
 */
template<class Record>
class MMapXTree {
public:
    using BucketType = XTreeBucket<Record>;
    using BucketPtr = MMapPtr<BucketType>;
    
    /**
     * Create or open an XTree backed by a memory-mapped file
     * @param filename Path to the storage file
     * @param dimension Number of dimensions
     * @param precision Precision for MBR coordinates
     * @param dimLabels Dimension labels
     * @param create_new Whether to create new file or open existing
     */
    MMapXTree(const std::string& filename,
              unsigned short dimension,
              unsigned short precision,
              std::vector<const char*>* dimLabels,
              bool create_new = false);
    
    ~MMapXTree();
    
    /**
     * Insert a record into the tree
     */
    void insert(Record* record);
    
    /**
     * Search for records that intersect with the given key
     */
    std::vector<Record*> search(const KeyMBR& searchKey, SearchType searchType = INTERSECTS);
    
    /**
     * Get the root bucket
     */
    BucketPtr getRoot();
    
    /**
     * Get access statistics for optimization
     */
    LRUAccessTracker* getAccessTracker() { return access_tracker_.get(); }
    
    /**
     * Get hot node detector for tree optimization
     */
    HotNodeDetector* getHotNodeDetector() { return hot_node_detector_.get(); }
    
    /**
     * Pin frequently accessed nodes in memory
     * @param max_pinned_mb Maximum memory to use for pinning (in MB)
     */
    void optimize_memory_pinning(size_t max_pinned_mb = 64);
    
    /**
     * Get recommendations for thread affinity optimizations
     */
    std::vector<HotNodeDetector::OptimizationSuggestion> get_threading_suggestions();
    
    /**
     * Sync changes to disk
     */
    void sync() { mmap_file_->sync(); }
    
    /**
     * Get storage statistics
     */
    struct StorageStats {
        size_t file_size;
        size_t mapped_size;
        size_t tracked_nodes;
        size_t pinned_nodes;
        size_t pinned_memory_mb;
    };
    
    StorageStats get_storage_stats() const;
    
private:
    std::unique_ptr<MMapFile> mmap_file_;
    std::unique_ptr<MMapFile> data_mmap_;
    std::unique_ptr<XTreeSerializer<Record>> serializer_;
    std::unique_ptr<LRUAccessTracker> access_tracker_;
    std::unique_ptr<HotNodeDetector> hot_node_detector_;
    std::unique_ptr<IndexDetails<Record>> index_details_;
    
    unsigned short dimension_;
    unsigned short precision_;
    std::vector<const char*>* dimLabels_;
    
    uint64_t root_offset_ = 0;
    XTreeBucket<Record>* root_bucket_ = nullptr;
    
    /**
     * Allocate a new bucket in the memory-mapped file
     */
    BucketPtr allocate_bucket(bool isRoot = false, KeyMBR* key = nullptr, 
                              bool isLeaf = true);
    
    /**
     * Get bucket at given offset
     */
    BucketPtr get_bucket(size_t offset);
    
    /**
     * Initialize a new tree
     */
    void initialize_new_tree(std::unique_ptr<xtree::XTreeSerializer<Record>> serializer,
                              std::unique_ptr<MMapFile> data_mmap);
    
    /**
     * Load existing tree from file
     */
    void load_existing_tree(std::unique_ptr<xtree::XTreeSerializer<Record>> serializer,
                            std::unique_ptr<MMapFile> data_mmap);
    
    /**
     * Prefetch pages into memory for better cache performance
     * @param offset Starting offset (will be aligned to page boundary)
     * @param num_pages Number of pages to prefetch
     */
    void prefetchPages(uint64_t offset, size_t num_pages);
};

// Factory is now defined in xtree_mmap_factory.h

} // namespace xtree