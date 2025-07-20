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

#include <cstdint>
#include <cstring>
#include <vector>
#include "xtree.h"
#include "mmapfile.h"

namespace xtree {

/**
 * Storage format for memory-mapped XTree persistence
 * 
 * TWO-FILE APPROACH:
 * 1. .xtree file: Tree structure (buckets with MBRs and data record offsets)
 * 2. .xdata file: Actual data records (can be compressed separately)
 * 
 * Benefits:
 * - Better cache locality for tree traversal (only MBRs loaded)
 * - Separate compression strategies for tree vs data
 * - Easier backup/replication strategies
 * - Optimized access patterns
 */

// Binary format version for compatibility
static constexpr uint32_t XTREE_STORAGE_VERSION = 1;

/**
 * File headers for both .xtree and .xdata files
 */
struct XTreeFileHeader {
    uint32_t magic;              // "XTRE" for .xtree, "XDAT" for .xdata
    uint32_t version;            // Format version
    uint32_t dimension_count;    // Number of spatial dimensions
    uint32_t precision;          // MBR coordinate precision
    uint64_t root_offset;        // Offset of root bucket in .xtree file
    uint64_t total_records;      // Total number of data records
    uint64_t tree_size;          // Size of tree structure
    uint64_t data_size;          // Size of data records
    char reserved[32];           // Reserved for future use
} __attribute__((packed));

static constexpr uint32_t XTREE_MAGIC = 0x58545245; // "XTRE"
static constexpr uint32_t XDATA_MAGIC = 0x58444154; // "XDAT"

/**
 * Binary format for serialized XTreeBucket
 * Layout in .xtree file:
 */
struct SerializedBucketHeader {
    uint32_t size;               // Total size of this bucket (including variable data)
    uint32_t n;                  // Number of active children
    uint8_t is_leaf;             // Is this a leaf bucket?
    uint8_t is_supernode;        // Is this a supernode?
    uint8_t owns_preallocated;   // Owns pre-allocated nodes flag
    uint8_t reserved;            // Padding for alignment
    uint64_t parent_offset;      // Offset of parent bucket (0 if root)
    uint64_t next_child_offset;  // Offset of next child bucket (0 if none)
    uint64_t prev_child_offset;  // Offset of previous child bucket (0 if none)
} __attribute__((packed));

/**
 * Binary format for KeyMBR (bounding box)
 */
struct SerializedKeyMBR {
    uint16_t dimension_count;    // Number of dimensions
    uint16_t precision;          // Coordinate precision
    uint32_t data_size;          // Size of coordinate data in bytes
    // Followed by coordinate data (variable length)
} __attribute__((packed));

/**
 * Binary format for _MBRKeyNode
 */
struct SerializedMBRKeyNode {
    uint8_t is_leaf;             // Is this node a leaf?
    uint8_t is_cached;           // Is record cached in memory?
    uint8_t reserved[2];         // Padding
    uint64_t record_offset;      // Offset in .xdata file (if leaf) or .xtree file (if internal)
    uint64_t key_mbr_offset;     // Offset of KeyMBR data within this bucket
} __attribute__((packed));

/**
 * Binary format for data record in .xdata file
 */
struct SerializedDataRecord {
    uint32_t size;               // Total size of this record
    uint32_t type_id;            // Type identifier for record type
    uint64_t key_mbr_offset;     // Offset of KeyMBR within this record
    // Followed by serialized record data and KeyMBR
} __attribute__((packed));

/**
 * Serialized DataRecord-specific content
 * Follows the SerializedDataRecord header in .xdata file
 */
struct SerializedDataRecordContent {
    uint32_t rowid_length;   // Length of row ID string
    uint32_t num_points;     // Number of points in this record
    uint16_t dimension;      // Dimension of each point
    uint16_t precision;      // Precision bits
    // Followed by:
    // - Row ID string (rowid_length bytes)
    // - Points data (num_points * dimension * sizeof(double))
    // - KeyMBR data
} __attribute__((packed));

/**
 * XTree serialization manager
 * Handles conversion between in-memory XTree structures and binary format
 */
template<typename Record>
class XTreeSerializer {
public:
    /**
     * Initialize serializer for specific record type
     */
    XTreeSerializer(MMapFile* tree_file, MMapFile* data_file);
    
    /**
     * Serialize an XTreeBucket to the .xtree file
     * @param bucket The bucket to serialize
     * @param parent_offset Offset of parent bucket (0 for root)
     * @return Offset where bucket was written
     */
    uint64_t serializeBucket(const XTreeBucket<Record>* bucket, uint64_t parent_offset = 0);
    
    /**
     * Deserialize an XTreeBucket from the .xtree file
     * @param offset Offset in file where bucket is stored
     * @param idx Index details for the tree
     * @return Reconstructed bucket (caller owns)
     */
    XTreeBucket<Record>* deserializeBucket(uint64_t offset, IndexDetails<Record>* idx);
    
    /**
     * Serialize a data record to the .xdata file
     * @param record The record to serialize
     * @return Offset where record was written
     */
    uint64_t serializeDataRecord(const Record* record);
    
    /**
     * Deserialize a data record from the .xdata file
     * @param offset Offset in data file where record is stored
     * @return Reconstructed record (caller owns)
     */
    Record* deserializeDataRecord(uint64_t offset);
    
    /**
     * Write file headers
     */
    void writeTreeHeader(uint32_t dimension_count, uint32_t precision);
    void writeDataHeader(uint32_t dimension_count, uint32_t precision);
    
    /**
     * Read and validate file headers
     */
    XTreeFileHeader readTreeHeader();
    XTreeFileHeader readDataHeader();
    
private:
    MMapFile* tree_file_;
    MMapFile* data_file_;
    
    // Helper methods
    uint64_t serializeKeyMBR(const KeyMBR* key_mbr);
    KeyMBR* deserializeKeyMBR(uint64_t offset);
    uint64_t serializeMBRKeyNode(const typename XTreeBucket<Record>::_MBRKeyNode* node);
    typename XTreeBucket<Record>::_MBRKeyNode* deserializeMBRKeyNode(uint64_t offset);
    
    // Record type serialization (template specialization needed)
    uint64_t serializeRecordData(const Record* record);
    Record* deserializeRecordData(uint64_t offset, uint32_t size);
};

/**
 * Data storage manager for .xdata file
 * Handles allocation and compression of data records
 */
class DataStorageManager {
public:
    DataStorageManager(MMapFile* data_file);
    
    /**
     * Store a data record and return its offset
     */
    uint64_t storeRecord(const void* data, uint32_t size, uint32_t type_id);
    
    /**
     * Retrieve a data record by offset
     */
    std::vector<uint8_t> getRecord(uint64_t offset);
    
    /**
     * Get record header without loading full data
     */
    SerializedDataRecord getRecordHeader(uint64_t offset);
    
    /**
     * Allocate space for a new record
     * @param size Size needed in bytes
     * @return Offset where space was allocated, 0 on failure
     */
    uint64_t allocate(uint32_t size);
    
    /**
     * Compact data file by removing unused space
     */
    void compact();
    
private:
    MMapFile* data_file_;
    
    struct FreeBlock {
        uint64_t offset;
        uint32_t size;
    };
    
    std::vector<FreeBlock> free_blocks_;
    
    uint64_t findFreeSpace(uint32_t size);
    void addFreeBlock(uint64_t offset, uint32_t size);
};

} // namespace xtree