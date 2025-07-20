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

#include "xtree_serialization.h"
#include <cassert>
#include <algorithm>
#include "float_utils.h"
#include "xtree_mmap_factory.h"
#include "util/log.h"

namespace xtree {

// DataStorageManager implementation
DataStorageManager::DataStorageManager(MMapFile* data_file) : data_file_(data_file) {
    // Ensure space for both MMapFile header and XTree data header
    size_t min_size = MMapFile::HEADER_SIZE + sizeof(XTreeFileHeader);
    if (data_file_->size() < min_size) {
        data_file_->expand(min_size);
    }
}

uint64_t DataStorageManager::storeRecord(const void* data, uint32_t size, uint32_t type_id) {
    // Calculate total space needed
    uint32_t total_size = sizeof(SerializedDataRecord) + size;
    
    // Find free space or allocate at end
    uint64_t offset = findFreeSpace(total_size);
    if (offset == 0) {
        offset = data_file_->allocate(total_size);
        if (offset == 0) {
            // Need to expand file
            size_t current_size = data_file_->size();
            size_t new_size = current_size + std::max(total_size, 1024*1024U); // At least 1MB expansion
            if (!data_file_->expand(new_size)) {
                return 0; // Failed to expand
            }
            offset = data_file_->allocate(total_size);
        }
    }
    
    // Write record header
    SerializedDataRecord header;
    header.size = total_size;
    header.type_id = type_id;
    header.key_mbr_offset = sizeof(SerializedDataRecord); // MBR comes right after header
    
    void* record_ptr = data_file_->getPointer(offset);
    if (!record_ptr) return 0;
    
    std::memcpy(record_ptr, &header, sizeof(header));
    
    // Write record data
    void* data_ptr = static_cast<char*>(record_ptr) + sizeof(header);
    std::memcpy(data_ptr, data, size);
    
    return offset;
}

std::vector<uint8_t> DataStorageManager::getRecord(uint64_t offset) {
    SerializedDataRecord header = getRecordHeader(offset);
    
    void* record_ptr = data_file_->getPointer(offset + sizeof(SerializedDataRecord));
    if (!record_ptr) return {};
    
    uint32_t data_size = header.size - sizeof(SerializedDataRecord);
    std::vector<uint8_t> data(data_size);
    std::memcpy(data.data(), record_ptr, data_size);
    
    return data;
}

SerializedDataRecord DataStorageManager::getRecordHeader(uint64_t offset) {
    SerializedDataRecord header;
    void* header_ptr = data_file_->getPointer(offset);
    if (header_ptr) {
        std::memcpy(&header, header_ptr, sizeof(header));
    }
    return header;
}

uint64_t DataStorageManager::allocate(uint32_t size) {
    // Try to find free space first
    uint64_t offset = findFreeSpace(size);
    if (offset == 0) {
        // No free space, allocate at end
        offset = data_file_->allocate(size);
        if (offset == 0) {
            // Need to expand file
            size_t current_size = data_file_->size();
            size_t new_size = current_size + std::max(size, 1024*1024U); // At least 1MB expansion
            if (!data_file_->expand(new_size)) {
                return 0; // Failed to expand
            }
            offset = data_file_->allocate(size);
        }
    }
    return offset;
}

uint64_t DataStorageManager::findFreeSpace(uint32_t size) {
    // Simple first-fit allocation from free blocks
    for (auto it = free_blocks_.begin(); it != free_blocks_.end(); ++it) {
        if (it->size >= size) {
            uint64_t offset = it->offset;
            
            // If block is larger than needed, split it
            if (it->size > size) {
                it->offset += size;
                it->size -= size;
            } else {
                // Use entire block
                free_blocks_.erase(it);
            }
            
            return offset;
        }
    }
    
    return 0; // No suitable free space found
}

void DataStorageManager::addFreeBlock(uint64_t offset, uint32_t size) {
    FreeBlock block = {offset, size};
    
    // Insert in sorted order and try to merge with adjacent blocks
    auto it = std::lower_bound(free_blocks_.begin(), free_blocks_.end(), block,
                               [](const FreeBlock& a, const FreeBlock& b) {
                                   return a.offset < b.offset;
                               });
    
    free_blocks_.insert(it, block);
    
    // TODO: Implement block merging for better space efficiency
}

void DataStorageManager::compact() {
    // TODO: Implement compaction by moving records to eliminate fragmentation
    // This would involve:
    // 1. Scanning all records
    // 2. Moving them to eliminate gaps
    // 3. Updating all references to moved records
    // 4. Truncating file to new size
}

// XTreeSerializer template implementation
template<typename Record>
XTreeSerializer<Record>::XTreeSerializer(MMapFile* tree_file, MMapFile* data_file)
    : tree_file_(tree_file), data_file_(data_file) {
    // Reserve space for XTree headers (they come after MMapFile header)
    // This ensures allocations don't overwrite our headers
    if (tree_file_ && tree_file_->allocate(0) == MMapFile::HEADER_SIZE) {
        tree_file_->allocate(sizeof(XTreeFileHeader));
    }
    if (data_file_ && data_file_->allocate(0) == MMapFile::HEADER_SIZE) {
        data_file_->allocate(sizeof(XTreeFileHeader));
    }
}

template<typename Record>
void XTreeSerializer<Record>::writeTreeHeader(uint32_t dimension_count, uint32_t precision) {
    XTreeFileHeader header;
    header.magic = XTREE_MAGIC;
    header.version = XTREE_STORAGE_VERSION;
    header.dimension_count = dimension_count;
    header.precision = precision;
    header.root_offset = 0; // Will be set when root is serialized
    header.total_records = 0;
    header.tree_size = 0;
    header.data_size = 0;
    std::memset(header.reserved, 0, sizeof(header.reserved));
    
    // Write XTree header after MMapFile header
    size_t xtree_header_offset = MMapFile::HEADER_SIZE;
    void* header_ptr = tree_file_->getPointer(xtree_header_offset);
    if (header_ptr) {
        std::memcpy(header_ptr, &header, sizeof(header));
    }
}

template<typename Record>
void XTreeSerializer<Record>::writeDataHeader(uint32_t dimension_count, uint32_t precision) {
    XTreeFileHeader header;
    header.magic = XDATA_MAGIC;
    header.version = XTREE_STORAGE_VERSION;
    header.dimension_count = dimension_count;
    header.precision = precision;
    header.root_offset = 0; // Not used for data file
    header.total_records = 0;
    header.tree_size = 0;
    header.data_size = 0;
    std::memset(header.reserved, 0, sizeof(header.reserved));
    
    // Write XData header after MMapFile header
    size_t xdata_header_offset = MMapFile::HEADER_SIZE;
    void* header_ptr = data_file_->getPointer(xdata_header_offset);
    if (header_ptr) {
        std::memcpy(header_ptr, &header, sizeof(header));
    }
}

template<typename Record>
XTreeFileHeader XTreeSerializer<Record>::readTreeHeader() {
    XTreeFileHeader header;
    // Read XTree header from after MMapFile header
    size_t xtree_header_offset = MMapFile::HEADER_SIZE;
    void* header_ptr = tree_file_->getPointer(xtree_header_offset);
    if (header_ptr) {
        std::memcpy(&header, header_ptr, sizeof(header));
        
        // Validate header
        if (header.magic != XTREE_MAGIC) {
            throw std::runtime_error("Invalid .xtree file magic number");
        }
        if (header.version != XTREE_STORAGE_VERSION) {
            throw std::runtime_error("Unsupported .xtree file version");
        }
    }
    return header;
}

template<typename Record>
XTreeFileHeader XTreeSerializer<Record>::readDataHeader() {
    XTreeFileHeader header;
    // Read XData header from after MMapFile header
    size_t xdata_header_offset = MMapFile::HEADER_SIZE;
    void* header_ptr = data_file_->getPointer(xdata_header_offset);
    if (header_ptr) {
        std::memcpy(&header, header_ptr, sizeof(header));
        
        // Validate header
        if (header.magic != XDATA_MAGIC) {
            throw std::runtime_error("Invalid .xdata file magic number");
        }
        if (header.version != XTREE_STORAGE_VERSION) {
            throw std::runtime_error("Unsupported .xdata file version");
        }
    }
    return header;
}

template<typename Record>
uint64_t XTreeSerializer<Record>::serializeBucket(const XTreeBucket<Record>* bucket, uint64_t parent_offset) {
    if (!bucket) return 0;
    
    // Calculate total size needed
    uint32_t header_size = sizeof(SerializedBucketHeader);
    uint32_t key_mbr_size = 0;
    uint32_t children_size = 0;
    
    // Size for bucket's key MBR
    if (bucket->getKey()) {
        KeyMBR* key = bucket->getKey();
        key_mbr_size = sizeof(SerializedKeyMBR) + key->memUsage();
    }
    
    // Size for children
    children_size = bucket->n() * sizeof(SerializedMBRKeyNode);
    for (int i = 0; i < bucket->n(); ++i) {
        // Add size for each child's key MBR
        // Need to cast away const to access protected _kn method
        auto child_node = const_cast<XTreeBucket<Record>*>(bucket)->_kn(i);
        if (child_node && child_node->getKey()) {
            KeyMBR* key = child_node->getKey();
            children_size += sizeof(SerializedKeyMBR) + key->memUsage();
        }
    }
    
    uint32_t total_size = header_size + key_mbr_size + children_size;
    
    // For better page cache performance, align bucket allocations to cache line boundaries
    // This reduces false sharing and improves memory access patterns
    constexpr size_t CACHE_LINE_SIZE = 64;  // Common cache line size
    total_size = ((total_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
    
    // Allocate space
    uint64_t bucket_offset = tree_file_->allocate(total_size);
    if (bucket_offset == 0) return 0;
    
    void* bucket_ptr = tree_file_->getPointer(bucket_offset);
    if (!bucket_ptr) return 0;
    
    // Write bucket header
    SerializedBucketHeader header;
    header.size = total_size;
    header.n = bucket->n();
    
    // Cast to access protected members through friend access
    XTreeBucket<Record>* non_const_bucket = const_cast<XTreeBucket<Record>*>(bucket);
    header.is_leaf = non_const_bucket->isLeaf() ? 1 : 0;
    header.is_supernode = non_const_bucket->_isSupernode ? 1 : 0;
    header.owns_preallocated = non_const_bucket->_ownsPreallocatedNodes ? 1 : 0;
    header.reserved = 0;
    header.parent_offset = parent_offset;
    header.next_child_offset = 0; // TODO: Handle sibling links if needed
    header.prev_child_offset = 0;
    
    std::memcpy(bucket_ptr, &header, sizeof(header));
    
    // Current write position
    uint64_t write_pos = bucket_offset + sizeof(header);
    
    // Write bucket's key MBR
    if (bucket->getKey()) {
        uint64_t key_offset = serializeKeyMBR(bucket->getKey());
        // For now, we'll embed the key data directly in the bucket
        // In a more sophisticated implementation, we might reference it by offset
    }
    
    // Write children
    for (int i = 0; i < bucket->n(); ++i) {
        auto child_node = const_cast<XTreeBucket<Record>*>(bucket)->_kn(i);
        if (child_node) {
            // Serialize child node
            SerializedMBRKeyNode serialized_child;
            serialized_child.is_leaf = child_node->getLeaf() ? 1 : 0;
            serialized_child.is_cached = child_node->getCached() ? 1 : 0;
            serialized_child.reserved[0] = serialized_child.reserved[1] = 0;
            
            // Handle record offset based on whether it's a leaf or internal node
            if (child_node->getLeaf()) {
                // For leaf nodes, record points to data in .xdata file
                // TODO: Serialize the actual record and store offset
                serialized_child.record_offset = 0; // Placeholder
            } else {
                // For internal nodes, record points to child bucket in .xtree file
                // TODO: Recursively serialize child bucket
                serialized_child.record_offset = 0; // Placeholder
            }
            
            // Serialize child's key MBR
            if (child_node->getKey()) {
                serialized_child.key_mbr_offset = serializeKeyMBR(child_node->getKey());
            } else {
                serialized_child.key_mbr_offset = 0;
            }
            
            void* child_ptr = tree_file_->getPointer(write_pos);
            if (child_ptr) {
                std::memcpy(child_ptr, &serialized_child, sizeof(serialized_child));
                write_pos += sizeof(serialized_child);
            }
        }
    }
    
#ifdef _DEBUG
    log() << "[SERIALIZE] Bucket at offset " << bucket_offset 
          << " (size=" << total_size << ", n=" << bucket->n() 
          << ", leaf=" << non_const_bucket->isLeaf() << ")" << endl;
#endif
    
    return bucket_offset;
}

template<typename Record>
uint64_t XTreeSerializer<Record>::serializeKeyMBR(const KeyMBR* key_mbr) {
    if (!key_mbr) return 0;
    
    uint32_t dimension_count = key_mbr->getDimensionCount();
    // Size needed for coordinate data: 2 * dimension_count * sizeof(int32_t)
    uint32_t coord_data_size = 2 * dimension_count * sizeof(int32_t);
    uint32_t total_size = sizeof(SerializedKeyMBR) + coord_data_size;
    
    uint64_t offset = tree_file_->allocate(total_size);
    if (offset == 0) return 0;
    
    void* mbr_ptr = tree_file_->getPointer(offset);
    if (!mbr_ptr) return 0;
    
    // Write KeyMBR header
    SerializedKeyMBR header;
    header.dimension_count = dimension_count;
    header.precision = 32; // KeyMBR uses 32-bit sortable integers
    header.data_size = coord_data_size;
    
    std::memcpy(mbr_ptr, &header, sizeof(header));
    
    // Write KeyMBR coordinate data
    void* data_ptr = static_cast<char*>(mbr_ptr) + sizeof(header);
    
    // Extract coordinate data using getSortableBoxVal
    int32_t* coord_array = static_cast<int32_t*>(data_ptr);
    for (uint32_t i = 0; i < 2 * dimension_count; ++i) {
        coord_array[i] = key_mbr->getSortableBoxVal(i);
    }
    
    return offset;
}

template<typename Record>
uint64_t XTreeSerializer<Record>::serializeDataRecord(const Record* record) {
    if (!record) return 0;
    
    // Generic implementation - delegates to type-specific serializer
    return serializeRecordData(record);
}

// Template specialization for DataRecord
template<>
uint64_t XTreeSerializer<DataRecord>::serializeRecordData(const DataRecord* record) {
    if (!record) return 0;
    
    // Get record components
    const std::string& rowid = const_cast<DataRecord*>(record)->getRowID();
    auto points = const_cast<DataRecord*>(record)->getPoints();
    KeyMBR* key = record->getKey();
    
    // Calculate sizes
    uint32_t content_header_size = sizeof(SerializedDataRecordContent);
    uint32_t rowid_size = rowid.length();
    uint32_t points_data_size = 0;
    if (!points.empty() && !points[0].empty()) {
        points_data_size = points.size() * points[0].size() * sizeof(double);
    }
    uint32_t key_mbr_size = 0;
    
    // Only serialize KeyMBR if we have points (empty records have invalid KeyMBR)
    if (key && !points.empty()) {
        key_mbr_size = sizeof(SerializedKeyMBR) + 2 * key->getDimensionCount() * sizeof(int32_t);
    }
    
    uint32_t total_content_size = content_header_size + rowid_size + points_data_size + key_mbr_size;
    uint32_t total_size = sizeof(SerializedDataRecord) + total_content_size;
    
    // Use DataStorageManager to allocate space
    DataStorageManager storage_mgr(data_file_);
    uint64_t offset = storage_mgr.allocate(total_size);
    if (offset == 0) return 0;
    
    void* record_ptr = data_file_->getPointer(offset);
    if (!record_ptr) return 0;
    
    // Write SerializedDataRecord header
    SerializedDataRecord header;
    header.size = total_size;
    header.type_id = 1; // Type ID for DataRecord
    header.key_mbr_offset = (key_mbr_size > 0) ? 
        sizeof(SerializedDataRecord) + content_header_size + rowid_size + points_data_size : 0;
    
    std::memcpy(record_ptr, &header, sizeof(header));
    
    // Write SerializedDataRecordContent
    void* content_ptr = static_cast<char*>(record_ptr) + sizeof(header);
    SerializedDataRecordContent content;
    content.rowid_length = rowid_size;
    content.num_points = points.size();
    content.dimension = points.empty() ? 0 : points[0].size();
    content.precision = 32; // Default precision
    
    std::memcpy(content_ptr, &content, sizeof(content));
    
    // Write row ID string
    void* rowid_ptr = static_cast<char*>(content_ptr) + sizeof(content);
    std::memcpy(rowid_ptr, rowid.c_str(), rowid_size);
    
    // Write points data
    void* points_ptr = static_cast<char*>(rowid_ptr) + rowid_size;
    for (const auto& point : points) {
        std::memcpy(points_ptr, point.data(), point.size() * sizeof(double));
        points_ptr = static_cast<char*>(points_ptr) + point.size() * sizeof(double);
    }
    
    // Write KeyMBR data (only if we have points)
    if (key && !points.empty()) {
        void* key_ptr = static_cast<char*>(rowid_ptr) + rowid_size + points_data_size;
        
        SerializedKeyMBR key_header;
        key_header.dimension_count = key->getDimensionCount();
        key_header.precision = 32;
        key_header.data_size = 2 * key->getDimensionCount() * sizeof(int32_t);
        
        std::memcpy(key_ptr, &key_header, sizeof(key_header));
        
        // Write coordinate data
        int32_t* coord_ptr = reinterpret_cast<int32_t*>(static_cast<char*>(key_ptr) + sizeof(key_header));
        for (uint32_t i = 0; i < 2 * key->getDimensionCount(); ++i) {
            coord_ptr[i] = key->getSortableBoxVal(i);
        }
    }
    
#ifdef _DEBUG
    log() << "[SERIALIZE] DataRecord at offset " << offset 
          << " (rowid=" << rowid << ", points=" << points.size() 
          << ", size=" << total_size << ")" << endl;
#endif
    
    return offset;
}

template<typename Record>
XTreeBucket<Record>* XTreeSerializer<Record>::deserializeBucket(uint64_t offset, IndexDetails<Record>* idx) {
    if (offset == 0 || !idx) return nullptr;
    
    void* bucket_ptr = tree_file_->getPointer(offset);
    if (!bucket_ptr) return nullptr;
    
    // Read bucket header
    SerializedBucketHeader header;
    std::memcpy(&header, bucket_ptr, sizeof(header));
    
    // Validate header
    if (header.size == 0 || header.size > 1024*1024 || // Reasonable size limits
        header.n > 1000) { // Reasonable child count limit
        return nullptr; // Invalid header
    }
    
#ifdef _DEBUG
    log() << "[DESERIALIZE] Bucket from offset " << offset 
          << " (size=" << header.size << ", n=" << header.n 
          << ", leaf=" << (header.is_leaf ? "true" : "false") << ")" << endl;
#endif
    
    // Read position tracker
    uint64_t read_pos = offset + sizeof(header);
    
    // Deserialize bucket's key MBR if present
    KeyMBR* bucket_key = nullptr;
    if (read_pos < offset + header.size) {
        // Check if there's a KeyMBR at this position
        void* mbr_check_ptr = tree_file_->getPointer(read_pos);
        if (mbr_check_ptr) {
            SerializedKeyMBR* mbr_header = static_cast<SerializedKeyMBR*>(mbr_check_ptr);
            if (mbr_header->dimension_count > 0 && mbr_header->dimension_count <= 10) { // Sanity check
                bucket_key = deserializeKeyMBR(read_pos);
                if (bucket_key) {
                    read_pos += sizeof(SerializedKeyMBR) + mbr_header->data_size;
                }
            }
        }
    }
    
    // Create the bucket
    // Note: We need to create children vector first
    std::vector<typename XTreeBucket<Record>::_MBRKeyNode*>* children = nullptr;
    if (header.n > 0) {
        children = new std::vector<typename XTreeBucket<Record>::_MBRKeyNode*>();
        children->reserve(header.n);
        
        // Deserialize children
        for (uint32_t i = 0; i < header.n; ++i) {
            if (read_pos >= offset + header.size) break;
            
            void* child_ptr = tree_file_->getPointer(read_pos);
            if (!child_ptr) break;
            
            SerializedMBRKeyNode serialized_child;
            std::memcpy(&serialized_child, child_ptr, sizeof(serialized_child));
            read_pos += sizeof(serialized_child);
            
            // Create child node
            auto child_node = new typename XTreeBucket<Record>::_MBRKeyNode();
            child_node->setLeaf(serialized_child.is_leaf);
            child_node->setCached(false); // Always start uncached from disk
            
            // Deserialize child's key MBR
            if (serialized_child.key_mbr_offset > 0) {
                KeyMBR* child_key = deserializeKeyMBR(serialized_child.key_mbr_offset);
                if (child_key) {
                    child_node->setKey(child_key);
                }
            }
            
            // Handle record/child bucket reference
            // For now, we store the offset as a UniqueId placeholder
            // TODO: Implement proper data record loading for leaf nodes
            // TODO: Implement proper bucket reference resolution for internal nodes
            UniqueId placeholder_id = static_cast<UniqueId>(serialized_child.record_offset);
            
            // Since setRecord expects a pointer to UniqueId data to copy from,
            // we need to ensure the data persists. For now, we'll skip setting
            // the record ID as it requires proper cache integration.
            // The record offset is preserved in serialized_child for future use.
            
            children->push_back(child_node);
        }
    }
    
    // Create bucket with deserialized data
    XTreeBucket<Record>* bucket = new XTreeBucket<Record>(
        idx,
        false,  // isRoot (will be set by caller if needed)
        bucket_key,
        children,
        0,      // split_index (not relevant for deserialization)
        header.is_leaf,
        header.n
    );
    
    // Set additional properties
    bucket->_isSupernode = header.is_supernode;
    bucket->_ownsPreallocatedNodes = header.owns_preallocated;
    
    // Clean up temporary children vector if created
    if (children) {
        delete children;
    }
    
    return bucket;
}

template<typename Record>
KeyMBR* XTreeSerializer<Record>::deserializeKeyMBR(uint64_t offset) {
    if (offset == 0) return nullptr;
    
    void* mbr_ptr = tree_file_->getPointer(offset);
    if (!mbr_ptr) return nullptr;
    
    // Read KeyMBR header
    SerializedKeyMBR header;
    std::memcpy(&header, mbr_ptr, sizeof(header));
    
    // Create new KeyMBR
    KeyMBR* key_mbr = new KeyMBR(header.dimension_count, header.precision);
    
    // Read coordinate data
    void* data_ptr = static_cast<char*>(mbr_ptr) + sizeof(header);
    int32_t* coord_array = static_cast<int32_t*>(data_ptr);
    
    // Restore coordinates by expanding the KeyMBR with the stored bounds
    // First reset the KeyMBR to prepare for expansion
    key_mbr->reset();
    
    // For each dimension, create a temporary KeyMBR with the min/max values
    // and expand our KeyMBR with it
    for (uint32_t d = 0; d < header.dimension_count; ++d) {
        // Get min and max values for this dimension
        float min_val = sortableIntToFloat(coord_array[2 * d]);
        float max_val = sortableIntToFloat(coord_array[2 * d + 1]);
        
        // Create a point at the min corner
        std::vector<double> min_point(header.dimension_count, 0.0);
        min_point[d] = min_val;
        key_mbr->expandWithPoint(&min_point);
        
        // Create a point at the max corner
        std::vector<double> max_point(header.dimension_count, 0.0);
        max_point[d] = max_val;
        key_mbr->expandWithPoint(&max_point);
    }
    
#ifdef _DEBUG
    log() << "[DESERIALIZE] KeyMBR from offset " << offset 
          << " (dims=" << header.dimension_count 
          << ", precision=" << header.precision 
          << ", size=" << header.data_size << ")" << endl;
#endif
    
    return key_mbr;
}

template<typename Record>
Record* XTreeSerializer<Record>::deserializeDataRecord(uint64_t offset) {
    if (offset == 0) return nullptr;
    
    // Generic implementation - delegates to type-specific deserializer
    return deserializeRecordData(offset, 0);
}

// Template specialization for DataRecord deserialization
template<>
DataRecord* XTreeSerializer<DataRecord>::deserializeRecordData(uint64_t offset, uint32_t size) {
    if (offset == 0) return nullptr;
    
    // Check if offset is reasonable (within file bounds)
    if (offset > data_file_->size()) {
        return nullptr;
    }
    
    // Read the record header first
    void* header_ptr = data_file_->getPointer(offset);
    if (!header_ptr) {
        return nullptr;
    }
    
    SerializedDataRecord header;
    std::memcpy(&header, header_ptr, sizeof(header));
    
    // Validate header for sanity
    if (header.size == 0 || header.size > 100*1024*1024 || // Max 100MB per record 
        header.type_id != 1 ||  // Type ID 1 for DataRecord
        header.key_mbr_offset > header.size) {  // Offset can't be beyond record size
        return nullptr;
    }
    
    // Read content header
    void* content_ptr = static_cast<char*>(header_ptr) + sizeof(header);
    SerializedDataRecordContent content;
    std::memcpy(&content, content_ptr, sizeof(content));
    
    // Read row ID
    void* rowid_ptr = static_cast<char*>(content_ptr) + sizeof(content);
    std::string rowid(static_cast<char*>(rowid_ptr), content.rowid_length);
    
    // Create the DataRecord
    DataRecord* record = new DataRecord(content.dimension, content.precision, rowid);
    
    // Read and add points
    if (content.num_points > 0) {
        void* points_ptr = static_cast<char*>(rowid_ptr) + content.rowid_length;
        for (uint32_t i = 0; i < content.num_points; ++i) {
            std::vector<double> point(content.dimension);
            std::memcpy(point.data(), points_ptr, content.dimension * sizeof(double));
            record->putPoint(&point);
            points_ptr = static_cast<char*>(points_ptr) + content.dimension * sizeof(double);
        }
    }
    
    // Read and restore KeyMBR if present
    if (header.key_mbr_offset > 0) {
        void* key_ptr = static_cast<char*>(header_ptr) + header.key_mbr_offset;
        
        SerializedKeyMBR key_header;
        std::memcpy(&key_header, key_ptr, sizeof(key_header));
        
        // The KeyMBR should already be properly set from the points
        // But we can validate it matches what was stored
        int32_t* coord_ptr = reinterpret_cast<int32_t*>(static_cast<char*>(key_ptr) + sizeof(key_header));
        
        // Optional: Validate the stored KeyMBR matches the computed one
        KeyMBR* stored_key = record->getKey();
        if (stored_key) {
            bool matches = true;
            for (uint32_t i = 0; i < 2 * key_header.dimension_count; ++i) {
                if (coord_ptr[i] != stored_key->getSortableBoxVal(i)) {
                    matches = false;
                    break;
                }
            }
            if (!matches) {
                warning() << "[DESERIALIZE] Warning: Stored KeyMBR doesn't match computed MBR" << endl;
            }
        }
    }
    
#ifdef _DEBUG
    log() << "[DESERIALIZE] DataRecord from offset " << offset 
          << " (rowid=" << rowid << ", points=" << content.num_points 
          << ", dims=" << content.dimension << ")" << endl;
#endif
    
    return record;
}

template<typename Record>
typename XTreeBucket<Record>::_MBRKeyNode* XTreeSerializer<Record>::deserializeMBRKeyNode(uint64_t offset) {
    // This is handled inline in deserializeBucket for efficiency
    return nullptr;
}

// Stub implementations for remaining private methods
template<typename Record>
uint64_t XTreeSerializer<Record>::serializeMBRKeyNode(const typename XTreeBucket<Record>::_MBRKeyNode* node) {
    // Already implemented inline in serializeBucket
    return 0;
}

template<typename Record>
uint64_t XTreeSerializer<Record>::serializeRecordData(const Record* record) {
    // Generic implementation - should be specialized for specific record types
    return 0;
}

template<typename Record>
Record* XTreeSerializer<Record>::deserializeRecordData(uint64_t offset, uint32_t size) {
    // Generic implementation - should be specialized for specific record types
    return nullptr;
}

// Explicit template instantiation for common record types
// This allows the template implementation to be in the .cpp file

// Forward declaration of common record types
class DataRecord; // Assuming this exists

// Explicit instantiations
template class XTreeSerializer<DataRecord>;

} // namespace xtree