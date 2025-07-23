/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * XTree Data File Management
 * 
 * Manages .xd (data) files separately from .xt (tree) files
 * This separation allows:
 * - Different compression for data vs tree
 * - Independent backup strategies  
 * - Tree reconstruction without data loss
 * - More efficient memory usage
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>

namespace xtree {

/**
 * Data file header for .xd files
 */
struct DataFileHeader {
    uint32_t magic;           // 'XTRD' - XTree Data
    uint32_t version;         // File format version
    uint32_t record_count;    // Number of records
    uint32_t dimension_count; // Dimensions per point
    uint64_t file_size;       // Total file size
    uint64_t data_offset;     // Offset to first record
    uint64_t timestamp;       // Creation time
    uint32_t checksum;        // CRC32 of header + data
    uint32_t reserved[8];     // Future use
    
    static constexpr uint32_t DATA_MAGIC = 0x58545244;  // 'XTRD'
    static constexpr uint32_t DATA_VERSION = 1;
};

/**
 * Individual data record in .xd file
 * No magic number per record - validated by file header
 */
struct DataRecordEntry {
    uint32_t record_id;       // Unique ID for this record
    uint32_t rowid_length;    // Length of rowid string
    uint16_t point_count;     // Number of points
    uint16_t flags;           // Reserved flags
    // Variable data follows:
    // - float points[point_count * dimensions]
    // - char rowid[rowid_length + 1]
    
    size_t size(uint16_t dimensions) const {
        return sizeof(DataRecordEntry) + 
               point_count * dimensions * sizeof(float) +
               rowid_length + 1;
    }
};

/**
 * Manages data file I/O separately from tree structure
 */
class DataFileManager {
public:
    DataFileManager(const std::string& data_path, uint16_t dimensions)
        : data_path_(data_path), dimensions_(dimensions), next_record_id_(0) {
    }
    
    /**
     * Append a data record and return its ID
     */
    uint32_t append_record(const std::vector<float>& points, 
                          const std::string& rowid) {
        // In production, this would:
        // 1. Append to current .xd file
        // 2. Update index mapping
        // 3. Return unique record ID
        // 4. Handle file rotation when size limit reached
        
        uint32_t record_id = next_record_id_++;
        
        // For now, just track in memory
        // Real implementation would write to file
        records_[record_id] = {points, rowid};
        
        return record_id;
    }
    
    /**
     * Read a record by ID
     */
    bool read_record(uint32_t record_id, std::vector<float>& points,
                    std::string& rowid) {
        auto it = records_.find(record_id);
        if (it != records_.end()) {
            points = it->second.first;
            rowid = it->second.second;
            return true;
        }
        return false;
    }
    
    /**
     * Create a new data file with header
     */
    void create_data_file(const std::string& filename) {
        std::ofstream file(filename, std::ios::binary);
        
        DataFileHeader header{};
        header.magic = DataFileHeader::DATA_MAGIC;
        header.version = DataFileHeader::DATA_VERSION;
        header.record_count = 0;
        header.dimension_count = dimensions_;
        header.file_size = sizeof(header);
        header.data_offset = sizeof(header);
        header.timestamp = std::time(nullptr);
        header.checksum = 0;  // Would calculate CRC32
        
        file.write(reinterpret_cast<char*>(&header), sizeof(header));
    }
    
    /**
     * Calculate CRC32 checksum
     */
    static uint32_t calculate_crc32(const void* data, size_t length) {
        // Simple CRC32 implementation
        const uint32_t polynomial = 0xEDB88320;
        uint32_t crc = 0xFFFFFFFF;
        
        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < length; i++) {
            crc ^= bytes[i];
            for (int j = 0; j < 8; j++) {
                crc = (crc >> 1) ^ (-(crc & 1) & polynomial);
            }
        }
        
        return ~crc;
    }
    
private:
    std::string data_path_;
    uint16_t dimensions_;
    uint32_t next_record_id_;
    
    // Temporary in-memory storage
    // Production would use file I/O
    std::unordered_map<uint32_t, 
        std::pair<std::vector<float>, std::string>> records_;
};

/**
 * Tree file header for .xt files
 */
struct TreeFileHeader {
    uint32_t magic;           // 'XTRT' - XTree Tree
    uint32_t version;         // File format version  
    uint32_t root_offset;     // Root node offset
    uint32_t node_count;      // Number of nodes
    uint16_t dimensions;      // Tree dimensions
    uint16_t precision;       // Tree precision
    uint64_t data_file_id;    // Associated .xd file ID
    uint64_t timestamp;       // Creation time
    uint32_t checksum;        // CRC32 of header + tree
    uint32_t reserved[7];     // Future use
    
    static constexpr uint32_t TREE_MAGIC = 0x58545254;  // 'XTRT'
    static constexpr uint32_t TREE_VERSION = 1;
};

} // namespace xtree