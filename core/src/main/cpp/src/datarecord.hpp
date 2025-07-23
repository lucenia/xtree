/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

#pragma once

#include "pch.h"
#include "irecord.hpp"
#include "keymbr.h"
#include "persistence/node_id.hpp"
#include "persistence/mapping_manager.h"
#include "util/endian.hpp"
#include <vector>
#include <string>
#include <cstring>
#include <mutex>

namespace xtree {

/**
 * Interface for data records that contain row IDs.
 * Only DataRecord and DataRecordView implement this, not XTreeBucket.
 * This provides clean interface segregation and type safety.
 */
struct IDataRecord {
    virtual ~IDataRecord() = default;

    /**
     * Get a zero-copy view of the row ID.
     * Lifetime: same as record/view lifetime (for views: until next iterator step)
     * @return string_view pointing to the row ID
     */
    virtual std::string_view getRowIDView() const noexcept = 0;

    /**
     * Get a copy of the row ID when you need ownership.
     * @return string copy of the row ID
     */
    virtual std::string getRowID() const { 
        auto v = getRowIDView(); 
        return std::string(v.data(), v.size()); 
    }
};

/**
 * DataRecord: Traditional heap-allocated data record
 * 
 * Represents a data node containing points and a rowid.
 * This is the mutable, heap-allocated version used for:
 * - IN_MEMORY mode (always)
 * - DURABLE mode during insertions (before persistence)
 * - DURABLE mode when mutations are needed
 */
class alignas(8) DataRecord : public IRecord, public IDataRecord {
public:
    DataRecord(unsigned short dim, unsigned short prc, std::string s)
        : IRecord(new KeyMBR(dim, prc)), _rowid(s), _node_id_raw(persist::NodeID::invalid().raw()) {
#ifdef _DEBUG
        if(_key == NULL) {
            log() << "DataRecord: KEY IS NULL!!!" << std::endl;
            log() << "Memory usage: " << getAvailableSystemMemory() << std::endl;
        }
#endif
    }

    void putPoint(std::vector<double>* location) {
        _points.push_back(*location);
        _key->expandWithPoint(location);
    }

    // IDataRecord interface
    std::string_view getRowIDView() const noexcept override { 
        return std::string_view(_rowid); 
    }
    // getRowID() inherits default implementation from IDataRecord
    
    KeyMBR* getKey() const override { return _key; }
    const bool isLeaf() const override { return true; }
    const bool isDataNode() const override { return true; }
    
    // RTTI-free conversion
    IDataRecord* asDataRecord() noexcept override { return this; }
    const IDataRecord* asDataRecord() const noexcept override { return this; }

    long memoryUsage() const override { 
        return (_points.size()) ? (_points.size()*_points[0].size()*sizeof(double)) : 0; 
    }

    std::vector<std::vector<double>> getPoints() const {
        return _points;
    }

    friend std::ostream& operator<<(std::ostream& os, const DataRecord dr) {
        os << "This DataRecord has " << dr._points.size() << "points";
        return os;
    }

    void purge() override {
        log() << "PURGING DATA RECORD" << std::endl;
    }
    
    // NodeID accessors for persistence - alignment-safe
    void setNodeID(persist::NodeID id) noexcept { _node_id_raw = id.raw(); }
    persist::NodeID getNodeID() const noexcept { return persist::NodeID::from_raw(_node_id_raw); }
    bool hasNodeID() const noexcept { return persist::NodeID::from_raw(_node_id_raw).valid(); }
    
    // Wire format serialization for persistence layer
    size_t wire_size(unsigned short dims) const {
        // Format: keyMBR + rowid_len(2) + rowid + num_points(2) + point_data
        // ALWAYS include space for KeyMBR to maintain consistent format
        static_assert(sizeof(float)==4 && sizeof(double)==8, "wire layout assumes IEEE-754 sizes");
        
        // Wire format is defined as little-endian for portability
        // Using util::store_le16/load_le16 for endian-safe serialization
        
        size_t size = dims * 2 * sizeof(float);  // min/max for each dimension
        
        // Guard against oversized rowid
        if (_rowid.size() > 0xFFFF) {
            throw std::runtime_error("rowid too large (>65535 bytes)");
        }
        
        size += 2 + _rowid.size() + 2;  // rowid_len + rowid + num_points
        if (!_points.empty()) {
            size += _points.size() * dims * sizeof(double);
        }
        return size;
    }
    
    uint8_t* to_wire(uint8_t* out, unsigned short dims) const {
        // ALWAYS write KeyMBR data (zeros if NULL) to maintain consistent format
        if (_key) {
            out = _key->to_wire(out, dims);
        } else {
            // Write zeros if no KeyMBR
            size_t mbr_bytes = dims * 2 * sizeof(float);
            std::memset(out, 0, mbr_bytes);
            out += mbr_bytes;
        }
        
        // Write rowid (with size guard and clamping for safety)
        // Clamp to max uint16_t value to prevent silent overflow
        uint16_t rowid_len = static_cast<uint16_t>(std::min<size_t>(_rowid.size(), 0xFFFF));
        if (_rowid.size() > 0xFFFF) {
            // Log warning but continue with truncated rowid
            // In production, you might want to throw instead
            // throw std::runtime_error("rowid too large (>65535 bytes)");
        }
        util::store_le16(out, rowid_len); out += 2;
        std::memcpy(out, _rowid.data(), rowid_len); out += rowid_len;
        
        // Write points
        uint16_t num_points = _points.size();
        util::store_le16(out, num_points); out += 2;
        for (const auto& point : _points) {
            for (double coord : point) {
                std::memcpy(out, &coord, sizeof(double)); 
                out += sizeof(double);
            }
        }
        return out;
    }
    
    const uint8_t* from_wire(const uint8_t* in, unsigned short dims, unsigned short precision) {
        // Read KeyMBR data - always create if missing
        if (!_key) {
            _key = new KeyMBR(dims, precision);
        }
        in = _key->from_wire(in, dims);
        
        // Read rowid
        uint16_t rowid_len = util::load_le16(reinterpret_cast<const uint8_t*>(in)); 
        in += 2;
        _rowid.assign(reinterpret_cast<const char*>(in), rowid_len); in += rowid_len;
        
        // Read points
        uint16_t num_points = util::load_le16(reinterpret_cast<const uint8_t*>(in)); 
        in += 2;
        _points.clear();
        _points.reserve(num_points);
        
        for (uint16_t i = 0; i < num_points; ++i) {
            std::vector<double> point;
            point.reserve(dims);
            for (unsigned short d = 0; d < dims; ++d) {
                double coord;
                std::memcpy(&coord, in, sizeof(double)); 
                in += sizeof(double);
                point.push_back(coord);
            }
            _points.push_back(std::move(point));
        }
        
        return in;
    }

private:
    std::vector<std::vector<double>> _points;  // The hyper dimensional points for this record
    std::string _rowid;                        // The rowid in accumulo for which this record references
    uint64_t _node_id_raw;                     // NodeID raw value for persistence (alignment-safe)
};

/**
 * DataRecordView: Zero-copy, read-only facade over wire bytes
 * 
 * This lightweight view directly interprets mmap'd wire format data
 * without copying to heap. Used in DURABLE mode for read operations.
 * 
 * The view holds a MappingManager::Pin to keep the memory mapped
 * while the object is alive. When the view is destroyed, the Pin
 * is released and the memory can be unmapped.
 */
class alignas(8) DataRecordView final : public IRecord, public IDataRecord {
public:
    DataRecordView(persist::MappingManager::Pin pin, const uint8_t* data, size_t size,
                   uint16_t dims, uint16_t prec, persist::NodeID node_id)
        : IRecord(nullptr), // We'll create KeyMBR lazily
          pin_(std::move(pin)),
          data_(data),
          size_(size),
          dims_(dims),
          prec_(prec),
          node_id_raw_(node_id.raw()) {}
    
    ~DataRecordView() override {
        // Pin will be released automatically, unpinning the memory
        if (_key) {
            delete _key;
            _key = nullptr;
        }
    }
    
    // Make non-copyable and non-movable (std::once_flag prevents move)
    DataRecordView(const DataRecordView&) = delete;
    DataRecordView& operator=(const DataRecordView&) = delete;
    DataRecordView(DataRecordView&&) = delete;
    DataRecordView& operator=(DataRecordView&&) = delete;

    // IRecord interface implementation
    KeyMBR* getKey() const override {
        std::call_once(mbr_once_, [this]() {
            compute_layout();
            if (!layout_ok_) return;
            
            if (!_key) {
                const_cast<DataRecordView*>(this)->_key = new KeyMBR(dims_, prec_);
            }
            // Safe: KeyMBR::from_wire reads exactly dims*2*sizeof(float) bytes
            _key->from_wire(data_, dims_);
        });
        return _key;
    }
    
    const bool isLeaf() const override { return true; }
    const bool isDataNode() const override { return true; }
    
    // RTTI-free conversion
    IDataRecord* asDataRecord() noexcept override { return this; }
    const IDataRecord* asDataRecord() const noexcept override { return this; }
    
    long memoryUsage() const override { 
        // View itself is small, actual data is mmap'd
        return sizeof(*this) + (_key ? sizeof(KeyMBR) : 0);
    }

    // IDataRecord interface
    std::string_view getRowIDView() const noexcept override {
        std::call_once(layout_once_, [this]() { compute_layout(); });
        if (!layout_ok_) return {};
        return std::string_view(reinterpret_cast<const char*>(data_ + rowid_off_), rowid_len_);
    }
    // getRowID() inherits default implementation from IDataRecord
    
    // For queries that need points (expensive - parses on demand)
    std::vector<std::vector<double>> getPoints() const {
        std::call_once(points_once_, [this]() {
            compute_layout();
            if (!layout_ok_) return;
            parse_points_from_wire();
        });
        return points_;
    }
    
    // Persistence metadata - alignment-safe accessors
    persist::NodeID getNodeID() const noexcept {
        return persist::NodeID::from_raw(node_id_raw_);
    }
    bool hasNodeID() const noexcept {
        return node_id_raw_ != persist::NodeID::INVALID_RAW;
    }

    // No mutators - this is a read-only view!
    // For mutations, create a heap DataRecord, modify it, then persist

private:
    // Bounds checking helper
    bool ensure(size_t offset, size_t need) const {
        // Check: offset + need <= size_ (with overflow guard)
        return need <= size_ && offset <= size_ - need;
    }
    
    // Alternative: use end pointer for even safer bounds checking
    bool ensure_ptr(const uint8_t* ptr, size_t need) const {
        return ptr >= data_ && ptr + need <= data_ + size_;
    }
    
    // Precompute layout offsets once to avoid repeated scans
    void compute_layout() const {
        
        size_t off = 0;
        
        // MBR section
        const size_t mbr_bytes = static_cast<size_t>(dims_) * 2 * sizeof(float);
        if (!ensure(off, mbr_bytes)) { 
            layout_ok_ = false; 
            return; 
        }
        off += mbr_bytes;
        
        // Rowid section
        if (!ensure(off, 2)) { 
            layout_ok_ = false; 
            return; 
        }
        uint16_t rid_len = util::load_le16(data_ + off);
        off += 2;
        
        if (!ensure(off, rid_len)) { 
            layout_ok_ = false; 
            return; 
        }
        rowid_off_ = off;
        rowid_len_ = rid_len;
        off += rid_len;
        
        // Points section
        if (!ensure(off, 2)) { 
            layout_ok_ = false; 
            return; 
        }
        uint16_t npts = util::load_le16(data_ + off);
        off += 2;
        
        points_off_ = off;
        points_count_ = npts;
        
        const size_t pts_bytes = static_cast<size_t>(npts) * dims_ * sizeof(double);
        if (!ensure(off, pts_bytes)) { 
            layout_ok_ = false; 
            return; 
        }
        
        layout_ok_ = true;
    }
    
    void parse_points_from_wire() const {
        // Assumes compute_layout() was called and layout_ok_ is true
        const uint8_t* ptr = data_ + points_off_;
        
        points_.clear();
        points_.reserve(points_count_);
        
        for (uint16_t i = 0; i < points_count_; ++i) {
            std::vector<double> point;
            point.reserve(dims_);
            for (unsigned short d = 0; d < dims_; ++d) {
                double coord;
                std::memcpy(&coord, ptr, sizeof(double)); 
                ptr += sizeof(double);
                point.push_back(coord);
            }
            points_.push_back(std::move(point));
        }
    }

    persist::MappingManager::Pin pin_;   // Keeps memory mapped while view is alive
    const uint8_t* data_ = nullptr;      // Pointer to wire format data
    size_t size_ = 0;                    // Size of wire format data
    uint16_t dims_ = 0;                  // Number of dimensions
    uint16_t prec_ = 0;                  // Precision
    uint64_t node_id_raw_;               // This record's NodeID as raw uint64_t (alignment-safe)
    
    // Thread-safe lazy initialization flags
    mutable std::once_flag layout_once_;
    mutable std::once_flag mbr_once_;
    mutable std::once_flag points_once_;
    
    // Cached layout offsets (mutable for const methods)
    mutable bool layout_ok_ = false;
    mutable size_t rowid_off_ = 0;
    mutable uint16_t rowid_len_ = 0;
    mutable size_t points_off_ = 0;
    mutable uint16_t points_count_ = 0;
    
    // Lazily parsed fields
    mutable std::vector<std::vector<double>> points_;
};

} // namespace xtree