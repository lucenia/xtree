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
#include "./util/log.h"
#include "typemgr.h"
#include "util/float_utils.h"
#include "util/endian.hpp"  // For portable little-endian wire format
#include <limits>
#include <jni.h>

namespace xtree {

/*
#pragma pack(push, 1)
    struct KeySerializer {
        unsigned short dimension;   // 2 bytes
        unsigned short bits;        // 2 bytes
        int numBytes;               // 4 bytes
        bool dirty;                 // 1 byte
        unsigned char data[1];      // 8 bytes
      //double *box;  // volatile: not for casting
      //double *area; // volatile: not for casting

        operator KeyMBR*() {
            return new KeyMBR(dimension, bits, numBytes, data);
        }
    };
#pragma pack(pop)
*/

    class KeyMBR {
    private:
        void init() {
            this->_box = new float[this->dimension*2];
            for(unsigned short d=0; d<this->dimension*2; d+=2) {
                // Initialize with float min/max values
                _box[d] = std::numeric_limits<float>::max();    // min values start at max
                _box[d+1] = -std::numeric_limits<float>::max(); // max values start at min
            }
//            this->numBytes = (unsigned short)(ceil(((double)(bits*2.0*dimension)/8.0)));
//            this->data = NULL;
        }

    public:

        // Default constructor for placement new
        KeyMBR() : dimension(0), _box(NULL), _area(NULL), _owns_box(true) {}
        
        explicit KeyMBR(unsigned short dim, unsigned short numBits, unsigned short mbrBytes, unsigned char* mbrData) :
            dimension(dim), /*bits(numBits), numBytes((int)mbrBytes),*/
            _box(NULL), /*dirty(false), data(mbrData),*/ _area(NULL), _owns_box(true) {} //unpack(); }

        /**
         * Creates a new KeyMBR from config vals defined by
         * IndexDetails
         */
        KeyMBR(unsigned short dim, unsigned short numBits) : 
            dimension(dim), _box(NULL), /*bits(numBits), dirty(true),*/ _area(NULL), _owns_box(true) {
            init();
        };

        KeyMBR(unsigned short dim, unsigned short numBits, JNIEnv *env, jobjectArray points);

        //KeyMBR() : box(NULL), dirty(false), data(NULL) {}

        ~KeyMBR() {
            // free the box array
            free();
            // free the data array
//            delete [] data;
            if(_area) delete _area;
        }

        void free() {
            if (_owns_box && _box) {
                delete [] _box;
            }
            _box = nullptr;
        }

        void reset() {
            assert(_box!=NULL);
            // resets the box data to numeric limits
            for(unsigned short d=0; d<dimension*2; d+=2) {
                _box[d] = std::numeric_limits<float>::max();    // min values start at max
                _box[d+1] = -std::numeric_limits<float>::max(); // max values start at min
            }
//            dirty = true;
            // @TODO: don't care if data is null here.  Need some error checking though
            // in the case that someone calls reset and unpack. Result will be invalid! 
        }

        unsigned int memUsage() { return sizeof(KeyMBR) +
                                         ((_area != NULL) ? sizeof(double) : 0) +
                                         ((_box != NULL) ? (2*dimension*sizeof(float)) : 0); }

        /*~KeyMBR() {
            delete [] box;
            delete [] data;
        }*/

        //void wrapMBRData(unsigned char* mbrData) { data = mbrData; }
        void expand(const KeyMBR &mbr);
        void expandWithPoint(const vector<double> *loc);
        //void expandWithPoint(const BSONObj loc);
//        void pack();
//        void unpack();
//        unsigned* _convert();
//        void _unconvert(double &result, unsigned const val, unsigned short axis);

//        int dataSize() const { return numBytes; }
        float getBoxVal(int idx) const { return this->_box[idx]; }
        float getMin(unsigned short axis) const { return this->_box[2*axis]; }
        float getMax(unsigned short axis) const { return this->_box[(2*axis)+1]; }
        unsigned short getDimensionCount() const { return dimension; }
        
        // Methods for disk serialization/deserialization
        void serializeToSortableInts(int32_t* buffer) const {
            for(unsigned short i = 0; i < dimension*2; i++) {
                buffer[i] = floatToSortableInt(this->_box[i]);
            }
        }
        
        void deserializeFromSortableInts(const int32_t* buffer) {
            for(unsigned short i = 0; i < dimension*2; i++) {
                this->_box[i] = sortableIntToFloat(buffer[i]);
            }
            // Clear cached area since we loaded new values
            if(_area) {
                delete _area;
                _area = NULL;
            }
        }
//        unsigned short getBits() const { return bits; }
//        unsigned char * getData() const { return data; }
        long getMemoryUsed() const {
            return long(sizeof(KeyMBR) + /*numBytes +*/ dimension*2*sizeof(int32_t)); }

        /**
         * quantitative characteristics of this KeyMBR
         * These calculations are used in the Split Algorithm
         */
        double edgeDeltas() const;     // inline
        double area();        // inline
        double overlap(const KeyMBR& mbr) const;
        bool intersects(const KeyMBR& bb) const;
        bool contains(const KeyMBR& bb) const;
        
        // Fast binary comparison of MBR bounds
        bool equals(const KeyMBR& other) const {
            if (dimension != other.dimension) return false;
            if (!_box || !other._box) return _box == other._box;
            return memcmp(_box, other._box, dimension * 2 * sizeof(float)) == 0;
        }

        // Symmetric API for snapshot comparison
        bool equals(const struct KeyMBRSnapshot& snap) const noexcept;
        double percentOverlap(/*const*/ KeyMBR& mbr) {
#if		0
            log() << "OVERLAP BETWEEN: " << this->toString() << endl;
            log() << "AND: " << mbr.toString() << endl;
            double over = overlap(mbr);
            log() << "IS: " << over << endl;
            double pctover = (2.0*over)/(this->area()+mbr.area());
            log() << "PERCENT OVERLAP IS: " << pctover << endl;
#endif
            return (this->isPoint() || overlap(mbr)==0.0) ?
                    0.0 : (2.0*overlap(mbr))/(this->area()+mbr.area());
        }
        double areaEnlargement(const KeyMBR& key) const; 
        bool isPoint() const;

        string toString() const;
        friend ostream& operator<<(ostream &os, const KeyMBR& mbr);

        bool operator!=(const KeyMBR& mbr) {
/*            if(this->dirty) pack();
            bool retVal = true;
            // bitwise AND to evaluate equality
            for(unsigned short i=0; i<this->numBytes; ++i)
                retVal = retVal&&(!(data[i] ^ mbr.data[i]));
            // convert result to a boolean
            return !retVal;
*/
            assert( _box!=NULL );
            bool retVal = true;
            for(unsigned short d=0; d<dimension*2; d+=2) {
                retVal = (this->_box[d]==this->_box[d+1]);
                if(!retVal) return retVal;
            }
            return retVal;
        }

        // Copy constructor
        KeyMBR(const KeyMBR& rhs) : dimension(rhs.dimension), _box(NULL), _area(NULL), _owns_box(true) {
            if (rhs._box) {
                _box = new float[dimension * 2];
                memcpy(_box, rhs._box, dimension * 2 * sizeof(float));
            }
            if (rhs._area) {
                _area = new double(*rhs._area);
            }
        }
        
        KeyMBR& operator=(const KeyMBR& rhs) {
            if (this != &rhs) {
                // Clean up existing data
                if (_owns_box) delete[] _box;
                delete _area;
                
                // Copy dimension
                dimension = rhs.dimension;
                
                // Allocate and copy box data
                if (rhs._box) {
                    _box = new float[dimension * 2];
                    memcpy(_box, rhs._box, dimension * 2 * sizeof(float));
                    _owns_box = true;  // We allocated new memory
                } else {
                    _box = NULL;
                    _owns_box = true;
                }
                
                // Copy area if present
                if (rhs._area) {
                    _area = new double(*rhs._area);
                } else {
                    _area = NULL;
                }
            }
            return *this;
        }

    public:
        // Accessors for raw data - needed for persistence
        const float* data() const { return _box; }
        float* data() { return _box; }
        size_t data_size_bytes() const { return size_t(dimension) * 2 * sizeof(float); }
        void invalidate_area() { if (_area) { delete _area; _area = nullptr; } }
        
        /**
         * Construct KeyMBR to adopt external storage (no allocation, no free).
         * Used when KeyMBR is co-located with XTreeBucket in persistent storage.
         * Layout: [KeyMBR object][float[2*dims]] contiguously
         */
        static KeyMBR* construct_external(void* place, unsigned short dims,
                                         const float* interleaved_or_null) {
            // Placement-new the KeyMBR object itself at `place`
            KeyMBR* k = ::new (place) KeyMBR();
            k->dimension = dims;
            k->_owns_box = false;  // Don't free on destruction
            k->_area = nullptr;
            
            // Place the float buffer right after the KeyMBR object
            auto* buf = reinterpret_cast<float*>(
                reinterpret_cast<uint8_t*>(place) + sizeof(KeyMBR));
            k->_box = buf;
            
            // Initialize contents
            if (interleaved_or_null) {
                // data is [min0,max0,min1,max1,...]
                std::memcpy(buf, interleaved_or_null, sizeof(float) * dims * 2);
            } else {
                // default infinities (for new empty MBRs)
                for (unsigned short d = 0; d < dims; ++d) {
                    buf[2*d]   =  std::numeric_limits<float>::max();
                    buf[2*d+1] = -std::numeric_limits<float>::max();
                }
            }
            return k;
        }
        
        // Copy helper used by split paths etc.
        void copy_from(const KeyMBR& src) {
            assert(_box && src._box);
            assert(dimension == src.dimension);
            std::memcpy(_box, src._box, sizeof(float) * dimension * 2);
            invalidate_area();
        }
        
        // Wire serialization helpers
        void set_from_interleaved(const float* f, unsigned short dims) {
            // Ensure storage exists and dimensions match
            if (_box == nullptr || dimension != dims) {
                if (_owns_box) delete[] _box;
                dimension = dims;
                _box = new float[2 * dims];
                _owns_box = true;
            }
            std::memcpy(_box, f, sizeof(float) * 2 * dims);
            invalidate_area();
        }
        
        size_t wire_size(unsigned short dims) const {
            return sizeof(float) * 2 * dims;
        }

        // Debug: Check if _area is valid (NULL or a valid heap address)
        bool debug_check_area() const {
            if (_area == nullptr) return true;
            uintptr_t area_val = reinterpret_cast<uintptr_t>(_area);
            return area_val >= 0x1000;  // Valid heap address should be >= 4KB
        }

        // Debug: Get _area value for diagnostics
        uintptr_t debug_area_value() const {
            return reinterpret_cast<uintptr_t>(_area);
        }
        
        // Serialize this KeyMBR into 'out'. Caller guarantees capacity.
        // Returns pointer advanced past written bytes.
        // Serialize this KeyMBR into 'out'. Caller guarantees capacity.
        // Returns pointer advanced past written bytes.
        // Wire format is always little-endian for portability.
        uint8_t* to_wire(uint8_t* out, unsigned short dims) const {
            // Write each float in little-endian format
            for (unsigned short i = 0; i < dims * 2; ++i) {
                xtree::util::store_lef32(out, _box[i]);
                out += sizeof(float);
            }
            return out;
        }
        
        // Deserialize from 'in' into this KeyMBR. Returns pointer advanced past read bytes.
        // Wire format is always little-endian for portability.
        const uint8_t* from_wire(const uint8_t* in, unsigned short dims) {
            // Ensure storage exists
            if (_box == nullptr || dimension != dims) {
                if (_owns_box) delete[] _box;
                dimension = dims;
                _box = new float[2 * dims];
                _owns_box = true;
            }
            // Read each float in little-endian format
            for (unsigned short i = 0; i < dims * 2; ++i) {
                _box[i] = xtree::util::load_lef32(in);
                in += sizeof(float);
            }
            invalidate_area();
            return in;
        }
        
        // Set a single dimension's min/max pair directly
        void set_pair(unsigned short dim, float mn, float mx) {
            assert(_box);
            _box[2*dim] = mn;
            _box[2*dim+1] = mx;
            invalidate_area();
        }
        
    protected:

//        // copy packed mbr data from one data array to another
//        void _copy( char * dst , const char * src ) {
//            for ( int d=0; d<numBytes; d++ ) {
//                dst[d] = src[d];
//            }
//        }

        unsigned short dimension;   // 2 bytes
//        unsigned short bits;        // 2 bytes
//        int numBytes;               // 4 bytes
        //double *box;                // 8 bytes (should be null when written to disk)
        float *_box;  // Stores coordinates as floats in memory
//        bool dirty;                 // 1 byte
//        unsigned char* data;        // 8 bytes

        // characteristics
        double* _area;              // 8 bytes
        bool _owns_box = true;      // Whether we own the _box allocation
    };

    // Lightweight immutable snapshot of a KeyMBR for change detection
    struct KeyMBRSnapshot {
        unsigned short dimension = 0;
        std::vector<float> bounds; // [min0, max0, min1, max1, ...]

        explicit KeyMBRSnapshot(const KeyMBR& mbr) noexcept
            : dimension(mbr.getDimensionCount()),
              bounds(dimension * 2) {
            if (dimension > 0 && mbr.data()) {
                memcpy(bounds.data(), mbr.data(), dimension * 2 * sizeof(float));
            }
        }

        bool equals(const KeyMBR& other) const noexcept {
            if (dimension != other.getDimensionCount()) return false;
            if (dimension == 0) return true;
            return memcmp(bounds.data(), other.data(), dimension * 2 * sizeof(float)) == 0;
        }
    };

    /**
     * Calculates the sum of all deltas between edges
     */
    inline double KeyMBR::edgeDeltas() const {
        assert(_box != NULL);
        double distance = 0;
        for (unsigned short d = 0; d < dimension*2; d+=2) {
            distance += this->_box[d+1] - this->_box[d];
        }
  
        return distance;
    }
  
    /** 
     * Calculates the area of the MBR
     */
    inline double KeyMBR::area() /*const*/ {
        if(_area != NULL) return *_area;
        else {
            assert(_box != NULL);
            double area = 1.0;
            
            // Unroll common 2D case for performance
            if (dimension == 2) {
                double width = this->_box[1] - this->_box[0];
                double height = this->_box[3] - this->_box[2];
                area = width * height;
            } else {
                for (unsigned short d = 0; d < dimension*2; d+=2) {
                    area *= (double)(this->_box[d+1] - this->_box[d]);
                }
            }
            
            _area = new double(area);
        }
        return *_area;
    }

    inline bool KeyMBR::isPoint() const {
//        if(this->dirty) pack();
        assert(_box != NULL);
        bool retVal = true;
        for(unsigned short d=0; d<dimension*2; d+=2)
            if(retVal==true)
                retVal = retVal&&(this->_box[d]==this->_box[d+1]);
            else
                break;
        return retVal;
    }

    // Implementation of KeyMBR::equals(const KeyMBRSnapshot&)
    inline bool KeyMBR::equals(const KeyMBRSnapshot& snap) const noexcept {
        return snap.equals(*this);
    }

}
