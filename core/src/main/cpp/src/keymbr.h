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

#include "pch.h"
#include "./util/log.h"
#include "typemgr.h"
#include <jni.h>

namespace xtree {

 /*   struct KeyMem {
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

    };*/

    class KeyMBR {
    private:
        void init() {
            this->_box = new float[this->dimension*2];
            for(unsigned short d=0; d<this->dimension*2; d+=2) {
                _box[d] = numeric_limits<float>::max();//(1<<this->bits)-1;
                _box[d+1] = -(numeric_limits<float>::max());//-((1<<this->bits)-1);
            }
//            this->numBytes = (unsigned short)(ceil(((double)(bits*2.0*dimension)/8.0)));
//            this->data = NULL;
        }

    public:

        explicit KeyMBR(unsigned short dim, unsigned short numBits, unsigned short mbrBytes, unsigned char* mbrData) :
            dimension(dim), /*bits(numBits), numBytes((int)mbrBytes),*/
            _box(NULL), /*dirty(false), data(mbrData),*/ _area(NULL) {} //unpack(); }

        /**
         * Creates a new KeyMBR from config vals defined by
         * IndexDetails
         */
        KeyMBR(unsigned short dim, unsigned short numBits) : 
            dimension(dim), _box(NULL), /*bits(numBits), dirty(true),*/ _area(NULL) {
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
            delete [] _box;
        }

        void reset() {
            assert(_box!=NULL);
            // resets the box data to numeric limits of the bitsize
            for(unsigned short d=0; d<dimension*2; d+=2) {
                _box[d] = numeric_limits<float>::max(); //(1<<bits)-1;
                _box[d+1] = -numeric_limits<float>::max(); //-((1<<bits)-1);
            }
//            dirty = true;
            // @TODO: don't care if data is null here.  Need some error checking though
            // in the case that someone calls reset and unpack. Result will be invalid! 
        }

        unsigned int memUsage() { return sizeof(KeyMBR) +
                                         (_area != NULL) ? sizeof(double) : 0 +
                                         (_box != NULL) ? (2*dimension*sizeof(float)) : 0; }

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
//        unsigned short getBits() const { return bits; }
//        unsigned char * getData() const { return data; }
        long getMemoryUsed() const {
            return long(sizeof(KeyMBR) + /*numBytes +*/ dimension*2.0*sizeof(float/*double*/)); }

        /**
         * quantitative characteristics of this KeyMBR
         * These calculations are used in the Split Algorithm
         */
        double edgeDeltas() const;     // inline
        double area();        // inline
        double overlap(const KeyMBR& mbr) const;
        bool intersects(const KeyMBR& bb) const;
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

        KeyMBR& operator=(const KeyMBR& rhs) {
            char* boxData = reinterpret_cast<char*>(_box);
            memcpy(boxData, rhs._box, dimension*2*sizeof(float));
            return *this;
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
        float *_box;
//        bool dirty;                 // 1 byte
//        unsigned char* data;        // 8 bytes

        // characteristics
        double* _area;              // 8 bytes
    };

    /**
     * Calculates the sum of all deltas between edges
     */
    inline double KeyMBR::edgeDeltas() const {
        assert(_box != NULL);
        double distance = 0;
        for (unsigned short d = 0; d < dimension*2; d+=2)
            distance += this->_box[d+1] - this->_box[d];
  
        return distance;
    }
  
    /** 
     * Calculates the area of the MBR
     */
    inline double KeyMBR::area() /*const*/ {
        if(_area != NULL) return *_area;
        else {
            assert(_box != NULL);
//            double area = 1;
            _area = new double(1.0);
            for (unsigned short d = 0; d < dimension*2; d+=2)
                *_area *= (double)(this->_box[d+1] - this->_box[d]);
//                area *= (double)(box[d+1] - box[d]);
        }
//        cout << "RETURNING AREA: " << *_area << endl;
        return *_area;
//        return area;
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


}
