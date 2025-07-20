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

#include "keymbr.h"


namespace xtree {

    KeyMBR::KeyMBR(unsigned short dim, unsigned short numBits, JNIEnv *env, jobjectArray points) :
        dimension(dim), /*bits(numBits), dirty(true),*/ _area(NULL) {
        init();
        // loop through each point and add it to the KeyMBR
        int numPts = env->GetArrayLength(points);

        // allocate the vector on the heap (man I hate new)
        // perhaps a vector here is overkill, probably could use a 2d array instead
        vector<double> *loc = new vector<double>();

        for(int p=0; p<numPts; ++p) {
            // get each point
            jdoubleArray curPnt =  (jdoubleArray)(env->GetObjectArrayElement(points, p));
            // get each value out of the current point
            jdouble *axVal = env->GetDoubleArrayElements(curPnt, 0);
            for(short axis=0; axis<dim; ++axis) {
                loc->push_back(axVal[axis]);
            }
            this->expandWithPoint(loc);
            loc->clear();
        }

        // call pack
//        this->pack();

        // delete the loc
        delete loc;
    }

    /** 
     * expands the MBR given the input point
     * which is modeled as a BSONObject containing 
     * values for each axis
     */
    void KeyMBR::expandWithPoint(const vector<double>/*const BSONObj*/ *loc) {
        const double* data = loc->data(); // Direct pointer access
        
        // Unroll common 2D case
        if (dimension == 2) {
            int32_t x = floatToSortableInt((float)data[0]);
            int32_t y = floatToSortableInt((float)data[1]);
            this->_box[0] = MIN(this->_box[0], x);
            this->_box[1] = MAX(this->_box[1], x);
            this->_box[2] = MIN(this->_box[2], y);
            this->_box[3] = MAX(this->_box[3], y);
        } else {
            // General case
            for(unsigned short d=0; d<dimension; d++) {
                int32_t sortableValue = floatToSortableInt((float)data[d]);
                unsigned short idx = d*2;
                this->_box[idx] = MIN(this->_box[idx], sortableValue);
                this->_box[idx+1] = MAX(this->_box[idx+1], sortableValue);
            }
        }
        // Clear cached area
        if(_area) {
            delete _area;
            _area = NULL;
        }
    }

    /**
     * Expands this MBR given a child MBR (leaf or internal node)
     */
    void KeyMBR::expand(const KeyMBR &mbr) {
        // Unroll common 2D case
        if (dimension == 2) {
            // Direct memory access for performance
            this->_box[0] = MIN(this->_box[0], mbr._box[0]);
            this->_box[1] = MAX(this->_box[1], mbr._box[1]);
            this->_box[2] = MIN(this->_box[2], mbr._box[2]);
            this->_box[3] = MAX(this->_box[3], mbr._box[3]);
        } else {
            // General case
            for(unsigned short d=0; d<dimension*2; d+=2) {
                this->_box[d] = MIN(this->_box[d], mbr._box[d]);
                this->_box[d+1] = MAX(this->_box[d+1], mbr._box[d+1]);
            }
        }
        // Clear cached area
        if(_area) {
            delete _area;
            _area = NULL;
        }
    }

    /**
     * Packs the values into a byte buffer based on bitsize and dimension
     */
//    void KeyMBR::pack() {
//        unsigned *trans = _convert();
//
//        // bit pack logic
//        unsigned short numBytes = ceil((double)(dimension*bits*2.0/8.0));
//        if(data==NULL) data = new unsigned char[numBytes];
//        // sets the byte array to all 0's
//        for(unsigned short i=0; i<numBytes; i++) data[i] = 0x00;
//
//        unsigned msb = 0;
//        // loop through mbr values
//        for(unsigned short d = 0; d<dimension*2; d+=2) {
//            for(unsigned short i=0; i<2; i++) {
//                unsigned short dVal = d+i;
//                unsigned trVal = trans[dVal];
//                unsigned *dataVal = (reinterpret_cast<unsigned int *>(data+((unsigned short)(bits*dVal/8))));
//
//                unsigned offs = (unsigned)((bits*dVal)&7);  // same as (bits*dVal)%8; but faster than mod op
//                if(offs) {
//                    // now, we don't want to lose this iteration MSB data, it becomes LSB in the next byte
//                    // mask the important bits, right shift remaining to store the msb data
//                    unsigned mask = ((0x1<<offs)-1)<<(32-offs);
//                    msb = (trVal&mask)>>(32-offs);
//
//                    // store in overflow byte
//                    unsigned char *overflow = (reinterpret_cast<unsigned char*>(dataVal+1));
//                    *overflow = *overflow | msb;
//
//                    // left shift the number value
//                    trVal = trVal << offs;
//                }
//                *dataVal |= trVal;
//            }
//        }
//        dirty = false;
//        delete[] trans;
//    }


    /**
     * This method restores the MBR by decoding the binary 
     * data in sorted in the data buffer
     */ 
//    void KeyMBR::unpack() {
//        // ensure there is data to unpack
//    //    uassert( 18128 , "KeyMBR::unpack() - no data to unpack", data!=NULL );
//
//        // ensure we have space for the unpacked data
//        /** @TODO: MAKE SURE THIS IS CLEANED UP IN A DESTRUCTOR */
//        if(box==NULL) box = new double[dimension*2];
//
//        // the mask the pull the data
//        unsigned mask = (((unsigned long)0x1)<<bits)-1;
//        unsigned overflowVal = 0;
//
//        for(unsigned short d=0; d<dimension*2; d+=2) {
//            for(unsigned short i=0; i<2; i++) {
//                unsigned short dVal = d+i;
//                unsigned char* dataPtr = (data+((unsigned short)bits*dVal/8));
//                unsigned dataVal = *(reinterpret_cast<unsigned int*>(dataPtr));
//                unsigned offs = (unsigned)((bits*dVal)&7);// same as (bits*dVal)%8; but faster that mod op
//                if(offs) {
//                    // shift the number right
//                    dataVal = dataVal>>offs;
//
//                    // pull out "overflow"
//                    int bitsOver = (bits+offs)-32;
//                    if(bitsOver>0) {
//                        unsigned char overflow = *(dataPtr+4);
//                        overflowVal = ((unsigned)overflow) << (32-offs);
//                        dataVal = dataVal | overflowVal;
//                    }
//                }
//
//                // pull out the 'bits' number of bits
//                dataVal&=mask;
//                _unconvert(box[d+i], dataVal, d/2);
//            }
//        }
//    }

    /**
     * Converts the data from signed to unsigned values
     */
//    unsigned* KeyMBR::_convert() {
//        unsigned *trans = new unsigned[dimension*2];
//        uint64_t numBuckets = 0x1ULL << bits;
//        double scaling = 0;
//        double minAxVal = 0;
//
//        for(unsigned short d=0; d<dimension*2; d+=2) {
//            switch(d/2) {
//            case 0: // lon (x)
//            case 1: // lat (y)
//            {   minAxVal = -180;
//                scaling = (double)(numBuckets/360.0);
//                break;
//            }
//            case 2: // alt (z)
//            {   minAxVal = -6400; //rounded radius of spherical earth (km)
//                scaling = (double)(numBuckets/(356400.0)); // ~distance to moon...alice (km)
//                break;
//            }
//            default:
//                scaling = (double)(numBuckets/((uint64_t)0x1ULL<<(bits)));
//            }
//            double translatedA = (double)(box[d]-minAxVal);
//            double translatedB = (double)(box[d+1]-minAxVal);
//            trans[d] = translatedA*scaling;
//            trans[d+1] = translatedB*scaling;
//        }
//
//        return trans;
//    }

    /**
     *  Converts the data from unsigned back to signed values
     */
//    void KeyMBR::_unconvert(double &result, unsigned const val, unsigned short axis) {
//        unsigned long long numBuckets = 0x1ULL << bits;
//        double scaling = 0;
//        double minAxVal = 0;
//        switch(axis) {
//        case 0:
//        case 1:
//        {   minAxVal = -180.0;
//            scaling = (double)(numBuckets/360.0);
//            break;
//        }
//        case 2: // alt
//        {   minAxVal = -6400.0;
//            scaling = (double)(numBuckets/(356400.0));
//            break;
//        }
//        default:
//            scaling = (double)(numBuckets/((uint64_t)(0x1Ull<<bits)));
//        }
//        result = (double)((val/scaling)+minAxVal);
//    }

    double KeyMBR::overlap(const KeyMBR& bb) const {
        double area = -1.0;
        for( unsigned short d=0; d<dimension*2; d+=2 ) {
            // Convert back to float for area calculation
            float thisMin = sortableIntToFloat(this->_box[d]);
            float thisMax = sortableIntToFloat(this->_box[d+1]);
            float bbMin = bb.getMin(d/2);
            float bbMax = bb.getMax(d/2);
            area = abs(area)*MAX(0.0f, (MIN(thisMax, bbMax) - MAX(thisMin, bbMin)));
        }
        if(area < 0) area=0.0;
        return area;
    }

    bool KeyMBR::intersects(const KeyMBR& bb) const {
        // Optimize for common 2D case
        if (dimension == 2 && !this->isPoint()) {
            // Direct integer comparison with unrolled loop
            return !(this->_box[1] < bb._box[0] ||  // this.maxX < bb.minX
                     bb._box[1] < this->_box[0] ||   // bb.maxX < this.minX
                     this->_box[3] < bb._box[2] ||   // this.maxY < bb.minY
                     bb._box[3] < this->_box[2]);    // bb.maxY < this.minY
        }
        
        if(this->isPoint()) {
            // check that each dimension is contained
            for( unsigned short d=0; d<bb.getDimensionCount()*2; d+=2 ) {
                // Direct integer comparison - no epsilon needed
                if( !((this->_box[d]>=bb._box[d]) &&
                    (this->_box[d+1]<=bb._box[d+1])) ) {
                    return false;
                }
            }
        } else {
            // check that the hyperpoly intersects
            for( unsigned short d=0; d<bb.getDimensionCount()*2; d+=2 ) {
                // Direct integer comparison - if max < min, no overlap
                if(this->_box[d+1] < bb._box[d] || 
                   bb._box[d+1] < this->_box[d]) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Calculate the area the keynode will need to be enlarged
     * to accomodate the provided key.
     */
     double KeyMBR::areaEnlargement(const KeyMBR& key) const {
        double areaOrig = -1.0;
        //if(areaOrig!=-1.0) areaOrig = -1.0;
        double areaNew = -1.0;
        double M = 0.0; 
        double m = 0.0;
        for(unsigned short d=0; d<dimension*2; d+=2) {
            areaOrig = abs(areaOrig*abs(this->_box[d+1] - this->_box[d]));
            if(areaOrig == 0)
                break;
            else {
                m = MIN(this->_box[d], key.getBoxVal(d));
                M = MAX(this->_box[d+1], key.getBoxVal(d+1));
                areaNew = abs(areaNew*abs(M-m));
            }
        }

        return areaNew - areaOrig;
    }

    /**
     *  Output stream operator for debugging
     */
    ostream& operator<<( ostream& os, const KeyMBR& mbr) {
        os.precision(10);
        os << mbr.toString() << endl;
        return os;
    }

    string KeyMBR::toString() const {
    	ostringstream oss;
        oss << "[";

        for(unsigned short d=0; d<this->getDimensionCount()*2; d+=2) {
            oss << "(" << this->getBoxVal((int)d) << ", " << this->getBoxVal((int)d+1) << ") ";
        }
        oss << "]";
        return oss.str();
    }
}
