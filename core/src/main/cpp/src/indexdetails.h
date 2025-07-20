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
#include "uniqueid.h"
#include "lru.hpp"

namespace xtree {

    // forward declarations
    class IRecord;

    template< class RecordType >
    class Iterator;

    template< class Record >
    class IndexDetails {
    public:

        IndexDetails( const unsigned short dimension, const unsigned short precision,
                      vector<const char*> *dimLabels, long xtMaxMem, JNIEnv* env, jobject* xtPOJO ) :
            _xtPOJO(xtPOJO), _dimension(dimension), _dimensionLabels(dimLabels),
            _precision(precision)/*, _cache(getAvailableSystemMemory()*/ /*/IndexDetails<Record>::indexes.size())*/,
            _nodeCount(0) {
//            cout << "CACHE SIZE: " << _cache.getMaxMemory() << endl;

            // retain a handle to the jvm (used for java callbacks)
            if(!IndexDetails<Record>::jvm)
                IndexDetails<Record>::jvm = env;

            IndexDetails<Record>::indexes.push_back(this);

            /** TODO: Working on dynamically setting the LRU memory space... this will use mlock */
//          cout << "xtMaxMem: " << xtMaxMem << endl;

            // update cache size for each index
   //         for_each(IndexDetails<Record>::indexes.begin(),
   //                  IndexDetails<Record>::indexes.end(),
   //                  [&](IndexDetails<Record>* idx){ /*idx.updateMaxMemory();*/ });
            //_cache.updateMaxMemory(getAvailableSystemMemory);
        }

        /**
         * explicit copy constructor
         */
        IndexDetails( const IndexDetails& idx) :
            _dimension(idx.getDimensionCount()), _precision(idx.getPrecision()) {}

        /**
         * Entry point for deleting an XTree index
         */
        ~IndexDetails() {
            // Don't delete _dimensionLabels here as it's managed by the caller
            // The caller is responsible for the lifecycle of dimension labels
        }

        unsigned short getDimensionCount() const {
            return this->_dimension;
        }

        unsigned short getDimensionIdx(string label ) const {
            // assert dimension label vector is not null
            assert(_dimensionLabels);
            // assert a label was passed in and not a null pointer
            assert(label.empty() == false);
            vector<const char*>::iterator it = find(_dimensionLabels->begin(), _dimensionLabels->end(), label.c_str());
            // assert the label exists in the iterator
            assert(it != _dimensionLabels->end());
            // return the index of the dimension label
            return (short) distance(_dimensionLabels->begin(), it);
        }

        unsigned short getPrecision() const {
            return this->_precision;
        }

        long getRootAddress() const {
            return this->_rootAddress;
        }

        void setRootAddress(const long rootAddress) {
            this->_rootAddress = rootAddress;
        }


        static LRUCache<IRecord, UniqueId, LRUDeleteObject>& getCache() { return cache; }

        void updateDetails(unsigned short precision,
                      vector<const char*> *dimLabels) {
        	// Set precision
        	_precision = precision;

        	// Update labels
        	// Note: We don't delete the old labels here because IndexDetails
        	// doesn't own the dimension labels - they're managed by the caller
        	_dimensionLabels = dimLabels;

        	// If # dimensions is same, do not modify tree
        	// Iff # dimensions has changed, rebuild tree somehow

        }

        const UniqueId getNextNodeID() { return ++_nodeCount; }

//        IRecord* getCachedNode( UniqueId recordAddress ) { return NULL; }

        /**
         * Serializes an IndexDetail for persistence in HDFS
         **/
        template <class Archive>
        void serialize( Archive & ar ) const {
            ar( _dimension, _dimensionLabels, _rootAddress, _nodeCount );
        }

    protected:

    private:
        static JNIEnv *jvm;
        static vector<IndexDetails<Record>*> indexes;
        static LRUCache<IRecord, UniqueId, LRUDeleteObject> cache;

        jobject* _xtPOJO;
        unsigned short _dimension;                              // 2 bytes
        vector<const char*> *_dimensionLabels;
        unsigned short _precision;                              // 2 bytes
        long _rootAddress;                                      // 8 bytes
        // create the cache for the tree
//        LRUCache<IRecord, UniqueId, LRUDeleteObject> _cache;    // 48 bytes
        UniqueId _nodeCount;                                    // 8 bytes
        vector<Iterator<Record>*> *_iterators;
    }; // TOTAL: 68 bytes
}
