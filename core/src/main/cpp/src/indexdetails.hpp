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
#include "uniqueid.h"
#include "lru.hpp"
#include "memmgr/cow_memmgr.hpp"
#include "cow_allocator.hpp"

namespace xtree {

    // forward declarations
    class IRecord;

    template< class RecordType >
    class Iterator;

    template< class Record >
    class IndexDetails {
    public:

        IndexDetails( const unsigned short dimension, const unsigned short precision,
                      vector<const char*> *dimLabels, long xtMaxMem, JNIEnv* env, jobject* xtPOJO,
                      bool use_cow = false, const std::string& snapshot_file = "xtree.snapshot" ) :
            _xtPOJO(xtPOJO), _dimension(dimension), _dimensionLabels(dimLabels),
            _precision(precision)/*, _cache(getAvailableSystemMemory()*/ /*/IndexDetails<Record>::indexes.size())*/,
            _nodeCount(0), cow_manager_(nullptr), allocator_(nullptr) {
//            cout << "CACHE SIZE: " << _cache.getMaxMemory() << endl;

            // retain a handle to the jvm (used for java callbacks)
            if(!IndexDetails<Record>::jvm)
                IndexDetails<Record>::jvm = env;

            IndexDetails<Record>::indexes.push_back(this);

            /** TODO: Working on dynamically setting the LRU memory space... this will use mlock */
//          cout << "xtMaxMem: " << xtMaxMem << endl;

            // Initialize COW manager if requested
            if (use_cow) {
                cow_manager_ = new DirectMemoryCOWManager<Record>(this, snapshot_file);
                allocator_ = new COWXTreeAllocator<Record>(cow_manager_);
            }

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
            
            // Clean up COW resources
            delete allocator_;
            delete cow_manager_;
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


        static LRUCache<IRecord, UniqueId, LRUDeleteNone>& getCache() { return cache; }
        
        // Clear the cache - useful for test cleanup to prevent memory leaks
        // When using LRUDeleteNone, this will only delete cache nodes, not the cached objects
        static void clearCache() { cache.clear(); }

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

        // COW management methods
        bool hasCOWManager() const { return cow_manager_ != nullptr; }
        
        DirectMemoryCOWManager<Record>* getCOWManager() { 
            return cow_manager_; 
        }
        
        COWXTreeAllocator<Record>* getCOWAllocator() { 
            return allocator_; 
        }
        
        // Helper method to record write operations for COW tracking
        void recordWrite(void* ptr) {
            if (cow_manager_) {
                cow_manager_->record_operation_with_write(ptr);
            }
        }
        
        // Helper method to record any operation for COW tracking  
        void recordOperation() {
            if (cow_manager_) {
                cow_manager_->record_operation();
            }
        }

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
        static LRUCache<IRecord, UniqueId, LRUDeleteNone> cache;
        
        // COW memory management (optional)
        DirectMemoryCOWManager<Record>* cow_manager_;
        COWXTreeAllocator<Record>* allocator_;

        jobject* _xtPOJO;
        unsigned short _dimension;                              // 2 bytes
        vector<const char*> *_dimensionLabels;
        unsigned short _precision;                              // 2 bytes
        long _rootAddress;                                      // 8 bytes
        // create the cache for the tree
//        LRUCache<IRecord, UniqueId, LRUDeleteObject> _cache;    // 48 bytes
        UniqueId _nodeCount;                                    // 8 bytes
        
        // Allow factory to initialize static members
        template<typename R> friend class MMapXTreeFactory;
        vector<Iterator<Record>*> *_iterators;
    }; // TOTAL: 68 bytes
}
