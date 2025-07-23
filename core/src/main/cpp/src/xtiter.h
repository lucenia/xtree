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

#include <iostream>
#include <memory>
#include <deque>
#include <string_view>
#include "datarecord.hpp"  // For IDataRecord interface

namespace xtree {

    /**
     * Iterator
     */
    template< class RecordType >
    class Iterator {

    typedef Iterator<RecordType>                                 _SelfType;
    typedef typename xtree::XTreeBucket<RecordType>::CacheNode   CacheNode;
    typedef typename xtree::XTreeBucket<RecordType>::_MBRKeyNode MBRKeyNode;
    typedef typename xtree::XTreeBucket<RecordType>::DFS         DFS;
    typedef typename xtree::XTreeBucket<RecordType>::BFS         BFS;
    
    // Queue item that carries enough info to resolve records
    struct QueueItem {
        CacheNode* cn;      // may be null for ephemeral durable records
        MBRKeyNode* kn;     // never null - holds NodeID and can resolve
    };

    public:
        Iterator(CacheNode* startNode, IRecord* searchKey, SearchType searchType, 
                 IndexDetails<RecordType>* idx = nullptr) :
            _startNode(startNode),
            _searchKey(searchKey),
            _searchType(searchType),
            _hasNext(true),
            _invalidated(false),
            _traversalOrder(NULL),
            _idx(idx) {
            _init();
        }

        friend ostream& operator<<(ostream& os, const Iterator iter) {
            os << "Printing iterator";
            return os;
        }

        /**
         * Get the next record from the iterator.
         * 
         * IMPORTANT: The returned pointer is valid ONLY until the next call to next()
         * or until the iterator is destroyed. For DURABLE mode, this may return an
         * ephemeral DataRecordView that is owned by the iterator.
         * 
         * @return IRecord* pointer to the next record, or nullptr if no more records
         */
        inline IRecord* next() {
            while (!_recordQueue.empty()) {
                QueueItem& qi = _recordQueue.front();
                
                IRecord* rec = nullptr;
                
                // Fast path: cached/in-memory object exists
                if (qi.cn && qi.cn->object) {
                    // Drop any previous ephemeral; we're returning a cached object
                    owned_ephemeral_.reset();
                    rec = qi.cn->object;
                }
                // Slow path: resolve via cache_or_load (DURABLE mode)
                else if (qi.kn && _idx) {
                    // Production path: cache_or_load returns cached or loads from persistence
                    // This may return a DataRecordView (zero-copy) or DataRecord (heap)
                    auto* cn = qi.kn->template cache_or_load<RecordType>(_idx);
                    if (cn && cn->object) {
                        // Drop any previous ephemeral - we're using cached object
                        owned_ephemeral_.reset();
                        rec = cn->object;
                    }
#ifndef NDEBUG
                    else {
                        // Debug: log failed loads
                        trace() << "[ITER_LOAD_FAIL] Failed to load DataRecord NodeID "
                                  << (qi.kn->hasNodeID() ? qi.kn->getNodeID().raw() : 0)
                                  << " (hasNodeID=" << qi.kn->hasNodeID() << ")" << std::endl;
                    }
#endif
                }
                
                // Consume this queue item
                _recordQueue.pop_front();
                
                if (rec) {
                    // Prefetch next page when we're almost empty
                    if (_hasNext && _recordQueue.size() == 1) {
                        _init();  // appends to _recordQueue
                    }
                    return rec;
                }
                
                // Couldn't resolve -> try to refill if running out
                if (_hasNext && _recordQueue.size() == 1) {
                    _init();
                }
                // Loop continues to next item
            }
            return nullptr;
        }

        /**
         * Get the next data record, skipping any non-data nodes.
         * @return IDataRecord* pointer to the next data record, or nullptr if none
         */
        inline IDataRecord* nextData() {
            while (IRecord* r = next()) {
                if (auto* p = r->asDataRecord()) {
                    return p;  // O(1) virtual call, no RTTI
                }
                // Continue looping to skip non-data nodes
            }
            return nullptr;  // exhausted
        }
        
        /**
         * Get the row ID of the next data record without exposing the record object.
         * @param out string_view that will be set to the row ID
         * @return true if a row ID was fetched, false if no more records
         */
        inline bool nextRowID(std::string_view& out) {
            if (auto* d = nextData()) {
                out = d->getRowIDView();
                return true;
            }
            return false;
        }

        bool hasNext() {
            return _hasNext || _recordQueue.size() > 0;
        }

        void invalidate() { this->_invalidated = true; }

    protected:

        //////////////////////////
        //    Query Methods     //
        //////////////////////////
        bool intersects(CacheNode* nodeHandle, ...);
        bool contains(CacheNode* nodeHandle, ...);

        template< typename TraversalOrder >
        void traverse( CacheNode* nodeHandle, bool(xtree::Iterator<RecordType>::* visit)(CacheNode*, ...) );

    private:
        void _init() {
            bool (Iterator<RecordType>::* visit)(CacheNode*, ...);
            switch(_searchType) {
            case CONTAINS:
                visit = &xtree::Iterator<RecordType>::contains;
            break;
            case INTERSECTS:
                // call the traverse method with (return type, visit function, traversalType)
                // traverse will fill the _recordQueue first with cached items that match the target key
//                traverse<uint64_t, decltype(visit), BFS>( visit )
              //  this =
              visit = &xtree::Iterator<RecordType>::intersects;
            break;
            default:
                assert(0);
            break;
            }

            // traverse the tree
            (*this).template traverse<DFS>(_startNode, visit);
        }

//        string toJSON(/*DataRecord* record*/) {
//           // DataRecord* dr = (DataRecord*)record;
//
//            // build the json
//            /** TODO: signature should eventually move to IRecord and implemented in its
//                derived children - This is quick and dirty due to time crunch */
//            mObject recObj;
//            recObj["type"] = "Feature";
//            recObj["coordinates"] = dr->getJSONPoints();
//
//            ostringstream oss;
//            write_stream(mValue(recObj), oss, pretty_print );
//
//            return oss.str();
//        }

        CacheNode* _startNode;
        IRecord* _searchKey;
        SearchType _searchType;
        std::deque<QueueItem> _recordQueue;     // O(1) pop_front() for efficient queue ops
        std::unique_ptr<IRecord> owned_ephemeral_;  // Owns at most one ephemeral view
        IndexDetails<RecordType>* _idx;         // Needed to resolve DURABLE records
        bool _hasNext;
        bool _invalidated;
        void* _traversalOrder;                  // void pointers aren't really smart, as
                                                // you can guarantee proper destruction
                                                // of the object pointed to
    };
}
