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

    public:
        Iterator(CacheNode* startNode, IRecord* searchKey, SearchType searchType) :
            _startNode(startNode),
            _searchKey(searchKey),
            _searchType(searchType),
            _hasNext(true),
            _invalidated(false),
            _traversalOrder(NULL) {
            _init();
        }

        friend ostream& operator<<(ostream& os, const Iterator iter) {
            os << "Printing iterator";
            return os;
        }

        DataRecord* next() {
            // this approach removes each element from the vector one-by-one
            DataRecord* ret = NULL;
            if(_recordQueue.size() > 0) {
                IRecord* record = _recordQueue.at(0)->object;
                _recordQueue.erase(_recordQueue.begin());
                ret = reinterpret_cast<DataRecord*>(record);
                // if we're past the end of the page
                if(_hasNext && _recordQueue.size()==1)
                    // get the next page of results
                    _init();
            }
            return ret;
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
        vector<CacheNode*> _recordQueue;    // holds matching records
        bool _hasNext;
        bool _invalidated;
        void* _traversalOrder;              // void pointers aren't really smart, as
                                            // you can guarantee proper destruction
                                            // of the object pointed to
    };
}
