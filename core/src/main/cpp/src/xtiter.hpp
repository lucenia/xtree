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

#include "xtiter.h"

namespace xtree {

    /**
     * Intersect search - In this context, the definition of intersect is "overlap".  Meaning a key intersects
     *  another key if there is any overlap in any dimension.
     */
    template< class RecordType >
    bool Iterator<RecordType>::intersects(typename Iterator<RecordType>::CacheNode* nodeHandle, ...) {
        // get the key for the host node
        KeyMBR* hostKey = nodeHandle->object->getKey();
        
        if (!hostKey) {
            return false;
        }
        if (!_searchKey || !_searchKey->getKey()) {
            return false;
        }

        // determine if the host key intersects the search key
        bool ret = hostKey->intersects(*(_searchKey->getKey()));

        // if this is a data node we add it to the resulting record queue
        if(ret && nodeHandle->object->isDataNode()) {
            // The nodeHandle contains the resolved DataRecord already
            // No _MBRKeyNode needed since this is the actual data
            QueueItem item{nodeHandle, nullptr};
            _recordQueue.push_back(item);
        }

        return ret;
    }

    /**
     * Contains search - in this context, the definition of contains is the Data Record completely contains the search
     *  poly.  The approach is rather straight forward, check all children of the starting node.  If a subtree child is
     *  completely contained, then add all of the leaf records to the result queue and we're done with that subtree.
     *  If a subtree child intersects, then we need to investigate deeper by traversing into that subtree.
     */
    template< class RecordType >
    bool Iterator<RecordType>::contains(typename Iterator<RecordType>::CacheNode* nodeHandle, ...) {
//        cout << "calling contain search - not yet implemented" << endl;

        // get the key for the host node
//        KeyMBR* hostKey = nodeHandle->object->getKey();

        // check if the host key completely contains the search key
//        if( hostKey->contains(*(_searchKey->getKey())) )

        return true;
    }

    /**
     * Traversal framework method
     *
     * Internal nodes are cached and traversed normally.
     * Data records are never materialized here - we use their MBR for filtering
     * and defer loading to next() for zero heap retention during traversal.
     */
    template< class RecordType >
    template< typename TraversalOrder >
    void Iterator<RecordType>::traverse(typename Iterator<RecordType>::CacheNode* nodeHandle,
                                        bool(xtree::Iterator<RecordType>::* visit)(
                                        typename Iterator<RecordType>::CacheNode*, ...) ) {
        // Init/restore traversal order (stack or queue)
        TraversalOrder *sq;
        if(_traversalOrder == NULL) {
            sq = new TraversalOrder();
            _traversalOrder = static_cast<void*>(sq);
            
            if (nodeHandle && nodeHandle->object) {
                sq->push(nodeHandle);
            } else {
                // Root should always be cached; bail safely if not
                _hasNext = false;
                delete sq;
                _traversalOrder = NULL;
                return;
            }
        } else {
            sq = static_cast<TraversalOrder*>(_traversalOrder);
        }

        // MBR-based predicate for data children (avoids materialization)
        const auto mbr_matches = [this](MBRKeyNode* kn) -> bool {
            const KeyMBR* childKey = kn ? kn->getKey() : nullptr;
            if (!childKey || !_searchKey || !_searchKey->getKey()) {
                return false;
            }
            
            switch (_searchType) {
                case INTERSECTS:
                    return childKey->intersects(*(_searchKey->getKey()));
                case WITHIN:
                    // For CONTAINS, the search key should contain the child
                    return _searchKey->getKey()->contains(*childKey);
                case CONTAINS:
                    // For CONTAINS, the child should contain the search key
                    return childKey->contains(*(_searchKey->getKey()));
                default:
                    return true;
            }
        };

        // Walk until we've filled a page of results or traversal is empty
        while(!sq->empty() && _recordQueue.size()<XTREE_ITER_PAGE_SIZE) {
            CacheNode* cur = top(*sq);
            
            // Safety: internal nodes must have materialized objects
            if (cur && cur->object && !cur->object->isDataNode()) {
                bool proceed = (this->* visit)(pull(*sq));  // visit internal bucket
                
                if( proceed ) {
                    // Expand children
                    XTreeBucket<RecordType>* bucket = reinterpret_cast<XTreeBucket<RecordType>*>(cur->object);
                    auto* children = bucket ? bucket->getChildren() : nullptr;
                    if (!children) continue;
                    
                    int n = 0;
                    for(typename vector<MBRKeyNode*>::const_iterator iter = children->begin();
                        n<bucket->n() && iter != children->end(); iter++, ++n
                    ) {
                        MBRKeyNode* kn = *iter;
                        if (!kn) continue;
                        
                        // Internal child: use cache_or_load for unified lazy loading
                        if (!kn->isDataRecord()) {
                            // Production path: cache_or_load handles both cached and persistent nodes
                            CacheNode* childCN = kn->template cache_or_load<RecordType>(_idx);
                            if (childCN) {
                                sq->push(childCN);
                            }
#ifndef NDEBUG
                            else if (kn->hasNodeID()) {
                                // Development builds: warn if we have a valid NodeID but couldn't load
                                log() << "WARN: Iterator skipping unloadable child bucket NodeID " 
                                      << kn->getNodeID().raw() << std::endl;
                            }
#endif
                            continue;
                        }
                        
                        // Data child: do NOT materialize. Use MBR filter then enqueue resolver.
                        if (mbr_matches(kn)) {
                            _recordQueue.push_back(QueueItem{nullptr, kn});
#ifndef NDEBUG
                            // Debug: verify DataRecord has valid NodeID
                            if (!kn->hasNodeID() || !kn->getNodeID().valid()) {
                                trace() << "[ITER_WARN] DataRecord child missing NodeID in bucket "
                                          << (bucket ? bucket->getNodeID().raw() : 0) << std::endl;
                            }
#endif
                        }
                    }
                }
            } else {
                // Unexpected null object in traversal order - pop and continue
                pull(*sq);
            }
        }
        _hasNext = (sq->size()>0);
        // since _traversalOrder is a void pointer you can't guarantee that the
        // referenced object is properly destructed, by deleting sq and resetting
        // _traversalOrder we are ensuring proper cleanup of the {stack|queue}
        if(_hasNext == false) {
            delete sq;
            _traversalOrder = NULL;
        }
    };
}
