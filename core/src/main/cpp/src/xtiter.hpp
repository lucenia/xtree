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

        // determine if the host key intersects the search key
        bool ret = hostKey->intersects(*(_searchKey->getKey()));

        // if this is a data node we add it to the resulting record vector
        if(ret && nodeHandle->object->isDataNode())
            _recordQueue.push_back(nodeHandle);

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
        cout << "calling contain search - not yet implemented" << endl;

        // get the key for the host node
//        KeyMBR* hostKey = nodeHandle->object->getKey();

        // check if the host key completely contains the search key
//        if( hostKey->contains(*(_searchKey->getKey())) )

        return true;
    }

    /**
     * Traversal framework method
     *
     *  There is a memory trade off in this design that needs to be (re-)considered.  This implementation
     *  is designed such that 'traverse' is a generic method for handling breadth-first/depth-first tree
     *  traversal.  In this manner, we can implement other XTree processing algorithms without having
     *  to separately carry all of the bookeeping for both breadth and depth first traversal.  The
     *  trade-off for compact code, however, is that the _traversalOrder container could get quite
     *  large and consume significant memory.  The fact that XTree is a wide (vice deep) tree structure
     *  exacerbates this problem and potentially warrants the need for a recursive depth-first traversal
     *  implementation.  Since we don't expect a deep tree structure we shouldn't expect stackOverflow
     *  issues.  Further benchmarking to determine if this is a problem is left as a todo
     */
    template< class RecordType >
    template< typename TraversalOrder >
    void Iterator<RecordType>::traverse(typename Iterator<RecordType>::CacheNode* nodeHandle,
                                        bool(xtree::Iterator<RecordType>::* visit)(
                                        typename Iterator<RecordType>::CacheNode*, ...) ) {
        // breadth first or depth first order
        // we hold on to the state of the traversal using
        // the _traversalOrder member (which resolves to a stack or
        // queue based on the template parameter)
        TraversalOrder *sq;
        if(_traversalOrder == NULL) {
            sq = new TraversalOrder();
            _traversalOrder = static_cast<void*>(sq);
            // pushes the starting node onto the stack/queue
            sq->push(nodeHandle);
        } else {
            sq = static_cast<TraversalOrder*>(_traversalOrder);
        }

        IRecord* rec = NULL;
        int n;
        // while the stack/queue is not empty continue to traverse
        while(!sq->empty() && _recordQueue.size()<XTREE_ITER_PAGE_SIZE) {
            rec = top(*sq)->object;
            // proceed deeper/wider? in the traversal
            // this is determined by the visit function referenced by the function pointer
            bool proceed = (this->* visit)(pull(*sq));
            if( proceed ) {
                // add children if we're not at a data node
                if(rec->isDataNode() == false) {
                    n=0;
                    XTreeBucket<RecordType>* bucket = reinterpret_cast<XTreeBucket<RecordType>*>(rec);
                    for(typename vector<MBRKeyNode*>::const_iterator iter = bucket->getChildren()->begin();
                        n<bucket->n(); iter++, ++n
                    ) {
                        CacheNode* childNode = (*iter)->getCacheRecord();
                        if(childNode != NULL) {
                            sq->push(childNode);
                        }
                    }
                }
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
