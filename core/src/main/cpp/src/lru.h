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

//#include "xtree.h"

#include <cstddef>
namespace xtree {

    enum LRUCacheDeleteType {
        LRUDeleteNone,
        LRUDeleteObject,
        LRUDeleteArray,
        LRUFreeMalloc
    };

    /**
     * Generic Node for LRU Caching
     */
    template< typename CachedObjectType,
              typename IdType,
              LRUCacheDeleteType deleteType >
    struct LRUCacheNode {

        typedef LRUCacheNode<CachedObjectType, IdType, deleteType> _SelfType;

        explicit LRUCacheNode(const IdType &i, CachedObjectType *o, _SelfType *n)
                : id(i), object(o), next(n), prev(NULL) {}

        IdType id;
        CachedObjectType *object;

        // linked list of cache nodes
        _SelfType *next;
        _SelfType *prev;
    };

    /**
     *  Used for deleting a node
     */
    template< typename CachedObjectType, typename IdType >
    struct LRUCacheNode< CachedObjectType, IdType, LRUDeleteObject > {
        typedef LRUCacheNode<CachedObjectType, IdType, LRUDeleteObject> _SelfType;

        explicit LRUCacheNode(const IdType &i, CachedObjectType *o, _SelfType *n)
                : id(i), object(o), next(n), prev(NULL) {}

        ~LRUCacheNode() { if(object) delete object; }

        IdType id;
        CachedObjectType *object;

        // linked list of cache nodes
        _SelfType *next;
        _SelfType *prev;
    };

    /**
     * Used for deleting an array of nodes
     */
    template< typename CachedObjectType, typename IdType >
    struct LRUCacheNode<CachedObjectType, IdType, LRUDeleteArray> {
        typedef LRUCacheNode<CachedObjectType, IdType, LRUDeleteArray> _SelfType;

        explicit LRUCacheNode(const IdType &i, CachedObjectType *o, _SelfType *n)
                : id(i), object(o), next(n), prev(NULL) {}

        ~LRUCacheNode() { if(object) delete[] object; }

        IdType id;
        CachedObjectType *object;

        // linked list of cache nodes
        _SelfType *next;
        _SelfType *prev;
    };

    /**
     * Used for calling free / malloc
     */
    template< typename CachedObjectType, typename IdType >
    struct LRUCacheNode<CachedObjectType, IdType, LRUFreeMalloc> {
        typedef LRUCacheNode<CachedObjectType, IdType, LRUFreeMalloc> _SelfType;

        explicit LRUCacheNode(IdType &i, CachedObjectType *o, _SelfType *n)
                : id(i), object(o), next(n), prev(NULL) {}

        ~LRUCacheNode() { if(object) free(object); }

        IdType id;
        CachedObjectType *object;

        // linked list functionality
        _SelfType *next;
        _SelfType *prev;
    };

    /**
     * Used for sorting cache nodes
     */
     template< typename _LRUCacheNode >
     struct LRUCacheNodeSorter {
        typedef _LRUCacheNode first_argument_type;
        typedef _LRUCacheNode second_argument_type;
        typedef bool result_type;

        bool operator() (const _LRUCacheNode* const n1, const _LRUCacheNode* const n2) {
            return n1->id < n2->id;
        }
     };

     /**
      * Definition for LRU Cache functionality
      */
     template< typename CachedObjectType, typename IdType, LRUCacheDeleteType deleteObject >
     class LRUCache {
     public:
//        typedef LRUCache<CachedObjectType, IdType, deleteObject> _SelfType;
        typedef size_t sizeType;
        typedef LRUCacheNode<CachedObjectType, IdType, deleteObject> Node;
        typedef vector<Node*> NodeArray;
        typedef typename NodeArray::iterator NodeIterator;
        typedef LRUCacheNodeSorter<Node> NodeSorter;

        explicit LRUCache(const sizeType &maxMemory)
                : _first(NULL), _last(NULL), _maxMemory(maxMemory) {
            // memory size based - this will be dynamic
        }

        /**
         * Delete all of the nodes from the cache
         *     this calls the destructor contained in LRUCacheNode
         */
        ~LRUCache() {
            NodeIterator i = _nodes.begin();
            const NodeIterator end = _nodes.end();
            for(; i!=end; i++)
                delete *i;
        }

        size_t getMaxMemory() { return _maxMemory; }
        void updateMaxMemory(sizeType maxMemory) { _maxMemory = maxMemory; }


        /**
         * Gets an item by id
         */
        CachedObjectType* get(const IdType &id);

        /**
         * adds an item to the LRUCache
         */
         Node* add(const IdType &id, CachedObjectType *object);

        /**
         * removes an arbitrary node from the cache
         */
         Node* removeOne();

         /**
          * removes the CachedObjectType pointer from the vector and
          * adjusts the doubly linked list
          */
         void remove(CachedObjectType *object) {
        	 typename NodeArray::iterator trgtIter = find(_nodes.begin(), _nodes.end(), object);
        	 Node* trgt = *trgtIter;
        	 trgt->object->prev->next = trgt->object->next;
        	 trgt->object->next->prev = trgt->object->prev;

        	 _nodes.erase(trgtIter);
         }

     private:
        //
//        static _SelfType* _lruCache;

        // facilitate easy lookup by ID
        NodeArray _nodes;       // 24 bytes

        // facilitate order by LRU
        Node* _first;           // 8 bytes
        Node* _last;            // 8 bytes

        sizeType _maxMemory;    // 8 bytes
     };  // TOTAL: 48 bytes

}
