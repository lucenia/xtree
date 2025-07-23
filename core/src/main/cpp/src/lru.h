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
             // Find the node that contains this object
             typename NodeArray::iterator trgtIter = _nodes.begin();
             for (; trgtIter != _nodes.end(); ++trgtIter) {
                 if ((*trgtIter)->object == object) {
                     break;
                 }
             }
             
             if (trgtIter == _nodes.end()) {
                 return; // Object not found
             }
             
             Node* trgt = *trgtIter;
             
             // Update the doubly-linked list
             if (trgt->prev) {
                 trgt->prev->next = trgt->next;
             } else {
                 // This was the first node
                 _first = trgt->next;
             }
             
             if (trgt->next) {
                 trgt->next->prev = trgt->prev;
             } else {
                 // This was the last node
                 _last = trgt->prev;
             }
             
             // Remove from the vector
             _nodes.erase(trgtIter);
             
             // Delete the node
             delete trgt;
         }
         
         /**
          * Clears all nodes from the cache, deleting all cached objects.
          * This is useful for test cleanup to prevent memory leaks.
          */
         void clear() {
             // Delete all nodes in the cache
             NodeIterator i = _nodes.begin();
             const NodeIterator end = _nodes.end();
             for(; i!=end; i++)
                 delete *i;
             _nodes.clear();
             _first = NULL;
             _last = NULL;
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
