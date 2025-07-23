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

#include "lru.h"
#include <algorithm>

namespace xtree {

    /**
     * Adds a node to the cache list
     */
    template< typename CachedObjectType,
              typename IdType,
              LRUCacheDeleteType deleteObject >
    typename LRUCache<CachedObjectType, IdType, deleteObject>::Node*
        LRUCache<CachedObjectType, IdType, deleteObject>::add(const IdType &id, CachedObjectType *object) {

        //Node* ret = NULL;

        // set the first node to a new node
        this->_first = new Node(id, object, _first);

        if( _nodes.empty() ) {
            this->_last = this->_first;
            this->_nodes.push_back(this->_first);
            return this->_first;
        }

        // set the doubly-linked list
        this->_first->next->prev = _first;
        
        // Add the new node to _nodes
        // For performance, we add to the end instead of the beginning
        // The LRU order is maintained by the linked list, not the vector order
        this->_nodes.push_back(this->_first);

        // if adding a node exceeds memory...
        // we will need to initiate a min compaction

        return this->_first;
    }

    /**
     * Removes the LRU node
     *   Useful for clearing the cache and writing to persistent storage
     */
    template< typename CachedObjectType,
              typename IdType,
              LRUCacheDeleteType deleteObject >
    typename LRUCache<CachedObjectType, IdType, deleteObject>::Node*
        LRUCache<CachedObjectType, IdType, deleteObject>::removeOne() {
        if(_nodes.empty())
            return NULL;

        Node* node = _nodes.back();
        _nodes.pop_back();
        return node;
    }

    template< typename CachedObjectType,
    		  typename IdType,
    		  LRUCacheDeleteType deleteObject >
    CachedObjectType* LRUCache<CachedObjectType, IdType, deleteObject>::get(const IdType &id ) {
    	return NULL;
    }

}
