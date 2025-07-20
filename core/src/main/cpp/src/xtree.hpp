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

#include "xtree.h"
#include "xtiter.h"
#include "xtiter.hpp"

namespace xtree {

//static ofstream rstarlog("subtree.log", ios::out);
//static ofstream splitlog("split.log", ios::out);

    // generic method for visiting a sequence (whether its a stack - DFS, or queue - BFS)
    template <typename T>
    T& top(stack<T>& s) { return s.top(); }
    template <typename T>
    T& pull(stack<T>& s) {
        T& ret = s.top();
        s.pop();
        return ret;
    }

    template <typename T>
    T& top(queue<T>& q) { return q.front(); }
    template <typename T>
    T& pull(queue<T>& q) {
        T& ret = q.front();
        q.pop();
        return ret;
    }


    template< class RecordType >
    int XTreeBucket<RecordType>::_insert(CacheNode* thisCacheNode, CacheNode* cachedRecord) {
        XTreeBucket<RecordType>* subTree = this;

        // traverse to a leaf level
        while(!subTree->isLeaf()) {
            // checking all subtrees
            subTree = subTree->chooseSubtree(cachedRecord);
        }

        // subTree is a leaf...so insert here
        return subTree->insertHere(thisCacheNode, cachedRecord);
    }

    /**
     * this is where most of the recursion occurs. if we can't perform a basicInsert successfully then we
     * try to find a good split by calling split.  we let the split algorithm handle the creation of supernodes
     */
    template< class RecordType >
    int XTreeBucket<RecordType>::insertHere(CacheNode* thisCacheNode, CacheNode* cachedRecord) {
        if( !this->basicInsert(cachedRecord) ) {
            this->split(thisCacheNode, cachedRecord);
            return 0;
        }
        return 0;
    }

    /**
     * this is about as simple as it gets.  this here method performs a simple insert. if the size of the bucket
     * violates threshold values, then we request a split
     */
    template< class RecordType >
    bool XTreeBucket<RecordType>::basicInsert(CacheNode* cachedRecord) {
#ifdef _DEBUG
//    	log() << "::basicInsert()" << endl;
//    	log() << "\t IRecord* = " << cachedRecord->object << endl;
#endif

        // on overflow, either split or create/maintain a supernode
        if(this->_isSupernode && this->_n >= (XTREE_M<<1)) {
            // try to split again
            return false;
        } else if( ((this->_n >= XTREE_M) && (!this->_isSupernode)) )
            return false;

//        rstarlog << "\t basicInserting " << this->_n << " " << record << " in: " << this << endl;
        // otherwise just insert the record (can be a DataRecord or another XTreeBucket (internal node)
        this->kn(cachedRecord);

        // create a new MBRKeyNode
        return true;
    }

    /**
     * this algorithm is, arguably, the most important. if a good subtree is not chosen, we could spend a lot of
     * time splitting. we don't want that since split is expensive
     */
    template< class RecordType >
    XTreeBucket<RecordType>* XTreeBucket<RecordType>::chooseSubtree(CacheNode* cachedRecord) {
        IRecord* record = cachedRecord->object;
#ifdef _DEBUG
//        log() << "::chooseSubtree() " << endl;
//        log() << "\t IRecord* = " << cachedRecord->object << endl;
//        log() << "\t this = " << this << endl;
//        log() << "\t this->_n = " << this->_n << endl;
//        log() << "\t hasLeaves() = " << this->hasLeaves() << endl;
//        int nkCount = 0;
//        for( typename vector<_MBRKeyNode*>::iterator it = this->_children.begin(); it != this->_children.begin()+this->_n; ++it, ++nkCount)
//            rstarlog << "\t checking subtree at : " << (*it)->getRecord(_idx->getCache()) << " with mbr: " << *((*it)->getKey()) << endl;
#endif
        // If this buckets keys point to leaf buckets
    	if (this->hasLeaves()) {
    	    // determine the minimum overlap cost
    		if (this->_n > XTREE_CHOOSE_SUBTREE_P) {
    		    // ** alternative algorithm:
    			// Sort the rectangles in N in increasing order of
    			// then area enlargement needed to include the new
    			// data rectangle
                bool hasZeroEnlargement = false;
#ifdef _DEBUG
                log() << "::chooseSubtree() Doing partial_sort with _n = " << this->_n;
                log() << "\t_children.size() = " << this->_children.size() << endl;
#endif
    			// Let A be the group of the first p entrles
    			std::partial_sort( this->_children.begin(), this->_children.begin() + XTREE_CHOOSE_SUBTREE_P, (this->_children.begin()+this->_n),
    				SortKeysByAreaEnlargement<RecordType>(record->getKey(), &hasZeroEnlargement));

                XTreeBucket<RecordType>* retVal = NULL;
                if(hasZeroEnlargement)
                    retVal = (XTreeBucket<RecordType>*)(_children.at(0)->getRecord(_idx->getCache()));
                else
                    retVal = (XTreeBucket<RecordType>*)((*std::min_element(this->_children.begin(), this->_children.begin()+XTREE_CHOOSE_SUBTREE_P,
                                SortKeysByOverlapEnlargement<RecordType>(record->getKey(), this->_children.begin(), this->_children.begin()+XTREE_CHOOSE_SUBTREE_P)))->getRecord(_idx->getCache()));
                // NOTE: if you don't need to enlarge the area then why even calculate the overlap?
                // can optimize this step based on selecting the bounding rectangle with 0 area enlargment
                // and minimum margin (maximizing node utilization)

    		    // From the items in A, considering all items in
    			// N, choose the leaf whose rectangle needs least
    			// overlap enlargement
                return retVal;
    		}

    		// choose the leaf in N whose rectangle needs least
    		// overlap enlargement to include the new data
    		// rectangle Resolve ties by choosmg the leaf
    		// whose rectangle needs least area enlargement, then
    		// the leaf with the rectangle of smallest area
//            unsigned long before = GetTimeMicro64();
            bool hasZeroEnlargement = false;
    	    std::sort( this->_children.begin(), this->_children.begin() + this->_n,
    				SortKeysByAreaEnlargement<RecordType>(record->getKey(), &hasZeroEnlargement));

            // if there is no enlargement then we're just going to return the first child
            /**@todo nwk: Choosing the first child is the naive approach, need to choose the child that reduces margin */
            XTreeBucket<RecordType>* retVal = NULL;
            if(hasZeroEnlargement)
                retVal = (XTreeBucket<RecordType>*)(_children.at(0)->getRecord(_idx->getCache()));
            else
                retVal =  (XTreeBucket<RecordType>*)((*std::min_element(this->_children.begin(), (this->_children.begin()+this->_n),
                    SortKeysByOverlapEnlargement<RecordType>(record->getKey(), this->_children.begin(), (this->_children.begin()+this->_n))))->getRecord(_idx->getCache()));

            return retVal;
        }

    	// [determine the minimum area cost],
    	// choose the leaf in N whose rectangle needs least
    	// area enlargement to include the new data
    	// rectangle. Resolve ties by choosing the leaf
    	// with the rectangle of smallest area
//        cout << "RETURNING SORT BY AREA ENLARGEMENT WITH " << this->_children.size() << " ELEMENTS" << endl;
        bool hasZeroEnlargement = false;
//        return (XTreeBucket<RecordType>*)((*std::min_element(this->_children.begin(), this->_children.begin()+this->_n,
//            SortKeysByAreaEnlargement<RecordType>(record->getKey(), &hasZeroEnlargement)))->getRecord());
        XTreeBucket<RecordType>* retBucket = (XTreeBucket<RecordType>*)((*std::min_element(this->_children.begin(), this->_children.begin()+this->_n,
                  SortKeysByAreaEnlargement<RecordType>(record->getKey(), &hasZeroEnlargement)))->getRecord(_idx->getCache()));
//        if(hasZeroEnlargement)
//            cout << "WOOT WOOT ZERO ENLARGEMENT **************************************************" << endl;
        return retBucket;
    }

    /**
     * This here method includes the logic for calculating the optimal split of *this* bucket
     * This method ONLY calculates the optimal split, carrying out the split in the data structure is
     * handled in splitCommit
     */
    template< class RecordType >
    bool XTreeBucket<RecordType>::split(CacheNode* thisCacheNode, CacheNode* cachedRecord) {
        // add the new key to this bucket (so it will be included when we split, or when
        // we turn into a supernode)
        this->kn(cachedRecord);

#ifdef _DEBUG
    	log() << "::split()" << endl;
    	log() << this->toString() << endl;
#endif

        // now we need to figure out how to split the MBRs
        unsigned short n_items = this->_n;
        // in the R*Tree implementation, nodes are typically degree 4 because its simple, but this is a XTree
        // where internal nodes are called "Supernodes". That is, they can contain an extrememly large number of
        // keys. The number of children can vary based on the dimension of the tree (because of the mbrSize in bytes)
        // and the size of the bucket. Thus we need to calculate the min_child_items at runtime as a function of
        // BucketSize and MBR key size.
        unsigned short min_child_items = floor((XTREE_M/2.0)*0.4);//floor(((V::BucketSize - _headerSize())/knSize())*0.4);
        unsigned short distribution_count = n_items - 2*min_child_items +1;  // taken from (Kriegel, 1999)
        unsigned short split_axis = _idx->getDimensionCount() + 1, split_edge = 0, split_index = 0; //, split_margin = 0;

        //assert(distribution_count > 0);
        //assert(min_child_items + distribution_count-1 <= n_items);



        unsigned short dist_edge = 0, dist_index = 0;
        double dist_overlap, dist_area;
        double dist_prctOverlap = 1.0;
        dist_area = dist_overlap = numeric_limits<double>::max();

        KeyMBR* mbr1 = new KeyMBR(this->_idx->getDimensionCount(), this->_idx->getPrecision());
        KeyMBR* mbr2 = new KeyMBR(this->_idx->getDimensionCount(), this->_idx->getPrecision());

        // loop through each axis
        for(unsigned short axis = 0; axis<this->_idx->getDimensionCount(); ++axis) {
            // initialize per-loop items
            int margin = 0;
            double overlap = 0;
            double prctOverlap = 1.0;

            // sort items by minimum then by maximum values of the MBR on this particular axis and determine
            // all of the distributions as described by Kriegel, 1999. Compute S, the sum of all margin-values
            // of the different distributions.
            // minimum val = 0, maximum val = 1
            for(unsigned short val = 0; val < 2; ++val) {
               // sort the keys by the correct value (min val, max val)
               if(val == 0)
                    std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMin<RecordType>(axis));
                else
                    std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMax<RecordType>(axis));

                // Distributions: pick a point m in the middle of the Bucket and call the left
                // r1 and the right r2.  Calculate the margin, area, and overlap values used
                // to determine the optimal split
                // calculate the bounding box for R1 and R2 within the loop
                double area;
                for(unsigned short k=0; k < distribution_count; ++k) {
                    area = 0;
                    // first half (this bucket)
                    mbr1->reset();
					for_each(this->_children.begin(), this->_children.begin()+(min_child_items+k), StretchBoundingBox<RecordType>(mbr1));

                    // second half (split bucket)
                    mbr2->reset();
					for_each(this->_children.begin()+(min_child_items+k), this->_children.begin()+this->_n, StretchBoundingBox<RecordType>(mbr2));

                    // calculate the three values
					margin 	+= mbr1->edgeDeltas() + mbr2->edgeDeltas();
					area 	+= mbr1->area() + mbr2->area();		// TODO: need to subtract.. overlap?
					overlap =  mbr1->overlap(*mbr2);
					prctOverlap = mbr1->percentOverlap(*mbr2);
					// CSI1: Along the split axis, choose the distribution with the
					// minimum overlap-value. Resolve ties by choosing the distribution
					// with minimum area-value.
					if (overlap < dist_overlap || (overlap == dist_overlap && area < dist_area)) {
						// if so, store the parameters that allow us to recreate it at the end
						split_axis = axis;
                        dist_edge = 	val;
                        split_edge = dist_edge;
						dist_index = 	min_child_items+k;
                        split_index = dist_index;
						dist_overlap = 	overlap;
						dist_prctOverlap = prctOverlap;
						dist_area = 	area;
					}
                } // distribution
            } // min val, max val sort

#ifdef _DEBUG
            // quick sanity check
//            log() << "\t AXIS TEST " << axis << endl;
//            log() << "\t   split_axis:  " << split_axis << endl;
//            log() << "\t   split_edge:  " << split_edge << endl;
//            log() << "\t   split_index: " << split_index << endl;
#endif
        } // along each axis


		// S3: Distribute the items into two groups
		// we're done. the best distribution on the selected split
		// axis has been recorded, so we just have to recreate it and
		// return the correct index
        /** @TODO revisit this recreation step, it can/will be better optimized*/
		if (split_edge == 0)
			std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMin<RecordType>(split_axis));

		// only reinsert the sort key if we have to
		else if (split_axis != _idx->getDimensionCount()-1)
			std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMax<RecordType>(split_axis));
        // split_index indicates where to divide the keys

        // check the percentage overlap, if it meets the threshold then we found a good split
        // if it doesn't then we need to great a supernode (aka: grow _children beyond the XTREE_M bounds
        if(dist_prctOverlap <= XTREE_MAX_OVERLAP ) {
//            cout << "FOUND A GOOD SPLIT!!! dist_prctOverlap IS: "  << dist_prctOverlap << endl;
            this->splitCommit( thisCacheNode, mbr1, mbr2, split_index );
            return true;
        } else {
#ifdef _DEBUG
        	log() << "COULDN'T FIND A GOOD SPLIT BECAUSE dist_prctOverlap IS: " << dist_prctOverlap << " THIS IS A SUPERNODE!" << endl;
#endif
        	// at some point we need to cut off the fanout and just insert
        	if(this->_n >= XTREE_MAX_FANOUT) {
                this->splitCommit( thisCacheNode, mbr1, mbr2, split_index );
        		return true;
        	}
        }

        return false;
    }

    /**
     * This here method is where we commit the split to the data structure.  This is the first cut,
     * we will obviously be optimizing and checking for any memory leaks.
     */
    template< class RecordType >
    void XTreeBucket<RecordType>::splitCommit( CacheNode* thisCacheNode, KeyMBR* mbr1, KeyMBR* mbr2, const unsigned int &split_index ) {
        // create a new XTreeBucket to hold MBR2 contents
        XTreeBucket<RecordType>* splitBucket = new XTreeBucket<RecordType>(this->_idx, false, mbr2, this->getChildren(), split_index, this->_leaf, this->_n);
        // cache the new split bucket
        CacheNode* cachedSplitNode = this->_idx->getCache().add( _idx->getNextNodeID(), splitBucket);

        // erase the split elements from this bucket and adjust memory consumption
#ifdef _DEBUG
        log() << "ERASING INDEX " << split_index+1 << " : " << this->_n;
#endif
        this->_children.erase(this->_children.begin()+split_index+1, this->_children.begin()+this->_n);
        this->_memoryUsage -= (this->_n - this->_children.size())*sizeof(_MBRKeyNode*);
        this->_n = split_index;
        this->setKey(mbr1);

//        splitlog << "splitting node " << this << " @ idx " << split_index << " into ";
//        splitlog << this << "[0-" << split_index << ") and " << splitBucket << " [";
//        splitlog << split_index << "-" << (split_index+splitBucket->_children.size()) << endl;


        // create a new root and insert the new split bucket
        if(this->_parent == NULL) {
            this->splitRoot(thisCacheNode, cachedSplitNode);
        } else {
            this->splitNode(thisCacheNode, cachedSplitNode);
        }
    }

    template< class RecordType >
    void XTreeBucket<RecordType>::splitRoot(CacheNode* thisCacheNode, CacheNode* cachedSplitBucket) {
//        cout << "***************************** SPLITTING THE ROOT *************************" << endl;
        XTreeBucket<RecordType>* splitBucket = reinterpret_cast<XTreeBucket<RecordType>*>(cachedSplitBucket->object);
        // create a new root bucket
        XTreeBucket<RecordType>* rootBucket = new XTreeBucket<RecordType>(this->_idx, true);
        // cache the root bucket
        CacheNode* cachedRootNode = this->_idx->getCache().add( _idx->getNextNodeID(), rootBucket);

//        splitlog << "\t created new root node " << rootBucket << endl;
        rootBucket->_leaf = false;
        // set the parent and nextChild accordingly
        _MBRKeyNode* rootKN = splitBucket->createParentKN(cachedRootNode);
        this->setParent(rootKN);
        this->setNextChild(splitBucket);
        this->_prevChild = rootBucket;
        splitBucket->_prevChild = this;
        rootBucket->setNextChild(this);
        // set the new root address in the index
        this->_idx->setRootAddress((long)cachedRootNode);
        // insert the 2 buckets in the rootBucket
//        cout << "INSERTING " << this << " INTO NEW ROOT" << endl;
        // insert this cacheNode into the root
        rootBucket->insertHere(cachedRootNode , thisCacheNode);
//        cout << "INSERTING " << splitBucket << " INTO NEW ROOT" << endl;
        rootBucket->insertHere(cachedRootNode, cachedSplitBucket);
//        cout << "HERE IS THE AMOUNT OF MEMORY FOR THE ENTIRE TREE: " << rootBucket->treeMemUsage(cachedRootNode);
    }

    template< class RecordType >
    void XTreeBucket<RecordType>::splitNode(CacheNode* thisCacheNode, CacheNode* cachedSplitBucket) {
//        cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SPLITTING INTERNAL NODE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << endl;
        XTreeBucket<RecordType>* splitBucket = reinterpret_cast<XTreeBucket<RecordType>*>(cachedSplitBucket->object);
        splitBucket->setParent(this->_parent);

        // insert the split bucket in the linked list
        splitBucket->setNextChild(this->_nextChild);
        splitBucket->_prevChild = this;
        this->setNextChild(splitBucket);

        XTreeBucket<RecordType>* parent = reinterpret_cast<XTreeBucket<RecordType>*>(this->_parent->getRecord(_idx->getCache()));
        parent->insertHere(this->_parent->getCacheRecord(), cachedSplitBucket);
//        cout << "THIS->PARENT (" << parent << ") NOW HAS: " << parent->_n << " CHILDREN" << endl;
    }

    template< class RecordType >
    void XTreeBucket<RecordType>::xt_purge(CacheNode* thisCacheNode) {
    	auto visit = [&](CacheNode* cn, Unit* result, ...) {
    		IRecord* rec = cn->object;
    		rec->purge();
    	};
    	(*this).template traverse<Unit, decltype(visit), DFS>( thisCacheNode, visit );
    }

    /**
     * Returns the amount of memory used by this tree (or subtree)
     */
    template< class RecordType >
    long XTreeBucket<RecordType>::treeMemUsage(CacheNode* cacheNode) const {
        int nodesVisited = 0;
        auto visit = [&](/*XTreeBucket<RecordType>*/CacheNode* cn, uint64_t* result, ...) {
        	IRecord* rec = cn->object;
            (*result) += rec->memoryUsage();
            nodesVisited++;
        };
        uint64_t memUsage = (*this).template traverse<uint64_t, decltype(visit), BFS>( cacheNode, visit );
//        cout << "VISITED " << nodesVisited << " NODES" << endl;
        return memUsage;
    }

    /**
     * This is a generalized breadth first search algorithm (which will be later generalized
     * to accommodate DFS.  This method is reused for traversing (e.g., searching the tree) and
     * returning a result based on the method specified by the Visit template parameter.
     *     The typical way to use this method is exemplified in treeMemUsage, where we traverse
     *     the tree and sum the memory usage.  The parameters are outlined below:
     *           Result - the type (class or primitive) BFS should return
     *           Visit - the method to be called on each node (lambda closure or generic functor)
     */
    template< class RecordType >
    template< typename Result, typename Visit, typename TraversalOrder >
    Result XTreeBucket<RecordType>::traverse(CacheNode* thisCacheNode, Visit visit, ...) const {
        Result result{};
        va_list arguments;
        // store all values after visit
        va_start(arguments, visit);
        // $FS queue
        TraversalOrder bfsq;
        bfsq.push(/*const_cast<XTreeBucket<RecordType>*>(this)*/ thisCacheNode);
        CacheNode* cn = NULL;
        IRecord* rec = NULL;
        int n;
        while(!bfsq.empty()) {
            // poll the queue, visit, and append the children
            cn = top(bfsq);
            rec = cn->object;
            // visit (aka: call the provided lambda closure)
            visit(cn, &result, arguments);

            // if this isn't a data node push all the children
            if(rec->isDataNode() == false) {
                n=0;
                XTreeBucket<RecordType>* bucket = reinterpret_cast<XTreeBucket<RecordType>*>(rec);
                for(typename vector<_MBRKeyNode*>::const_iterator iter = bucket->getChildren()->begin();
                    n<bucket->n(); iter++, ++n) {
                    // if this child is cached then get its memory usage
//                    if((*iter)->getCached())
//                        bfsq.push( ((*iter)->getRecord( this->_idx->getCache() )) );
                    bfsq.push( (*iter)->getCacheRecord() );
                }
            }
            bfsq.pop();
        }
        return result;
    }

    template< class RecordType >
    Iterator<RecordType>* XTreeBucket<RecordType>::getIterator(CacheNode* thisCacheNode, IRecord* searchKey, int queryType) {
        Iterator<RecordType>* iter = new Iterator<RecordType>(thisCacheNode, searchKey, static_cast<SearchType>(queryType));
        return iter;
    }

} // namespace xtree
