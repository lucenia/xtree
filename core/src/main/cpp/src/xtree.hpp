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
    XTreeBucket<RecordType>* XTreeBucket<RecordType>::_insert(CacheNode* thisCacheNode, CacheNode* cachedRecord) {
        // Contract: caller must pass a real, pinned cache node
        // _insert's job is traversal + placement, not cache lifecycle
        assert(cachedRecord && cachedRecord->isPinned() &&
               "_insert requires a cache-managed, pinned node");

        XTreeBucket<RecordType>* subTree = this;
        // CRITICAL: Track the cache node for the CURRENT bucket during descent
        // thisCacheNode must always correspond to subTree, not the original root
        CacheNode* currentCacheNode = thisCacheNode;

        // traverse to a leaf level
        while(!subTree->isLeaf()) {
            subTree = subTree->chooseSubtree(cachedRecord);
            if (!subTree) {
                throw std::runtime_error("_insert: null subtree during descent");
            }

            // CRITICAL FIX: Update currentCacheNode to track the cache node for subTree
            // subTree->_parent is the _MBRKeyNode in the parent bucket that references subTree.
            // Its getCacheRecord() returns the cache node for subTree (set by setCacheAlias during load).
            // SAFETY: When eviction is possible, getCacheRecord() may return dangling pointer.
            // Always use find() to get a validated cache node.
            if (subTree->_parent && subTree->hasNodeID()) {
                using Alloc = XAlloc<RecordType>;
                uint64_t key = Alloc::cache_key_for(subTree->getNodeID(), subTree);
                auto* cn = this->_idx->getCache().find(key);
                if (cn && cn->object == reinterpret_cast<IRecord*>(subTree)) {
                    currentCacheNode = cn;
                }
            }
        }

        // Pass the CORRECT cache node for the leaf bucket, not the original root's cache node
        return subTree->insertHere(currentCacheNode, cachedRecord);
    }

    /**
     * this is where most of the recursion occurs. if we can't perform a basicInsert successfully then we
     * try to find a good split by calling split.  we let the split algorithm handle the creation of supernodes
     */
    template< class RecordType >
    XTreeBucket<RecordType>* XTreeBucket<RecordType>::insertHere(CacheNode* thisCacheNode, CacheNode* cachedRecord) {
        // Contract: data records being persisted must be pinned to prevent eviction during insertion
        // But bucket nodes (used during splits) don't require pinning
        assert(cachedRecord && "insertHere requires a non-null cached node");

#ifndef NDEBUG
        // CRITICAL: Tripwire to catch re-entry with same payload pointer
        static thread_local const void* last_payload = nullptr;
        trace() << "[TRIPWIRE] insertHere called with cachedRecord=" << cachedRecord
                  << " last_payload=" << last_payload << std::endl;
        assert(last_payload != cachedRecord && "Same cachedRecord re-used in immediate re-entry");
        last_payload = cachedRecord;
        struct Reset { ~Reset(){ last_payload = nullptr; } } _reset_;
#endif

        // Only check pinning for data records, not for bucket nodes
        if (cachedRecord->object && cachedRecord->object->isDataNode()) {
            assert(cachedRecord->isPinned() &&
                   "insertHere requires data records to be pinned");
        }

        // Capture NodeID before insertion (for reallocation detection)
        persist::NodeID id_before = this->getNodeID();

#ifndef NDEBUG
        assert(this->_idx && "insertHere requires valid index context");
#endif

        // Try to insert locally first
        if (this->basicInsert(cachedRecord)) {
            // May publish & relocate this bucket
            ensure_bucket_live(this->_idx, this, thisCacheNode);

            // IMPORTANT: refresh the bucket pointer after potential relocation
            // The publish operation may have moved the bucket to a new memory location
            auto* current_bucket = (thisCacheNode && thisCacheNode->object)
                ? reinterpret_cast<XTreeBucket<RecordType>*>(thisCacheNode->object)
                : this;

#ifndef NDEBUG
            if (current_bucket != this) {
                trace() << "[INSERT_RELOCATE] bucket moved: " << this
                          << " -> " << current_bucket
                          << " (old id=" << id_before.raw()
                          << " new id=" << current_bucket->getNodeID().raw() << ")\n";
            }
#endif

            // CRITICAL FIX: Mark this bucket dirty regardless of MBR changes
            // because we added a new child. The children vector changed even if
            // the bounding MBR didn't expand (record MBR inside existing bounds).
            current_bucket->markDirty();

            // Cache parent once
            auto* parent_after = current_bucket->parent_bucket();

#ifndef NDEBUG
            // Detect parent/child NodeID collision (allocator bug)
            if (parent_after && current_bucket->getNodeID() == parent_after->getNodeID()) {
                trace() << "[ID_COLLISION] child NodeID matches parent after publish: "
                          << current_bucket->getNodeID().raw() << "\n";
                assert(false && "allocator/id-publish must never collide with parent NodeID");
            }
#endif

            // If the bucket was reallocated during persistence, re-stamp the parent's KN
            if (current_bucket->getNodeID() != id_before) {
#ifndef NDEBUG
                trace() << "[INSERT_REBIND] Leaf reallocated: "
                          << id_before.raw() << " -> " << current_bucket->getNodeID().raw() << "\n";
#endif
                if (auto* kn = current_bucket->_parent) {
                    // Use a stable copy of the MBR to avoid aliasing/UAF
                    KeyMBR stable_mbr = *current_bucket->_key;
                    kn->setDurableBucketChild(stable_mbr, current_bucket->getNodeID(), current_bucket->_leaf);
                    if (thisCacheNode) kn->setCacheAlias(thisCacheNode);
#ifndef NDEBUG
                    assert(kn->getNodeID() == current_bucket->getNodeID());
#endif
                } else if (!current_bucket->_parent && current_bucket->_idx) {
                    // Root case: update root identity using canonical cache key
                    using Alloc = XTreeAllocatorTraits<RecordType>;
                    const uint64_t cache_key = Alloc::cache_key_for(current_bucket->getNodeID(), current_bucket);
                    current_bucket->_idx->setRootIdentity(cache_key, current_bucket->getNodeID(), thisCacheNode);
                }

                // Verify no self-alias corruption (cache should be consistent with rekey)
#ifndef NDEBUG
                if (auto* kn = current_bucket->_parent) {
                    if (auto* cn = kn->getCacheRecord()) {
                        if (cn->object == reinterpret_cast<IRecord*>(parent_after)) {
                            trace() << "[CACHE_CORRUPTION] parent KN cache record points to parent bucket!\n"
                                      << "  parent NodeID: " << parent_after->getNodeID().raw() << "\n"
                                      << "  expected child NodeID: " << current_bucket->getNodeID().raw() << "\n";
                            assert(false && "Cache self-alias corruption in insertHere");
                        }
                    }
                }
#endif

#ifndef NDEBUG
                // Sibling NodeID dup detection
                if (parent_after) {
                    unsigned dups = 0;
                    for (unsigned i = 0; i < parent_after->_n; ++i) {
                        auto* pkn = parent_after->_kn(i);
                        if (pkn && !pkn->isDataRecord() && pkn->hasNodeID() &&
                            pkn->getNodeID() == current_bucket->getNodeID()) {
                            ++dups;
                        }
                    }
                    assert(dups == 1 && "Sibling NodeID collision detected under parent");
                }
                assert(!current_bucket->_parent || current_bucket->_parent->getNodeID() == current_bucket->getNodeID());
#endif
            }

            // Use the refreshed pointer for downstream work
            current_bucket->propagateMBRUpdate(thisCacheNode, /*childChangedHint=*/true);
            return current_bucket;
        }

        // Overflow → split or grow (both complete the insertion)
        auto s = this->split(thisCacheNode, cachedRecord);

        switch (s.kind) {
        case SplitResult::Kind::Split: {
            // Split happened, record already inserted during split
            // No further action needed - split handles parent updates
            return this; // or could return s.next_target if we track it properly
        }
        case SplitResult::Kind::Grew: {
            // Supernode growth, record already inserted
            this->propagateMBRUpdate(thisCacheNode, /*childChangedHint=*/true);
            return this;
        }
        case SplitResult::Kind::Failed:
        default:
            assert(false && "split() returned Failed; check split/growth logic");
            return this; // defensive
        }
    }

    /**
     * this is about as simple as it gets.  this here method performs a simple insert. if the size of the bucket
     * violates threshold values, then we request a split
     */
    template< class RecordType >
    bool XTreeBucket<RecordType>::basicInsert(CacheNode* cachedRecord) {
        // on overflow, either split or create/maintain a supernode
        if(this->_isSupernode && this->_n >= (XTREE_M<<1)) { // supernodes more aggressively try to split every insert >= 100 children
            // try to split again
            return false;
        } else if( ((this->_n >= XTREE_M) && (!this->_isSupernode)) ) { // non supernodes try to split
            return false;
        }

        // otherwise just insert the record (can be a DataRecord or another XTreeBucket (internal node)

        // For DURABLE mode: persist DataRecords to .xd files before inserting
        // This ensures they have a NodeID that can be stored in the leaf bucket
        // Only do this for types that support persistence (have wire_size, to_wire, etc.)
        if (cachedRecord && cachedRecord->object && cachedRecord->object->isDataNode()) {
            using Alloc = XAlloc<RecordType>;
            // Extra safety: compile-time check for wire methods
            if constexpr (Alloc::template has_wire_methods<RecordType>::value) {
                Alloc::persist_data_record(this->_idx, static_cast<RecordType*>(cachedRecord->object));
            }
            // SFINAE already makes persist_data_record a no-op for types without wire methods,
            // but if constexpr provides an extra layer of safety and clarity
        }

#ifndef NDEBUG
        // Debug assertions to catch invalid cachedRecord before kn() call
        assert(cachedRecord && "cachedRecord must be non-null");
        assert(reinterpret_cast<uintptr_t>(cachedRecord) % alignof(CacheNode) == 0 &&
               "cachedRecord must be properly aligned");
        assert(cachedRecord->object && "cachedRecord->object must be non-null");

        // CRITICAL: Hard tripwire to catch type mismatches immediately
        if (this->_leaf) {
            assert(cachedRecord->object->isDataNode() && "Leaf bucket must only insert DataRecord objects");
        } else {
            assert(!cachedRecord->object->isDataNode() && "Internal bucket must only insert bucket objects");
        }

        // Check for non-canonical pointer (Rosetta crash symptom)
        auto ptr = reinterpret_cast<uintptr_t>(cachedRecord);
        if ((ptr >> 48) != 0 && (ptr >> 48) != 0xFFFF) {
            trace() << "[INSERT_GUARD] Non-canonical cachedRecord pointer: "
                      << cachedRecord << std::endl;
            assert(false && "cachedRecord pointer looks non-canonical (likely dangling)");
        }

        // Verify it's actually an IRecord type
        auto* recObj = dynamic_cast<IRecord*>(cachedRecord->object);
        assert(recObj && "cachedRecord->object must be IRecord");
        assert(recObj->getKey() && "IRecord must have a key");
#endif

        this->kn(cachedRecord);

        // create a new MBRKeyNode
        return true;
    }


    /**
     * Helper function to load a child bucket with proper error handling
     * Throws if the child cannot be loaded (corrupted persistence)
     */
    template<class RecordType>
    static XTreeBucket<RecordType>* checked_load(__MBRKeyNode<RecordType>* kn,
                                                 IndexDetails<RecordType>* idx,
                                                 const char* context)
    {
        if (!kn) {
            throw std::runtime_error(std::string("chooseSubtree: null kn pointer [") + context + "]");
        }

        const persist::NodeID pid = kn->getNodeID();
        auto* cn = kn->template cache_or_load<RecordType>(idx);

        if (!cn || !cn->object) {
            throw std::runtime_error(std::string("chooseSubtree: failed to load child bucket [") +
                                     context + "] (NodeID=" + std::to_string(pid.raw()) + ")");
        }

        auto* result = reinterpret_cast<XTreeBucket<RecordType>*>(cn->object);
        if (!result) {
            throw std::runtime_error("checked_load: cast failed (null result)");
        }

        // Validate the existing alias (if any). Only clear if truly invalid.
        // SAFETY: When eviction is possible, getCacheRecord() may return a dangling pointer.
        // Use find() instead to validate the cache state.
        const bool may_evict = idx->getCache().getMaxMemory() > 0;
        decltype(cn) cached_cn = nullptr;
        if (may_evict && pid.valid()) {
            // Safe path: validate via cache lookup
            cached_cn = idx->getCache().find(pid.raw());
        } else {
            // Fast path: trust the stored pointer
            cached_cn = kn->getCacheRecord();
        }
        if (cached_cn) {
            auto* aliased = reinterpret_cast<XTreeBucket<RecordType>*>(cached_cn->object);
            const bool alias_valid =
                aliased && aliased->hasNodeID() && (aliased->getNodeID() == pid);

            if (!alias_valid) {
                // Clear the KN alias, but DO NOT double-add: rebind using lookup_or_attach.
                kn->setCacheAlias(nullptr);

                using Alloc = XTreeAllocatorTraits<RecordType>;
                const uint64_t key = Alloc::cache_key_for(pid, result);
                auto& cache = idx->getCache();

                // Reattach (find existing or add if truly missing)
                auto* node = cache.lookup_or_attach(key, reinterpret_cast<IRecord*>(result));
                kn->setCacheAlias(node);

                // Also refresh 'cn' to the canonical node we just (re)attached
                cn = node;
                result = reinterpret_cast<XTreeBucket<RecordType>*>(cn->object);
            }
        }

        // Normalize runtime NodeID to the durable ID we requested
        if (result->getNodeID() != pid) {
            result->setNodeID(pid);
            assert(result->getNodeID() == pid && "Runtime NodeID normalization failed");
        }

        return result;
    }

    /**
     * this algorithm is, arguably, the most important. if a good subtree is not chosen, we could spend a lot of
     * time splitting. we don't want that since split is expensive
     */
    template< class RecordType >
    XTreeBucket<RecordType>* XTreeBucket<RecordType>::chooseSubtree(CacheNode* cachedRecord) {
        IRecord* record = cachedRecord->object;

#ifndef NDEBUG
        // Sanity check: chooseSubtree should only be called on internal nodes
        if (this->_leaf) {
            throw std::runtime_error("chooseSubtree: called on leaf bucket! Should insert here instead of descending");
        }

#ifndef NDEBUG
        // Clear any self-referencing aliases that could cause cache_or_load() to return the parent
        // SAFETY: When eviction is possible, getCacheRecord() may return a dangling pointer.
        // Use find() instead to validate.
        const bool eviction_enabled = _idx->getCache().getMaxMemory() > 0;
        for (unsigned i = 0; i < this->_n; ++i) {
            _MBRKeyNode* kn = this->_kn(i);
            if (!kn) continue;

            // Use auto to avoid CacheNode type issues
            auto* cn_check = eviction_enabled && kn->hasNodeID()
                ? _idx->getCache().find(kn->getNodeID().raw())
                : kn->getCacheRecord();

            if (cn_check && cn_check->object == reinterpret_cast<IRecord*>(this)) {
                trace() << "[DESCENT_SANITIZE] Clearing self-referencing alias on parent nid="
                          << this->getNodeID().raw() << " at idx=" << i << "\n";
                kn->setCacheAlias(nullptr);
            }
        }
#endif

        // CRITICAL: Detect self-referencing corruption before descent
        persist::NodeID parentId = this->getNodeID();
        if (parentId.valid()) {
            for (uint32_t i = 0; i < this->_n; ++i) {
                auto* childKN = this->_children[i];
                if (childKN && childKN->hasNodeID()) {
                    persist::NodeID childId = childKN->getNodeID();
                    if (childId.raw() == parentId.raw()) {
                        trace() << "[DESCENT_GUARD] Parent " << parentId.raw()
                                  << " has self-referencing child at idx=" << i
                                  << " (n=" << this->_n << ", _leaf=" << this->_leaf << ")" << std::endl;
                        assert(false && "Parent references itself as child (corrupt entry)");
                    }
                }
            }
        }

        // Unified debug helper for validating and logging chosen child
        // Works regardless of whether _children has been reordered by sort
        auto debugValidateAndPrint = [this](const _MBRKeyNode* kn, const char* path) {
            assert(kn && "child KN must not be null");
            assert(!kn->isDataRecord() && "child KN must be a bucket");

            // Check for self-reference
            if (kn->hasNodeID() && this->hasNodeID()) {
                assert(kn->getNodeID().raw() != this->getNodeID().raw() &&
                       "child cannot reference parent itself");
            }

            // Always print by child_id for clarity (not index, which may be meaningless after sort)
            trace() << "[DESCENT] parent=" << this->getNodeID().raw()
                      << " n=" << this->_n
                      << " child_id=" << kn->getNodeID().raw()
                      << " (" << path << ")" << std::endl;

            // Verify linkage by pointer and NodeID
            this->template debugVerifyLinkKN<RecordType>(kn);

            // Verify NodeKind if durable store is present and child is not staged
            // NOTE: This check is disabled temporarily due to NodeKind tracking issues
            // TODO: Fix NodeKind tracking during bucket reallocation
            if (_idx && _idx->hasDurableStore()) {
                auto* store = _idx->getStore();
                bool staged = false;
                (void)store->is_node_present(kn->getNodeID(), &staged);
                if (!staged) {
                    persist::NodeKind k{};
                    if (store->get_node_kind(kn->getNodeID(), k)) {
                        const auto expected = kn->getLeaf()
                            ? persist::NodeKind::Leaf
                            : persist::NodeKind::Internal;
                        if (k != expected) {
                            trace() << "[WARN] NodeKind mismatch at descent: "
                                      << "parent=" << this->getNodeID().raw()
                                      << " child=" << kn->getNodeID().raw()
                                      << " expected=" << (expected == persist::NodeKind::Leaf ? "Leaf" : "Internal")
                                      << " actual=" << (k == persist::NodeKind::Leaf ? "Leaf" : "Internal")
                                      << std::endl;
                            // Temporarily disabled: assert(false && "NodeKind mismatch: child vs expected kind at descent");
                        }
                    }
                }
            }
        };

#endif

#ifdef _DEBUG
//        trace() << "::chooseSubtree() " << endl;
//        trace() << "\t IRecord* = " << cachedRecord->object << endl;
//        trace() << "\t this = " << this << endl;
//        trace() << "\t this->_n = " << this->_n << endl;
//        trace() << "\t hasLeaves() = " << this->hasLeaves() << endl;
//        int nkCount = 0;
//        for( typename vector<_MBRKeyNode*>::iterator it = this->_children.begin(); it != this->_children.begin()+this->_n; ++it, ++nkCount)
//            trace() << "\t checking subtree at : " << (*it)->getRecord(_idx) << " with mbr: " << *((*it)->getKey());
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
                if(hasZeroEnlargement) {
#ifndef NDEBUG
                    debugValidateAndPrint(_children.at(0), "hasLeaves+partial+zeroEnlargement");
#endif
                    retVal = checked_load<RecordType>(_children.at(0), _idx, "hasLeaves+partial+zeroEnlargement path");
                } else {
                    auto* kn = (*std::min_element(this->_children.begin(), this->_children.begin() + XTREE_CHOOSE_SUBTREE_P,
                                SortKeysByOverlapEnlargement<RecordType>(record->getKey(), this->_children.begin(), this->_children.begin()+XTREE_CHOOSE_SUBTREE_P)));
#ifndef NDEBUG
                    debugValidateAndPrint(kn, "hasLeaves+partial+overlapEnlargement");
#endif
                    retVal = checked_load<RecordType>(kn, _idx, "hasLeaves+partial+overlapEnlargement path");
                }
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
            if(hasZeroEnlargement) {
#ifndef NDEBUG
                debugValidateAndPrint(_children.at(0), "hasLeaves+fullSort+zeroEnlargement");
#endif
                retVal = checked_load<RecordType>(_children.at(0), _idx, "hasLeaves+fullSort+zeroEnlargement path");
            } else {
                auto* kn = (*std::min_element(this->_children.begin(), (this->_children.begin()+this->_n),
                    SortKeysByOverlapEnlargement<RecordType>(record->getKey(), this->_children.begin(), (this->_children.begin()+this->_n))));
#ifndef NDEBUG
                debugValidateAndPrint(kn, "hasLeaves+fullSort+overlapEnlargement");
#endif
                retVal = checked_load<RecordType>(kn, _idx, "hasLeaves+fullSort+overlapEnlargement path");
            }

            return retVal;
        }

    	// [determine the minimum area cost],
    	// choose the leaf in N whose rectangle needs least
    	// area enlargement to include the new data
    	// rectangle. Resolve ties by choosing the leaf
    	// with the rectangle of smallest area
        trace() << "[CHOOSE_SUBTREE] Internal node path (no leaves), n=" << this->_n;
        bool hasZeroEnlargement = false;
        auto* kn = (*std::min_element(this->_children.begin(), this->_children.begin()+this->_n,
                  SortKeysByAreaEnlargement<RecordType>(record->getKey(), &hasZeroEnlargement)));
#ifndef NDEBUG
        debugValidateAndPrint(kn, "internal+areaEnlargement");
#endif
        return checked_load<RecordType>(kn, _idx, "internal+areaEnlargement path");
    }

    /**
     * This here method includes the logic for calculating the optimal split of *this* bucket
     * This method ONLY calculates the optimal split, carrying out the split in the data structure is
     * handled in splitCommit
     */
    template< class RecordType >
    typename XTreeBucket<RecordType>::SplitResult XTreeBucket<RecordType>::split(CacheNode* thisCacheNode, const CacheNode* insertingCN) {
#ifndef NDEBUG
        // CRITICAL: Capture original payload type and make it read-only
        IRecord* const origObj = insertingCN->object;
        const bool origIsData = origObj && origObj->isDataNode();
        if (this->_leaf) {
            assert(origIsData && "Leaf split must be driven by DataRecord");
        } else {
            assert(!origIsData && "Internal split must be driven by bucket");
        }
#endif

        // CRITICAL FIX: Ensure durable DataRecords get a NodeID before we wire them in
        // Without this, split path inserts have invalid NodeIDs (0) and disappear after recovery
        if (insertingCN && insertingCN->object && insertingCN->object->isDataNode()) {
            using Alloc = XAlloc<RecordType>;
            // Only persist for types that support persistence (have wire methods)
            if constexpr (Alloc::template has_wire_methods<RecordType>::value) {
                Alloc::persist_data_record(this->_idx, static_cast<RecordType*>(insertingCN->object));
            }
        }
        
        // add the new key to this bucket (so it will be included when we split, or when
        // we turn into a supernode)
#ifndef NDEBUG
        // Debug assertions to catch invalid insertingCN before kn() call
        assert(insertingCN && "insertingCN must be non-null");
        assert(insertingCN->object && "insertingCN->object must be non-null");
        auto* recObj = dynamic_cast<IRecord*>(insertingCN->object);
        assert(recObj && "insertingCN->object must be IRecord");
#endif
        this->kn(const_cast<CacheNode*>(insertingCN));

#ifdef _DEBUG
    	debug() << "::split()";
    	debug() << this->toString();
#endif

        // now we need to figure out how to split the MBRs
        unsigned short n_items = this->_n;
        // in the R*Tree implementation, nodes are typically degree 4 because its simple, but this is a XTree
        // where internal nodes are called "Supernodes". That is, they can contain an extrememly large number of
        // keys. The number of children can vary based on the dimension of the tree (because of the mbrSize in bytes)
        // and the size of the bucket. Thus we need to calculate the min_child_items at runtime as a function of
        // BucketSize and MBR key size.
        const unsigned short min_child_items = floor((XTREE_M/2.0)*0.4);//floor(((V::BucketSize - _headerSize())/knSize())*0.4);
        const unsigned short distribution_count = n_items - 2*min_child_items +1;  // taken from (Kriegel, 1999)
        unsigned short split_axis = _idx->getDimensionCount() + 1, split_edge = 0, split_index = 0; //, split_margin = 0;

        //assert(distribution_count > 0);
        //assert(min_child_items + distribution_count-1 <= n_items);

        unsigned short dist_edge = 0, dist_index = 0;
        double dist_overlap, dist_area;
        double dist_prctOverlap = 1.0;
        dist_area = dist_overlap = numeric_limits<double>::max();

        auto mbr1 = std::make_unique<KeyMBR>(this->_idx->getDimensionCount(), this->_idx->getPrecision());
        auto mbr2 = std::make_unique<KeyMBR>(this->_idx->getDimensionCount(), this->_idx->getPrecision());

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
               if(val == 0) {
                    std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMin<RecordType>(axis));
               } else {
                    std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMax<RecordType>(axis));
               }

                // Distributions: pick a point m in the middle of the Bucket and call the left
                // r1 and the right r2.  Calculate the margin, area, and overlap values used
                // to determine the optimal split
                // calculate the bounding box for R1 and R2 within the loop
                double area;
                for(unsigned short k=0; k < distribution_count; ++k) {
                    area = 0;
                    // first half (this bucket)
                    mbr1->reset();
					for_each(this->_children.begin(), this->_children.begin()+(min_child_items+k), StretchBoundingBox<RecordType>(mbr1.get()));

                    // second half (split bucket)
                    mbr2->reset();
					for_each(this->_children.begin()+(min_child_items+k), this->_children.begin()+this->_n, StretchBoundingBox<RecordType>(mbr2.get()));

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
            trace() << "\t AXIS TEST " << axis;
            trace() << "\t   split_axis:  " << split_axis;
            trace() << "\t   split_edge:  " << split_edge;
            trace() << "\t   split_index: " << split_index;
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
        if(__builtin_expect(dist_prctOverlap <= XTREE_MAX_OVERLAP, 1)) {
#ifdef _DEBUG
            trace() << "FOUND A GOOD SPLIT!!! dist_prctOverlap IS: "  << dist_prctOverlap;
#endif
            this->splitCommit( thisCacheNode, mbr1.get(), mbr2.release(), split_index );

#ifndef NDEBUG
            // CRITICAL: Postcondition - ensure payload wasn't mutated during split
            assert(insertingCN->object == origObj && "insertingCN object was mutated during split");
            if (this->_leaf) {
                assert(origObj->isDataNode() && "DataRecord payload corrupted during leaf split");
            } else {
                assert(!origObj->isDataNode() && "Bucket payload corrupted during internal split");
            }
#endif
            return { SplitResult::Kind::Split, nullptr };
        } else {
#ifdef _DEBUG
        	log() << "COULDN'T FIND A GOOD SPLIT BECAUSE dist_prctOverlap IS: " << dist_prctOverlap << " THIS IS A SUPERNODE!" << endl;
#endif
        	// at some point we need to cut off the fanout and just insert
        	if(this->_n >= XTREE_MAX_FANOUT) {
                this->splitCommit( thisCacheNode, mbr1.get(), mbr2.release(), split_index );

#ifndef NDEBUG
                // CRITICAL: Postcondition - ensure payload wasn't mutated during forced split
                assert(insertingCN->object == origObj && "insertingCN object was mutated during forced split");
                if (this->_leaf) {
                    assert(origObj->isDataNode() && "DataRecord payload corrupted during forced leaf split");
                } else {
                    assert(!origObj->isDataNode() && "Bucket payload corrupted during forced internal split");
                }
#endif
        		return { SplitResult::Kind::Split, nullptr };
        	}
        }

        // Supernode growth: record already inserted by kn() above
#ifndef NDEBUG
        // CRITICAL: Postcondition - ensure payload wasn't mutated during supernode growth
        assert(insertingCN->object == origObj && "insertingCN object was mutated during supernode growth");
        if (this->_leaf) {
            assert(origObj->isDataNode() && "DataRecord payload corrupted during leaf supernode growth");
        } else {
            assert(!origObj->isDataNode() && "Bucket payload corrupted during internal supernode growth");
        }
#endif
        return { SplitResult::Kind::Grew, nullptr };
    }

    /**
     * Try to cascade split this bucket after a child split added a sibling.
     * Unlike split(), this doesn't insert a new record - all children are already present.
     * Follows X-tree semantics:
     *   1. If good split available (low overlap) → split
     *   2. Else if at MAX_FANOUT → force split regardless
     *   3. Else → stay as supernode (do nothing)
     */
    template< class RecordType >
    void XTreeBucket<RecordType>::forceCascadeSplit(CacheNode* thisCacheNode) {
        assert(this->_n > XTREE_M && "forceCascadeSplit called but not over XTREE_M");
        assert(!this->_leaf && "Cascade split should only happen on internal nodes");

#ifndef NDEBUG
        trace() << "[CASCADE_SPLIT] Evaluating split on bucket with " << this->_n << " children"
                  << " (M=" << XTREE_M << ", max=" << XTREE_MAX_FANOUT << ")"
                  << " NodeID=" << this->getNodeID().raw()
                  << " is_root=" << (this->_parent == nullptr)
                  << std::endl;
#endif

        // Evaluate the best split distribution (same logic as split())
        unsigned short n_items = this->_n;
        const unsigned short min_child_items = floor((XTREE_M/2.0)*0.4);
        const unsigned short distribution_count = n_items - 2*min_child_items + 1;
        unsigned short split_axis = _idx->getDimensionCount() + 1, split_edge = 0, split_index = 0;

        unsigned short dist_edge = 0, dist_index = 0;
        double dist_overlap, dist_area;
        double dist_prctOverlap = 1.0;
        dist_area = dist_overlap = numeric_limits<double>::max();

        auto mbr1 = std::make_unique<KeyMBR>(this->_idx->getDimensionCount(), this->_idx->getPrecision());
        auto mbr2 = std::make_unique<KeyMBR>(this->_idx->getDimensionCount(), this->_idx->getPrecision());

        // Loop through each axis to find the best split
        for(unsigned short axis = 0; axis < this->_idx->getDimensionCount(); ++axis) {
            int margin = 0;
            double overlap = 0;
            double prctOverlap = 1.0;

            for(unsigned short val = 0; val < 2; ++val) {
                if(val == 0) {
                    std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMin<RecordType>(axis));
                } else {
                    std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMax<RecordType>(axis));
                }

                double area;
                for(unsigned short k = 0; k < distribution_count; ++k) {
                    area = 0;
                    mbr1->reset();
                    for_each(this->_children.begin(), this->_children.begin()+(min_child_items+k), StretchBoundingBox<RecordType>(mbr1.get()));

                    mbr2->reset();
                    for_each(this->_children.begin()+(min_child_items+k), this->_children.begin()+this->_n, StretchBoundingBox<RecordType>(mbr2.get()));

                    margin += mbr1->edgeDeltas() + mbr2->edgeDeltas();
                    area += mbr1->area() + mbr2->area();
                    overlap = mbr1->overlap(*mbr2);
                    prctOverlap = mbr1->percentOverlap(*mbr2);

                    if (overlap < dist_overlap || (overlap == dist_overlap && area < dist_area)) {
                        split_axis = axis;
                        dist_edge = val;
                        split_edge = dist_edge;
                        dist_index = min_child_items + k;
                        split_index = dist_index;
                        dist_overlap = overlap;
                        dist_prctOverlap = prctOverlap;
                        dist_area = area;
                    }
                }
            }
        }

        // Re-sort by the chosen split axis and edge
        if (split_edge == 0) {
            std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMin<RecordType>(split_axis));
        } else if (split_axis != _idx->getDimensionCount()-1) {
            std::sort(this->_children.begin(), this->_children.begin()+this->_n, SortKeysByRangeMax<RecordType>(split_axis));
        }

        // X-tree decision: split if good overlap, or force if at MAX_FANOUT
        if (dist_prctOverlap <= XTREE_MAX_OVERLAP) {
#ifndef NDEBUG
            trace() << "[CASCADE_SPLIT] Good split found (overlap=" << dist_prctOverlap
                      << " <= " << XTREE_MAX_OVERLAP << "), splitting"
                      << std::endl;
#endif
            this->splitCommit(thisCacheNode, mbr1.get(), mbr2.release(), split_index);
        } else if (this->_n >= XTREE_MAX_FANOUT) {
#ifndef NDEBUG
            trace() << "[CASCADE_SPLIT] At MAX_FANOUT (" << this->_n
                      << " >= " << XTREE_MAX_FANOUT << "), forcing split despite overlap="
                      << dist_prctOverlap << std::endl;
#endif
            this->splitCommit(thisCacheNode, mbr1.get(), mbr2.release(), split_index);
        } else {
#ifndef NDEBUG
            trace() << "[CASCADE_SPLIT] Staying as supernode (overlap=" << dist_prctOverlap
                      << " > " << XTREE_MAX_OVERLAP << ", n=" << this->_n
                      << " < " << XTREE_MAX_FANOUT << ")" << std::endl;
#endif
            // Stay as supernode - do nothing, kn() already added the child
        }
    }

    /**
     * This here method commits the split to the data structure.
     * Follows the bottom-up algorithm transparently for both IN_MEMORY and DURABLE modes:
     * 1. Create right sibling with children from [split_index+1, _n)
     * 2. Remove those children from left (this) bucket
     * 3. Update parent or create new root
     */
    template< class RecordType >
    void XTreeBucket<RecordType>::splitCommit( CacheNode* thisCacheNode, KeyMBR* mbr1, KeyMBR* mbr2, const unsigned int &split_index ) {
        using Alloc = XAlloc<RecordType>;
        const auto kind = this->_leaf ? persist::NodeKind::Leaf : persist::NodeKind::Internal;
        
        // Preconditions
        assert(split_index < this->_n);
        assert(split_index + 1 < this->_n); // ensure right sibling gets at least 1 child
        
        // Capture original state before mutation
        const unsigned int old_n = this->_n;
        
        // Step 1: Create empty right sibling (NO source children - we'll use kn_from_entry)
        auto rightRef = Alloc::allocate_bucket(
            this->_idx, kind,
            /*isRoot*/ false,
            /*key*/    mbr2,
            /*source*/ nullptr,  // CRITICAL: Don't pass source
            /*split*/  0,
            /*isLeaf*/ this->_leaf,
            /*sourceN*/0
        );
        
        auto* rightBucket = rightRef.ptr;
        Alloc::record_write(this->_idx, rightBucket);  // Optional COW instrumentation

#ifndef NDEBUG
        // Verify split allocator used correct kind
        assert(rightBucket && rightBucket->_leaf == this->_leaf &&
               "Right sibling of a split must have the same leaf flag");

        // In durable mode, verify NodeID is valid
        if (this->_idx && this->_idx->getPersistenceMode() == IndexDetails<RecordType>::PersistenceMode::DURABLE) {
            persist::NodeID rightId = rightBucket->getNodeID();
            assert(rightId.valid() && "Right bucket must have valid NodeID after allocation");
            // Parity check removed - type will be validated via ObjectTable metadata (Phase 4)
        }
#endif

        // Step 2: Adopt children [split_index+1, old_n) into right using kn_from_entry()
        // This preserves (MBR, NodeID) without re-persisting or requiring heap objects
        int moved = 0;
        for (unsigned int i = split_index + 1; i < old_n; ++i) {
            auto* src = this->_children[i];
            if (!src) continue;
            
            // Adopt the child entry preserving its NodeID
            auto* dst = rightBucket->kn_from_entry(*src);
            if (!dst) {
                throw std::runtime_error("splitCommit: failed to adopt child to right bucket");
            }
            ++moved;
        }

#ifndef NDEBUG
        // Verify we moved the expected number of children
        const int expected_moved = static_cast<int>(old_n - split_index - 1);
        if (moved != expected_moved) {
            trace() << "[DEBUG] splitCommit: moved count mismatch - expected " << expected_moved
                      << " got " << moved
                      << " | left_n=" << this->_n
                      << " old_n=" << old_n
                      << " split_index=" << split_index
                      << " right_n=" << rightBucket->n()
                      << " left_leaf=" << this->_leaf
                      << " right_leaf=" << rightBucket->_leaf
                      << std::endl;
            assert(false && "splitCommit: moved count mismatch");
        }

        // Verify right bucket has the correct count
        if (static_cast<unsigned>(rightBucket->n()) != static_cast<unsigned>(moved)) {
            trace() << "[DEBUG] splitCommit: right bucket child count mismatch - expected " << moved
                      << " got " << rightBucket->n()
                      << " | left_n=" << this->_n
                      << " right_leaf=" << rightBucket->_leaf
                      << std::endl;
            assert(false && "splitCommit: right bucket child count mismatch");
        }

        // Phase 4: Validate adopted children have correct NodeKinds
        if (this->_idx && this->_idx->getStore()) {
            auto* store = this->_idx->getStore();

            // Helper to compute expected NodeKind
            auto compute_expected_kind = [&](auto* child) -> persist::NodeKind {
                if (rightBucket->_leaf) {
                    return persist::NodeKind::DataRecord;
                } else {
                    return child->getLeaf() ? persist::NodeKind::Leaf : persist::NodeKind::Internal;
                }
            };

            for (size_t j = 0; j < static_cast<size_t>(rightBucket->n()); ++j) {
                auto* child = rightBucket->_children[j];
                assert(child && "Right bucket has null child after adoption");

                // Only validate if child has NodeID (durable mode)
                if (child->hasNodeID()) {
                    const auto& id = child->getNodeID();
                    assert(id.valid() && "Child NodeID must be valid in durable mode");

                    // Verify child type matches parent expectations
                    if (rightBucket->_leaf) {
                        assert(child->isDataRecord() && "Leaf bucket child must be a DataRecord");
                    } else {
                        assert(!child->isDataRecord() && "Internal split placed a DataRecord in a bucket child slot");
                    }

                    // Only assert if the store can tell us (durable). In-memory returns false.
                    persist::NodeKind actual = persist::NodeKind::Invalid;
                    if (store->get_node_kind(id, actual)) {
                        persist::NodeKind expected = compute_expected_kind(child);
                        if (actual != expected) {
                            trace() << "NodeKind mismatch after split: expected="
                                      << static_cast<int>(expected)
                                      << " actual=" << static_cast<int>(actual) << std::endl;
                            assert(false && "NodeKind mismatch after split");
                        }
                    }
                }
            }
        }
#endif
        
        // Right bucket housekeeping
        rightBucket->recalculateMBR();  // Ensure MBR matches actual adopted children
        rightBucket->markDirty();        // Mark for batch publishing
        
        // Step 3: Cache insert (mode-transparent key)
        const uint64_t cacheKey = Alloc::cache_key_for(rightRef.id, rightBucket);
        CacheNode* cachedSplitNode = this->_idx->getCache().add(cacheKey, reinterpret_cast<IRecord*>(rightBucket));

        // CRITICAL: Pin the bucket now that it's in cache.
        // markDirty() was called before cache insertion, so it couldn't pin.
        rightBucket->ensureDirtyPinned(cachedSplitNode);

        // Step 4: Mutate LEFT: erase moved tail, fix _n, recompute MBR
        if (moved > 0) {
            this->_children.erase(this->_children.begin() + split_index + 1,
                                  this->_children.begin() + old_n);
            this->_memoryUsage -= moved * sizeof(_MBRKeyNode*);
        }
        
        // CRITICAL FIX: Keep pivot at split_index => count is split_index + 1
        // Previous bug: _n = split_index dropped the pivot record!
        this->_n = split_index + 1;
        
#ifndef NDEBUG
        assert(this->_n == split_index + 1 && "Left bucket should retain pivot");
        assert(this->_n + moved == old_n && "Total children should be preserved");
#endif
        
        // Recalculate MBR based on remaining children
        this->recalculateMBR();
        
        // Mark dirty since this bucket was split (structural change)
        this->markDirty();

#ifndef NDEBUG
        // Validate both buckets after split to catch corruption immediately
        XTREE_DEBUG_VALIDATE_CHILDREN(this);
        XTREE_DEBUG_VALIDATE_CHILDREN(rightBucket);
#endif

        // Step 5: Bottom-up parent update
        if(this->_parent == NULL) {
            this->splitRoot(thisCacheNode, cachedSplitNode);
        } else {
            this->splitNode(thisCacheNode, cachedSplitNode);
        }
        // Transaction-level commit() will make everything atomic to readers
    }

    // Make sure a freshly created bucket is LIVE in the OT before we wire it
    // into a parent KN. Does nothing in IN_MEMORY mode.
    template<class RecordType>
    static inline void ensure_bucket_live(IndexDetails<RecordType>* idx,
                                          XTreeBucket<RecordType>* bucket,
                                          typename XTreeBucket<RecordType>::CacheNode* bucketCN /*may be null*/) noexcept(false) {
        if (!idx->hasDurableStore()) return;

        auto* store = idx->getStore();
        persist::NodeKind kind{};
        const persist::NodeID nid = bucket->getNodeID();

        // If already LIVE, we're done
        if (store->get_node_kind(nid, kind)) return;

        // Persist bucket body so LIVE points at a valid payload (no fsync here).
        // Precondition: nid is RESERVED and was allocated for this bucket.

        // Use the reallocation-aware publishing infrastructure
        // This handles capacity overflow, reallocates if needed, and updates parent refs
        using Alloc = XTreeAllocatorTraits<RecordType>;
        try {
            auto old_id = bucket->getNodeID();
            auto pub_result = Alloc::publish_with_realloc(idx, bucket);

            // If the bucket was reallocated to a new NodeID, update cache and parent reference
            if (pub_result.id.valid() && pub_result.id != old_id) {
                // Rekey the cache entry from old NodeID to new NodeID
                // This maintains cache consistency after COW reallocation
                idx->getCache().rekey(old_id.raw(), pub_result.id.raw());

                // CRITICAL: Update the parent's _MBRKeyNode to reference the new NodeID.
                // Without this, the parent bucket will still serialize/reference the old NodeID,
                // causing "not found" errors during traversal after reload.
                if (auto* parentKN = bucket->getParent()) {
                    parentKN->setNodeID(pub_result.id);

                    // CRITICAL FIX: Mark the parent bucket dirty since its child reference changed.
                    // Without this, the parent's wire format would still have the old child NodeID,
                    // causing child nodes to become unreachable after recovery.
                    auto* parentBucket = bucket->parent_bucket();
                    if (parentBucket) {
                        // IMPORTANT: Clear enlisted flag first, then re-mark dirty.
                        // If parent was already in the dirty batch, markDirty() would silently
                        // fail due to try_enlist(). By clearing first, we ensure parent gets
                        // re-added to the dirty list for re-serialization.
                        parentBucket->clearEnlistedFlag();
                        parentBucket->markDirty();
#ifndef NDEBUG
                        trace() << "[ENSURE_BUCKET_LIVE_REALLOC] Marked parent " << parentBucket->getNodeID().raw()
                                  << " dirty (isRoot=" << (parentBucket->getParent() == nullptr) << ")"
                                  << std::endl;
#endif
                    }
                }

#ifndef NDEBUG
                trace() << "[ENSURE_BUCKET_LIVE] Rekeyed cache: "
                          << old_id.raw() << " -> " << pub_result.id.raw() << "\n";
#endif
            }
        } catch (...) {
            // Publish failed: node remains RESERVED (caller can abort reservation)
            throw;
        }

#ifndef NDEBUG
        // NOTE: get_node_kind() only returns true for LIVE entries.
        // In the durable write path, buckets remain STAGED (RESERVED) until commit(),
        // so we use is_node_present() to verify existence regardless of state.
        const persist::NodeID final_nid = bucket->getNodeID();  // May have changed during reallocation
        bool staged = false;
        bool present = store->is_node_present(final_nid, &staged);
        assert(present && "bucket must exist (staged or live) right after publish");
#endif
    }

    template< class RecordType >
    void XTreeBucket<RecordType>::splitRoot(CacheNode* thisCacheNode, CacheNode* cachedSplitBucket) {
        using Alloc = XAlloc<RecordType>;
        
        // Right sibling already allocated & cached upstream
        auto* splitBucket = reinterpret_cast<XTreeBucket<RecordType>*>(cachedSplitBucket->object);
        assert(splitBucket && "cachedSplitBucket must point to a valid bucket");
        assert(this->_leaf == splitBucket->_leaf && "siblings should agree on leaf-ness");
        
        // Step 1: Allocate a new root (Internal, isRoot=true)
        auto rootRef = Alloc::allocate_bucket(
            this->_idx, persist::NodeKind::Internal,
            /*isRoot*/ true
        );
        auto* rootBucket = rootRef.ptr;
        Alloc::record_write(this->_idx, rootBucket);  // Optional COW/metrics

        // Ensure root is internal (even though allocate_bucket should handle this)
        rootBucket->_leaf = false;

        // Root invariants: new root should have no parent and should have a key for MBR calculation
        assert(rootBucket->_parent == nullptr && "New root must have no parent");
        assert(rootBucket->_key != nullptr && "New root must have key for MBR recalculation");

        // Debug: Check rootBucket->_key state immediately after allocation
        trace() << "[SPLIT_ROOT_DEBUG] After allocate: _key=" << (void*)rootBucket->_key
                  << " _key->data()=" << (void*)rootBucket->_key->data()
                  << " _area=0x" << std::hex << rootBucket->_key->debug_area_value() << std::dec
                  << " valid=" << rootBucket->_key->debug_check_area()
                  << std::endl;
        
        // Step 2: Cache the new root under mode-transparent identity
        const uint64_t rootKey = Alloc::cache_key_for(rootRef.id, rootBucket);
        CacheNode* cachedRootNode = this->_idx->getCache().add(rootKey, reinterpret_cast<IRecord*>(rootBucket));

        // Debug: Check after caching
        if (!rootBucket->_key->debug_check_area()) {
            trace() << "[SPLIT_ROOT_DEBUG] CORRUPTION after caching: _area=0x"
                      << std::hex << rootBucket->_key->debug_area_value() << std::dec << std::endl;
        }

        // Step 3: Wire sibling pointers (legacy logic preserved)
        // Link the siblings
        this->setNextChild(splitBucket);
        this->_prevChild = rootBucket;
        splitBucket->_prevChild = this;
        rootBucket->setNextChild(this);

        // Debug: Check after sibling wiring
        if (!rootBucket->_key->debug_check_area()) {
            trace() << "[SPLIT_ROOT_DEBUG] CORRUPTION after sibling wiring: _area=0x"
                      << std::hex << rootBucket->_key->debug_area_value() << std::dec << std::endl;
        }

        // Step 4: Insert both children into the new root
#ifndef NDEBUG
        // Sanity check: Both children must have valid NodeIDs before insertion
        assert(this->hasNodeID() && this->getNodeID().valid() &&
               "Left child (original root) must have valid NodeID during splitRoot");
        assert(splitBucket->hasNodeID() && splitBucket->getNodeID().valid() &&
               "Right child (split bucket) must have valid NodeID during splitRoot");

        // Both children (original root and split bucket) must agree on leaf-ness
        // They can be leaf buckets (first root split) or internal buckets (cascade split)
        assert(this->_leaf == splitBucket->_leaf && "Siblings must agree on leaf-ness");
#endif

        // Debug output for root split (always runs, even in Release mode)
        trace() << "[SPLIT_ROOT] newRoot=" << rootBucket->getNodeID().raw()
                  << " left=" << this->getNodeID().raw()
                  << " right=" << splitBucket->getNodeID().raw() << std::endl;

        // --- Step 4: Wire both children into the new root (structural, no insertHere) ---

        // Ensure both children are LIVE before parent wiring
        ensure_bucket_live(this->_idx, /*left*/ this,            thisCacheNode);
        ensure_bucket_live(this->_idx, /*right*/ splitBucket,    cachedSplitBucket);

        // Debug: Check after ensure_bucket_live
        if (!rootBucket->_key->debug_check_area()) {
            trace() << "[SPLIT_ROOT_DEBUG] CORRUPTION after ensure_bucket_live: _area=0x"
                      << std::hex << rootBucket->_key->debug_area_value() << std::dec << std::endl;
        }

#ifndef NDEBUG
        // Verify both children are present (staged or live) before wiring
        if (this->_idx->hasDurableStore()) {
            auto* store = this->_idx->getStore();
            persist::NodeKind tmp{};
            bool okL = store->is_node_present(this->getNodeID());
            assert(okL && "left child must be present before wiring into parent");
            bool okR = store->is_node_present(splitBucket->getNodeID());
            assert(okR && "right child must be present before wiring into parent");
        }
#endif

        _MBRKeyNode* left_kn = rootBucket->_kn(rootBucket->_n++);

        // Debug: Check after _kn(0)
        if (!rootBucket->_key->debug_check_area()) {
            trace() << "[SPLIT_ROOT_DEBUG] CORRUPTION after _kn(0): _area=0x"
                      << std::hex << rootBucket->_key->debug_area_value() << std::dec << std::endl;
        }

        // Use a stable copy of the MBR to avoid aliasing/UAF
        KeyMBR stable_mbr = *this->_key;
        left_kn->setDurableBucketChild(stable_mbr, this->getNodeID(), this->_leaf);
        left_kn->setCacheAlias(thisCacheNode);
        left_kn->_owner = rootBucket;
        this->setParent(left_kn);

        // Debug: Check after left child wiring
        if (!rootBucket->_key->debug_check_area()) {
            trace() << "[SPLIT_ROOT_DEBUG] CORRUPTION after left child wiring: _area=0x"
                      << std::hex << rootBucket->_key->debug_area_value() << std::dec << std::endl;
        }

        _MBRKeyNode* right_kn = rootBucket->_kn(rootBucket->_n++);

        // Debug: Check after _kn(1) - always print to verify this checkpoint executes
        trace() << "[SPLIT_ROOT_DEBUG] After _kn(1): _area=0x"
                  << std::hex << rootBucket->_key->debug_area_value() << std::dec
                  << " valid=" << rootBucket->_key->debug_check_area() << std::endl;

        right_kn->setDurableBucketChild(*splitBucket->_key, splitBucket->getNodeID(), splitBucket->_leaf);
        right_kn->setCacheAlias(cachedSplitBucket);
        right_kn->_owner = rootBucket;
        splitBucket->setParent(right_kn);

        // Debug: Check after right child wiring
        if (!rootBucket->_key->debug_check_area()) {
            trace() << "[SPLIT_ROOT_DEBUG] CORRUPTION after right child wiring: _area=0x"
                      << std::hex << rootBucket->_key->debug_area_value() << std::dec << std::endl;
        }

#ifndef NDEBUG
        // Verify cache aliasing is working correctly
        assert(left_kn->getCacheRecord()  == thisCacheNode);
        assert(right_kn->getCacheRecord() == cachedSplitBucket);

        // Verify wiring is exactly as intended (no search needed)
        assert(left_kn->_owner  == rootBucket && "Left child owner must be root");
        assert(right_kn->_owner == rootBucket && "Right child owner must be root");
        assert(this->_parent         == left_kn  && "Left bucket parent must be left_kn");
        assert(splitBucket->_parent  == right_kn && "Right bucket parent must be right_kn");

        // Verify KN→NodeID parity
        assert(left_kn->hasNodeID()  && left_kn->getNodeID()  == this->getNodeID());
        assert(right_kn->hasNodeID() && right_kn->getNodeID() == splitBucket->getNodeID());
#endif

        // Debug: Check rootBucket->_key state before recalculateMBR
        trace() << "[SPLIT_ROOT_DEBUG] Before recalculateMBR: rootBucket=" << (void*)rootBucket
                  << " _key=" << (void*)rootBucket->_key
                  << " _key->data()=" << (void*)rootBucket->_key->data()
                  << " _area=0x" << std::hex << rootBucket->_key->debug_area_value() << std::dec
                  << " valid=" << rootBucket->_key->debug_check_area()
                  << " _n=" << rootBucket->_n
                  << " NodeID=" << rootBucket->getNodeID().raw()
                  << std::endl;

        // Recompute root's MBR from children, then propagate once
        rootBucket->recalculateMBR();
        rootBucket->propagateMBRUpdate(cachedRootNode);

        // Step 6: Register root identity (pointer-cache key + durable NodeID + cache node)
        this->_idx->setRootIdentity(rootKey, rootRef.id, cachedRootNode);

        // Step 6b: Notify IndexDetails of root split to trigger version increment
        // This ensures root_cache_node() will auto-refresh on next access
        this->_idx->on_root_split(rootRef.id);

        // Critical check: ensure the old root (now left child) remains present.
        // Use is_node_present() which accepts both RESERVED and LIVE states,
        // since during split the node may still be RESERVED (not yet committed).
        if (this->_idx->hasDurableStore()) {
            auto* store = this->_idx->getStore();

            bool staged = false;
            const bool present = store->is_node_present(this->getNodeID(), &staged);
            if (!present) {
                trace() << "[OT_ERROR] Old root missing from OT after split! nid="
                          << this->getNodeID().raw() << std::endl;
#ifndef NDEBUG
                assert(false && "[splitRoot] Old root must be present (RESERVED or LIVE) in ObjectTable");
#endif
            }

            // Only probe kind if NOT staged (probing staged may deref transient state in some backends)
            if (!staged) {
                persist::NodeKind k{};
                if (store->get_node_kind(this->getNodeID(), k)) {
                    const auto expected = this->_leaf ? persist::NodeKind::Leaf
                                                      : persist::NodeKind::Internal;
                    if (k != expected) {
                        trace() << "[OT_ERROR] Old root has wrong OT kind after split! nid="
                                  << this->getNodeID().raw()
                                  << " kind=" << static_cast<int>(k)
                                  << " expected=" << static_cast<int>(expected) << std::endl;
#ifndef NDEBUG
                        assert(false && "[splitRoot] OT kind mismatch for old root");
#endif
                    }
                }
                // If get_node_kind returns false, it's not LIVE (could be RESERVED) – that's fine.
            }
        }

        // Step 7: Mark nodes dirty for batch publishing
        // All three nodes have structural changes
        this->markDirty();          // Left child (parent changed)
        splitBucket->markDirty();   // Right child (new node)
        rootBucket->markDirty();    // New root (new node)

#ifndef NDEBUG
        // Validate new root has exactly 2 bucket children
        assert(!rootBucket->_leaf && "New root must be internal");
        assert(rootBucket->n() == 2 && "New root should have exactly 2 children");
        XTREE_DEBUG_VALIDATE_CHILDREN(rootBucket);
#endif
    }

    template< class RecordType >
    void XTreeBucket<RecordType>::splitNode(CacheNode* thisCacheNode, CacheNode* cachedSplitBucket) {
        // Assert inputs and alias sanity up front
        assert(thisCacheNode && "thisCacheNode must be non-null");
        assert(cachedSplitBucket && "cachedSplitBucket must be non-null");
        auto* splitBucket = reinterpret_cast<XTreeBucket<RecordType>*>(cachedSplitBucket->object);
        assert(splitBucket && "cachedSplitBucket must point to a valid bucket");
        assert(cachedSplitBucket->object == reinterpret_cast<IRecord*>(splitBucket) &&
               "cachedSplitBucket->object must equal splitBucket");
        assert(this->_leaf == splitBucket->_leaf && "siblings should agree on leaf-ness");

        // Parent must be an internal bucket
        auto* parent = this->parent_bucket();
        assert(parent && "parent bucket must be resolvable");
#ifndef NDEBUG
        assert(!parent->_leaf && "Parent of a bucket must be internal");
#endif

        // Step 1: Wire sibling links (no parent pointer set yet on splitBucket)
        // NOTE: Sibling links are optimizations for traversal, not critical for correctness.
        // With cache eviction enabled, _nextChild can become a dangling pointer to freed memory.
        // Rather than risk use-after-free, we skip sibling link maintenance in DURABLE mode with eviction.
        // The parent's _children vector maintains proper ordering.
        const bool eviction_enabled = this->_idx && this->_idx->hasDurableStore() &&
                                      this->_idx->getCache().getMaxMemory() > 0;
        if (!eviction_enabled) {
            // IN_MEMORY or no eviction: safe to use raw sibling pointers
            auto* next = this->_nextChild;
            splitBucket->setNextChild(next);      // Right sibling -> next
            splitBucket->_prevChild = this;       // Right sibling <- this
            this->setNextChild(splitBucket);      // This -> right sibling
            if (next && next->_prevChild != splitBucket) {
                next->_prevChild = splitBucket;   // ensure back-link consistency
            }
        } else {
            // DURABLE with eviction: skip sibling chain (may have dangling pointers)
            splitBucket->_prevChild = this;       // Right sibling <- this (safe, splitBucket is fresh)
            this->setNextChild(splitBucket);      // This -> right sibling
            splitBucket->setNextChild(nullptr);   // Clear outgoing link
        }

        // Step 2: Mark mutated children dirty for batch publishing
        this->markDirty();
        splitBucket->markDirty();

        // Step 3: Wire right sibling into parent (structural, no insertHere)
        // Use NodeID-based matching instead of pointer-based (post-reallocation safe)
        int left_idx = -1;
        for (unsigned i = 0; i < parent->_n; ++i) {
            auto* kn = parent->_kn(i);
            if (!kn || kn->isDataRecord()) continue;
            if (kn->hasNodeID()) {
                if (kn->getNodeID() == this->getNodeID()) {
                    left_idx = static_cast<int>(i);
                    break;
                }
            } else if (this->_parent && !kn->hasNodeID() && kn == this->_parent) { // secondary fallback for mixed/staged states
                left_idx = static_cast<int>(i);
                break;
            }
        }
        assert(left_idx >= 0 && "Left child's KN not found in parent");

#ifndef NDEBUG
        // Assert uniqueness: no other child should have the same NodeID
        for (unsigned j = left_idx + 1; j < parent->_n; ++j) {
            assert(!(parent->_kn(j)->hasNodeID() && parent->_kn(j)->getNodeID() == this->getNodeID()) &&
                   "duplicate child NodeID under same parent");
        }
#endif

        _MBRKeyNode* left_kn = parent->_kn(left_idx);


        // Rebind child's parent KN if it drifted due to reallocation/rebuild
        if (this->_parent != left_kn) {
#ifndef NDEBUG
            trace() << "[SPLIT_NODE] Rebinding left child _parent KN ("
                      << (void*)this->_parent << " -> " << (void*)left_kn
                      << ") for child nid=" << this->getNodeID().raw() << "\n";
#endif
            this->setParent(left_kn);
        }

#ifndef NDEBUG
        // Extra parity check after rebind
        assert(this->_parent == left_kn);

        // Sanity: left_kn must not be a self-reference to the parent bucket
        if (auto* cn = left_kn->getCacheRecord()) {
            auto* maybeParent = reinterpret_cast<XTreeBucket<RecordType>*>(cn->object);
            assert(maybeParent != parent && "left_kn cache alias points to parent bucket (self-ref)");
        }

        assert((unsigned)(left_idx + 1) <= parent->_n && "insert position out of range");
#endif

        // DURABLE guard: make both children present before wiring
        persist::NodeID left_old = this->getNodeID();
        ensure_bucket_live(this->_idx, this,         thisCacheNode);
        ensure_bucket_live(this->_idx, splitBucket,  cachedSplitBucket);

        // REFRESH pointers after potential relocation
        auto* curLeft  = thisCacheNode && thisCacheNode->object
                       ? reinterpret_cast<XTreeBucket<RecordType>*>(thisCacheNode->object)
                       : this;
        auto* curRight = cachedSplitBucket && cachedSplitBucket->object
                       ? reinterpret_cast<XTreeBucket<RecordType>*>(cachedSplitBucket->object)
                       : splitBucket;

        // Cache parent once for collision detection
        auto* parent_after = curLeft->parent_bucket();

        // Verify no self-alias corruption (cache should be consistent with rekey)
#ifndef NDEBUG
        if (auto* cn = left_kn->getCacheRecord()) {
            if (cn->object == reinterpret_cast<IRecord*>(parent_after)) {
                trace() << "[CACHE_CORRUPTION] left_kn cache record points to parent bucket!\n"
                          << "  parent NodeID: " << parent_after->getNodeID().raw() << "\n"
                          << "  expected child NodeID: " << curLeft->getNodeID().raw() << "\n";
                assert(false && "Cache self-alias corruption detected - rekey should have prevented this");
            }
        }
#endif

#ifndef NDEBUG
        // Detect NodeID collision between left child and parent (allocator bug)
        if (parent_after && curLeft->getNodeID() == parent_after->getNodeID()) {
            trace() << "[ID_COLLISION] left child NodeID matches parent after split publish: "
                      << curLeft->getNodeID().raw() << "\n";
            assert(false && "allocator/id-publish must never collide with parent NodeID");
        }

        // Detect NodeID collision between right child and parent (allocator bug)
        if (parent_after && curRight->getNodeID() == parent_after->getNodeID()) {
            trace() << "[ID_COLLISION] right child NodeID matches parent after split publish: "
                      << curRight->getNodeID().raw() << "\n";
            assert(false && "allocator/id-publish must never collide with parent NodeID");
        }

        // Detect NodeID collision between siblings (allocator bug)
        if (curLeft->getNodeID() == curRight->getNodeID()) {
            trace() << "[ID_COLLISION] left and right siblings have identical NodeIDs: "
                      << curLeft->getNodeID().raw() << "\n";
            assert(false && "allocator/id-publish must assign unique NodeIDs to siblings");
        }
#endif

        // If left child reallocated, refresh its existing KN in parent
        if (curLeft->getNodeID() != left_old) {
#ifndef NDEBUG
            trace() << "[SPLIT_NODE] Left child reallocated: "
                      << left_old.raw() << " -> " << curLeft->getNodeID().raw() << "\n";
#endif
            // Re-stamp the left KN with the new durable NodeID and restore alias
            // Use canonical left_kn instead of potentially stale parent pointers
            // Use a stable copy of the MBR to avoid aliasing/UAF
            KeyMBR stable_mbr_left = *curLeft->_key;
            left_kn->setDurableBucketChild(stable_mbr_left, curLeft->getNodeID(), curLeft->_leaf);
            if (thisCacheNode) left_kn->setCacheAlias(thisCacheNode);

            // Verify no self-alias corruption after reallocation
#ifndef NDEBUG
            if (auto* cn = left_kn->getCacheRecord()) {
                if (cn->object == reinterpret_cast<IRecord*>(parent_after)) {
                    trace() << "[CACHE_CORRUPTION] left_kn cache record points to parent after realloc!\n";
                    assert(false && "Cache self-alias corruption after reallocation");
                }
            }
#endif

#ifndef NDEBUG
            assert(left_kn->getNodeID() == curLeft->getNodeID());
#endif
        }

#ifndef NDEBUG
        // Verify both children are present (staged or live) before wiring
        if (curLeft->_idx->hasDurableStore()) {
            auto* store = curLeft->_idx->getStore();
            bool okL = store->is_node_present(curLeft->getNodeID());
            assert(okL && "left child must be present before sibling wiring");
            bool okR = store->is_node_present(curRight->getNodeID());
            assert(okR && "right child must be present before wiring into parent");
        }
#endif

        // ============================================================
        // DIRECT SIBLING INSERTION (original working approach)
        // Insert sibling via kn() which handles both in-memory and durable modes.
        // This may promote parent to supernode if capacity exceeded.
        // Cascade split at MAX_FANOUT prevents unbounded growth.
        // ============================================================

        // Insert sibling into parent via kn() - this handles all wiring
        parent->kn(cachedSplitBucket);

        // Find the KN that was just created for the sibling
        _MBRKeyNode* right_kn = nullptr;
        for (unsigned i = 0; i < parent->_n; ++i) {
            auto* kn = parent->_kn(i);
            if (kn && !kn->isDataRecord() && kn->hasNodeID()) {
                if (kn->getNodeID() == curRight->getNodeID()) {
                    right_kn = kn;
                    break;
                }
            }
        }
        assert(right_kn && "Sibling KN must exist in parent after kn() insertion");

        // Wire the sibling's parent pointer
        curRight->setParent(right_kn);

#ifndef NDEBUG
        // Verify wiring
        assert(right_kn->getNodeID() == curRight->getNodeID());
        assert(curRight->_parent == right_kn);

        // Verify no self-alias corruption
        if (auto* cn = right_kn->getCacheRecord()) {
            if (cn->object == reinterpret_cast<IRecord*>(parent)) {
                trace() << "[CACHE_CORRUPTION] right_kn cache record points to parent!\n";
                assert(false && "Cache self-alias corruption for right child");
            }
        }

        // Sibling NodeID uniqueness check
        unsigned dups_right = 0;
        for (unsigned i = 0; i < parent->_n; ++i) {
            auto* pkn = parent->_kn(i);
            if (pkn && !pkn->isDataRecord() && pkn->hasNodeID()) {
                if (pkn->getNodeID() == curRight->getNodeID()) ++dups_right;
            }
        }
        assert(dups_right == 1 && "Right sibling NodeID collision detected under parent");

        // Parent is internal
        assert(!parent->_leaf);
        XTREE_DEBUG_VALIDATE_CHILDREN(parent);
#endif

        // Get parent's cache node for MBR propagation
        using Alloc = XTreeAllocatorTraits<RecordType>;
        const uint64_t pKey = Alloc::cache_key_for(parent->getNodeID(), parent);
        CacheNode* parentCN = this->_idx->getCache().lookup_or_attach(pKey, reinterpret_cast<IRecord*>(parent));

        // Recompute parent MBR and propagate
        parent->recalculateMBR();
        parent->propagateMBRUpdate(parentCN);
        parent->markDirty();

        // ============================================================
        // CASCADE SPLIT: If parent exceeds XTREE_M, try to split
        // X-tree semantics: try split if overflow, stay supernode if high overlap,
        // force split at MAX_FANOUT. This propagates up to the root.
        // ============================================================
        if (parent->_n > XTREE_M) {
            // Try cascade split on parent - may split, become supernode, or force split
            parent->forceCascadeSplit(parentCN);
        }
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
#ifdef _DEBUG
        trace() << "VISITED " << nodesVisited << " NODES";
#endif
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
        Iterator<RecordType>* iter = new Iterator<RecordType>(thisCacheNode, searchKey, static_cast<SearchType>(queryType), this->_idx);
        return iter;
    }

} // namespace xtree
