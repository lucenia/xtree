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

#include "./util/logmanager.h"
#include "config.h"
#include "util.h"
#include "indexdetails.h"
#include "keymbr.h"

namespace xtree {

#pragma pack(1)

    // enumerator for breadth-first, depth-first traversal
    enum TraversalOrder { BFS, DFS };

    // search type
    enum SearchType { CONTAINS, INTERSECTS };

    // used for void templates
    struct Unit {};

    // forward declaration of XTree iterator
    template< class RecordType >
    class Iterator;

    /**
     * Interface for XTreeBucket and DataRecord
     */
    class IRecord {
    public:
//    	typedef char							char_type;
//    	typedef iostreams::seekable_device_tag	category;

        IRecord() : _key(NULL) {}
        IRecord(KeyMBR* key) : _key(key) {
#ifdef _DEBUG
            if(key == NULL) {
                log() << "IRecord: KEY IS NULL!!!" << endl;
                log() << "Memory usage: " << getAvailableSystemMemory() << endl;
            }
#endif

        }

        virtual ~IRecord()=0;
        virtual KeyMBR* getKey() const=0;
        virtual const bool isLeaf() const=0;
        virtual const bool isDataNode() const=0;
        virtual long memoryUsage() const=0;
        virtual void purge() {};
//        virtual string toJSON() const=0;  // future

    protected:
        KeyMBR* _key;
    };

    inline IRecord::~IRecord() { delete _key; }

    /**
     * Represents a data node.
     *
     * Leaf Nodes contain DataRecords in _children
     *
     */
    class DataRecord : public IRecord {
    public:
        DataRecord(unsigned short dim, unsigned short prc, string s) : IRecord(new KeyMBR(dim, prc)), _rowid(s) {
#ifdef _DEBUG
            if(_key == NULL) {
                log() << "DataRecord: KEY IS NULL!!!" << endl;
                log() << "Memory usage: " << getAvailableSystemMemory() << endl;
            }
#endif
        }

        void putPoint(vector<double>* location) {
            _points.push_back(*location);
            _key->expandWithPoint(location);
        }

        string getRowID() { return _rowid; }

        KeyMBR* getKey() const { return _key; }

        const bool isLeaf() const { return true; }
        const bool isDataNode() const { return true; }

        long memoryUsage() const {   return (_points.size()) ?
                                        (_points.size()*_points[0].size()*sizeof(double)) : 0; }

        vector<vector<double>> getPoints() {
            return _points;
        }

        friend ostream& operator<<(ostream& os, const DataRecord dr) {
            os << "This DataRecord has " << dr._points.size() << "points";

            return os;
        }

        void purge() {
        	log() << "PURGING DATA RECORD" << endl;
        }

        /**
         * Serializes a DataRecord for persistence in HDFS
         **/
        template <class Archive>
        void serialize( Archive & ar ) const {
            ar( _points, _rowid );
        }

//        // read up to n characters from the DataRecord into the buffer s,
//        // returning the number of characters read; return -1 to indicate EOF
//        std::streamsize read(char_type* s, std::streamsize n) {
//        	std::streamsize amt =
//        }
//
//        // write up to n characters to the underlying sink into the buffer s,
//        // returning the number of characters written
//        std::streamsize write(const char* s, std::streamsize n) {
//
//        }
//
//        // seek to position off and return the new stream position.  The argument offInterpretation
//        // indicates how off is interpreted (std::ios_base::beg indicates offset from sequence beginning,
//        // std::ios_base::cur indicates offset from the current character position, std::ios_base::end
//        // indicates offset from the sequence end
//        iostreams::stream_offset seek(iostreams::stream_offset off, std::ios_base::seekdir offInterpretation) {
//
//        }

    private:
        // the hyper dimensional points for this record
        vector<vector<double>> _points;
        // the rowid in accumulo for which this record references
        string _rowid;
    };

    // forward declaration
    template< class Record > class XTreeBucket;

    /**
     * This is a semi-fixed width data component for
     * storage of mbr keys within the _children vector
     * of a XTreeBucket.
     */
    template< class RecordType >
    class __MBRKeyNode {
    public:
        typedef LRUCacheNode<IRecord, UniqueId, LRUDeleteObject> CacheNode;
        // reuse 8 bytes for either:
        //   a.  a unique ID for cache to locate the record on "disk"
        //   b.  the memory address of the cached object
        union RecordID {
            char id[8];
            CacheNode* address;
        };

        __MBRKeyNode<RecordType>() : _leaf(false), _cached(false),/* _record(NULL),*/ _recordKey(NULL) {}
        __MBRKeyNode<RecordType>(bool isLeaf, CacheNode* record)
            : _leaf(isLeaf) { this->setRecord(record); }

        bool getLeaf() { return _leaf; }
        void setLeaf(const bool leaf) { _leaf = leaf; }

        bool getCached() { return _cached; }
        void setCached(const bool cached) { _cached = cached; }

        ostream& getRecordID(ostream& os) {
            (_cached) ? os << _record.id : os << _record.address;
             return os;
        }

        /** @todo replace NULL with LRU lookup */
        CacheNode* getCacheRecord() { return (_cached) ? _record.address : NULL; }

        /** pull the record from cache */
        IRecord* getRecord(LRUCache<IRecord, UniqueId, LRUDeleteObject> &cache) {
                return (_cached) ?
                    reinterpret_cast<CacheNode*>(_record.address)->object :
                    cache.get( *(reinterpret_cast<UniqueId*>(&(_record.id))) ); }

        // sets the record based on the address of the IRecord (cached record)
        void setRecord(CacheNode* record) {
            _record.address = record;
            _recordKey = record->object->getKey();
            _cached = true;
        }
        // sets the record based on the unique id (disk based record)
        // TODO This does not fill out the same data as setRecord(record), but
        // it is not currently used
        void setRecord(UniqueId id) { memcpy(_record.id, id, sizeof(UniqueId));  }

        KeyMBR* getKey() { return _recordKey; }
        void setKey(KeyMBR* key) { _recordKey = key; }

        const KeyMBR* getKey() const { return _recordKey; }

        struct CumulativeOverlap {
            typedef const typename XTreeBucket<RecordType>::_MBRKeyNode* argument_type;
            typedef void result_type;

            __MBRKeyNode* _candidateKN; // *this mbr
            const KeyMBR *_key; // the proposed key
            typename vector<__MBRKeyNode*>::iterator *_start;
            double overlap;

            explicit CumulativeOverlap(/*const*/ __MBRKeyNode *ckn, const KeyMBR *key,
                typename vector<__MBRKeyNode*>::iterator* start) :
                _candidateKN(ckn), _key(key), _start(start), overlap(0.0) {}

            void operator() (const typename XTreeBucket<RecordType>::_MBRKeyNode* mbrkn) {
                double minY=0.0;
                double maxX=0.0;
                double areaOverlap = -1.0;

                if(_candidateKN != mbrkn) {
                    for(unsigned short d=0; d<_key->getDimensionCount()*2; d+=2) {
                        // we need to first enlarge the candidate KeyNode to enclose the given key
                        maxX = MAX(_candidateKN->getKey()->getBoxVal(d+1), _key->getBoxVal(d+1));
                        minY = mbrkn->getKey()->getBoxVal(d);
                        // compute the overlap area
                        if(areaOverlap == 0.0) break;
                        else {
                            if(minY>maxX) continue;
                            else
                                areaOverlap = abs(areaOverlap)*(abs(maxX-minY));
                        }
                    }
                    // then compute the area overlap with all KeyNodes (but this one)
                    if(areaOverlap<0.0)
                        areaOverlap = 0.0;
                    overlap+=areaOverlap;
                }
            }
        };

        const double overlapEnlargement(const KeyMBR* key, typename vector<__MBRKeyNode*>::iterator start,
                                        typename vector<__MBRKeyNode*>::iterator end) const {
            // walks through each element in keyVec and calculates the cumulative overlap area for
            // the enlarged(this->mbr) with every other MBR in keyVec
            CumulativeOverlap co = for_each(start, end, CumulativeOverlap(const_cast<__MBRKeyNode*>(this), key, &start));
            return co.overlap;
        }

        /**
         * Serializes a DataRecord for persistence in HDFS
         **/
        template <class Archive>
        void serialize( Archive & ar ) const {
            ar( _leaf, _cached, _record, _recordKey );
        }

    private:
        // Indicates whether this is a leaf or internal node
        // A leafnode is a bucket whose keys point to documents
        // An internal node is a bucket whose keys point to other buckets
        bool _leaf;

        // indicates whether this node is cached
        // this tells us whether the next 8 bytes is a pointer to an IRecord or a
        // unique Id to pull from disk
        bool _cached;

        // The record associated with this key.
        // NOTE: The record may be NULL, so we have _recordKey that will keep
        // the key in memory without requiring the full bucket.
        RecordID _record;
        KeyMBR* _recordKey;

        unsigned short size(const unsigned short &mbrBytes)  { 
            return mbrBytes + sizeof(bool) + sizeof(XTreeBucket<RecordType>*) + sizeof(unsigned char);
        }

        friend ostream& operator <<(ostream &os, const __MBRKeyNode kn) {
            os << kn._record << " isLeaf: " << kn._leaf << endl;
            return os;
        }
    }; // __MBRKeyNode



    /**
     * XTreeBucket
     *
     * @TODO: Better comments shortly
     */
    template< class Record >
    class XTreeBucket : public IRecord {
    public:
        // grant access to privates
        template< class R >
        friend class Iterator;

        typedef __MBRKeyNode<Record> _MBRKeyNode;
        typedef typename _MBRKeyNode::CacheNode CacheNode;
        typedef stack<CacheNode*> DFS;
        typedef queue<CacheNode*> BFS;

        XTreeBucket<Record>(IndexDetails<Record>* idx, bool isRoot, KeyMBR* key=NULL, const vector<_MBRKeyNode*>* sourceChildren = NULL,
            unsigned int split_index=0, bool isLeaf=true, unsigned int sourceN=0): IRecord(key),
            _memoryUsage(sizeof(XTreeBucket<Record>)), _idx(idx), _parent(NULL), _nextChild(NULL), _prevChild(NULL),
            _n(0), _isSupernode(false), _leaf(isLeaf) {
                // if source children are not provided
                if(sourceChildren==NULL)
                    generate_n(back_inserter(_children), XTREE_CHILDVEC_INIT_SIZE,
                        [&](){return new _MBRKeyNode();});
                else {
                    this->_n = sourceN-(split_index+1);
                    typename vector<_MBRKeyNode*>::const_iterator srcChildIter = (sourceChildren->begin()+split_index);
                    generate_n(back_inserter(_children), this->_n,
                        [&](){return *(++srcChildIter);});
                }

                // create the key for this bucket
                // NOTE: this memory space is shared by _MBRKeyNode,
                // so when a XTreeBucket is paged, we need to be sure
                // not to free the Key
                if(_key == NULL) {
                    _key = new KeyMBR(_idx->getDimensionCount(), _idx->getPrecision());
                }

                _memoryUsage += (MAX(XTREE_CHILDVEC_INIT_SIZE, this->_n)*knSize()) + _key->memUsage();
        }

        // Destructor to clean up allocated memory
        ~XTreeBucket() {
            // Only delete the parent node if it was allocated by createParentKN
            // This fixes the memory leak detected by valgrind in splitRoot
            if (_parent != NULL) {
                delete _parent;
                _parent = NULL;
            }

            // Clean up pre-allocated but unused child nodes
            // These are nodes that were allocated in the constructor but never used
            // (i.e., nodes where index >= _n)
            for (size_t i = _n; i < _children.size(); i++) {
                if (_children[i] != NULL) {
                    delete _children[i];
                }
            }
            
            // Note: We don't delete active child nodes (i < _n) because they
            // are cached objects that should be managed by their owners

            // Note: _key is deleted by IRecord destructor
            
            // IMPORTANT: There is a known issue where splitRoot adds new buckets to the
            // real static cache, but tests using fake cache nodes don't clean them up.
            // This can cause memory leaks in tests. The proper fix is to ensure tests
            // clean up all cached buckets, not just fake cache nodes.
        }

        // generic traversal with a generic lambda function to be called on each
        // visit
        template<typename Result, typename Visit, typename TraversalOrder>
        Result traverse(CacheNode* thisCacheNode, Visit visit, ...) const;


        // returns the total memory consumption of JUST this bucket
        long memoryUsage() const { return this->_memoryUsage; }
        // returns the total memory consumption of this subtree
        long treeMemUsage(CacheNode* cachedNode) const;

        // get an iterator for traversing the tree
        Iterator<Record>* getIterator(CacheNode* thisCacheNode, IRecord* searchKey, int queryType);

        // wrapper around _insert that caches the record for insertion
        // into the tree
        void xt_insert(CacheNode* thisCacheNode, IRecord* record) {
            // add the record to the cache
            CacheNode* cachedRecord = this->_idx->getCache().add( _idx->getNextNodeID(), record);

            // insert the cached record into the tree
            _insert(thisCacheNode, cachedRecord);
        }

        // completely purges this XTreeBucket, along w/ all children buckets and data
        void xt_purge(CacheNode* thisCacheNode);

        /** @return the number of bytes required for the mbr */
        unsigned short mbrBytes() { return this->_mbrBytes; }

        const unsigned short knSize() const {  return (sizeof(_MBRKeyNode)); }
         //return (sizeof(bool) + sizeof(Record) + this->_mbrBytes); }

        // returns the number of children
        const int n() const { return _n; }

        virtual KeyMBR* getKey() const {
            return _key;
        }

        void setKey(KeyMBR* key) {
            // set the key data
            *(this->_key) = *key;
            // delete the
            delete key;
        }

        // returns the index details
        const IndexDetails<Record>* getIdxDetails() const { return this->_idx; }

        /**
         * Serializes a XTreeBucket for persistence in HDFS
         **/
        template <class Archive>
        void serialize( Archive & ar ) const {
            ar( _memoryUsage, _idx, _parent, _nextChild, _prevChild, _n, _isSupernode, _leaf, _children );
        }
        
        // print out this bucket for logging
        string toString(int indentLevel=0) {
            ostringstream oss;
            string indents;

            for(short i=0; i<indentLevel; ++i) indents += "\t";

            oss << indents << "this: " << this << endl;
            oss << indents << "this->_memoryUsage: " << this->_memoryUsage << endl;
            oss << indents << "this->_idx: " << this->_idx << endl;
            oss << indents << "this->_parent: " << this->_parent << endl;
            oss << indents << "this->_n: " << this->_n << endl;
            oss << indents << "this->_isSupernode: " << this->_isSupernode << endl;
            oss << indents << "this->_leaf: " << this->_leaf << endl;
            oss << indents << "this->_children->size(): " << this->_children.size() << endl;

            return oss.str();
        }

    protected:
        // accessors
        XTreeBucket<Record>* nextChild() const { return _nextChild; }
        const vector<_MBRKeyNode*>* getChildren() { return &_children; }

        // returns the size of the header
        unsigned short _headerSize() const { return (sizeof(XTreeBucket) + this->_mbrBytes); }
        // returns a pointer to the mbrData of *this* bucket 
        char * mbrData() { return this->data + 1; /*sizeof(bool) + sizeof(unsigned short);*/ }
        // returns a pointer to the data
        char * dataAt(short ofs) { return mbrData() + this->_mbrBytes + ofs; }
        // returns MBRKeyNode data as a char pointer
        const char* k(int i) { return static_cast<XTreeBucket<Record> *>(this)->dataAt((i*(knSize()))); }

        // creates or updates a key node
        _MBRKeyNode* kn(CacheNode* record, int n=-1) {
            // get the nth child
            _MBRKeyNode* newChild = (n < 0) ? _kn(_n++) : _kn(n);
            // set leaf flag
            newChild->setLeaf(record->object->isLeaf());
            // set cached flag
            newChild->setCached(true);
            // set the record pointer
            newChild->setRecord(record);
            // set the _MBRKeyNode Key pointer
            newChild->setKey(record->object->getKey());
            // update this bucket's mbr key
            /** @todo CRITICAL: If we're updating then we need
             *  to remove the old MBR and possibly contract the parent based
             *  on the updated MBR */
            this->_key->expand(*(record->object->getKey()));

            return newChild;
        }

        // returns pointer to a KeyNode in the vector (or memory mapped file)
        //_MBRKeyNode* kn(int i) const { return const_cast<XTreeBucket<Record> *>(this)->_kn(i); }
        _MBRKeyNode* _kn(unsigned int i) {
            if (i <= XTREE_M) {
                if(i>=this->_children.size()) {
                    if((this->_children.size()<<1) < XTREE_M )
                        this->_expandChildren(this->_children.size());
                    else
                        this->_expandChildren((XTREE_M+1)-this->_children.size());
                }
            } else {
                _expandSupernode();
            }
            return this->_children.at(i);
        }

        _MBRKeyNode* _expandSupernode() {
            assert(this->_n>=XTREE_M);
            // double the block size, or just add 1
            if (_n >= _children.size()) {
                (this->_n<=(XTREE_M*2)) ? _expandChildren(XTREE_M) : _expandChildren(1);
            }
            this->_isSupernode = true;
            return this->_children.at(this->_n);
        }

        void _expandChildren(unsigned int i) {
            generate_n(back_inserter(_children), i, [&](){return new _MBRKeyNode();});
            _memoryUsage += i*sizeof(_MBRKeyNode);
        }

        KeyMBR kmbr(int i, unsigned short dimension, unsigned short bitsize) const {
            /**@TODO check mbr types... should not need to reinterpret_cast here*/
            return KeyMBR(dimension, bitsize, this->_mbrBytes, reinterpret_cast<const char*>(kn(i)->mbr));
        }

        const bool isDataNode() const { return false; }
        const bool isLeaf() const { return this->_leaf; }
        bool hasLeaves() { return (this->_n>0) ? _kn(0)->getLeaf() : false; }

        /** xt_insert() is basically just a wrapper around this. */
        int _insert(CacheNode* thisCacheNode, CacheNode*  record);
        int insertHere(CacheNode* thisCacheNode, CacheNode* record);
        bool basicInsert(/*const KeyMBR& key,*/ /*IRecord* */ CacheNode* record);

        /**
         * purges this bucket from memory
         */
        void purge(CacheNode* thisCacheNode) {}

        // choose subtree (complex algorithm)
        XTreeBucket<Record>* chooseSubtree(CacheNode* record);

        /*****************
         * Split methods *
         *****************/
        bool split(CacheNode* thisCacheNode, CacheNode* cachedRecord);
        void splitCommit( CacheNode* thisCacheNode, KeyMBR* mbr1, KeyMBR* mbr2, const unsigned int &split_index );
        void splitRoot( CacheNode* thisCacheNode, CacheNode* cachedSplitNode);
        void splitNode( CacheNode* thisCacheNode, CacheNode* cachedSplitNode);

        _MBRKeyNode* createParentKN( CacheNode* cachedNode ) {
            assert(this->_parent == NULL);
            this->_parent = new _MBRKeyNode( false, cachedNode);
            _memoryUsage += sizeof(_MBRKeyNode);
            return this->_parent;
        }
        void setParent(_MBRKeyNode *parent) { this->_parent = parent; }
        void setNextChild(XTreeBucket<Record> *nextChild) { this->_nextChild = nextChild; }

    private:
        /** header data below */

        // memory usage for this bucket
        int _memoryUsage;                   // 4 bytes
        // details of the index
        IndexDetails<Record>* _idx;         // 8 bytes

        // pointer to _parent
        _MBRKeyNode* _parent;               // 8 bytes

        // pointer to "next child". Defined as the next sibling, or
        // the "first" node on the next level. Used for BFS
        XTreeBucket<Record>* _nextChild;    // 8 bytes
        // pointer to the "previous child". Defined as the previous sibling.
        XTreeBucket<Record>* _prevChild;    // 8 bytes
        // number of keys in the bucket
        unsigned int _n;                             // 4 bytes
        // is this a supernode
        bool _isSupernode;                  // 1 byte
        // internal or leaf node
        bool _leaf;                         // 1 byte
        // in memory child pointers
        vector<_MBRKeyNode*> _children;     // 24 bytes
                                            // 8 bytes from IRecord
    }; // XTreeBucket
   
    /**********************************************************
     * R Tree related functors used for sorting MBRKeyNodes
     **********************************************************/
    /**
     * InMemory key vector sort by minimum range value
     */
    template< class Record >
    struct SortKeysByRangeMin {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        const unsigned short _axis;
        explicit SortKeysByRangeMin( const unsigned short axis ) : _axis(axis) {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) const {
#ifdef _DEBUG
            if(key1 == NULL) log()  << "SortKeysByRangeMin: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByRangeMin: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log()  << "SortKeysByRangeMin: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByRangeMin: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            return key1->getKey()->getMin(_axis) < key2->getKey()->getMin(_axis);
        }
    };

    /**
     * InMemory key vector sorting by maximum range value
     */
    template< class Record >
    struct SortKeysByRangeMax {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        const unsigned short _axis;
        explicit SortKeysByRangeMax(const unsigned short axis) : _axis(axis) {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) const  {
#ifdef _DEBUG
            if(key1 == NULL) log() << "SortKeysByRangeMax: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByRangeMax: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log() << "SortKeysByRangeMax: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByRangeMax: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            return key1->getKey()->getMax(_axis) < key2->getKey()->getMax(_axis);
        }
    };

    /**
     * Expands a target MBR given an input MBR
     */
    template< class Record >
    struct StretchBoundingBox {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* argument_type;
        typedef void result_type;
        KeyMBR *_mbr;
        explicit StretchBoundingBox(KeyMBR *key) : _mbr(key) {}

        void operator() (const typename XTreeBucket<Record>::_MBRKeyNode*/*KeyMBR**/ mbr) {
            _mbr->expand(*(mbr->getKey()));
        }
    };

    /**
     * InMemory key vector sorting by area enlargement required for inserting a new key
     */
    template< class Record >
    struct SortKeysByAreaEnlargement {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        //const double _area;
        const KeyMBR* _key;
        bool* _zeroEnlargement;
        explicit SortKeysByAreaEnlargement(const KeyMBR* center, bool* zeroEnlargement) :
            _key(center/*.area()*/), _zeroEnlargement(zeroEnlargement)  {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) /*const*/ {
#ifdef _DEBUG
            if(key1 == NULL) log()  << "SortKeysByAreaEnlargement: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByAreaEnlargement: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log()  << "SortKeysByAreaEnlargement: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByAreaEnlargement: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            double key1AE = key1->getKey()->areaEnlargement(*_key);
            double key2AE = key2->getKey()->areaEnlargement(*_key);
            if(!(*_zeroEnlargement))
                *_zeroEnlargement = (key1AE == 0 || key2AE == 0);
            return key1AE < key2AE;
        }
    };

    /**
     * InMemory key vector sorting by overlap enlargement required for inserting a new key
     */
    template< class Record >
    struct SortKeysByOverlapEnlargement {
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* first_argument_type;
        typedef const typename XTreeBucket<Record>::_MBRKeyNode* second_argument_type;
        typedef bool result_type;
        const KeyMBR* _key;
        typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator _start;
        typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator _end;

        explicit SortKeysByOverlapEnlargement(const KeyMBR* key,
            typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator start,
            typename vector<typename XTreeBucket<Record>::_MBRKeyNode*>::iterator end) :
            _key(key), _start(start), _end(end) {}

        bool operator() (const typename XTreeBucket<Record>::_MBRKeyNode* key1,
                         const typename XTreeBucket<Record>::_MBRKeyNode* key2) const {
#ifdef _DEBUG
            if(key1 == NULL) log()  << "SortKeysByOverlapEnlargement: KEY 1 IS NULL!!!" << endl;
            else if(key1->getKey() == NULL) log() << "SortKeysByOverlapEnlargement: KEY 1 KEY DATA IS NULL!!!!" << endl;
            if(key2 == NULL) log()  << "SortKeysByOverlapEnlargement: KEY 2 IS NULL!!!" << endl;
            else if(key2->getKey() == NULL) log() << "SortKeysByOverlapEnlargement: KEY 2 KEY DATA IS NULL!!!" << endl;
#endif
            return key1->overlapEnlargement(_key, _start, _end) < key2->overlapEnlargement(_key, _start, _end);
        }
    };

#pragma pack()

} // namespace xtree
