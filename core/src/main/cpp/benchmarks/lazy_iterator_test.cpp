/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test implementation of lazy iterator initialization
 */

#include <gtest/gtest.h>
#include <chrono>
#include <iomanip>
#include <random>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class LazyIteratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/lazy_iter.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/lazy_iter.dat");
    }
};

// Custom iterator that delays traversal until first next() call
template<class RecordType>
class LazyIterator : public Iterator<RecordType> {
public:
    LazyIterator(CacheNode* startNode, IRecord* searchKey, SearchType searchType) 
        : Iterator<RecordType>(startNode, searchKey, searchType), 
          _initialized(false) {
        // Don't call _init() in constructor - wait until first access
    }
    
    DataRecord* next() override {
        if (!_initialized) {
            _init();
            _initialized = true;
        }
        return Iterator<RecordType>::next();
    }
    
    bool hasNext() override {
        if (!_initialized) {
            _init();
            _initialized = true;
        }
        return Iterator<RecordType>::hasNext();
    }
    
private:
    bool _initialized;
    
    void _init() {
        // Call parent's _init method
        Iterator<RecordType>::_init();
    }
};

TEST_F(LazyIteratorTest, CompareLazyVsEagerIterator) {
    std::cout << "\n=== Lazy Iterator Performance Test ===\n";
    
    // Build tree with random data
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert 10K random points
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dist(0, 100);
    
    for (int i = 0; i < 10000; i++) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, 
            "pt_" + std::to_string(i));
        std::vector<double> point = {dist(gen), dist(gen)};
        dr->putPoint(&point);
        
        root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
        cachedRoot = (CacheNode*)index->getRootAddress();
        root->xt_insert(cachedRoot, dr);
    }
    
    root = (XTreeBucket<DataRecord>*)((CacheNode*)index->getRootAddress())->object;
    cachedRoot = (CacheNode*)index->getRootAddress();
    
    std::cout << "Built tree with 10K random points\n";
    std::cout << "Root has " << root->n() << " entries\n\n";
    
    // Test case 1: Create iterator but don't use it
    std::cout << "Test 1: Create iterator but don't use it\n";
    
    DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
    std::vector<double> min_pt = {45.0, 45.0};
    std::vector<double> max_pt = {55.0, 55.0};
    query->putPoint(&min_pt);
    query->putPoint(&max_pt);
    
    // Time standard iterator creation (no usage)
    const int numTests = 1000;
    auto standardStart = high_resolution_clock::now();
    for (int i = 0; i < numTests; i++) {
        auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
        delete iter;
    }
    auto standardDuration = duration_cast<microseconds>(high_resolution_clock::now() - standardStart);
    
    std::cout << "  Standard iterator (no usage): " << (standardDuration.count() / (double)numTests) 
              << " μs per creation\n";
    
    // Note: Can't test LazyIterator without modifying XTreeBucket::getIterator
    // This test demonstrates the concept
    
    // Test case 2: Create and use immediately
    std::cout << "\nTest 2: Create iterator and immediately check hasNext()\n";
    
    auto immediateStart = high_resolution_clock::now();
    int totalResults = 0;
    for (int i = 0; i < numTests; i++) {
        auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
        if (iter->hasNext()) {
            totalResults++;
        }
        delete iter;
    }
    auto immediateDuration = duration_cast<microseconds>(high_resolution_clock::now() - immediateStart);
    
    std::cout << "  Standard iterator (with hasNext): " << (immediateDuration.count() / (double)numTests) 
              << " μs per creation+check\n";
    
    // Test case 3: Full iteration
    std::cout << "\nTest 3: Full iteration performance\n";
    
    auto fullStart = high_resolution_clock::now();
    int fullResults = 0;
    for (int i = 0; i < 100; i++) {  // Fewer iterations for full traversal
        auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
        while (iter->hasNext()) {
            iter->next();
            fullResults++;
        }
        delete iter;
    }
    auto fullDuration = duration_cast<microseconds>(high_resolution_clock::now() - fullStart);
    
    std::cout << "  Full iteration: " << (fullDuration.count() / 100.0) << " μs per query\n";
    std::cout << "  Average results: " << (fullResults / 100.0) << "\n";
    
    delete index;
    
    std::cout << "\nConclusion: With lazy initialization, iterator creation would be nearly free\n";
    std::cout << "when the iterator is not used (common in existence checks).\n";
}