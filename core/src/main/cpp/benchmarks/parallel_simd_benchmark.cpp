/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Parallel and SIMD optimized query benchmark
 */

#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>
#include <random>
#include <immintrin.h>  // For AVX/SSE intrinsics
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/util/cpu_features.h"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

// SIMD-optimized MBR intersection for 2D points
inline bool intersects_simd_2d(const int32_t* box1, const int32_t* box2) {
#ifdef __AVX2__
    // Load 4 int32_t values at once (minX, maxX, minY, maxY for each box)
    __m128i b1 = _mm_loadu_si128((__m128i*)box1);
    __m128i b2 = _mm_loadu_si128((__m128i*)box2);
    
    // Shuffle to arrange for comparison
    // We need: box1.maxX >= box2.minX && box2.maxX >= box1.minX &&
    //          box1.maxY >= box2.minY && box2.maxY >= box1.minY
    
    // Extract components
    __m128i b1_max = _mm_shuffle_epi32(b1, _MM_SHUFFLE(3, 3, 1, 1)); // maxY, maxY, maxX, maxX
    __m128i b2_min = _mm_shuffle_epi32(b2, _MM_SHUFFLE(2, 2, 0, 0)); // minY, minY, minX, minX
    __m128i b2_max = _mm_shuffle_epi32(b2, _MM_SHUFFLE(3, 3, 1, 1)); // maxY, maxY, maxX, maxX
    __m128i b1_min = _mm_shuffle_epi32(b1, _MM_SHUFFLE(2, 2, 0, 0)); // minY, minY, minX, minX
    
    // Compare: b1_max >= b2_min AND b2_max >= b1_min
    __m128i cmp1 = _mm_cmpgt_epi32(b1_max, b2_min);
    __m128i cmp2 = _mm_cmpgt_epi32(b2_max, b1_min);
    __m128i eq1 = _mm_cmpeq_epi32(b1_max, b2_min);
    __m128i eq2 = _mm_cmpeq_epi32(b2_max, b1_min);
    
    // Combine comparisons (greater than OR equal)
    __m128i ge1 = _mm_or_si128(cmp1, eq1);
    __m128i ge2 = _mm_or_si128(cmp2, eq2);
    
    // All comparisons must be true
    __m128i result = _mm_and_si128(ge1, ge2);
    
    // Extract result - we only care about positions 0 and 2 (X and Y comparisons)
    int mask = _mm_movemask_epi8(result);
    return (mask & 0x00F000F0) == 0x00F000F0;
#else
    // Fallback to optimized scalar version
    return !(box1[1] < box2[0] ||   // box1.maxX < box2.minX
             box2[1] < box1[0] ||   // box2.maxX < box1.minX
             box1[3] < box2[2] ||   // box1.maxY < box2.minY
             box2[3] < box1[2]);    // box2.maxY < box1.minY
#endif
}

// Worker function for parallel queries
void queryWorker(IndexDetails<DataRecord>* index, XTreeBucket<DataRecord>* root, CacheNode* cachedRoot,
                 const std::vector<std::pair<double, double>>& queries,
                 size_t start, size_t end, double boxSize,
                 std::atomic<int>& totalResults) {
    DataRecord* query = XAlloc<DataRecord>::allocate_record(index, 2, 32, "query");
    int localResults = 0;
    
    for (size_t i = start; i < end; i++) {
        query->getKey()->reset();
        std::vector<double> min_pt = {queries[i].first, queries[i].second};
        std::vector<double> max_pt = {queries[i].first + boxSize, queries[i].second + boxSize};
        query->putPoint(&min_pt);
        query->putPoint(&max_pt);
        
        auto iter = root->getIterator(cachedRoot, query, INTERSECTS);
        while (iter->hasNext()) {
            iter->next();
            localResults++;
        }
        delete iter;
    }
    
    // Query is managed by allocator, don't delete directly
    totalResults += localResults;
}

class ParallelSIMDBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/parallel_benchmark.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/parallel_benchmark.dat");
    }
    
    DataRecord* createPointRecord(IndexDetails<DataRecord>* index, const std::string& id, double x, double y) {
        DataRecord* dr = XAlloc<DataRecord>::allocate_record(index, 2, 32, id);
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        return dr;
    }
};

TEST_F(ParallelSIMDBenchmark, MultiThreadedQueries) {
    std::cout << "\n=== Multi-Threaded Query Performance ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"x", "y"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert test data
    std::cout << "Inserting 100,000 points...\n";
    const int GRID_SIZE = 316;
    for (int x = 0; x < GRID_SIZE; x++) {
        for (int y = 0; y < GRID_SIZE; y++) {
            DataRecord* dr = createPointRecord(index,
                "grid_" + std::to_string(x) + "_" + std::to_string(y),
                (double)x,
                (double)y
            );
            root->xt_insert(cachedRoot, dr);
        }
    }
    
    // Prepare queries
    const int NUM_QUERIES = 1000000;
    std::vector<std::pair<double, double>> queryPositions;
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(0, GRID_SIZE - 10);
    
    for (int i = 0; i < NUM_QUERIES; i++) {
        queryPositions.push_back({dis(gen), dis(gen)});
    }
    
    // Get root reference
    auto rootAddress = index->getRootAddress();
    CacheNode* cacheNode = (CacheNode*)rootAddress;
    XTreeBucket<DataRecord>* currentRoot = (XTreeBucket<DataRecord>*)(cacheNode->object);
    
    // Test with different thread counts
    std::vector<int> threadCounts = {1, 2, 4, 8, 16};
    
    std::cout << "\nSmall range queries (10x10 box):\n";
    std::cout << "Threads | Time (ms) | QPS | Speedup\n";
    std::cout << "--------|-----------|-----|--------\n";
    
    double baselineTime = 0;
    
    for (int numThreads : threadCounts) {
        std::atomic<int> totalResults(0);
        
        auto startTime = high_resolution_clock::now();
        
        if (numThreads == 1) {
            // Single-threaded baseline
            queryWorker(index, currentRoot, cacheNode, queryPositions, 
                       0, NUM_QUERIES, 10.0, totalResults);
        } else {
            // Multi-threaded execution
            std::vector<std::thread> threads;
            size_t queriesPerThread = NUM_QUERIES / numThreads;
            
            for (int t = 0; t < numThreads; t++) {
                size_t start = t * queriesPerThread;
                size_t end = (t == numThreads - 1) ? NUM_QUERIES : start + queriesPerThread;
                
                threads.emplace_back(queryWorker, index,
                                   currentRoot, cacheNode, 
                                   std::ref(queryPositions), start, end, 10.0,
                                   std::ref(totalResults));
            }
            
            for (auto& t : threads) {
                t.join();
            }
        }
        
        auto endTime = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(endTime - startTime);
        double timeMs = duration.count() / 1000.0;
        
        if (numThreads == 1) baselineTime = timeMs;
        
        double qps = NUM_QUERIES * 1000.0 / timeMs;
        double speedup = baselineTime / timeMs;
        
        std::cout << std::setw(7) << numThreads << " | "
                  << std::setw(9) << std::fixed << std::setprecision(1) << timeMs << " | "
                  << std::setw(5) << std::setprecision(0) << qps << " | "
                  << std::setw(6) << std::setprecision(2) << speedup << "x\n";
    }
    
    delete index;
}

TEST_F(ParallelSIMDBenchmark, SIMDIntersectionTest) {
    std::cout << "\n=== SIMD Intersection Performance ===\n";
    
    // Create test MBRs
    const int NUM_TESTS = 100000000;
    
    // Test data - aligned for SIMD
    alignas(16) int32_t box1[] = {10, 20, 30, 40};  // minX=10, maxX=20, minY=30, maxY=40
    alignas(16) int32_t box2[] = {15, 25, 35, 45};  // overlapping
    alignas(16) int32_t box3[] = {25, 35, 45, 55};  // non-overlapping
    
    // Warm up
    for (int i = 0; i < 1000; i++) {
        volatile bool result = intersects_simd_2d(box1, box2);
        (void)result;
    }
    
    // Test SIMD version
    {
        auto start = high_resolution_clock::now();
        int matches = 0;
        
        for (int i = 0; i < NUM_TESTS; i++) {
            if (intersects_simd_2d(box1, (i % 2) ? box2 : box3)) {
                matches++;
            }
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        std::cout << "SIMD intersection test:\n";
        std::cout << "  Total tests: " << NUM_TESTS << "\n";
        std::cout << "  Time: " << (duration.count() / 1000000.0) << " ms\n";
        std::cout << "  Rate: " << (NUM_TESTS * 1000.0 / duration.count()) << " M ops/sec\n";
        std::cout << "  Time per op: " << (duration.count() / (double)NUM_TESTS) << " ns\n";
        std::cout << "  Matches: " << matches << "\n";
    }
    
    // Test scalar version for comparison
    {
        auto start = high_resolution_clock::now();
        int matches = 0;
        
        for (int i = 0; i < NUM_TESTS; i++) {
            const int32_t* testBox = (i % 2) ? box2 : box3;
            bool intersects = !(box1[1] < testBox[0] || 
                               testBox[1] < box1[0] || 
                               box1[3] < testBox[2] || 
                               testBox[3] < box1[2]);
            if (intersects) matches++;
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        std::cout << "\nScalar intersection test:\n";
        std::cout << "  Total tests: " << NUM_TESTS << "\n";
        std::cout << "  Time: " << (duration.count() / 1000000.0) << " ms\n";
        std::cout << "  Rate: " << (NUM_TESTS * 1000.0 / duration.count()) << " M ops/sec\n";
        std::cout << "  Time per op: " << (duration.count() / (double)NUM_TESTS) << " ns\n";
        std::cout << "  Matches: " << matches << "\n";
    }
    
    // Check runtime CPU features
    const auto& features = CPUFeatures::get();
    std::cout << "\nSIMD support: ";
    if (features.has_avx2) {
        std::cout << "AVX2 enabled (runtime)\n";
    } else if (features.has_sse2) {
        std::cout << "SSE2 enabled (runtime)\n";
    } else if (features.has_neon) {
        std::cout << "NEON enabled (runtime)\n";
    } else {
        std::cout << "Scalar fallback\n";
    }
}

TEST_F(ParallelSIMDBenchmark, OptimalConfiguration) {
    std::cout << "\n=== Optimal Configuration Test ===\n";
    
    // Detect hardware capabilities
    unsigned int numCores = std::thread::hardware_concurrency();
    std::cout << "Hardware threads available: " << numCores << "\n";
    
    const auto& features = CPUFeatures::get();
    std::cout << "SIMD support: ";
    if (features.has_avx2) {
        std::cout << "AVX2 (runtime)\n";
    } else if (features.has_sse2) {
        std::cout << "SSE2 (runtime)\n";
    } else if (features.has_neon) {
        std::cout << "NEON (runtime)\n";
    } else {
        std::cout << "None\n";
    }
    
#ifdef __APPLE__
    // Check if running under Rosetta
    int ret = 0;
    size_t size = sizeof(ret);
    if (sysctlbyname("sysctl.proc_translated", &ret, &size, nullptr, 0) == 0 && ret == 1) {
        std::cout << "Running under Rosetta 2 translation\n";
    }
#endif
    
    std::cout << "\nRecommended configuration:\n";
    std::cout << "- Query threads: " << (numCores > 4 ? numCores / 2 : numCores) << "\n";
    std::cout << "- Leave " << (numCores > 4 ? numCores / 2 : 1) << " threads for system/insert operations\n";
    std::cout << "- Enable SIMD optimizations in KeyMBR::intersects for 2D queries\n";
    std::cout << "- Use thread-local query objects to avoid allocation overhead\n";
    std::cout << "- Consider work-stealing queue for better load balancing\n";
}