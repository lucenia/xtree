/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test concurrent search operations with realistic point data
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/indexdetails.hpp"
#include "../src/compact_xtree_allocator.hpp"
#include "../src/memmgr/compact_snapshot_manager.hpp"
#include "../src/memmgr/concurrent_compact_allocator.hpp"

using namespace xtree;
using namespace std::chrono;
using CacheNode = LRUCacheNode<IRecord, UniqueId, LRUDeleteNone>;

class ConcurrentPointSearchTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::remove("/tmp/concurrent_point_test.dat");
    }
    
    void TearDown() override {
        std::remove("/tmp/concurrent_point_test.dat");
    }
    
    DataRecord* createPoint(const std::string& id, double lon, double lat) {
        DataRecord* dr = new DataRecord(2, 32, id);
        std::vector<double> point = {lon, lat};
        dr->putPoint(&point);
        return dr;
    }
    
    DataRecord* createSearchBox(double minLon, double minLat, double maxLon, double maxLat) {
        DataRecord* dr = new DataRecord(2, 32, "search");
        std::vector<double> min_pt = {minLon, minLat};
        std::vector<double> max_pt = {maxLon, maxLat};
        dr->putPoint(&min_pt);
        dr->putPoint(&max_pt);
        return dr;
    }
};

TEST_F(ConcurrentPointSearchTest, DISABLED_RealisticConcurrentSearches) {
    std::cout << "\n=== Concurrent Point Search Test ===\n";
    
    // Create index
    std::vector<const char*> dimLabels = {"longitude", "latitude"};
    auto* index = new IndexDetails<DataRecord>(
        2, 32, &dimLabels, nullptr, nullptr,
        IndexDetails<DataRecord>::PersistenceMode::MMAP, 
        "/tmp/concurrent_point_test.dat"
    );
    
    auto* root = XAlloc<DataRecord>::allocate_bucket(index, true);
    auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
    index->setRootAddress((long)cachedRoot);
    
    // Insert realistic point data - US cities
    std::cout << "Inserting US city data...\n";
    
    struct City {
        std::string name;
        double lon;
        double lat;
        int population;
    };
    
    std::vector<City> cities = {
        // Major cities
        {"New_York", -74.006, 40.7128, 8336817},
        {"Los_Angeles", -118.2437, 34.0522, 3979576},
        {"Chicago", -87.6298, 41.8781, 2693976},
        {"Houston", -95.3698, 29.7604, 2320268},
        {"Phoenix", -112.074, 33.4484, 1680992},
        {"Philadelphia", -75.1652, 39.9526, 1584064},
        {"San_Antonio", -98.4936, 29.4241, 1547253},
        {"San_Diego", -117.1611, 32.7157, 1423851},
        {"Dallas", -96.7970, 32.7767, 1343573},
        {"San_Jose", -121.8863, 37.3382, 1021795},
        
        // Medium cities
        {"Austin", -97.7431, 30.2672, 978908},
        {"Jacksonville", -81.6557, 30.3322, 911507},
        {"Fort_Worth", -97.3308, 32.7555, 909585},
        {"Columbus", -82.9988, 39.9612, 898553},
        {"Charlotte", -80.8431, 35.2271, 885708},
        {"San_Francisco", -122.4194, 37.7749, 881549},
        {"Indianapolis", -86.1581, 39.7684, 876384},
        {"Seattle", -122.3321, 47.6062, 753675},
        {"Denver", -104.9903, 39.7392, 727211},
        {"Boston", -71.0589, 42.3601, 692600},
        
        // Smaller cities for variety
        {"Portland", -122.6765, 45.5152, 654741},
        {"Las_Vegas", -115.1398, 36.1699, 651319},
        {"Memphis", -90.0490, 35.1495, 651073},
        {"Louisville", -85.7585, 38.2527, 617638},
        {"Baltimore", -76.6122, 39.2904, 593490},
        {"Milwaukee", -87.9065, 43.0389, 590157},
        {"Albuquerque", -106.6504, 35.0844, 560513},
        {"Tucson", -110.9747, 32.2226, 548073},
        {"Fresno", -119.7871, 36.7378, 542012},
        {"Sacramento", -121.4944, 38.5816, 513624}
    };
    
    for (const auto& city : cities) {
        DataRecord* dr = createPoint(city.name, city.lon, city.lat);
        root->xt_insert(cachedRoot, dr);
    }
    
    std::cout << "Inserted " << cities.size() << " cities\n";
    std::cout << "Root has " << root->n() << " entries\n";
    
    // Define search regions
    struct SearchRegion {
        std::string name;
        double minLon, minLat, maxLon, maxLat;
        int expectedMinCount;
    };
    
    std::vector<SearchRegion> regions = {
        {"Northeast", -80.0, 38.0, -70.0, 45.0, 3},      // NYC, Boston, Philly, etc.
        {"California", -125.0, 32.0, -114.0, 42.0, 4},   // LA, SF, SD, SJ
        {"Texas", -107.0, 25.0, -93.0, 37.0, 4},         // Houston, Dallas, SA, Austin
        {"Midwest", -95.0, 38.0, -80.0, 48.0, 3},        // Chicago, Milwaukee, etc.
        {"Southeast", -90.0, 25.0, -75.0, 38.0, 2},      // Jacksonville, Charlotte, Memphis
        {"Southwest", -120.0, 30.0, -105.0, 40.0, 4},    // Phoenix, Las Vegas, Albuquerque
        {"Northwest", -125.0, 42.0, -115.0, 50.0, 2}     // Seattle, Portland
    };
    
    // Test concurrent searches
    std::cout << "\nPerforming concurrent searches across regions...\n";
    
    std::atomic<int> totalSearches{0};
    std::atomic<int> totalResults{0};
    std::vector<std::thread> searchThreads;
    
    auto startTime = high_resolution_clock::now();
    
    // Launch concurrent search threads
    for (const auto& region : regions) {
        searchThreads.emplace_back([&]() {
            // Each thread performs multiple searches in its region
            for (int i = 0; i < 100; i++) {
                DataRecord* searchBox = createSearchBox(
                    region.minLon, region.minLat, 
                    region.maxLon, region.maxLat
                );
                
                auto iter = root->getIterator(cachedRoot, searchBox, INTERSECTS);
                
                int count = 0;
                while (iter->hasNext()) {
                    iter->next();
                    count++;
                }
                
                delete iter;
                delete searchBox;
                
                totalSearches.fetch_add(1);
                totalResults.fetch_add(count);
                
                if (i == 0) {
                    std::cout << "  " << region.name << " found " << count << " cities\n";
                    EXPECT_GE(count, region.expectedMinCount);
                }
            }
        });
    }
    
    // Wait for all searches to complete
    for (auto& thread : searchThreads) {
        thread.join();
    }
    
    auto endTime = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(endTime - startTime);
    
    std::cout << "\nConcurrent search results:\n";
    std::cout << "  Total searches: " << totalSearches.load() << "\n";
    std::cout << "  Total results found: " << totalResults.load() << "\n";
    std::cout << "  Time: " << duration.count() << " ms\n";
    std::cout << "  Searches per second: " << (totalSearches.load() * 1000.0 / duration.count()) << "\n";
    
    // Test concurrent insert and search
    std::cout << "\nTesting concurrent insert and search...\n";
    
    std::atomic<bool> stopInserting{false};
    std::atomic<int> insertCount{0};
    std::atomic<int> searchCount{0};
    
    // Insert thread - adds random points
    std::thread insertThread([&]() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> lon_dist(-125.0, -65.0);
        std::uniform_real_distribution<> lat_dist(25.0, 50.0);
        
        while (!stopInserting.load()) {
            double lon = lon_dist(gen);
            double lat = lat_dist(gen);
            DataRecord* dr = createPoint("dynamic_" + std::to_string(insertCount.load()), lon, lat);
            root->xt_insert(cachedRoot, dr);
            insertCount.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
    
    // Search threads - continuously search while inserts happen
    std::vector<std::thread> concurrentSearchThreads;
    for (int t = 0; t < 4; t++) {
        concurrentSearchThreads.emplace_back([&]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> region_dist(0, regions.size() - 1);
            
            while (!stopInserting.load()) {
                int regionIdx = region_dist(gen);
                const auto& region = regions[regionIdx];
                
                DataRecord* searchBox = createSearchBox(
                    region.minLon, region.minLat, 
                    region.maxLon, region.maxLat
                );
                
                auto iter = root->getIterator(cachedRoot, searchBox, INTERSECTS);
                
                int count = 0;
                while (iter->hasNext()) {
                    iter->next();
                    count++;
                }
                
                delete iter;
                delete searchBox;
                
                searchCount.fetch_add(1);
            }
        });
    }
    
    // Let it run for 2 seconds
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stopInserting.store(true);
    
    insertThread.join();
    for (auto& thread : concurrentSearchThreads) {
        thread.join();
    }
    
    std::cout << "Concurrent operations complete:\n";
    std::cout << "  Points inserted: " << insertCount.load() << "\n";
    std::cout << "  Searches performed: " << searchCount.load() << "\n";
    
    auto* compact_alloc = index->getCompactAllocator();
    std::cout << "  Final memory used: " << (compact_alloc->get_snapshot_manager()->get_snapshot_size() / (1024.0 * 1024.0)) << " MB\n";
    
    delete index;
}