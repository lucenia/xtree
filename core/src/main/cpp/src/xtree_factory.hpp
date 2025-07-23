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

#include "xtree.h"
#include "indexdetails.hpp"
#include "compact_xtree_allocator.hpp"

namespace xtree {

/**
 * Factory for creating XTree instances with different persistence modes
 * This provides a clean interface for creating XTree with memory management
 */
template<typename Record>
class XTreeFactory {
public:
    struct XTreeConfig {
        // XTree configuration
        unsigned short dimension = 2;
        unsigned short precision = 32;
        std::vector<const char*>* dimensionLabels = nullptr;
        
        // Persistence configuration
        IndexDetails<Record>::PersistenceMode persistenceMode = IndexDetails<Record>::PersistenceMode::IN_MEMORY;
        std::string snapshotFile = "xtree.snapshot";
        
        // JNI configuration (can be null for non-Java usage)
        JNIEnv* env = nullptr;
        jobject* xtPOJO = nullptr;
    };
    
    /**
     * Create a new XTree with specified persistence mode
     */
    static std::unique_ptr<IndexDetails<Record>> create(const XTreeConfig& config) {
        // Create IndexDetails with specified persistence mode
        auto index = std::make_unique<IndexDetails<Record>>(
            config.dimension,
            config.precision,
            config.dimensionLabels,
            config.env,
            config.xtPOJO,
            config.persistenceMode,
            config.snapshotFile
        );
        
        return index;
    }
    
    /**
     * Create root bucket for the XTree using appropriate allocator
     */
    static XTreeBucket<Record>* create_root(IndexDetails<Record>* index) {
        XTreeBucket<Record>* root = nullptr;
        
        if (auto* compactAlloc = index->getCompactAllocator()) {
            // Use compact allocator for MMAP mode
            root = compactAlloc->allocate_bucket(index, true);
            index->recordWrite(root);
        } else {
            // Fallback to standard allocation for IN_MEMORY mode
            root = new XTreeBucket<Record>(index, true);
        }
        
        // Cache the root bucket
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress(reinterpret_cast<long>(cachedRoot));
        
        return root;
    }
    
    /**
     * Load XTree from snapshot
     */
    static std::unique_ptr<IndexDetails<Record>> load_from_snapshot(
        const std::string& snapshotFile,
        std::vector<const char*>* dimensionLabels = nullptr,
        JNIEnv* env = nullptr,
        jobject* xtPOJO = nullptr) {
        
        // First validate the snapshot
        DirectMemoryCOWManager<Record> temp_manager(nullptr, snapshotFile);
        if (!temp_manager.validate_snapshot(snapshotFile)) {
            throw std::runtime_error("Invalid snapshot file: " + snapshotFile);
        }
        
        // Get header to extract dimensions
        auto header = temp_manager.get_snapshot_header(snapshotFile);
        
        // Create IndexDetails with COW enabled
        auto index = std::make_unique<IndexDetails<Record>>(
            header.dimension,
            header.precision,
            dimensionLabels,
            0,  // maxMemory will be determined from snapshot
            env,
            xtPOJO,
            true,  // use_cow = true
            snapshotFile
        );
        
        // TODO: Implement actual snapshot loading with pointer fixup
        // This is complex and requires:
        // 1. Memory mapping the snapshot file at the same addresses
        // 2. Or implementing address translation for all pointers
        // 3. Or converting XTree to use relative offsets instead of pointers
        
        throw std::runtime_error("Snapshot loading not yet implemented - pointer fixup required");
    }
    
    /**
     * Create a simple 2D geospatial index with default settings
     */
    static std::unique_ptr<IndexDetails<Record>> create_2d_spatial(
        const std::string& snapshotFile = "spatial_2d.snapshot") {
        
        static std::vector<const char*> labels = {"longitude", "latitude"};
        
        COWXTreeConfig config;
        config.dimension = 2;
        config.precision = 32;
        config.dimensionLabels = &labels;
        config.snapshotFile = snapshotFile;
        
        return create(config);
    }
    
    /**
     * Create a 3D spatial index (e.g., for 3D games, CAD)
     */
    static std::unique_ptr<IndexDetails<Record>> create_3d_spatial(
        const std::string& snapshotFile = "spatial_3d.snapshot") {
        
        static std::vector<const char*> labels = {"x", "y", "z"};
        
        COWXTreeConfig config;
        config.dimension = 3;
        config.precision = 32;
        config.dimensionLabels = &labels;
        config.snapshotFile = snapshotFile;
        
        return create(config);
    }
    
    /**
     * Create a time series index (1D temporal)
     */
    static std::unique_ptr<IndexDetails<Record>> create_time_series(
        const std::string& snapshotFile = "timeseries.snapshot") {
        
        static std::vector<const char*> labels = {"timestamp"};
        
        COWXTreeConfig config;
        config.dimension = 1;
        config.precision = 64;  // Higher precision for timestamps
        config.dimensionLabels = &labels;
        config.snapshotFile = snapshotFile;
        
        return create(config);
    }
    
    /**
     * Create a multi-dimensional feature index (e.g., for ML embeddings)
     */
    static std::unique_ptr<IndexDetails<Record>> create_feature_index(
        unsigned short dimensions,
        const std::string& snapshotFile = "features.snapshot") {
        
        COWXTreeConfig config;
        config.dimension = dimensions;
        config.precision = 32;
        config.snapshotFile = snapshotFile;
        // No dimension labels for high-dimensional data
        
        return create(config);
    }
};

} // namespace xtree