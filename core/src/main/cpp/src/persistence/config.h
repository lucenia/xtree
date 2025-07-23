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
#include <cstdint>
#include <cstddef>
#include <cstdlib>

#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

namespace xtree {
namespace persist {

// System configuration - determined once at startup
namespace sys_config {
    // Get the system page size (determined at runtime, cached)
    inline size_t get_page_size() {
        static size_t page_size = 0;
        if (page_size == 0) {
            #ifdef _WIN32
                SYSTEM_INFO si;
                GetSystemInfo(&si);
                page_size = si.dwPageSize;
            #else
                long ps = sysconf(_SC_PAGE_SIZE);
                page_size = (ps > 0) ? static_cast<size_t>(ps) : 4096;
            #endif
        }
        return page_size;
    }
    
    // Get a page-aligned size (round up to next page boundary)
    inline size_t page_align(size_t size) {
        size_t page = get_page_size();
        return ((size + page - 1) / page) * page;
    }
}

// Size class configuration
namespace size_class {
    // Size classes optimized for both small DataRecords and large XTreeBuckets:
    // IMPORTANT: 256B minimum was found to cause data loss due to insufficient padding
    // Size class configuration optimized for XTree persistence
    //
    // CRITICAL: XTreeBucket can grow significantly after initial allocation
    // - Normal buckets: up to XTREE_M (50) children
    // - Supernodes: up to 3*XTREE_M (150) children
    // - 2D bucket with 150 children = 2420 bytes
    //
    // Fix implemented in xtree_allocator_traits.hpp:
    // - Pre-allocates space for expected growth (XTREE_M to 1.5*XTREE_M children)
    // - Prevents buffer overflow when buckets grow
    // - Trade-off: Uses more space but avoids reallocation complexity
    //
    // Size classes:
    // Small classes (for tiny nodes):
    //   64B, 128B - Reasonable minimum for tree nodes
    // Medium classes (for small buckets):
    //   256B, 512B, 1KB, 2KB, 4KB
    // Large classes (for XTreeBuckets with many children):
    //   8KB, 16KB, 32KB, 64KB, 128KB, 256KB
    constexpr size_t kSizes[] = {
        64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144
    };
    constexpr uint8_t kNumClasses = sizeof(kSizes) / sizeof(kSizes[0]);
    constexpr size_t kMinSize = kSizes[0];        // 64B - Minimum allocation size
    constexpr size_t kMaxSize = kSizes[kNumClasses - 1];    // 256KB
}

// Object Table configuration
namespace object_table {
    constexpr size_t kInitialCapacity = 1 << 20;  // 1M handles
    constexpr size_t kMaxHandles = 1ULL << 56;    // 56-bit handle space
    constexpr uint8_t kMaxTag = 255;              // 8-bit tag space
    
    // Slab configuration for stable OTEntry addresses
    // Target 256KB slabs for good L3 cache locality
    constexpr size_t kSlabTargetBytes = 256 * 1024;  // 256KB per slab
    constexpr size_t kOTEntrySize = 64;              // Approximate size with alignment
    constexpr size_t kEntriesPerSlab = kSlabTargetBytes / kOTEntrySize;  // ~4096 entries
    
    // For runtime configuration via environment or settings
    constexpr const char* kSlabSizeEnvVar = "XTREE_OT_SLAB_KB";
    constexpr size_t kMinSlabKB = 64;   // Minimum 64KB slabs
    constexpr size_t kMaxSlabKB = 1024; // Maximum 1MB slabs
}

// Segment allocator configuration
namespace segment {
    constexpr size_t kDefaultSegmentSize = 1 * 1024 * 1024;   // 1MB segments (was 16MB - too wasteful)
    constexpr size_t kMaxSegmentSize = 256 * 1024 * 1024;     // 256MB max
    constexpr double kFragmentationThreshold = 0.5;           // 50% dead = candidate for compaction
    constexpr size_t kMinFreeSpacePercent = 5;                // Keep 5% free in each class
    
    // Filesystem alignment for optimal performance
    // Many SSDs have 4MB erase blocks; NVMe typically uses 512KB-2MB stripes
    // Using 2MB alignment provides good balance for most storage types
    constexpr size_t kSegmentAlignment = 2 * 1024 * 1024;     // 2MB alignment
}

// MVCC configuration
namespace mvcc {
    constexpr uint64_t kInvalidEpoch = ~uint64_t{0};
    constexpr size_t kInitialPinSlots = 1024;                 // Initial TLS pin slots
    constexpr size_t kMaxPinSlots = 65536;                    // Maximum concurrent readers
}

// Superblock configuration
namespace superblock {
    constexpr uint64_t kMagic = 0x5854524545505331ULL;        // "XTREEPS1"
    constexpr uint32_t kVersion = 1;
    constexpr size_t kHeaderSize = 4096;                      // 4KB header
    constexpr size_t kPadSize = 256;                          // Future expansion
}

// Delta log configuration
namespace delta_log {
    constexpr size_t kMaxBatchSize = 1024;                    // Max records per batch
    constexpr size_t kRotateSize = 64 * 1024 * 1024;         // Rotate at 64MB
    constexpr uint64_t kRotateAge = 3600;                     // Rotate after 1 hour
    constexpr size_t kBufferSize = 4 * 1024 * 1024;          // 4MB write buffer
}

// Checkpoint configuration
namespace checkpoint {
    constexpr size_t kTriggerSize = 128 * 1024 * 1024;       // Checkpoint at 128MB delta
    constexpr uint64_t kTriggerTime = 300;                    // Checkpoint every 5 minutes
    constexpr size_t kCompressionLevel = 6;                   // zstd level 6
}

// Compaction configuration
namespace compaction {
    constexpr double kDeadRatioThreshold = 0.4;              // Compact at 40% dead
    constexpr double kTombstoneRatioThreshold = 0.3;         // Compact at 30% tombstones
    constexpr size_t kMinSegmentAge = 60;                     // Don't compact segments < 1 minute old
    constexpr size_t kMaxConcurrentCompactions = 2;          // Limit concurrent compactions
    constexpr double kTargetCpuPercent = 10.0;               // Target < 10% CPU for compaction
}

// Recovery configuration
namespace recovery {
    constexpr size_t kMaxRecoveryTime = 2000;                // Target < 2 seconds
    constexpr size_t kPrefetchSize = 4 * 1024 * 1024;       // Prefetch 4MB chunks
    constexpr bool kVerifyChecksums = true;                  // Verify CRCs during recovery
}

// Hotset configuration
namespace hotset {
    constexpr size_t kL0Size = 64 * 1024;                    // 64KB for root
    constexpr size_t kL1Size = 1024 * 1024;                  // 1MB for L1
    constexpr size_t kL2Size = 16 * 1024 * 1024;            // 16MB for L2
    constexpr bool kAsyncWarmup = true;                      // Warm up in background
}

// Platform-specific configuration
namespace platform {
    #ifdef _WIN32
        constexpr bool kUseWindowsLargePage = true;
        constexpr size_t kLargePageSize = 2 * 1024 * 1024;   // 2MB on Windows
    #else
        constexpr bool kUseMadvise = true;
        constexpr bool kUseHugePages = true;
        constexpr size_t kHugePageSize = 2 * 1024 * 1024;    // 2MB on Linux
    #endif
}

// Debug configuration
namespace debug_config {
    #ifdef NDEBUG
        constexpr bool kValidateTags = false;
        constexpr bool kTrackAllocations = false;
        constexpr bool kChecksumWrites = false;
    #else
        constexpr bool kValidateTags = true;
        constexpr bool kTrackAllocations = true;
        constexpr bool kChecksumWrites = true;
    #endif
}

// File naming configuration
namespace files {
    constexpr const char* kMetaFile = "xtree.meta";
    // Separate file types for tree structure vs data records
    constexpr const char* kIndexPrefix = "xtree";          // Base name for index files (buckets)
    constexpr const char* kIndexExtension = ".xi";         // Extension for index files
    constexpr const char* kDataPrefix = "xtree_data";      // Base name for data files (records)  
    constexpr const char* kDataExtension = ".xd";          // Extension for data files
    constexpr const char* kDeltaLogFile = "ot_delta.wal";  // WAL = Write-Ahead Log
    constexpr const char* kCheckpointPrefix = "ot_checkpoint";
    constexpr const char* kManifestFile = "manifest.json";
    
    // File organization strategy
    constexpr bool kFilePerSizeClass = true;              // Separate file per size class for locality
    
    // File size configuration - adjust for your workload:
    // - 1GB default: Good balance for most workloads
    // - 4GB (1ULL << 32): Better for large datasets (10M+ nodes)
    // - 16GB (1ULL << 34): Maximum for very large datasets
    // - 256MB (256 * 1024 * 1024): Better for memory-constrained systems
    constexpr size_t kMaxFileSize = 1ULL << 30;           // 1GB max per file before rotation
    constexpr size_t kTargetFileSize = 256 * 1024 * 1024; // 256MB target for good OS caching
    
    // MMap window size - should be >= kMaxFileSize
    constexpr size_t kMMapWindowSize = 1ULL << 30;        // 1GB mmap windows
}

} // namespace persist
} // namespace xtree