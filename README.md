# Lucenia XTree

A high-performance native C++ spatial indexing library optimized for billions of points, providing logarithmic query performance with memory safety guarantees.

## Overview

Lucenia XTree is an advanced dimensional index that significantly outperforms traditional spatial indexing approaches. Built as part of the Lucenia stack, it provides:

- **Predictable Performance**: Consistent O(log n) query times even at billion-point scale
- **Memory Safety**: RAII-based design with bounds checking and null pointer protection
- **Cache Optimized**: LRU cache with intelligent prefetching for hot data paths
- **Float Precision**: Sortable integer representation eliminates floating-point errors
- **Cross-Platform**: Native builds for Linux, macOS, and Windows

## Key Features

### Spatial Indexing
- Multi-dimensional point and range indexing (2D, 3D, nD)
- Efficient MBR (Minimum Bounding Rectangle) operations
- Optimized intersection and expansion calculations
- Hardware-friendly branch prediction hints

### Performance Optimizations
- Zero-copy sortable integer coordinates
- Branchless min/max operations for hot paths
- SIMD-friendly data layouts
- Static constants for boundary values
- Prefetch hints for sequential access

### Memory Management
- Configurable LRU cache with automatic eviction
- Reference-counted cache nodes
- Stack allocation preferred over heap
- Automatic resource cleanup (RAII)

## Performance Characteristics

XTree consistently outperforms traditional approaches:

| Operation | XTree | BKD Tree | R-Tree |
|-----------|-------|----------|---------|
| Point Insert | O(log n) | O(log n) | O(log n) |
| Range Query | O(log n + k) | O(log n + k) | O(log n + k) |
| Memory Usage | Optimal | High | Moderate |
| Cache Efficiency | Excellent | Poor | Good |
| Float Precision | Exact | Approximate | Approximate |

Where `n` is the number of points and `k` is the number of results.

## Quick Start

```bash
# Clone and build
git clone https://github.com/lucenia/xtree.git
cd xtree
./gradlew build

# Run tests
./gradlew test
```

The build produces a versioned shared library:
- **Linux**: `libXTree-Linux-x86_64-64-0.5.0.so`
- **macOS**: `libXTree-Mac_OS_X-x86_64-64-0.5.0.dylib`
- **Windows**: `XTree-Windows-x86_64-64-0.5.0.dll`

## Usage Example

```cpp
#include "xtree.h"

// Create a 2D spatial index
XTree tree(2);

// Insert points
float point1[] = {37.7749f, -122.4194f};  // San Francisco
tree.insert(point1, 1001);

float point2[] = {40.7128f, -74.0060f};   // New York
tree.insert(point2, 1002);

// Range query
float min[] = {35.0f, -125.0f};
float max[] = {42.0f, -70.0f};
auto results = tree.search(min, max);

// Process results
for (uint64_t id : results) {
    std::cout << "Found point with ID: " << id << std::endl;
}
```

## Architecture

XTree uses a hierarchical structure optimized for spatial data:

```
XTree
├── Root Bucket
│   ├── Internal Nodes (spatial partitions)
│   └── Leaf Nodes (data points)
├── LRU Cache (hot data)
└── Memory Manager (allocation tracking)
```

### Key Components

- **KeyMBR**: Minimum Bounding Rectangle with sortable integer coordinates
- **XTreeBucket**: Node container with configurable fanout
- **LRUCache**: Thread-safe cache with automatic eviction
- **Iterator**: Efficient traversal with minimal memory overhead

## Build Options

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed build instructions.

Quick build commands:
- **Development**: `./gradlew build`
- **Snapshot**: `./gradlew build -Psnapshot`
- **Release**: Tag and build for version-stamped artifacts

## Testing

Comprehensive test suite covering unit, integration, and performance tests. See [TESTING.md](TESTING.md) for details.

```bash
# Run all tests
./gradlew test

# Run specific tests
./build/native/bin/xtree_tests --gtest_filter=KeyMBRTest*
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Development setup
- Code style guidelines
- Testing requirements
- Pull request process

## License

Lucenia XTree is licensed under the Server Side Public License, version 1 (SSPL-1.0). See LICENSE file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/lucenia/xtree/issues)
- **Documentation**: [https://lucenia.io](https://lucenia.io)
- **Community**: Join the Lucenia discussions

---

Part of the [Lucenia](https://lucenia.io) project - Open source search and analytics.