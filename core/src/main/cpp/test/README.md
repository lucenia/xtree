# XTree Test Organization

Tests are organized into logical groups for better maintainability:

## Directory Structure

### `/test/` (Core XTree Tests)
Core functionality tests that directly test XTree operations:
- `test_xtree.cpp` - Basic XTree operations
- `test_keymbr.cpp` - KeyMBR (Minimum Bounding Rectangle) tests
- `test_components.cpp` - XTree component tests
- `test_search.cpp` - Search functionality tests
- `test_performance.cpp` - Performance benchmarks
- `test_xtree_concurrent_search.cpp` - Concurrent search tests
- `test_xtree_simple_concurrent.cpp` - Simple concurrent operations
- `test_xtree_point_search.cpp` - Point search tests
- `test_concurrent_point_search.cpp` - Concurrent point search

### `/test/memmgr/` (Memory Management Tests)
Tests for memory management components:
- `test_page_write_tracker.cpp` - Page write tracking
- `test_cow_memory.cpp` - Copy-on-Write memory management
- `test_cow_backend_comparison.cpp` - COW backend comparisons
- `test_compact_allocator.cpp` - Compact allocator tests
- `test_segmented_allocator.cpp` - Segmented allocator tests
- `test_concurrent_operations.cpp` - Concurrent memory operations

### `/test/util/` (Utility Tests)
Tests for utility functions and helpers:
- `test_globals.cpp` - Global state tests
- `test_float_utils.cpp` - Float utility functions
- `test_symbols.cpp` - Symbol visibility tests

### `/test/integration/` (Integration Tests)
Full system integration tests:
- `test_integration.cpp` - Basic integration tests
- `test_cow_integration.cpp` - COW integration
- `test_compact_cow_integration.cpp` - Compact allocator + COW
- `test_mmap_comprehensive.cpp` - Comprehensive MMAP tests
- `test_mmap_full_reload.cpp` - MMAP persistence and reload
- `test_compact_root_persistence.cpp` - Root persistence tests
- `test_xtree_allocator_integration.cpp` - XTree + allocator integration
- `test_snapshot_size_comparison.cpp` - Snapshot size analysis
- `test_10k_issue.cpp` - Regression test for 10K insert issue
- `test_root_tracking.cpp` - Root node tracking tests
- `test_large_scale_segmented.cpp` - Large scale segmented allocation

## Running Tests

### Run all tests:
```bash
./xtree_tests
```

### Run specific test group:
```bash
# Core tests only
./xtree_tests --gtest_filter="XTree*:KeyMBR*:Components*"

# Memory management tests
./xtree_tests --gtest_filter="*Allocator*:*COW*:PageWriteTracker*"

# Integration tests
./xtree_tests --gtest_filter="*Integration*:*MMAP*:*Persistence*"
```

### Run specific test:
```bash
./xtree_tests --gtest_filter="CompactAllocatorTest.SnapshotSaveLoad"
```

## Test Categories

### Unit Tests
- Focus on individual components in isolation
- Located in core test directory and subdirectories
- Fast execution, minimal dependencies

### Integration Tests
- Test multiple components working together
- Located in `/test/integration/`
- May involve file I/O, larger datasets

### Performance Tests
- Benchmarks and performance measurements
- Located in `/benchmarks/` directory
- Not part of regular test runs

## Adding New Tests

1. Determine the appropriate category:
   - Core XTree functionality → `/test/`
   - Memory management → `/test/memmgr/`
   - Utilities → `/test/util/`
   - Multi-component → `/test/integration/`

2. Follow naming convention: `test_<component>.cpp`

3. Update `CMakeLists.txt` in the appropriate section

4. Include appropriate headers based on test type