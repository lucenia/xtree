# Memory-Mapped XTree Test Suite

This directory contains comprehensive tests for the new memory-mapped XTree components that **do not interfere** with the original in-memory XTree implementation.

## Test Files Overview

### 1. `test_mmapfile.cpp` - Memory-Mapped File Tests
Tests the low-level memory-mapped file functionality:

- **File Creation & Management**: Creating new files, opening existing files, proper cleanup
- **Memory Mapping**: Mapping files into virtual address space, handling file expansion
- **Allocation**: Block allocation within mapped files, allocation tracking
- **Data Persistence**: Writing data, syncing to disk, verifying persistence across reopens
- **Memory Locking**: Testing `mlock`/`munlock` for pinning memory regions
- **Error Handling**: Invalid paths, permission issues, boundary conditions
- **Concurrency**: Multi-threaded allocation and access patterns
- **Performance**: Large file handling, random access patterns
- **Data Integrity**: Corruption detection, consistency across multiple sessions

**Key Test Categories**:
- Basic functionality (create, open, allocate)
- Data persistence and integrity
- Concurrent access safety
- Performance with large datasets
- Error and edge case handling

### 2. `test_lru_tracker.cpp` - LRU Access Tracking Tests
Tests the access pattern tracking system:

- **Access Recording**: Basic access counting, frequency calculation, temporal tracking
- **Memory Pinning**: Pin/unpin operations, pinning failure handling, pin count tracking
- **Hot Node Detection**: Identifying frequently accessed nodes, ranking by access patterns
- **LRU Eviction**: Proper eviction when max tracked nodes exceeded
- **Statistics Management**: Memory usage tracking, statistics clearing, stale entry cleanup
- **Concurrency**: Multi-threaded access recording, thread-safe statistics
- **Performance**: High-volume access recording, efficient statistics lookup

**Key Test Categories**:
- Access pattern recording and analysis
- Memory pinning functionality
- LRU eviction and memory management
- Concurrent access handling
- Performance under load

### 3. `test_hot_node_detector.cpp` - Hot Node Detection Tests
Tests the optimization suggestion system:

- **Hotness Detection**: Identifying hot vs cold nodes, confidence scoring
- **Optimization Suggestions**: PIN_NODE, UNPIN_NODE, THREAD_AFFINITY recommendations
- **Temporal Analysis**: Time-windowed analysis, recent vs historical patterns
- **Confidence Scoring**: Accurate confidence levels based on access patterns
- **Suggestion Reasoning**: Descriptive explanations for optimization decisions
- **Dynamic Adaptation**: Handling changing access patterns over time

**Key Test Categories**:
- Hot node identification algorithms
- Optimization suggestion generation
- Confidence scoring accuracy
- Temporal pattern analysis
- Dynamic pattern adaptation

### 4. `test_mmap_xtree_integration.cpp` - Full Integration Tests
Tests the complete memory-mapped XTree system:

- **Tree Operations**: Creation, insertion, searching with mmap storage
- **Access Tracking Integration**: Automatic tracking during tree operations
- **Persistence**: Multi-session data persistence, recovery after restart
- **Optimization Integration**: Memory pinning, hot node detection during real usage
- **Performance**: Large dataset handling, search performance
- **Error Handling**: Invalid operations, file corruption recovery

**Key Test Categories**:
- End-to-end tree functionality
- Integration between all components
- Real-world usage scenarios
- Performance benchmarking
- Error recovery

## Test Data Isolation

### Complete Separation from Original Tests
- **Different Record Types**: Uses `MMapTestRecord` instead of original `DataRecord`
- **Separate File Paths**: All tests use isolated temporary directories
- **Mock Objects**: Uses mock `MMapFile` for unit tests to avoid file I/O
- **Independent Test Data**: No shared state with original XTree tests
- **Isolated Namespaces**: Tests run in separate test suites

### No Impact on Existing Code
- **Original XTree Unchanged**: All original XTree logic remains untouched
- **Parallel Implementation**: New mmap code exists alongside original code
- **Backward Compatibility**: Original in-memory mode continues to work
- **Independent Build**: Mmap tests can be excluded without affecting original tests

## Test Coverage

### Functional Coverage
- ✅ **Basic Operations**: File creation, mapping, allocation, search, insert
- ✅ **Advanced Features**: Memory pinning, hot node detection, optimization
- ✅ **Error Handling**: Invalid inputs, system failures, edge cases
- ✅ **Concurrency**: Multi-threaded access, race condition detection
- ✅ **Persistence**: Data integrity across sessions, crash recovery

### Performance Coverage
- ✅ **Scalability**: Large datasets (1000+ records), big files (100MB+)
- ✅ **Latency**: Search performance, insertion speed
- ✅ **Memory Usage**: Tracking overhead, memory efficiency
- ✅ **System Integration**: OS-level memory management, file system performance

### Platform Coverage
- ✅ **Cross-Platform**: Works on Linux, macOS, Windows (with appropriate mmap)
- ✅ **Permission Handling**: Graceful failure when mlock not available
- ✅ **File System**: Various file systems, network storage compatibility

## Running the Tests

### Individual Test Suites
```bash
# Run MMapFile tests only
./build/native/bin/xtree_tests --gtest_filter="MMapFileTest.*"

# Run LRU tracker tests only  
./build/native/bin/xtree_tests --gtest_filter="LRUAccessTrackerTest.*"

# Run hot node detector tests only
./build/native/bin/xtree_tests --gtest_filter="HotNodeDetectorTest.*"

# Run integration tests only
./build/native/bin/xtree_tests --gtest_filter="MMapXTreeIntegrationTest.*"
```

### All MMap Tests
```bash
# Run all memory-mapped tests
./build/native/bin/xtree_tests --gtest_filter="*MMap*"
```

### Exclude MMap Tests (Original Tests Only)
```bash
# Run only original XTree tests
./build/native/bin/xtree_tests --gtest_filter="-*MMap*"
```

## Test Configuration

### Environment Variables
```bash
# Increase test timeout for large dataset tests
export GTEST_TIMEOUT=300

# Enable verbose output
export GTEST_VERBOSE=1

# Use specific temp directory
export TMPDIR=/fast/ssd/tmp
```

### Memory Requirements
- **Minimum**: 512MB RAM for basic tests
- **Recommended**: 2GB RAM for large dataset tests
- **Storage**: 1GB temp space for integration tests

## Performance Benchmarks

The tests include performance benchmarks that verify:

### Insertion Performance
- **Target**: >1000 records/second for typical spatial data
- **Large Dataset**: 1000 records in <10 seconds

### Search Performance  
- **Target**: <10ms average search latency
- **Concurrent**: 100 searches in <5 seconds

### Memory Efficiency
- **Tracking Overhead**: <1% of total memory usage
- **File Mapping**: Minimal virtual memory overhead

## Integration with CI/CD

### GitHub Actions
The tests are designed to run in CI environments:
- **Permissions**: Handle mlock failures gracefully in containers
- **Timeouts**: Reasonable time limits for automated testing
- **Resource Usage**: Efficient temp file cleanup
- **Parallel Execution**: Safe for parallel test execution

### Test Stability
- **No Flaky Tests**: Deterministic behavior, proper timing
- **Clean Teardown**: All temp files removed after tests
- **Resource Management**: No memory leaks, file handle cleanup

This test suite provides comprehensive coverage of the new memory-mapped XTree functionality while maintaining complete isolation from the original codebase.