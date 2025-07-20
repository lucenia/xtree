# Testing Guide for Lucenia XTree

This document describes how to run and write tests for the Lucenia XTree project.

## Running Tests

### Quick Start

Run all tests with Gradle:
```bash
./gradlew test
```

### Test Execution Options

#### Run Tests Directly

After building, you can run the test executable directly:
```bash
./build/native/bin/xtree_tests
```

#### Filter Tests

Run specific test suites or individual tests:
```bash
# Run all KeyMBR tests
./build/native/bin/xtree_tests --gtest_filter=KeyMBRTest*

# Run specific test
./build/native/bin/xtree_tests --gtest_filter=KeyMBRTest.FloatConstructor

# Run multiple test suites
./build/native/bin/xtree_tests --gtest_filter=KeyMBRTest*:XTreeTest*

# Exclude tests
./build/native/bin/xtree_tests --gtest_filter=-*Performance*
```

#### Verbose Output

Get detailed test output:
```bash
./build/native/bin/xtree_tests --gtest_print_time=1 --gtest_color=yes
```

#### List Available Tests

See all available tests without running them:
```bash
./build/native/bin/xtree_tests --gtest_list_tests
```

## Test Categories

### Unit Tests

Located in `core/src/main/cpp/test/`:

1. **test_keymbr.cpp** - Spatial MBR operations
   - Construction and initialization
   - Intersection and expansion
   - Float to sortable int conversions
   - Area calculations

2. **test_components.cpp** - Core component tests
   - Bucket operations
   - Node management
   - Cache behavior
   - Memory management

3. **test_xtree.cpp** - Main XTree functionality
   - Tree construction
   - Insertion operations
   - Tree balancing
   - Iterator behavior

4. **test_search.cpp** - Search operations
   - Point queries
   - Range searches
   - Nearest neighbor
   - Spatial predicates

5. **test_float_utils.cpp** - Float conversion utilities
   - Round-trip conversions
   - Boundary conditions
   - Performance characteristics

### Integration Tests

Located in `test_integration.cpp`:
- End-to-end scenarios
- Multi-threaded operations
- Large dataset handling
- Memory pressure tests

### Performance Tests

Located in `test_performance.cpp`:
- Benchmark insertions
- Search performance
- Memory usage
- Cache efficiency

Run performance tests separately:
```bash
./build/native/bin/xtree_tests --gtest_filter=*Performance*
```

## Writing Tests

### Test Structure

Use Google Test framework:

```cpp
#include <gtest/gtest.h>
#include "xtree.h"

TEST(XTreeTest, BasicInsertion) {
    // Arrange
    XTree tree(2);  // 2D tree
    
    // Act
    float coords[] = {1.0f, 2.0f};
    tree.insert(coords, 12345);
    
    // Assert
    EXPECT_EQ(tree.size(), 1);
}
```

### Test Fixtures

For tests requiring setup/teardown:

```cpp
class XTreeTestFixture : public ::testing::Test {
protected:
    void SetUp() override {
        tree = std::make_unique<XTree>(2);
        // Additional setup
    }
    
    void TearDown() override {
        // Cleanup if needed
    }
    
    std::unique_ptr<XTree> tree;
};

TEST_F(XTreeTestFixture, ComplexOperation) {
    // Use tree member variable
    ASSERT_NE(tree, nullptr);
    // Test implementation
}
```

### Best Practices

1. **Test Naming**: Use descriptive names that explain what is being tested
   ```cpp
   TEST(KeyMBRTest, IntersectsReturnsTrueForOverlappingRegions)
   ```

2. **Assertions**: Use appropriate assertion macros
   - `EXPECT_*` for non-fatal assertions
   - `ASSERT_*` for fatal assertions that should stop the test
   - `EXPECT_FLOAT_EQ` for floating-point comparisons

3. **Test Data**: Use meaningful test data
   ```cpp
   // Good: Clear what's being tested
   float point[] = {-180.0f, 90.0f};  // Max longitude, latitude
   
   // Bad: Magic numbers
   float point[] = {1.234f, 5.678f};
   ```

4. **Edge Cases**: Always test boundaries
   - Empty trees
   - Single element
   - Maximum capacity
   - Negative values
   - Zero values
   - Float limits

### Memory Testing

Run tests with valgrind to check for memory issues:
```bash
valgrind --leak-check=full --track-origins=yes ./build/native/bin/xtree_tests
```

### Thread Safety Testing

Test concurrent operations:
```cpp
TEST(XTreeTest, ConcurrentInsertions) {
    XTree tree(2);
    const int numThreads = 4;
    const int insertsPerThread = 1000;
    
    std::vector<std::thread> threads;
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&tree, i, insertsPerThread]() {
            for (int j = 0; j < insertsPerThread; ++j) {
                float coords[] = {float(i), float(j)};
                tree.insert(coords, i * insertsPerThread + j);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(tree.size(), numThreads * insertsPerThread);
}
```

## Debugging Tests

### GDB Debugging

Debug a specific test:
```bash
gdb ./build/native/bin/xtree_tests
(gdb) break test_keymbr.cpp:42
(gdb) run --gtest_filter=KeyMBRTest.Intersection
```

### Enable Debug Output

Add debug logging to tests:
```cpp
TEST(XTreeTest, DebugOutput) {
    ::testing::internal::CaptureStdout();
    
    // Your test code that produces output
    
    std::string output = ::testing::internal::GetCapturedStdout();
    std::cout << "Debug output:\n" << output << std::endl;
}
```

## Continuous Integration

Tests are automatically run on:
- Every pull request
- Commits to main branch
- Release tags

CI ensures tests pass on:
- Linux (Ubuntu latest)
- macOS (latest)
- Windows (latest)

## Coverage

Generate test coverage report:
```bash
# Build with coverage flags
cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERAGE=ON ..
make
make test

# Generate coverage report
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '/usr/*' --output-file coverage.info
lcov --list coverage.info
```

## Performance Benchmarking

Run performance benchmarks:
```bash
# Run all benchmarks
./build/native/bin/xtree_tests --gtest_filter=*Benchmark*

# Run with detailed timing
./build/native/bin/xtree_tests --gtest_filter=*Benchmark* --gtest_print_time=1
```

Compare performance across commits:
```bash
# Baseline
git checkout main
./gradlew clean build
./build/native/bin/xtree_tests --gtest_filter=*Benchmark* > baseline.txt

# Your changes
git checkout feature-branch
./gradlew clean build
./build/native/bin/xtree_tests --gtest_filter=*Benchmark* > feature.txt

# Compare
diff baseline.txt feature.txt
```