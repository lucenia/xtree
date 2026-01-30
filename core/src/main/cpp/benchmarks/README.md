# XTree Performance Benchmarks

This directory contains performance benchmarks and regression tests for the XTree spatial index.

## Structure

- `performance_regression.cpp` - Performance regression test suite
- `performance_baseline.json` - Baseline performance metrics
- `run_benchmarks.sh` - Script to run benchmarks

## Running Benchmarks

### Run all performance tests
```bash
cd core/src/main/cpp
./benchmarks/run_benchmarks.sh
```

### Check for performance regressions
```bash
./benchmarks/run_benchmarks.sh regression
```

### Update baseline after intentional changes
```bash
./benchmarks/run_benchmarks.sh update-baseline
```

## Performance Metrics Tracked

1. **Spatial Query Throughput** - Queries per second for range queries
2. **Bulk Insert Throughput** - Inserts per second for bulk loading
3. **MBR Operations** - Expand and intersect operations per millisecond
4. **COW Snapshot Time** - Time to create memory snapshots in microseconds
5. **Page Write Tracking** - Operations per millisecond for page tracking

## Regression Detection

The regression test compares current performance against the baseline stored in `performance_baseline.json`:

- **Regression**: Performance drop of more than 10% triggers a test failure
- **Improvement**: Performance increase of more than 20% is highlighted
- **Stable**: Changes within ±10% are considered normal variance

## Integration with CI/CD

The benchmark tests are disabled by default (using `DISABLED_` prefix) to avoid running them in regular CI builds. They can be explicitly enabled when needed.

To run in CI:
```bash
./build/native/bin/xtree_tests --gtest_filter="*Performance*" --gtest_also_run_disabled_tests
```