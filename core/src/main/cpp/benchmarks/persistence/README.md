# Persistence Layer Performance Benchmarks

This directory contains comprehensive benchmarks for the XTree persistence layer, focusing on the new COW MVCC design with NodeID-based persistence.

## Benchmark Suite

### 1. WAL Performance (`bench_wal_comprehensive.cpp`)
Comprehensive Write-Ahead Log benchmarks including:
- Basic throughput and latency tests
- Sync overhead measurements  
- Concurrent scalability analysis
- Batch size optimization
- Payload-in-WAL performance (EVENTUAL mode)
- Performance summary with recommendations

### 2. Checkpoint Performance (`bench_checkpoint_performance.cpp`)
Measures checkpoint operations:
- Write speed for various entry counts
- Load/recovery speed
- Compression ratios for different data patterns
- Incremental checkpoint overhead
- Checkpoint coordinator trigger performance

### 3. Recovery Performance (`bench_recovery_performance.cpp`)
Tests system recovery scenarios:
- Cold start recovery with various checkpoint/delta combinations
- Delta log replay speed
- Recovery with corruption handling
- Parallel recovery component performance
- Recovery memory usage

### 4. Segment Allocator Performance (`bench_segment_allocator.cpp`)
Benchmarks the size-class segment allocator:
- Allocation throughput for different size classes
- Fragmentation under various workloads
- Concurrent allocation performance
- Free/reclaim overhead
- Compaction effectiveness

### 5. Group Commit Performance (`bench_group_commit.cpp`)
Tests group commit optimization:
- Commit batching effectiveness
- Latency vs throughput trade-offs
- Multi-writer scalability
- Sync reduction metrics

### 6. Durability Mode Comparison (`bench_durability_modes.cpp`)
Compares the three durability modes:
- STRICT mode performance (sync everything)
- EVENTUAL mode with payload-in-WAL
- BALANCED mode with coalesced flushing
- Recovery behavior for each mode
- Trade-off analysis

## Running the Benchmarks

### Run all benchmarks (from project root)
```bash
./gradlew benchmarks
```

### Run specific benchmark suites
```bash
# Run all WAL benchmarks
./build/native/bin/xtree_benchmarks --gtest_filter="WALBenchmark.*"

# Run all checkpoint benchmarks  
./build/native/bin/xtree_benchmarks --gtest_filter="CheckpointPerformanceBench.*"

# Run all recovery benchmarks
./build/native/bin/xtree_benchmarks --gtest_filter="RecoveryPerformanceBench.*"
```

### Run individual tests
```bash
# WAL basic throughput
./build/native/bin/xtree_benchmarks --gtest_filter="WALBenchmark.BasicThroughput"

# Find optimal batch size
./build/native/bin/xtree_benchmarks --gtest_filter="WALBenchmark.OptimalBatchSize"

# Concurrent scalability
./build/native/bin/xtree_benchmarks --gtest_filter="WALBenchmark.ConcurrentScalability"

# Quick summary
./build/native/bin/xtree_benchmarks --gtest_filter="WALBenchmark.PerformanceSummary"
```

## Key Metrics

### Performance Targets
- **WAL Append**: > 1M records/sec (batched)
- **Checkpoint Write**: > 500K entries/sec
- **Recovery**: < 2 seconds for 1M entries
- **Group Commit**: 10-100x sync reduction
- **Segment Allocation**: < 100ns per allocation

### Memory Targets
- **OT Memory**: ~100 bytes per entry
- **Recovery Memory**: < 2x checkpoint size
- **WAL Buffer**: < 8MB per thread

## Comparison with Old MMAP Benchmarks

The old COW MMAP benchmarks have been removed:
- `mmap_memory_pressure_test.cpp` - Replaced by recovery benchmarks
- `mmap_vs_memory_benchmark.cpp` - Replaced by durability mode comparison

The new benchmarks focus on:
1. **NodeID-based persistence** instead of direct memory mapping
2. **MVCC epoch management** for concurrent readers
3. **Size-class segment allocation** for fragmentation control
4. **Group commit and batching** for throughput
5. **Configurable durability policies** for flexibility

## Integration with CI/CD

These benchmarks should be run:
1. On every PR to detect performance regressions
2. Nightly with full datasets
3. Before releases with extended stress tests

### Performance Regression Detection
```bash
# Compare with baseline
./persistence_bench_suite --baseline=performance_baseline.json
```

## Future Enhancements

1. **Compaction Benchmarks**: Test online compaction under load
2. **MVCC Benchmarks**: Reader scalability with many epochs
3. **Fault Injection**: Performance under I/O errors
4. **Cross-Platform**: Windows vs Linux vs macOS comparison
5. **Hardware Specific**: NVMe vs SSD vs HDD performance

## Notes

- All benchmarks use `/tmp` for test files by default
- Clean up is automatic after each test
- Results are printed to stdout in a parseable format
- Use `--benchmark_format=json` for machine-readable output