# XTree Persistence Metrics Guide

## Overview

The durability stress test now reports comprehensive metrics to help understand storage efficiency and memory usage. This guide explains what each metric means and how to use them for optimization.

## Metrics Reported

### 1. Memory Footprint
- **What it measures**: Physical memory (RSS - Resident Set Size) used by the process
- **Typical values**: 
  - 10M records: ~500MB - 2GB
  - Depends on Object Table size and mmap'd regions
- **What affects it**:
  - Object Table entries (~64 bytes per node)
  - LRU cache size
  - Memory-mapped windows (demand-paged, not all resident)

### 2. Storage Metrics

#### Total Disk Usage
- **What**: All files created by the persistence layer
- **Breakdown**:
  - **Data files (.xd)**: Actual tree nodes (internal + leaf)
  - **WAL files**: Metadata-only write-ahead logs
  - **Checkpoints**: Object Table snapshots
  - **Other files**: Manifest, superblock, etc.

#### Efficiency Percentages
- **Data %**: Percentage of disk used for actual index data
  - Good: > 90%
  - Acceptable: 80-90%
  - Poor: < 80% (too much metadata overhead)
  
- **Metadata %**: WAL + checkpoints percentage
  - Good: < 10%
  - Acceptable: 10-20%
  - Poor: > 20% (excessive checkpointing)

### 3. Fragmentation Estimate
- **What it measures**: Unused space in data files
- **Calculation**: `1 - (actual_bytes / (num_files * file_size))`
- **Interpretation**:
  - < 10%: Excellent, files well utilized
  - 10-30%: Normal, some wasted space in last file
  - > 30%: High fragmentation, consider compaction

### 4. Per-Record Metrics
- **Data bytes per record**: Actual index bytes / num records
  - Typical: 3,500-4,000 bytes for spatial data
  - Includes internal nodes + leaves
  
- **Total bytes per record**: All disk bytes / num records
  - Includes metadata overhead
  - Should be < 110% of data bytes

## Example Output Interpretation

```
=== Storage Metrics ===
Total disk usage: 35.2 GB
  Data files (.xd): 34.8 GB (9 files)
  WAL files: 256 MB (1 files)
  Checkpoints: 1.2 GB (3 files)
  Other files: 2 KB

Efficiency:
  Data: 98.9%
  Metadata: 1.1%
  Fragmentation estimate: 2.5%

Per-record metrics:
  Data bytes per record: 3,480 bytes
  Total bytes per record: 3,520 bytes
```

**Analysis**:
- ✅ Excellent efficiency (98.9% data)
- ✅ Low metadata overhead (1.1%)
- ✅ Minimal fragmentation (2.5%)
- ✅ Reasonable per-record size

## Red Flags to Watch For

### 1. High Fragmentation (>30%)
**Symptoms**:
- Many partially filled .xd files
- High fragmentation estimate

**Causes**:
- File size too large for dataset
- Many deletions without compaction

**Solutions**:
- Reduce `kMaxFileSize` in config
- Implement compaction (future)

### 2. Excessive Metadata (>20%)
**Symptoms**:
- Many checkpoint files
- Large WAL files

**Causes**:
- Too frequent checkpointing
- Checkpoint cleanup not working

**Solutions**:
- Increase checkpoint thresholds
- Verify cleanup is working (keep only 3)

### 3. High Memory Usage
**Symptoms**:
- RSS > 10% of data size

**Causes**:
- Object Table too large
- Too many mmap'd windows

**Solutions**:
- Implement OT pruning
- Reduce window size

## Optimization Guidelines

### For 10M Records (~35GB)

**Good Configuration**:
```cpp
kMaxFileSize = 4GB         // 9 files total
checkpoint_keep_count = 2   // Save 400MB
```

**Expected Metrics**:
- Fragmentation: < 5%
- Data efficiency: > 95%
- Memory: 500MB - 1GB
- Per-record: ~3,500 bytes

### For 100M Records (~350GB)

**Good Configuration**:
```cpp
kMaxFileSize = 16GB        // 22 files total
checkpoint_keep_count = 2   // Save 4GB
```

**Expected Metrics**:
- Fragmentation: < 3%
- Data efficiency: > 97%
- Memory: 5GB - 10GB
- Per-record: ~3,500 bytes

## When to Implement Compaction

Consider implementing compaction when:
1. Fragmentation > 30% consistently
2. Data efficiency < 80%
3. Many small .xd files with low utilization
4. After many updates/deletes

## Monitoring Commands

```bash
# Watch file growth during test
watch -n 1 'ls -lh /tmp/xtree_durable_stress_* | tail -20'

# Monitor process memory
top -pid $(pgrep xtree_tests)

# Check file utilization
du -h /tmp/xtree_durable_stress_* | sort -h
```

## Future Improvements

1. **Real-time metrics**: Report during insertion
2. **Compaction triggers**: Auto-compact at thresholds
3. **Memory breakdown**: OT vs cache vs mmap
4. **I/O metrics**: Read/write bandwidth
5. **Compression ratio**: When implemented