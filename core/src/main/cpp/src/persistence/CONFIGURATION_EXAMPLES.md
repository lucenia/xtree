# XTree Storage Configuration Examples

## How to Configure Storage

The XTree persistence layer now supports runtime configuration instead of compile-time constants. Here are the various ways to configure it:

## 1. Using Environment Variables (Simplest)

```bash
# Set environment variables before running
export XTREE_MAX_FILE_SIZE=4294967296  # 4GB
export XTREE_CHECKPOINT_KEEP_COUNT=2   # Keep only 2 checkpoints

# Run your application
./your_app
```

## 2. Using Predefined Configurations

```cpp
#include "persistence/storage_config.h"

// For 10M+ records
auto config = StorageConfig::large_dataset();
// Sets: 4GB files, 4GB windows, 2 checkpoints

// For 100M+ records  
auto config = StorageConfig::huge_dataset();
// Sets: 16GB files, 16GB windows, 2 checkpoints

// For memory-constrained systems
auto config = StorageConfig::low_memory();
// Sets: 256MB files, 256MB windows, 2 checkpoints

// Pass to SegmentAllocator
SegmentAllocator allocator(data_dir, config);
```

## 3. Custom Configuration

```cpp
StorageConfig config;
config.max_file_size = 8ULL << 30;        // 8GB files
config.mmap_window_size = 8ULL << 30;     // 8GB windows
config.checkpoint_keep_count = 4;         // Keep 4 checkpoints
config.max_open_files = 512;              // Use more FDs

// Validate before use
if (!config.validate()) {
    throw std::runtime_error("Invalid config");
}

SegmentAllocator allocator(data_dir, config);
```

## 4. Per-Index Configuration (Future)

Eventually, this will be integrated with IndexDetails:

```cpp
// Future API (not yet implemented)
StorageConfig storage_config = StorageConfig::large_dataset();
DurabilityPolicy durability_policy;
durability_policy.mode = DurabilityMode::BALANCED;

IndexDetails<DataRecord> index(
    dimensions,
    precision,
    labels,
    max_memory,
    storage_config,    // Per-index storage config
    durability_policy  // Per-index durability
);
```

## 5. Configuration File (Future)

A YAML/JSON config file could be added:

```yaml
# xtree.conf (future)
storage:
  max_file_size: 4294967296     # 4GB
  mmap_window_size: 4294967296  # 4GB
  checkpoint_keep_count: 3
  max_open_files: 256

durability:
  mode: BALANCED
  checkpoint_interval_ms: 1000
```

## Current Implementation Status

✅ **Available Now**:
- Environment variable configuration
- Predefined configs (large_dataset, huge_dataset, low_memory)
- Custom StorageConfig objects
- SegmentAllocator accepts StorageConfig

🔄 **In Progress**:
- Integration with IndexDetails
- Pass-through to DurableStore

⏳ **Future**:
- Configuration files (YAML/JSON)
- Dynamic reconfiguration
- Per-tree configuration in multi-tree systems

## Migration Guide

### From Hardcoded (old):
```cpp
// Old: Edit config.h and recompile
constexpr size_t kMaxFileSize = 1ULL << 30;  // 1GB
```

### To Runtime Config (new):
```cpp
// New: Configure at runtime
StorageConfig config;
config.max_file_size = 4ULL << 30;  // 4GB
SegmentAllocator allocator(data_dir, config);
```

## Testing Different Configurations

```bash
# Test with 4GB files
XTREE_MAX_FILE_SIZE=4294967296 ./run_tests

# Test with 16GB files
XTREE_MAX_FILE_SIZE=17179869184 ./run_tests

# Test with minimal checkpoints
XTREE_CHECKPOINT_KEEP_COUNT=1 ./run_tests
```

## Recommendations by Workload

| Records | File Size | Checkpoints | Config Method |
|---------|-----------|------------|---------------|
| < 1M | 1GB (default) | 3 | `StorageConfig::defaults()` |
| 1M-10M | 4GB | 2 | `StorageConfig::large_dataset()` |
| 10M-100M | 16GB | 2 | `StorageConfig::huge_dataset()` |
| > 100M | 16-64GB | 2 | Custom config |

## Performance Impact

```
1GB files (default):
- 10M records → ~35 files
- More file rotation overhead
- Better for mixed workloads

4GB files (large_dataset):
- 10M records → ~9 files
- Less rotation, ~5-10% faster writes
- Better for dedicated large trees

16GB files (huge_dataset):
- 10M records → ~3 files
- Minimal rotation overhead
- Best for very large datasets
```