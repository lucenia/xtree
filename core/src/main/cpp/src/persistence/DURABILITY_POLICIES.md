# Durability Policies for XTree Persistence Layer

**Status**: Design Document  
**Author**: System Design  
**Last Updated**: 2025-08-14  
**Default Policy**: BALANCED

## Overview

The XTree persistence layer supports configurable durability policies that allow users to choose their preferred trade-off between performance and safety. Rather than forcing a choice between "slow but safe" and "fast but dangerous," the system provides three distinct durability modes that can be selected based on workload characteristics and business requirements.

## Durability Modes

### 1. STRICT - Per-Commit Durable Data

**Contract**: After `commit(epoch)` returns, both the WAL and the node bytes are guaranteed to be on disk.

**Implementation**:
1. Flush node data pages (`msync`/`FlushViewOfFile`)
2. Append deltas to WAL
3. Fsync WAL (`fsync`/`FlushFileBuffers`)
4. Publish to superblock
5. Fsync superblock

**Cost**: Highest latency under write bursts due to synchronous data flushing.

**Use Cases**:
- Testing and verification
- Financial or regulatory compliance scenarios
- Systems requiring absolute durability guarantees
- Small datasets where performance is not critical

**Recovery**: Simple - replay WAL deltas, all referenced data is guaranteed on disk.

### 2. EVENTUAL - WAL-First with Payload-in-WAL

**Contract**: Small/medium nodes (≤8-32KB) have their payload stored directly in the WAL. Commit only fsyncs the WAL, not data files.

**Implementation**:
1. For small nodes: Embed payload in WAL frame `[LEN | DELTA | PAYLOAD | CRC]`
2. Append to WAL with embedded payloads
3. Fsync WAL only
4. Publish to superblock
5. Background spiller later materializes payloads to segments

**Cost**: 
- Much faster commits (no data file sync)
- Larger WAL size
- Slower recovery (more bytes to replay)

**Use Cases**:
- High QPS bursts with small nodes
- Internal tree nodes and metadata leaves
- Systems with ample WAL storage
- Write-heavy workloads with small records

**Recovery**: Rehydrate nodes from WAL payloads; no dependency on segment msync timing.

### 3. BALANCED - WAL-Only with Coalesced Data Flush (DEFAULT)

**Contract**: Commits fsync only the WAL. Node bytes are coalesced and flushed by the checkpoint coordinator based on configurable thresholds.

**Implementation**:
1. Append deltas to WAL (with checksums but no payload)
2. Fsync WAL
3. Publish to superblock
4. Track dirty ranges per file
5. Coordinator flushes dirty ranges when:
   - `dirty_bytes > threshold` (64-128MB default)
   - `age > threshold` (1-3 seconds default)

**Mitigation for Safety**:
- Include per-node CRC32C in WAL deltas
- On recovery, validate segment bytes against checksums
- Ignore deltas from last epoch if checksums fail (rare)
- Epoch fencing prevents partial parent-child relationships

**Cost**: 
- Fast commits (no synchronous data flush)
- Minimal WAL growth
- Small window of potential data loss (last unflushed epoch only)

**Use Cases**:
- Most production systems (recommended default)
- High throughput requirements
- Systems that can tolerate losing the last ~1-3 seconds of data in catastrophic failure
- Large nodes where payload-in-WAL would be expensive

**Recovery**: Replay WAL, validate checksums, skip corrupted final epoch if needed.

## Configuration

### DurabilityPolicy Structure

```cpp
enum class DurabilityMode {
    STRICT,    // Synchronous data + WAL flush
    EVENTUAL,  // Payload-in-WAL for small nodes
    BALANCED   // WAL-only with coalesced flush (default)
};

struct DurabilityPolicy {
    DurabilityMode mode = DurabilityMode::BALANCED;
    
    // EVENTUAL mode settings
    size_t max_payload_in_wal = 8192;  // Max node size to embed in WAL
    
    // BALANCED mode settings
    size_t dirty_flush_bytes = 128 * 1024 * 1024;  // 128MB
    std::chrono::seconds dirty_flush_age{3};        // 3 seconds
    bool validate_checksums_on_recovery = true;
    
    // Optimization flags (apply to all modes)
    bool coalesce_flushes = true;      // Group contiguous ranges
    bool use_fdatasync = true;          // Use fdatasync vs fsync where possible
    size_t group_commit_ms = 5;         // Group commit window
};
```

## Performance Optimizations

### Applicable to All Modes

1. **Group Commit**: Collapse multiple writers' commits into single fsync
2. **Coalesced Flushes**: Group contiguous dirty ranges per file
3. **Proper Sync Primitives**:
   - Linux: `fdatasync()` for data, `fsync()` for metadata
   - macOS: `F_FULLFSYNC` for critical paths
   - Windows: `FlushFileBuffers()` with proper flags
4. **Page Alignment**: Align nodes to 4KB boundaries within size classes
5. **Batched WAL Appends**: Buffer multiple deltas before append

### Platform-Specific Optimizations

- **Linux**: Use `sync_file_range()` for batched flushes
- **Windows**: `FlushViewOfFile()` with larger ranges
- **macOS**: Careful use of `F_BARRIERFSYNC` vs `F_FULLFSYNC`

## Durability Contract by Mode

### STRICT Mode Contract
```
1. Memory: Write node bytes to mapped memory
2. Flush:  msync()/FlushViewOfFile() all dirty pages
3. WAL:    Append deltas, fsync WAL
4. Commit: Update superblock, fsync superblock
5. Return: All data guaranteed on disk
```

### EVENTUAL Mode Contract
```
1. Memory: Write node bytes to mapped memory (no flush)
2. WAL:    Append deltas WITH payloads for small nodes
3. Fsync:  WAL only
4. Commit: Update superblock
5. Return: WAL is durable, data pages may be in OS cache
6. Later:  Background spiller materializes to segments
```

### BALANCED Mode Contract
```
1. Memory: Write node bytes to mapped memory (no flush)
2. Track:  Record dirty ranges for coordinator
3. WAL:    Append deltas with CRCs (no payload)
4. Fsync:  WAL only
5. Commit: Update superblock
6. Return: WAL is durable, data will flush soon
7. Later:  Coordinator flushes by threshold/time
```

## Recovery Behavior by Mode

### STRICT Recovery
- Simple: Replay WAL, all data guaranteed present
- No validation needed (data was fsynced before WAL)

### EVENTUAL Recovery
- Read payloads from WAL for non-materialized nodes
- Check for "materialized" markers in later deltas
- No data file dependency for small nodes

### BALANCED Recovery
- Replay WAL with CRC validation
- If CRC fails for recent epoch, treat as uncommitted
- Log warning about skipped corrupted epoch
- Continue with last known good state

## Choosing a Policy

### Decision Matrix

| Scenario | Recommended Mode | Rationale |
|----------|-----------------|-----------|
| Unit Tests | STRICT | Deterministic, simple debugging |
| Financial Systems | STRICT | Regulatory compliance |
| High-QPS Small Records | EVENTUAL | Payload-in-WAL avoids data sync |
| General Production | BALANCED | Best throughput/safety trade-off |
| Read-Heavy Workload | BALANCED | Writes are rare, optimize for them |
| Large Blob Storage | BALANCED | Payload-in-WAL too expensive |
| Dev/Staging | BALANCED | Match production behavior |

### Performance Expectations

| Mode | Commit Latency | Recovery Time | Space Overhead |
|------|---------------|---------------|----------------|
| STRICT | 5-50ms | Fast | Minimal |
| EVENTUAL | 0.5-2ms | Slower | WAL grows ~2-10x |
| BALANCED | 1-3ms | Fast | Minimal |

## Implementation Notes

1. **Thread-Local Batching**: Use TLS to batch writes per thread, flush in commit()
2. **Dirty Range Tracking**: Maintain per-file dirty ranges for coalesced flushing
3. **Checksum Everywhere**: Include CRC32C in all WAL records for validation
4. **Epoch Fencing**: Never allow partial parent-child relationships to be visible
5. **Coordinator Integration**: Leverage existing CheckpointCoordinator for background flushing

## Future Enhancements

1. **Adaptive Mode Selection**: Automatically switch modes based on workload
2. **Per-Store Policies**: Different durability for different data structures
3. **Compression**: Compress payloads in EVENTUAL mode
4. **Tiered Durability**: Different policies for internal vs leaf nodes
5. **Async Commit API**: Non-blocking commit with completion callback

## Summary

The three durability modes provide a spectrum of choices:
- **STRICT**: Maximum safety, synchronous everything
- **EVENTUAL**: Fast commits via payload-in-WAL
- **BALANCED** (default): Best of both worlds with coalesced flushing

Most production systems should use BALANCED mode, which provides excellent performance while maintaining strong durability guarantees through the WAL. The small window of potential data loss (last unflushed epoch) is acceptable for most use cases and can be further mitigated through checksums and epoch fencing.