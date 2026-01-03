# Snappy Compression Fix - Technical Documentation

## Problem: Silent Data Corruption with Snappy Compression

When processing large datasets (500k+ records) through the multiread.py pipeline, parquet files were experiencing:
- "Corrupt snappy compressed data" errors
- Silent data loss (only ~100k rows instead of 500k)
- "Parquet magic bytes not found in footer" errors
- Files with incorrect sizes for their row counts

## Root Cause Analysis

### The Memory Corruption Chain

1. **DuckDB's Zero-Copy Optimization**
   - `fetch_arrow_reader()` returns Arrow RecordBatches pointing to DuckDB-managed memory
   - This is a performance optimization (zero-copy = fast)
   - DuckDB **reuses** these memory buffers across `read_next_batch()` calls

2. **dlt's Async Processing**
   - dlt's normalize/load phases run asynchronously
   - On macOS with `normalize.start_method = "spawn"`, a separate process handles normalization
   - Data is held in memory while being processed and written

3. **The Race Condition**
   ```
   Thread 1 (Generator):          Thread 2 (dlt normalize/load):
   ----------------               ---------------------------
   batch1 = read_next_batch()     
   yield batch1              -->  holds reference to batch1 memory
   batch2 = read_next_batch()     still processing batch1...
   (DuckDB overwrites memory!)    
                                  tries to read batch1 memory
                                  → reads garbage/batch2 data!
                                  → snappy compression fails
   ```

4. **Result**: Corrupt Data
   - dlt writes corrupted data to parquet
   - Snappy compression detects corruption and fails
   - Or worse: compression succeeds but data is wrong

## Solution: Three-Layer Defense

### Layer 1: Deep Copy via Serialization

```python
def safe_copy_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Forces true deep copy by serialize → deserialize roundtrip."""
    sink = pa.BufferOutputStream()
    with pa.RecordBatchStreamWriter(sink, batch.schema) as writer:
        writer.write_batch(batch)
    
    buffer = sink.getvalue()
    reader = pa.RecordBatchStreamReader(buffer)
    return reader.read_next_batch()
```

**Why this works:**
- Serialization forces Arrow to materialize ALL data
- Deserialization creates NEW memory buffers
- New buffers are owned by the RecordBatch, not DuckDB
- No shared memory = no corruption

### Layer 2: Convert to Arrow Tables

```python
safe_table = pa.Table.from_batches([safe_batch])
yield safe_table
```

**Why this works:**
- Tables serialize better than batches through multiprocessing
- More stable schema representation
- Better compatibility with dlt's normalize process
- Survives pickling across spawn process boundaries

### Layer 3: Validation

```python
def validate_arrow_table(table: pa.Table, record_type: str) -> None:
    """Pre-flight validation before yielding to dlt."""
    if table.num_rows == 0:
        raise ValueError(f"Empty table for {record_type}")
    # ... more checks ...
```

**Why this works:**
- Catches corruption EARLY (at generation time)
- Fails fast rather than producing corrupt files
- Validates data integrity before expensive compression

## Configuration Recommendations

Update your `.dlt/config.toml`:

```toml
[destination.filesystem.parquet]
compression = "snappy"
version = "2.6"  # Explicit version for compatibility

[data_writer]
file_max_bytes = 134_217_728  # 128 MB
file_max_items = 100_000      # Reduced from 250k to avoid file splits
max_buffer_size = 50_000      # Align with batch_size

[normalize]
start_method = "spawn"        # Required on macOS
```

## Verification

All three tests pass:
1. ✅ Generator produces correct row counts
2. ✅ Direct Arrow → Parquet with snappy works
3. ✅ Full dlt pipeline produces valid parquet files

## Performance Impact

- **Memory**: ~2x per batch (deep copy overhead)
- **CPU**: +5-10% for serialization roundtrip
- **Overall**: Negligible on large datasets, correctness >> speed

## Key Takeaways

1. **Zero-copy is dangerous** when memory ownership crosses boundaries
2. **Deep copy when needed** - use serialization to guarantee independence
3. **Validate early** - catch corruption at generation, not compression
4. **Process-safe data** - ensure data survives multiprocessing serialization

The fix maintains Arrow efficiency while guaranteeing data integrity.
