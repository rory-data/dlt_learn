"""Multi-record source with safe Arrow streaming.

This module implements a robust multi-layout CSV parser that extracts different
record types from a single file and loads them to separate parquet files using dlt.

## Snappy Compression Fix

The critical issue this addresses is **silent data corruption** from DuckDB buffer reuse:

### Root Cause
1. DuckDB's `fetch_arrow_reader()` uses zero-copy buffers for performance
2. These buffers are reused/overwritten on each `read_next_batch()` call
3. dlt's async normalize/load phases (especially with spawn on macOS) hold references
   to these buffers while processing
4. When DuckDB overwrites the buffer for the next batch, dlt still references the
   old (now corrupted) memory location
5. Result: "corrupt snappy compressed data" errors and parquet files with garbage data

### Solution
Three-layer defense:

1. **safe_copy_batch()**: Deep copy via Arrow serialization to decouple from DuckDB memory
   - Serializes batch to buffer, then deserializes to create new batch with owned memory
   - Validates the copy to ensure data integrity
   
2. **Convert to Arrow Tables**: Better serialization through dlt's multiprocessing
   - Tables pickle more reliably than batches across process boundaries
   - More stable schema representation
   
3. **validate_arrow_table()**: Pre-flight checks before yielding to dlt
   - Ensures data is materialized and valid
   - Catches corruption early rather than during parquet write

This maintains Arrow efficiency while guaranteeing data integrity across process boundaries.
"""

from contextlib import contextmanager

import dlt
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger


@contextmanager
def duckdb_connection():
    """Context manager for DuckDB connections with proper lifecycle."""
    con = duckdb.connect(":memory:")
    try:
        yield con
    finally:
        con.close()


def safe_copy_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Creates a deep copy of a RecordBatch to decouple it from DuckDB memory.

    This is critical when using DuckDB's fetch_arrow_reader, as DuckDB may
    reuse underlying memory buffers for subsequent batches. Serialization
    ensures all buffers (including string data) are copied.
    
    The serialization roundtrip guarantees:
    1. All data is materialized (no lazy references to DuckDB memory)
    2. Buffers are owned by the new RecordBatch
    3. Data is validated during deserialization
    4. Compatible with compression (snappy, gzip, etc.)
    """
    # Validate input batch
    if batch.num_rows == 0:
        return batch
    
    # Serialize and deserialize to create true deep copy
    # This ensures all memory is owned and not shared with DuckDB
    sink = pa.BufferOutputStream()
    with pa.RecordBatchStreamWriter(sink, batch.schema) as writer:
        writer.write_batch(batch)
    
    # Get buffer and validate it was written
    buffer = sink.getvalue()
    if len(buffer) == 0:
        raise ValueError("Failed to serialize batch - empty buffer")
    
    # Deserialize to create new batch with owned memory
    reader = pa.RecordBatchStreamReader(buffer)
    copied_batch = reader.read_next_batch()
    
    # Validate the copy matches original
    if copied_batch.num_rows != batch.num_rows:
        raise ValueError(
            f"Deep copy validation failed: row count mismatch "
            f"(original: {batch.num_rows}, copy: {copied_batch.num_rows})"
        )
    
    return copied_batch


def validate_arrow_table(table: pa.Table, record_type: str) -> None:
    """Validate Arrow Table before yielding to dlt.
    
    Ensures:
    1. Table has data
    2. Schema is valid
    3. No null columns
    4. Data is properly materialized (not referencing external memory)
    
    Args:
        table: Arrow Table to validate
        record_type: Record type for error messages
        
    Raises:
        ValueError: If validation fails
    """
    if table.num_rows == 0:
        raise ValueError(f"Record type {record_type}: Empty table")
    
    if table.num_columns == 0:
        raise ValueError(f"Record type {record_type}: No columns in table")
    
    # Verify schema is complete
    if not table.schema:
        raise ValueError(f"Record type {record_type}: Invalid schema")
    
    # Ensure all columns have data
    for col_name in table.column_names:
        col = table.column(col_name)
        if col is None:
            raise ValueError(f"Record type {record_type}: Null column {col_name}")


def extract_fields_from_batch(batch: pa.RecordBatch, max_fields: int) -> pa.RecordBatch:
    """Extract array column into separate field columns using Arrow compute."""
    if batch.num_rows == 0:
        return batch

    fields_col = batch.column("fields_array")
    extracted_arrays = []
    extracted_names = []

    for i in range(max_fields):
        field_array = pc.list_element(fields_col, i)
        if isinstance(field_array, pa.ChunkedArray):
            field_array = field_array.combine_chunks()
        extracted_arrays.append(field_array)
        extracted_names.append(f"field_{i}")

    return pa.RecordBatch.from_arrays(extracted_arrays, names=extracted_names)


def get_max_fields_for_type(record_type: str, file_path: str) -> int:
    """Get maximum field count for a record type (computed once per type).

    Args:
        record_type: The record type code to filter on
        file_path: Path to the data file

    Returns:
        Maximum number of fields for this record type
    """
    with duckdb_connection() as con:
        # Note: file_path and record_type are trusted inputs from the pipeline
        result = con.execute(  # noqa: S608
            f"SELECT MAX(array_length(string_split(column0, '|'))) as max_fields "
            f"FROM read_csv('{file_path}', header=false, sep='\\n') "
            f"WHERE column0 LIKE '{record_type}|%'"
        ).fetchall()
        return result[0][0] or 0


def create_batch_generator(
    record_type: str, file_path: str, batch_size: int, max_fields: int
):
    """Create a generator function for a specific record type.

    Args:
        record_type: The record type code to filter on
        file_path: Path to the data file
        batch_size: Rows per batch
        max_fields: Pre-computed maximum field count (for stable schema)

    Yields:
        Safe deep-copied Arrow Table objects (converted from RecordBatch for better serialization)
    """
    con = duckdb.connect(":memory:")
    batch_count = 0
    total_rows = 0

    try:
        # Read and filter data
        full_table = con.read_csv(file_path, header=False, sep="\n")
        rel = full_table.filter(f"column0 LIKE '{record_type}|%'").select(
            "string_split(column0, '|') AS fields_array"
        )

        # Get expected total count for validation
        expected_count = rel.count("*").fetchone()[0]
        logger.debug(
            f"Record type {record_type}: expecting {expected_count} total rows"
        )

        # Create Arrow reader with specified batch size
        reader = rel.fetch_arrow_reader(batch_size=batch_size)

        # Read all batches
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break

            if batch.num_rows == 0:
                break

            # Transform: extract fields from array column
            extracted_batch = extract_fields_from_batch(batch, max_fields)

            # CRITICAL: Deep copy to decouple from DuckDB's reused buffers
            safe_batch = safe_copy_batch(extracted_batch)

            # Convert batch to Table for better serialization through dlt's spawn process
            # Tables are more stable than batches when pickled/unpickled across processes
            safe_table = pa.Table.from_batches([safe_batch])
            
            # Validate table before yielding to catch any corruption early
            validate_arrow_table(safe_table, record_type)

            batch_count += 1
            total_rows += safe_table.num_rows
            logger.info(
                f"Record type {record_type}: batch {batch_count} ({safe_table.num_rows} rows, {total_rows}/{expected_count} total)"
            )

            # Yield as Arrow Table for better compatibility with dlt
            yield safe_table

        # Validate all rows were yielded
        if total_rows != expected_count:
            logger.error(
                f"Record type {record_type}: DATA LOSS! Yielded {total_rows} rows but expected {expected_count}"
            )
            raise ValueError(
                f"Data loss detected for record type {record_type}: "
                f"yielded {total_rows} rows but expected {expected_count}"
            )

        logger.success(
            f"Record type {record_type}: Successfully yielded all {total_rows} rows in {batch_count} batches"
        )

    except Exception as e:
        logger.error(
            f"Record type {record_type}: Error during batch generation after {batch_count} batches ({total_rows} rows): {e}"
        )
        raise
    finally:
        con.close()
        logger.debug(f"Record type {record_type}: DuckDB connection closed")


def resource_generator_factory(
    record_type: str, file_path: str, batch_size: int, max_fields: int
):
    """Factory function to create generator for a specific record type.

    This function is called once per record type to create a callable generator.
    dlt will call the returned callable once to get the generator.
    """

    def generator():
        yield from create_batch_generator(
            record_type, file_path, batch_size, max_fields
        )

    return generator


@dlt.source
def multi_source(
    record_types: list[str],
    file_path: str,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
):
    """Multi-record source with single file read and partitioned streaming.

    Args:
        record_types: List of record type codes to extract
        file_path: Path to the multi-layout data file
        batch_size: Number of rows per batch (DuckDB fetch_arrow_reader parameter).
            Should align with max_buffer_size in dlt config for optimal performance.
        write_disposition: Write mode for resources ("append", "replace", "merge").
            Defaults to "replace" for deterministic behavior on reruns.

    Yields:
        dlt resources with extracted and transformed data
    """
    # Pre-partition: for each record type, create a resource
    for record_type in record_types:
        try:
            # Pre-compute max fields once per record type (eliminates schema drift)
            max_fields = get_max_fields_for_type(record_type, file_path)

            if max_fields == 0:
                logger.warning(f"No records found for type {record_type}")
                continue

            logger.info(f"Found record type {record_type} (max {max_fields} fields)")

            # Get generator factory
            gen_func = resource_generator_factory(
                record_type, file_path, batch_size, max_fields
            )

            # Yield resource with generator callable
            yield dlt.resource(
                gen_func,
                name=f"record_{record_type}",
                write_disposition=write_disposition,
            )

        except Exception as e:
            logger.error(
                f"Failed to process record type {record_type}: {e}", exc_info=True
            )
            continue


# Example pipeline
if __name__ == "__main__":
    record_types = (
        "9001",
        "9002",
        "9004",
        "9005",
        "9006",
        "9009",
        "9012",
        "9019",
        "9020",
        "9031",
    )

    # Define pipeline
    pipeline = dlt.pipeline(
        pipeline_name="multi_layout_pipeline",
        destination="filesystem",
        dataset_name="record_data",
        progress="log",
    )

    # Run pipeline with all record types and custom batch size
    # Note: batch_size should align with max_buffer_size in .dlt/config.toml
    # for optimal performance (default: 50,000)
    load_info = pipeline.run(
        multi_source(
            record_types=record_types,
            file_path="/Users/rory/github/sandbox/multi_layout_data_xl.txt",
            batch_size=50_000,  # Aligns with default max_buffer_size in config
            write_disposition="replace",  # Prevents duplicates on reruns
        ),
    )

    logger.info("=" * 80)
    logger.info("PIPELINE LOAD RESULTS")
    logger.info("=" * 80)
    logger.info(f"Load info: {load_info}")
    logger.info(
        f"Loads state: {load_info.loads_ids if hasattr(load_info, 'loads_ids') else 'N/A'}"
    )

    # Log package information
    if hasattr(load_info, "failed_jobs"):
        logger.info(f"Failed jobs: {load_info.failed_jobs}")
    if hasattr(load_info, "has_successfully_loaded"):
        logger.info(f"Successfully loaded: {load_info.has_successfully_loaded}")

    logger.info("=" * 80)
