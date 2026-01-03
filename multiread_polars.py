"""Multi-record source with Polars - Simpler and safer alternative to DuckDB.

This is an alternative implementation using Polars instead of DuckDB.
Polars eliminates the buffer reuse issue entirely because it owns its memory by default.

## Key Advantages Over DuckDB

1. **No Buffer Reuse**: Polars creates owned Arrow arrays, no zero-copy corruption
2. **Simpler Code**: No need for safe_copy_batch(), validate_arrow_table(), or workarounds
3. **Memory Safety**: All data is materialized and owned from the start
4. **Same Performance**: Polars is as fast as DuckDB for CSV → Arrow operations
5. **Native Streaming**: Built-in support for lazy evaluation and batching

## Comparison

**DuckDB approach (current):**
- Requires 3-layer defense (deep copy, validation, pre-flight checks)
- Complex memory management with serialization roundtrips
- Risk of buffer corruption if workarounds fail

**Polars approach (this file):**
- Direct CSV → Arrow conversion with owned memory
- No deep copy needed - data is safe by design
- Simpler, more maintainable code
"""

import dlt
import polars as pl
import pyarrow as pa
from loguru import logger


def get_max_fields_for_type_polars(record_type: str, file_path: str) -> int:
    """Get maximum field count for a record type using Polars.

    Args:
        record_type: The record type code to filter on
        file_path: Path to the data file

    Returns:
        Maximum number of fields for this record type
    """
    # Use lazy evaluation for efficiency
    df = pl.scan_csv(
        file_path,
        has_header=False,
        separator="\n",
        new_columns=["raw_line"],
    ).filter(
        pl.col("raw_line").str.starts_with(f"{record_type}|")
    ).select(
        pl.col("raw_line").str.split("|").alias("fields")
    ).select(
        pl.col("fields").list.len().alias("field_count")
    ).select(
        pl.col("field_count").max()
    ).collect()
    
    max_fields = df["field_count"][0]
    return max_fields if max_fields else 0


def create_batch_generator_polars(
    record_type: str, file_path: str, batch_size: int, max_fields: int
):
    """Create a generator using Polars - no buffer corruption issues.

    Args:
        record_type: The record type code to filter on
        file_path: Path to the data file
        batch_size: Rows per batch
        max_fields: Pre-computed maximum field count (for stable schema)

    Yields:
        Arrow Table objects with owned memory (no corruption risk)
    """
    batch_count = 0
    total_rows = 0

    try:
        # Read, filter, and split in one lazy query
        df = pl.scan_csv(
            file_path,
            has_header=False,
            separator="\n",
            new_columns=["raw_line"],
        ).filter(
            pl.col("raw_line").str.starts_with(f"{record_type}|")
        ).select(
            pl.col("raw_line").str.split("|").alias("fields")
        )

        # Get expected count for validation
        expected_count = df.select(pl.len()).collect()["len"][0]
        logger.debug(
            f"Record type {record_type}: expecting {expected_count} total rows"
        )

        # Collect and process in batches
        # Note: Polars doesn't have the same streaming API as DuckDB,
        # but we can collect and split into batches
        full_df = df.collect()

        # Process in batches
        for i in range(0, len(full_df), batch_size):
            batch_df = full_df[i:i + batch_size]

            # Extract fields into separate columns
            # Polars makes this easy with list operations
            field_exprs = [
                pl.col("fields").list.get(j).alias(f"field_{j}")
                for j in range(max_fields)
            ]
            
            extracted_df = batch_df.select(field_exprs)

            # Convert to Arrow Table
            # ✨ KEY DIFFERENCE: Polars owns this memory, no corruption risk!
            arrow_table = extracted_df.to_arrow()

            batch_count += 1
            total_rows += len(arrow_table)
            logger.info(
                f"Record type {record_type}: batch {batch_count} ({len(arrow_table)} rows, {total_rows}/{expected_count} total)"
            )

            # Yield Arrow Table - no deep copy needed!
            yield arrow_table

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


def resource_generator_factory_polars(
    record_type: str, file_path: str, batch_size: int, max_fields: int
):
    """Factory function to create generator for a specific record type using Polars.

    This function is called once per record type to create a callable generator.
    dlt will call the returned callable once to get the generator.
    """

    def generator():
        yield from create_batch_generator_polars(
            record_type, file_path, batch_size, max_fields
        )

    return generator


@dlt.source
def multi_source_polars(
    record_types: list[str],
    file_path: str,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
):
    """Multi-record source using Polars - simpler and safer than DuckDB.

    Args:
        record_types: List of record type codes to extract
        file_path: Path to the multi-layout data file
        batch_size: Number of rows per batch
        write_disposition: Write mode for resources ("append", "replace", "merge")

    Yields:
        dlt resources with extracted and transformed data
    """
    for record_type in record_types:
        try:
            # Pre-compute max fields once per record type
            max_fields = get_max_fields_for_type_polars(record_type, file_path)

            if max_fields == 0:
                logger.warning(f"No records found for type {record_type}")
                continue

            logger.info(f"Found record type {record_type} (max {max_fields} fields)")

            # Get generator factory
            gen_func = resource_generator_factory_polars(
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


# Example usage comparison
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
        pipeline_name="multi_layout_pipeline_polars",
        destination="filesystem",
        dataset_name="record_data_polars",
        progress="log",
    )

    # Run pipeline with Polars-based source
    load_info = pipeline.run(
        multi_source_polars(
            record_types=record_types,
            file_path="/Users/rory/github/sandbox/multi_layout_data_xl.txt",
            batch_size=50_000,
            write_disposition="replace",
        ),
    )

    logger.info("=" * 80)
    logger.info("POLARS-BASED PIPELINE LOAD RESULTS")
    logger.info("=" * 80)
    logger.info(f"Load info: {load_info}")
