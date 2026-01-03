"""Clean Polars-based multi-record source implementation.

This implementation is designed from scratch for Polars, focusing on:
- Memory efficiency with streaming-like behavior
- Clean separation of concerns
- Proper resource isolation for dlt's async processing
- Post-processing validation of output files

Design Philosophy:
- Use Polars' lazy evaluation (scan_csv) for memory efficiency
- Process each record type independently (isolated resources)
- Yield Arrow Tables incrementally for dlt's streaming pipeline
- Polars owns all memory - no buffer corruption issues
- Post-process validation ensures data integrity

References:
- dlt documentation: https://dlthub.com/docs
- Polars documentation: https://docs.pola.rs/
"""

from pathlib import Path
from typing import Iterator, Dict, Optional, Any
import glob

import dlt
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


def scan_and_filter_record_type(
    file_path: str, record_type: str
) -> pl.LazyFrame:
    """Scan CSV and filter for specific record type using lazy evaluation.
    
    This is memory-efficient as Polars doesn't load the entire file.
    
    Args:
        file_path: Path to the multi-layout CSV file
        record_type: Record type code to filter (e.g., "9001")
    
    Returns:
        LazyFrame filtered for the specified record type
    """
    return (
        pl.scan_csv(
            file_path,
            has_header=False,
            separator="\n",
            new_columns=["raw_line"],
        )
        .filter(pl.col("raw_line").str.starts_with(f"{record_type}|"))
        .select(
            pl.col("raw_line")
            .str.split("|")
            .alias("fields")
        )
    )


def compute_schema_for_record_type(
    file_path: str, record_type: str
) -> int:
    """Compute maximum field count for a record type.
    
    This determines the schema width (number of columns) for this record type.
    Uses lazy evaluation and only materializes the count.
    
    Args:
        file_path: Path to the multi-layout CSV file
        record_type: Record type code
        
    Returns:
        Maximum number of fields found for this record type
    """
    lazy_df = scan_and_filter_record_type(file_path, record_type)
    
    max_fields_df = (
        lazy_df
        .select(pl.col("fields").list.len().alias("field_count"))
        .select(pl.col("field_count").max())
        .collect()
    )
    
    max_fields = max_fields_df["field_count"][0]
    return max_fields if max_fields is not None else 0


def extract_fields_to_columns(
    df: pl.DataFrame, num_fields: int
) -> pl.DataFrame:
    """Extract list of fields into separate columns.
    
    Transforms from: [["9001", "a", "b"]] 
    To: {"field_0": "9001", "field_1": "a", "field_2": "b"}
    
    Args:
        df: DataFrame with a 'fields' column containing lists
        num_fields: Number of field columns to extract
        
    Returns:
        DataFrame with fields extracted to separate columns
    """
    return df.select([
        pl.col("fields").list.get(i).alias(f"field_{i}")
        for i in range(num_fields)
    ])


def stream_record_type_batches(
    file_path: str,
    record_type: str,
    batch_size: int,
    num_fields: int,
) -> Iterator[pa.Table]:
    """Stream batches of records for a specific record type.
    
    This is the core generator that produces Arrow Tables for dlt.
    Each table is independent with owned memory (no corruption risk).
    
    Design:
    - Lazy scan filters early (memory efficient)
    - Collect only what's needed for this record type
    - Process in batches to control memory
    - Each Arrow Table owns its memory
    
    Args:
        file_path: Path to the multi-layout CSV file
        record_type: Record type code to process
        batch_size: Number of rows per batch
        num_fields: Number of field columns in schema
        
    Yields:
        Arrow Tables, each containing a batch of records
    """
    # Lazy scan and filter (doesn't load into memory yet)
    lazy_df = scan_and_filter_record_type(file_path, record_type)
    
    # Collect filtered data (only this record type)
    # This is unavoidable but isolated per record type
    filtered_df = lazy_df.collect()
    
    total_rows = len(filtered_df)
    logger.debug(f"Record type {record_type}: {total_rows} rows to process")
    
    # Process in batches for memory efficiency
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, total_rows)
        
        # Slice batch
        batch_df = filtered_df.slice(start_idx, end_idx - start_idx)
        
        # Extract fields to columns
        extracted_df = extract_fields_to_columns(batch_df, num_fields)
        
        # Convert to Arrow Table (Polars owns memory, safe to yield)
        arrow_table = extracted_df.to_arrow()
        
        logger.info(
            f"Record type {record_type}: batch {batch_idx + 1}/{num_batches} "
            f"({len(arrow_table)} rows)"
        )
        
        yield arrow_table
    
    logger.success(
        f"Record type {record_type}: Successfully yielded {total_rows} rows "
        f"in {num_batches} batches"
    )


def create_resource_for_record_type(
    record_type: str,
    file_path: str,
    batch_size: int,
    write_disposition: str,
) -> Optional[Any]:
    """Create an isolated dlt resource for a single record type.
    
    Each resource is independent, ensuring dlt's async processing
    doesn't interfere between record types.
    
    Args:
        record_type: Record type code
        file_path: Path to the multi-layout CSV file
        batch_size: Number of rows per batch
        write_disposition: dlt write mode ("replace", "append", "merge")
        
    Returns:
        dlt Resource configured for this record type
    """
    # Compute schema once
    num_fields = compute_schema_for_record_type(file_path, record_type)
    
    if num_fields == 0:
        logger.warning(f"No records found for type {record_type}")
        return None
    
    logger.info(f"Record type {record_type}: schema width = {num_fields} fields")
    
    # Create generator function (dlt will call this once per resource)
    def generator():
        yield from stream_record_type_batches(
            file_path, record_type, batch_size, num_fields
        )
    
    # Return dlt resource with isolated configuration
    return dlt.resource(
        generator,
        name=f"record_{record_type}",
        write_disposition=write_disposition,
    )


@dlt.source
def multi_source_polars(
    record_types: list[str],
    file_path: str,
    batch_size: int = 50_000,
    write_disposition: str = "replace",
):
    """Multi-record source using Polars with clean, memory-efficient design.
    
    This source creates isolated resources for each record type, ensuring
    dlt's async processing keeps them separate and safe.
    
    Key Features:
    - Each record type is an isolated resource
    - Lazy evaluation for memory efficiency
    - Streaming-like batch processing
    - Polars owns all memory (no corruption)
    - Compatible with all dlt features
    
    Args:
        record_types: List of record type codes to extract (e.g., ["9001", "9002"])
        file_path: Path to the multi-layout CSV file
        batch_size: Number of rows per batch (default: 50,000)
        write_disposition: dlt write mode (default: "replace")
            Options: "replace", "append", "merge"
    
    Yields:
        dlt Resources, one per record type
        
    Example:
        >>> pipeline = dlt.pipeline(
        ...     pipeline_name="my_pipeline",
        ...     destination="filesystem",
        ...     dataset_name="my_data"
        ... )
        >>> load_info = pipeline.run(
        ...     multi_source_polars(
        ...         record_types=["9001", "9002"],
        ...         file_path="data.txt",
        ...         batch_size=50_000
        ...     )
        ... )
    """
    for record_type in record_types:
        try:
            resource = create_resource_for_record_type(
                record_type, file_path, batch_size, write_disposition
            )
            
            if resource is not None:
                yield resource
                
        except Exception as e:
            logger.error(
                f"Failed to create resource for record type {record_type}: {e}",
                exc_info=True
            )
            continue


def validate_parquet_outputs(
    pipeline_storage_path: str,
    expected_counts: Dict[str, int],
    record_types: list[str],
) -> Dict[str, dict]:
    """Post-processing validation of generated parquet files.
    
    Validates:
    1. Record counts match expected values
    2. First column (field_0) contains only the correct record type
    3. Files are readable and not corrupted
    
    Args:
        pipeline_storage_path: Path to dlt pipeline storage
        expected_counts: Dict mapping record_type -> expected row count
        record_types: List of record types to validate
        
    Returns:
        Dict with validation results per record type
        
    Example:
        >>> results = validate_parquet_outputs(
        ...     pipeline._pipeline_storage.storage_path,
        ...     {"9001": 1000, "9002": 2000},
        ...     ["9001", "9002"]
        ... )
    """
    logger.info("=" * 80)
    logger.info("POST-PROCESSING VALIDATION")
    logger.info("=" * 80)
    
    # Find all parquet files
    parquet_pattern = str(Path(pipeline_storage_path) / "**" / "*.parquet")
    parquet_files = glob.glob(parquet_pattern, recursive=True)
    
    logger.info(f"Found {len(parquet_files)} parquet files to validate")
    
    results = {}
    
    for record_type in record_types:
        # Find files for this record type
        type_files = [
            f for f in parquet_files 
            if f"record_{record_type}" in Path(f).name
        ]
        
        if not type_files:
            logger.warning(f"No parquet files found for record type {record_type}")
            results[record_type] = {
                "status": "missing",
                "files": 0,
                "total_rows": 0,
                "errors": ["No files found"]
            }
            continue
        
        logger.info(f"\nValidating record type {record_type} ({len(type_files)} files):")
        
        total_rows = 0
        errors = []
        
        for file_path in type_files:
            try:
                # Read parquet file
                table = pq.read_table(file_path)
                file_rows = len(table)
                total_rows += file_rows
                
                logger.info(f"  {Path(file_path).name}: {file_rows} rows")
                
                # Validate field_0 contains only the correct record type
                if "field_0" in table.column_names:
                    field_0_values = table.column("field_0").to_pylist()
                    unique_values = set(field_0_values)
                    
                    if unique_values != {record_type}:
                        error = f"field_0 contains incorrect values: {unique_values}"
                        errors.append(error)
                        logger.error(f"    ✗ {error}")
                    else:
                        logger.success(f"    ✓ field_0 validation passed")
                else:
                    errors.append("field_0 column not found")
                    logger.error(f"    ✗ field_0 column not found")
                    
            except Exception as e:
                error = f"Failed to read {Path(file_path).name}: {e}"
                errors.append(error)
                logger.error(f"    ✗ {error}")
        
        # Check against expected count
        expected = expected_counts.get(record_type)
        count_match = total_rows == expected if expected else None
        
        if expected and not count_match:
            error = f"Count mismatch: expected {expected}, got {total_rows}"
            errors.append(error)
            logger.error(f"  ✗ {error}")
        elif expected:
            logger.success(f"  ✓ Count matches expected: {total_rows}")
        
        results[record_type] = {
            "status": "valid" if not errors else "invalid",
            "files": len(type_files),
            "total_rows": total_rows,
            "expected_rows": expected,
            "count_match": count_match,
            "errors": errors
        }
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)
    
    all_valid = all(r["status"] == "valid" for r in results.values())
    
    for record_type, result in results.items():
        status_icon = "✓" if result["status"] == "valid" else "✗"
        logger.info(
            f"{status_icon} {record_type}: {result['total_rows']} rows "
            f"in {result['files']} files - {result['status'].upper()}"
        )
        
        if result["errors"]:
            for error in result["errors"]:
                logger.error(f"    - {error}")
    
    if all_valid:
        logger.success("\n✅ All validations passed!")
    else:
        logger.error("\n✗ Some validations failed!")
    
    logger.info("=" * 80)
    
    return results


# Example usage
if __name__ == "__main__":
    import os
    
    record_types = [
        "9001", "9002", "9004", "9005", "9006",
        "9009", "9012", "9019", "9020", "9031",
    ]
    
    # Define pipeline
    pipeline = dlt.pipeline(
        pipeline_name="multi_layout_pipeline_polars_clean",
        destination="filesystem",
        dataset_name="record_data_polars_clean",
        progress="log",
    )
    
    # Run pipeline
    file_path = "/Users/rory/github/sandbox/multi_layout_data_xl.txt"
    
    load_info = pipeline.run(
        multi_source_polars(
            record_types=record_types,
            file_path=file_path,
            batch_size=50_000,
            write_disposition="replace",
        ),
    )
    
    logger.info("=" * 80)
    logger.info("PIPELINE RESULTS")
    logger.info("=" * 80)
    logger.info(f"Load info: {load_info}")
    
    # Post-processing validation
    # Note: You would need to provide expected counts from your data source
    expected_counts = {
        # Example: these would come from trailer records or pre-scan
        # "9001": 500000,
        # "9002": 500000,
        # etc.
    }
    
    validation_results = validate_parquet_outputs(
        pipeline._pipeline_storage.storage_path,
        expected_counts,
        record_types,
    )
