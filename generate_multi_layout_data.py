"""Generate mock multi-layout pipe-delimited data file with 100k records."""

import random
import string
from datetime import datetime
from pathlib import Path

from loguru import logger


def generate_random_string(max_length: int, optional: bool = True) -> str:
    """Generate random string data.

    Args:
        max_length: Maximum length of the string
        optional: If True, 30% chance of returning empty string
    """
    if optional and random.random() < 0.3:
        return ""

    # Generate string between 1 and max_length
    length = random.randint(1, max_length)
    chars = string.ascii_letters + string.digits + " "
    return "".join(random.choices(chars, k=length))


def generate_record_9001() -> str:
    """Generate record type 9001 with 96 columns."""
    record_type = "9001"

    # Mix of different length fields
    # 8 long fields (max 255), rest shorter
    fields = []
    for i in range(96):
        if i < 8:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            # Shorter fields with varying lengths
            max_len = random.choice([10, 20, 50, 100])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9002() -> str:
    """Generate record type 9002 with 60 columns."""
    record_type = "9002"

    fields = []
    for i in range(60):
        if i < 5:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 30, 50])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9004() -> str:
    """Generate record type 9004 with 36 columns."""
    record_type = "9004"

    fields = []
    for i in range(36):
        if i < 3:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9005() -> str:
    """Generate record type 9005 with 16 columns."""
    record_type = "9005"

    fields = []
    for i in range(16):
        if i < 2:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 40])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9006() -> str:
    """Generate record type 9006 with 21 columns."""
    record_type = "9006"

    fields = []
    for i in range(21):
        if i < 2:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 30])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9009() -> str:
    """Generate record type 9009 with 34 columns."""
    record_type = "9009"

    fields = []
    for i in range(34):
        if i < 3:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9012() -> str:
    """Generate record type 9012 with 37 columns."""
    record_type = "9012"

    fields = []
    for i in range(37):
        if i < 3:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9019() -> str:
    """Generate record type 9019 with 27 columns."""
    record_type = "9019"

    fields = []
    for i in range(27):
        if i < 3:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9020() -> str:
    """Generate record type 9020 with 21 columns."""
    record_type = "9020"

    fields = []
    for i in range(21):
        if i < 2:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 30])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_record_9031() -> str:
    """Generate record type 9031 with 39 columns."""
    record_type = "9031"

    fields = []
    for i in range(39):
        if i < 3:
            # Long fields
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50, 75])
            fields.append(generate_random_string(max_len))

    return record_type + "|" + "|".join(fields)


def generate_multi_layout_file(
    output_path: str = "multi_layout_data.txt", master_9002_count: int = 500_000
) -> None:
    """Generate multi-layout data file with proportional record counts.

    Args:
        output_path: Path to output file
        master_9002_count: Number of 9002 records (master count that others scale from)
    """
    # Define proportions relative to 9002 (based on 500k reference)
    # 9002 is the master, others are relative proportions
    PROPORTIONS = {
        "9001": 1.0,  # 500k / 500k
        "9002": 1.0,  # master
        "9004": 0.094,  # 47k / 500k
        "9005": 0.276,  # 138k / 500k
        "9006": 1.29,  # 645k / 500k
        "9009": 0.04,  # 20k / 500k
        "9012": 0.118,  # 59k / 500k
        "9019": 1.0,  # 500k / 500k
        "9020": 0.126,  # 63k / 500k
        "9031": 0.314,  # 157k / 500k
    }

    # Calculate target counts for each record type
    target_counts = {
        rt: int(master_9002_count * proportion)
        for rt, proportion in PROPORTIONS.items()
    }

    total_records = sum(target_counts.values())
    logger.info(
        "Starting data generation: {:,} total records (master 9002 count: {:,})",
        total_records,
        master_9002_count,
    )

    # Record type generators
    generators = {
        "9001": generate_record_9001,
        "9002": generate_record_9002,
        "9004": generate_record_9004,
        "9005": generate_record_9005,
        "9006": generate_record_9006,
        "9009": generate_record_9009,
        "9012": generate_record_9012,
        "9019": generate_record_9019,
        "9020": generate_record_9020,
        "9031": generate_record_9031,
    }

    # Build list of records to generate (shuffled for realistic distribution)
    records_to_generate = []
    for record_type, count in target_counts.items():
        records_to_generate.extend([record_type] * count)

    random.shuffle(records_to_generate)

    # Track actual counts for trailer
    record_counts = dict.fromkeys(generators.keys(), 0)

    # Get file info for header
    filename = Path(output_path).name
    created_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(output_path, "w", encoding="utf-8") as f:
        # Write header record
        header = f"H|{filename}|{created_datetime}"
        f.write(header + "\n")

        # Generate data records
        for i, record_type in enumerate(records_to_generate, 1):
            generator = generators[record_type]
            record = generator()
            f.write(record + "\n")
            record_counts[record_type] += 1

            # Progress indicator
            if i % 50_000 == 0:
                progress_pct = round((i / total_records) * 100, 1)
                logger.debug(
                    "Progress update: {:,} records generated ({}%)", i, progress_pct
                )

        # Write trailer record
        trailer_parts = [
            f"{rt}-{count:010d}" for rt, count in sorted(record_counts.items())
        ]
        trailer = "T|" + "|".join(trailer_parts)
        f.write(trailer + "\n")

    logger.success(
        "File generation complete: {} ({:,} records)", output_path, total_records
    )

    logger.info("Record type breakdown:")
    for record_type, count in sorted(record_counts.items()):
        percentage = (count / total_records) * 100
        logger.info("  {}: {:,} ({:.1f}%)", record_type, count, percentage)

    # File size
    file_size = Path(output_path).stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    file_size_gb = file_size / (1024**3)
    logger.info("File size: {:.2f} MB ({:.2f} GB)", file_size_mb, file_size_gb)


if __name__ == "__main__":
    # Generate the file
    # Adjust master_9002_count to scale all other record types proportionally
    generate_multi_layout_file(
        output_path="multi_layout_data_xl.txt",
        master_9002_count=500_000,  # Change this to scale all record types
    )
