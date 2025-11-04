"""
build_standard_met_year_catalog.py 

Generate intake catalog for standard-met-year dataset from S3.
Creates CSV file with metadata for each file and YAML definition for intake catalog

"""

import re
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
import yaml
import boto3


# S3 configuration
BUCKET_NAME = "cadcat"
PREFIX = "climate-profiles"

# Output filepaths
CATALOG_FILENAME = "cae-standard-met-year-collection"  # Filename no extension
OUTPUT_CSV_FILEPATH = f"{CATALOG_FILENAME}.csv"
OUTPUT_YAML_FILEPATH = f"{CATALOG_FILENAME}.yaml"


def parse_stdyr_filename(filename: str) -> Optional[Dict[str, str]]:
    """
    Parse standard-met-year filename.

    Parameters
    ----------
    filename : str
        Filename to parse following pattern:
        stdyr_[VARIABLE]_[PERCENTILE]_[STATION_NAME]_[TIME_PERIOD].csv

    Returns
    -------
    dict or None
        Dictionary containing parsed metadata with keys:
        - station_name : str
        - variable : str
        - percentile : str
        - time_period : str
        - filename : str
        Returns None if filename cannot be parsed.
    """
    # Pattern: stdyr_[VARIABLE]_[PERCENTILE]_[STATION_NAME]_[TIME_PERIOD].csv
    # VARIABLE can contain underscores, PERCENTILE is like "05ptile", "50ptile", "95ptile"
    pattern = r"stdyr_(.+)_(\d{2}ptile)_(.+)_([^_]+)\.csv$"
    match = re.match(pattern, filename)

    if not match:
        return None

    variable, percentile, station_name, time_period = match.groups()

    return {
        "station_name": station_name,
        "variable": variable,
        "percentile": percentile,
        "time_period": time_period,
        "filename": filename,
    }


def generate_catalog(s3_bucket: str, s3_prefix: str) -> None:
    """
    Generate intake catalog for standard-met-year dataset from S3.

    Creates two files:
    - CSV catalog with file metadata
    - YAML intake catalog definition

    Parameters
    ----------
    s3_bucket : str
        S3 bucket name containing the data
    s3_prefix : str
        S3 prefix path (e.g., 'climate-profiles')

    Returns
    -------
    None
    """
    # Connect to S3
    s3 = boto3.client("s3")

    # Build the full prefix for standard-met-year data
    data_prefix = f"{s3_prefix}/standard-met-year/"

    print(f"Scanning s3://{s3_bucket}/{data_prefix}...")

    # Collect all CSV files and their metadata from S3
    entries = []

    # Use paginator to handle large number of files
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=data_prefix)

    for page in pages:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            filename = Path(key).name

            # Only process CSV files
            if not filename.endswith(".csv"):
                continue

            metadata = parse_stdyr_filename(filename)

            if metadata is None:
                print(f"WARNING: Could not parse filename: {filename}")
                continue

            # Build S3 path
            path = f"s3://{s3_bucket}/{key}"

            entries.append(
                {
                    "station_name": metadata["station_name"],
                    "variable": metadata["variable"],
                    "percentile": metadata["percentile"],
                    "time_period": metadata["time_period"],
                    "path": path,
                }
            )

    if not entries:
        print("ERROR: No CSV files found in S3!")
        return

    print(f"Found {len(entries)} CSV files")

    # Create DataFrame and write CSV catalog
    df = pd.DataFrame(entries)
    df = df.sort_values(["station_name", "variable", "percentile", "time_period"])

    df.to_csv(OUTPUT_CSV_FILEPATH, index=False)

    print(f"✓ Created CSV catalog: {OUTPUT_CSV_FILEPATH}")

    # Write YAML definition

    catalog_def = {
        "sources": {
            "standard_met_year": {
                "description": "Standard Meteorological Year climate data with various variables and percentiles",
                "driver": "csv",
                "args": {
                    "urlpath": OUTPUT_CSV_FILEPATH,
                    "csv_kwargs": {
                        "dtype": {
                            "station_name": "str",
                            "variable": "str",
                            "percentile": "str",
                            "time_period": "str",
                            "path": "str",
                        }
                    },
                },
                "metadata": {
                    "fields": {
                        "station_name": "Weather station name",
                        "variable": "Climate variable (t2, rh_derived, wind_speed_derived, swdnb, noaa_heat_index_derived)",
                        "percentile": "Percentile value (05ptile, 50ptile, 95ptile)",
                        "time_period": "Time period (present-day, near-future, mid-century, mid-late-century)",
                        "path": "File path (local or S3)",
                    }
                },
            }
        }
    }

    with open(OUTPUT_YAML_FILEPATH, "w") as f:
        yaml.dump(catalog_def, f, default_flow_style=False, sort_keys=False)

    print(f"✓ Created YAML definition: {OUTPUT_YAML_FILEPATH}")

    # Print summary statistics
    print("\n=== Catalog Summary ===")
    print(f"Total files: {len(entries)}")
    print(f"Stations: {df['station_name'].nunique()}")
    print(f"Variables: {df['variable'].nunique()}")
    print(f"  {', '.join(sorted(df['variable'].unique()))}")
    print(f"Percentiles: {df['percentile'].nunique()}")
    print(f"  {', '.join(sorted(df['percentile'].unique()))}")
    print(f"Time periods: {df['time_period'].nunique()}")
    print(f"  {', '.join(sorted(df['time_period'].unique()))}")


if __name__ == "__main__":
    generate_catalog(BUCKET_NAME, PREFIX)
