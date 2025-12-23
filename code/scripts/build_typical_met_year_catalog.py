"""
build_typical_met_year_catalog.py 

Generate intake catalog for typical-met-year dataset from S3.
Creates CSV file with metadata for each file and YAML definition for intake catalog

"""

import re
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
import yaml
import boto3
import s3fs

# S3 configuration
BUCKET_NAME = "cadcat"
PREFIX = "climate-profiles"

# Output filepaths
CATALOG_FILENAME = "cae-typical-met-year-collection"  # Filename no extension
AWS_PATH = "s3://cadcat/climate-profiles/"
OUTPUT_CSV_FILEPATH = f"{AWS_PATH}{CATALOG_FILENAME}.csv"
OUTPUT_YAML_FILEPATH = f"{AWS_PATH}{CATALOG_FILENAME}.yaml"


def parse_tmy_filename(filename: str) -> Optional[Dict[str, str]]:
    """
    Parse typical-met-year filename.

    Parameters
    ----------
    filename : str
        Filename to parse following pattern:
        tmy_[station_id]_[ACTIVITY_ID]_[SOURCE_ID]_[MEMBER_ID]_[TIME_PERIOD].(csv|epw)

    Returns
    -------
    dict or None
        Dictionary containing parsed metadata with keys:
        - station_id : str
        - activity_id : str
        - source_id : str
        - member_id : str
        - time_period : str
        - filename : str
        Returns None if filename cannot be parsed.
    """
    # Pattern: tmy_[STATION_ID]_[ACTIVITY_ID]_[SOURCE_ID]_[MEMBER_ID]_[TIME_PERIOD].(csv|epw)
    pattern = r"tmy_(.+)_(wrf)_([^_]+)_([^_]+)_([^_]+)\.(csv|epw)$"
    match = re.match(pattern, filename)

    if not match:
        return None

    station_id, activity_id, source_id, member_id, time_period, _ = match.groups()

    return {
        "station_id": station_id,
        "activity_id": activity_id,
        "source_id": source_id,
        "member_id": member_id,
        "time_period": time_period,
        "filename": filename,
    }


def generate_catalog(s3_bucket: str, s3_prefix: str) -> None:
    """
    Generate intake catalog for typical-met-year dataset from S3.

    Creates two files:
    - catalog_typical_met_year.csv: CSV catalog with file metadata
    - catalog_typical_met_year.yaml: YAML intake catalog definition

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

    # Build the full prefix for typical-met-year data
    data_prefix = f"{s3_prefix}/typical-met-year/"

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

            metadata = parse_tmy_filename(filename)

            if metadata is None:
                print(f"WARNING: Could not parse filename: {filename}")
                continue

            # Build S3 path
            path = f"s3://{s3_bucket}/{key}"

            entries.append(
                {
                    "station_id": metadata["station_id"],
                    "activity_id": metadata["activity_id"],
                    "source_id": metadata["source_id"],
                    "member_id": metadata["member_id"],
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
    df = df.sort_values(
        ["station_id", "activity_id", "source_id", "member_id", "time_period"]
    )

    df.to_csv(OUTPUT_CSV_FILEPATH, index=False)

    print(f"✓ Created CSV catalog: {OUTPUT_CSV_FILEPATH}")

    # Write YAML definition

    # Build templated URL path for parametrized access
    templated_urlpath = (
        "s3://" + BUCKET_NAME + "/" + PREFIX + "/typical-met-year/"
        "{{station_id}}/{{source_id}}/{{time_period}}/"
        "tmy_{{station_id}}_{{activity_id}}_{{source_id}}_{{member_id}}_{{time_period}}.csv"
    )

    catalog_def = {
        "sources": {
            "metadata": {
                "description": "Metadata catalog listing all available TMY datasets",
                "driver": "csv",
                "args": {
                    "urlpath": OUTPUT_CSV_FILEPATH,
                    "dtype": {
                        "station_id": "str",
                        "activity_id": "str",
                        "source_id": "str",
                        "member_id": "str",
                        "time_period": "str",
                        "path": "str",
                    },
                },
                "metadata": {
                    "fields": {
                        "station_id": "Weather station name",
                        "activity_id": "Downscaling method",
                        "source_id": "Climate model source (e.g., mpi-esm1-2-hr, miroc6)",
                        "member_id": "Ensemble member identifier (e.g., r1i1p1f1, r3i1p1f1)",
                        "time_period": "30-year time period (present-day, near-future, mid-century, mid-late-century)",
                        "path": "File path in S3",
                    }
                },
            },
            "typical_met_year": {
                "description": "Typical Meteorological Year (TMY) climate data with parametrized access",
                "driver": "csv",
                "parameters": {
                    "station_id": {
                        "description": "Weather station identifier (e.g., 'arcata_eureka_airport_kacv')",
                        "type": "str",
                    },
                    "source_id": {
                        "description": "Climate model source (e.g., 'ec-earth3', 'miroc6', 'mpi-esm1-2-hr', 'taiesm1')",
                        "type": "str",
                    },
                    "time_period": {
                        "description": "30-year time period ('present-day', 'near-future', 'mid-century', 'mid-late-century')",
                        "type": "str",
                    },
                    "member_id": {
                        "description": "Ensemble member identifier (e.g., 'r1i1p1f1', 'r3i1p1f1')",
                        "type": "str",
                    },
                    "activity_id": {
                        "description": "Downscaling methodology",
                        "type": "str",
                    },
                },
                "args": {
                    "urlpath": templated_urlpath,
                },
            },
        }
    }

    fs = s3fs.S3FileSystem()
    with fs.open(OUTPUT_YAML_FILEPATH, "w") as f:
        yaml.dump(catalog_def, f, default_flow_style=False, sort_keys=False)

    print(f"✓ Created YAML definition: {OUTPUT_YAML_FILEPATH}")

    # Print summary statistics
    print("\n=== Catalog Summary ===")
    print(f"Total files: {len(entries)}")
    print(f"Stations: {df['station_id'].nunique()}")
    print(f"Source models: {df['source_id'].nunique()}")
    print(f"  {', '.join(sorted(df['source_id'].unique()))}")
    print(f"Time periods: {df['time_period'].nunique()}")
    print(f"  {', '.join(sorted(df['time_period'].unique()))}")


if __name__ == "__main__":
    generate_catalog(BUCKET_NAME, PREFIX)
