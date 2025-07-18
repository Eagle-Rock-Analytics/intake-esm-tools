"""build_hdp_catalog.py

Builds an intake-ESM catalog for the Historical Data Platform (HDP) dataset.

This script performs the following steps:
1. Initializes an `ecgtools.Builder` object to crawl S3-based Zarr data.
2. Applies a custom parsing function to extract metadata from file paths.
3. Cleans and builds a catalog of valid assets.
4. Saves the catalog files (CSV and JSON) back to the same S3 directory.

Attributes
----------
S3_URI : str
    s3 directory where the output catalog files will be stored.
HTTP_URL: str
    Public HTTPS base URL for catalog files
CAT_NAME : str
    Name of the catalog files (without file extension).
    
"""

import traceback
import time
import inspect
import s3fs
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK
from utils import update_catalog_file_key


S3_URI = "s3://cadcat/histwxstns"  # Directory to store output files in
CAT_NAME = "era-hdp-collection"  # Name to give catalog csv and json files (don't include file extension)

# Public HTTPS base URL for catalog files
# NEED TO REPLACE AND RERUN ONCE WE HAVE PUBLIC-FACING HTTP URL
HTTP_URL = "https://cadcat.s3.amazonaws.com/histwxstns"


def parse_hdp_data(filepath):
    """
    Parses the S3 filepath to extract metadata for HDP data

    Extracts information like installation, simulation model, experiment,
    frequency, variable, grid resolution, and the file path (without `.zmetadata`).

    Parameters
    ----------
    filepath : str
        The S3 URL of the file.

    Notes
    -----
    The `try/except` block handles errors in extracting information from the `filepath`.
    If the filepath structure does not match the expected format or if any error occurs
    while splitting the string, the `except` block will capture the exception and return
    a dictionary with the error message and traceback.
    """
    try:
        # Get the data info from the filepath
        network, station_id, _ = filepath.split(S3_URI + "/")[1].split("/")
        # Remove .zmetadata from the filepath, since the actual path to the zarr doesn't include this
        filepath = filepath.split(".zmetadata")[0]
    except Exception as e:
        # If an error occurs (e.g., wrong filepath structure), return error details
        return {INVALID_ASSET: filepath, TRACEBACK: traceback.format_exc()}

    # Add filepath info to dictionary
    station_id = station_id.split(".zarr")[0]  # Remove .zarr extension from station_id
    info = {"network_id": network, "station_id": station_id, "path": filepath}

    return info


def get_zarr_paths():
    """
    Retrieve paths to zarr stores for each HDP station using fs.glob

    Returns
    --------
    list of string
        Paths to zarr stores for each station

    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")
    print(
        "Warning: This function may take a while as it crawls through the s3 bucket looking for files"
    )

    fs = s3fs.S3FileSystem()
    glob_s3 = fs.glob(f"{S3_URI}/**/**/.zmetadata")
    zarr_paths = [
        "s3://" + file.split(".zmetadata")[0] for file in glob_s3
    ]  # Remove .zmetadata from the path
    zarr_paths = [
        path for path in zarr_paths if "VALLEYWATER" not in path
    ]  # Remove VALLEYWATER stations

    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return zarr_paths


def init_builder(zarr_paths):
    """
    Initializes the ecgtools Builder object with crawl settings for HDP.

    Parameters
    ----------
    zarr_paths: list of string
        Paths to zarr stores for each station

    Returns
    -------
    ecgtools.builder.Builder
        Configured Builder instance ready for building the catalog.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    builder = Builder(
        paths=zarr_paths,
        depth=0,  # No crawling
        include_patterns=["**/.zmetadata"],  # Glob patterns to include
    )
    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")
    return builder


def build_catalog(builder_obj):
    """
    Builds and cleans the intake-ESM catalog using a custom parser.

    Parameters
    ----------
    builder_obj : ecgtools.builder.Builder
        The initialized ecgtools Builder object.

    Returns
    -------
    ecgtools.builder.Builder
        Updated Builder with cleaned catalog data.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")
    builder_obj.build(parsing_func=parse_hdp_data)
    builder_obj.clean_dataframe()  # Exclude invalid assets and removing duplicate entries
    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")
    return builder_obj


def export_catalog_files(builder, cat_directory, cat_name):
    """Export catalog json and csv files

    Parameters
    ---------
    builder: ecgtools.builder.Builder
        Pre-built builder object
    cat_directory: str
        Directory to save the output catalog files
    cat_name: str
        Name to give the catalog (no file extension)

    Returns
    -------
    None

    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")
    print(
        f"Creating catalog files in directory '{S3_URI}' with name '{CAT_NAME}.csv' and '{CAT_NAME}.json'"
    )
    builder.save(
        name=cat_name,
        directory=cat_directory,
        # Column name including filepath
        path_column_name="path",
        # Column name including variables
        variable_column_name="station_id",
        # Data file format - could be netcdf or zarr (in this case, zarr)
        data_format="zarr",
        # Which attributes to groupby when reading in variables using intake-esm
        # These are used to construct the key name for accessing data using intake
        groupby_attrs=[
            "network_id",
            "station_id",
        ],
        # Aggregations which are fed into xarray when reading in data using intake
        aggregations=[
            {"type": "union", "attribute_name": "station_id"},
        ],
        description="Eagle Rock Analytics Historical Data Platform Catalog",
    )
    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")


def main():
    """Runs the catalog-building process and measures execution time."""

    print("Starting script build_hdp_catalog.py")

    start_time = time.time()

    zarr_paths = get_zarr_paths()

    hdp_builder = init_builder(zarr_paths)

    hdp_builder = build_catalog(hdp_builder)

    export_catalog_files(hdp_builder, S3_URI, CAT_NAME)

    update_catalog_file_key(S3_URI, HTTP_URL, CAT_NAME)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Script complete!\n Total execution time: {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    main()
