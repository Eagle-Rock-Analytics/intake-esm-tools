"""build_hdp_catalog.py

Builds an intake-ESM catalog for the Historical Data Platform (HDP) dataset.

This script performs the following steps:
1. Initializes an `ecgtools.Builder` object to crawl S3-based Zarr data.
2. Applies a custom parsing function to extract metadata from file paths.
3. Cleans and builds a catalog of valid assets.
4. Saves the catalog files (CSV and JSON) back to the same S3 directory.

Catalog is saved with name defined in CAT_NAME, and written to ROOT_DIR.
"""

import traceback
import time
import inspect
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK

CAT_NAME = "era-hdp-collection"  # Name to give catalog csv and json files (don't include file extension)
ROOT_DIR = "s3://wecc-historical-wx/4_merge_wx/"  # Root directory to data on S3 (and, where the catalog files will go)


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
        network, station_id, _ = filepath.split(ROOT_DIR)[1].split("/")
        # Remove .zmetadata from the filepath, since the actual path to the zarr doesn't include this
        filepath = filepath.split(".zmetadata")[0]
    except Exception as e:
        # If an error occurs (e.g., wrong filepath structure), return error details
        return {INVALID_ASSET: filepath, TRACEBACK: traceback.format_exc()}

    # Add filepath info to dictionary
    station_id = station_id.split(".zarr")[0]  # Remove .zarr extension from station_id
    info = {"network_id": network, "station_id": station_id, "path": filepath}

    return info


def init_builder():
    """
    Initializes the ecgtools Builder object with crawl settings for HDP.

    Returns
    -------
    ecgtools.builder.Builder
        Configured Builder instance ready for building the catalog.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    exclude_patterns = [
        "**/VALLEYWATER/**",
        "**/eraqc_counts/**",
        "**/qaqc_logs/**",
    ]  # Glob patterns to exclude (don't crawl through these directories or include these files)
    builder = Builder(
        paths=[ROOT_DIR],
        depth=2,  # Crawl through 2 directories
        exclude_patterns=exclude_patterns,
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


def save_builder(builder_obj):
    """
    Saves the catalog to the ROOT_DIR with intake-ESM configuration.

    Parameters
    ----------
    builder_obj : ecgtools.builder.Builder
        The Builder object containing the parsed catalog data.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")
    builder_obj.save(
        name=CAT_NAME,
        directory=ROOT_DIR,
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
    """
    Main execution routine to build and upload the HDP intake-ESM catalog.
    """

    start_time = time.time()
    print("Started running build_hdp_catalog.py")

    # Initialize the builder
    builder = init_builder()

    # Actually build the catalog
    builder = build_catalog(builder)

    # Upload catalog to s3
    save_builder(builder)

    # Print elapsed time
    elapsed = int(time.time() - start_time)
    print(f"Elapsed time: {elapsed//3600:02}:{(elapsed%3600)//60:02}:{elapsed%60:02}")

    print("Script complete.")


if __name__ == "__main__":
    main()
