"""
build_catalog_renewables.py

Builds an Intake-ESM catalog for ERA renewables data stored in an S3 bucket.

The script extracts metadata from S3 file paths, filters datasets based on predefined 
criteria, and generates a structured catalog in CSV and JSON formats. This catalog 
allows for efficient querying and dataset management.

Attributes
----------
CAT_DIRECTORY : str
    Directory where the output catalog files will be stored.
CAT_NAME : str
    Name of the catalog files (without file extension).

Functions
---------
parse_ae_ren_data(filepath)
    Parses the S3 filepath to extract metadata for climate simulation data.
build_catalog()
    Creates and builds the catalog using `ecgtools.Builder`.
export_catalog_files(builder, cat_directory, cat_name)
    Saves the catalog as JSON and CSV files.
main()
    Executes the catalog-building process.

Outputs
-------
- `{CAT_NAME}.csv` and `{CAT_NAME}.json` stored in `{CAT_DIRECTORY}`.

Examples
--------
To build and export the catalog, run:

>>> python build_catalog_renewables.py

"""

import traceback
import time
from ecgtools import Builder
from ecgtools.builder import INVALID_ASSET, TRACEBACK

CAT_DIRECTORY = "../../catalogs/era-ren-collection" # Directory to store output files in 
CAT_NAME = "era-ren-collection" # Name to give catalog csv and json files (don't include file extension)

def parse_ae_ren_data(filepath):
    """
    Parses the S3 filepath to extract metadata for climate simulation data.
    
    Extracts information like installation, simulation model, experiment, 
    frequency, variable, grid resolution, and the file path (without `.zmetadata`).

    Parameters
    ----------
    filepath : str
        The S3 URL of the file.

    Returns
    -------
    dict
        A dictionary with parsed metadata:
        - installation, activity_id, institution_id, source_id, experiment_id, 
          table_id, variable_id, grid_label, path.
        If parsing fails, returns a dictionary with the error details:
        - INVALID_ASSET and TRACEBACK.

    Example
    -------
    >>> parse_ae_ren_data('s3://wfclimres/ERA/WRF/EC-Earth3/experiment/precipitation/variable/zarr/file.zmetadata')
    {
        "installation": "WRF",
        "activity_id": "WRF",
        "institution_id": "ERA",
        "source_id": "EC-Earth3",
        "experiment_id": "experiment",
        "table_id": "precipitation",
        "variable_id": "variable",
        "grid_label": "zarr",
        "path": "s3://wfclimres/ERA/WRF/EC-Earth3/experiment/precipitation/variable/zarr/file"
    }

    Notes
    -----
    The `try/except` block handles errors in extracting information from the `filepath`. 
    If the filepath structure does not match the expected format or if any error occurs 
    while splitting the string, the `except` block will capture the exception and return 
    a dictionary with the error message and traceback.
    """
    try:
        # Get the data info from the filepath
        institution_id, installation, source_id, experiment_id, table_id, variable_id, grid_label, _ = filepath.split("s3://wfclimres/")[1].split("/")
        # Remove .zmetadata from the filepath, since the actual path to the zarr doesn't include this 
        filepath = filepath.split(".zmetadata")[0]
    except Exception as e:
        # If an error occurs (e.g., wrong filepath structure), return error details
        return {INVALID_ASSET: filepath, TRACEBACK: traceback.format_exc()}
    
    # Simulation string mapping
    simulation_dict = {
        "ec-earth3": "EC-Earth3",
        "mpi-esm1-2-hr": "MPI-ESM1-2-HR",
        "miroc6": "MIROC6",
        "taiesm1": "TaiESM1",
        "era5": "ERA5"
    }

    # Add filepath info to dictionary
    info = {
        "installation": installation,
        "activity_id": "WRF", 
        "institution_id": "ERA",
        "source_id": simulation_dict[source_id],
        "experiment_id": experiment_id,
        "table_id": table_id,
        "variable_id": variable_id,
        "grid_label": grid_label,
        "path": filepath
    }
    
    return info


def build_catalog(): 
    """Create and build catalog using custom parsing function 

    Returns
    -------
    builder: ecgtools.builder.Builder
        
    """
    # Settings for the Builder 
    root_dir = 's3://wfclimres/era/' # Root directory 
    installations = ["pv_distributed", "pv_utility", "windpower_offshore", "windpower_onshore"]
    exclude_patterns = [
        "**/EC-Earth3/**", 
        "**/ERA5/**", 
        "**/MIROC6/**", 
        "**/MPI-ESM1-2-HR/**", 
        "**TaiESM1/**"
        ]
    
    # Instantiate the Builder 
    builder = Builder(
        paths=[f'{root_dir}{installation}/' for installation in installations], 
        depth=5, # Crawl through 5 directories 
        exclude_patterns=exclude_patterns, # Glob patterns to exclude
        include_patterns=["**/.zmetadata"] # Glob patterns to include 
    )

    # Build the catalog and use custom parsing function 
    builder.build(parsing_func=parse_ae_ren_data) 
    builder.clean_dataframe() # Exclude invalid assets and removing duplicate entries
    
    return builder

def export_catalog_files(builder, cat_directory, cat_name): 
    """Export catalog json and csv files to local drive 
    
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

    builder.save(
        name=cat_name,
        directory=cat_directory,
        # Column name including filepath
        path_column_name='path',
        # Column name including variables
        variable_column_name='variable_id',
        # Data file format - could be netcdf or zarr (in this case, zarr)
        data_format="zarr",
        # Which attributes to groupby when reading in variables using intake-esm
        groupby_attrs=["installation","activity_id","institution_id","source_id","experiment_id","table_id","grid_label"], 
        # Aggregations which are fed into xarray when reading in data using intake
        aggregations=[
            {'type': 'union', 'attribute_name': 'variable_id'},
        ],
        description="Eagle Rock Analytics Renewables Data Catalog"
    )


def main(): 
    """Runs the catalog-building process and measures execution time."""
    
    start_time = time.time()
    
    print("Building catalog...")
    ren_builder = build_catalog()
    print("Catalog building complete.")

    print(f"Creating catalog files in directory '{CAT_DIRECTORY}' with name '{CAT_NAME}.csv' and '{CAT_NAME}.json'")
    export_catalog_files(ren_builder, CAT_DIRECTORY, CAT_NAME)
    print("Catalog files successfully created!")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Script complete!\n Total execution time: {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    main()