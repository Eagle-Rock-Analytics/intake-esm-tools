"""utils.py 

Helper functions common to multiple catalog generation scripts 

"""

import json
import fsspec


def update_catalog_file_key(s3_uri: str, https_url: str, cat_name: str) -> None:
    """
    Update the "catalog_file" key in a JSON catalog stored at an S3 URI.

    This function modifies an Intake-ESM JSON catalog by injecting (or updating) the "catalog_file"
    field, which points to a public HTTPS URL for the associated CSV file. This is necessary
    because Intake-ESM cannot read from S3 URIs (e.g., 's3://bucket/path/file.csv') directly when
    using the `catalog_file` key. Intake expects a web-accessible HTTPS URL or a local path for public data.

    Parameters
    ----------
    s3_uri : str
        Base S3 URI where the catalog JSON is stored (e.g., 's3://mybucket/catalogs').
    https_url : str
        Public HTTPS URL where the CSV catalog will be accessible.
    cat_name : str
        Catalog name (used for both JSON and CSV filenames).

    Returns
    -------
    None
        Modifies the JSON file in-place by injecting or updating the "catalog_file" key.
    """
    json_path = f"{s3_uri}/{cat_name}.json"

    with fsspec.open(json_path, "r") as f:
        catalog = json.load(f)

    catalog["catalog_file"] = f"{https_url}/{cat_name}.csv"

    with fsspec.open(json_path, "w") as f:
        json.dump(catalog, f, indent=2)
