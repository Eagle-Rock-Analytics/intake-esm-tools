# intake-esm-tools

This repository contains code and documentation for building and maintaining [intake-esm](https://intake-esm.readthedocs.io/en/stable/) catalogs for Eagle Rock Analytics datasets stored in Amazon S3. These catalogs provide a scalable, schema-driven interface to search, load, and analyze large collections of NetCDF or Zarr files.

The catalogs built here enable consistent and shareable data access across projects and users, supporting internal research and application development with cloud-optimized datasets.


--- 

## 📥 Basic usage of intake catalogs 
Intake catalogs can be easily read in using the `intake` catalog. Here's some example usage using the renewables catalog: 
```python
import intake

# Read from local machine
cat = intake.open_esm_datastore("code/notebooks/era-ren-collection.json")

# Read from AWS using s3 URI for json file 
cat = intake.open_esm_datastore("https://wfclimres.s3.amazonaws.com/era/era-ren-collection.json")
```

After loading the catalog file, you can then easily subset the catalog and read in the zarrs as `xarray` Dataset objects using the following method: 

```python
# Access catalog as dataframe and inspect the first few rows
cat_df = cat.df

# Form query dictionary
query = {
    # GCM name
    'source_id': 'EC-Earth3',
    # time period - historical or emissions scenario
    'experiment_id': ['historical', 'ssp370'],
    # variable
    'variable_id': 'cf',
    # time resolution 
    'table_id': 'day',
    # grid resolution: d01 = 45km, d02 = 9km, d03 = 3km
    'grid_label': 'd03'
}

# Subset catalog 
cat_subset = cat.search(**query)

# Get dataset dictionary 
dsets = cat_subset.to_dataset_dict(
    xarray_open_kwargs={'consolidated': True},
    storage_options={'anon': True}
)

# Display one of the files :) 
dsets["pv_distributed.WRF.ERA.EC-Earth3.historical.day.d03"]
```

---

## 🗂️ Repository Structure

```
intake-esm-tools/
├── code/
│   ├── notebooks/         # Jupyter notebooks for exploring and testing catalog logic
│   └── scripts/           # Python scripts for generating catalog files per project
├── environment.yml        # Conda environment definition
└── README.md
```

---

## 🛠️ Getting Started

All required dependencies are listed in the `environment.yml` file. To build and activate the conda environment:

```bash
conda env create -f environment.yml
conda activate intake-esm-tools
```

You can then run any script in `code/scripts/` to generate or update catalogs for a particular project.

---

## 📚 Learn More

For more details on how `intake-esm` works—including how catalogs are structured, queried, and used in analysis—see the official docs:  
👉 [https://intake-esm.readthedocs.io/en/stable/](https://intake-esm.readthedocs.io/en/stable/)

---

## 🔧 Example Usage

Check out the [example notebook](https://github.com/Eagle-Rock-Analytics/intake-esm-tools/blob/main/code/notebooks/build_catalog_guide.ipynb) demonstrating catalog usage with `intake-esm` and `xarray` for the renewables project.
