# intake-esm-tools

This repository contains code and documentation for building and maintaining [intake-esm](https://intake-esm.readthedocs.io/en/stable/) catalogs for Eagle Rock Analytics' datasets stored in Amazon S3. These catalogs provide a scalable, schema-driven interface to search, load, and analyze large collections of NetCDF or Zarr files.

The catalogs built here enable consistent and shareable data access across projects and users, supporting internal research and application development with cloud-optimized datasets.


--- 

## 📥 Basic usage of intake catalogs 
Intake catalogs can be easily read in using the `intake` catalog: 
```python
import intake

# Read from local machine
intake.open_esm_datastore("catalogs/era-ren-collection/era-ren-collection.json")

# Read from AWS using s3 URI for json file 
intake.open_esm_datastore("s3_uri")
```

---

## 🗂️ Repository Structure

```
intake-esm-tools/
├── code/
│   ├── notebooks/         # Jupyter notebooks for exploring and testing catalog logic
│   └── scripts/           # Python scripts for generating catalog files per project
├── catalogs/              # Output catalogs (.json, .csv) organized by project name
├── environment.yml        # Conda environment definition
└── README.md
```

- **`code/scripts/`**: Scripts for building intake-esm catalogs for different data collections. These typically define the asset structure, variable mapping, and call `intake_esm.build_catalog()` logic.
- **`catalogs/`**: Stores finalized catalog `.json` and `.csv` files for each project. Each subdirectory corresponds to a specific dataset collection or domain.

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
