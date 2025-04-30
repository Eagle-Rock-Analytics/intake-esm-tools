# Code

Builder scripts for unique data catalogs are stored in `scripts/`. There's also a Jupyter notebook guide (`notebooks/build_catalog_guide.ipynb`) that walks through and explains the steps for building a catalog, using the renewables data catalog as an example. 

## 🔗 Useful Resources  
- 📄 **Building a Custom Parser with `ecgtools`**:  
  [ecgtools documentation](https://ecgtools.readthedocs.io/en/latest/how-to/use-a-custom-parser.html)  
- 📄 **Creating a Catalog with `intake-esm`**:  
  [Intake-ESM documentation](https://intake-esm.readthedocs.io/en/stable/how-to/build-a-catalog-from-timeseries-files.html) 

## 🛠️ Guide to the `ecgtools` Builder Class 

The primary function of the `Builder` class is to create catalogs from lists of netCDF files or Zarr stores. It "crawls" through your data directories for you, looking for file paths using the information you have specified. You can refer to the official documentation [here](https://ecgtools.readthedocs.io/en/latest/reference/index.html#builder), but a brief overview and additional notes are provided below. 📚

### 📝 Nicole's notes on the Builder class 

Let’s say you have an **S3 bucket** named `salsa_cities` 💃, which contains a CSV file and two directories: `new_york` and `miami`. Each of these directories contains **Zarr stores** with climate data specific to their locations. It's important to remember that Zarr stores are **not files**; they are storage structures represented as **directories** in the S3 file system. Each Zarr store consists of several nested directories and metadata files. In this case, `precip/`, `x/`, `y/`, and `time/` are directories within the Zarr store.

```plaintext
salsa_cities/
├── new_york/
│   ├── precip/
│   ├── temp/
│   ├── x/
│   ├── y/
│   ├── time/
│   ├── .zattrs
│   ├── .zgroup
│   └── .zmetadata
├── miami/
│   ├── precip/
│   ├── temp/
│   ├── x/
│   ├── y/
│   ├── time/
│   ├── .zattrs
│   ├── .zgroup
│   └── .zmetadata
└── best_cities_to_salsa_dance.csv
```

Each Zarr store includes the following structure: 

- **Data variable directories**: `precip/`, `temp/`
- **Spatial and temporal dimension directories**: `x/`, `y/`, `time/`
- **Metadata files**: `.zattrs`, `.zgroup`, `.zmetadata`

#### 💻 Using the Builder to build a catalog 

Now, let's use the `Builder` class to build a catalog for the Zarr stores in our bucket:

```python
builder = Builder(
    paths=["s3://salsa_cities/"], # A list of paths to the data; this should be the root directory of your data files.
    depth=1, # Maximum depth to crawl for assets.
    exclude_patterns=['**/best_cities_to_salsa_dance.csv'], # List of glob patterns to exclude from crawling.
    include_patterns=['**/.zmetadata'] # List of glob patterns to include when crawling 
)
```

After that, we will "build" the catalog using this Builder object and a custom parser function, and then generate a CSV file and JSON file. More information on these steps are in the code. 

### 🔍 Additional notes on the inputs to Builder 
- **depth**: How far within the paths above should the Builder search for your files? Say we just want to look for the files in `paths=["s3://salsa_cities/new_york/"]`. In this case, our depth would be 0, because the root directory contains our Zarr store in it. 
- **exclude_patterns**: We're not interested in the CSV file, so we ignore it when crawling. You can also add directories here using this formatting: `**/another_directory/**`
- **include_patterns**: Since Zarrs are stores and not individual files, we need to be a bit clever here.
  - We just look at the `.zmetadata` file because it's a terminal file within the root directory of the Zarr store; we don't want the crawler to look into the dimensions or data directories.
  - The Builder class isn't really optimized to work with Zarrs, so we need to avoid it crawling into the other directories within the Zarr store. These other directories will contain the metadata files `.zattrs` and `.zgroup`, but not `.zmetadata`; only the root directory has that file. By setting `include_patterns=['/**.zmetadata']`, we are solving two issues: we are ignoring the nested directories within the root directory of the Zarr (`precip`, `x`, `y`, and `time`), and then returning only a single path for each Zarr (the paths for `.zgroup` and `.zattrs` within the root directory are also ignored). 🎯
