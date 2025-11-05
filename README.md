# c3s-cds
This repository contains scripts to download, preprocess, standardize, and consolidate the catalogues available in the CDS.
## Environment:
The environment is the one used in the c3s-atlas user tools: https://github.com/ecmwf-projects/c3s-atlas/blob/main/environment.yml

## Directory Structure

The repository uses a structured directory path format to organize downloaded, derived, and interpolated data:

```
{base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
```

**Examples:**

1. **Raw ERA5 hourly wind components:**
   ```
   /lustre/.../raw/reanalysis-era5-single-levels/hourly/native/u10/
   ```

**Note:** Interpolated data is stored under `derived` with the `interpolation` field indicating the target grid (e.g., `gr006`). This distinguishes it from calculated variables which use `interpolation=native`.

## Filename format:
Format of the files is "{var}\_{dataset}\_{date}.nc"
With date: 
- "{year}{month}" for big datasets like CERRA saved month by month (download is faster this way). 
- "{year}" for the other datasets the data is saved year by year. 

## Creating Directory Structure

Before downloading data, you can create the complete folder structure without downloading or calculating any data:

```bash
# Preview what directories would be created (dry-run mode)
python scripts/create_folder_structure.py --dry-run

# Create all directories
python scripts/create_folder_structure.py
```

The script reads all CSV files in the `requests/` directory and creates the directory structure according to the format:
`{base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/`


## Contents

| Directory | Contents |
| :-------- | :------- |
|  [requests](https://github.com/SantanderMetGroup/c3s-cds/tree/main/requests) | Contains one CSV file per CDS catalogue, listing the requested variables, temporal resolution, interpolation method, the target save directory, and whether the variable is raw or requires post-processing to be standardized.
| [provenance](https://github.com/SantanderMetGroup/c3s-cds/tree/main/provenance) | Contains one JSON file per catalogue describing the provenance and definitions of each variable.
|  [scripts/download](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts/download) | 	Python scripts to download data from the CDS.
| [scripts/standardization](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts/standardization) |  Python recipes to standardize the variables.
| [scripts/derived](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts/derived) |  Python recipes to calculate derived products from the variables.
| [scripts/interpolation](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts/interpolation) |  Python recipes to interpolate data.
| [scripts/catalogue](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts/catalogue) |  Python recipes to produce the catalogues of downloaded data.
| [catalogues](https://github.com/SantanderMetGroup/c3s-cds/tree/main/catalogues) |  	CSV catalogues of datasets consolidated in Lustre or GPFS. The catalogues are updated through a nightly CI job.


