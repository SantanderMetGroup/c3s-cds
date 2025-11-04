# c3s-cds
This repository contains scripts to download, preprocess, standardize, and consolidate the catalogues available in the CDS.
## Environment:
The environment is the one used in the c3s-atlas user tools: https://github.com/ecmwf-projects/c3s-atlas/blob/main/environment.yml

## Directory Structure

The repository uses a structured directory path format to organize downloaded, derived, and interpolated data:

```
{base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
```

**Path Components:**
- `product_type`: Type of data processing
  - `raw`: Data downloaded directly from CDS
  - `derived`: Variables calculated from raw data (e.g., wind speed from u/v components) or interpolated to different grids
- `dataset`: CDS dataset name (e.g., `reanalysis-era5-single-levels`, `reanalysis-cerra-single-levels`)
- `temporal_resolution`: Time frequency of the data
  - `hourly`: Hourly data (e.g., ERA5 with all 24 hours)
  - `daily`: Daily data or daily statistics
  - `3hourly`: 3-hourly data (e.g., CERRA)
  - `6hourly`: 6-hourly data
  - `monthly`: Monthly data
- `interpolation`: Grid specification
  - `native`: Original grid (no interpolation)
  - `gr006`: 0.0625 degree grid (6.25 km)
  - Other grid specifications as needed
- `variable`: Variable name (e.g., `u10`, `v10`, `sfcwind`, `t2m`)

**Examples:**

1. **Raw ERA5 hourly wind components:**
   ```
   /lustre/.../raw/reanalysis-era5-single-levels/hourly/native/u10/
   /lustre/.../raw/reanalysis-era5-single-levels/hourly/native/v10/
   ```

2. **Derived daily wind speed from ERA5 (calculated variable):**
   ```
   /lustre/.../derived/reanalysis-era5-single-levels/daily/native/sfcwind/
   ```

3. **Raw CERRA 3-hourly data:**
   ```
   /lustre/.../raw/reanalysis-cerra-single-levels/3hourly/native/t2m/
   ```

4. **Derived CERRA data interpolated to 0.0625 degree grid:**
   ```
   /lustre/.../derived/reanalysis-cerra-single-levels/3hourly/gr006/t2m/
   ```

5. **ERA5 daily statistics:**
   ```
   /lustre/.../raw/derived-era5-single-levels-daily-statistics/daily/native/t2m/
   ```

**Note:** Interpolated data is stored under `derived` with the `interpolation` field indicating the target grid (e.g., `gr006`). This distinguishes it from calculated variables which use `interpolation=native`.

## Filename format:
Format of the files is "{var}\_{dataset}\_{date}.nc"
With date: 
- "{year}{month}" for big datasets like CERRA saved month by month (download is faster this way). 
- "{year}" for the other datasets that are saved year by year. 

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

This is useful for:
- Setting up the directory structure before starting downloads
- Verifying the paths that will be used
- Preparing storage locations in advance

## Contents

| Directory | Contents |
| :-------- | :------- |
|  [requests](https://github.com/SantanderMetGroup/c3s-cds/tree/main/requests) | Contains one CSV file per CDS catalogue, listing the requested variables, temporal resolution, interpolation method, the target save directory, and whether the variable is raw or requires post-processing to be standardized.
|  [scripts](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts) | 	Python scripts to download data from the CDS.
| [provenance](https://github.com/SantanderMetGroup/c3s-cds/tree/main/provenance) | Contains one JSON file per catalogue describing the provenance and definitions of each variable.
| [standardization](https://github.com/SantanderMetGroup/c3s-cds/tree/main/standardization) |  Python recipes to standardize the variables.
| [derived](https://github.com/SantanderMetGroup/c3s-cds/tree/main/derived) |  Python recipes to calculate derived products from the variables.
| [catalogues](https://github.com/SantanderMetGroup/c3s-cds/tree/main/catalogues) |  	CSV catalogues of datasets consolidated in Lustre or GPFS. The catalogues are updated through a nightly CI job.


