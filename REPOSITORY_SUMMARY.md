# C3S-CDS Repository Summary

## Overview
This repository is designed to **download, preprocess, standardize, and catalog climate data from the Copernicus Climate Data Store (CDS)**. It provides a complete workflow for managing climate reanalysis datasets (like ERA5 and CERRA) including downloading raw data, deriving new variables, interpolating to reference grids, standardizing formats, and maintaining catalogues of available data.

## Purpose
The repository automates the process of:
1. **Downloading** climate data from CDS using the CDS API
2. **Deriving** new variables from raw data (e.g., calculating wind speed from u and v components)
3. **Interpolating** datasets to reference grids for spatial consistency
4. **Standardizing** variables to comply with CF conventions and CMIP standards
5. **Cataloguing** available datasets and generating visual reports

## Data Flow Workflow

### 1️⃣ **Configuration Phase**
- Edit CSV files in `requests/` to specify:
  - Which variables to download
  - Year ranges
  - CDS API parameters
  - Output paths
  - Interpolation reference grid (if needed)

### 2️⃣ **Download Phase (Raw Data)**
```
requests/*.csv → scripts/*.py → CDS API → NetCDF files
```
- Scripts read CSVs
- Create CDS API requests
- Download raw data as NetCDF files
- Files saved as: `{var}_{dataset}_{year}.nc`

### 3️⃣ **Derivation Phase**
```
Raw NetCDF → derived/*.py → Derived NetCDF
```
- Scripts identify "derived" variables from CSVs
- Load necessary raw data files
- Perform calculations (e.g., wind speed from components)
- Resample to daily values if needed
- Save derived variables

### 4️⃣ **Interpolation Phase**
```
Raw/Derived NetCDF → interpolation/*.py → Interpolated NetCDF
```
- Scripts identify "interpolated" variables from CSVs
- Load reference grid specified in the `interpolation` column
- Apply conservative interpolation to regrid data
- Save interpolated variables to specified output path

### 5️⃣ **Standardization Phase**
```
Derived/Raw NetCDF → standardization/*.py → Standardized NetCDF
```
- Apply unit conversions
- Update metadata attributes
- Ensure CF convention compliance

### 6️⃣ **Cataloguing Phase**
```
All NetCDF files → catalogues/produce_catalog.py → CSV + PDF reports
```
- Scan all output directories
- Check file existence for each year
- Generate availability reports
- Create visual heatmaps
- Publish via GitHub Actions nightly

## Repository Structure

---

### 📋 **requests/**

Contains CSV files that define **what data to download**
- Each CSV corresponds to a CDS catalogue (e.g., `reanalysis-era5-single-levels.csv`)
- Columns include:
  - `filename_variable`: Variable name for saved files
  - `cds_request_variable`: Variable name in CDS API
  - `cds_years_start/end`: Year range to download
  - `product_type`: `raw`, `derived`, or `interpolated`
  - `interpolation`: Reference grid filename (e.g., `land_sea_mask_0.0625degree.nc4`)
  - `output_path`: Where to save the data
  - `script`: Which Python script handles this dataset

**Example:** A row specifying to download u10 (10m wind u-component) for years 2022-2024 from ERA5.

---

### ⬇️ **scripts/**

Python scripts that **download data from CDS**
- One script per CDS catalogue (e.g., `reanalysis-era5-single-levels.py`)
- Uses `utils.py` which provides:
  - `download_single_file()`: Downloads individual files via CDS API
  - `download_files()`: Orchestrates parallel downloads based on CSV configuration
- Reads request CSVs and creates API requests
- Saves files with format: `{variable}_{dataset}_{year}.nc`

**Workflow:**
1. Read CSV from `requests/` directory
2. For each variable marked as "raw", create CDS API requests
3. Download files to specified output path
4. Skip files that already exist

---

### 🔬 **derived/**

Python scripts that **calculate derived variables** from raw data
- Example: `reanalysis-era5-single-levels.py` calculates:
  - `sfcwind` (wind speed) from `u10` and `v10` components using: `sfcwind = √(u10² + v10²)`
- Uses `operations.py` which provides utility functions:
  - `sfcwind_from_u_v()`: Calculate wind speed from components
  - `resample_to_daily()`: Aggregate hourly data to daily statistics
  - `load_path_from_df()`: Load file paths from configuration

**Workflow:**
1. Read CSV to identify variables marked as "derived"
2. Load required raw data files
3. Apply mathematical operations
4. Resample to daily values if needed
5. Save derived variables

---

### 🌐 **interpolation/**

Python scripts that **interpolate datasets to reference grids**
- Example: `reanalysis-cerra-single-levels.py` interpolates CERRA data
- Reference grid is specified in the `interpolation` column of request CSVs
- Uses conservative interpolation method via xESMF
- CERRA is the reference example for future dataset updates
- Reference grids will be moved to a `resources/` folder in future updates

**Workflow:**
1. Read CSV to identify variables marked as "interpolated"
2. Load reference grid from specified file (e.g., `land_sea_mask_0.0625degree.nc4`)
3. Apply conservative_normed interpolation to regrid data
4. Save interpolated variables to output path
5. Skip files that already exist

---

### 📏 **standardization/**

Python scripts that **standardize variables** to CF conventions
- Example: `derived-era5-single-levels-daily-statistics.py` contains functions like:
  - `tp()`: Convert precipitation from m/day to kg/m²/s (flux)
  - `e()`: Convert evaporation with proper units and attributes
  - `ssrd()`: Convert solar radiation from J/m² to W/m²
- Updates variable attributes (units, standard_name, long_name, etc.)

**Purpose:** Ensure data complies with Climate and Forecast (CF) metadata conventions and CMIP6 standards for interoperability.

---

### 📖 **provenance/**

JSON files documenting **metadata and provenance** for each variable
- Includes:
  - Variable names and mappings
  - Frequency (hourly, daily, monthly)
  - Product type (raw or derived)
  - Links to CMIP6 CMOR tables for standard definitions

**Example:**
```json
{
  "uas": {
    "var_name": "u10",
    "provenance": "https://github.com/PCMDI/cmip6-cmor-tables/...",
    "frequency": "hourly",
    "type_product": "raw"
  }
}
```

---

### 📊 **catalogues/**

Scripts that **generate visual catalogues** of available data
- `produce_catalog.py` / `produce_catalog_v2.py`:
  - Scans output directories for downloaded files
  - Checks which years exist for each variable
  - Creates CSV catalogues showing data availability
  - Generates heatmap visualizations (PDF images)
- `generate_resumen.py`: Creates summary reports
- Updated nightly via GitHub Actions CI/CD

**Output:**
- CSV files: Lists all variables, datasets, date ranges, file paths
- PDF heatmaps: Visual representation of data availability (green=downloaded, orange=partial, red=missing)

---

### 📓 **notebooks/**

Jupyter notebooks for **exploration and testing**

---

### 🔧 **ci/**

Continuous Integration examples and helper scripts

---

## Technical Details

### File Naming Convention
**Format:** `{variable}_{dataset}_{date}.nc`

**Date formats:**
- `{year}{month}`: For large datasets like CERRA (monthly files for faster downloads)
- `{year}`: For smaller datasets saved annually

**Examples:**
- `u10_reanalysis-era5-single-levels_2023.nc`
- `sfcwind_reanalysis-cerra-single-levels_202301.nc`

### Automation
- **GitHub Actions workflows:**
  - `catalog_executor.yml`: Runs nightly to update catalogues
  - `run_all_requests_scripts.yml`: Can trigger download scripts
- **SLURM scripts:**
  - `launch_all_requests_scripts.sh`: Batch job launcher for HPC environments
  - Designed for cluster computing with job scheduling

## Supported Datasets

- reanalysis-era5-single-levels
- reanalysis-cerra-single-levels
- reanalysis-cerra-land
- derived-era5-single-levels-daily-statistics

## Usage Example

### To download ERA5 data:
1. Edit `requests/reanalysis-era5-single-levels.csv` to specify years and variables
2. Run: `python scripts/reanalysis-era5-single-levels.py`
3. Raw data downloads to specified `output_path`

### To calculate derived variables:
1. Ensure raw data is downloaded
2. Run: `python derived/reanalysis-era5-single-levels.py`
3. Derived variables saved to derived directory

### To interpolate data:
1. Ensure raw data is downloaded
2. Specify reference grid in the `interpolation` column of request CSV
3. Run: `python interpolation/reanalysis-cerra-single-levels.py`
4. Interpolated data saved to specified output path

### To update catalogues:
1. Run: `python catalogues/produce_catalog.py`
2. Generates CSV catalogues and PDF visualizations
3. Shows data availability status

## Integration
This repository is part of the **C3S Atlas** ecosystem and uses the same conda environment. It serves as the data acquisition and preprocessing layer, providing standardized climate data for downstream analysis tools.

## Summary
The **c3s-cds repository** is a comprehensive data management system for climate reanalysis datasets. It automates the entire pipeline from CDS API downloads through interpolation and standardization to catalog generation, making climate data readily accessible and well-documented for scientific research.
