# C3S-CDS Repository Summary

## Overview
This repository is designed to **download, preprocess, standardize, and catalog climate data from the Copernicus Climate Data Store (CDS)**. It provides a complete set of tools for managing climate reanalysis datasets (like ERA5 and CERRA) including downloading raw data, deriving new variables, interpolating to reference grids, standardizing formats, and maintaining catalogues of available data.

## Purpose
The repository automates the process of:
1. **Downloading** climate data from CDS using the CDS API
2. **Deriving** new variables from raw data (e.g., calculating wind speed from u and v components)
3. **Interpolating** datasets to reference grids for spatial consistency
4. **Standardizing** Unit transformations
5. **Cataloguing** available datasets and generating visual reports

## Data Flow Tools

### 1️⃣ **Configuration Phase**
- Edit CSV files in `requests/` to specify:
  - Which variables to download
  - Year ranges
  - CDS API parameters
  - Output paths and temporal resolution
  - Interpolation method (native, gr006, etc.)

### 2️⃣ **Download Phase (Raw Data)**
```
requests/*.csv → scripts/download/*.py → CDS API → NetCDF files
```
- Scripts read CSVs
- Create CDS API requests
- Download raw data as NetCDF files
- Files saved to: `{base}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/`

### 3️⃣ **Derivation Phase**
```
Raw NetCDF → scripts/derived/*.py → Derived NetCDF
```
- Scripts identify "derived" variables from CSVs
- Load necessary raw data files
- Perform calculations (e.g., wind speed from components)
- Resample to daily values if needed
- Save derived variables with temporal resolution metadata

### 4️⃣ **Interpolation Phase**
```
Raw NetCDF → scripts/interpolation/*.py → Interpolated NetCDF (stored as derived)
```
- Scripts identify variables needing interpolation from CSVs
- Load reference grid specified in the `interpolation_file` column
- Apply conservative interpolation to regrid data
- Save to derived directory with interpolation method (e.g., gr006)

### 5️⃣ **Standardization Phase**
```
Derived/Raw NetCDF → scripts/standardization/*.py → Standardized NetCDF
```
- Apply unit conversions
- Update metadata attributes
- Ensure CF convention compliance

### 6️⃣ **Cataloguing Phase**
```
All NetCDF files → scripts/catalogue/produce_catalog.py → CSV + PDF reports
```
- Scan all output directories
- Check file existence for each year
- Generate availability reports with temporal resolution
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
  - `product_type`: `raw` or `derived` (interpolated data is stored as derived)
  - `temporal_resolution`: hourly, daily, 3hourly, 6hourly, monthly
  - `interpolation`: native (non-interpolated) or grid specification (e.g., gr006)
  - `interpolation_file`: Reference grid file for interpolation (if needed)
  - `output_path`: Base directory for saving data
  - `script`: Which Python script handles this dataset

**Example:** A row specifying to download u10 (10m wind u-component) for years 2022-2024 from ERA5.

---

### 📂 **scripts/**

Organized directory containing all Python scripts:

#### **scripts/download/**
Scripts that **download data from CDS**
- One script per CDS catalogue (e.g., `reanalysis-era5-single-levels.py`)
- Reads request CSVs and creates API requests
- Downloads files to directory structure: `{base}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/`
- Skip files that already exist

#### **scripts/utilities/**
Centralized utility functions
- `utils.py`: Core functions for path construction and file downloads
  - `build_output_path()`: Constructs directory paths with temporal resolution and interpolation
  - `load_output_path_from_row()`: Extracts output path from CSV row
  - `load_input_path_from_row()`: Extracts input path from CSV row
  - `load_path_from_df()`: Lookup path for a variable in DataFrame
  - `download_files()`: Orchestrates parallel downloads based on CSV configuration
- `create_folder_structure.py`: Creates complete directory structure from CSVs without downloading

#### **scripts/derived/**
Scripts that **calculate derived variables** from raw data
- Example: `reanalysis-era5-single-levels.py` calculates:
  - `sfcwind` (wind speed) from `u10` and `v10` components using: `sfcwind = √(u10² + v10²)`
- Uses `operations.py` which provides utility functions:
  - `sfcwind_from_u_v()`: Calculate wind speed from components
  - `resample_to_daily()`: Aggregate hourly data to daily statistics

**Workflow:**
1. Read CSV to identify variables marked as "derived"
2. Load required raw data files
3. Apply mathematical operations
4. Resample to daily values if needed
5. Save derived variables with new temporal resolution

#### **scripts/interpolation/**
Scripts that **interpolate datasets to reference grids**
- Example: `reanalysis-cerra-single-levels.py` interpolates CERRA data
- Reference grid specified in `interpolation_file` column of request CSVs
- Uses conservative interpolation method via xESMF
- Saves to derived directory with interpolation method identifier (e.g., gr006)

**Workflow:**
1. Read CSV to identify variables needing interpolation (interpolation != 'native')
2. Load reference grid from specified file
3. Apply conservative_normed interpolation to regrid data
4. Save to: `{base}/derived/{dataset}/{temporal_resolution}/{interpolation}/{variable}/`

#### **scripts/standardization/**
Scripts that **standardize variables** to CF conventions
- Example: `derived-era5-single-levels-daily-statistics.py` contains functions like:
  - `tp()`: Convert precipitation from m/day to kg/m²/s (flux)
  - `e()`: Convert evaporation with proper units and attributes
  - `ssrd()`: Convert solar radiation from J/m² to W/m²

#### **scripts/catalogue/**
Scripts that **generate visual catalogues** of available data
- `produce_catalog.py`: Scans directories, creates CSV catalogues showing data availability, generates heatmap visualizations
- `generate_resumen.py`: Creates summary reports
- Output saved to `catalogues/catalogues/` and `catalogues/images/`

#### **scripts/notebooks/**
Jupyter notebooks for **exploration and testing**

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

Output directory for **catalogues and visualizations**
- `catalogues/`: CSV files listing all variables, datasets, date ranges, file paths
- `images/`: PDF heatmaps showing data availability (green=downloaded, orange=partial, red=missing)
- Updated nightly via GitHub Actions CI/CD

---

## Technical Details

### Directory Structure
**Enhanced structure with temporal resolution and interpolation metadata:**

```
{base}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
```

Where:
- `product_type`: `raw` or `derived` (interpolated data stored as derived)
- `temporal_resolution`: hourly, daily, 3hourly, 6hourly, monthly
- `interpolation`: native (non-interpolated) or grid specification (e.g., gr006)

**Examples:**
- Raw hourly ERA5: `/lustre/.../raw/reanalysis-era5-single-levels/hourly/native/u10/`
- Derived daily wind: `/lustre/.../derived/reanalysis-era5-single-levels/daily/native/sfcwind/`
- Interpolated CERRA: `/lustre/.../derived/reanalysis-cerra-single-levels/3hourly/gr006/t2m/`

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
  - `scripts/download/launch_all_requests_scripts.sh`: Batch job launcher for HPC environments
  - Designed for cluster computing with job scheduling

## Supported Datasets

- reanalysis-era5-single-levels
- reanalysis-cerra-single-levels
- reanalysis-cerra-land
- derived-era5-single-levels-daily-statistics

## Usage Example

### To download ERA5 data:
1. Edit `requests/reanalysis-era5-single-levels.csv` to specify years and variables
2. Run: `python scripts/download/reanalysis-era5-single-levels.py`
3. Raw data downloads to: `{base}/raw/{dataset}/{temporal_resolution}/native/{variable}/`

### To calculate derived variables:
1. Ensure raw data is downloaded
2. Run: `python scripts/derived/reanalysis-era5-single-levels.py`
3. Derived variables saved to: `{base}/derived/{dataset}/{temporal_resolution}/native/{variable}/`

### To interpolate data:
1. Ensure raw data is downloaded
2. Specify reference grid in the `interpolation_file` column of request CSV
3. Run: `python scripts/interpolation/reanalysis-cerra-single-levels.py`
4. Interpolated data saved to: `{base}/derived/{dataset}/{temporal_resolution}/{grid_spec}/{variable}/`

### To create folder structure:
1. Run: `python scripts/utilities/create_folder_structure.py --dry-run` (preview)
2. Run: `python scripts/utilities/create_folder_structure.py` (create)
3. Creates all directories based on CSV configurations without downloading

### To update catalogues:
1. Run: `python scripts/catalogue/produce_catalog.py`
2. Generates CSV catalogues and PDF visualizations in `catalogues/`
3. Shows data availability status with temporal resolution metadata

## Integration
This repository is part of the **C3S Atlas** ecosystem and uses the same conda environment. It serves as the data acquisition and preprocessing layer, providing standardized climate data for downstream analysis tools.

## Summary
The **c3s-cds repository** is a comprehensive data management system for climate reanalysis datasets. It automates the entire pipeline from CDS API downloads through interpolation and standardization to catalog generation, making climate data readily accessible and well-documented for scientific research.
