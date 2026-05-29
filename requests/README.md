# requests

This folder contains one CSV file per dataset/catalogue request used by the pipeline.

## What it contains
- Request inventory files (for example `reanalysis-era5-single-levels.csv`, `satellite-sea-level-global.csv`).
- Per-variable rows with request and processing metadata (dataset, variable, output/input paths, product type, temporal resolution, interpolation, script).

## Role in the workflow
- Defines what to download from CDS (or related sources).
- Drives directory creation (`scripts/utilities/create_folder_structure.py`).
- Points to the processing scripts used for download, derived products, and interpolation.
- Feeds catalogue and validation steps downstream.
