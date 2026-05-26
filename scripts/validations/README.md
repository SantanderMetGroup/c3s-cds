# scripts/validations

Automated validation scripts used to check data quality and support CI.

## What it contains
- `ci_cd_validations.py`: catalogue-driven outlier checks on NetCDF files with xarray/dask, with non-zero exit on failures.
- `generate_timeseries.py`: helper generation of validation timeseries/diagnostic artifacts.

## Role in the workflow
- Provides automated quality gates and reproducible validation outputs for catalogues and dashboards.
