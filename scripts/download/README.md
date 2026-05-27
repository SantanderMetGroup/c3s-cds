# scripts/download

Scripts in this folder download raw data from CDS (and related configured sources), typically one script per dataset.

## What it contains
- Dataset-specific download scripts (for example `reanalysis-era5-single-levels.py`).
- `launch_all_requests_scripts.sh` to collect script paths from `requests/*.csv` and submit batch jobs with SLURM after activating `c3s-atlas`.

## Role in the workflow
- First operational step for raw data ingestion.
- Produces data in the configured directory structure used by downstream standardization, derived, interpolation, and catalogue steps.
