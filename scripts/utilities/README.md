# scripts/utilities

Shared helpers used across download, derived, interpolation, catalogue, and validation scripts.

## What it contains
- Path and request helpers (`utils.py`, `utils_download.py`).
- Logging and execution helpers (`logging_utils.py`, `run_with_memlog.py`).
- Dask/SLURM helpers (`utils_dask_slurm.py`).
- Derived-pipeline dependency and processing helpers (`derived_variable_dependencies.py`, `utils_derived_pipeline.py`).
- Fix-oriented helpers (`utils_fixes.py`).
- `create_folder_structure.py` to create the full directory tree from `requests/*.csv` (supports `--dry-run`) using:
  `{base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/`.

## Role in the workflow
- Centralizes reusable logic so dataset-specific scripts stay focused on data operations.
