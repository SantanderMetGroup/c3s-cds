# scripts

This folder contains the operational pipeline scripts used to download, process, catalogue, and validate data.

## Subfolders
- `download/`: request execution and raw data download.
- `standardization/`: variable harmonization to repository conventions.
- `derived/`: computation of derived products.
- `interpolation/`: remapping to target grids.
- `catalogue/`: catalogue generation and summary artifacts.
- `utilities/`: shared helpers (paths, logging, Dask/SLURM, dependencies, fixes).
- `validations/`: automated quality checks used in CI and reporting.
- `notebooks/`: exploratory examples.

## Workflow fit
Requests in `../requests/` point to scripts here; outputs are then consolidated in `catalogues/`, visualized in `docs/`, and checked in `validations/`.
