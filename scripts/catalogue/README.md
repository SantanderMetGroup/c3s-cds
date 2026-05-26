# scripts/catalogue

This folder contains scripts that build and summarize catalogues from downloaded/processed data.

## What it contains
- `produce_catalog.py`: reads request CSVs, checks NetCDF availability/date coverage in output folders, writes per-dataset catalogues and a consolidated `all_catalogues.csv`, and generates heatmaps for raw datasets.
- `generate_resumen.py`: updates the catalogue overview markdown.
- `catalog_executor.sh`: batch entrypoint used in scheduled execution environments.

## Role in the workflow
- Consolidates pipeline outputs into machine-readable catalogues and visual summaries consumed by `catalogues/` and `docs/`.
