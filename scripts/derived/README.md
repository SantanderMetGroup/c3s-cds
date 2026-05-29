# scripts/derived

This folder computes derived products that are not directly downloaded as final variables.

## What it contains
- Dataset-specific derived pipelines.
- Shared scientific operations in `operations.py` (for example relative/specific humidity, surface wind, radiation components, MRT, UTCI).

## Role in the workflow
- Transforms raw variables into derived variables requested in `requests/*.csv`.
- Produces `derived` outputs used by catalogue generation and validations.
