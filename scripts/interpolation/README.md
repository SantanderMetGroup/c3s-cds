# scripts/interpolation

Scripts in this folder remap variables from native grids to target grids.

## What it contains
- Dataset/grid-specific interpolation pipelines.
- Configured target-grid references (via request metadata such as `interpolation` and optional interpolation files).

## Role in the workflow
- Generates interpolated products stored under `derived` with non-`native` interpolation labels (for example `gr006`, `gr100`).
- Distinguishes interpolated outputs from calculated-native derived products.
