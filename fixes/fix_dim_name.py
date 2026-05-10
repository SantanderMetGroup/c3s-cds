#!/usr/bin/env python3

import os
import glob
from netCDF4 import Dataset


# =========================
# USER PARAMETERS (EDIT ME)
# =========================
ROOT_FOLDER = "/path/to/netcdf/files"
OLD_NAME = "time"
NEW_NAME = "time_new"
# =========================


def rename_in_file(nc_path, old_name, new_name):
    try:
        with Dataset(nc_path, "a") as ds:

            # ---- Rename dimension ----
            if old_name in ds.dimensions:
                ds.renameDimension(old_name, new_name)
                print(f"[OK] Dimension renamed: {nc_path}")
            else:
                print(f"[SKIP] Dimension '{old_name}' not found: {nc_path}")

            # ---- Rename variable (coordinate or regular variable) ----
            if old_name in ds.variables:
                ds.renameVariable(old_name, new_name)
                print(f"[OK] Variable renamed:   {nc_path}")
            else:
                print(f"[WARN] Variable '{old_name}' not found: {nc_path}")

    except Exception as e:
        print(f"[ERROR] {nc_path}: {e}")


def process_folder(root_folder, old_name, new_name):
    pattern = os.path.join(root_folder, "**", "*.nc")
    files = glob.glob(pattern, recursive=True)

    print(f"Found {len(files)} NetCDF files\n")

    for f in files:
        rename_in_file(f, old_name, new_name)


if __name__ == "__main__":
    process_folder(ROOT_FOLDER, OLD_NAME, NEW_NAME)