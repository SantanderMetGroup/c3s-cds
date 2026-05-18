import os
import argparse
import xarray as xr
import glob
import pandas as pd
import matplotlib.pyplot as plt
import sys
import tempfile
import shutil

# Go up TWO directories from validations/ (to reach c3s-cds root)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Add the utilities directory to sys.path to resolve module imports
scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(scripts_dir, 'utilities'))

BATCH_SIZE = 50


def main(catalog_path, output_root="validations"):
    df = pd.read_csv(catalog_path)

    for index, row in df.iterrows():
        dataset_name = row['dataset']
        variable = row['variable']
        data_path = row['data_path']

        if pd.isna(data_path) or not isinstance(data_path, str):
            continue

        print(f"Processing dataset {dataset_name} | variable: {variable}")
        generate_timeseries_for_variable(
            data_path, output_root, dataset_name, variable,
        )


def _pick_variable(files, expected_var):
    with xr.open_dataset(files[0], decode_cf=True) as sample_ds:
        if expected_var in sample_ds.data_vars:
            return expected_var
        return list(sample_ds.data_vars)[0]


def _reduce_batch(batch_files, var_name, temp_dir):
    """Open a batch of files, compute the spatial mean, and save to a temp NetCDF."""
    def _preprocess(ds):
        if var_name in ds.data_vars:
            return ds[[var_name]]
        return ds

    with xr.open_mfdataset(
        batch_files,
        combine="by_coords",
        chunks={},
        parallel=False,
        coords="minimal",
        compat="override",
        data_vars="minimal",
        join="override",
        preprocess=_preprocess,
    ) as ds:
        var_data = ds[var_name]
        dims_to_reduce = [d for d in var_data.dims if d not in ("time", "valid_time")]

        if dims_to_reduce:
            ts = var_data.mean(dim=dims_to_reduce)
        else:
            ts = var_data

        ts = ts.compute()

        out_path = os.path.join(temp_dir, f"batch_{os.path.basename(batch_files[0])}_{os.path.basename(batch_files[-1])}.nc")
        ts.to_netcdf(out_path)
        print(f"  Batch {len(batch_files)} files -> {out_path} ({ts.sizes.get('time', 0)} timesteps)")
        return out_path


def generate_timeseries_for_variable(
    input_dir, output_root, dataset_name, expected_var,
):
    output_dir = os.path.join(output_root, dataset_name)
    os.makedirs(output_dir, exist_ok=True)

    file_pattern = os.path.join(input_dir, "*.nc")
    files = sorted(glob.glob(file_pattern))

    if not files:
        print(f"Skipping: No NetCDF files found in {input_dir}")
        return

    png_file = os.path.join(output_dir, f"{expected_var}_timeseries.png")

    if os.path.exists(png_file):
        print(f"Skipping: Plot already exists for {expected_var} in {dataset_name}")
        return

    print(f"Found {len(files)} files in {input_dir}.")

    try:
        var_name = _pick_variable(files, expected_var)
    except Exception as e:
        print(f"Error inspecting variables in {input_dir}: {e}")
        return

    if len(files) > BATCH_SIZE:
        print(f"Large dataset ({len(files)} files). Processing in batches of {BATCH_SIZE}...")

        temp_dir = tempfile.mkdtemp(prefix="ts_batches_")
        try:
            batch_paths = []
            for i in range(0, len(files), BATCH_SIZE):
                batch = files[i : i + BATCH_SIZE]
                print(f"Processing batch {i // BATCH_SIZE + 1}/{(len(files) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} files)...")
                path = _reduce_batch(batch, var_name, temp_dir)
                batch_paths.append(path)

            print(f"Combining {len(batch_paths)} batch results...")
            ts = xr.open_mfdataset(batch_paths, combine="by_coords", join="override")[var_name]
            ts = ts.sortby("time")

            if ts.sizes.get("time", 0) > 10000:
                print(f"Resampling to daily to reduce memory usage")
                ts = ts.resample(time="1D").mean()

            ts = ts.compute()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        print(f"Opening {len(files)} files directly...")

        def _preprocess(ds):
            if var_name in ds.data_vars:
                return ds[[var_name]]
            return ds

        try:
            ds = xr.open_mfdataset(
                files,
                combine="by_coords",
                chunks={},
                parallel=False,
                coords="minimal",
                compat="override",
                data_vars="minimal",
                join="override",
                preprocess=_preprocess,
            )
        except Exception as e:
            print(f"Error opening files in {input_dir}: {e}")
            return

        var_data = ds[var_name]
        dims_to_reduce = [d for d in var_data.dims if d not in ("time", "valid_time")]

        if dims_to_reduce:
            ts = var_data.mean(dim=dims_to_reduce)
        else:
            ts = var_data

        if ts.sizes.get("time", 0) > 10000:
            print(f"Resampling to daily to reduce memory usage")
            ts = ts.resample(time="1D").mean()

        ts = ts.compute()
        ds.close()

    print(f"Plotting and saving to {png_file}...")
    plt.figure(figsize=(12, 6))
    ts.plot()
    plt.title(f"Timeseries: {dataset_name} - {var_name}")
    plt.ylabel(expected_var)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_file)
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate timeseries automatically from catalog.")
    parser.add_argument("--catalog", default="catalogues/catalogues/all_catalogues.csv", help="Path to the all_catalogues.csv file")
    parser.add_argument("--output-root", default="validations", help="Root directory for validations")
    
    args = parser.parse_args()
    
    main(args.catalog, args.output_root)
