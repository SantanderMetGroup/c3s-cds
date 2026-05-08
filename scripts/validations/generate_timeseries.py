import os
import argparse
import xarray as xr
import glob
import pandas as pd
import matplotlib.pyplot as plt

def process_catalog(catalog_path, output_root="validations"):
    """
    Reads the catalog and processes each entry to generate an aggregated time series.
    """
    df = pd.read_csv(catalog_path)
    
    for index, row in df.iterrows():
        dataset_name = row['dataset']
        variable = row['variable']
        data_path = row['data_path']
        
        if pd.isna(data_path) or not isinstance(data_path, str):
            continue
            
        print(f"Processing dataset {dataset_name} | variable: {variable}")
        generate_timeseries_for_variable(data_path, output_root, dataset_name, variable)

def generate_timeseries_for_variable(input_dir, output_root, dataset_name, expected_var):
    """
    Generates time series for all combined years of a specific variable.
    """
    # Create the output directory for the specific dataset
    output_dir = os.path.join(output_root, dataset_name)
    os.makedirs(output_dir, exist_ok=True)

    # Find all NetCDF files in the input directory
    file_pattern = os.path.join(input_dir, "*.nc")
    files = sorted(glob.glob(file_pattern))

    if not files:
        print(f"Skipping: No NetCDF files found in {input_dir}")
        return

    output_file = os.path.join(output_dir, f"{expected_var}_timeseries.nc")
    png_file = os.path.join(output_dir, f"{expected_var}_timeseries.png")
    csv_file = os.path.join(output_dir, f"{expected_var}_timeseries.csv")
    
    if os.path.exists(png_file):
        print(f"Skipping: Plot already exists for {expected_var} in {dataset_name}")
        return

    print(f"Found {len(files)} files in {input_dir}. Opening dataset...")
    
    try:
        ds = xr.open_mfdataset(files, combine='by_coords')
    except Exception as e:
        print(f"Error opening files in {input_dir}: {e}")
        return

    # Use the expected variable from the catalog if it matches, else try to find the data var
    var_name = expected_var if expected_var in ds.data_vars else list(ds.data_vars)[0]
    var_data = ds[var_name]
    
    # Calculate the spatial mean to get a time series
    dims_to_reduce = [dim for dim in var_data.dims if dim not in ['time', 'valid_time']]
    
    if dims_to_reduce:
        print(f"Reducing dimensions: {dims_to_reduce} for {var_name}")
        ts = var_data.mean(dim=dims_to_reduce, keep_attrs=True)
    else:
        ts = var_data

    print(f"Plotting and saving to {png_file}...")
    plt.figure(figsize=(12, 6))
    ts.plot()
    plt.title(f"Timeseries: {dataset_name} - {expected_var}")
    plt.ylabel(expected_var)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_file)
    plt.close()
    
    ds.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate timeseries automatically from catalog.")
    parser.add_argument("--catalog", default="catalogues/catalogues/all_catalogues.csv", help="Path to the all_catalogues.csv file")
    parser.add_argument("--output-root", default="validations", help="Root directory for validations")
    
    args = parser.parse_args()
    
    process_catalog(args.catalog, args.output_root)
