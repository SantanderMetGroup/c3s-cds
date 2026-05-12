import c3s_atlas.interpolation as xesmfCICA
import numpy as np
import xarray as xr
import glob
import os
import pandas as pd
from pathlib import Path
import sys
sys.path.append('../utilities')
from utils import  load_output_path_from_row,require_single_row,is_valid_netcdf,VARIABLE_DEPENDENCIES
from utils_derived_pipeline import get_original_var
def write_to_netcdf(dataset: xr.Dataset, path: str, var: str):
    """
    Save a xarray.Dataset as a netCDF file.

    Each file will be saved with a specific encoding where we can define
    the chunk strategy, the compress level etc.

    Parameters
    ----------
    dataset (xarray.Dataset): data stored by dimension
    """
    chunksizes = (len(dataset.time), 40, 40)
    encoding_var = dict(
        dtype="float32",
        shuffle=True,
        zlib=True,
        complevel=1,
        chunksizes=chunksizes
    )
    encoding = {var:encoding_var}
    dataset.to_netcdf(path=path, encoding=encoding)

def process_dataset(dataset: str, interpolation_file_default="ECMWF_Land_Medcof.nc"):
    """
    Process a dataset by interpolating derived variables with non-native interpolation.
    
    Parameters
    ----------
    dataset : str
        Name of the dataset to process.
    interpolation_file_default : str
        Default reference interpolation file if not specified in CSV.
    """
    variables_file_path = Path(f"../../requests/{dataset}.csv")
    df_parameters = pd.read_csv(variables_file_path)
    ds_ref = xr.open_dataset(f"/lustre/gmeteo/PTICLIMA/Auxiliary-material/Masks/{interpolation_file_default}")
    
    for _, row in df_parameters.iterrows():
        if row["interpolation"] == "native":
            continue
        if row["product_type"] != "derived":
            continue
        if row.get("interpolation_file", interpolation_file_default) != interpolation_file_default:
            continue

        ds_variable = row["filename_variable"]
        
        # Load raw variable for reference
        if ds_variable in VARIABLE_DEPENDENCIES:
            origin_var="derived"
            mask = (df_parameters['filename_variable'] == ds_variable) & (df_parameters['product_type'] == 'derived')& (df_parameters['interpolation'] == 'native')& (df_parameters['temporal_resolution'] == 'daily')
        else:
            origin_var="raw"
            mask = (df_parameters['filename_variable'] == ds_variable) & (df_parameters['product_type'] == 'raw')     
        raw_row = require_single_row(df_parameters, mask, f"{ds_variable}/{origin_var}")
        orig_dir = load_output_path_from_row(raw_row, dataset)
        
        # Output directory
        output_dir = load_output_path_from_row(row, dataset, raw=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for file in sorted(glob.glob(str(orig_dir / "*.nc"))):
            print(f"Processing file: {file}")
            filename = os.path.basename(file)
            output_file = output_dir / filename
            
            if output_file.exists():
                if is_valid_netcdf(Path(str(output_file).replace('zip','nc'))):
                    print(f"File {output_file} already exists. Skipping...")
                    continue
            
            ds = xr.open_dataset(file)
            if "valid_time" in ds.dims:
                ds = ds.rename({"valid_time": "time"})
            
            original_var = get_original_var(dataset, ds_variable)
            
            # Interpolation
            int_attr = {
                'interpolation_method': 'bilinear',
                'lats': ds_ref.latitude.values,
                'lons': ds_ref.longitude.values,
                'var_name': original_var
            }
            INTER = xesmfCICA.Interpolator(int_attr)
            ds_i = INTER(ds)
            
            write_to_netcdf(ds_i, str(output_file), original_var)
            
            ds.close()
            ds_i.close()
            del ds, ds_i
    ds_ref.close()

if __name__ == "__main__":
    # Example usage for multiple datasets
    datasets = [
        "derived-era5-single-levels-daily-statistics",
        "reanalysis-era5-single-levels"
    ]
    
    for dataset in datasets:
        print(f"Processing dataset: {dataset}")
        process_dataset(dataset)
