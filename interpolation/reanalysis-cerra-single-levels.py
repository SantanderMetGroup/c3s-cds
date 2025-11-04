import c3s_atlas.interpolation as xesmfCICA
import numpy as np
import xarray as xr
import glob
import os
import pandas as pd
from pathlib import Path

def write_to_netcdf(dataset: xr.Dataset, path: str, var: str):
    """
    Save a xarray.Dataset as a netCDF file.

    Each file will be saved with a specific encoding where we can define
    the chunk strategy, the compress level etc.

    Parameters
    ----------
    dataset (xarray.Dataset): data stored by dimension
    """
    chunksizes = (len(dataset.time), 50, 50)
    encoding_var = dict(
        dtype="float32",
        shuffle=True,
        zlib=True,
        complevel=1,
        chunksizes=chunksizes
    )
    encoding = {var:encoding_var}
    dataset.to_netcdf(path=path, encoding=encoding)

def main():
    dataset="reanalysis-cerra-single-levels"
    variables_file_path = f"../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    ds_ref=xr.open_dataset("/lustre/gmeteo/WORK/chantreuxa/cica/data/resources/reference-grids/land_sea_mask_0.0625degree.nc4")

    for index, row in df_parameters.iterrows():
        # Process rows with non-native interpolation (interpolated data is now in derived product_type)
        if row["interpolation"] == "native":
            continue
        if row["product_type"] != "derived":
            continue
        ds_variable=row["filename_variable"]
        
        # Build input path using new structure
        base_input_path = row['input_path']
        temporal_resolution = row['temporal_resolution']
        # Input is from raw data, so we need to get the interpolation value from the raw data row
        raw_row = df_parameters[(df_parameters['filename_variable'] == ds_variable) & (df_parameters['product_type'] == 'raw')].iloc[0]
        raw_interpolation = raw_row['interpolation']
        
        orig_dir = Path(base_input_path) / 'raw' / dataset / temporal_resolution / raw_interpolation / ds_variable
        
        # Build output path using new structure
        base_output_path = row['output_path']
        output_interpolation = row['interpolation']
        output_dir = Path(base_output_path) / row['product_type'] / dataset / temporal_resolution / output_interpolation / ds_variable
        output_dir.mkdir(parents=True, exist_ok=True)
        
        pattern="*.nc"
        paths=np.sort(glob.glob(str(orig_dir / pattern)))
        for file in paths:
            filename = os.path.basename(file)
            print(file)
            output_file = output_dir / filename
            if output_file.exists():
                print(f"File {output_file} already exists. Skipping...")
                continue
            ds=xr.open_dataset(file)
            if "valid_time" in ds.dims:
                ds = ds.rename({"valid_time": "time"})

            # interpolate data
            int_attr = {'interpolation_method' : 'conservative_normed', 
                        'lats' : ds_ref.lat.values,
                        'lons' : ds_ref.lon.values,
                        'var_name' : ds_variable
            }

            INTER = xesmfCICA.Interpolator(int_attr)

            ds_i = INTER(ds)
            write_to_netcdf(ds_i, str(output_file), ds_variable)
            ds.close()
            ds_i.close()
            del ds,ds_i
if __name__ == "__main__":
    main()
