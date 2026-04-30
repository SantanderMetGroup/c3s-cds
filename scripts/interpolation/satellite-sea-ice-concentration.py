import c3s_atlas.interpolation as xesmfCICA
import numpy as np
import xarray as xr
import glob
import os
import pandas as pd
from pathlib import Path
from pyproj import Transformer
import sys
sys.path.append('../utilities')
from utils import  load_output_path_from_row,require_rows,require_single_row

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

def process_dataset(dataset):
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    cds_cdr_unique = df_parameters["cds_cdr_type"].unique().tolist()
    for cds_cdr in cds_cdr_unique:
        #Take first row with interpolation not native and product_type derived as reference for loading the reference grid
        mask_interpolation = (df_parameters['interpolation'] != 'native') & (df_parameters['product_type'] == 'derived') & (df_parameters['cds_cdr_type'] == cds_cdr)
        interpolated_row= require_single_row(df_parameters, mask_interpolation, "interpolation not native and product_type derived")
        interpolation_file = interpolated_row.get('interpolation_file', 'land_sea_mask_grd025.nc')
        ds_ref=xr.open_dataset(f"/lustre/gmeteo/WORK/chantreuxa/cica/data/resources/reference-grids/{interpolation_file}")
        proj_laea = "+proj=laea +lat_0=90 +lon_0=0 +datum=WGS84 +ellps=WGS84"
        transformer = Transformer.from_crs(
            proj_laea,
            "EPSG:4326",
            always_xy=True
        )
        for index, row in df_parameters.iterrows():
            # Process rows with non-native interpolation (interpolated data is now in derived product_type)
            if row["interpolation"] == "native":
                continue
            if row["product_type"] != "derived":
                continue
            ds_variable=row["filename_variable"]
            
            # Use utility function to load input path (from raw data)
            mask_raw = (df_parameters['filename_variable'] == ds_variable) & (df_parameters['product_type'] == 'raw') & (df_parameters['cds_cdr_type'] == cds_cdr)
            raw_row = require_single_row(df_parameters, mask_raw, f"{ds_variable}/raw")

            orig_dir = load_output_path_from_row(raw_row, dataset)
            
            # Use utility function to load output path
            output_dir = load_output_path_from_row(row, dataset)
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

                #xc = ds["xc"].values
                #yc = ds["yc"].values
                #xc2d, yc2d = np.meshgrid(xc, yc)
                #lon_src, lat_src = transformer.transform(xc2d, yc2d)
                # 1. Rename dimensions
                ds = ds.rename({"xc": "x", "yc": "y"})
                #ds = ds.assign_coords(
                #lon_src=(("y", "x"), lon_src),
                #lat_src=(("y", "x"), lat_src),
                #)
                
                # interpolate data
                int_attr = {
                    "interpolation_method": "conservative_normed",

                    # SOURCE GRID (IMPORTANT for LAEA sea ice)
                    "src_lats": ds["lat"].values,
                    "src_lons": ds["lon"].values,

                    # TARGET GRID (your reference lat/lon grid)
                    "lats": ds_ref["lat"].values,
                    "lons": ds_ref["lon"].values,

                    "var_name": ds_variable
                }

                INTER = xesmfCICA.Interpolator(int_attr)

                ds_i = INTER(ds)
                write_to_netcdf(ds_i, str(output_file), ds_variable)
                ds.close()
                ds_i.close()
                del ds,ds_i
if __name__ == "__main__":
    datasets=["satellite-sea-ice-concentration_nh","satellite-sea-ice-concentration_sh"]
    for dataset in datasets:
        process_dataset(dataset)
