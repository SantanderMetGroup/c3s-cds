import c3s_atlas.interpolation as xesmfCICA
import numpy as np
import xarray as xr
import glob
import os
import pandas as pd

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
        ds_variable=row["filename_variable"]
        orig_dir = f"{row['path_download']}/{dataset}/{ds_variable}/"
        output=f"/lustre/gmeteo/WORK/DATA/C3S-CDS/ERA5_temp/gr006/{dataset}/{ds_variable}/"
        os.makedirs(output, exist_ok=True) 
        pattern="*.nc"
        paths=np.sort(glob.glob(orig_dir+pattern))
        for file in paths:
            print(file)
            ds=xr.open_dataset(file)
            if "valid_time" in ds.dims:
                ds = ds.rename({"valid_time": "time"})

            # interpolate data
            int_attr = {'interpolation_method' : 'conservative_normed', 
                        'lats' : ds_ref.lat.values,
                        'lons' : ds_ref.lon.values,
                        'var_name' : ds_variable
            }
            filename = os.path.basename(file)
            if os.path.exists(output+filename):
                print(f"File {output+filename} already exists. Skipping...")
                continue
            INTER = xesmfCICA.Interpolator(int_attr)

            ds_i = INTER(ds)
            write_to_netcdf(ds_i, output+filename,ds_variable)
if __name__ == "__main__":
    main()
