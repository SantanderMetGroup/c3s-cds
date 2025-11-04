from pathlib import Path
import operations
import pandas as pd
import xarray as xr
import glob
import numpy as np
import logging
import os
import calendar
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def write_to_netcdf(dataset: xr.Dataset, path: Path, var: str, lonlattime_name: list =["x,y,valid_time"]):
    """
    Save a xarray.Dataset as a netCDF file.

    Each file will be saved with a specific encoding where we can define
    the chunk strategy, the compress level etc.

    Parameters
    ----------
    dataset (xarray.Dataset): data stored by dimension
    path (pathlib.Path): output path to save the data
    """
    lon="longitude"
    lat="latitude"
    time="valid_time"
    if len(dataset[lon]) < 50 or len(dataset[lat]) < 50:
        chunksizes = (len(dataset[time]), len(dataset[lat]), len(dataset[lon]))
    else:
        chunksizes = (len(dataset[time]), 50, 50)

    encoding_var = dict(
        dtype="float32",
        shuffle=True,
        zlib=True,
        complevel=1,
        chunksizes=chunksizes,
    )
    #dataset = dataset.assign_coords(lat=dataset.lat.astype('float32'), lon=dataset.lon.astype('float32'))
    encoding = {var: encoding_var}
    logging.info(f"Writing {path} with encoding: {encoding} ")
    dataset.to_netcdf(path=path, encoding=encoding)
    dataset.close()

def check_time_gap(file1, file2, expected_timestep='1h'):
    """
    Check for gaps between the times of two files.

    Args:
        file1 (str): Path to the first file.
        file2 (str): Path to the second file.
        expected_timestep (str): Expected time step between consecutive times (e.g., '1h', '30m').

    Raises:
        ValueError: If a gap is detected between the files.
    """
    # Open the two files individually
    ds1 = xr.open_dataset(file1)
    ds2 = xr.open_dataset(file2)

    # Extract the last time of the first file and the first time of the second file
    last_time_file1 = ds1.valid_time.values[-1]
    first_time_file2 = ds2.valid_time.values[0]

    # Calculate the time difference
    time_diff = first_time_file2 - last_time_file1

    # Convert expected_timestep to a numpy timedelta64
    expected_timedelta = np.timedelta64(int(expected_timestep[:-1]), expected_timestep[-1])

    # Check for gaps
    if time_diff > expected_timedelta:
        raise ValueError(f"Gap detected between {file1} and {file2}: {time_diff}. Expected: {expected_timestep}.")
    else:
        logging.info(f"No gap detected between {file1} and {file2}. time_diff: {expected_timedelta}")
    ds1.close()
    ds2.close()
    del ds1, ds2


def get_first_month_accumulated(ds_accumulated):
    # Get the first year and month from the valid_time dimension
    first_year = ds_accumulated.valid_time.dt.year[0].values
    first_month = ds_accumulated.valid_time.dt.month[0].values

    # Determine the number of days in the first month
    last_day = calendar.monthrange(first_year, first_month)[1]

    # Define the start and end of the first month
    start_date = f'{first_year}-{first_month:02d}-01'
    end_date = f'{first_year}-{first_month:02d}-{last_day}'

    # Select the first month
    first_month_data = ds_accumulated.sel(valid_time=slice(start_date, end_date))

    return first_month_data


def accumulation(ds,var):
    time_dim="valid_time"
    # Define the hours of interest
    h3_list = ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00']
    h3_list = ['00', '03', '06', '09','12','15','18', '21']

    #select only 3-hourly times (drop others BEFORE shifting)
    hours = ds[time_dim].dt.strftime("%H")
    ds_3h = ds[var].where(hours.isin(h3_list), drop=True)
    # Shift 00 timestamps to previous day
    shifted_time = ds_3h[time_dim].where(
        ds_3h[time_dim].dt.strftime("%H") != "00",
        ds_3h[time_dim] - np.timedelta64(30, "m")
    )

    # Add a shifted time coordinate (no drop)
    ds_shift = ds_3h.assign_coords({time_dim: shifted_time}).sortby(time_dim)

    # Daily accumulation via resample
    daily = ds_shift.resample({time_dim: "1D"}).sum()

    return daily


if __name__ == "__main__":
    dataset="reanalysis-cerra-land"
    variables_file_path = f"../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_variables = df_parameters[df_parameters['product_type'] == 'derived']['filename_variable']
    derived_variables_list = derived_variables.tolist()
    logging.info(f"List of derived variables: {derived_variables_list}")

    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        var_row=df_parameters[df_parameters['filename_variable'] == var]
        var_download_path = operations.load_path_from_df(df_parameters, var, path_column='input_path', product_type='raw')
        var_files = np.sort(glob.glob(f"{var_download_path}/*.nc"))
        print(f"{var_download_path}/*.nc")
        logging.info(f"List of file variables: {var_files}")
        # Iterate over files and process accumulation, need two files for last hour accumulation
        for i,file in enumerate(var_files):
            next_file=var_files[i+1] if i+1 < len(var_files) else None
            logging.info(f"Processing file {file} and next file {next_file}")

            check_time_gap(file, next_file, expected_timestep='1h')
            ds_var = xr.open_mfdataset([file,next_file],concat_dim='valid_time', combine='nested') 
            ds_accumulated=accumulation(ds_var,var)
            first_month_data = get_first_month_accumulated(ds_accumulated)

            # Build output path using new structure
            row = var_row.iloc[0]
            base_path = row['output_path']
            product_type = row['product_type']
            temporal_resolution = row['temporal_resolution']
            interpolation = row['interpolation']
            
            dest_dir = Path(base_path) / product_type / dataset / temporal_resolution / interpolation / var
            logging.info(f"Saving calculated {var} to {dest_dir}")
            os.makedirs(dest_dir, exist_ok=True)     
            var_file = os.path.basename(file).replace(".nc", "_daily_accumulated.nc")
            write_to_netcdf(first_month_data, Path(f"{dest_dir}/{var_file}"), var=var)
            
            ds_var.close()
            del ds_var, ds_accumulated, first_month_data

    
        