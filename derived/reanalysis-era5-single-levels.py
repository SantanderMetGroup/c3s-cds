import operations
import pandas as pd
import xarray as xr
import glob
import os
import logging
from pathlib import Path
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    dataset="reanalysis-era5-single-levels"
    variables_file_path = f"../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_variables = df_parameters[df_parameters['product_type'] == 'derived']['filename_variable']
    derived_variables_list = derived_variables.tolist()
    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        var_row=df_parameters[df_parameters['filename_variable'] == var]
        # Create a list of years from start to end ye
        year_list = list(range(var_row["cds_years_start"].squeeze() , var_row["cds_years_end"].squeeze()  + 1))
        for year in year_list:

            if var == "sfcwind":
                u10_download_path = operations.load_path_from_df(df_parameters, 'u10')
                u_10_file = glob.glob(f"{u10_download_path}/*{year}*.nc")[0]
                v10_download_path = operations.load_path_from_df(df_parameters, 'v10')
                v_10_file = glob.glob(f"{v10_download_path}/*{year}*.nc")[0]


                logging.info(f"Calculating sfcwind from {u_10_file} and {v_10_file}")
                ds_u = xr.open_dataset(u_10_file)
                ds_v = xr.open_dataset(v_10_file)
                ds_merge = xr.merge([ds_u, ds_v])
                sfcwind = operations.sfcwind_from_u_v(ds_merge)
                sfcwind_daily = operations.resample_to_daily(sfcwind,"valid_time")
                
                # Build output path using new structure
                row = var_row.iloc[0]
                base_path = row['output_path']
                product_type = row['product_type']
                temporal_resolution = row['temporal_resolution']
                interpolation = row['interpolation']
                
                dest_dir = Path(base_path) / product_type / dataset / temporal_resolution / interpolation / var
                os.makedirs(dest_dir, exist_ok=True)
                sfcwind_file = os.path.basename(u_10_file).replace("u10", "sfcwind")
                logging.info(f"Saving calculated sfcwind to {dest_dir}")     
                sfcwind_daily.to_netcdf(f"{dest_dir}/{sfcwind_file}")

if __name__ == "__main__":
    main()