import operations
import pandas as pd
import xarray as xr
import glob
import os
import logging
from pathlib import Path
import sys
sys.path.append('../utilities')
from utils import load_path_from_df, load_output_path_from_row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    dataset="reanalysis-era5-single-levels"
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_variables = df_parameters[df_parameters['product_type'] == 'derived']['filename_variable']
    derived_variables_list = derived_variables.tolist()
    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        input_row = df_parameters[(df_parameters['filename_variable'] == var) & (df_parameters['product_type'] == 'raw')]
        var_row = df_parameters[(df_parameters['filename_variable'] == var) & (df_parameters['product_type'] == 'derived')]
        
        # Create a list of years from start to end
        year_list = list(range(var_row["cds_years_start"].squeeze() , var_row["cds_years_end"].squeeze()  + 1))
        for year in year_list:

            if var == "sfcwind":
                


                input_row_u10 = df_parameters[(df_parameters['filename_variable'] == "u10") & (df_parameters['product_type'] == 'raw')]
                input_row_v10 = df_parameters[(df_parameters['filename_variable'] == "v10") & (df_parameters['product_type'] == 'raw')]
                # Use utility function to load input paths
                u10_download_path = load_output_path_from_row(input_row_u10.iloc[0], dataset)
                u_10_file = glob.glob(f"{u10_download_path}/*{year}*.nc")[0]
                v10_download_path = load_output_path_from_row(input_row_v10.iloc[0], dataset)
                v_10_file = glob.glob(f"{v10_download_path}/*{year}*.nc")[0]

                # Use utility function to build output path
                dest_dir = load_output_path_from_row(var_row.iloc[0], dataset)
                os.makedirs(dest_dir, exist_ok=True)
                sfcwind_file = os.path.basename(u_10_file).replace("u10", "sfcwind")
                output_file=Path(f"{dest_dir}/{sfcwind_file}")
                logging.info(f"output_file: {output_file}")
                if output_file.exists():
                    logging.info(f"File {output_file} already exists. Skipping...")
                    continue
                logging.info(f"Calculating sfcwind from {u_10_file} and {v_10_file}")
                ds_u = xr.open_dataset(u_10_file)
                ds_v = xr.open_dataset(v_10_file)
                ds_merge = xr.merge([ds_u, ds_v])
                sfcwind = operations.sfcwind_from_u_v(ds_merge)
                sfcwind_daily = operations.resample_to_daily(sfcwind,"valid_time")
                

                logging.info(f"Saving calculated sfcwind to {dest_dir}")
                sfcwind_daily.to_netcdf(output_file)

if __name__ == "__main__":
    main()