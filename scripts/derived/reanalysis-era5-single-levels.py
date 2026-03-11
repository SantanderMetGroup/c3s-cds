import operations
import pandas as pd
import xarray as xr
import glob
import os
import logging
from pathlib import Path
import sys
sys.path.append('../utilities')
from utils import load_output_path_from_row, require_single_row, load_derived_dependencies

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONTH_LIST = [f"{i:02d}" for i in range(1, 13)]


def main():
    dataset="reanalysis-era5-single-levels"
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_dependencies = load_derived_dependencies()
    derived_variables = df_parameters[df_parameters['product_type'] == 'derived']['filename_variable']
    derived_variables_list = derived_variables.tolist()
    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        mask_var = (df_parameters['filename_variable'] == var) & (df_parameters['product_type'] == 'derived')
        matches = df_parameters[mask_var]
        if matches.shape[0] == 0:
            raise KeyError(f"No row found: {var}/derived")

        # process each temporal_resolution (e.g., hourly and daily) so both get calculated.
        for _, var_row in matches.iterrows():
            # Create a list of years from start to end for this specific row
            # year_list = list(range(var_row["cds_years_start"].squeeze(), var_row["cds_years_end"].squeeze() + 1))
            year_list = list(range(int(var_row["cds_years_start"]), int(var_row["cds_years_end"]) + 1))
            for year in year_list:

                dependencies = derived_dependencies.get(var, [])
                if not dependencies:
                    logging.warning(f"No dependencies declared for derived variable {var}. Skipping...")
                    continue

                if var == "sfcwind":               
                    input_row_u10 = require_single_row(df_parameters, (df_parameters['filename_variable'] == dependencies[0]) & (df_parameters['product_type'] == 'raw'), f"{dependencies[0]}/raw")
                    input_row_v10 = require_single_row(df_parameters, (df_parameters['filename_variable'] == dependencies[1]) & (df_parameters['product_type'] == 'raw'), f"{dependencies[1]}/raw")
                    # Use utility function to load input paths
                    u10_download_path = load_output_path_from_row(input_row_u10, dataset)
                    u_10_file = glob.glob(f"{u10_download_path}/*{year}*.nc")[0]
                    v10_download_path = load_output_path_from_row(input_row_v10, dataset)
                    v_10_file = glob.glob(f"{v10_download_path}/*{year}*.nc")[0]
                    # Use utility function to build output path
                    dest_dir = load_output_path_from_row(var_row, dataset)
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
                    # Take into account that the calculus can be hourly
                    if var_row["temporal_resolution"]=="daily":
                        sfcwind = operations.resample_to_daily(sfcwind,"valid_time")

                    logging.info(f"Saving calculated sfcwind to {dest_dir}")
                    sfcwind.to_netcdf(output_file)

                if var == "hurs" and var_row["temporal_resolution"]=="hourly":               
                    input_row_d2m = require_single_row(df_parameters, (df_parameters['filename_variable'] == dependencies[0]) & (df_parameters['product_type'] == 'raw'), f"{dependencies[0]}/raw")
                    input_row_t2m = require_single_row(df_parameters, (df_parameters['filename_variable'] == dependencies[1]) & (df_parameters['product_type'] == 'raw'), f"{dependencies[1]}/raw")
                    # Use utility function to load input paths
                    d2m_download_path = load_output_path_from_row(input_row_d2m, dataset)
                    d2m_file = glob.glob(f"{d2m_download_path}/*{year}*.nc")[0]
                    t2m_download_path = load_output_path_from_row(input_row_t2m, dataset)
                    t2m_file = glob.glob(f"{t2m_download_path}/*{year}*.nc")[0]
                    # Use utility function to build output path
                    dest_dir = load_output_path_from_row(var_row, dataset)
                    os.makedirs(dest_dir, exist_ok=True)
                    hurs_file = os.path.basename(d2m_file).replace("d2m", "hurs")
                    output_file=Path(f"{dest_dir}/{hurs_file}")
                    logging.info(f"output_file: {output_file}")
                    if output_file.exists():
                        logging.info(f"File {output_file} already exists. Skipping...")
                        continue


                    for month in MONTH_LIST:

                        time_sel = f"{year}-{month}"
                        logging.info(f"Calculating hurs from {d2m_file} and {t2m_file}")
                        ds_d2m = (
                            xr.open_dataset(d2m_file, chunks={"time": 744})
                            .rename({"valid_time": "time"})
                            .sel(time=time_sel)
                        )
                        ds_t2m = (
                            xr.open_dataset(t2m_file, chunks={"time": 744})
                            .sel(time=time_sel)
                        )
                        ds_t2m =  (
                            ds_t2m
                            .assign_coords(longitude=(ds_t2m.longitude % 360))
                            .sortby("longitude")
                        )
                        logging.info(f"Processing month: {month} for year: {year}")


                        hurs = operations.rh_from_thermofeel(ds_d2m, ds_t2m)

                        # build a month-specific output filename safely
                        output_file_month = Path(str(output_file).replace(str(year), f"{year}{month}"))
                        
                        logging.info(f"Saving calculated hurs to {output_file_month}")
                        if output_file_month.exists():
                            logging.info(f"File {output_file_month} already exists. Skipping...")
                            continue
                        hurs.to_netcdf(output_file_month)

                        
                        hurs.close()
                    ds_d2m.close()
                    ds_t2m.close()
                    del ds_d2m, ds_t2m
                    # no lingering references
if __name__ == "__main__":
    main()