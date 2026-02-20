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

def main():
    dataset="derived-era5-single-levels-daily-statistics"
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_dependencies = load_derived_dependencies()
    derived_variables = df_parameters[df_parameters['product_type'] == 'derived']['filename_variable']
    derived_variables_list = derived_variables.tolist()
    logging.info(f"Derived variables to process: {derived_variables_list}")
    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        mask_var = (df_parameters['filename_variable'] == var) & (df_parameters['product_type'] == 'derived')
        var_row = require_single_row(df_parameters, mask_var, f"{var}/derived")
        
        # Create a list of years from start to end
        year_list = list(range(var_row["cds_years_start"].squeeze() , var_row["cds_years_end"].squeeze()  + 1))
        for year in year_list:

            dependencies = derived_dependencies.get(var, [])
            if not dependencies:
                logging.warning(f"No dependencies declared for derived variable {var}. Skipping...")
                continue

            if var == "hurs":               
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
                logging.info(f"Calculating hurs from {d2m_file} and {t2m_file}")
                ds_d2m = xr.open_dataset(d2m_file)
                ds_t2m = xr.open_dataset(t2m_file)
                ds_merge = xr.merge([ds_d2m, ds_t2m])
                hurs = operations.rh_from_thermofeel(ds_merge, "d2m", "t2m")

                logging.info(f"Saving calculated hurs to {dest_dir}")
                hurs.to_netcdf(output_file)

                ds_d2m.close()
                ds_t2m.close()
                hurs.close()
                del ds_d2m, ds_t2m, hurs

if __name__ == "__main__":
    main()