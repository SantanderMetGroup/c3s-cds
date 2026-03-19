import operations
import pandas as pd
import xarray as xr
import glob
import os
import logging
from pathlib import Path
import sys
sys.path.append('../utilities')
from utils import load_output_path_from_row, require_single_row, load_derived_dependencies, get_original_var,is_valid_netcdf
# Remove any handlers added by imports like utils
root_logger = logging.getLogger()
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Configure logging now
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def raw_condition(df, orig_var, dep):
    condition = (
        (df['filename_variable'] == orig_var) &
        (df['product_type'] == 'raw')
    )
    label = f"{orig_var}/raw"
    return condition, label

def process_derived(
    var,
    dataset,
    dependencies,
    df_parameters,
    var_row,
    year,
    function,
    condition_func  
):
    # Get original variable names
    original_vars = [get_original_var(dataset, dep) for dep in dependencies]

    # Fetch parameter rows using injected condition
    input_rows = [
        require_single_row(
            df_parameters,
            *condition_func(df_parameters, orig_var, dep)  # 👈 unpack (condition, label)
        )
        for orig_var, dep in zip(original_vars, dependencies)
    ]

    # Load download paths
    download_paths = [
        load_output_path_from_row(row, dataset) for row in input_rows
    ]

    # Get files
    files = [
        glob.glob(f"{path}/*{year}*.nc")[0] for path in download_paths
    ]

    # Output path
    dest_dir = load_output_path_from_row(var_row, dataset)
    os.makedirs(dest_dir, exist_ok=True)

    # Build output filename
    var_file = os.path.basename(files[0]).replace(original_vars[0], var)
    output_file = Path(f"{dest_dir}/{var_file}")

    logging.info(f"output_file: {output_file}")
    if output_file.exists():
        if is_valid_netcdf(Path(str(output_file).replace('zip','nc'))):
            logging.info(f"{output_file} already exists and is valid, skipping")
            return True

    logging.info(f"Calculating {var} from {files}")

    # Open datasets
    datasets = [xr.open_dataset(f) for f in files]

    # Rename variables dynamically
    for i, ds in enumerate(datasets):
        if original_vars[i] != dependencies[i]:
            datasets[i] = ds.rename({original_vars[i]: dependencies[i]})

    # Compute result
    result = function(*datasets)

    # Save result
    logging.info(f"Saving calculated {var} to {dest_dir}")
    result.to_netcdf(output_file)

    # Close datasets
    for ds in datasets:
        ds.close()
    result.close()


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
            logging.info(f"Derived variable {var} has dependencies: {dependencies}")
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
                hurs = operations.rh_from_thermofeel(ds_d2m, ds_t2m)

                logging.info(f"Saving calculated hurs to {dest_dir}")
                hurs.to_netcdf(output_file)

                ds_d2m.close()
                ds_t2m.close()
                hurs.close()
                del ds_d2m, ds_t2m, hurs
            if var == "huss":
                process_derived(
                                var,
                                dataset,
                                dependencies,
                                df_parameters,
                                var_row,
                                year,
                                operations.sh_xclim,
                                raw_condition  
                            )





if __name__ == "__main__":
    main()
