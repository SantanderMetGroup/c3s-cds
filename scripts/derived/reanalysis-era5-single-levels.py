import operations
import pandas as pd
import logging
import sys
import os
from dask.distributed import Client

sys.path.append('../utilities')
from utils import load_derived_dependencies, raw_condition, derived_condition
from utils_dask_slurm import load_slurm_dask_config
from utils_derived_pipeline import process_derived
root_logger = logging.getLogger()
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Configure logging now
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

MONTH_LIST = [f"{i:02d}" for i in range(1, 13)]

PARAMS_SLURM = load_slurm_dask_config()
logging.info(f"System parameters for Dask configuration: {PARAMS_SLURM}")



def main():
    client = Client(
    n_workers=1,
    threads_per_worker=PARAMS_SLURM["threads"],
    memory_limit=PARAMS_SLURM["memory_limit"]
)
    logging.info("Starting derived variable calculations for reanalysis-era5-single-levels")
    dataset="reanalysis-era5-single-levels"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    variables_file_path = os.path.join(script_dir, "..", "..", "requests", f"{dataset}.csv")
    df_parameters = pd.read_csv(variables_file_path)
    derived_dependencies = load_derived_dependencies()
    native_derived_condition = (df_parameters['product_type'] == 'derived') & (df_parameters['interpolation'] == 'native')
    derived_variables = df_parameters[native_derived_condition]['filename_variable']
    derived_variables_list = derived_variables.tolist()
    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        mask_var = (df_parameters['filename_variable'] == var) & (native_derived_condition)
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
                logging.info(f"Derived variable {var} has dependencies: {dependencies}")
                if var == "sfcwind" and var_row["temporal_resolution"]=="daily":
                        resampling={"agg_freq": "1D", "agg_func": "mean"}                        
                        process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        operations.sfcwind_from_u_v,
                        raw_condition,
                        resampling=resampling
                    )
                elif var == "sfcwind" and var_row["temporal_resolution"]=="hourly":
                    for month in MONTH_LIST:
                        process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        operations.sfcwind_from_u_v,
                        raw_condition,
                        month=month
                    )
                elif var == "hurs" and var_row["temporal_resolution"]=="hourly":
                    for month in MONTH_LIST:
                        process_derived(
                            var,
                            dataset,
                            dependencies,
                            df_parameters,
                            var_row,
                            year,
                            operations.rh_from_thermofeel,
                            raw_condition,
                            month
                        )
                elif var == "rsus" and var_row["temporal_resolution"]=="hourly":
                    process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        operations.rsus_from_rsds_rsns,
                        raw_condition,
                    )
                elif var == "rlus" and var_row["temporal_resolution"]=="hourly":
                    process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        operations.rlus_from_rlds_rlns,
                        raw_condition,
                    )
                elif var == "mrt" and var_row["temporal_resolution"]=="hourly":
                    start_time = time.time()
                    process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        operations.mrt_from_rsus_rlus_rsds_rlds,
                        [derived_condition, derived_condition, raw_condition, raw_condition],
                    )
                    end_time = time.time()
                    logging.info(f"Processing time for {var} in year {year}: {end_time - start_time} seconds")
                elif var == "utci" and var_row["temporal_resolution"]=="hourly":
                    #timestamps for checking how fast is the processing
                    start_time = time.time()
                    print(start_time)
                    process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        operations.utci_from_t2m_sfcwind_hurs_mrt,
                        [raw_condition, derived_condition, derived_condition, derived_condition],
                    )
                    end_time = time.time()
                    print(end_time)
                    logging.info(f"Processing time for {var} in year {year}: {end_time - start_time} seconds")
                else:
                    raise ValueError(f"Unexpected variable {var} with temporal resolution {var_row['temporal_resolution']}. Check configuration and if processing logic is implemented for this case.")
if __name__ == "__main__":
    main()