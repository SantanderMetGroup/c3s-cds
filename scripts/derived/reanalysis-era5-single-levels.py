import operations
import pandas as pd
import logging
import sys
sys.path.append('../utilities')
from utils import load_derived_dependencies, raw_condition
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


def main():
    logging.info("Starting derived variable calculations for reanalysis-era5-single-levels")
    dataset="reanalysis-era5-single-levels"
    variables_file_path = f"../../requests/{dataset}.csv"
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
                if var == "hurs" and var_row["temporal_resolution"]=="hourly":
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

if __name__ == "__main__":
    main()