import operations
import pandas as pd
import logging
import sys
sys.path.append('../utilities')
from utils import  require_single_row, load_derived_dependencies,  raw_condition
from logging_utils import setup_logging
from utils_derived_pipeline import process_derived

logger = logging.getLogger(__name__)

# =============================================================================
# Derived Variable Configuration Guide
# =============================================================================
#
# Each derived variable maps to an entry in VAR_CONFIG below.  The key is the
# variable name (string), and the value is a dict with:
#
#   func  — The operation function from operations.py.
#   cond  — Condition function used to locate the input data.
#
# Example:
#
#     "myvar": {
#         "func": operations.myvar_compute,
#         "cond": raw_condition,
#     },
# =============================================================================
VAR_CONFIG = {
    "hurs": {
        "func": operations.rh_from_thermofeel,
        "cond": raw_condition,
    },
    "huss": {
        "func": operations.sh_xclim,
        "cond": raw_condition,
    },
}


def main():
    setup_logging()
    dataset="derived-era5-single-levels-daily-statistics"
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_dependencies = load_derived_dependencies()
    native_derived_condition = (df_parameters['product_type'] == 'derived') & (df_parameters['interpolation'] == 'native')
    derived_variables = df_parameters[native_derived_condition]['filename_variable']
    derived_variables_list = derived_variables.tolist()
    logger.info(f"Derived variables to process: {derived_variables_list}")
    for var in derived_variables_list:
        logger.info(f"Calculating {var}")
        mask_var = (df_parameters['filename_variable'] == var) & (native_derived_condition)
        
        var_row = require_single_row(df_parameters, mask_var, f"{var}/derived")
        
        # Create a list of years from start to end
        year_list = list(range(var_row["cds_years_start"].squeeze() , var_row["cds_years_end"].squeeze()  + 1))
        for year in year_list:

            dependencies = derived_dependencies.get(var, [])
            logger.info(f"Derived variable {var} has dependencies: {dependencies}")
            if not dependencies:
                logger.warning(f"No dependencies declared for derived variable {var}. Skipping...")
                continue

            cfg = VAR_CONFIG.get(var)
            if cfg is None:
                logger.warning(
                    f"No configuration found for variable '{var}'. "
                    f"Add an entry to VAR_CONFIG in this file."
                )
                continue

            process_derived(
                var,
                dataset,
                dependencies,
                df_parameters,
                var_row,
                year,
                cfg["func"],
                cfg["cond"],
            )





if __name__ == "__main__":
    main()
