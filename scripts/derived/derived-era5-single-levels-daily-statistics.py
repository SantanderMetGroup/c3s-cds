import operations
import pandas as pd
import logging
import sys
sys.path.append('../utilities')
from utils import  require_single_row, load_derived_dependencies,  raw_condition
from utils_derived_pipeline import process_derived
# Remove any handlers added by imports like utils
root_logger = logging.getLogger()
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Configure logging now
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)




def main():
    dataset="derived-era5-single-levels-daily-statistics"
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_dependencies = load_derived_dependencies()
    native_derived_condition = (df_parameters['product_type'] == 'derived') & (df_parameters['interpolation'] == 'native')
    derived_variables = df_parameters[native_derived_condition]['filename_variable']
    derived_variables_list = derived_variables.tolist()
    logging.info(f"Derived variables to process: {derived_variables_list}")
    for var in derived_variables_list:
        logging.info(f"Calculating {var}")
        mask_var = (df_parameters['filename_variable'] == var) & (native_derived_condition)
        
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
                process_derived(
                                var,
                                dataset,
                                dependencies,
                                df_parameters,
                                var_row,
                                year,
                                operations.rh_from_thermofeel,
                                raw_condition
                            )
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
