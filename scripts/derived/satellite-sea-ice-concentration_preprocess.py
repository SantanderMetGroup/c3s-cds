import xarray as xr
import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
from datetime import datetime
from utils import load_output_path_from_row, require_single_row
import logging
from logging_utils import setup_logging
logger = logging.getLogger(__name__)


def restore_low_sea_ice_concentration(
    ds: xr.Dataset,
    threshold: float = 10,
) -> xr.Dataset:
    """
    Restore sea-ice concentration values below the filtering threshold
    using the raw sea-ice concentration variable.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset containing `ice_conc` and `raw_ice_conc`.
    threshold : float, optional
        Threshold below which `ice_conc` is replaced by `raw_ice_conc`.
        Default is 10 (for concentrations expressed as percentages).

    Returns
    -------
    xr.Dataset
        Dataset with corrected `ice_conc`.
    """
    ds = ds.copy()

    ds["ice_conc"] = xr.where(
        ds["ice_conc"] >= threshold,
        ds["ice_conc"],
        ds["raw_ice_conc"],
    )

    return ds

if __name__ == "__main__":
    setup_logging()
    dataset="reanalysis-cerra-land"
    variables_file_path = f"../../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)
    derived_variables = df_parameters[df_parameters['product_type'] == 'derived']['filename_variable']
    derived_variables_list = derived_variables.tolist()
    logger.info(f"List of derived variables: {derived_variables_list}")

    for var in derived_variables_list:
        logger.info(f"Calculating {var}")
        mask_input = (df_parameters['filename_variable'] == var) & (df_parameters['product_type'] == 'raw')
        input_row = require_single_row(df_parameters, mask_input, f"{var}/raw")

        mask_var = (df_parameters['filename_variable'] == var) & (df_parameters['product_type'] == 'derived')
        var_row = require_single_row(df_parameters, mask_var, f"{var}/derived")
        # Use utility function to load input path
        var_download_path = load_output_path_from_row(input_row, dataset)
        var_files = np.sort(glob.glob(f"{var_download_path}/*.nc"))
        logger.info(f"{var_download_path}/*.nc")
        logger.info(f"List of file variables: {var_files}")
        # Iterate over files and process accumulation, need two files for last hour accumulation
        for i,file in enumerate(var_files):
            basename = os.path.basename(file)
            logger.info(basename)
            date_str = basename.split('_')[-1].replace(".nc","")  
            date_obj = datetime.strptime(date_str, "%Y%m")
            year = date_obj.year
            logger.info(f"Processing year: {year} and end year: {var_row.cds_years_end}")
            if year > var_row.cds_years_end:
                logger.info("Skipping file as it is after the end year")
                continue
            dest_dir = load_output_path_from_row(var_row, dataset)
            var_file = os.path.basename(file).replace(".nc", "_daily_accumulated.nc")
            output_file=Path(f"{dest_dir}/{var_file}")
            logger.info(f"Saving calculated {var} to {dest_dir}")
            os.makedirs(dest_dir, exist_ok=True)   
            if output_file.exists():
                logger.info(f"File {output_file} already exists. Skipping...")
                continue