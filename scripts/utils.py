import datetime
import warnings
from pathlib import Path
import cdsapi
import os
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
warnings.filterwarnings("ignore")

def read_from_yaml(file_path):
    """
    Read variables and their corresponding dataset names from a YAML file.
    Parameters
    ----------
    file_path : str
        Path to the YAML file containing the variable mappings.
    Returns
    -------
    variables : dict
        A dictionary mapping variable names to dataset variable names.
    """
    with open(file_path, 'r') as file:
        variables = yaml.safe_load(file)
    return variables

def download_single_file(catalogue_id: str, catalogue_entry: dict, output_path: Path) -> Path:
    """
    Download a file from a given catalogue ID with the given parameters.
    This method retrieves the file from the CDS API, saves it in a temporary
    directory, and returns the `Path` object pointing to the downloaded file.
    Parameters
    ----------
    catalogue_id : str
        A string containing the CDS catalogue id. For instance: projections-cmip6.
    catalogue_entry: dict
        A dictionary containing the fields of the requested data.
    output_path: Path
        Path associated with the request params.
    Returns
    -------
    output_path: Path
        A `Path` object pointing to the downloaded file.
    Raises
    ------
    Exception
        If the download request failed.
    """
    start_time = datetime.datetime.now()
    c = cdsapi.Client(timeout=500, quiet=True)
    logging.info(f"Downloading the data from {catalogue_id} with parameters {catalogue_entry}")
    r = c.retrieve(catalogue_id, catalogue_entry)
    r.download(output_path)
    end_time = datetime.datetime.now()
    final_time = end_time - start_time
    logging.info(f"Duration of the process to download data: {final_time}")
    logging.info(f"Saving to {output_path}")
    return output_path





def build_output_path(base_path, dataset, product_type, temporal_resolution, interpolation, variable):
    """
    Build the output path following the new directory structure.
    
    Structure: {base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
    
    Parameters
    ----------
    base_path : str or Path
        Base directory path
    dataset : str
        Dataset name (e.g., 'reanalysis-era5-single-levels')
    product_type : str
        Type of product: 'raw', 'derived', or 'interpolated'
    temporal_resolution : str
        Temporal resolution: 'hourly', 'daily', '3hourly', '6hourly', 'monthly', etc.
    interpolation : str
        Interpolation method: 'native' (for non-interpolated), 'gr006', etc.
    variable : str
        Variable name (e.g., 'u10', 'v10', 'sfcwind')
    
    Returns
    -------
    Path
        Full output path
    """
    return Path(base_path) / product_type / dataset / temporal_resolution / interpolation / variable

def download_files(dataset, variables_file_path, create_request_func, get_output_filename_func, monthly_request=False):
    """
    Download files for the specified variables and years.

    Parameters
    ----------
    dataset : str
        The dataset name.
    variables_file_path : str
        Path to the CSV file containing the variables and other parameters.
    create_request_func : function
        Function to create the request dictionary.
    get_output_filename_func : function
        Function to get the output filename.
    monthly_request : bool
        Whether to make monthly requests instead of yearly requests.
    """
    df_parameters = pd.read_csv(variables_file_path)
    for index, row in df_parameters.iterrows():
        if row["product_type"] != "raw":
            continue
        dest_dir = build_output_path(
            row["output_path"], 
            dataset, 
            row["product_type"],
            row["temporal_resolution"],
            row["interpolation"],
            row["filename_variable"]
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        year_list = list(range(row["cds_years_start"], row["cds_years_end"] + 1))

        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = []
            if monthly_request:
                month_list = [f"{month:02d}" for month in range(1, 13)]
                for year in year_list:
                    for month in  month_list:
                        request = create_request_func(row, year,month)
                        file = get_output_filename_func(row, dataset, year,month)
                        path_file = dest_dir / file
                        if path_file.exists():
                            logging.info(f"{path_file} already exists, skipping")
                            continue
                        futures.append(executor.submit(download_single_file, dataset, request, path_file))
            else:
                for year in year_list:
                    request = create_request_func(row, year)
                    file = get_output_filename_func(row, dataset, year)
                    path_file = dest_dir / file
                    if path_file.exists():
                        logging.info(f"{path_file} already exists, skipping")
                        continue
                    futures.append(executor.submit(download_single_file, dataset, request, path_file))

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Failed to download file: {e}")