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

def download_file(catalogue_id: str, catalogue_entry: dict, output_path: Path) -> Path:
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
    return output_path





