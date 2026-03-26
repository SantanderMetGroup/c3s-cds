import datetime
import warnings
import json
from pathlib import Path
import cdsapi
import xarray as xr
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import zipfile
import os
from derived_variable_dependencies import VARIABLE_DEPENDENCIES, dataset_variable_mapping
from c3s_atlas.utils import (
    extract_zip_and_delete
)
# Configure logging
import logging

# Configure logger if not already configured
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")
def get_original_var(dataset_name, var_name):
    """
    Get the original variable name for a dataset, logging info if missing.

    Returns the mapped output if it exists, otherwise the variable name itself.
    """
    dataset_dict = dataset_variable_mapping.get(dataset_name)
    if dataset_dict is None:
        logger.info(f"Dataset '{dataset_name}' not found in mapping. Returning variable name '{var_name}'.")
        return var_name

    output = dataset_dict.get(var_name)
    if output is None:
        logger.info(f"Variable '{var_name}' not found in dataset '{dataset_name}'. Returning variable name '{var_name}'.")
        return var_name
    logger.info(f"Mapping variable '{var_name}' to original variable '{output}' for dataset '{dataset_name}'.")
    return output

def load_derived_dependencies() -> dict:
    """
    Load shared derived-variable dependencies from the global variable.
    """
    return VARIABLE_DEPENDENCIES


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
        Type of product: 'raw' or 'derived'
        - 'raw': Data downloaded directly from CDS
        - 'derived': Calculated or interpolated data
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


def load_output_path_from_row(row, dataset=None):
    """
    Load the output path from a CSV row.
    
    Parameters
    ----------
    row : pandas.Series
        Row from the CSV file containing the variable information
    dataset : str, optional
        Dataset name. If not provided, will use row['dataset']
    
    Returns
    -------
    Path
        Full output path for the data
    """
    if dataset is None:
        dataset = row['dataset']
    
    return build_output_path(
        row['output_path'],
        dataset,
        row['product_type'],
        row['temporal_resolution'],
        row['interpolation'],
        row['filename_variable']
    )


def load_input_path_from_row(row, dataset=None, product_type='raw', interpolation='native'):
    """
    Load the input path from a CSV row.
    
    For derived/interpolated data, this typically points to the raw data source.
    
    Parameters
    ----------
    row : pandas.Series
        Row from the CSV file containing the variable information
    dataset : str, optional
        Dataset name. If not provided, will use row['dataset']
    product_type : str, optional
        Product type for input data (default: 'raw')
    interpolation : str, optional
        Interpolation method for input data (default: 'native')
    
    Returns
    -------
    Path
        Full input path for the data
    """
    if dataset is None:
        dataset = row['dataset']
    
    return build_output_path(
        row['input_path'],
        dataset,
        product_type,
        row['temporal_resolution'],
        interpolation,
        row['filename_variable']
    )


def load_path_from_df(df, variable_name, variable_column='filename_variable', 
                      path_column='input_path', product_type='raw', dataset=None):
    """
    Load the path for a given variable from a DataFrame.
    
    This function searches for a variable in the DataFrame and constructs
    the full path based on the directory structure.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the variable information
    variable_name : str
        The variable name to search for
    variable_column : str, optional
        Column name containing variable names (default: 'filename_variable')
    path_column : str, optional
        Column name containing base paths (default: 'input_path')
    product_type : str, optional
        Product type to filter by (default: 'raw')
    dataset : str, optional
        Dataset name. If not provided, will use the dataset from the row
    
    Returns
    -------
    str or None
        Full path as string, or None if variable not found
    """
    # Filter the DataFrame to find the row with the specified variable
    filtered_df = df[(df[variable_column] == variable_name) & (df['product_type'] == product_type)]
    
    # Check if any row matches the variable
    if not filtered_df.empty:
        row = filtered_df.iloc[0]
        if dataset is None:
            dataset = row['dataset']
        
        base_path = row[path_column]
        temporal_resolution = row['temporal_resolution']
        interpolation = row['interpolation']
        
        # Build path: {base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
        full_path = Path(base_path) / product_type / dataset / temporal_resolution / interpolation / variable_name
        return str(full_path)
    else:
        # Return None if no matching variable is found
        return None

def is_valid_netcdf(path_file: Path) -> bool:
    """Check if a NetCDF file is valid and can be opened."""
    try:
        with xr.open_dataset(path_file) as ds:
            # Optionally, do a quick check to ensure there is data
            if ds.dims:
                return True
            else:
                logging.warning(f"{path_file} opened but has no dimensions.")
                return False
    except Exception as e:
        logging.warning(f"Failed to open {path_file}: {e}")
        return False


def handle_special_zip(zip_path, delete_zip=False):
    """
    Extract zip files and keep the files generated by the zip as-is.
    Does NOT rename extracted .nc files to match the zip name.
    Non-.nc files extracted alongside will be removed.

    Parameters
    ----------
    zip_path : pathlib.Path or str
        Path to the downloaded zip file
    delete_zip : bool
        If True, delete the original zip after extraction
    """

    zip_path = Path(zip_path)

    # Only proceed if the file is actually a zip archive. This detects zip
    # archives by signature, so it works even when the filename ends with
    # .nc (some providers return zipped content with a .nc filename).

    if not zipfile.is_zipfile(zip_path):
        logging.info(f"File {zip_path} is not a zip archive; nothing to extract")
        return



    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        names = zip_ref.namelist()
        zip_ref.extractall(zip_path.parent)
        logging.info(f"Extracted {zip_path} to {zip_path.parent}")

        for name in names:
            logging.info(f"Processing extracted file: {name}")
            extracted_path = zip_path.parent / name

            # Keep directories and netCDF files; remove other auxiliary files
            # that sometimes come inside downloads (e.g., README, .xml).
            if extracted_path.is_dir() or extracted_path.suffix == ".nc":
                continue
            try:
                if extracted_path.exists():
                    os.remove(extracted_path)
            except Exception as e:
                logging.warning(f"Could not remove extracted file {extracted_path}: {e}")


    if delete_zip:
        try:
            zip_path.unlink()
        except Exception as e:
            logging.warning(f"Could not delete zip file {zip_path}: {e}")




def check_file_exists(path_file, executor, dataset, request, futures=[], multinetcdf_zip=None):
    """
    Check for an existing valid NetCDF file corresponding to path_file, and if missing or invalid,
    schedule download and/or post-processing tasks on the provided executor.

    Parameters
    ----------
    path_file : pathlib.Path or str
        Path to the target file. If a .zip path is provided, the function uses a corresponding
        .nc path (by replacing 'zip' with 'nc') to check for an already extracted NetCDF file.
    executor : concurrent.futures.Executor
        Executor used to schedule asynchronous tasks (e.g., ThreadPoolExecutor or ProcessPoolExecutor).
    dataset : object
        Dataset descriptor passed to download_single_file; shape and contents depend on the calling code.
    request : object
        Request descriptor passed to download_single_file; shape and contents depend on the calling code.
    futures : list, optional
        Mutable list to which newly scheduled concurrent.futures.Future objects will be appended.
    multinetcdf_zip : bool or None, optional
        Controls how .zip files are handled:
    Returns
    -------
    bool or None
        - True if an existing corresponding NetCDF file was found and validated successfully.
        - None in all other cases (tasks are scheduled via executor and futures are appended).

    """
    if multinetcdf_zip:
        if Path(str(path_file)).exists():
            return True
        else:
            logging.warning(f"{path_file} exists but is corrupt, redownloading")
    elif Path(str(path_file).replace('zip','nc')).exists():
        if is_valid_netcdf(Path(str(path_file).replace('zip','nc'))):
            logging.info(f"{path_file} already exists and is valid, skipping")
            return True
        else:
            logging.warning(f"{path_file} exists but is corrupt, redownloading")

    if path_file.suffix == '.zip':
        # evaluate the variable settings_file to determine how to handle the zip file
        if multinetcdf_zip:
            logging.info("********************************")
            logging.info(f"{path_file} is a zip containing multiple NetCDF files, scheduling download without extraction")
            futures.append(executor.submit(handle_special_zip, path_file))
        elif not multinetcdf_zip:
            futures.append(executor.submit(extract_zip_and_delete, path_file))            
    else:
        futures.append(executor.submit(download_single_file, dataset, request, path_file))


            


def download_files(dataset, variables_file_path, create_request_func, get_output_filename_func, monthly_request=False,year_request=True):
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
        if "is_multinetcdf_zip" in df_parameters.columns:
            val = row["is_multinetcdf_zip"]
            is_multinetcdf_zip = None if pd.isna(val) else bool(val)
        else:
            is_multinetcdf_zip = None

        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = []
            if monthly_request:
                month_list = [f"{month:02d}" for month in range(1, 13)]
                for year in year_list:
                    for month in  month_list:
                        request = create_request_func(row, year,month)
                        file = get_output_filename_func(row, dataset, year,month)
                        path_file = dest_dir / file
                        
                        check_file_exists(
                            path_file,
                            executor,
                            dataset,
                            request,
                            futures,
                            is_multinetcdf_zip
                        )


            elif year_request:
                for year in year_list:
                    request = create_request_func(row, year)
                    file = get_output_filename_func(row, dataset, year)
                    path_file = dest_dir / file

                    check_file_exists(
                        path_file,
                        executor,
                        dataset,
                        request,
                        futures,
                        is_multinetcdf_zip
                    )
                    
            else:
                request = create_request_func(row)
                file = get_output_filename_func(row, dataset)
                path_file = dest_dir / file
                
                check_file_exists(
                    path_file,
                    executor,
                    dataset,
                    request,
                    futures,
                    is_multinetcdf_zip
                )


            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Failed to download file: {e}")

def require_single_row(df, mask, desc=None):
    """
    Select a single row from a DataFrame that matches a given mask.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to search.
    mask : pandas.Series or array-like of bool
        Boolean mask used to filter rows in `df`.   
    desc : str, optional
        Description to include in the error message if no row or multiple rows are found.

    Returns
    -------
    pandas.Series
        The single row from `df` that matches the mask.

    Raises
    ------
    KeyError
        If no row matches the mask.
    ValueError
        If more than one row matches the mask.

    """
    matches = df[mask]
    if matches.shape[0] == 0:
        raise KeyError(f"No row found{': ' + desc if desc else ''}")
    if matches.shape[0] > 1:
        raise ValueError(f"{matches.shape[0]} rows found{': ' + desc if desc else ''} — expected exactly 1")
    return matches.iloc[0]