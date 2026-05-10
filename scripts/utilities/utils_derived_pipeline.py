import warnings
import glob
from pathlib import Path
import xarray as xr
import logging
import os
from derived_variable_dependencies import dataset_variable_mapping
from utils_fixes import fix_dataset
from utils import load_output_path_from_row, require_single_row, is_valid_netcdf
# Configure logging
import logging

# Configure logger if not already configured
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")

def resample_dataset(ds, time_dim='time', agg_freq='1D', agg_func='mean'):
    """
    Resample the dataset to daily values.

    Parameters:
    - ds: xarray DataFrame containing the time series data.
    - agg_freq: The frequency for resampling (default is '1D' for daily).
    - agg_func: The aggregation function to apply ('mean', 'sum', 'max', 'min').

    Returns:
    - A resampled xarray DataFrame.
    """

    # Choose the aggregation function
    if agg_func == 'mean':
         resampled = ds.resample({time_dim: agg_freq}).mean(dim=time_dim)
    elif agg_func == 'sum':
        resampled = ds.resample({time_dim: agg_freq}).sum(dim=time_dim)
    elif agg_func == 'max':
        resampled = ds.resample({time_dim: agg_freq}).max(dim=time_dim)
    elif agg_func == 'min':
        resampled = ds.resample({time_dim: agg_freq}).min(dim=time_dim)
    else:
        raise ValueError("Invalid aggregation function. Choose 'mean', 'sum', 'max', or 'min'.")
    logger.info(f"Resampled dataset using {agg_func} with frequency {agg_freq}")

    return resampled


def get_original_var(dataset_name, var_name):
    """
    Get the original variable name for a dataset, logging info if missing.

    Returns the mapped output if it exists, otherwise the variable name itself.
    """
    dataset_dict = dataset_variable_mapping.get(dataset_name)
    if dataset_dict is None:
        logger.info(f"Dataset mapping '{dataset_name}' not found. Returning variable name '{var_name}'.")
        return var_name

    output = dataset_dict.get(var_name)
    if output is None:
        logger.info(f"Variable '{var_name}' not found in dataset mapping '{dataset_name}'. Returning variable name '{var_name}'.")
        return var_name
    logger.info(f"Mapping variable '{var_name}' to original variable '{output}' for dataset mapping '{dataset_name}'.")
    return output

def normalize_var_names(ds, dataset_name):
    """
    Rename dataset-specific variable names to standard names using
    the dataset variable mapping.
    """
    mapping = dataset_variable_mapping.get(dataset_name, {})

    rename_dict = {
        original: standard
        for standard, original in mapping.items()
        if original in ds.data_vars
    }

    return ds.rename(rename_dict)

def load_files(
    dataset_name,
    dependencies,
    df_parameters,
    condition_func,
    year,
):
    """
    Resolve and load file paths for the required dependency variables.

    This function maps standardized dependency names to dataset-specific
    variable names, retrieves parameter rows, and resolves corresponding
    NetCDF file paths for a given year (and optionally month) based n the df conditon function in input.
    """
    # Resolve original variable names
    original_vars = [
        get_original_var(dataset_name, dep)
        for dep in dependencies
    ]

    # Fetch parameter rows
    input_rows = [
        require_single_row(
            df_parameters,
            *condition_func(df_parameters, orig_var, dep)
        )
        for orig_var, dep in zip(original_vars, dependencies)
    ]

    # Get download paths
    download_paths = [
        load_output_path_from_row(row, dataset_name)
        for row in input_rows
    ]

    # Resolve files
    files = []
    for path in download_paths:
        matches = glob.glob(f"{path}/*{year}*.nc")
        if not matches:
            raise FileNotFoundError(f"No files found in {path} for {year}")
        files.append(matches[0])

    logger.info(f"Resolved files for dependencies: {files}")    
    return files, original_vars


def load_and_fix_datasets(
    files,
    dataset_name,
    year,
    month=None,
    chunks={"time": 744}
):
    """
    Load NetCDF files into xarray datasets and apply standard preprocessing.

    This includes structure fixing, optional time filtering, and variable
    name normalization to standard dependency names.
    """

    datasets = [
        xr.open_dataset(f, chunks=chunks)
        for f in files
    ]

    # Fix structure
    datasets = [fix_dataset(ds) for ds in datasets]

    # Time selection
    if month:
        time_sel = f"{year}-{month}"
    else:
        time_sel = f"{year}"
    datasets = [
            ds.sel(time=time_sel)
            for ds in datasets
        ]

    # Normalize variable names
    datasets = [
        normalize_var_names(ds, dataset_name)
        for ds in datasets
    ]

    return datasets

def validate_and_build_inputs(datasets, dependencies):
    """
    Validate datasets and build ordered inputs matching dependency order.

    Ensures each dataset contains exactly one variable, checks for duplicates,
    and returns DataArrays ordered according to the required dependencies.
    """
    datasets_by_var = {}

    for ds in datasets:
        if len(ds.data_vars) != 1:
            raise ValueError("Dataset must contain exactly one variable")

        ds_var = list(ds.data_vars)[0]

        if ds_var in datasets_by_var:
            raise ValueError(f"Duplicate variable detected: {ds_var}")

        datasets_by_var[ds_var] = ds

    # Validate dependencies
    missing = [d for d in dependencies if d not in datasets_by_var]
    if missing:
        raise ValueError(f"Missing dependencies: {missing}")

    # Build ordered inputs
    inputs = [
        datasets_by_var[dep]
        for dep in dependencies
    ]

    return inputs

def build_output_path(
    var,
    dataset_name,
    var_row,
    files,
    original_vars,
    year,
    month=None
):
    dest_dir = load_output_path_from_row(var_row, dataset_name)
    os.makedirs(dest_dir, exist_ok=True)

    base_file = os.path.basename(files[0])
    var_file = base_file.replace(original_vars[0], var)

    output_file = Path(dest_dir) / var_file

    if month:
        output_file = Path(
            str(output_file).replace(str(year), f"{year}{month}")
        )

    return output_file, dest_dir
def resolve_output_file(output_file):
    if output_file.exists():
        nc_file = Path(str(output_file).replace("zip", "nc"))

        if is_valid_netcdf(nc_file):
            return True

    return False
def process_derived(
    var,
    dataset_name,
    dependencies,
    df_parameters,
    var_row,
    year,
    function,
    condition_func,
    month=None,
    parallel=False,
    resampling=None,
):
    """
    Execute a full derived climate variable computation pipeline.

    This function:
    - Resolves dataset-specific file paths for required dependencies
    - Loads and preprocesses NetCDF datasets
    - Normalizes variable names to standard dependency names
    - Validates and orders inputs according to dependency specification
    - Computes the derived variable using the provided function
    - Saves the result to disk if not already computed

    Parameters
    ----------
    var : str
        Name of the derived variable to compute (output variable name).
    dataset_name : str
        Identifier of the dataset used for variable mapping and paths.
    dependencies : list of str
        List of required input variables in dependency order.
    df_parameters : pandas.DataFrame
        Metadata table used to resolve file locations.
    var_row : pandas.Series or dict
        Output configuration row used to determine save location.
    year : int or str
        Year of data to process.
    function : callable
        Function that computes the derived variable from input datasets.
    condition_func : callable
        Function used to filter parameter rows for dependency resolution.
    month : int, optional
        If provided, restricts processing to a specific month.
    parallel : bool, optional
        Reserved for future parallel execution support.
    resampling : dict, optional
        If provided, resample the time dimension of the result (e.g., {"time": "1ME"} for monthly mean).

    Returns
    -------
    bool
        True if computation was completed or skipped due to existing valid output.
    """
    # Load files
    files, original_vars = load_files(
        dataset_name,
        dependencies,
        df_parameters,
        condition_func,
        year,
    )

    # ------------------------------------------------------------
    # Output (single call)
    # ------------------------------------------------------------
    output_file, dest_dir = build_output_path(
        var,
        dataset_name,
        var_row,
        files,
        original_vars,
        year,
        month
    )

    logging.info(f"output_file: {output_file}")

    if resolve_output_file(output_file):
        logging.info(f"{output_file} already exists and is valid, skipping")
        return True

    logging.info(f"Calculating {var} from {files}")

    # Load + fix datasets
    datasets = load_and_fix_datasets(
        files,
        dataset_name,
        year,
        month
    )

    # Validate + prepare inputs
    inputs = validate_and_build_inputs(
        datasets,
        dependencies
    )

    # Compute
    result = function(*inputs)

    # Resample if needed
    if resampling:
        result = resample_dataset(result, time_dim="time", agg_freq=resampling["agg_freq"], agg_func=resampling["agg_func"])

    # Save
    logging.info(f"Saving calculated {var} to {dest_dir}")
    result.to_netcdf(output_file)

    # Cleanup
    for ds in datasets:
        ds.close()
    result.close()

    return True
