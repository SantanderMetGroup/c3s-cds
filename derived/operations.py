import numpy as np
from pathlib import Path


def sfcwind_from_u_v(ds):
    """Calculate wind speed from components u and v."""
    sfcwind = np.power(np.power(ds["u10"], 2) + np.power(ds["v10"], 2), 0.5)
    ds["sfcwind"] = sfcwind
    ds = ds.drop_vars(["u10", "v10"])
    return ds


def resample_to_daily(ds, time_dim='time', agg_freq='1D', agg_func='mean'):
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

    return resampled


def build_output_path(base_path, dataset, product_type, temporal_resolution, interpolation, variable):
    """
    Build the output path following the directory structure.
    
    Structure: {base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
    
    Parameters
    ----------
    base_path : str or Path
        Base directory path
    dataset : str
        Dataset name
    product_type : str
        Type of product: 'raw' or 'derived'
    temporal_resolution : str
        Temporal resolution
    interpolation : str
        Interpolation method
    variable : str
        Variable name
    
    Returns
    -------
    Path
        Full output path
    """
    return Path(base_path) / product_type / dataset / temporal_resolution / interpolation / variable


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