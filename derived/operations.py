import numpy as np


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

def load_path_from_df(df, variable_name, variable_column='filename_variable', path_column='input_path', product_type="raw"):
    """
    Load the path for a given variable from a df file.

    Parameters:
    - df
    - variable_name: The variable name to search for in the CSV.
    - variable_column: The column name in the CSV that contains variable names. Default is 'filename_variable'.
    - path_column: The column name in the CSV that contains paths. Default is 'path_download'.

    Returns:
    - The path corresponding to the variable, or None if the variable is not found.
    """

    # Filter the DataFrame to find the row with the specified variable
    filtered_df = df[(df[variable_column] == variable_name) & (df['product_type'] == product_type)]
    # Check if any row matches the variable
    if not filtered_df.empty:
        # Build the full path using the new structure
        row = filtered_df.iloc[0]
        from pathlib import Path
        base_path = row[path_column]
        dataset = row['dataset']
        temporal_resolution = row['temporal_resolution']
        interpolation = row['interpolation']
        
        # Build path: {base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
        full_path = Path(base_path) / product_type / dataset / temporal_resolution / interpolation / variable_name
        return str(full_path)
    else:
        # Return None if no matching variable is found
        return None