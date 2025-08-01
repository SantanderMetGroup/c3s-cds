import numpy as np


def sfcwind_from_u_v(ds):
    """Calculate wind speed from components u and v."""
    sfcwind = np.power(np.power(ds["u10"], 2) + np.power(ds["v10"], 2), 0.5)
    ds["sfcwind"] = sfcwind
    ds = ds.drop_vars(["u10", "v10"])
    return ds


def resample_to_daily(ds, agg_freq='1D', agg_func='mean'):
    """
    Resample the dataset to daily values.

    Parameters:
    - ds: pandas DataFrame containing the time series data.
    - agg_freq: The frequency for resampling (default is '1D' for daily).
    - agg_func: The aggregation function to apply ('mean', 'sum', 'max', 'min').

    Returns:
    - A resampled pandas DataFrame.
    """

    # Choose the aggregation function
    if agg_func == 'mean':
        resampled = ds.resample(agg_freq).mean()
    elif agg_func == 'sum':
        resampled = ds.resample(agg_freq).sum()
    elif agg_func == 'max':
        resampled = ds.resample(agg_freq).max()
    elif agg_func == 'min':
        resampled = ds.resample(agg_freq).min()
    else:
        raise ValueError("Invalid aggregation function. Choose 'mean', 'sum', 'max', or 'min'.")

    return resampled

def load_path_from_df(df, variable_name, variable_column='filename_variable', path_column='path_download'):
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
    filtered_df = df[df[variable_column] == variable_name]

    # Check if any row matches the variable
    if not filtered_df.empty:
        # Return the path from the first matching row
        return filtered_df[path_column].iloc[0]
    else:
        # Return None if no matching variable is found
        return None