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