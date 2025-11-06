
import xarray as xr
import numpy as np
from thermofeel.thermofeel import calculate_relative_humidity_percent

# Import the original computation (the function lives in the same package)


def rh_from_thermofeel(ds: xr.Dataset, td_var: str, t2_var: str) -> xr.Dataset:
    """
    Calculate relative humidity (%) from a single xarray Dataset containing both dew-point and 2m air temperature.
    Based in https://github.com/ecmwf/thermofeel/blob/master/thermofeel/thermofeel.py#L49 and applied using xarray.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset containing both the dew-point temperature and 2m air temperature variables.
    td_var : str
        Name of the dew-point temperature variable (units: Kelvin expected).
    t2_var : str
        Name of the 2m air temperature variable (units: Kelvin expected).

    Returns
    -------
    xarray.Dataset
        A copy of the input dataset with a new DataArray "relative_humidity" (percent) added,
        and the original variables (td_var, t2_var) removed.
    """
    # Validate inputs
    if td_var not in ds:
        raise KeyError(f"Variable '{td_var}' not found in dataset.")
    if t2_var not in ds:
        raise KeyError(f"Variable '{t2_var}' not found in dataset.")

    td = ds[td_var]
    t2 = ds[t2_var]

    # Align inputs so broadcasting/coordinates are handled by xarray
    t2_a, td_a = xr.align(t2, td)

    # Use xr.apply_ufunc to apply the numpy-aware function while preserving coords and dask support
    rh_da = xr.apply_ufunc(
        calculate_relative_humidity_percent,
        t2_a,
        td_a,
        vectorize=True,
        dask="parallelized",
        output_dtypes=[float],
    )

    # Name and attributes for the output
    rh_da.name = "relative_humidity"
    rh_da.attrs["units"] = "%"
    rh_da.attrs["long_name"] = "Relative Humidity"

    # Build output dataset (copy ds so we keep coords and any ancillary variables)
    ds_out = ds.copy()

    # Add the new variable and remove the original ones
    ds_out["relative_humidity"] = rh_da
    ds_out = ds_out.drop_vars([td_var, t2_var])

    return ds_out






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