
import xarray as xr
import numpy as np
from thermofeel.thermofeel import calculate_relative_humidity_percent
from xclim.indicators.convert import specific_humidity_from_dewpoint
import xarray as xr

def copy_cf_metadata(src_ds: xr.Dataset, target_ds: xr.Dataset) -> xr.Dataset:
    """
    Copy coordinate variables and their CF-compliant metadata from a source dataset
    to a target dataset.

    Parameters
    ----------
    src_ds : xarray.Dataset
        Source dataset containing the coordinates with metadata.
    target_ds : xarray.Dataset
        Target dataset where coordinates will be copied.

    Returns
    -------
    xarray.Dataset
        Target dataset with coordinates and metadata copied from source.
    """
    coords_to_copy = list(src_ds.coords.keys())

    for coord in coords_to_copy:
        target_ds[coord] = src_ds[coord]
            # Ensure CF-compliant metadata
    return target_ds
def rh_from_thermofeel(ds_td: xr.Dataset, ds_t2: xr.Dataset) -> xr.Dataset:
    """
    Calculate relative humidity (%) from a single xarray Dataset containing both dew-point and 2m air temperature.
    Based in https://github.com/ecmwf/thermofeel/blob/master/thermofeel/thermofeel.py#L49 and applied using xarray.

    Parameters
    ----------
    ds_td : xarray.Dataset
        Dataset containing the dew-point temperature variable.
    ds_t2 : xarray.Dataset
        Dataset containing the 2m air temperature variable.


    Returns
    -------
    xarray.Dataset
        A copy of the input dataset with a new DataArray "relative_humidity" (percent) added,
        and the original variables (td_var, t2_var) removed.
    """
    # Validate inputs
    if "d2m" not in ds_td:
        raise KeyError(f"Variable 'd2m' not found in dataset.")
    if "t2m" not in ds_t2:
        raise KeyError(f"Variable 't2m' not found in dataset.")

    td = ds_td["d2m"]
    t2 = ds_t2["t2m"]

    # Align inputs so broadcasting/coordinates are handled by xarray
    t2_a, td_a = xr.align(t2, td, join="exact")

    # Use xr.apply_ufunc to apply the numpy-aware function while preserving coords and dask support
    rh_da = xr.apply_ufunc(
        calculate_relative_humidity_percent,
        t2_a,
        td_a,
        dask="parallelized",
        output_dtypes=[float],
    )
    # Ensure that RH values are within physical bounds [0, 100]
    rh_da = rh_da.clip(min=0.0, max=100.0)
    # Name and attributes for the output
    rh_da.name = "hurs"
    rh_da.attrs = {"units": "%", "long_name": "Relative Humidity"}
    # Build output dataset
    rh_ds = rh_da.to_dataset()

    # Copy CF metadata from ds_td
    rh_ds = copy_cf_metadata(ds_td, rh_ds)

    return rh_ds

def sh_xclim(tdps: xr.Dataset, ps: xr.Dataset) -> xr.Dataset:

    return specific_humidity_from_dewpoint(tdps=tdps['d2m'], ps=ps['ps'],method="buck81")

def sfcwind_from_u_v(ds):
    """Calculate wind speed from components u and v."""
    sfcwind = np.power(np.power(ds["u10"], 2) + np.power(ds["v10"], 2), 0.5)
    ds["sfcwind"] = sfcwind
    ds["sfcwind"].attrs["units"] = ds["u10"].attrs["units"]
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