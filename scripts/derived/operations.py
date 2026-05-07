
import xarray as xr
import numpy as np
from thermofeel.thermofeel import calculate_relative_humidity_percent
from xclim.indicators.convert import specific_humidity_from_dewpoint

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
    if "d2m" not in ds_td.data_vars:
        raise KeyError(f"Variable 'd2m' not found in dataset.")
    if "t2m" not in ds_t2.data_vars:
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
    if "d2m" not in tdps.data_vars:
        raise KeyError(f"Variable 'd2m' not found in dataset.")
    if "ps" not in ps.data_vars:
        raise KeyError(f"Variable 'ps' not found in dataset.")

    return specific_humidity_from_dewpoint(tdps=tdps['d2m'], ps=ps['ps'],method="buck81")

def sfcwind_from_u_v(ds_u10: xr.Dataset, ds_v10: xr.Dataset) -> xr.Dataset:
    """Calculate wind speed from components u and v."""
    if "u10" not in ds_u10.data_vars:
        raise KeyError(f"Variable 'u10' not found in dataset.")
    if "v10" not in ds_v10.data_vars:
        raise KeyError(f"Variable 'v10' not found in dataset.")
    sfcwind = np.power(np.power(ds_u10["u10"], 2) + np.power(ds_v10["v10"], 2), 0.5)
    ds = xr.Dataset()
    ds["sfcwind"] = sfcwind
    ds["sfcwind"].attrs["units"] = ds_u10["u10"].attrs["units"]
    return ds



