
import xarray as xr
import numpy as np
from thermofeel.thermofeel import calculate_relative_humidity_percent
from xclim.indicators.convert import specific_humidity_from_dewpoint, mean_radiant_temperature, universal_thermal_climate_index

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



def rsus_from_rsds_rsns(ds_rsds: xr.Dataset, ds_rsns: xr.Dataset) -> xr.Dataset:
    """Calculate surface upwelling shortwave radiation from surface downwelling and net shortwave."""
    if "rsds" not in ds_rsds.data_vars:
        raise KeyError(f"Variable 'rsds' not found in dataset.")
    if "rsns" not in ds_rsns.data_vars:
        raise KeyError(f"Variable 'rsns' not found in dataset.")
    rsus = ds_rsds["rsds"] - ds_rsns["rsns"]
    ds = xr.Dataset()
    ds["rsus"] = rsus
    ds["rsus"].attrs["units"] = ds_rsds["rsds"].attrs["units"]
    return ds

def rlus_from_rlds_rlns(ds_rlds: xr.Dataset, ds_rlns: xr.Dataset) -> xr.Dataset:
    """Calculate surface upwelling longwave radiation from surface downwelling and net longwave."""
    if "rlds" not in ds_rlds.data_vars:
        raise KeyError(f"Variable 'rlds' not found in dataset.")
    if "rlns" not in ds_rlns.data_vars:
        raise KeyError(f"Variable 'rlns' not found in dataset.")
    rlus = ds_rlds["rlds"] - ds_rlns["rlns"]
    ds = xr.Dataset()
    ds["rlus"] = rlus
    ds["rlus"].attrs["units"] = ds_rlds["rlds"].attrs["units"]
    return ds

def mrt_from_rsus_rlus_rsds_rlds(ds_rsus: xr.Dataset, ds_rlus: xr.Dataset, ds_rsds: xr.Dataset, ds_rlds: xr.Dataset) -> xr.Dataset:
    """Calculate mean radiant temperature from surface upwelling/downwelling shortwave and longwave radiation."""
    if "rsus" not in ds_rsus.data_vars:
        raise KeyError(f"Variable 'rsus' not found in dataset.")
    if "rlus" not in ds_rlus.data_vars:
        raise KeyError(f"Variable 'rlus' not found in dataset.")
    if "rsds" not in ds_rsds.data_vars:
        raise KeyError(f"Variable 'rsds' not found in dataset.")
    if "rlds" not in ds_rlds.data_vars:
        raise KeyError(f"Variable 'rlds' not found in dataset.")
    # Placeholder for actual MRT calculation using the provided variables
    mrt = mean_radiant_temperature(ds_rsus["rsus"], ds_rlus["rlus"], ds_rsds["rsds"], ds_rlds["rlds"], stat="sunlit")
    ds = xr.Dataset()
    ds["mrt"] = mrt
    ds["mrt"].attrs["units"] = "K"  # Assuming MRT is in Kelvin
    return ds

def utci_from_t2m_sfcwind_hurs_mrt(ds_t2m: xr.Dataset, ds_sfcwind: xr.Dataset, ds_hurs: xr.Dataset, ds_mrt: xr.Dataset) -> xr.Dataset:
    """Calculate UTCI from air temperature, surface wind speed, relative humidity, and mean radiant temperature."""
    if "t2m" not in ds_t2m.data_vars:
        raise KeyError(f"Variable 't2m' not found in dataset.")
    if "sfcwind" not in ds_sfcwind.data_vars:
        raise KeyError(f"Variable 'sfcwind' not found in dataset.")
    if "hurs" not in ds_hurs.data_vars:
        raise KeyError(f"Variable 'hurs' not found in dataset.")
    if "mrt" not in ds_mrt.data_vars:
        raise KeyError(f"Variable 'mrt' not found in dataset.")
    # xclim's utci expects 'tas', rename internally
    ds_t2m_renamed = ds_t2m.rename({"t2m": "tas"})
    utci = universal_thermal_climate_index(
        tas=ds_t2m_renamed["tas"],
        sfcwind=ds_sfcwind["sfcwind"],
        hurs=ds_hurs["hurs"],
        mrt=ds_mrt["mrt"],
        mask_invalid=False,
    )
    ds = xr.Dataset()
    ds["utci"] = utci
    ds["utci"].attrs["units"] = "K"
    return ds

