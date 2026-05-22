
import xarray as xr
import numpy as np
import logging
from thermofeel.thermofeel import calculate_relative_humidity_percent
from xclim.indicators.convert import specific_humidity_from_dewpoint, mean_radiant_temperature, universal_thermal_climate_index

logger = logging.getLogger(__name__)

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

    return specific_humidity_from_dewpoint(tdps=tdps['d2m'], ps=ps['ps'],method="tetens30")

def sfcwind_from_u_v(ds_u10: xr.Dataset, ds_v10: xr.Dataset) -> xr.Dataset:
    """Calculate wind speed from components u and v."""
    if "u10" not in ds_u10:
        raise KeyError("Variable 'u10' not found in dataset.")
    if "v10" not in ds_v10:
        raise KeyError("Variable 'v10' not found in dataset.")

    sfcwind = np.hypot(ds_u10["u10"], ds_v10["v10"])
    sfcwind.attrs["units"] = ds_u10["u10"].attrs.get("units")

    return xr.Dataset({"sfcwind": sfcwind})



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

def _to_radiation_flux(da: xr.DataArray) -> xr.DataArray:
    """Convert J m-2 to W m-2 (÷3600 for hourly data)."""
    units = da.attrs.get("units", "")
    if "W m-2" in units:
        return da
    out = da / 3600
    out.attrs["units"] = "W m-2"
    return out


def determine_solar_time_shift(ds: xr.Dataset, radiation_vars: list[str]) -> np.timedelta64:
    """
    Inspects ERA5/CDS GRIB metadata attributes across multiple variables to automatically
    determine if an accumulation time shift (-1 hour) is required.
    """
    is_accumulated = False
    detected_vars = []
    non_accumulated_vars = []
    for var in radiation_vars:
        if var in ds.data_vars:
            da = ds[var]
            grib_step_type = str(da.attrs.get("GRIB_stepType", "")).lower()
            grib_data_type = str(da.attrs.get("GRIB_dataType", "")).lower()
            cell_methods = str(da.attrs.get("cell_methods", "")).lower()
            long_name = str(da.attrs.get("long_name", "")).lower()
            if not any([grib_step_type, grib_data_type, cell_methods, long_name]):
                logger.warning(f"No relevant metadata found for variable '{var}' to determine accumulation.")
                continue
            # Check if this specific variable is an accumulation
            if (
            grib_step_type == "accum" or 
            "accum" in cell_methods or 
            "accumulation" in long_name or
            grib_data_type == "accum" or
            var in ["ssrd", "strd", "ssr", "str"] # Safe fallbacks for classic shortnames
            ):
                is_accumulated = True
                detected_vars.append(var)
            else:
                non_accumulated_vars.append(var)
    if non_accumulated_vars and detected_vars:
        raise ValueError(f"Mixed accumulation types detected. Accumulated: {detected_vars}, Non-accumulated: {non_accumulated_vars}. Check dataset metadata for consistency.")
    if is_accumulated:
        logger.info(f"Accumulation detected in variables: {detected_vars}")
        logger.info("Action: Applying -1 hour shift for geometric alignment.")
        return -np.timedelta64(1, "h")
        
    logger.info("No accumulated variables detected. No shift needed.")
    return np.timedelta64(0, "h")
def mrt_from_rsus_rlus_rsds_rlds(
    ds_rsus: xr.Dataset, 
    ds_rlus: xr.Dataset, 
    ds_rsds: xr.Dataset, 
    ds_rlds: xr.Dataset,
) -> xr.Dataset:
    """
    Efficiently merges ERA5 radiation datasets, handles necessary accumulation 
    time shifts natively across all variables, computes MRT, and restores original timestamps.
    """
    # 1. Merge all variables into a single unified dataset for efficient graph calculation
    # xr.merge automatically handles coordinate alignments
    rad_vars = ["rsus", "rlus", "rsds", "rlds"]
    ds_combined = xr.merge([ds_rsus, ds_rlus, ds_rsds, ds_rlds])
    
    for var in rad_vars:
        if var not in ds_combined.data_vars:
            raise KeyError(f"Required variable '{var}' missing from inputs.")

    # 2. Automatically determine the time shift required based on metadata
    time_shift = determine_solar_time_shift(ds_combined, rad_vars)
    
    # 3. Apply the time shift BEFORE calculation if necessary (-1 hour)
    if time_shift != np.timedelta64(0, "h"):
        logger.info(f"Applying time shift of {time_shift} to all radiation variables for alignment.")
        ds_combined = ds_combined.assign_coords(time=ds_combined.time + time_shift)

    # 4. Extract and normalize radiation fluxes
    rsus = _to_radiation_flux(ds_combined["rsus"])
    rlus = _to_radiation_flux(ds_combined["rlus"])
    rsds = _to_radiation_flux(ds_combined["rsds"])
    rlds = _to_radiation_flux(ds_combined["rlds"])
    
    # Unify chunks within the combined dataset to prevent Dask fragmentation
    rsds, rlds, rsus, rlus = xr.unify_chunks(rsds, rlds, rsus, rlus)
    
    logger.info("Chunk structure unified:")
    logger.info(f"rsds chunks: {rsds.chunks}")

    # 5. Compute mean radiant temperature
    mrt = mean_radiant_temperature(rsus=rsus, rlus=rlus, rsds=rsds, rlds=rlds, stat="sunlit")
    
    # Build output dataset
    ds_out = xr.Dataset()
    ds_out["mrt"] = mrt
    ds_out["mrt"].attrs["units"] = "K"
    
    # 6. Apply the reverse time shift AFTER calculation to match original ERA5 time (+1 hour)
    if time_shift != np.timedelta64(0, "h"):
        logger.info(f"Reversing time shift by applying +{abs(time_shift)} to output MRT.")
        ds_out = ds_out.assign_coords(time=ds_out.time - time_shift)
        
    return ds_out

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
        sfcWind=ds_sfcwind["sfcwind"],
        hurs=ds_hurs["hurs"],
        mrt=ds_mrt["mrt"],
        mask_invalid=False,
    )
    ds = xr.Dataset()
    ds["utci"] = utci
    ds["utci"].attrs["units"] = "K"
    return ds

