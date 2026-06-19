from pathlib import Path
import glob
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd

ACCUMULATED_VARS_CERRA = ["tp"]

UNIT_CHANGE = {
    "CARRA": {"siconc": lambda x: x * 100,
              "sst": lambda x: x - 273.15,
              "strd": lambda x: x / (3600*24),
              "ssrd": lambda x: x / (3600*24),
              "tp": lambda x: x / 1000,
              "msl": lambda x: x / 100,
              }

}
# --- USER CONFIG: map variable names and dataset roots ---
PAIRS_MONTHLY_CICA = {
    # "logical name": ("CARRA_varname", "ERA5_varname")
    "sfcwind": ("si10", "sfcwind"),
    "sst": ("sst", "sst"),
    "siconc": ("siconc", "siconc"),
    "clt": ("tcc", "clt"),
    "eva": ("eva", "evspsbl"),
    "msl": ("msl", "psl"),
    "ssrd": ("ssrd", "rsds"),
    "strd": ("strd", "rlds"),
    # add more pairs...
}



PAIRS_DAILY = {
    # "logical name": ("CARRA_varname", "ERA5_varname")
    "tas": ("t2m", "t2m"),
    "tp": ("tp", "tp"),
    # add more pairs...
}
YEAR=2000
ROOTS_MONTHLY_CICA = {
    "CARRA": "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/reanalysis-pan-carra-means/monthly/native/",   # change to your CARRA root folder
    "ERA5": "/lustre/gmeteo/WORK/PROYECTOS/2022_C3S_Atlas/workflow/datasets/CICAv2/final_products/climate_index/ERA5/{var}/raw/",     # change to your ERA5 root folder
}


ROOTS_DAILY = {
    "CARRA": "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/reanalysis-pan-carra-means/daily/native/",   # change to your CARRA root folder
    "ERA5": "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/derived-era5-single-levels-daily-statistics/daily/native/{var}/",     # change to your ERA5 root folder
}
# File glob patterns (change to match your filenames)
PATTERN = {
    "CARRA": f"*{YEAR}*.nc",
    "ERA5":  f"*{YEAR}*.nc",
}

OUTDIR = Path("./compare_plots")
OUTDIR.mkdir(parents=True, exist_ok=True)


# --- helpers ---
def find_files(root: Path, pattern: str):
    print(f"Searching for files in {root + pattern}")
    files = glob.glob(str(root + pattern))
    print(f"Found {len(files)} files")
    return np.sort(files)

def _preprocess(ds):
    if "valid_time" in ds.coords and "time" not in ds.dims:
        ds = ds.drop_vars("time", errors="ignore")
        if "valid_time" not in ds.dims:
            ds = (
                ds                
                .rename({"valid_time": "time"})
                .expand_dims("time")
                .drop_vars("valid_time", errors="ignore")
            )
        if "valid_time"  in ds.dims:
            ds = ds.rename({"valid_time": "time"}).drop_vars("valid_time", errors="ignore")
            
    if "time" in ds.coords and "time" not in ds.dims and "valid_time" not in ds.coords:
        ds = ds.expand_dims("time")
    return ds
def open_multi(files, var=None):
    return xr.open_mfdataset(files, combine="by_coords", parallel=False, preprocess=_preprocess)


def unify_longitude(ds):
    """Map longitudes to -180..180 inplace (returns new ds)"""
    if "lon" not in ds.coords:
        return ds
    lon = ds.lon
    if lon.max() > 180:
        newlon = ((lon + 180) % 360) - 180
        ds = ds.assign_coords(lon=newlon)
        # sort lon dimension for correct sel
        ds = ds.sortby("lon")
    return ds


def get_carra_bbox(ds, var):
    """
    Compute bounding box (min_lon, max_lon, min_lat, max_lat) of non-NaN area
    using any-time non-NaN mask. Works with irregular dims names 'lat'/'lon'.
    """
    if var not in ds:
        # try fallback approximate variable selection
        var = list(ds.data_vars)[0]
    da = ds[var]
    # ensure lon convention consistent
    ds2 = unify_longitude(ds)
    da2 = ds2[var]
    # collapse time (if present) to find any non-NaN
    if "time" in da2.dims:
        mask = da2.notnull().any(dim="time")
    else:
        mask = da2.notnull()
    # if mask is 2D with lat/lon dims
    latname = [n for n in mask.dims if "lat" in n.lower()][0]
    lonname = [n for n in mask.dims if "lon" in n.lower()][0]
    lat_vals = ds2[latname].values
    lon_vals = ds2[lonname].values
    # find indices where mask True
    idx = np.where(mask.values)
    if idx[0].size == 0:
        # fallback to full extent
        return float(lon_vals.min()), float(lon_vals.max()), float(lat_vals.min()), float(lat_vals.max())
    # dims order may be (lat, lon) or (y,x)
    lat_idx = np.unique(idx[mask.dims.index(latname)]) if mask.dims else None
    lon_idx = np.unique(idx[mask.dims.index(lonname)]) if mask.dims else None
    # easier: compute coordinates where mask True
    true_lats = lat_vals[mask.values.any(axis=1) if mask.values.ndim==2 else mask.values]
    true_lons = lon_vals[mask.values.any(axis=0) if mask.values.ndim==2 else mask.values]
    if true_lats.size == 0 or true_lons.size == 0:
        return float(lon_vals.min()), float(lon_vals.max()), float(lat_vals.min()), float(lat_vals.max())
    return float(true_lons.min()), float(true_lons.max()), float(true_lats.min()), float(true_lats.max())


def subset_to_bbox(ds, bbox):
    ds = unify_longitude(ds)

    lonname = [n for n in ds.coords if "lon" in n.lower()][0]
    latname = [n for n in ds.coords if "lat" in n.lower()][0]

    min_lon, max_lon, min_lat, max_lat = bbox

    # longitude (usually safe after unify_longitude)
    ds = ds.sel({lonname: slice(min_lon, max_lon)})

    # latitude (handle descending vs ascending)
    lat = ds[latname]

    if lat[0] < lat[-1]:
        # ascending
        ds = ds.sel({latname: slice(min_lat, max_lat)})
    else:
        # descending (your ERA5 case)
        ds = ds.sel({latname: slice(max_lat, min_lat)})

    return ds


def lonlat_mean(da, dataset="CARRA"):
    if dataset == "CARRA":
        da_mean = da.mean(dim=["y", "x"])
    # find coordinate names
    latname = [n for n in da.coords if "lat" in n.lower()][0]
    lonname = [n for n in da.coords if "lon" in n.lower()][0]
    print(f"Computing mean for {latname}, {lonname}")
    if dataset == "ERA5":
        weights = np.cos(np.deg2rad(da[latname]))
        da_mean  = da.weighted(weights).mean(dim=[latname, lonname])
    return da_mean.compute()

def transform_units(da, var_da, dataset="CARRA"):
    if dataset in UNIT_CHANGE:
        for var, func in UNIT_CHANGE[dataset].items():
            if var == var_da:
                da = func(da)
                print(f"Transformed {var} in {dataset} using {func}")
    return da

def correct_time_accumulated_CARRA(ds,var):
    if var in ACCUMULATED_VARS_CERRA:
        if var == "tp":
            ds = ds.assign_coords(
                time=pd.to_datetime(ds.time.values) + pd.Timedelta(days=1)
            )
            ds = ds.assign_coords(
                time=pd.to_datetime(ds.time.values).normalize()
            )
        else:
            period = (
                pd.to_datetime(ds.time.values)
                + pd.offsets.MonthBegin(1)
            ).to_period("M")

            # Middle of each month
            month_start = period.to_timestamp()
            month_end = (period + 1).to_timestamp()

            mid = month_start + (month_end - month_start) / 2

            # Replace the time coordinate
            ds = ds.assign_coords(time=mid)
    return ds        
# --- main compare routine ---
def compare_year(PAIRS, ROOTS, PATTERN, year=2000):
    for key, (car_var, era_var) in PAIRS.items():
        print(f"Processing {key} -> CARRA:{car_var} vs ERA5:{era_var}")
        # find files

        root_car = str(ROOTS["CARRA"])+f"/{car_var}/"
        root_era = ROOTS["ERA5"].format(var=era_var)
        car_files = find_files(root_car, PATTERN["CARRA"])
        era_files = find_files(root_era, PATTERN["ERA5"])
        if len(car_files) == 0:
            print("  No CARRA files found; skipping")
            continue
        if len(era_files) == 0:
            print("  No ERA5 files found; skipping")
            continue
        print(f"Found {(car_files)} CARRA files, {(era_files)} ERA5 files")
        car_ds = open_multi(car_files)
        print(f"Opened CARRA dataset: {car_ds}")
        car_ds = correct_time_accumulated_CARRA(car_ds, car_var)
        era_ds = open_multi(era_files)
        units = era_ds[era_var].attrs.get("units", "unknown")
        print(f"Opened datasets: CARRA {car_ds} vs ERA5 {era_ds}")
        if car_ds is None or era_ds is None:
            print("  failed to open datasets")
            continue

        # compute bbox from CARRA
        try:
            bbox = get_carra_bbox(car_ds, car_var)
        except Exception as e:
            print("  Error computing bbox:", e)
            # fallback to dataset full extent
            lonname = [n for n in car_ds.coords if "lon" in n.lower()][0]
            latname = [n for n in car_ds.coords if "lat" in n.lower()][0]
            bbox = (float(car_ds[lonname].min()), float(car_ds[lonname].max()),
                    float(car_ds[latname].min()), float(car_ds[latname].max()))
        # subset ERA5 to bbox
        print(f"ERA mean = {era_ds[era_var].mean().values}")
        era_sub = subset_to_bbox(era_ds, bbox)
        print(f"ERA mean subset = {era_sub.mean().values}")
        car_sub = car_ds

        # select variable arrays
        if car_var in car_sub:
            car_da = car_sub[car_var]
        else:
            # try first variable
            car_da = list(car_sub.data_vars)[0]
            car_da = car_sub[car_da]

        if era_var in era_sub:
            era_da = era_sub[era_var]
        else:
            era_da = list(era_sub.data_vars)[0]
            era_da = era_sub[era_da]
        #transform units
        car_da = transform_units(car_da,car_var, dataset="CARRA")
        # compute lon-lat mean
        car_ts = lonlat_mean(car_da, dataset="CARRA")
        era_ts = lonlat_mean(era_da, dataset="ERA5")

        # select year slice
        car_ts_y = car_ts.sel(time=slice(f"{year-1}-12-31", f"{year}-12-31"))
        era_ts_y = era_ts.sel(time=slice(f"{year-1}-12-31", f"{year}-12-31"))
        # plotting
        plt.figure(figsize=(10, 4))
        plt.plot(car_ts_y.time, car_ts_y.values, label=f"CARRA {car_var}")
        plt.plot(era_ts_y.time, era_ts_y.values, label=f"ERA5 {era_var}")
        plt.title(f"{key} - {year} - mean over bbox")
        plt.xlabel("time")
        plt.ylabel(f"value in units {units}")
        plt.legend()
        out_png = OUTDIR / f"{key}_{year}_carra_vs_era5.png"
        plt.tight_layout()
        plt.savefig(out_png, dpi=150)
        plt.close()
        print(f"  Saved plot: {out_png}")


if __name__ == "__main__":
    compare_year(PAIRS_MONTHLY_CICA, ROOTS_MONTHLY_CICA, PATTERN, year=2000)
    #compare_year(PAIRS_MONTHLY_STANDARDIZED, ROOTS_MONTHLY_STANDARDIZED, PATTERN, year=2000)
    #compare_year(PAIRS_DAILY, ROOTS_DAILY, PATTERN, year=2000)