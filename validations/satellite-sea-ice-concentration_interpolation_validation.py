import os

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import glob
# ----------------------------------
# INPUT FILES
# ----------------------------------
root1=f"/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/satellite-sea-ice-concentration_nh/daily/native/ice_conc/ice_conc_satellite-sea-ice-concentration_nh_1980*.nc"
root2=f"/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/derived/satellite-sea-ice-concentration_nh/daily/gr025/ice_conc/ice_conc_satellite-sea-ice-concentration_nh_1980*.nc"
root3=f"/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/derived/satellite-sea-ice-concentration_nh/daily/gr025/ice_conc/save_method_xcyc//ice_conc_satellite-sea-ice-concentration_nh_1980*.nc"



files_native = glob.glob(root1)        # LAEA grid (xc, yc)
files_interp = glob.glob(root2)
files_newlatlon = glob.glob(root3)        # lat/lon grid
var = "ice_conc"

ds_native = xr.open_mfdataset(files_native, combine="by_coords").sel(time=slice("1980-01-01", "1980-12-31")).load()
ds_interp = xr.open_mfdataset(files_interp, combine="by_coords").sel(time=slice("1980-01-01", "1980-12-31")).load()
ds_newlatlon = xr.open_mfdataset(files_newlatlon, combine="by_coords").sel(time=slice("1980-01-01", "1980-12-31")).load()


native = ds_native[var].where(ds_native[var] != -32767)
interp = ds_interp[var]
newlatlon = ds_newlatlon[var]



# 2D latitude field
lat2d = ds_native["lat"]

lat_bins = np.arange(-90, 91, 1)

native_binned = native.groupby_bins(ds_native["lat"], lat_bins).mean()

lat_centers = 0.5 * (lat_bins[:-1] + lat_bins[1:])

native_zonal = native_binned.assign_coords(
    lat=("lat_bins", lat_centers)
)

# Time mean (optional)
native_zonal_mean = native_zonal.mean(dim="time")

# ----------------------------------
# --- INTERPOLATED: zonal mean ---
# ----------------------------------

weights = np.cos(np.deg2rad(interp["lat"]))

interp_zonal = interp.weighted(weights).mean(dim="lon")
interp_zonal_mean = interp_zonal.mean(dim="time")

newlatlon_zonal = newlatlon.weighted(weights).mean(dim="lon")
newlatlon_zonal_mean = newlatlon_zonal.mean(dim="time")

outdir = "figures_validation"
os.makedirs(outdir, exist_ok=True)
# ----------------------------------
# PLOT
# ----------------------------------
plt.figure(figsize=(6, 8))

plt.plot(native_zonal_mean, lat_centers, label="Native (LAEA, no interp)", linestyle="-")
plt.plot(interp_zonal_mean, interp_zonal_mean["lat"], label="Interpolated_original", linestyle="--")
plt.plot(newlatlon_zonal_mean, newlatlon_zonal_mean["lat"], label="Interpolated_newlatlon", linestyle=":")


plt.xlabel("Sea Ice Concentration (%)")
plt.ylabel("Latitude")
plt.title("Zonal Mean Sea Ice Concentration (1980)")

plt.legend()
plt.grid()

plt.tight_layout()
plt.show()
plt.savefig(f"{outdir}/zonal_mean_comparison_1980.png", dpi=300)
# ----------------------------------
# DIFFERENCE (optional)
# ----------------------------------

# Interpolate interpolated curve onto same lat bins for comparison
interp_on_bins = interp_zonal_mean.interp(lat=lat_centers)

diff = interp_on_bins - native_zonal_mean

plt.figure(figsize=(6, 8))
plt.plot(diff, lat_centers)
plt.axvline(0, linestyle="--")

plt.xlabel("Difference (%)")
plt.ylabel("Latitude")
plt.title("Difference (Interp - Native)")

plt.grid()
plt.tight_layout()
plt.show()
plt.savefig(f"{outdir}/difference_comparison.png", dpi=300)
# ----------------------------------
# CLEANUP
# ----------------------------------
ds_native.close()
ds_interp.close()