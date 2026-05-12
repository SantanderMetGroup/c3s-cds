import xarray as xr
import matplotlib.pyplot as plt
import glob
import os

# ---------------------------
# 1. File path
# ---------------------------
var="ssrd"
path = f"/lustre/gmeteo/PTICLIMA/DATA/REANALYSIS/ERA5/data_derived/medcof_1degree/day/{var}/*.nc"
#path = f"/lustre/gmeteo/WORK/chantreuxa/pr_ERA5/raw/derived-era5-single-levels-daily-statistics/daily/native/{var}/**/*.nc"

files = glob.glob(path, recursive=True)

print(f"Found {len(files)} files")

# ---------------------------
# 2. Open dataset
# ---------------------------
ds = xr.open_mfdataset(
    files,
    combine="by_coords",
    chunks={"time": 100}
)

if "valid_time" in ds.dims:
    ds = ds.rename({"valid_time": "time"})

print("Dataset opened successfully")

# ---------------------------
# 3. REGIONS (ADD MORE HERE)
# ---------------------------
regions = {
    "djibouti": (41.66176, 10.9268785669, 43.3178524107, 12.6996385767),
    "spain": (-9.5, 35.0, 3.5, 44.5)
}

region_name = "spain"   

lon_min, lat_min, lon_max, lat_max = regions[region_name]

print(f"{region_name.upper()} bounding box:")
print(f"lon [{lon_min}, {lon_max}], lat [{lat_min}, {lat_max}]")

# ---------------------------
# 4. Subset region
# ---------------------------
ds_reg = ds.sel(
    lon=slice(lon_min, lon_max),
    lat=slice(lat_max, lat_min)  # ERA5 lat is descending
)

# ---------------------------
# 5. Extract precipitation
# ---------------------------
tp = ds_reg[var]
unit=tp.attrs.get("units", "unknown")
if var == "tp":
    tp_mm = tp * 1000.0
    var_cica="pr"
elif var == "e":
    tp_mm = tp * -1000.0
    var_cica="evspsbl"
elif var == "ssrd":
    tp_mm = tp / 86400
    var_cica="rsds"

path_cica= f"/lustre/gmeteo/WORK/PROYECTOS/2022_C3S_Atlas/workflow/datasets/CICAv2/intermediate_products/provider/ERA5/{var_cica}/raw/*nc"
files = glob.glob(path_cica, recursive=True)

print(f"Found {len(files)} files")

# ---------------------------
# 2. Open dataset
# ---------------------------
ds2 = xr.open_mfdataset(
    files,
    combine="by_coords",
    chunks={"time": 100}
)
ds2_reg = ds2.sel(
    lon=slice(lon_min, lon_max),
    lat=slice(lat_min, lat_max)  # ERA5 lat is descending
)
tp2_mm = ds2_reg[var_cica]
print(f"Extracted '{var_cica}' for {region_name} with shape {tp_mm.shape}")

# ---------------------------
# 6. Area mean time series
# ---------------------------
tp_ts = tp_mm.mean(dim=["lat", "lon"]).sortby("time").compute()
tp2_ts = tp2_mm.mean(dim=["lat", "lon"]).sortby("time").compute()
    
# ---------------------------
# 7. Highlight period (2021–2024)
# ---------------------------
mask_special = (tp_ts["time"].dt.year >= 2021) & (tp_ts["time"].dt.year <= 2024)
tp_special = tp_ts.where(mask_special)

# ---------------------------
# 8. Plot
# ---------------------------
plt.figure(figsize=(12, 5))

plt.plot(tp_ts["time"], tp_ts, color="blue", alpha=0.5, label="All years")
plt.plot(tp_special["time"], tp_special, color="red", linewidth=1, label="2021–2024")
plt.plot(tp2_ts["time"], tp2_ts, color="green", linestyle='--', alpha=0.5, label="CICAv2 (ERA5)")
if var in ["e","ssrd"]:
    mon_tp_ts = tp_ts.resample(time="1ME").mean()
    mon_tp_special= tp_special.resample(time="1ME").mean()
    plt.plot(mon_tp_ts["time"], mon_tp_ts, color="blue", alpha=0.5, label="All years mon")
    plt.plot(mon_tp_special["time"],  mon_tp_special, color="orange", linewidth=1.5, label="2021–2024 mon")
plt.title(f"{region_name.capitalize()} Mean Daily Total {var.upper()} (ERA5)")
plt.ylabel(f"{var.upper()} ({unit})")
plt.xlabel("Time")
plt.grid(True)
plt.legend()

# ---------------------------
# Save figure
# ---------------------------
outdir = "figures_validation"
os.makedirs(outdir, exist_ok=True)

plt.tight_layout()
plt.savefig(f"{outdir}/{var}_{region_name}.png", dpi=300)
plt.show()
print(f" MEAN {region_name} ds1 down: {tp_ts.mean().compute()} mm")
print(f" MEAN {region_name} ds2 CICA: {tp2_ts.mean().compute()} mm")