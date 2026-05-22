import operations
import pandas as pd
import xarray as xr
import glob
import os
import logging
from pathlib import Path
import sys
import numpy as np
import xclim
from logging_utils import setup_logging

logger = logging.getLogger(__name__)

setup_logging()

root_data="/lustre/gmeteo/WORK/DATA/C3S-CDS/ERA5/day/"

var1="d2m"
var2="t2m"

start_period="2015-01-01"
end_period="2020-12-31"
year_list = list(range(2015, 2021))


for year in year_list:

    output_dir="/lustre/gmeteo/WORK/chantreuxa/test_hurs/derived/derived-era5-single-levels-daily-statistics/daily/native/hurs/"
    os.makedirs(output_dir, exist_ok=True)
    hurs_file = f"era5_day_hurs_{year}.nc"
    output_file=Path(f"{output_dir}/{hurs_file}")
    if output_file.exists():
        logger.info(f"File {output_file} already exists. Skipping...")
        continue
    logger.info(f"Searching files with pattern: {root_data}/{var1}/{year}/*{year}*.nc")
    list_files_d2m= np.sort(glob.glob(f"{root_data}/{var1}/{year}/*{year}*.nc"))
    list_files_t2m = np.sort(glob.glob(f"{root_data}/{var2}/{year}/*{year}*.nc"))


    logger.info(f"Calculating hurs for year {year} from {var1} and {var2}")
    ds_d2m = xr.open_mfdataset(list_files_d2m, concat_dim = 'time', combine='nested').sel(time=slice(start_period, end_period))
    ds_t2m = xr.open_mfdataset(list_files_t2m, concat_dim = 'time', combine='nested').sel(time=slice(start_period, end_period))
    logger.info(f"Datasets opened for year {year}")
    ds_merge = xr.merge([ds_d2m, ds_t2m])
    logger.info(f"Datasets merged for year {year}")
    #hurs = operations.rh_from_thermofeel(ds_merge, "d2m", "t2m")


    hurs = xclim.indicators.convert.relative_humidity_from_dewpoint(ds_merge.tas, ds_merge.d2m)
    logger.info(f"hurs calculated for year {year}, now saving... to {output_file}")
    hurs.to_netcdf(output_file)
    ds_d2m.close()
    ds_t2m.close()
    ds_merge.close()
    hurs.close()
    del ds_d2m, ds_t2m, ds_merge, hurs
    logger.info(f"File saved: {output_file}")