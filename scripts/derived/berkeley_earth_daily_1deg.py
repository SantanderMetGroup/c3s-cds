import sys
import os
from pathlib import Path

sys.path.append("../utilities")

import logging

import numpy as np
import pandas as pd
import xarray as xr

from logging_utils import setup_logging
from utils import build_output_path


logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

TIME_UNITS = "days since 1880-01-01 00:00:00"
TIME_CALENDAR = "standard"

DATASET = "berkeley_earth_daily_1deg"

RENAMING_VAR_MAP = {  
    "TAVG": {"variable_name": "tas", "long_name": "Daily Mean Near-Surface Air Temperature","standard_name": "air_temperature", "units": "degC"},
    "TMAX": {"variable_name": "tasmax", "long_name": "Daily Maximum Near-Surface Air Temperature","standard_name": "air_temperature", "units": "degC"},
    "TMIN": {"variable_name": "tasmin", "long_name": "Daily Minimum Near-Surface Air Temperature","standard_name": "air_temperature", "units": "degC"},
}
# ----------------------------------------------------------------------
# File handling
# ----------------------------------------------------------------------

def get_input_files(input_dir):
    """Return all Berkeley Earth NetCDF files in the input directory."""

    logger.info(f"Looking for input files in: {input_dir}")

    files = sorted(input_dir.glob("*.nc"))

    logger.info(f"Found {len(files)} input files")

    return files


# ----------------------------------------------------------------------
# Time
# ----------------------------------------------------------------------

def create_time_coordinate(ds):
    """
    Create a standard datetime coordinate from Berkeley Earth
    year, month and day variables.
    """

    logger.info("Creating standard datetime coordinate")

    time_coords = pd.to_datetime(
        dict(
            year=ds["year"].values,
            month=ds["month"].values,
            day=ds["day"].values,
        )
    )

    return ds.assign_coords(
        time=("time", time_coords)
    )


def remove_leap_days(ds):
    """
    Remove February 29 from the dataset.

    Berkeley Earth climatology contains 365 days, so leap days
    are removed before reconstructing the daily temperature.
    """

    logger.info("Removing February 29")

    leap_day = (
        (ds["time"].dt.month == 2)
        & (ds["time"].dt.day == 29)
    )

    n_leap_days = int(leap_day.sum())

    logger.info(f"Removing {n_leap_days} leap days")

    return ds.sel(time=~leap_day)


# ----------------------------------------------------------------------
# Temperature
# ----------------------------------------------------------------------

def reconstruct_temperature(ds, variable_name):
    """
    Reconstruct daily temperature following the Berkeley Earth
    representation:

        temperature = climatology + anomaly

    Berkeley Earth climatology contains one 365-day climatological
    year. The climatology is repeated for every year in the file
    and aligned with the anomaly time coordinate.
    """

    logger.info("Reconstructing daily temperature")

    # Remove leap days so every year contains exactly 365 days.
    ds_no_leap = remove_leap_days(ds)

    n_days = ds_no_leap.sizes["time"]

    if n_days % 365 != 0:
        raise ValueError(
            f"Number of non-leap days ({n_days}) is not divisible by 365"
        )

    n_years = n_days // 365

    logger.info(
        f"Found {n_years} years and {n_days} non-leap days"
    )

    # Berkeley climatology contains one climatological year.
    climatology = ds_no_leap["climatology"]

    if climatology.sizes["day_number"] != 365:
        raise ValueError(
            "Expected Berkeley Earth climatology to contain "
            f"365 days, found {climatology.sizes['day_number']}"
        )

    # Repeat the climatology for every year.
    climatology_repeated = xr.concat(
        [climatology] * n_years,
        dim="day_number",
    )

    # Rename the climatological day dimension to time.
    climatology_repeated = climatology_repeated.rename(
        {"day_number": "time"}
    )

    # Give the repeated climatology the actual dates.
    climatology_repeated = climatology_repeated.assign_coords(
        time=ds_no_leap["time"].values
    )

    # Add anomaly and climatology.
    tas = (
        ds_no_leap["temperature"]
        + climatology_repeated
    )

    tas.name = RENAMING_VAR_MAP[variable_name]["variable_name"]

    tas.attrs = {
        "long_name": RENAMING_VAR_MAP[variable_name]["long_name"],
        "units": RENAMING_VAR_MAP[variable_name]["units"],
        "standard_name": RENAMING_VAR_MAP[variable_name]["standard_name"],
    }
    return tas, ds_no_leap,tas.name


# ----------------------------------------------------------------------
# Coordinates / dataset
# ----------------------------------------------------------------------

def create_standard_dataset(ds, data, variable_name):
    """Create a clean C3S-style dataset."""

    out = xr.Dataset(
        {
            variable_name: data,
        },
        coords={
            "time": ds["time"],
            "lat": ds["latitude"],
            "lon": ds["longitude"],
        },
    )

    out["lat"].attrs = {
        "standard_name": "latitude",
        "long_name": "Latitude",
        "units": "degrees_north",
    }

    out["lon"].attrs = {
        "standard_name": "longitude",
        "long_name": "Longitude",
        "units": "degrees_east",
    }

    out["time"].attrs = {
        "standard_name": "time",
        "long_name": "Time",
    }

    return out


# ----------------------------------------------------------------------
# Yearly output
# ----------------------------------------------------------------------

def save_year(ds, year, output_dir, variable_name):
    """Save one year as a NetCDF file."""

    output_file = (
        output_dir
        / f"{DATASET}_{variable_name}_{year}.nc"
    )

    if output_file.exists():
        logger.info(f"Already exists: {output_file.name}")
        return

    year_ds = ds.sel(
        time=ds.time.dt.year == year
    )

    encoding = {
        "time": {
            "units": TIME_UNITS,
            "calendar": TIME_CALENDAR,
        },
        variable_name: {
            "zlib": True,
            "complevel": 4,
        },
    }

    logger.info(
        f"Saving: {output_file.name} "
        f"({year_ds.sizes['time']} days)"
    )

    year_ds.to_netcdf(
        output_file,
        encoding=encoding,
    )


# ----------------------------------------------------------------------
# Process one file
# ----------------------------------------------------------------------

def process_file(input_file, output_dir, variable_name):
    """Process one Berkeley Earth file."""

    logger.info(
        f"Processing: {input_file.name} "
        f"for variable: {variable_name}"
    )

    logger.info(f"Opening: {input_file.name}")

    ds = xr.open_dataset(input_file)


    try:

        # --------------------------------------------------------------
        # Create real Gregorian dates from Berkeley year/month/day.
        # --------------------------------------------------------------

        ds = create_time_coordinate(ds)
        if "2020" in str(input_file):
            logger.info(f"Found 2020 file: {input_file.name}")
            ds=ds.sel(time=slice("2020-01-01", "2021-12-31"))
            logger.info(f"Selected time slice: {ds.time.min().values} to {ds.time.max().values} because 2022 is not complete")
        # --------------------------------------------------------------
        # Reconstruct temperature using climatology + anomaly.
        # This also removes February 29.
        # Feb 29 is excluded because the climatology has 365 days.
        # --------------------------------------------------------------

        data, ds_no_leap,variable_name = reconstruct_temperature(
            ds,
            variable_name,
        )

        # --------------------------------------------------------------
        # Create C3S-style output dataset.
        # --------------------------------------------------------------

        output_ds = create_standard_dataset(
            ds_no_leap,
            data,
            variable_name,
        )

        # --------------------------------------------------------------
        # Save one file per year.
        # --------------------------------------------------------------

        years = np.unique(
            output_ds.time.dt.year.values
        )

        logger.info(
            f"Years found: {years[0]} - {years[-1]}"
        )

        for year in years:
            save_year(
                output_ds,
                year,
                output_dir,
                variable_name,
            )

    finally:
        ds.close()


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():

    setup_logging()

    logger.info(
        "Starting Berkeley Earth daily 1-degree "
        "processing workflow"
    )

    script_dir = os.path.dirname(
        os.path.abspath(__file__)
    )

    variables_file_path = os.path.join(
        script_dir,
        "..",
        "..",
        "requests_external",
        f"{DATASET}.csv",
    )

    df_parameters = pd.read_csv(
        variables_file_path
    )

    for _, row in df_parameters.iterrows():

        if row["product_type"] != "raw":
            continue

        input_dir = build_output_path(
            row["output_path"],
            DATASET,
            row["product_type"],
            row["temporal_resolution"],
            row["interpolation"],
            row["filename_variable"],
        )

        output_dir = Path(
            str(input_dir).replace(
                "/raw/",
                "/derived/",
            )
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        files = get_input_files(input_dir)

        variable = row["filename_variable"]

        for input_file in files:

            process_file(
                input_file,
                output_dir,
                variable,
            )


if __name__ == "__main__":
    main()