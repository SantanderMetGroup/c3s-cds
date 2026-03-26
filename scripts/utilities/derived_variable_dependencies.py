
VARIABLE_DEPENDENCIES = {
    "hurs": ["d2m", "t2m"],   # relative humidity from dewpoint & temperature
    "sfcwind": ["u10", "v10"], # wind speed from u/v components
    "huss": ["d2m", "ps"],      # specific humidity from dewpoint & pressure
    "rsus": ["ssrd", "ssr"],   
    "rlus": ["strd", "str"],
}

dataset_variable_mapping = {
    "reanalysis-era5-single-levels": {
    },
    "derived-era5-single-levels-daily-statistics": {
        "ps": "sp"
    },
    "insitu-gridded-observations-europe": {

    },
    "reanalysis-cerra-land": {

    },
    "reanalysis-cerra-single-levels": {

    }
}

