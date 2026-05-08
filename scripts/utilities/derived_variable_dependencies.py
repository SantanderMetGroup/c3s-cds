
VARIABLE_DEPENDENCIES = {
    "hurs": ["d2m", "t2m"],   # relative humidity from dewpoint & temperature
    "sfcwind": ["u10", "v10"], # wind speed from u/v components
    "huss": ["d2m", "ps"],      # specific humidity from dewpoint & pressure
    "rsus": ["ssrd", "ssr"],   # surface upwelling shortwave radiation from surface downwelling & net shortwave
    "rlus": ["strd", "str"],   # surface upwelling longwave radiation from surface downwelling & net longwave
}

dataset_variable_mapping = {
    "reanalysis-era5-single-levels": {
    },
    "derived-era5-single-levels-daily-statistics": {
        "ps": "sp",
        "t2mx": "t2m",
        "t2mn": "t2m",
    },
    "insitu-gridded-observations-europe": {

    },
    "reanalysis-cerra-land": {

    },
    "reanalysis-cerra-single-levels": {

    }
}

