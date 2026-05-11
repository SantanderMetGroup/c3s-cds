
VARIABLE_DEPENDENCIES = {
    "hurs": ["d2m", "t2m"],   # relative humidity from dewpoint & temperature
    "sfcwind": ["u10", "v10"], # wind speed from u/v components
    "huss": ["d2m", "ps"],      # specific humidity from dewpoint & pressure
    "rsus": ["rsds", "rsns"],
    "rlus": ["rlds", "rlns"],
    "mrt": ["rsus", "rlus", "rsds", "rlds"],
    "utci": ["t2m", "sfcwind", "hurs", "mrt"],
}

dataset_variable_mapping = {
    "reanalysis-era5-single-levels": {
        "rsds": "ssrd",
        "rsns": "ssr",
        "rlds": "strd",
        "rlns": "str",
    },
    "derived-era5-single-levels-daily-statistics": {
        "rsds": "ssrd",
        "rsns": "ssr",
        "rlds": "strd",
        "rlns": "str",
        "ps": "sp",
        "t2mx": "t2m",
        "t2mn": "t2m",
    },
    "insitu-gridded-observations-europe": {

    },
    "reanalysis-cerra-land": {
        "rsds": "ssrd",
        "rlds": "strd",
    },
    "reanalysis-cerra-single-levels": {
        "rsds": "ssrd",
    },
}

