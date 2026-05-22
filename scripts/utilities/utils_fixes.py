import numpy as np


def fix_dim_time(dataset):
    if "time" not in dataset.coords and "valid_time" in dataset.coords:
        dataset = dataset.rename({"valid_time": "time"})
    return dataset
def get_lon_lat_names(dataset):
    """
    Return the names of the longitude and latitude coordinates.
    """

    lon_name = None
    lat_name = None

    # --- 1. Try CF metadata (most robust) ---
    for coord in dataset.coords:
        std_name = dataset[coord].attrs.get("standard_name", "").lower()
        axis = dataset[coord].attrs.get("axis", "").upper()

        if std_name == "longitude" or axis == "X":
            lon_name = coord
        elif std_name == "latitude" or axis == "Y":
            lat_name = coord

    # --- 2. Fallback to common names ---
    if lon_name is None:
        for name in ["lon", "longitude", "LONGITUDE", "LON", "x"]:
            if name in dataset.coords:
                lon_name = name
                break

    if lat_name is None:
        for name in ["lat", "latitude", "LATITUDE", "LAT", "y"]:
            if name in dataset.coords:
                lat_name = name
                break

    return lon_name, lat_name


def _deduplicate_longitude(dataset, lon_name):
    """Drop duplicate longitude coordinates while preserving order."""
    lon_values = dataset[lon_name].values
    _, unique_index = np.unique(lon_values, return_index=True)
    if unique_index.size != lon_values.size:
        dataset = dataset.isel({lon_name: np.sort(unique_index)})
    return dataset


def convert_longitudes_to_minus180_180(dataset):
    """
    Convert longitude coordinates from [0, 360] (or mixed) to [-180, 180),
    then sort and drop duplicate longitudes if they appear at the dateline.

    The function is xarray-native and works for both Dataset and DataArray.
    """
    lon_name, _ = get_lon_lat_names(dataset)
    if lon_name is None:
        return dataset

    # Wrap to [-180, 180)
    lon_180 = ((dataset[lon_name] + 180) % 360) - 180
    dataset = dataset.assign_coords({lon_name: lon_180}).sortby(lon_name)
    return _deduplicate_longitude(dataset, lon_name)




def convert_longitudes_to_360(dataset):
    """
    Convert longitude coordinates to [0, 360), then sort and drop duplicates.

    Use this when your workflow expects 0..360 longitudes.
    """
    lon_name, _ = get_lon_lat_names(dataset)
    if lon_name is None:
        return dataset

    lon_360 = dataset[lon_name] % 360
    dataset = dataset.assign_coords({lon_name: lon_360}).sortby(lon_name)
    return _deduplicate_longitude(dataset, lon_name)

def fix_dataset(dataset):
    dataset = fix_dim_time(dataset)
    if "expver" in dataset.data_vars:
        dataset = dataset.drop_vars("expver")
    # dataset = convert_longitudes_to_minus180_180(dataset)
    return dataset

