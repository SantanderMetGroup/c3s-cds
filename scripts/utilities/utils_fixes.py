def fix_dim_time(dataset):
    if "valid_time" in dataset.dims:
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
def convert_longitudes_to_360(dataset):
    lon_name, _ = get_lon_lat_names(dataset)

    if lon_name is not None:
        dataset = (
            dataset
            .assign_coords({lon_name: (dataset[lon_name] % 360)})
            .sortby(lon_name)
        )

    return dataset

def fix_dataset(dataset):
    dataset = fix_dim_time(dataset)
    dataset = convert_longitudes_to_360(dataset)
    return dataset

