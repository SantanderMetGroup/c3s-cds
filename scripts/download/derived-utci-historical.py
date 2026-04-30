import sys
sys.path.append('../utilities')
from utils_download import download_files



def create_request(row,year,month="all"):
    var=row["cds_request_variable"]
    day=row["cds_day"]

    #data_format=row["cds_data_format"]
    product_type=row["cds_product_type"]
    #level_type=row["cds_level_type"]
    #soil_layers=row["cds_soil_layer"]


    

    if day == "all":
        day = [
            "01", "02", "03", "04", "05", "06",
            "07", "08", "09", "10", "11", "12",
            "13", "14", "15", "16", "17", "18",
            "19", "20", "21", "22", "23", "24",
            "25", "26", "27", "28", "29", "30",
            "31"
        ]
    if month == "all":
        month = [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ]
    else:
        month=[month]
    #request = {
    #"variable": ["universal_thermal_climate_index"],
    #"version": "1_1",
    #"product_type": "consolidated_dataset",
    #"year": ["2020"],
    #"month": ["01"],
    #"day": ["01"],
    #}
    dict_request={
        "variable": [var],
        "version": "1_1",
        "product_type": [product_type],
        "year": [year],
        "month": month,
        "day":day,
    }


    return dict_request
def get_output_filename(row,dataset,year,month):
    var=row["filename_variable"]
    date=f"{year}{month}"
    return f"{var}_{dataset}_{date}.zip"


def main():
    dataset="derived-utci-historical"
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename, request_frequency="monthly")

if __name__ == "__main__":
    main()
