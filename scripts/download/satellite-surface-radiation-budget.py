import sys
sys.path.append('../utilities')
from utils_download import download_files



def create_request(row,year):
    #print(row)
    variable=row["cds_request_variable"]
    origin=row["cds_origin"]
    product_family=row["cds_product_family"]
    time_aggregation=row["cds_time_aggregation"]
    climate_data_record_type=row["cds_climate_data_record_type"]
    day=row["cds_day"]
    month=row["cds_month"]


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
    return {
        "variable":[variable],
        "origin": origin,
        "product_family": product_family,
        "climate_data_record_type": climate_data_record_type,
        "time_aggregation": time_aggregation,
        "year": [str(year)],
        "month": month,
    }
def get_output_filename(row,dataset,year):

    if row.cds_climate_data_record_type== "thematic_climate_data_record":
        sufix = "tcdr"
    else:
        sufix = "icdr"
    var=row["filename_variable"]
    date=f"{year}"
    return f"{var}_{dataset}_{date}_{sufix}.zip"

def main():
    dataset = "satellite-surface-radiation-budget"
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename, request_frequency="yearly", extracted_frequency="monthly")

if __name__ == "__main__":
    main()
