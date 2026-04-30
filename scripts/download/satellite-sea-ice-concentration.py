import sys
sys.path.append('../utilities')
from utils_download import download_files



def create_request(row,year):
    #print(row)
    variable=row["cds_request_variable"]
    sensor=row["cds_sensor"]
    region=row["cds_region"]
    cdr_type=row["cds_cdr_type"]
    temporal_aggregation=row["cds_temporal_aggregation"]
    day=row["cds_day"]
    month=row["cds_month"]
    cds_version=row["cds_version"]


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
        "variable":variable,
        "sensor": sensor,
        "region": [region],
        "cdr_type": [cdr_type],
        "temporal_aggregation": temporal_aggregation,
        "year": year,
        "month": month,
        "day":day,
        "version": cds_version,
    }
def get_output_filename(row,dataset,year):

    if row.cds_cdr_type == "icdr":
        sufix = "icdr"
    else:
        sufix = "cdr"
    if row.cds_version == "3_1":
        sufix = f"{sufix}_v3_1"
    elif row.cds_version == "3_0":
        sufix = f"{sufix}_v3_0"
    var=row["filename_variable"]
    date=f"{year}"
    return f"{var}_{dataset}_{date}_{sufix}.zip"

def main():
    dataset_list=["satellite-sea-ice-concentration_nh", "satellite-sea-ice-concentration_sh"]
    for dataset in dataset_list:
        variables_file_path = f"../../requests/{dataset}.csv"
        download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
