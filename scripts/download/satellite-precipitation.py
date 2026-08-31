import logging
import sys
sys.path.append('../utilities')
from logging_utils import setup_logging
from utils_download import download_files

logger = logging.getLogger(__name__)



def create_request(row,year):
    variable=row["cds_request_variable"]
    time_aggregation=row["cds_time_aggregation"]
    day=row["cds_day"]
    month=row["cds_month"]
    cds_version=row["cds_version"]
    if month == "all":
        month = [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ]

    return {
        "variable": variable,
        "time_aggregation": time_aggregation,
        "year": [str(year)],
        "month": month,
        "version": [cds_version],
    }

def get_output_filename(row,dataset,year):
    sufix = ""
    if row.cds_version :
        sufix = f"{sufix}_{row.cds_version}"

    var=row["filename_variable"]
    date=f"{year}"
    return f"{var}_{dataset}_{date}_{sufix}.zip"

def main():
    setup_logging()
    dataset_list=["satellite-precipitation"]
    for dataset in dataset_list:
        logger.info(f"Starting download workflow for {dataset}")
        variables_file_path = f"../../requests/{dataset}.csv"
        download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
