import logging
import sys
sys.path.append('../utilities')
from logging_utils import setup_logging
from utils_download import download_files

logger = logging.getLogger(__name__)



def create_request(row,year,month):
    #print(row)
    variable=row["cds_variable"]
    processinglevel=row["cds_processinglevel"]
    time_aggregation=row["cds_temporal_resolution"]
    sensor=row["cds_sensor_on_satellite"]
    cds_version=row["cds_version"]


    if month == "all":
        month = [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ]
    else:
         month=[month]
    return {
        "variable":variable,
        "processinglevel": processinglevel,
        "temporal_resolution": time_aggregation,
        "sensor_on_satellite": sensor,
        "year": [str(year)],
        "month": month,
        "version": [cds_version],
    }
def get_output_filename(row,dataset,year,month):


    version=row["cds_version"]
    var=row["filename_variable"]
    date=f"{year}{month}"
    return f"{var}_{dataset}_{date}_{version}.zip"


def main():
    setup_logging()
    dataset_list=["satellite-sea-surface-temperature"]
    for dataset in dataset_list:
        logger.info(f"Starting download workflow for {dataset}")
        variables_file_path = f"../../requests/{dataset}.csv"
        download_files(dataset, variables_file_path, create_request, get_output_filename, request_frequency="monthly")

if __name__ == "__main__":
    main()
