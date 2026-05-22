import logging
import sys
sys.path.append('../utilities')
from logging_utils import setup_logging
from utils_download import download_files

logger = logging.getLogger(__name__)



def create_request(row,year):
    temporal_resoution=row["cds_variable"]
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
        "variable": [temporal_resoution],
        "year": year,
        "month": month,
        "day":day,
        "version": cds_version,
    }
def get_output_filename(row,dataset,year):
    
    var=row["filename_variable"]
    date=f"{year}"
    return f"{var}_{dataset}_{date}.zip"

def main():
    setup_logging()
    dataset="satellite-sea-level-global"
    logger.info(f"Starting download workflow for {dataset}")
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
