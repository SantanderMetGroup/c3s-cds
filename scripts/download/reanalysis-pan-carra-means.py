import sys
import os
import logging

# Add the project root (c3s-cds)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
# Add the utilities directory so inner modules can resolve each other
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utilities')))

from scripts.utilities.utils_download import download_files
from logging_utils import setup_logging

logger = logging.getLogger(__name__)



def create_request(row,year,month):
    var=row["cds_request_variable"]
    day=row["cds_day"]
    data_format=row["cds_data_format"]
    product_type=row["cds_product_type"]
    level_type=row["cds_level_type"]
    time_aggregation=row["cds_time_aggregation"]
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
    return {
        "variable": [var],
        "time_aggregation": time_aggregation,
        "level_type": level_type,
        "product_type": product_type,
        "year": year,
        "month": month,
        "day":day,
        "data_format": data_format,
    }
def get_output_filename(row,dataset,year,month):
    
    var=row["filename_variable"]
    date=f"{year}{month}"
    return f"{var}_{dataset}_{date}.nc"

def main():
    setup_logging()
    dataset="reanalysis-pan-carra-means"
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename, request_frequency="monthly")

if __name__ == "__main__":
    main()
