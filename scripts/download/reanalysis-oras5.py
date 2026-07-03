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



def create_request(row,year):
    logger.info(f"Creating request for year: {year}, variable: {row['cds_request_variable']}")
    var=row["cds_request_variable"]
    product_type=row["cds_product_type"]
    vertical_resolution=row["cds_vertical_resolution"]


    month = [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ]

    return {
        "variable": [var],
        "vertical_resolution": vertical_resolution,
        "product_type": [product_type],
        "year": [year],
        "month": month,
    }
def get_output_filename(row,dataset,year):
    
    var=row["filename_variable"]
    date=f"{year}"
    return f"{var}_{dataset}_{date}.zip"

def main():
    setup_logging()
    dataset="reanalysis-oras5"
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
