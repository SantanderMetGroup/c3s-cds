import sys
sys.path.append('../utilities')
from utils import download_files




def get_output_filename(row,dataset,year):

    var=row["filename_variable"]
    date=f"{year}"
    return f"{var}_{dataset}_{date}.nc"

def create_request(row,year):
    var=row["cds_request_variable"]
    product_type=row["cds_product_type"]
    grid_resolution=row["cds_grid_resolution"]
    version=row["cds_version"]
    #year=yea
    


    return {
        "variable": [var],
        "product_type": product_type,
        "grid_resolution": grid_resolution,
        "period": "full_period",
        "version": version,
    }

def main():
    dataset = "insitu-gridded-observations-europe"
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
