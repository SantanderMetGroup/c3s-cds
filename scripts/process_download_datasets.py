import datetime
import warnings
from pathlib import Path
import cdsapi
import os
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
warnings.filterwarnings("ignore")

def read_from_yaml(file_path):
    """
    Read variables and their corresponding dataset names from a YAML file.
    Parameters
    ----------
    file_path : str
        Path to the YAML file containing the variable mappings.
    Returns
    -------
    variables : dict
        A dictionary mapping variable names to dataset variable names.
    """
    with open(file_path, 'r') as file:
        variables = yaml.safe_load(file)
    return variables

def download_file(catalogue_id: str, catalogue_entry: dict, output_path: Path) -> Path:
    """
    Download a file from a given catalogue ID with the given parameters.
    This method retrieves the file from the CDS API, saves it in a temporary
    directory, and returns the `Path` object pointing to the downloaded file.
    Parameters
    ----------
    catalogue_id : str
        A string containing the CDS catalogue id. For instance: projections-cmip6.
    catalogue_entry: dict
        A dictionary containing the fields of the requested data.
    output_path: Path
        Path associated with the request params.
    Returns
    -------
    output_path: Path
        A `Path` object pointing to the downloaded file.
    Raises
    ------
    Exception
        If the download request failed.
    """
    start_time = datetime.datetime.now()
    c = cdsapi.Client(timeout=500, quiet=True)
    logging.info(f"Downloading the data from {catalogue_id} with parameters {catalogue_entry}")
    r = c.retrieve(catalogue_id, catalogue_entry)
    r.download(output_path)
    end_time = datetime.datetime.now()
    final_time = end_time - start_time
    logging.info(f"Duration of the process to download data: {final_time}")
    return output_path

def create_request_era5_daily(row,year):
    var=row["request_variable"]
    daily_statistic=row["daily_statistic"]
    day=row["day"]
    month=row["month"]
    time_zone=row["time_zone"]
    frequency=row["frequency"]
    product_type=row["product_type"]
    
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
        "variable": [var],
        "product_type": [product_type],
        "year": year,
        "month": month,
        "day":day,
        "time_zone": time_zone,
        "frequency": frequency,
        "daily_statistic": daily_statistic,
    }


def create_request_era5_hourly(row,year):
    var=row["request_variable"]
    day=row["day"]
    month=row["month"]
    data_format=row["data_format"]
    product_type=row["product_type"]
    download_format=row["download_format"]

    time= [
        "00:00", "01:00", "02:00",
        "03:00", "04:00", "05:00",
        "06:00", "07:00", "08:00",
        "09:00", "10:00", "11:00",
        "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00",
        "18:00", "19:00", "20:00",
        "21:00", "22:00", "23:00"
    ]
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
        "variable": [var],
        "product_type": [product_type],
        "year": year,
        "month": month,
        "day":day,
        "data_format": data_format,
        "download_format": download_format,
        "time":time
    }


def get_request(row,year,dataset):
    if dataset=="derived-era5-single-levels-daily-statistics":
        return create_request_era5_daily(row,year)
    elif dataset=="reanalysis-era5-single-levels":
        return create_request_era5_hourly(row,year)
def get_output_filename(row,dataset,year):

    var=row["variable"]
    date=f"{year}-01-01_{year}-12-31"
    return f"{var}-{dataset}-{date}.nc"
def download_files(row,  dataset):
    """
    Download  files for the specified variables and years.
    Parameters
    ----------
    row : pd.Series
       A row from the configuration file.
    dataset : str
        The dataset name.
   
    """
    print(row)
    dest_dir = Path(row["path_download"])/ dataset / row["variable"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    year_list = list(range(row["years_start"], row["years_end"]+1))
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for year in year_list:
            request = get_request(row,year,dataset)
            file = get_output_filename(row, dataset, year)
            path_file = dest_dir / file
            if path_file.exists():
                logging.info(f"{path_file} already exists, skipping")
                continue
            futures.append(executor.submit(download_file, dataset, request, path_file))

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Failed to download file: {e}")

def main():
    # Path to the YAML file containing the variable mappings
    dataset_list = ["reanalysis-era5-single-levels","derived-era5-single-levels-daily-statistics"]
    for dataset in dataset_list:
        variables_file_path = f"{dataset}.csv"
        df_parameters = pd.read_csv(variables_file_path)

        for index, row in df_parameters.iterrows():        

            download_files(row, dataset)

if __name__ == "__main__":
    main()
