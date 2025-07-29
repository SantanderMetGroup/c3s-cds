from utils import download_file
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd



def create_request(row,year):
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
def get_output_filename(row,dataset,year):

    var=row["filename_variable"]
    date=f"{year}-01-01_{year}-12-31"
    return f"{var}-{dataset}-{date}.nc"

def main():
    """
    Download  files for the specified variables and years.
    Parameters
    ----------
    row : pd.Series
       A row from the configuration file.
    dataset : str
        The dataset name.
   
    """
    dataset="reanalysis-era5-single-levels"
    variables_file_path = f"../requests/{dataset}.csv"
    df_parameters = pd.read_csv(variables_file_path)

    for index, row in df_parameters.iterrows():        
        dest_dir = Path(row["path_download"])/ dataset / row["filename_variable"]
        dest_dir.mkdir(parents=True, exist_ok=True)
        year_list = list(range(row["years_start"], row["years_end"]+1))
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for year in year_list:
                request = create_request(row,year)
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

if __name__ == "__main__":
    main()
