from utils import download_file
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd



def get_output_filename(row,dataset,year):

    var=row["filename_variable"]
    date=f"{year}-01-01_{year}-12-31"
    return f"{var}-{dataset}-{date}.nc"

def create_request(row,year):
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
    dataset="derived-era5-single-levels-daily-statistics"
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
