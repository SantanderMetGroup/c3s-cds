from utils import download_files




def get_output_filename(row,dataset,year):

    var=row["filename_variable"]
    date=f"{year}-01-01_{year}-12-31"
    return f"{var}_{dataset}_{date}.nc"

def create_request(row,year):
    var=row["cds_request_variable"]
    daily_statistic=row["cds_daily_statistic"]
    day=row["cds_day"]
    month=row["cds_month"]
    time_zone=row["cds_time_zone"]
    frequency=row["cds_frequency"]
    product_type=row["cds_product_type"]
    
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
    dataset = "derived-era5-single-levels-daily-statistics"
    variables_file_path = f"../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
