from utils import download_files



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
    dataset="reanalysis-era5-single-levels"
    variables_file_path = f"../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename)

if __name__ == "__main__":
    main()
