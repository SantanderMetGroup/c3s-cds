from utils import download_files




def load_times(row):
    #First kewy word is for product type, second for time and 3rd for lead_time
    time_leadtime_mapping = {
        ("analysis","daily"):
          (
            ["06:00"],
            ["None"]#In cerra-land analysis for pr and accumulated vars the analysis at 6 give daily values
        )
    }

    # Retrieve the time and leadtime_hour based on the row values
    if row["cds_product_type"] == "analysis":
        result = time_leadtime_mapping.get((row["cds_product_type"],row["cds_time"]), ([], ["None"]))
    if  len(result)<2:
        raise ValueError(
            f"Time or leadtime_hour is empty; the keyword combination {row['cds_product_type']}- {row['cds_time']} is not supported."
        )


    return result



def create_request(row,year,month="all"):
    var=row["cds_request_variable"]
    day=row["cds_day"]

    data_format=row["cds_data_format"]
    product_type=row["cds_product_type"]
    level_type=row["cds_level_type"]
    time,leadtime_hour=load_times(row)


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
    dict_request={
        "variable": [var],
        "product_type": [product_type],
        "level_type": [level_type],
        "year": [year],
        "month": month,
        "day":day,
        "data_format": data_format,
        "time":time,
        "leadtime_hour":leadtime_hour
    }
    if leadtime_hour==["None"]:
        del dict_request["leadtime_hour"]
    return dict_request
def get_output_filename(row,dataset,year,month):
    var=row["filename_variable"]
    date=f"{year}{month}"
    return f"{var}_{dataset}_{date}.nc"

def main():
    dataset="reanalysis-cerra-land"
    variables_file_path = f"../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename,montlhy_request=True)

if __name__ == "__main__":
    main()
