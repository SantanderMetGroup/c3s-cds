import os

def rename_files(directory):
    print(f"Launching renaming in {directory}")
    # Loop through all files in the specified directory
    for filename in os.listdir(directory):
        # Check if the file matches the expected pattern
        if filename.endswith(".nc"):
            # Use regular expression to find the year in the filename
            if len(filename.split("_"))==4:
                var=filename.split("_")[0]
                dataset=filename.split("_")[1]
                year= filename.split("_")[-2].split("-")[0]
                # Construct the new filename
                new_filename = f"{var}_{dataset}_{year}.nc"
                os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))
                print(f"Renamed: ")
                print(f"{filename}")
                print(f"{new_filename}")

project="reanalysis-era5-single-levels"
if project=="derived-era5-single-levels-daily-statistics":
    vars=["t2m","t2mn","t2mx","tp","u10","v10","d2m","ssrd","e","sp"]
elif project=="reanalysis-era5-single-levels":
    vars=["u10","v10"]
for var in vars:
    # Specify the directory containing your files
    directory = f"/lustre/gmeteo/WORK/DATA/C3S-CDS/ERA5_temp/raw/{project}/{var}/"
    rename_files(directory)
