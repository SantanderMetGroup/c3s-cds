import os
import zipfile
import re

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





def extract_multizip_files(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # detects YEAR in ZIP name (4 digits)
    zip_year_pattern = re.compile(r'(\d{4})')

    # detects full timestamp in nc file
    nc_date_pattern = re.compile(r'(\d{8})\d{4}\.nc$')

    for zip_name in os.listdir(input_dir):
        if not zip_name.endswith(".zip"):
            continue

        zip_path = os.path.join(input_dir, zip_name)

        # ---- find original date in ZIP name ----
        m_zip = zip_year_pattern.search(zip_name)
        if not m_zip:
            print(f"Skipping (no year in zip): {zip_name}")
            continue

        original_date = m_zip.group(1)

        # base name without extension
        base_name = zip_name.replace(".zip", "")

        print(f"Processing: {zip_name}")

        with zipfile.ZipFile(zip_path, "r") as z:
            for nc_file in z.namelist():
                if not nc_file.endswith(".nc"):
                    continue

                # ---- extract real daily date ----
                m_nc = nc_date_pattern.search(nc_file)
                if not m_nc:
                    print(f"Skipping (no date): {nc_file}")
                    continue

                new_date = m_nc.group(1)

                # ---- replace original date in basename ----
                new_base = base_name.replace(original_date, new_date)

                new_name = f"{new_base}.nc"
                new_path = os.path.join(output_dir, new_name)

                with z.open(nc_file) as src, open(new_path, "wb") as dst:
                    dst.write(src.read())

                print(f"Saved: {new_name}")

    print("Done.")



sfcwind_era5=False
satellite_sea_ice_concentration=True

if sfcwind_era5:
    project="reanalysis-era5-single-levels"
    if project=="derived-era5-single-levels-daily-statistics":
        vars=["t2m","t2mn","t2mx","tp","u10","v10","d2m","ssrd","e","sp"]
    elif project=="reanalysis-era5-single-levels":
        vars=["u10","v10"]
    for var in vars:
        # Specify the directory containing your files
        directory = f"/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/{project}/{var}/"
        rename_files(directory)

if satellite_sea_ice_concentration:
    input_dir = "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/satellite-sea-ice-concentration/daily/native/ice_conc/"
    output_dir = "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/satellite-sea-ice-concentration/daily/native/ice_conc/"

    extract_multizip_files(input_dir, output_dir)