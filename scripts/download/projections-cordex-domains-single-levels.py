import sys
sys.path.append('../utilities')
from utils import download_files


def build_year_windows(row):
    start = int(row["cds_years_start"])
    end = int(row["cds_years_end"])
    rcm_model = str(row.get("rcm_model", "")).lower()

    # GERICS requests are expected in 5-year windows (last window can be shorter).
    if "gerics" in rcm_model:
        start_years = list(range(start, end + 1, 5))
        end_years = [min(s + 4, end) for s in start_years]
        return [str(y) for y in start_years], [str(y) for y in end_years]

    # Default: one full-period window.
    return [str(start)], [str(end)]

def get_output_filename(row, dataset, year=None):
    var = row["filename_variable"]
    domain = row.get("domain", "south_america")
    experiment = row.get("experiment", "historical")
    horizontal_resolution = row.get("horizontal_resolution", "0_22_degree_x_0_22_degree")
    gcm_model = row.get("gcm_model", "mohc_hadgem2_es")
    rcm_model = row.get("rcm_model", "gerics_remo2015")
    ensemble_member = row.get("ensemble_member", "r1i1p1")
    date = f"{row['cds_years_start']}_{row['cds_years_end']}"
    return f"{var}_{domain}_{horizontal_resolution}_{gcm_model}_{ensemble_member}_{rcm_model}_{experiment}_{dataset}_{date}.zip"

def create_request(row, year=None):
    var = row["cds_request_variable"]
    temporal_resolution = row.get("temporal_resolution", "daily_mean")
    domain = row.get("domain", "south_america")
    experiment = row.get("experiment", "historical")
    horizontal_resolution = row.get("horizontal_resolution", "0_22_degree_x_0_22_degree")
    gcm_model = row.get("gcm_model", "mohc_hadgem2_es")
    rcm_model = row.get("rcm_model", "gerics_remo2015")
    ensemble_member = row.get("ensemble_member", "r1i1p1")
    
    start_years, end_years = build_year_windows(row)

    return {
        "variable": [var],
        "domain": [domain],
        "experiment": [experiment],
        "horizontal_resolution": [horizontal_resolution],
        "gcm_model": [gcm_model],
        "rcm_model": [rcm_model],
        "ensemble_member": [ensemble_member],
        "temporal_resolution": [temporal_resolution],
        "start_year": start_years,
        "end_year": end_years,

    }

def main():
    dataset = "projections-cordex-domains-single-levels"
    variables_file_path = f"../../requests/{dataset}.csv"
    download_files(dataset, variables_file_path, create_request, get_output_filename, year_request=False)

if __name__ == "__main__":
    main()