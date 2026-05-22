from __future__ import annotations

import sys
from pathlib import Path


VALIDATIONS_DIR = Path(__file__).resolve().parent
UTILITIES_DIR = VALIDATIONS_DIR.parent / "scripts" / "utilities"

if str(VALIDATIONS_DIR) not in sys.path:
    sys.path.append(str(VALIDATIONS_DIR))

if str(UTILITIES_DIR) not in sys.path:
    sys.path.append(str(UTILITIES_DIR))


from logging_utils import setup_logging
from maps import save_triple_map


FILE_PATH_A = "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/derived-utci-historical/daily/native/mrt/mrt_derived-utci-historical_200001*.nc"
FILE_PATH_B = "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/derived/reanalysis-era5-single-levels/hourly/native/mrt/mrt_reanalysis-era5-single-levels_200001.nc"

VARIABLE_NAME_A = "mrt"
VARIABLE_NAME_B = VARIABLE_NAME_A

# Dataset A is daily, so the full day is selected.
TIME_PERIOD_A_LIST = [("2000-01-01 06:00", "2000-01-01 06:00"),("2000-01-01 00:00", "2000-01-31 23:59"),("2000-01-01 00:00", "2000-01-01 23:59")]

# Dataset B is hourly, so this selects the 06:00 timestep on 1 January 2000.
TIME_PERIOD_B_LIST = TIME_PERIOD_A_LIST
OUTPUT_NAME_LIST = [f"mrt_REFERENCE_vs_mrt_CALCULATED_{periods[0].replace(' ', '_').replace(':', '')}_to_{periods[1].replace(' ', '_').replace(':', '')}.png" for periods in TIME_PERIOD_A_LIST]
OUTPUT_PATH_LIST = [VALIDATIONS_DIR / "figures_validation" / name for name in OUTPUT_NAME_LIST]


def main() -> None:
    setup_logging()
    for TIME_PERIOD_A,TIME_PERIOD_B,OUTPUT_PATH in zip(TIME_PERIOD_A_LIST,TIME_PERIOD_B_LIST,OUTPUT_PATH_LIST):
        save_triple_map(
            file_path_a=FILE_PATH_A,
            file_path_b=FILE_PATH_B,
            variable_name_a=VARIABLE_NAME_A,
            variable_name_b=VARIABLE_NAME_B,
            time_period_a=TIME_PERIOD_A,
            time_period_b=TIME_PERIOD_B,
            output_path=OUTPUT_PATH,
            title_a=f"derived-utci-historical mrt\n{TIME_PERIOD_A[0]} to {TIME_PERIOD_A[1]}",
            title_b=f"ERA5-derived mrt\n{TIME_PERIOD_B[0]} to {TIME_PERIOD_B[1]}",
            diff_title=f"Difference: derived-utci-historical mrt - ERA5-derived mrt\n{TIME_PERIOD_A[0]} to {TIME_PERIOD_A[1]}",
        )


if __name__ == "__main__":
    main()