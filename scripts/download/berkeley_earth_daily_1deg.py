import sys
import pandas as pd

sys.path.append("../utilities")

import logging
from logging_utils import setup_logging
logger = logging.getLogger(__name__)
from utils_download import download_file
from utils import build_output_path


PERIODS = [
    1880,
    1890,
    1900,
    1910,
    1920,
    1930,
    1940,
    1950,
    1960,
    1970,
    1980,
    1990,
    2000,
    2010,
    2020,
]


def main():

    setup_logging()

    dataset = "berkeley_earth_daily_1deg"

    variables_file_path = f"../../requests_external/{dataset}.csv"

    df_parameters = pd.read_csv(variables_file_path)

    for period in PERIODS:

        logger.info(
            f"Starting download workflow for {dataset} - {period}"
        )

        for index, row in df_parameters.iterrows():

            if row["product_type"] != "raw":
                continue

            dest_dir = build_output_path(
                row["output_path"],
                dataset,
                row["product_type"],
                row["temporal_resolution"],
                row["interpolation"],
                row["filename_variable"],
            )

            dest_dir.mkdir(parents=True, exist_ok=True)

            variable = row["filename_variable"]

            filename = (
                f"Complete_{variable}_Daily_LatLong1_{period}.nc"
            )

            url = row["input_path"] + filename

            logger.info(
                f"Downloading {filename} for {variable}"
            )

            download_file(url, dest_dir)


if __name__ == "__main__":
    main()