
import sys
sys.path.append('../utilities')
import logging
from logging_utils import setup_logging
logger = logging.getLogger(__name__)
from utils_download import download_files_external





def main():
    setup_logging()
    dataset="cpc"

    logger.info(f"Starting download workflow for {dataset}")
    variables_file_path = f"../../requests_external/{dataset}.csv"
    download_files_external(dataset, variables_file_path,selection_pattern=".nc")
if __name__ == "__main__":
    main()