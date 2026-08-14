from pathlib import Path
import glob
import logging
import zipfile
import sys,os
from c3s_atlas.utils import extract_zip_and_delete
# Add the project root (c3s-cds)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
# Add the utilities directory so inner modules can resolve each other
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts/utilities')))
from utils_download import handle_special_zip



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

root_SST_SAT = Path(
    "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/satellite-sea-surface-temperature/monthly/native/analysed_sst/"
)
root_ORAS5 = Path(
    "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/reanalysis-oras5/monthly/native/ileadfra/"
)
def extract_all_netcdfs_in_dir(root_dir: Path):
    zips = sorted(root_dir.glob("*.zip"))
    if not zips:
        logger.info("No .zip files found in %s", root_dir)
        return

    for z in zips:
        logger.info("Processing zip: %s", z)
        extract_zip_and_delete(Path(z))
def extract_multizip_files_in_dir(root_dir: Path):
    zips = sorted(root_dir.glob("*.zip"))
    if not zips:
        logger.info("No .zip files found in %s", root_dir)
        return

    for z in zips:
        logger.info("Processing multi-netcdf zip: %s", z)
        handle_special_zip(Path(z),delete_zip=False, request_frequency="yearly", extracted_frequency="monthly")

if __name__ == "__main__":
    #extract_all_netcdfs_in_dir(root_SST_SAT)
    extract_multizip_files_in_dir(root_ORAS5)