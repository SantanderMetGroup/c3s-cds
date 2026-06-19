from pathlib import Path
import glob
import logging
import zipfile

from c3s_atlas.utils import extract_zip_and_delete

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

root = Path(
    "/lustre/gmeteo/WORK/DATA/C3S-CDS/CDS-Curated-Data/raw/satellite-sea-surface-temperature/monthly/native/analysed_sst/"
)

def extract_all_netcdfs_in_dir(root_dir: Path):
    zips = sorted(root_dir.glob("*.zip"))
    if not zips:
        logger.info("No .zip files found in %s", root_dir)
        return

    for z in zips:
        logger.info("Processing zip: %s", z)
        extract_zip_and_delete(Path(z))


if __name__ == "__main__":
    extract_all_netcdfs_in_dir(root)