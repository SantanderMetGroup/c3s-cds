#!/usr/bin/env python3
"""
Script to create all folder structures corresponding to the requests in the CSV files.

This script reads all CSV files in the requests/ directory and creates the 
complete directory structure without downloading or calculating any data.

Usage:
    python scripts/create_folder_structure.py [--dry-run]

Options:
    --dry-run    Show what directories would be created without actually creating them
"""

import os
import sys
import argparse
from pathlib import Path
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def build_output_path(base_path, dataset, product_type, temporal_resolution, interpolation, variable):
    """
    Build the output path following the directory structure.
    
    Structure: {base_path}/{product_type}/{dataset}/{temporal_resolution}/{interpolation}/{variable}/
    
    Parameters
    ----------
    base_path : str or Path
        Base directory path
    dataset : str
        Dataset name (e.g., 'reanalysis-era5-single-levels')
    product_type : str
        Type of product: 'raw', 'derived', or 'interpolated'
    temporal_resolution : str
        Temporal resolution: 'hourly', 'daily', '3hourly', '6hourly', 'monthly', etc.
    interpolation : str
        Interpolation method: 'native' (for non-interpolated), 'gr006', etc.
    variable : str
        Variable name (e.g., 'u10', 'v10', 'sfcwind')
    
    Returns
    -------
    Path
        Full output path
    """
    return Path(base_path) / product_type / dataset / temporal_resolution / interpolation / variable


def create_directories_from_csv(csv_file, dry_run=False):
    """
    Create directory structure from a single CSV file.
    
    Parameters
    ----------
    csv_file : str or Path
        Path to the CSV file
    dry_run : bool
        If True, only print what would be created without creating directories
    
    Returns
    -------
    list
        List of directories that were created (or would be created in dry-run mode)
    """
    csv_path = Path(csv_file)
    dataset_name = csv_path.stem  # e.g., 'reanalysis-era5-single-levels'
    
    logging.info(f"Processing: {csv_path.name}")
    
    # Read the CSV file
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        logging.error(f"Error reading {csv_file}: {e}")
        return []
    
    # Verify required columns exist
    required_columns = ['dataset', 'filename_variable', 'output_path', 
                        'product_type', 'temporal_resolution', 'interpolation']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.error(f"Missing required columns in {csv_file}: {missing_columns}")
        return []
    
    created_dirs = []
    
    # Process each row
    for index, row in df.iterrows():
        try:
            # Build the directory path
            directory = build_output_path(
                row['output_path'],
                row['dataset'],
                row['product_type'],
                row['temporal_resolution'],
                row['interpolation'],
                row['filename_variable']
            )
            
            # Create or report directory
            if dry_run:
                logging.info(f"  [DRY-RUN] Would create: {directory}")
            else:
                directory.mkdir(parents=True, exist_ok=True)
                logging.info(f"  Created: {directory}")
            
            created_dirs.append(str(directory))
            
        except Exception as e:
            logging.error(f"  Error processing row {index}: {e}")
            logging.error(f"  Row data: {row.to_dict()}")
            continue
    
    return created_dirs


def main():
    """Main function to process all CSV files in the requests directory."""
    parser = argparse.ArgumentParser(
        description='Create folder structure from CSV request files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what directories would be created without actually creating them'
    )
    parser.add_argument(
        '--requests-dir',
        type=str,
        default='../requests',
        help='Path to the requests directory (default: ../requests)'
    )
    
    args = parser.parse_args()
    
    # Determine the requests directory path
    script_dir = Path(__file__).parent
    requests_dir = (script_dir / args.requests_dir).resolve()
    
    if not requests_dir.exists():
        logging.error(f"Requests directory not found: {requests_dir}")
        sys.exit(1)
    
    logging.info("=" * 80)
    if args.dry_run:
        logging.info("DRY-RUN MODE: No directories will be created")
    else:
        logging.info("Creating folder structure from CSV files")
    logging.info("=" * 80)
    logging.info(f"Requests directory: {requests_dir}")
    logging.info("")
    
    # Find all CSV files in the requests directory
    csv_files = sorted(requests_dir.glob('*.csv'))
    
    if not csv_files:
        logging.warning(f"No CSV files found in {requests_dir}")
        sys.exit(0)
    
    logging.info(f"Found {len(csv_files)} CSV file(s)")
    logging.info("")
    
    # Process each CSV file
    all_created_dirs = []
    for csv_file in csv_files:
        created_dirs = create_directories_from_csv(csv_file, dry_run=args.dry_run)
        all_created_dirs.extend(created_dirs)
        logging.info("")
    
    # Summary
    logging.info("=" * 80)
    logging.info(f"Summary:")
    logging.info(f"  Total CSV files processed: {len(csv_files)}")
    logging.info(f"  Total directories {'that would be created' if args.dry_run else 'created'}: {len(all_created_dirs)}")
    
    # Show unique paths by category
    raw_dirs = [d for d in all_created_dirs if '/raw/' in d]
    derived_dirs = [d for d in all_created_dirs if '/derived/' in d]
    interpolated_dirs = [d for d in all_created_dirs if '/interpolated/' in d]
    
    logging.info(f"  - Raw data directories: {len(raw_dirs)}")
    logging.info(f"  - Derived data directories: {len(derived_dirs)}")
    logging.info(f"  - Interpolated data directories: {len(interpolated_dirs)}")
    logging.info("=" * 80)
    
    if args.dry_run:
        logging.info("")
        logging.info("Run without --dry-run to actually create the directories")


if __name__ == "__main__":
    main()
