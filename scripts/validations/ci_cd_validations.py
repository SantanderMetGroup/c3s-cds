import os
import sys
import argparse
import glob
import pandas as pd
import xarray as xr
import numpy as np

def validate_outliers(catalog_path, z_threshold=3.0, max_outlier_percent=1.0):
    """
    Iterates through the catalog and validates the number of outliers using Z-score.
    If a dataset has an outlier percentage greater than max_outlier_percent, 
    the validation fails.
    """
    df = pd.read_csv(catalog_path)
    failed = False
    
    for index, row in df.iterrows():
        dataset_name = row['dataset']
        expected_var = row['variable']
        data_path = row['data_path']
        
        if pd.isna(data_path) or not isinstance(data_path, str):
            continue
            
        print(f"Validating dataset: {dataset_name} | Variable: {expected_var}")
        
        file_pattern = os.path.join(data_path, "*.nc")
        files = glob.glob(file_pattern)
        
        if not files:
            print(f" Warning: No NetCDF files found in {data_path}")
            continue
            
        try:
            # Open all years together by combining coordinates
            ds = xr.open_mfdataset(files, combine='by_coords')
        except Exception as e:
            print(f" Error opening files in {data_path}: {e}")
            failed = True
            continue

        var_name = expected_var if expected_var in ds.data_vars else list(ds.data_vars)[0]
        var_data = ds[var_name]
        
        print("  -> Calculating mean, standard deviation, and Z-scores (this may take a while...)")
        
        try:
            # Perform calculation ignoring NaNs
            mean_val = var_data.mean().compute().item()
            std_val = var_data.std().compute().item()
            
            if std_val == 0:
                print("  Warning: Standard deviation is 0 (constant data).")
                continue
                
            # Calculate the number of non-null values
            total_elements = var_data.count().compute().item()
            
            # Calculate outliers. Dask will evaluate this condition over the entire dataset
            outliers = (np.abs((var_data - mean_val) / std_val) > z_threshold)
            num_outliers = outliers.sum().compute().item()
            
            if total_elements > 0:
                outlier_percent = (num_outliers / total_elements) * 100
            else:
                outlier_percent = 0.0
                
            print(f"  -> Outliers found: {num_outliers} / {total_elements} ({outlier_percent:.4f}%)")
            
            if outlier_percent > max_outlier_percent:
                print(f" FAIL: The outlier percentage ({outlier_percent:.2f}%) exceeds the allowed limit ({max_outlier_percent}%).")
                failed = True
            else:
                print(" PASS: Outlier percentage is within the threshold.")
                
        except Exception as e:
            print(f" Error during mathematical calculation for {dataset_name}: {e}")
            failed = True
        finally:
            ds.close()

    if failed:
        print("\n Validation failed due to an unacceptable amount of outliers or processing errors.")
        sys.exit(1)
    else:
        print("\n All datasets passed the outlier validation.")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate large-scale NetCDFs checking for atypical outliers.")
    parser.add_argument("--catalog", default="catalogues/catalogues/all_catalogues.csv", help="Path to the CSV catalog")
    parser.add_argument("--z", type=float, default=3.0, help="Z-score magnitude considered as an outlier (e.g. 3.0)")
    parser.add_argument("--max-outliers", type=float, default=1.0, help="Maximum outlier percentage tolerated without failing (e.g. 1.0)")
    
    args = parser.parse_args()
    validate_outliers(args.catalog, z_threshold=args.z, max_outlier_percent=args.max_outliers)