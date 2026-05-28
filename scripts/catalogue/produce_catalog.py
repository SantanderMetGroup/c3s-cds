"""
Catalogue Generation Script

Processes raw and derived dataset requests, evaluates file availability,
constructs status matrices, and generates visualization catalogues.
"""

import os
import glob
import logging
import sys

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

sys.path.append('../utilities')
from utils import (
    build_output_path,
    load_output_path_from_row,
    load_derived_dependencies
)
from logging_utils import setup_logging

# ------------------------------------------------------------
# Setup & Constants
# ------------------------------------------------------------
logger = logging.getLogger(__name__)
setup_logging()

CATALOGUE_DIR = "../../catalogues/catalogues"
IMAGE_DIR = "../../catalogues/images"
REQUEST_DIR = "../../requests"

os.makedirs(CATALOGUE_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)

TYPE_DATA_LIST = ["raw", "derived"]

FUSION_RULES = {
    "ice": ["cds_cdr_type", "cds_version"],
    "surface-radiation": ["cds_climate_data_record_type", "cds_version"],
    "soil-moisture": ["cds_type_of_record", "cds_version"],
}

VALUE_MAP = {"downloaded": 0, "partial": 1, "not_downloaded": 2}

# ------------------------------------------------------------
# File System Utilities
# ------------------------------------------------------------

def check_nc_file_for_year(directory, year):
    """Check if any NetCDF files matching the given year exist in the directory."""
    return bool(glob.glob(os.path.join(directory, f"*{year}*.nc")))


def get_earliest_and_latest_dates(directory):
    """Extract and return the minimum and maximum integer date components found in .nc filenames."""
    logger.info(f"Getting earliest and latest dates from directory: {directory}")
    files = glob.glob(os.path.join(directory, "*.nc"))
    dates = []

    for f in files:
        base = os.path.basename(f)
        try:
            # Check standard location index 2
            part = base.split("_")[2].replace(".nc", "").replace("-", "")
            if part.isdigit():
                dates.append(int(part))
        except Exception:
            continue

    return (min(dates), max(dates)) if dates else (None, None)


def check_origin_path(row, data_path):
    """Determine the source data path or joined dependency paths for derived dataset tracking."""
    if row["input_path"] == "CDS":
        return "CDS"

    if row["product_type"] != "derived":
        return data_path

    deps = load_derived_dependencies().get(str(row["filename_variable"]), [])
    if not isinstance(deps, list) or not deps:
        deps = [row["filename_variable"]]

    paths = [
        str(build_output_path(row["input_path"], row["dataset"], "raw", row["temporal_resolution"], "native", str(d)))
        for d in deps
    ]
    return ";".join(paths)


# ------------------------------------------------------------
# Data Processing Pipeline
# ------------------------------------------------------------

def apply_fusion_rules(df, basename):
    """Group rows and aggregate year boundaries if filename matches specific tracking fusion rules in cases where a variable is divided across multiple versions."""
    for key, cols in FUSION_RULES.items():
        if key in basename:
            drop_cols = [c for c in cols if c in df.columns]
            group_cols = [c for c in df.columns if c not in drop_cols + ["cds_years_start", "cds_years_end"]]
            return df.groupby(group_cols, dropna=False, as_index=False).agg({
                "cds_years_start": "min",
                "cds_years_end": "max"
            })
    return df


def create_auxiliar_df(data):
    """Build an auxiliary DataFrame containing calculated status metadata and physical file checks."""
    rows = []
    for _, row in data.iterrows():
        data_path = str(load_output_path_from_row(row))
        origin_path = check_origin_path(row, data_path)
        
        start_exists = check_nc_file_for_year(data_path, row['cds_years_start'])
        final_exists = check_nc_file_for_year(data_path, row['cds_years_end'])
        earliest, latest = get_earliest_and_latest_dates(data_path)

        new_row = {
            'variable': row['filename_variable'],
            'model': row['model'] if row['dataset_type'] == 'projections' else 'None',
            'experiment': row['experiment'] if row['dataset_type'] == 'projections' else 'historical',
            'dataset': row['dataset'],
            'dataset_type': row['dataset_type'],
            'product_type': row['product_type'],
            'temporal_resolution': row['temporal_resolution'],
            'interpolation': row['interpolation'],
            'data_path': data_path,
            'origin_path': origin_path,
            'start_file_exists': start_exists,
            'final_file_exists': final_exists,
            'earliest_date': earliest,
            'latest_date': latest,
            'script': row['script']
        }
        
        # Capture optional dynamic columns for handling step splits safely
        for opt_col in ['cds_climate_data_record_type', 'cds_cdr_type', 'cds_type_of_record']:
            if opt_col in row:
                new_row[opt_col] = row[opt_col]
                
        rows.append(new_row)

    df = pd.DataFrame(rows)
    return df, df['dataset_type'].iloc[0] if not df.empty else None


# ------------------------------------------------------------
# Matrix Construction (PRESERVES row expansion logic)
# ------------------------------------------------------------

def build_catalogue_matrix(aux_df, dataset_type, project):
    """Construct a status mapping matrix with a MultiIndex structure, duplicating rows on dynamic variable splits."""
    if dataset_type in ["reanalysis", "observation"]:
        simss = [project]
        scess = ["historical"]
    else:
        simss = list(aux_df['experiment'].unique())
        scess = list(aux_df['experiment'].unique())

    varss = aux_df['variable'].unique()
    cols = pd.MultiIndex.from_tuples([(v, s) for v in varss for s in scess])
    df_final = pd.DataFrame(index=simss, columns=cols)

    row_split_columns = ['temporal_resolution', 'cds_climate_data_record_type', 'cds_cdr_type', 'cds_type_of_record']
    
    # Mirroring the exact original process_variable execution map
    for ind in list(df_final.index):
        for col in df_final.columns:
            variable = col[0]
            filtered = aux_df[aux_df['variable'] == variable]
            
            if filtered.empty:
                df_final.loc[ind, col] = None
                continue
                
            present_splits = [c for c in row_split_columns if c in filtered.columns]
            has_multiple_rows = len(filtered) > 1

            if has_multiple_rows:
                logger.info(f"WARNING: Multiple rows found for variable '{variable}'")
            
            for _, row_aux in filtered.iterrows():
                # Compute original mapping values
                if row_aux['start_file_exists'] and row_aux['final_file_exists']:
                    val = 0
                elif pd.notna(row_aux['earliest_date']) or pd.notna(row_aux['latest_date']):
                    val = 1
                else:
                    val = 2
                
                if has_multiple_rows:
                    new_row = df_final.loc[ind].copy() if ind in df_final.index else pd.Series(index=df_final.columns, dtype=object)
                    new_row[col] = val
                    for c in present_splits:
                        new_row[c] = row_aux[c]
                    df_final = pd.concat([df_final, pd.DataFrame([new_row])], ignore_index=True)
                else:
                    df_final.loc[ind, col] = val

    return df_final, varss, scess


# ------------------------------------------------------------
# Plot 
# ------------------------------------------------------------

def plot_catalogue(df, varss, scess, project):
    """Generate and save a visual matrix plot map representing data availability statuses."""
    colors = ["#43A055", "#FF8000", "#FF0000"]

    mat_values = np.array(df.values, dtype='float64')
    fig, ax = plt.subplots(figsize=(len(df.columns), len(df.index)))

    list_values = ['downloaded', 'partial', 'not downloaded']
    unique_vals = np.unique(mat_values[~np.isnan(mat_values)])
    
    if len(unique_vals) < len(colors):
        colors = colors[:len(unique_vals)]
        orig_dict = {"downloaded": 0, "partial": 1, "not_downloaded": 2}
        for k, v in orig_dict.items():
            if v not in mat_values:
                # Mirroring primitive list removals safely
                alt_k = k.replace('_', ' ')
                if alt_k in list_values: list_values.remove(alt_k)
                elif k in list_values: list_values.remove(k)

    cmap = mpl.colors.ListedColormap(colors)
    cmap.set_bad(color='w', alpha=1.)

    # Keep layout strictly aligned via exact properties
    pl = ax.pcolormesh(mat_values, cmap=cmap, edgecolors='w', linewidths=0.005, vmin=0, vmax=len(scess))

    # Ticks & Labels formatting matching exactly
    ax.set_xticks(np.arange(0, len(df.columns), len(scess)) + len(scess)/2)
    ax.set_xticklabels([str(n) for n in varss], fontsize=10)
    ax.xaxis.tick_top()

    ax.set_yticks(np.arange(0.5, len(df.index) + 0.5))
    ax.set_yticklabels(['_'.join(str(col).split('_')[:2]) for col in df.index], fontsize=10)

    # Grid line separators
    for yv in np.arange(0, len(df.columns), len(scess)) + len(scess):
        ax.plot((yv, yv), (0, len(df.index)), 'k', linewidth=10)

    # Dynamic colorbar structural mapping blocks
    cbaxes = fig.add_axes([0.25, 0, 0.4, 0.08])
    cbar = fig.colorbar(pl, orientation='horizontal', cax=cbaxes, ticks=np.arange(1/(len(list_values)+1), 1, 1/(len(list_values))), pad=0.1)
    cbar.ax.set_xticklabels(list_values, fontsize=5)

    plt.savefig(f"{IMAGE_DIR}/{project}_catalogue.png", bbox_inches='tight', dpi=300)
    plt.close()


# ------------------------------------------------------------
# Core Execution Loop
# ------------------------------------------------------------

def process_csv_file(file_path, type_data):
    """Execute pipeline steps on an individual CSV request file to update data records and generate plots."""
    data = pd.read_csv(file_path)
    basename = os.path.basename(file_path)
    
    data = apply_fusion_rules(data, basename)
    data = data[data['product_type'] == type_data]
    if data.empty:
        return

    aux_df, dataset_type = create_auxiliar_df(data)
    project = os.path.splitext(basename)[0]

    df_final, varss, scess = build_catalogue_matrix(aux_df, dataset_type, project)
    
    aux_df.to_csv(os.path.join(CATALOGUE_DIR, f"{project}_{type_data}_catalogue.csv"), index=False)

    if type_data == "raw":
        plot_catalogue(df_final, varss, scess, project)


def main():
    """Main execution point to discover, process, and combine all dataset request catalogs."""
    for type_data in TYPE_DATA_LIST:
        for filename in os.listdir(REQUEST_DIR):
            if "cordex" in filename or not filename.endswith('.csv'):
                continue
            process_csv_file(os.path.join(REQUEST_DIR, filename), type_data)

    # Global concatenation assembly pass
    dataframes = []
    for filename in os.listdir(CATALOGUE_DIR):
        if filename.endswith('.csv') and filename != "all_catalogues.csv":
            df = pd.read_csv(os.path.join(CATALOGUE_DIR, filename))
            df['earliest_date'] = pd.to_numeric(df['earliest_date'], errors='coerce').astype('Int64')
            df['latest_date'] = pd.to_numeric(df['latest_date'], errors='coerce').astype('Int64')
            dataframes.append(df)

    if dataframes:
        pd.concat(dataframes, ignore_index=True).to_csv(
            os.path.join(CATALOGUE_DIR, "all_catalogues.csv"), index=False
        )
        logger.info("All catalogues compiled successfully.")


if __name__ == "__main__":
    main()