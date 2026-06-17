import argparse
import operations
import pandas as pd
import logging
import sys
import os
from dask.distributed import Client
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../utilities')))
from logging_utils import setup_logging
from utils import load_derived_dependencies, raw_condition, derived_condition, derived_condition_hourly_native
from utils_dask_slurm import load_slurm_dask_config
from utils_derived_pipeline import process_derived
logger = logging.getLogger(__name__)

MONTH_LIST = [f"{i:02d}" for i in range(1, 13)]

PARAMS_SLURM = load_slurm_dask_config()
logger.info(f"System parameters for Dask configuration: {PARAMS_SLURM}")

# =============================================================================
# Derived Variable Configuration Guide
# =============================================================================
#
# Instead of an if/elif chain, each variable is configured declaratively in
# the VAR_CONFIG dictionary below.  To add a new derived variable you only
# need to insert one entry — no new branches, no duplicated boilerplate.
#
# Key structure:  (variable_name, temporal_resolution) -> config dict
#
# Config fields:
#   func        — The operation function from operations.py that performs the
#                 computation (e.g. operations.rh_from_thermofeel).
#   cond        — Condition function(s) used to locate the input data for
#                 each dependency.  Can be a single callable (applied to all
#                 dependencies) or a list of callables (one per dependency).
#                 Common choices: raw_condition, derived_condition,
#                 derived_condition_hourly_native.
#   resampling  — Optional dict with "agg_freq" and "agg_func".  When set,
#                 the result is resampled after computation (e.g. hourly
#                 instantaneous → daily mean).
#   timing        — Optional boolean.  When True the processing time for each
#                   year is logged.
#   iterate_monthly — Optional boolean.  When True the work is split per month
#                   (12 iterations per year).  Defaults to False (yearly).
#
# Example — adding a hypothetical "myvar":
#
#     ("myvar", "hourly"): {
#         "func": operations.myvar_compute,
#         "cond": raw_condition,
#     },
#
# The framework (process_derived) handles the rest: file resolution,
# loading, preprocessing, computation, optional resampling, and saving.
# =============================================================================
VAR_CONFIG = {
    ("sfcwind", "daily"): {
        "func": operations.sfcwind_from_u_v,
        "cond": raw_condition,
        "resampling": {"agg_freq": "1D", "agg_func": "mean"},
    },
    ("sfcwind", "hourly"): {
        "func": operations.sfcwind_from_u_v,
        "cond": raw_condition,
        "iterate_monthly": True,
    },
    ("hurs", "hourly"): {
        "func": operations.rh_from_thermofeel,
        "cond": raw_condition,
        "iterate_monthly": True,
    },
    ("rsus", "hourly"): {
        "func": operations.rsus_from_rsds_rsns,
        "cond": raw_condition,
        "iterate_monthly": True,
    },
    ("rlus", "hourly"): {
        "func": operations.rlus_from_rlds_rlns,
        "cond": raw_condition,
        "iterate_monthly": True,
    },
    ("mrt", "hourly"): {
        "func": operations.mrt_from_rsus_rlus_rsds_rlds,
        "cond": [
            derived_condition,
            derived_condition,
            raw_condition,
            raw_condition,
        ],
        "timing": True,
        "iterate_monthly": True,
    },
    ("utci", "hourly"): {
        "func": operations.utci_from_t2m_sfcwind_hurs_mrt,
        "cond": [
            raw_condition,
            derived_condition_hourly_native,
            derived_condition,
            derived_condition,
        ],
        "timing": True,
        "iterate_monthly": True,
    },
}


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run reanalysis-era5-single-levels with optional variable/year/month filters.",
    )
    parser.add_argument("--year", type=int, default=None, help="Single year to process")
    parser.add_argument("--month", default=None, help="Single month to process (1-12 or 01-12)")
    parser.add_argument("--variable", default=None, help="Single filename_variable to process")
    return parser.parse_args()


def _normalize_month(month_arg):
    if month_arg is None:
        return None

    month_int = int(month_arg)
    if month_int < 1 or month_int > 12:
        raise ValueError(f"Invalid month '{month_arg}'. Expected values 1..12.")
    return f"{month_int:02d}"



def main():
    global MONTH_LIST

    setup_logging(force=True)
    args = _parse_args()

    selected_month = _normalize_month(args.month)
    if selected_month is not None:
        MONTH_LIST = [selected_month]

    client = Client(
    n_workers=1,
    threads_per_worker=PARAMS_SLURM["threads"],
    memory_limit=PARAMS_SLURM["memory_limit"]
)
    logger.info("Starting derived variable calculations for reanalysis-era5-single-levels")
    dataset="reanalysis-era5-single-levels"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    variables_file_path = os.path.join(script_dir, "..", "..", "requests", f"{dataset}.csv")
    df_parameters = pd.read_csv(variables_file_path)
    derived_dependencies = load_derived_dependencies()
    native_derived_condition = (df_parameters['product_type'] == 'derived') & (df_parameters['interpolation'] == 'native')
    derived_variables = df_parameters[native_derived_condition]['filename_variable']
    derived_variables_list = derived_variables.tolist()
    if args.variable:
        derived_variables_list = [var for var in derived_variables_list if var == args.variable]
        logger.info(f"Applied variable filter: {args.variable}")
    for var in derived_variables_list:
        logger.info(f"Calculating {var}")
        mask_var = (df_parameters['filename_variable'] == var) & (native_derived_condition)
        matches = df_parameters[mask_var]
        if matches.shape[0] == 0:
            raise KeyError(f"No row found: {var}/derived")

        # process each temporal_resolution (e.g., hourly and daily) so both get calculated.
        for _, var_row in matches.iterrows():
            # Create a list of years from start to end for this specific row
            # year_list = list(range(var_row["cds_years_start"].squeeze(), var_row["cds_years_end"].squeeze() + 1))
            if args.year is not None:
                year_list = [args.year]
            else:
                year_list = list(range(int(var_row["cds_years_start"]), int(var_row["cds_years_end"]) + 1))
            for year in year_list:

                dependencies = derived_dependencies.get(var, [])
                if not dependencies:
                    logger.warning(f"No dependencies declared for derived variable {var}. Skipping...")
                    continue
                logger.info(f"Derived variable {var} has dependencies: {dependencies}")

                key = (var, var_row["temporal_resolution"])
                cfg = VAR_CONFIG.get(key)
                if cfg is None:
                    raise ValueError(
                        f"Unexpected variable '{var}' with temporal resolution "
                        f"'{var_row['temporal_resolution']}'. "
                        f"Add an entry to VAR_CONFIG in this file."
                    )

                month_iter = MONTH_LIST if cfg.get("iterate_monthly") else [None]

                for month in month_iter:
                    if cfg.get("timing"):
                        start_time = time.time()

                    process_derived(
                        var,
                        dataset,
                        dependencies,
                        df_parameters,
                        var_row,
                        year,
                        cfg["func"],
                        cfg["cond"],
                        month=month,
                        resampling=cfg.get("resampling"),
                    )

                    if cfg.get("timing"):
                        end_time = time.time()
                        logger.info(
                            f"Processing time for {var} in year {year}: "
                            f"{end_time - start_time:.2f} seconds"
                        )
if __name__ == "__main__":
    main()
