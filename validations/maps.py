from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr


UTILITIES_DIR = Path(__file__).resolve().parents[1] / "scripts" / "utilities"
if str(UTILITIES_DIR) not in sys.path:
    sys.path.append(str(UTILITIES_DIR))

from logging_utils import setup_logging
from utils_fixes import convert_longitudes_to_minus180_180, get_lon_lat_names

logger = logging.getLogger(__name__)


TimePeriod = tuple[str, str]


def _resolve_input_paths(path_or_pattern: str | Path) -> list[str]:
    candidate = str(path_or_pattern)
    matched_paths = sorted(glob.glob(candidate))
    if matched_paths:
        return matched_paths

    path = Path(candidate)
    if path.exists():
        return [str(path)]

    raise FileNotFoundError(f"No files matched input path or pattern: {path_or_pattern}")


def _open_dataset(path_or_pattern: str | Path, chunks: dict[str, int] | None = None) -> xr.Dataset:
    paths = _resolve_input_paths(path_or_pattern)
    logger.info(f"Opening {len(paths)} file(s) from {path_or_pattern}")

    if len(paths) == 1:
        ds = xr.open_dataset(paths[0], chunks=chunks)
    else:
        ds = xr.open_mfdataset(paths, combine="by_coords", chunks=chunks)

    if "valid_time" in ds.dims and "time" not in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    if "latitude" in ds.coords and "lat" not in ds.coords:
        ds = ds.rename({"latitude": "lat"})
    if "longitude" in ds.coords and "lon" not in ds.coords:
        ds = ds.rename({"longitude": "lon"})

    lon_name, _ = get_lon_lat_names(ds)
    if lon_name is not None and float(ds[lon_name].max(skipna=True).load().item()) > 180.0:
        ds = convert_longitudes_to_minus180_180(ds)

    return ds


def _coord_name(ds: xr.Dataset | xr.DataArray, candidates: Iterable[str]) -> str:
    for name in candidates:
        if name in ds.coords or name in ds.dims:
            return name
    raise KeyError(f"None of the coordinate names {tuple(candidates)} were found.")


def _time_mean_field(
    path_or_pattern: str | Path,
    variable_name: str,
    time_period: TimePeriod,
    chunks: dict[str, int] | None = None,
) -> xr.DataArray:
    ds = _open_dataset(path_or_pattern, chunks=chunks)
    try:
        if variable_name not in ds.data_vars:
            raise KeyError(f"Variable '{variable_name}' not found in dataset. Available: {list(ds.data_vars)}")

        field = ds[variable_name]
        if "time" in field.dims:
            field = field.sel(time=slice(time_period[0], time_period[1]))
            if field.sizes.get("time", 0) == 0:
                raise ValueError(
                    f"No data found for variable '{variable_name}' in time period {time_period}."
                )
            field = field.mean(dim="time", keep_attrs=True)

        return field.load()
    finally:
        ds.close()


def _difference_limits(diff_field: xr.DataArray, quantile: float = 0.98) -> float:
    finite_values = np.asarray(diff_field.values)
    finite_values = finite_values[np.isfinite(finite_values)]
    if finite_values.size == 0:
        return 1.0

    limit = float(np.quantile(np.abs(finite_values), quantile))
    return limit if limit > 0 else 1.0


def make_single_map(
    field: xr.DataArray,
    ax: plt.Axes,
    title: str,
    cmap: str = "viridis",
    add_colorbar: bool = True,
) -> None:
    lon_name = _coord_name(field, ("lon", "longitude", "x"))
    lat_name = _coord_name(field, ("lat", "latitude", "y"))
    field.plot(ax=ax, x=lon_name, y=lat_name, cmap=cmap, add_colorbar=add_colorbar)
    ax.set_title(title)
    ax.set_xlabel(lon_name)
    ax.set_ylabel(lat_name)


def make_difference_map(
    field_a: xr.DataArray,
    field_b: xr.DataArray,
    ax: plt.Axes,
    title: str,
    cmap: str = "RdBu_r",
    add_colorbar: bool = True,
) -> xr.DataArray:
    aligned_a, aligned_b = xr.align(field_a, field_b, join="inner")
    diff_field = aligned_a - aligned_b
    limit = _difference_limits(diff_field)

    lon_name = _coord_name(diff_field, ("lon", "longitude", "x"))
    lat_name = _coord_name(diff_field, ("lat", "latitude", "y"))
    diff_field.plot(
        ax=ax,
        x=lon_name,
        y=lat_name,
        cmap=cmap,
        vmin=-limit,
        vmax=limit,
        add_colorbar=add_colorbar,
    )
    ax.set_title(title)
    ax.set_xlabel(lon_name)
    ax.set_ylabel(lat_name)
    return diff_field


def save_triple_map(
    file_path_a: str | Path,
    file_path_b: str | Path,
    variable_name_a: str,
    variable_name_b: str,
    time_period_a: TimePeriod,
    time_period_b: TimePeriod,
    output_path: str | Path,
    title_a: str | None = None,
    title_b: str | None = None,
    diff_title: str | None = None,
    chunks: dict[str, int] | None = None,
    figsize: tuple[int, int] = (18, 5),
    dpi: int = 150,
) -> Path:
    field_a = _time_mean_field(file_path_a, variable_name_a, time_period_a, chunks=chunks)
    field_b = _time_mean_field(file_path_b, variable_name_b, time_period_b, chunks=chunks)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(ncols=3, figsize=figsize, constrained_layout=True)
    make_single_map(
        field_a,
        axes[0],
        title_a or f"{variable_name_a} mean\n{time_period_a[0]} to {time_period_a[1]}",
    )
    make_single_map(
        field_b,
        axes[1],
        title_b or f"{variable_name_b} mean\n{time_period_b[0]} to {time_period_b[1]}",
    )
    make_difference_map(
        field_a,
        field_b,
        axes[2],
        diff_title or f"Difference: {variable_name_a} - {variable_name_b}",
    )

    fig.savefig(output, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved triple map to {output}")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate single maps for two datasets and a third map with their difference.",
    )
    parser.add_argument("file_path_a", help="First file path or glob pattern")
    parser.add_argument("file_path_b", help="Second file path or glob pattern")
    parser.add_argument("variable_name_a", help="Variable name for the first dataset")
    parser.add_argument("variable_name_b", help="Variable name for the second dataset")
    parser.add_argument("time_start_a", help="Start time for the first dataset selection")
    parser.add_argument("time_end_a", help="End time for the first dataset selection")
    parser.add_argument("time_start_b", help="Start time for the second dataset selection")
    parser.add_argument("time_end_b", help="End time for the second dataset selection")
    parser.add_argument("output_path", help="Output image path")
    return parser


def main() -> None:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()

    save_triple_map(
        file_path_a=args.file_path_a,
        file_path_b=args.file_path_b,
        variable_name_a=args.variable_name_a,
        variable_name_b=args.variable_name_b,
        time_period_a=(args.time_start_a, args.time_end_a),
        time_period_b=(args.time_start_b, args.time_end_b),
        output_path=args.output_path,
    )


if __name__ == "__main__":
    main()