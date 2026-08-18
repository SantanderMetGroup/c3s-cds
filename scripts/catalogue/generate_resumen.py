import os
import pandas as pd
import logging

from logging_utils import setup_logging

logger = logging.getLogger(__name__)

setup_logging()

# Paths corrected to match produce_catalog.py output
# produce_catalog.py saves images to ../../catalogues/images
images_folder = "../../catalogues/images"
# Prefer the docs CSV used by GitHub Pages, while keeping backward compatibility
all_catalogues_csv_candidates = [
    "../../docs/data/all_catalogues.csv",
    "../../catalogues/catalogues/all_catalogues.csv",
]
# Write the generated markdown into the catalogues folder
md_file = "../../catalogues/README.md"

def get_existing_csv_path(csv_paths):
    for csv_path in csv_paths:
        if os.path.exists(csv_path):
            return csv_path
    return None

def list_image_dataset_ids(images_dir):
    if not os.path.exists(images_dir):
        return []
    return sorted(
        image_name.replace("_catalogue.png", "")
        for image_name in os.listdir(images_dir)
        if image_name.endswith("_catalogue.png")
    )

all_catalogues_csv = get_existing_csv_path(all_catalogues_csv_candidates)
csv_dataset_ids = []
df = None

if all_catalogues_csv:
    df = pd.read_csv(all_catalogues_csv)
    if "dataset" in df.columns:
        csv_dataset_ids = sorted(
            {
                str(dataset).strip()
                for dataset in df["dataset"].dropna()
                if str(dataset).strip()
            }
        )
    else:
        logger.warning(
            "CSV file '%s' does not contain a 'dataset' column.",
            all_catalogues_csv,
        )

image_dataset_ids = list_image_dataset_ids(images_folder)
if csv_dataset_ids:
    dataset_ids = sorted(set(csv_dataset_ids).union(image_dataset_ids))
else:
    # Backward-compatible fallback: keep image-only behavior when CSV is absent
    dataset_ids = image_dataset_ids

with open(md_file, "w", encoding="utf-8") as f:
    f.write("# Catalogue Overview\n\n")

    rendered_dataset_ids = []
    if dataset_ids:
        for dataset_id in dataset_ids:
            image_name = f"{dataset_id}_catalogue.png"
            image_path = os.path.join(images_folder, image_name)
            title = f"{dataset_id.replace('_', ' ')} catalogue"
            f.write(f"## {title}\n\n")
            if os.path.exists(image_path):
                # Use relative path from the markdown file to the images
                rel_path = os.path.relpath(image_path, os.path.dirname(md_file))
                f.write(f"![{image_name}]({rel_path})\n\n")
            else:
                f.write("Catalogue image not available yet.\n\n")
            rendered_dataset_ids.append(dataset_id)
    elif not os.path.exists(images_folder):
        f.write("No images folder found.\n\n")
    else:
        f.write("No catalogue images found.\n\n")

    if csv_dataset_ids:
        # Regression guard: datasets from CSV must always be represented, even without an image.
        missing_dataset_sections = sorted(set(csv_dataset_ids) - set(rendered_dataset_ids))
        if missing_dataset_sections:
            raise RuntimeError(
                "Missing dataset sections from CSV: "
                + ", ".join(missing_dataset_sections)
            )

    # Añadir la tabla final
    if df is not None:
        f.write("## All Catalogues Table\n\n")
        f.write(df.to_markdown(index=False))
    else:
        f.write("No all_catalogues.csv found.\n")

logger.info(f"Markdown file '{md_file}' generated successfully!")
