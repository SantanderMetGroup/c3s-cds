import os
import pandas as pd

# Paths corrected to match produce_catalog.py output
# produce_catalog.py saves images to ../../catalogues/images
images_folder = "../../catalogues/images"
# produce_catalog.py writes the concatenated CSV to ../../catalogues/catalogues/all_catalogues.csv
all_catalogues_csv = "../../catalogues/catalogues/all_catalogues.csv"
# Write the generated markdown into the catalogues folder
md_file = "../../catalogues/README.md"

with open(md_file, "w", encoding="utf-8") as f:
    f.write("# Catalogue Overview\n\n")

    # Añadir imágenes
    if os.path.exists(images_folder):
        image_files = sorted([img for img in os.listdir(images_folder) if img.endswith(".png")])
        for img in image_files:
            title = img.replace('_', ' ').replace('.png','')
            # Use relative path from the markdown file to the images
            rel_path = os.path.relpath(os.path.join(images_folder, img), os.path.dirname(md_file))
            f.write(f"## {title}\n\n")
            f.write(f"![{img}]({rel_path})\n\n")
    else:
        f.write("No images folder found.\n\n")

    # Añadir la tabla final
    if os.path.exists(all_catalogues_csv):
        df = pd.read_csv(all_catalogues_csv)
        f.write("## All Catalogues Table\n\n")
        f.write(df.to_markdown(index=False))
    else:
        f.write("No all_catalogues.csv found.\n")

print(f"Markdown file '{md_file}' generated successfully!")
