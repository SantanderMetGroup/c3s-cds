import os
import pandas as pd

# Carpeta donde están las imágenes
images_folder = "images"
# CSV con la información completa
all_catalogues_csv = "catalogues/all_catalogues.csv"
# Nombre del fichero Markdown
md_file = "README.md"

with open(md_file, "w", encoding="utf-8") as f:
    f.write("# Catalogue Overview\n\n")

    # Añadir imágenes
    image_files = sorted([img for img in os.listdir(images_folder) if img.endswith(".png")])
    for img in image_files:
        f.write(f"## {img.replace('_', ' ').replace('.png','')}\n\n")
        f.write(f"![{img}]({images_folder}/{img})\n\n")

    # Añadir la tabla final
    if os.path.exists(all_catalogues_csv):
        df = pd.read_csv(all_catalogues_csv)
        f.write("## All Catalogues Table\n\n")
        f.write(df.to_markdown(index=False))
    else:
        f.write("No all_catalogues.csv found.\n")

print(f"Markdown file '{md_file}' generated successfully!")
