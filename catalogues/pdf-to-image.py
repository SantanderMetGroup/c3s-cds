import glob
import fitz  # pip install PyMuPDF
import os

# Carpeta de trabajo
folder = "."

# Buscar todos los PDFs en el directorio
pdf_files = glob.glob(os.path.join(folder, "*.pdf"))

# Nombre del Markdown a generar
md_file = os.path.join(folder, "catalog_reumen.md")

with open(md_file, "w") as md:
    for pdf_path in pdf_files:
        pdf_name = os.path.basename(pdf_path)
        md.write(f"# {pdf_name}\n\n")  # título del PDF

        # Abrir PDF con PyMuPDF
        pdf_doc = fitz.open(pdf_path)
        for i, page in enumerate(pdf_doc, 1):
            pix = page.get_pixmap(dpi=150)
            img_name = f"{os.path.splitext(pdf_name)[0]}-page-{i}.png"
            pix.save(os.path.join(folder, img_name))
            md.write(f"![{pdf_name} - Página {i}]({img_name})\n\n")

print(f"Markdown generado en {md_file}")
