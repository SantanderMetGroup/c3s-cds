#!/bin/bash
#SBATCH -p meteo_long
#SBATCH --job-name=catalog_c3s-cds
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=72:00:00
 
source ~/.bashrc
mamba activate c3s-atlas
# Navigate to the catalogue scripts directory
cd scripts/catalogue/

python produce_catalog.py
python generate_resumen.py
