#!/bin/bash
#SBATCH -p meteo_long
#SBATCH --job-name=catalog_c3s-cds
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=72:00:00

source ~/.bashrc
mamba activate c3s-atlas

python /lustre/gmeteo/WORK/garciar/c3s-cds/catalogues/produce_catalog.py
