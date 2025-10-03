#!/bin/bash
# Auto-generated SLURM batch launcher for all scripts in requests CSVs
#SBATCH -p meteo_long
#SBATCH --job-name=run_requests_scripts
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=2:00:00

# Get all unique script paths from all requests/*.csv files
SCRIPTS=$(awk -F',' 'NR>1 && $0!~/^$/ {for(i=1;i<=NF;i++) if($i ~ /scripts\// || $i ~ /derived\// || $i ~ /interpolation\//) print $i}' requests/*.csv | sort | uniq)

source ~/.bashrc
mamba activate c3s-atlas

for script in $SCRIPTS; do
    if [[ -f "$script" ]]; then
        echo "Launching SLURM job for $script on node wn54"
        sbatch --nodelist=wn054 --job-name=$(basename $script .py) --output=slurm-$(basename $script .py)-%j.out --wrap="python $script"
    else
        echo "Script not found: $script"
    fi
done
