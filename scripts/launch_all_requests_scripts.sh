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

source ~/.bashrc || true

# Try to activate the env robustly across HPCs
if command -v mamba >/dev/null 2>&1; then
    mamba activate c3s-atlas || true
elif command -v conda >/dev/null 2>&1; then
    # Ensure conda is initialized in non-interactive shells
    eval "$(conda shell.bash hook)" || true
    conda activate c3s-atlas || true
elif command -v micromamba >/dev/null 2>&1; then
    eval "$(micromamba shell hook -s bash)" || true
    micromamba activate c3s-atlas || true
fi

# Verify activation worked
python -c "import sys; assert sys.prefix and 'c3s-atlas' in sys.prefix" 2>/dev/null || {
    echo "Failed to activate conda/mamba env 'c3s-atlas'. Ensure it exists or install with:"
    echo "  conda env create -f environment.yml"
    exit 1
}

for script in $SCRIPTS; do
    if [[ -f "$script" ]]; then
        echo "Launching SLURM job for $script on node wn54"
        sbatch --nodelist=wn054 --job-name=$(basename $script .py) --output=slurm-$(basename $script .py)-%j.out --wrap="python $script"
    else
        echo "Script not found: $script"
    fi
done
