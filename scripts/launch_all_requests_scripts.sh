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

# Directory for per-run logs
LOG_DIR=logs
mkdir -p "$LOG_DIR"

source ~/.bashrc
mamba activate c3s-atlas

# Verify activation worked
python -c "import sys; assert sys.prefix and 'c3s-atlas' in sys.prefix" 2>/dev/null || {
    echo "Failed to activate conda/mamba env 'c3s-atlas'. Ensure it exists or install with:"
    echo "  conda env create -f environment.yml"
    exit 1
}

for script in $SCRIPTS; do
    if [[ -f "$script" ]]; then
        echo "Launching SLURM job for $script on node wn54"
        base=$(basename "$script" .py)
        script_dir=$(dirname "$script")
        script_file=$(basename "$script")
        out_file="$LOG_DIR/${base}-%j-%s.out"
        err_file="$LOG_DIR/${base}-%j-%s.err"
        sbatch --nodelist=wn054 -p meteo_long \
             --job-name="$base" \
             --output="$out_file" \
             --error="$err_file" \
             --chdir="$script_dir" \
             --wrap="python $script_file"
    else
        echo "Script not found: $script"
    fi
done
