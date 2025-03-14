#!/bin/sh

#SBATCH -J TCGA_DOWNLOAD
#SBATCH --mem=10G
#SBATCH --partition=all
#SBATCH --time=300:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4

# Clear environment
module purge > /dev/null 2>&1

# Load required modules
module load system/gcc/13.2.0
module load system/python/3.9.17
module load tools/miniconda/python3.9/4.12.0

# Activate virtual environment
source venv/bin/activate

echo "Running TCGA download..."
venv/bin/python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \
    --parent-dir "TCGA" \
    --manifest-dir "manifests" \
    --raw-annotations-dir "raw_annotations"
    --n-processes 4

echo "TCGA download complete."
deactivate