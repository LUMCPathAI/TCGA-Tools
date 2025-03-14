# TCGA-Tools

**TCGA-Tools** is a framework to facilitate the easy download of TCGA public Whole Slide Image (WSI) datasets for computational pathology projects. It leverages the GDC Data Transfer Tool (gdc-client) to efficiently download large datasets and processes clinical and biospecimen annotations for downstream analyses like survival prediction and classification.

## Features

- **GDC-Client Build & Installation:**  
  Automatically builds the [gdc-client](https://github.com/NCI-GDC/gdc-client) executable from source using a Git submodule.

- **Parallel Downloading:**  
  Supports parallel downloads by specifying the number of processes.

- **Annotation Processing:**  
  Processes clinical and biospecimen annotation files (tar.gz) from raw TCGA data to generate survival prediction and classification annotation files.

- **Configurable Workflow:**  
  Provides a command-line interface with multiple parameters for customization.

## Requirements

- **Operating System:**  
  Unix-like (Linux/macOS) or Windows (with a Unix-like shell, e.g., Git Bash).

- **Python:**  
  Python 3.x

- **Python Packages:**  
  - pandas  
  - tqdm  

- **Optional:**  
  SLURM for HPC job submission

## Repository Structure

After installation, your repository root should resemble:
```graphql
TCGA-Tools/
├── gdc-client/               # gdc-client Git submodule (cloned from https://github.com/NCI-GDC/gdc-client)
├── gdc-client_exec           # Built gdc-client executable
├── install_gdc.sh            # Script to build and install gdc-client and set up the venv
├── LICENSE
├── manifest_mapping.csv
├── manifests/                # Contains TCGA manifest files (e.g., gdc_manifest.tcga_brca.txt)
├── raw_annotations/          # Contains raw annotation tar.gz files (clinical and biospecimen)
├── README.md
├── tcga_tools.py             # Main download and processing script
└── venv/                     # Local virtual environment
```


## Installation

### 1. Clone the Repository with Submodules

1. Clone the repository (including the gdc-client submodule) with:

```bash
git clone --recurse-submodules https://github.com/your_username/TCGA-Tools.git
cd TCGA-Tools
```
2. Build and Install gdc-client
Run the provided installation script to build the gdc-client executable from the submodule. The script will install gdc-client, its virtual environment and the virtual environment for TCGA-tools.

```bash
./install_gdc.sh
```
After the script completes, verify that gdc-client_exec exists in your repository root.

# Usage
The main script, tcga_tools.py, downloads TCGA datasets using the gdc-client executable and processes annotation files. Its command-line interface supports multiple parameters.

## Basic Usage
```bash
python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \
    --parent-dir /path/to/data \
    --manifest-dir /path/to/manifests \
    --raw-annotations-dir /path/to/raw_annotations
```
### Input Parameters and Defaults
--datasets:
Required. List of TCGA dataset names to download (e.g., TCGA-LUSC TCGA-BRCA).

--parent-dir:
Required. Parent directory where the downloaded datasets will be stored.

--manifest-dir:
Directory containing the manifest files.
Default: ./manifests

--raw-annotations-dir:
Directory containing the raw annotation tar.gz files.
Default: ./raw_annotations

--gdc-client-dir:
Path to the gdc-client submodule directory.
Default: ./gdc-client

--gdc-client-path:
Path to the gdc-client executable.
Default: ./gdc-client_exec

--build-gdc:
If specified, forces building the gdc-client executable from source (even if one already exists).

--n-processes:
(Optional) Number of parallel download processes. Passed to gdc-client with -n. For example, --n-processes 4.

--verbose:
(Optional) Enable verbose output from gdc-client (passed as --debug).

Example: Using 4 Processes with Verbose Output
```bash
python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \
    --parent-dir "TCGA" \
    --manifest-dir "manifests" \
    --raw-annotations-dir "raw_annotations" \
    --n-processes 4 \
    --verbose
```
### Running on HPC (SLURM)
Below is an example SLURM job script (tcga_download.sbatch) for running the download with 4 processes:

```sh
#!/bin/sh
#SBATCH -J TCGA_DOWNLOAD
#SBATCH --mem=10G
#SBATCH --partition=all
#SBATCH --time=300:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4

module purge > /dev/null 2>&1
module load system/gcc/13.2.0
module load system/python/3.9.17
module load tools/miniconda/python3.9/4.12.0

source venv/bin/activate

echo "Running TCGA download..."
venv/bin/python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \
    --parent-dir "TCGA" \
    --manifest-dir "manifests" \
    --raw-annotations-dir "raw_annotations" \
    --n-processes 4 \
    --verbose

echo "TCGA download complete."
deactivate
```
Submit the job with:

```bash
sbatch shark_run.sbatch
```

### Contributing
Contributions, bug reports, and feature requests are welcome. Please open issues or submit pull requests on GitHub.

### License
This project is licensed under the terms specified in the LICENSE file.
