# TCGA-Tools

**TCGA-Tools** is a framework to facilitate the easy download of TCGA public Whole Slide Image (WSI) datasets for computational pathology projects. It leverages the GDC Data Transfer Tool (gdc-client) to efficiently download large datasets and processes clinical and biospecimen annotations for downstream analyses like survival prediction and classification.

## Features

- **GDC-Client Build & Installation:**  
  Automatically builds the [gdc-client](https://github.com/NCI-GDC/gdc-client) executable from source using a Git submodule.

- **Clean data storage**
  Ensures the datasets are stored in a clean and manageable way (e.g. no subdirectories) alogn with their annotations

- **Annotation Processing:**  
  Processes clinical and biospecimen annotation files (tar.gz) from raw TCGA data to generate survival prediction and classification annotation files, which can be used for downstream tasks like [PathBench-MIL](https://github.com/Sbrussee/PathBench-MIL)

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
venv/bin/python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \ # Can be any of the datasets present in ./manifests
    --parent-dir /path/to/data \
    --manifest-dir /path/to/manifests \ # Defaults to./manifests
    --raw-annotations-dir /path/to/raw_annotations # Defaults to ./raw_annotations
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
Below is an example SLURM job script (shark_run.sbatch) for running the download with 4 processes:

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

## Output
TCGA-Tools provides a clean directory output structure where slides and annotations are stored. For example, let us assume we have set 'parent_dir' to 'TCGA', and we downloaded the TCGA-LUSC and TCGA-BRCA datasets. Then our directory should look like this:
```graphql
📂 TCGA/
 ├── 📂 TCGA-LUSC/                      # Dataset directory (one per dataset)
 │   ├── 📝 sample_to_case_map_tcga_lusc.tsv # Sample ID → Case ID mapping
 │   ├── 📝 tcga-TCGA-LUSC-download.log  # Download log file
 │   ├── 📜 TCGA-60-2712-01Z.svs         # Slide file (renamed based on Sample ID)
 │   ├── 📜 TCGA-56-7221-01Z.svs
 │   ├── 📜 TCGA-21-A5DI-01Z.svs
 │   ├── 📜 TCGA-60-2695-01Z.svs
 │   ├── 📜 TCGA-60-2710-01Z.svs
 │   ├── 📜 TCGA-22-5472-01Z.svs
 │   ├── 📜 ... (more .svs slides)
 │   ├── 📝 clinical_tcga_lusc.tsv       # Extracted clinical data (dataset-specific)
 │   ├── 📝 survival_annotation_tcga_lusc.tsv   # Processed survival data
 │   ├── 📝 classification_annotation_tcga_lusc.tsv # Processed classification data
 │   ├── 📝 primary_diagnosis_annotation_tcga_lusc.tsv # Diagnosis-based classification
 │
 ├── 📂 TCGA-BRCA/                      # Another dataset, same structure
 │   ├── 📝 sample_to_case_map_tcga_brca.tsv
 │   ├── 📝 tcga-TCGA-BRCA-download.log
 │   ├── 📜 TCGA-XX-XXXX-01Z.svs
 │   ├── 📝 clinical_tcga_brca.tsv
 │   ├── 📝 survival_annotation_tcga_brca.tsv
 │   ├── 📝 classification_annotation_tcga_brca.tsv
 │   ├── 📝 primary_diagnosis_annotation_tcga_brca.tsv
 │
📂 manifests/                       # Directory for manifest files
 ├── 📝 gdc_manifest.tcga_lusc.txt
 ├── 📝 gdc_manifest.tcga_brca.txt
 │
📂 raw_annotations/                 # Directory for raw annotation files
 ├── 📜 clinical.project-tcga_lusc.tar.gz
 ├── 📜 biospecimen.project-tcga_lusc.tar.gz
 ├── 📜 clinical.project-tcga_brca.tar.gz
 ├── 📜 biospecimen.project-tcga_brca.tar.gz
 ├── 📝 gdc_sample_sheet_tcga_lusc.txt
 ├── 📝 gdc_sample_sheet_tcga_brca.txt
 |
 ├── 📝 README.md
 ```

### Contributing
Contributions, bug reports, and feature requests are welcome. Please open issues or submit pull requests on GitHub.

### License
This project is licensed under the terms specified in the LICENSE file.
