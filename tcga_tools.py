#!/usr/bin/env python3
"""
TCGA Data Downloader and Processor with GDC Data Transfer Tool

This script handles three main tasks:
1. Building/Installing the gdc-client:
   - The function `build_gdc_client()` checks if the gdc-client executable exists.
   - If not, it changes directory to the gdc-client submodule's bin directory and executes the package script (./package)
     which builds a zip file containing the executable.
   - It then extracts the zip file and copies the executable to a known location (by default, specified by --gdc-client-path).
2. Downloading TCGA datasets:
   - The function `download_with_manifest()` calls the gdc-client with the given manifest,
     download directory, and log file.
   - The function `download_dataset()` uses this to download all files for a given dataset.
3. Processing Annotation Files:
   - The script looks in a specified raw annotations directory (via --raw-annotations-dir) for tar.gz files with names:
         clinical.project-tcga_{normalized}.tar.gz
         biospecimen.project-tcga_{normalized}.tar.gz
     and processes these to generate survival prediction and classification annotation files.
     
Usage:
    python tcga_downloader.py --datasets TCGA-LUSC TCGA-BRCA \
        --parent-dir /path/to/data --manifest-dir /path/to/manifests \
        --raw-annotations-dir /path/to/raw_annotations [--build-gdc] \
        [--gdc-client-dir ./gdc-client] [--gdc-client-path ./gdc-client-bin]

Requirements:
    - Python 3.x
    - pandas, tqdm
    - A Unix-like shell environment (on Windows, use git-shell)
    - The gdc-client submodule cloned from https://github.com/NCI-GDC/gdc-client
"""

import os
import subprocess
import tarfile
import logging
import argparse
import zipfile
import shutil
import pandas as pd
from tqdm import tqdm

def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(level=logging.INFO, 
                        format="%(asctime)s [%(levelname)s] %(message)s", 
                        datefmt='%Y-%m-%d %H:%M:%S')

def normalize_dataset_name(dataset: str) -> str:
    """
    Normalize the dataset name to match file naming conventions.
    For example, "TCGA-BRCA" becomes "tcga_brca".
    """
    return dataset.lower().replace('-', '_')

def build_gdc_client(bin_dir: str, output_executable: str) -> str:
    """
    Build the gdc-client executable using the packaging script from the gdc-client submodule.
    
    Parameters:
        bin_dir (str): Path to the bin directory containing the package script (e.g. "./gdc-client/bin").
        output_executable (str): Desired path for the extracted gdc-client executable.
    
    Returns:
        str: The full path to the installed gdc-client executable.
    
    Raises:
        FileNotFoundError: If the packaging script or resulting zip file is not found.
    """
    package_script = os.path.join(bin_dir, "package")
    if not os.path.exists(package_script):
        raise FileNotFoundError(f"Packaging script not found at {package_script}")
    
    logging.info("Building gdc-client executable...")
    # Run the package script from within the bin directory.
    result = subprocess.run(["bash", package_script], cwd=bin_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logging.error(f"Error building gdc-client: {result.stderr}")
        raise Exception("gdc-client build failed")
    else:
        logging.info("gdc-client build completed successfully.")
    
    # Look for a zip file with a name starting with 'gdc-client' in the bin directory.
    zip_files = [f for f in os.listdir(bin_dir) if f.startswith("gdc-client") and f.endswith(".zip")]
    if not zip_files:
        raise FileNotFoundError("No gdc-client zip file found after build.")
    
    zip_path = os.path.join(bin_dir, zip_files[0])
    logging.info(f"Found zip file: {zip_path}. Extracting...")
    
    # Create a temporary directory for extraction.
    temp_extract_dir = os.path.join(bin_dir, "gdc_extract_temp")
    os.makedirs(temp_extract_dir, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(path=temp_extract_dir)
    
    # Assume the extracted executable is named "gdc-client" (or "gdc-client.exe" on Windows)
    executable_name = "gdc-client.exe" if os.name == "nt" else "gdc-client"
    extracted_executable = None
    for root, dirs, files in os.walk(temp_extract_dir):
        if executable_name in files:
            extracted_executable = os.path.join(root, executable_name)
            break
    
    if not extracted_executable or not os.path.exists(extracted_executable):
        raise FileNotFoundError("gdc-client executable not found in the extracted zip file.")
    
    # Move the executable to the desired output location.
    shutil.move(extracted_executable, output_executable)
    if os.name != "nt":
        os.chmod(output_executable, 0o755)
    
    shutil.rmtree(temp_extract_dir)
    logging.info(f"gdc-client installed at: {output_executable}")
    return os.path.abspath(output_executable)

def download_with_manifest(manifest_file: str, download_dir: str, log_file: str, gdc_client_executable: str):
    """
    Download all files listed in the manifest using the gdc-client download command.
    
    Parameters:
        manifest_file (str): Path to the GDC manifest file.
        download_dir (str): Directory where files will be downloaded.
        log_file (str): Path to the log file for the gdc-client download process.
        gdc_client_executable (str): Path to the gdc-client executable.
    
    Raises:
        Exception: If the download process fails.
    """
    logging.info(f"Starting download using manifest: {manifest_file}")
    
    cmd = [
        gdc_client_executable, "download",
        "--dir", download_dir,
        "--manifest", manifest_file,
        "--log-file", log_file
    ]
    logging.debug("Executing command: " + " ".join(cmd))
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if result.returncode != 0:
        logging.error(f"Error during download: {result.stderr}")
        raise Exception("gdc-client download failed.")
    else:
        logging.info(f"Download completed successfully. See log file: {log_file}")

def download_dataset(dataset_name: str, parent_dir: str, manifest_dir: str, raw_annotations_dir: str, gdc_client_executable: str):
    """
    Download a TCGA dataset using its manifest file and process annotation data.
    
    Parameters:
        dataset_name (str): The TCGA dataset name (e.g., "TCGA-BRCA").
        parent_dir (str): The parent directory for storing datasets.
        manifest_dir (str): Directory containing the manifest files.
        raw_annotations_dir (str): Directory containing raw annotation tar.gz files.
        gdc_client_executable (str): Path to the gdc-client executable.
    """
    logging.info(f"--- Processing dataset: {dataset_name} ---")
    
    dataset_dir = os.path.join(parent_dir, dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)
    
    norm_name = normalize_dataset_name(dataset_name)  # e.g. "tcga_brca"
    
    # Find the manifest file: expected to be "gdc_manifest.{norm_name}.txt"
    expected_manifest = f"gdc_manifest.{norm_name}.txt"
    manifest_file = os.path.join(manifest_dir, expected_manifest)
    if not os.path.exists(manifest_file):
        logging.error(f"Manifest file {manifest_file} not found for dataset {dataset_name}")
        return
    logging.info(f"Using manifest file: {manifest_file}")
    
    # Set log file path.
    log_file = os.path.join(dataset_dir, f"tcga-{dataset_name}-download.log")
    
    # Download files using gdc-client and the manifest.
    download_with_manifest(manifest_file, dataset_dir, log_file, gdc_client_executable)
    
    # Process annotation files from the raw annotations directory.
    # Expected file names:
    #   clinical: "clinical.project-tcga_{norm_name}.tar.gz"
    #   biospecimen: "biospecimen.project-tcga_{norm_name}.tar.gz"
    clinical_annotation_src = os.path.join(raw_annotations_dir, f"clinical.project-tcga_{norm_name}.tar.gz")
    biospecimen_annotation_src = os.path.join(raw_annotations_dir, f"biospecimen.project-tcga_{norm_name}.tar.gz")
    
    # Copy and process clinical annotation if available.
    if os.path.exists(clinical_annotation_src):
        clinical_annotation_dest = os.path.join(dataset_dir, os.path.basename(clinical_annotation_src))
        shutil.copy(clinical_annotation_src, clinical_annotation_dest)
        process_clinical_data(clinical_annotation_dest, dataset_dir)
    else:
        logging.warning(f"Clinical annotation file not found: {clinical_annotation_src}")
    
    # Copy and process biospecimen annotation if available.
    if os.path.exists(biospecimen_annotation_src):
        biospecimen_annotation_dest = os.path.join(dataset_dir, os.path.basename(biospecimen_annotation_src))
        shutil.copy(biospecimen_annotation_src, biospecimen_annotation_dest)
        process_biospecimen_data(biospecimen_annotation_dest, dataset_dir)
    else:
        logging.warning(f"Biospecimen annotation file not found: {biospecimen_annotation_src}")

def process_clinical_data(clinical_tar_path: str, output_dir: str):
    """
    Process the clinical data tarball to generate annotation files for survival prediction and classification.
    
    Extracts clinical.tsv from the tarball, then:
      - Computes a survival annotation file using 'days_to_death' and 'days_to_last_follow_up'.
      - Extracts columns containing 'Stage' to create a classification annotation file.
      - Creates an additional annotation file if 'primary_diagnosis' exists.
      
    Parameters:
        clinical_tar_path (str): Path to the clinical tar.gz file.
        output_dir (str): Directory where annotation files will be saved.
    """
    logging.info(f"Processing clinical data from {clinical_tar_path}")
    temp_extract_dir = os.path.join(output_dir, "extracted_clinical")
    os.makedirs(temp_extract_dir, exist_ok=True)
    
    with tarfile.open(clinical_tar_path, "r:gz") as tar:
        tar.extractall(path=temp_extract_dir)
    
    clinical_tsv_path = os.path.join(temp_extract_dir, "clinical.tsv")
    if not os.path.exists(clinical_tsv_path):
        logging.error("clinical.tsv not found after extraction.")
        return
    
    df = pd.read_csv(clinical_tsv_path, sep="\t", low_memory=False)
    logging.info(f"Loaded clinical data with {df.shape[0]} rows and {df.shape[1]} columns.")
    
    # Create Survival Prediction Annotation File
    def parse_days(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return None

    df["days_to_death"] = df["days_to_death"].apply(parse_days)
    df["days_to_last_follow_up"] = df["days_to_last_follow_up"].apply(parse_days)
    
    def compute_time(row):
        return row["days_to_death"] if pd.notnull(row["days_to_death"]) else row["days_to_last_follow_up"]
    
    def compute_event(row):
        return 1 if pd.notnull(row["days_to_death"]) else 0

    survival_df = pd.DataFrame({
        "case_id": df["case_id"],
        "time": df.apply(compute_time, axis=1),
        "event": df.apply(compute_event, axis=1)
    })
    survival_annotation_path = os.path.join(output_dir, "survival_annotation.tsv")
    survival_df.to_csv(survival_annotation_path, sep="\t", index=False)
    logging.info(f"Survival annotation file created at {survival_annotation_path}")
    
    # Create Classification Annotation File
    stage_columns = [col for col in df.columns if "Stage" in col]
    if stage_columns:
        classification_df = df[["case_id"] + stage_columns].copy()
        classification_annotation_path = os.path.join(output_dir, "classification_annotation.tsv")
        classification_df.to_csv(classification_annotation_path, sep="\t", index=False)
        logging.info(f"Classification annotation file created at {classification_annotation_path}")
    else:
        logging.info("No stage-related columns found for classification annotation.")
    
    # Additional Classification File from primary_diagnosis
    if "primary_diagnosis" in df.columns:
        primary_diag_df = df[["case_id", "primary_diagnosis"]].copy()
        primary_diag_annotation_path = os.path.join(output_dir, "primary_diagnosis_annotation.tsv")
        primary_diag_df.to_csv(primary_diag_annotation_path, sep="\t", index=False)
        logging.info(f"Primary diagnosis annotation file created at {primary_diag_annotation_path}")
    
    # Optionally, clean up the temporary extraction directory
    # shutil.rmtree(temp_extract_dir)

def process_biospecimen_data(biospecimen_tar_path: str, output_dir: str):
    """
    Process the biospecimen data tarball by extracting its contents.
    
    Parameters:
        biospecimen_tar_path (str): Path to the biospecimen tar.gz file.
        output_dir (str): Directory where the biospecimen data will be extracted.
    """
    logging.info(f"Processing biospecimen data from {biospecimen_tar_path}")
    temp_extract_dir = os.path.join(output_dir, "extracted_biospecimen")
    os.makedirs(temp_extract_dir, exist_ok=True)
    
    with tarfile.open(biospecimen_tar_path, "r:gz") as tar:
        tar.extractall(path=temp_extract_dir)
    logging.info(f"Biospecimen data extracted to {temp_extract_dir}")

def main():
    """
    Parse command-line arguments and process each specified TCGA dataset:
      - Optionally build the gdc-client executable.
      - Download dataset files using the manifest.
      - Process clinical and biospecimen annotation data from the raw annotations directory.
    """
    setup_logging()
    
    parser = argparse.ArgumentParser(description="TCGA Data Downloader and Processor with gdc-client")
    parser.add_argument("--datasets", nargs="+", required=True,
                        help="List of TCGA dataset names to download (e.g. TCGA-LUSC TCGA-BRCA)")
    parser.add_argument("--parent-dir", required=True,
                        help="Parent directory to store downloaded datasets")
    parser.add_argument("--manifest-dir", required=True,
                        help="Directory containing the manifest files")
    parser.add_argument("--raw-annotations-dir", required=True,
                        help="Directory containing the raw annotation tar.gz files")
    parser.add_argument("--gdc-client-dir", default="./gdc-client",
                        help="Path to the gdc-client submodule directory (default: ./gdc-client)")
    parser.add_argument("--gdc-client-path", default="./gdc-client-bin",
                        help="Path to the gdc-client executable (default: ./gdc-client-bin)")
    parser.add_argument("--build-gdc", action="store_true",
                        help="If set, build the gdc-client from source using the package script")
    
    args = parser.parse_args()
    
    # Build the gdc-client if requested or if not already present.
    gdc_client_executable = args.gdc_client_path
    if args.build_gdc or not os.path.exists(gdc_client_executable):
        bin_dir = os.path.join(args.gdc_client_dir, "bin")
        try:
            gdc_client_executable = build_gdc_client(bin_dir, args.gdc_client_path)
        except Exception as e:
            logging.error(f"Failed to build/install gdc-client: {e}")
            return
    
    # Process each dataset.
    for dataset in args.datasets:
        try:
            download_dataset(dataset, args.parent_dir, args.manifest_dir, args.raw_annotations_dir, gdc_client_executable)
        except Exception as e:
            logging.error(f"Error processing dataset {dataset}: {e}")
    
    logging.info("All datasets have been processed.")

if __name__ == "__main__":
    main()
