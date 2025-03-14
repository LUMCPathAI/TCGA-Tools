#!/usr/bin/env python3
"""
TCGA Data Downloader and Processor with GDC Data Transfer Tool

This script handles:
1. Building/Installing the gdc-client if needed.
2. Downloading TCGA datasets via a manifest file.
3. Processing clinical annotation files.
4. Using a GDC sample sheet (from raw_annotations) to rename .svs slides and flatten directories.

Usage:
    python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \
        --parent-dir /path/to/TCGA --manifest-dir /path/to/manifests \
        --raw-annotations-dir /path/to/raw_annotations [--build-gdc] \
        [--gdc-client-dir ./gdc-client] [--gdc-client-path ./gdc-client_exec] \
        [--n-processes 4] [--verbose]

A sample sheet named `gdc_sample_sheet_tcga_{norm_dataset_name}.txt`
(e.g. `gdc_sample_sheet_tcga_lusc.txt`) is expected in `raw_annotations`.
It should map each File ID (UUID) to File Name, Case ID, and Sample ID.
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
    """
    package_script = os.path.join(bin_dir, "package")
    if not os.path.exists(package_script):
        raise FileNotFoundError(f"Packaging script not found at {package_script}")
    
    logging.info("Building gdc-client executable...")
    result = subprocess.run(["bash", package_script], cwd=bin_dir,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logging.error(f"Error building gdc-client: {result.stderr}")
        raise Exception("gdc-client build failed")
    else:
        logging.info("gdc-client build completed successfully.")
    
    zip_files = [f for f in os.listdir(bin_dir) if f.startswith("gdc-client") and f.endswith(".zip")]
    if not zip_files:
        raise FileNotFoundError("No gdc-client zip file found after build.")
    
    zip_path = os.path.join(bin_dir, zip_files[0])
    logging.info(f"Found zip file: {zip_path}. Extracting...")
    
    temp_extract_dir = os.path.join(bin_dir, "gdc_extract_temp")
    os.makedirs(temp_extract_dir, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(path=temp_extract_dir)
    
    executable_name = "gdc-client.exe" if os.name == "nt" else "gdc-client"
    extracted_executable = None
    for root, dirs, files in os.walk(temp_extract_dir):
        if executable_name in files:
            extracted_executable = os.path.join(root, executable_name)
            break
    
    if not extracted_executable or not os.path.exists(extracted_executable):
        raise FileNotFoundError("gdc-client executable not found in the extracted zip file.")
    
    shutil.move(extracted_executable, output_executable)
    if os.name != "nt":
        os.chmod(output_executable, 0o755)
    
    shutil.rmtree(temp_extract_dir)
    logging.info(f"gdc-client installed at: {output_executable}")
    return os.path.abspath(output_executable)

def download_with_manifest(manifest_file: str, download_dir: str, log_file: str,
                           gdc_client_executable: str, n_processes: int = None, verbose: bool = False):
    """
    Download all files listed in the manifest using the gdc-client download command.
    """
    logging.info(f"Starting download using manifest: {manifest_file}")
    
    cmd = [
        gdc_client_executable, "download",
        "--dir", download_dir,
        "--manifest", manifest_file,
        "--log-file", log_file
    ]
    
    if n_processes is not None:
        cmd.extend(["-n", str(n_processes)])
    
    if verbose:
        cmd.append("--debug")
    
    logging.debug("Executing command: " + " ".join(cmd))
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if result.returncode != 0:
        logging.error(f"Error during download: {result.stderr}")
        raise Exception("gdc-client download failed.")
    else:
        logging.info(f"Download completed successfully. See log file: {log_file}")

def postprocess_slides(dataset_dir: str, sample_sheet_path: str, norm_name: str):
    """
    Process the slides by reading the sample sheet and:
      - Moving the .svs file from dataset_dir/<File ID>/<File Name> to dataset_dir/<Sample ID>.svs
      - Removing the now-empty directory
      - Creating a sample-to-case map saved as sample_to_case_map_tcga_{norm_name}.tsv
    """
    if not os.path.exists(sample_sheet_path):
        logging.warning(f"No sample sheet found at {sample_sheet_path}, skipping slide postprocessing.")
        return
    
    logging.info(f"Postprocessing slides using sample sheet: {sample_sheet_path}")
    
    df = pd.read_csv(sample_sheet_path, sep="\t", dtype=str)
    required_cols = ["File ID", "File Name", "Case ID", "Sample ID"]
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"Missing column '{col}' in sample sheet. Postprocessing aborted.")
            return
    
    mapping_rows = []
    for idx, row in df.iterrows():
        file_id = row["File ID"]
        file_name = row["File Name"]
        case_id = row["Case ID"]
        sample_id = row["Sample ID"]
        
        old_dir = os.path.join(dataset_dir, file_id)
        old_path = os.path.join(old_dir, file_name)
        _, ext = os.path.splitext(file_name)
        new_file_name = f"{sample_id}{ext}"
        new_path = os.path.join(dataset_dir, new_file_name)
        
        if not os.path.exists(old_path):
            logging.warning(f"Expected slide file not found: {old_path}")
            continue
        
        try:
            shutil.move(old_path, new_path)
            logging.info(f"Moved {old_path} -> {new_path}")
        except Exception as e:
            logging.error(f"Failed to move {old_path} to {new_path}: {e}")
            continue
        
        try:
            if os.path.isdir(old_dir) and not os.listdir(old_dir):
                os.rmdir(old_dir)
        except Exception as e:
            logging.warning(f"Could not remove directory {old_dir}: {e}")
        
        mapping_rows.append((sample_id, case_id))
    
    map_df = pd.DataFrame(mapping_rows, columns=["Sample ID", "Case ID"]).drop_duplicates()
    map_df_path = os.path.join(dataset_dir, f"sample_to_case_map_tcga_{norm_name}.tsv")
    map_df.to_csv(map_df_path, sep="\t", index=False)
    logging.info(f"Sample-to-case map saved to {map_df_path}")

def process_clinical_data(clinical_tar_path: str, output_dir: str, norm_name: str):
    """
    Process the clinical data tarball to:
      1. Extract and save the raw clinical data as clinical_tcga_{norm_name}.tsv.
      2. Create processed annotation files for survival, classification, and primary diagnosis.
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
    
    # Copy the extracted clinical.tsv to output_dir as clinical_tcga_{norm_name}.tsv
    clinical_output = os.path.join(output_dir, f"clinical_tcga_{norm_name}.tsv")
    shutil.copy(clinical_tsv_path, clinical_output)
    logging.info(f"Extracted clinical data saved to {clinical_output}")
    
    df = pd.read_csv(clinical_tsv_path, sep="\t", low_memory=False)
    logging.info(f"Loaded clinical data with {df.shape[0]} rows and {df.shape[1]} columns.")
    
    # Process survival annotation.
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
    survival_annotation_path = os.path.join(output_dir, f"survival_annotation_tcga_{norm_name}.tsv")
    survival_df.to_csv(survival_annotation_path, sep="\t", index=False)
    logging.info(f"Survival annotation file created at {survival_annotation_path}")
    
    # Process classification annotation.
    stage_columns = [col for col in df.columns if "Stage" in col]
    if stage_columns:
        classification_df = df[["case_id"] + stage_columns].copy()
        classification_annotation_path = os.path.join(output_dir, f"classification_annotation_tcga_{norm_name}.tsv")
        classification_df.to_csv(classification_annotation_path, sep="\t", index=False)
        logging.info(f"Classification annotation file created at {classification_annotation_path}")
    else:
        logging.info("No stage-related columns found for classification annotation.")
    
    # Process primary diagnosis annotation.
    if "primary_diagnosis" in df.columns:
        primary_diag_df = df[["case_id", "primary_diagnosis"]].copy()
        primary_diag_annotation_path = os.path.join(output_dir, f"primary_diagnosis_annotation_tcga_{norm_name}.tsv")
        primary_diag_df.to_csv(primary_diag_annotation_path, sep="\t", index=False)
        logging.info(f"Primary diagnosis annotation file created at {primary_diag_annotation_path}")
    
    # Clean up temporary extraction directory.
    shutil.rmtree(temp_extract_dir)

def download_dataset(dataset_name: str, parent_dir: str, manifest_dir: str,
                     raw_annotations_dir: str, gdc_client_executable: str,
                     n_processes: int = None, verbose: bool = False):
    """
    Download a TCGA dataset using its manifest file and process clinical data.
    Also flatten the downloaded .svs slide directories using the sample sheet
    (which is read from raw_annotations).
    """
    logging.info(f"--- Processing dataset: {dataset_name} ---")
    dataset_dir = os.path.join(parent_dir, dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)
    
    norm_name = normalize_dataset_name(dataset_name)  # e.g., "tcga_lusc"
    
    # Use the manifest file from the pre-existing manifests directory.
    expected_manifest = f"gdc_manifest.{norm_name}.txt"
    manifest_file = os.path.join(manifest_dir, expected_manifest)
    if not os.path.exists(manifest_file):
        logging.error(f"Manifest file {manifest_file} not found for dataset {dataset_name}")
        return
    logging.info(f"Using manifest file: {manifest_file}")
    
    # Download log file in the dataset directory.
    log_file = os.path.join(dataset_dir, f"tcga-{dataset_name}-download.log")
    
    download_with_manifest(
        manifest_file=manifest_file,
        download_dir=dataset_dir,
        log_file=log_file,
        gdc_client_executable=gdc_client_executable,
        n_processes=n_processes,
        verbose=verbose
    )
    
    # Process clinical annotation (read from raw_annotations; do not copy the tarball).
    clinical_annotation_src = os.path.join(raw_annotations_dir, f"clinical.project-tcga_{norm_name}.tar.gz")
    if os.path.exists(clinical_annotation_src):
        process_clinical_data(clinical_annotation_src, dataset_dir, norm_name)
    else:
        logging.warning(f"Clinical annotation file not found: {clinical_annotation_src}")
    
    # (Biospecimen processing is skipped, per the desired output.)
    
    # Process slides using the sample sheet from raw_annotations.
    sample_sheet_path = os.path.join(raw_annotations_dir, f"gdc_sample_sheet_tcga_{norm_name}.txt")
    postprocess_slides(dataset_dir, sample_sheet_path, norm_name)

def main():
    """
    Parse command-line arguments and process each specified TCGA dataset:
      - Optionally build the gdc-client executable.
      - Download dataset files using the manifest.
      - Process clinical annotation data.
      - Flatten .svs slide directories using the GDC sample sheet.
    """
    setup_logging()
    
    parser = argparse.ArgumentParser(description="TCGA Data Downloader and Processor with gdc-client")
    parser.add_argument("--datasets", nargs="+", required=True,
                        help="List of TCGA dataset names to download (e.g. TCGA-LUSC TCGA-BRCA)")
    parser.add_argument("--parent-dir", required=True,
                        help="Parent directory to store dataset folders (e.g. TCGA)")
    parser.add_argument("--manifest-dir", required=True,
                        help="Directory containing the manifest files (pre-existing)")
    parser.add_argument("--raw-annotations-dir", required=True,
                        help="Directory containing the raw annotation tar.gz files and sample sheets (pre-existing)")
    parser.add_argument("--gdc-client-dir", default="./gdc-client",
                        help="Path to the gdc-client submodule directory (default: ./gdc-client)")
    parser.add_argument("--gdc-client-path", default="./gdc-client_exec",
                        help="Path to the gdc-client executable (default: ./gdc-client_exec)")
    parser.add_argument("--build-gdc", action="store_true",
                        help="If set, build the gdc-client from source using the package script")
    parser.add_argument("--n-processes", type=int,
                        help="Number of processes to use for downloading (passed to gdc-client with -n)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose output from gdc-client")
    
    args = parser.parse_args()
    os.makedirs(args.parent_dir, exist_ok=True)
    
    # Build the gdc-client if needed.
    gdc_client_executable = args.gdc_client_path
    if args.build_gdc or not os.path.exists(gdc_client_executable):
        bin_dir = os.path.join(args.gdc_client_dir, "bin")
        try:
            gdc_client_executable = build_gdc_client(bin_dir, args.gdc_client_path)
        except Exception as e:
            logging.error(f"Failed to build/install gdc-client: {e}")
            return
    
    # Process each specified dataset.
    for dataset in args.datasets:
        try:
            download_dataset(dataset_name=dataset,
                             parent_dir=args.parent_dir,
                             manifest_dir=args.manifest_dir,
                             raw_annotations_dir=args.raw_annotations_dir,
                             gdc_client_executable=gdc_client_executable,
                             n_processes=args.n_processes,
                             verbose=args.verbose)
        except Exception as e:
            logging.error(f"Error processing dataset {dataset}: {e}")
    
    logging.info("All datasets have been processed.")

if __name__ == "__main__":
    main()
