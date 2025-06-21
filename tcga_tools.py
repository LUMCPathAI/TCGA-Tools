#!/usr/bin/env python3
"""
TCGA Data Downloader and Processor with GDC Data Transfer Tool

This script handles:
1. Building/Installing the gdc-client if needed.
2. Downloading TCGA datasets via a manifest file.
3. Processing clinical annotation files from JSON.
4. Locating slide files in the dataset directory and generating a slide-to-case mapping.
   (Slide filenames are assumed to be in the format {case_id_part}.{slide_id_part}.svs,
    where the patient (case) ID is defined as the first three tokens joined by "-" 
    and the remainder is the slide ID.)
5. Creating final annotation files by joining clinical annotations with the slide-to-case mapping.
   - Final survival annotation tables are generated separately for days, months, and quantiles.
   - Final classification annotation tables are generated separately for each classification label.
6. Optionally, flagging & removing rows with missing values and splitting the final annotations
   into train/test sets by adding a new column "dataset" with value "train" or "test".

Usage:
    python tcga_tools.py --datasets TCGA-LUSC TCGA-BRCA \
        --parent-dir /path/to/TCGA --manifest-dir /path/to/manifests \
        --raw-annotations-dir /path/to/raw_annotations [--build-gdc] \
        [--gdc-client-dir ./gdc-client] [--gdc-client-path ./gdc-client_exec] \
        [--n-processes 4] [--verbose] [--split] [--split-ratio 0.9] [--seed 42]

A sample sheet is no longer required for slide mapping.
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
import json  # For JSON processing
from sklearn.model_selection import train_test_split
import glob

def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt='%Y-%m-%d %H:%M:%S')

def normalize_dataset_name(dataset: str) -> str:
    """Normalize the dataset name (e.g. "TCGA-BRCA" becomes "tcga-brca")."""
    return dataset.lower()

def build_gdc_client(bin_dir: str, output_executable: str) -> str:
    """Build the gdc-client executable using the packaging script from the gdc-client submodule."""
    package_script = os.path.join(bin_dir, "package")
    if not os.path.exists(package_script):
        raise FileNotFoundError(f"Packaging script not found at {package_script}")
    
    logging.info("Building gdc-client executable...")
    result = subprocess.run(["bash", package_script], cwd=bin_dir,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logging.error(f"Error building gdc-client: {result.stderr}")
        raise Exception("gdc-client build failed")
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

def download_individual_files(manifest_file: str, download_dir: str, log_file: str,
                              gdc_client_executable: str, n_processes: int = None, verbose: bool = False,
                              max_retries: int = 5, sample_sheet_path: str = None):
    """
    Download each file in the manifest individually.
    Skip files that already exist; retry download if necessary.
    """
    df = pd.read_csv(manifest_file, sep="\t", dtype=str)
    failed_downloads = []
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Downloading slides"):
        file_id = row.get("id") or row.get("File ID")
        file_name = row.get("filename") or row.get("File Name")
        if not file_id or not file_name:
            logging.warning(f"Skipping row {index} because it lacks a file ID or file name.")
            continue
        expected_top_level = os.path.join(download_dir, file_name)
        if os.path.exists(expected_top_level):
            logging.info(f"Slide {file_id} already exists as {file_name}. Skipping download.")
            continue
        attempt = 0
        success = False
        while attempt < max_retries and not success:
            cmd = [gdc_client_executable, "download", "--dir", download_dir, "--log-file", log_file, file_id]
            if n_processes is not None:
                cmd.extend(["-n", str(n_processes)])
            if verbose:
                cmd.append("--debug")
            logging.info(f"Downloading slide {file_id} (attempt {attempt + 1}/{max_retries})...")
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode == 0:
                logging.info(f"Slide {file_id} downloaded successfully.")
                success = True
            else:
                logging.warning(f"Failed to download slide {file_id} (attempt {attempt + 1}): {result.stderr.strip()}")
                attempt += 1
        if not success:
            logging.warning(f"Skipping slide {file_id} after {max_retries} failed attempts.")
            failed_downloads.append(file_id)
    if failed_downloads:
        with open(log_file, "a") as f:
            for fid in failed_downloads:
                f.write(f"FAILED: {fid}\n")
        logging.info(f"{len(failed_downloads)} slides failed. See log file: {log_file}")

def remove_partial_files(dataset_dir: str):
    """Remove any partial slide files to allow for clean re-downloads."""
    for root, dirs, files in os.walk(dataset_dir):
        for file in files:
            if file.endswith('.svs.partial') or file.endswith('.tiff.partial'):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    logging.info(f"Removed partial file {file_path}")
                except Exception as e:
                    logging.error(f"Failed to remove partial file {file_path}: {e}")

def flatten_slide_directories(dataset_dir: str):
    """Flatten inner directories by moving remaining slide files to the dataset directory."""
    for entry in os.listdir(dataset_dir):
        inner_path = os.path.join(dataset_dir, entry)
        if os.path.isdir(inner_path):
            for item in os.listdir(inner_path):
                src_path = os.path.join(inner_path, item)
                if item.lower().endswith(".log") or (os.path.isdir(src_path) and item.lower() == "logs"):
                    try:
                        if os.path.isdir(src_path):
                            shutil.rmtree(src_path)
                        else:
                            os.remove(src_path)
                        logging.info(f"Discarded {src_path}")
                    except Exception as e:
                        logging.error(f"Failed to discard {src_path}: {e}")
                    continue
                dst_path = os.path.join(dataset_dir, item)
                if os.path.exists(dst_path):
                    base, ext = os.path.splitext(item)
                    counter = 1
                    new_dst_path = os.path.join(dataset_dir, f"{base}_{counter}{ext}")
                    while os.path.exists(new_dst_path):
                        counter += 1
                        new_dst_path = os.path.join(dataset_dir, f"{base}_{counter}{ext}")
                    dst_path = new_dst_path
                try:
                    shutil.move(src_path, dst_path)
                    logging.info(f"Moved {src_path} -> {dst_path}")
                except Exception as e:
                    logging.error(f"Failed to move {src_path} to {dst_path}: {e}")
            try:
                os.rmdir(inner_path)
                logging.info(f"Removed empty directory {inner_path}")
            except Exception as e:
                logging.warning(f"Could not remove directory {inner_path}: {e}")

# === Process Clinical JSON Annotations ===
def process_clinical_data_json(clinical_json_path: str, output_dir: str, norm_name: str, sample_sheet_path: str = None):
    """Process clinical JSON annotations to create survival and classification tables."""
    logging.info(f"Processing clinical data from {clinical_json_path} (JSON format)")
    with open(clinical_json_path, "r") as f:
        clinical_data = json.load(f)
    processed_records = []
    for record in clinical_data:
        case_id = record.get("submitter_id") or record.get("case_id")
        if not case_id:
            continue
        demographic = record.get("demographic", {})
        vital_status = demographic.get("vital_status", "").strip().lower() if demographic.get("vital_status") else None
        try:
            days_to_death = float(demographic.get("days_to_death")) if demographic.get("days_to_death") is not None else None
        except Exception:
            days_to_death = None
        follow_ups = record.get("follow_ups", [])
        follow_up_days = []
        for fu in follow_ups:
            try:
                d = float(fu.get("days_to_follow_up")) if fu.get("days_to_follow_up") is not None else None
            except Exception:
                d = None
            if d is not None:
                follow_up_days.append(d)
        days_to_last_follow_up = max(follow_up_days) if follow_up_days else None
        diagnoses = record.get("diagnoses", [])
        primary_diag = None
        for diag in diagnoses:
            if diag.get("diagnosis_is_primary_disease", "").lower() == "true":
                primary_diag = diag
                break
        if not primary_diag and diagnoses:
            primary_diag = diagnoses[0]
        primary_diagnosis = primary_diag.get("primary_diagnosis") if primary_diag else None
        staging_info = {}
        if primary_diag:
            for key, value in primary_diag.items():
                if "stage" in key.lower() and value is not None:
                    staging_info[key] = value
        rec = {
            "case_id": case_id,
            "days_to_death": days_to_death,
            "days_to_last_follow_up": days_to_last_follow_up,
            "vital_status": vital_status,
            "primary_diagnosis": primary_diagnosis
        }
        rec.update(staging_info)
        processed_records.append(rec)
    df = pd.DataFrame(processed_records)
    logging.info(f"Loaded clinical JSON data with {df.shape[0]} records.")
    def compute_time(row):
        if row["vital_status"] == "dead":
            return row["days_to_death"]
        elif row["vital_status"] == "alive":
            return row["days_to_last_follow_up"]
        else:
            if row["days_to_death"] is not None and row["days_to_death"] > 0:
                return row["days_to_death"]
            elif row["days_to_last_follow_up"] is not None and row["days_to_last_follow_up"] > 0:
                return row["days_to_last_follow_up"]
            else:
                return None
    def compute_event(row):
        if row["vital_status"] == "dead":
            return 1
        elif row["vital_status"] == "alive":
            return 0
        else:
            if row["days_to_death"] is not None and row["days_to_death"] > 0:
                return 1
            elif row["days_to_last_follow_up"] is not None and row["days_to_last_follow_up"] > 0:
                return 0
            else:
                return None
    df["time"] = df.apply(compute_time, axis=1)
    df["event"] = df.apply(compute_event, axis=1)
    clinical_output = os.path.join(output_dir, f"clinical_{norm_name}.csv")
    df.to_csv(clinical_output, index=False)
    logging.info(f"Processed clinical data saved to {clinical_output}")
    # Create survival table (in days)
    survival_df = df[["case_id", "time", "event"]].drop_duplicates()
    survival_df.dropna(subset=["case_id"], inplace=True)
    days_path = os.path.join(output_dir, f"survival_annotation_days_{norm_name}.csv")
    survival_df.to_csv(days_path, index=False)
    logging.info(f"Survival annotation (days) file created at {days_path}")
    # Create survival table in months
    survival_df_months = survival_df.copy()
    survival_df_months["time"] = survival_df_months["time"] / 30.44
    survival_df_months["time"] = survival_df_months["time"].round(0)
    months_path = os.path.join(output_dir, f"survival_annotation_months_{norm_name}.csv")
    survival_df_months.to_csv(months_path, index=False)
    logging.info(f"Survival annotation (months) file created at {months_path}")
    # Create survival table with quantiles
    survival_df_quantiles = survival_df.copy()
    try:
        survival_df_quantiles["time_quantile"] = pd.qcut(survival_df_quantiles["time"], q=4, labels=[1, 2, 3, 4])
    except Exception as e:
        logging.error(f"Error creating quantile bins: {e}")
        survival_df_quantiles["time_quantile"] = None
    quantiles_path = os.path.join(output_dir, f"survival_annotation_quantiles_{norm_name}.csv")
    survival_df_quantiles.to_csv(quantiles_path, index=False)
    logging.info(f"Survival annotation (quantiles) file created at {quantiles_path}")
    # Create classification table by combining classification labels into a single column "category"
    classification_cols = []
    if "primary_diagnosis" in df.columns:
        classification_cols.append("primary_diagnosis")
    additional = [col for col in df.columns if col not in ["case_id", "days_to_death", "days_to_last_follow_up", "vital_status", "time", "event", "primary_diagnosis"] and "stage" in col.lower()]
    classification_cols += additional
    if classification_cols:
        def combine_labels(row):
            labels = [str(row[col]) for col in classification_cols if pd.notna(row[col])]
            return "; ".join(labels) if labels else None
        df["category"] = df.apply(combine_labels, axis=1)
        class_df = df[["case_id", "category"]].drop_duplicates()
        class_file = os.path.join(output_dir, f"classification_annotation_{norm_name}.csv")
        class_df.to_csv(class_file, index=False)
        logging.info(f"Classification annotation file created at {class_file}")
    else:
        logging.info("No suitable classification columns found.")

def postprocess_slides(dataset_dir: str, norm_name: str):
    """
    Build a slide-to-case mapping from slide filenames in the dataset directory.
    Assumes filenames follow the format:
        {case_id_part}.{slide_id_part}.svs
    where case_id_part is defined as the first three tokens joined by "-" (e.g. "TCGA-3C-AALI")
    and the remaining part (after the first dot) is the slide_id.
    The mapping is saved with columns:
        full_slide_file, patient, slide_id
    """
    mapping_rows = []
    for root, dirs, files in os.walk(dataset_dir):
        for file in files:
            if file.lower().endswith(('.svs', '.tiff')):
                base, ext = os.path.splitext(file)
                if '.' in base:
                    pre, post = base.split('.', 1)
                else:
                    pre = base
                    post = ""
                tokens = pre.split('-')
                if len(tokens) < 3:
                    logging.warning(f"Filename {file} does not conform to expected format; skipping.")
                    continue
                patient = "-".join(tokens[:3])
                # The remaining tokens (if any) plus any string after the dot form the slide_id.
                sample_part = "-".join(tokens[3:]) if len(tokens) > 3 else ""
                slide_id = sample_part + ("." + post if post else "")
                mapping_rows.append((file, patient, slide_id))
    if mapping_rows:
        map_df = pd.DataFrame(mapping_rows, columns=["full_slide_file", "patient", "slide_id"]).drop_duplicates()
        map_df_path = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
        map_df.to_csv(map_df_path, index=False)
        logging.info(f"Slide-to-case mapping saved to {map_df_path}")
    else:
        logging.warning("No slide mappings were created.")

def download_project_tpm(project_id: str, output_dir: str = None) -> str:
    """
    Download all open-access HTSeq - TPM RNA-Seq files for a given TCGA project.

    Args:
        project_id (str): TCGA project name, e.g. "TCGA-BRCA".
        output_dir (str): Directory to save TPM files; defaults to '{project_id}_TPM'.
    Returns:
        str: Path to the directory containing downloaded TPM files and metadata.
    """
    if output_dir is None:
        output_dir = f"{project_id}_TPM"
    os.makedirs(output_dir, exist_ok=True)

    # Query GDC API for HTSeq - TPM files
    api_url = "https://api.gdc.cancer.gov/files"
    filters = {
        "op": "and",
        "content": [
            {"op": "in", "content": {"field": "cases.project.project_id", "value": [project_id]}},
            {"op": "in", "content": {"field": "data_type", "value": ["Gene Expression Quantification"]}},
            {"op": "in", "content": {"field": "analysis.workflow_type", "value": ["HTSeq - TPM"]}},
            {"op": "in", "content": {"field": "access", "value": ["open"]}},
        ]
    }
    params = {"filters": filters, "fields": "file_id,file_name,cases.submitter_id", "format": "JSON", "size": 10000}

    print(f"Querying GDC for {project_id} TPM files...")
    resp = requests.post(api_url, headers={"Content-Type": "application/json"}, data=json.dumps(params))
    resp.raise_for_status()
    hits = resp.json()["data"]["hits"]
    if not hits:
        raise RuntimeError(f"No TPM files found for project {project_id}")

    # Build metadata and download
    meta = []
    for entry in hits:
        fid = entry["file_id"]
        fname = entry["file_name"]
        case_id = entry.get("cases.submitter_id")
        if isinstance(case_id, list):
            case_id = case_id[0]
        meta.append({"file_id": fid, "file_name": fname, "case_id": case_id})

    meta_df = pd.DataFrame(meta)
    meta_csv = os.path.join(output_dir, "metadata_tpm.csv")
    meta_df.to_csv(meta_csv, index=False)

    print(f"Found {len(meta_df)} files; downloading...")
    for _, row in tqdm(meta_df.iterrows(), total=len(meta_df)):
        url = f"https://api.gdc.cancer.gov/data/{row['file_id']}"
        out_path = os.path.join(output_dir, row["file_name"])
        if os.path.exists(out_path):
            continue
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"Download complete: {output_dir}")
    return output_dir


def create_slide_gene_expression_matrix(dataset_dir: str, tpm_dir: str, norm_name: str) -> str:
    """
    Build a slide-level gene expression matrix (TPM) and save annotation file.

    Args:
        dataset_dir (str): Path to the TCGA dataset directory (where slide-to-case map is saved).
        tpm_dir (str): Directory returned by download_project_tpm().
        norm_name (str): Normalized dataset name (e.g., 'tcga-brca').
    Returns:
        str: Path to the CSV annotation file (slides x genes matrix).
    """
    # Load slide-to-case map
    map_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    map_df = pd.read_csv(map_file)

    # Load TPM metadata
    meta_file = os.path.join(tpm_dir, "metadata_tpm.csv")
    meta_df = pd.read_csv(meta_file)

    # Merge slides with TPM samples by case_id
    merged = pd.merge(map_df, meta_df, left_on="patient", right_on="case_id", how="inner")
    if merged.empty:
        raise RuntimeError("No matching cases between slides and RNA-Seq metadata.")

    # Assemble expression matrix
    expr_dfs = []
    for _, row in merged.iterrows():
        slide_name = os.path.splitext(row['full_slide_file'])[0]
        tpm_path = os.path.join(tpm_dir, row['file_name'])
        df = pd.read_csv(tpm_path, sep="\t", header=None, names=["gene_id", slide_name], index_col="gene_id")
        expr_dfs.append(df)

    # Concatenate on gene_id
    expr_mat = pd.concat(expr_dfs, axis=1)
    out_file = os.path.join(dataset_dir, f"slide_gene_expression_{norm_name}.csv")
    expr_mat.to_csv(out_file)
    print(f"Expression matrix saved to: {out_file}")
    return out_file

def create_separate_classification_annotations(dataset_dir: str, norm_name: str):
    """
    Create separate final classification annotation tables for each classification label.
    Clinical data is read from clinical_{norm_name}.csv.
    Mapping is read from sample_to_case_map_{norm_name}.csv.
    For each classification column (e.g. primary_diagnosis, any column containing "stage"),
    a final table with columns: slide, patient, category is saved as:
        final_classification_annotation_{norm_name}_{col}.csv.
    """
    clinical_file = os.path.join(dataset_dir, f"clinical_{norm_name}.csv")
    mapping_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    if not os.path.exists(clinical_file) or not os.path.exists(mapping_file):
        logging.error("Clinical file or mapping file not found. Cannot create final classification annotations.")
        return
    clinical_df = pd.read_csv(clinical_file, dtype=str)
    mapping_df = pd.read_csv(mapping_file, dtype=str)

    clinical_df.rename(columns={"case_id": "patient"}, inplace=True)
    mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    mapping_df.rename(columns={"full_slide_file": "slide"}, inplace=True)
    mapping_df["slide"] = mapping_df["slide"].str.replace(r"\.(svs|tiff)$", "", regex=True)
    base_cols = {"case_id", "days_to_death", "days_to_last_follow_up", "vital_status", "time", "event"}
    candidate_cols = [col for col in clinical_df.columns if col not in base_cols]
    classification_cols = [col for col in candidate_cols if "diagnosis" in col.lower() or "stage" in col.lower()]
    if not classification_cols:
        logging.info("No classification columns found in clinical data.")
        return
    clinical_df.rename(columns={"case_id": "patient"}, inplace=True)
    for col in classification_cols:
        class_df = clinical_df[["patient", col]].copy()
        class_df.rename(columns={col: "category"}, inplace=True)
        class_df = class_df.dropna(subset=["category"])
        if class_df.empty:
            logging.warning(f"No complete values for classification column {col}; skipping.")
            continue
        final_df = pd.merge(mapping_df, class_df, on="patient", how="inner")
        final_df = final_df[["slide", "patient", "category"]]
        out_file = os.path.join(dataset_dir, f"final_classification_annotation_{norm_name}_{col}.csv")
        final_df.to_csv(out_file, index=False)
        logging.info(f"Final classification annotation for '{col}' created at {out_file}")

def split_and_flag_annotations(annotation_file, stratify_column, split_ratio=0.9, seed=42):
    """
    Load an annotation file, flag rows with missing values,
    then add a new column 'dataset' with values 'train' or 'test' based on stratified sampling.
    The split data is saved as a new file with suffix '_split.csv'.
    For survival annotations, if stratify_column is "stratify_surv", the function dynamically creates it
    using the 'time' and 'event' columns. For classification annotations, if stratify_column is "category",
    that column is used.
    """
    df = pd.read_csv(annotation_file, dtype=str)
    complete_df = df.dropna()


    flagged_df = df[df.isnull().any(axis=1)]
    if not flagged_df.empty:
        flagged_file = annotation_file.replace(".csv", "_flagged.csv")
        flagged_df.to_csv(flagged_file, index=False)
        logging.info(f"Flagged {flagged_df.shape[0]} rows with missing data to {flagged_file}")
    if stratify_column == "stratify_surv":
        complete_df["time"] = pd.to_numeric(complete_df["time"], errors='coerce')
        complete_df["event"] = pd.to_numeric(complete_df["event"], errors='coerce')
        try:
            complete_df["time_bin"] = pd.qcut(complete_df["time"], q=4, duplicates='drop')
        except Exception as e:
            logging.error(f"Error in qcut for time: {e}")
            complete_df["time_bin"] = complete_df["time"]
        complete_df["stratify_surv"] = complete_df["event"].astype(str) + "_" + complete_df["time_bin"].astype(str)
        stratify_col = "stratify_surv"
    elif stratify_column == "category":
        stratify_col = "category"
    else:
        stratify_col = stratify_column
    if stratify_col not in complete_df.columns:
        logging.error(f"Stratify column '{stratify_col}' not found in {annotation_file}. Skipping split.")
        return
    if complete_df.shape[0] < 2:
        logging.error(f"Not enough complete samples ({complete_df.shape[0]}) in {annotation_file} to perform splitting. Skipping split.")
        return

    # Remove classes with fewer than 5 samples
    counts = complete_df[stratify_column].value_counts()
    valid_classes = counts[counts >= 5].index
    complete_df = complete_df[complete_df[stratify_column].isin(valid_classes)]

    train_df, test_df = train_test_split(complete_df, stratify=complete_df[stratify_col],
                                           test_size=(1 - split_ratio), random_state=seed)
    train_df["dataset"] = "train"
    test_df["dataset"] = "test"
    split_df = pd.concat([train_df, test_df]).sort_index()
    if "stratify_surv" in split_df.columns:
        split_df = split_df.drop(columns=["stratify_surv", "time_bin"])
    split_file = annotation_file.replace(".csv", "_split.csv")
    split_df.to_csv(split_file, index=False)
    logging.info(f"Split {annotation_file} into {split_file} with a 'dataset' column.")

def create_final_survival_annotations_days(dataset_dir: str, norm_name: str):
    """
    Create final survival annotation table (days) by joining the mapping with the survival (days) table.
    Final table columns: slide, patient, event, time
    where 'slide' is the full slide filename without extension.
    """
    mapping_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    survival_file = os.path.join(dataset_dir, f"survival_annotation_days_{norm_name}.csv")
    if not os.path.exists(mapping_file) or not os.path.exists(survival_file):
        logging.error("Mapping file or survival (days) file not found. Cannot create final survival annotations (days).")
        return
    mapping_df = pd.read_csv(mapping_file, dtype=str)
    survival_df = pd.read_csv(survival_file, dtype=str)
    # Map any case_id to patient
    mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    survival_df.rename(columns={"case_id": "patient"}, inplace=True)

    final_df = pd.merge(mapping_df, survival_df, on="patient", how="inner")
    final_df.rename(columns={"full_slide_file": "slide"}, inplace=True)
    final_df["slide"] = final_df["slide"].str.replace(r"\.(svs|tiff)$", "", regex=True)
    final_df = final_df[["slide", "patient", "event", "time"]]
    out_file = os.path.join(dataset_dir, f"final_survival_annotation_{norm_name}_days.csv")
    final_df.to_csv(out_file, index=False)
    logging.info(f"Final survival (days) annotation file created at {out_file}")

def create_final_survival_annotations_months(dataset_dir: str, norm_name: str):
    """
    Create final survival annotation table (months) by joining the mapping with the survival (months) table.
    Final table columns: slide, patient, event, time (in months).
    """
    mapping_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    survival_file = os.path.join(dataset_dir, f"survival_annotation_months_{norm_name}.csv")
    if not os.path.exists(mapping_file) or not os.path.exists(survival_file):
        logging.error("Mapping file or survival (months) file not found. Cannot create final survival annotations (months).")
        return
    mapping_df = pd.read_csv(mapping_file, dtype=str)
    survival_df = pd.read_csv(survival_file, dtype=str)
    # Map any case_id to patient
    mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    survival_df.rename(columns={"case_id": "patient"}, inplace=True)

    final_df = pd.merge(mapping_df, survival_df, on="patient", how="inner")
    final_df.rename(columns={"full_slide_file": "slide"}, inplace=True)
    final_df["slide"] = final_df["slide"].str.replace(r"\.(svs|tiff)$", "", regex=True)
    final_df = final_df[["slide", "patient", "event", "time"]]
    out_file = os.path.join(dataset_dir, f"final_survival_annotation_{norm_name}_months.csv")
    final_df.to_csv(out_file, index=False)
    logging.info(f"Final survival (months) annotation file created at {out_file}")

def create_final_survival_annotations_quantiles(dataset_dir: str, norm_name: str):
    """
    Create final survival annotation table (quantiles) by joining the mapping with the survival (quantiles) table.
    Final table columns: slide, patient, event, time, time_quantile.
    """
    mapping_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    survival_file = os.path.join(dataset_dir, f"survival_annotation_quantiles_{norm_name}.csv")
    if not os.path.exists(mapping_file) or not os.path.exists(survival_file):
        logging.error("Mapping file or survival (quantiles) file not found. Cannot create final survival annotations (quantiles).")
        return
    mapping_df = pd.read_csv(mapping_file, dtype=str)
    survival_df = pd.read_csv(survival_file, dtype=str)
    # Map any case_id to patient
    mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    survival_df.rename(columns={"case_id": "patient"}, inplace=True)

    final_df = pd.merge(mapping_df, survival_df, on="patient", how="inner")
    final_df.rename(columns={"full_slide_file": "slide"}, inplace=True)
    final_df["slide"] = final_df["slide"].str.replace(r"\.(svs|tiff)$", "", regex=True)
    final_df = final_df[["slide", "patient", "event", "time_quantile"]]
    #Map column name 'time_quantile' to 'time'
    final_df.rename(columns={"time_quantile": "time"}, inplace=True)
    out_file = os.path.join(dataset_dir, f"final_survival_annotation_{norm_name}_quantiles.csv")
    final_df.to_csv(out_file, index=False)
    logging.info(f"Final survival (quantiles) annotation file created at {out_file}")

def create_separate_classification_annotations(dataset_dir: str, norm_name: str):
    """
    Create separate final classification annotation tables for each classification label.
    Clinical data is read from clinical_{norm_name}.csv.
    Mapping is read from sample_to_case_map_{norm_name}.csv.
    For each classification column (e.g. primary_diagnosis, any column containing "stage"),
    a final table with columns: slide, patient, category is saved as:
        final_classification_annotation_{norm_name}_{col}.csv.
    """
    clinical_file = os.path.join(dataset_dir, f"clinical_{norm_name}.csv")
    mapping_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    if not os.path.exists(clinical_file) or not os.path.exists(mapping_file):
        logging.error("Clinical file or mapping file not found. Cannot create final classification annotations.")
        return
    clinical_df = pd.read_csv(clinical_file, dtype=str)
    mapping_df = pd.read_csv(mapping_file, dtype=str)
    # Map any case_id to patient
    mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    clinical_df.rename(columns={"case_id": "patient"}, inplace=True)
    
    mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    mapping_df.rename(columns={"full_slide_file": "slide"}, inplace=True)
    mapping_df["slide"] = mapping_df["slide"].str.replace(r"\.(svs|tiff)$", "", regex=True)
    base_cols = {"case_id", "days_to_death", "days_to_last_follow_up", "vital_status", "time", "event"}
    candidate_cols = [col for col in clinical_df.columns if col not in base_cols]
    classification_cols = [col for col in candidate_cols if "diagnosis" in col.lower() or "stage" in col.lower()]
    if not classification_cols:
        logging.info("No classification columns found in clinical data.")
        return
    clinical_df.rename(columns={"case_id": "patient"}, inplace=True)
    for col in classification_cols:
        class_df = clinical_df[["patient", col]].copy()
        class_df.rename(columns={col: "category"}, inplace=True)
        class_df = class_df.dropna(subset=["category"])
        if class_df.empty:
            logging.warning(f"No complete values for classification column {col}; skipping.")
            continue
        final_df = pd.merge(mapping_df, class_df, on="patient", how="inner")
        final_df = final_df[["slide", "patient", "category"]]
        out_file = os.path.join(dataset_dir, f"final_classification_annotation_{norm_name}_{col}.csv")
        final_df.to_csv(out_file, index=False)
        logging.info(f"Final classification annotation for '{col}' created at {out_file}")

def create_final_grouped_stage_annotation(dataset_dir: str, norm_name: str):
    """
    Create final grouped stage annotation table by joining the mapping with the stage information from clinical data.
    The stage information (e.g. 'Stage IIA', 'Stage IIB') is grouped by stripping trailing characters from the 
    numeral part that are not valid Roman numeral letters (valid set: "IVXLCDM").
    For example, 'Stage IIA' becomes 'Stage II' and 'Stage I' remains unchanged.
    The final table columns: slide, patient, grouped_stage.
    """
    clinical_file = os.path.join(dataset_dir, f"clinical_{norm_name}.csv")
    mapping_file = os.path.join(dataset_dir, f"sample_to_case_map_{norm_name}.csv")
    if not os.path.exists(clinical_file) or not os.path.exists(mapping_file):
        logging.error("Clinical file or mapping file not found. Cannot create final grouped stage annotation.")
        return
    clinical_df = pd.read_csv(clinical_file, dtype=str)
    mapping_df = pd.read_csv(mapping_file, dtype=str)
    
    # Normalize column names for merging.
    if "case_id" in clinical_df.columns:
        clinical_df.rename(columns={"case_id": "patient"}, inplace=True)
    if "case_id" in mapping_df.columns:
        mapping_df.rename(columns={"case_id": "patient"}, inplace=True)
    mapping_df.rename(columns={"full_slide_file": "slide"}, inplace=True)
    mapping_df["slide"] = mapping_df["slide"].str.replace(r"\.(svs|tiff)$", "", regex=True)
    
    # Determine the stage column: prefer 'pathologic_stage', then 'clinical_stage', else any with "stage".
    stage_col = None
    for col in clinical_df.columns:
        if col.lower() == "pathologic_stage":
            stage_col = col
            break
    if stage_col is None:
        for col in clinical_df.columns:
            if col.lower() == "clinical_stage":
                stage_col = col
                break
    if stage_col is None:
        stage_cols = [col for col in clinical_df.columns if "stage" in col.lower()]
        if stage_cols:
            stage_col = stage_cols[0]
    if stage_col is None:
        logging.info("No stage column found in clinical data. Skipping grouped stage annotation creation.")
        return

    def group_stage(stage_str):
        if not stage_str or pd.isna(stage_str):
            return None
        stage_str = stage_str.strip()
        # Split the string into tokens; assume the numeral part is the last token.
        tokens = stage_str.split()
        if not tokens:
            return stage_str
        numeral = tokens[-1]
        # Define the allowed characters in a valid Roman numeral.
        allowed = "IV"
        # Remove trailing characters from the numeral that are not allowed.
        while len(numeral) > 1 and numeral[-1] not in allowed:
            numeral = numeral[:-1]
        tokens[-1] = numeral
        return " ".join(tokens)
    
    #Filter any rows that do not have "I", "II", "III", "IV", "V", "VI" in the stage column
    clinical_df = clinical_df[clinical_df[stage_col].str.contains(r"[IV]", na=False)]
    # Apply the grouping logic to the chosen stage column.
    clinical_df["grouped_stage"] = clinical_df[stage_col].apply(group_stage)
    
    # Merge the clinical data with the slide mapping so that each slide gets its grouped stage.
    final_df = pd.merge(mapping_df, clinical_df[["patient", "grouped_stage"]], on="patient", how="inner")
    final_df = final_df[["slide", "patient", "grouped_stage"]]

    #Rename the column 'grouped_stage' to 'category'
    final_df.rename(columns={"grouped_stage": "category"}, inplace=True)
    # Save the final grouped stage annotation file.
    out_file = os.path.join(dataset_dir, f"final_classification_annotation_groupstage_{norm_name}.csv")
    final_df.to_csv(out_file, index=False)
    logging.info(f"Final grouped stage annotation file created at {out_file}")

def create_final_survival_annotations(dataset_dir: str, norm_name: str):
    """
    Create separate final survival annotation tables for days, months, and quantiles.
    Each table has columns: slide, patient, event, time, (and time_quantile for quantiles).
    """
    create_final_survival_annotations_days(dataset_dir, norm_name)
    create_final_survival_annotations_months(dataset_dir, norm_name)
    create_final_survival_annotations_quantiles(dataset_dir, norm_name)

def write_if_not_exists(df: pd.DataFrame, path: str, **to_csv_kwargs):
    if os.path.exists(path):
        logging.info(f"{path} already exists -> skipping")
    else:
        df.to_csv(path, index=False, **to_csv_kwargs)
        logging.info(f"Wrote {path}")

def create_source_prediction(parent_dir: str,
                             dataset_names: list[str],
                             split_ratio: float = 0.9,
                             seed: int = 42):
    out_path = os.path.join(parent_dir, "source_prediction.csv")
    if os.path.exists(out_path):
        print(f"{out_path} already exists, skipping.")
        return

    frames = []
    for ds in dataset_names:
        norm = ds.lower()
        map_file = os.path.join(parent_dir, ds, f"sample_to_case_map_{norm}.csv")
        if not os.path.exists(map_file):
            print(f"  ✗ mapping file not found for {ds}: {map_file}")
            continue
        df = pd.read_csv(map_file, dtype=str)
        # strip extension off 'full_slide_file'
        df["slide"] = df["full_slide_file"].str.replace(r"\.(svs|tiff)$", "", regex=True)
        df = df[["slide", "patient"]]
        df["source"] = ds
        frames.append(df)

    if not frames:
        print("No mappings loaded; aborting.")
        return

    all_df = pd.concat(frames, ignore_index=True)
    # Now stratify-split on 'source'
    train_df, test_df = train_test_split(
        all_df,
        stratify=all_df["source"],
        test_size=(1 - split_ratio),
        random_state=seed
    )
    train_df["dataset"] = "train"
    test_df["dataset"] = "test"
    result = pd.concat([train_df, test_df], ignore_index=True)

    result.to_csv(out_path, index=False)
    print(f"Saved source prediction to {out_path}")


def download_dataset(dataset_name: str, parent_dir: str, manifest_dir: str,
                     raw_annotations_dir: str, gdc_client_executable: str,
                     n_processes: int = None, verbose: bool = False):
    """
    Download a TCGA dataset, process clinical annotations, and generate final annotation tables.
    """
    logging.info(f"--- Processing dataset: {dataset_name} ---")
    dataset_dir = os.path.join(parent_dir, dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)
    norm_name = normalize_dataset_name(dataset_name)
    expected_manifest = f"gdc_manifest.{norm_name.replace('-', '_')}.txt"
    manifest_file = os.path.join(manifest_dir, expected_manifest)
    if not os.path.exists(manifest_file):
        logging.error(f"Manifest file {manifest_file} not found for dataset {dataset_name}")
        return
    logging.info(f"Using manifest file: {manifest_file}")
    log_file = os.path.join(dataset_dir, f"tcga-{dataset_name}-download.log")
    download_individual_files(
        manifest_file=manifest_file,
        download_dir=dataset_dir,
        log_file=log_file,
        gdc_client_executable=gdc_client_executable,
        n_processes=n_processes,
        verbose=verbose,
        max_retries=5,
        sample_sheet_path=None
    )
    logging.info(f"Downloaded files for dataset {dataset_name} to {dataset_dir}")
    logging.info(f"Looking for clinical annotations in {raw_annotations_dir}, expected file: clinical.project-{norm_name}.json")
    clinical_annotation_src = os.path.join(raw_annotations_dir, f"clinical.project-{norm_name}.json")
    if os.path.exists(clinical_annotation_src):
        process_clinical_data_json(clinical_annotation_src, dataset_dir, norm_name, sample_sheet_path=None)
    else:
        logging.warning(f"Clinical annotation file not found: {clinical_annotation_src}")
    remove_partial_files(dataset_dir)
    postprocess_slides(dataset_dir, norm_name)
    logging.info("Post-processed slides.")
    flatten_slide_directories(dataset_dir)
    logging.info("Flattened slide directories.")
    create_final_survival_annotations(dataset_dir, norm_name)
    logging.info("Created final survival annotations (days, months, quantiles).")
    create_separate_classification_annotations(dataset_dir, norm_name)
    logging.info("Created final classification annotations.")
    create_final_grouped_stage_annotation(dataset_dir, norm_name)
    logging.info("Created final grouped stage annotation.")

def main():
    """
    Parse command-line arguments and process each specified TCGA dataset.
    Optionally, split final annotations into train/test sets by adding a 'dataset' column.
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
                        help="Directory containing the raw annotation JSON files (pre-existing)")
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
    parser.add_argument("--split", action="store_true",
                        help="If set, add a 'dataset' column to final annotations indicating 'train' or 'test'")
    parser.add_argument("--split-ratio", type=float, default=0.9,
                        help="Train split ratio (default: 0.9, meaning 90%% train, 10%% test)")
    parser.add_argument("--create-source-prediction", action="store_true",
                        help="If set, create a source prediction file that combines all datasets' splits with a 'source' column")
    parser.add_argument("--rna-seq-tpm", action="store_true", default=False,
                        help="If set, download RNA-Seq TPM files for the specified datasets")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for splitting (default: 42)")
    args = parser.parse_args()
    os.makedirs(args.parent_dir, exist_ok=True)
    gdc_client_executable = args.gdc_client_path
    if args.build_gdc or not os.path.exists(gdc_client_executable):
        bin_dir = os.path.join(args.gdc_client_dir, "bin")
        try:
            gdc_client_executable = build_gdc_client(bin_dir, args.gdc_client_path)
        except Exception as e:
            logging.error(f"Failed to build/install gdc-client: {e}")
            return
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

        if args.rna_seq_tpm:
            # Download TPM files for the dataset
            tpm_dir = download_project_tpm(project_id=dataset, output_dir=os.path.join(args.parent_dir, f"{dataset}_TPM"))
            if tpm_dir:
                # Create slide-level gene expression matrix
                create_slide_gene_expression_matrix(dataset_dir=os.path.join(args.parent_dir, dataset),
                                                    tpm_dir=tpm_dir,
                                                    norm_name=normalize_dataset_name(dataset))
            else:
                logging.error(f"Failed to download TPM files for {dataset}")
    if args.split:
        for dataset in args.datasets:
            norm_name = normalize_dataset_name(dataset)
            dataset_dir = os.path.join(args.parent_dir, dataset)
            # Process each final survival file (days, months, quantiles)
            for suffix in ["days", "months", "quantiles"]:
                surv_file = os.path.join(dataset_dir, f"final_survival_annotation_{norm_name}_{suffix}.csv")
                if os.path.exists(surv_file):
                    split_and_flag_annotations(surv_file, stratify_column="stratify_surv", split_ratio=args.split_ratio, seed=args.seed)
            # Process each final classification file (each separate file)
            for file in os.listdir(dataset_dir):
                if file.startswith(f"final_classification_annotation") and file.endswith(".csv"):
                    class_file = os.path.join(dataset_dir, file)
                    split_and_flag_annotations(class_file, stratify_column="category", split_ratio=args.split_ratio, seed=args.seed)
    
    logging.info("All datasets have been processed.")

    if args.create_source_prediction:
        #If no datasets were specified, use all datasets in the parent directory
        if not args.datasets:
            args.datasets = [d for d in os.listdir(args.parent_dir) if os.path.isdir(os.path.join(args.parent_dir, d))]
            if not args.datasets:
                logging.error("No datasets found in the parent directory. Cannot create source prediction file.")
                return
        # Create source prediction file with all datasets
        create_source_prediction(
            parent_dir=args.parent_dir,
            dataset_names=args.datasets,
            seed=args.seed
        )
        logging.info("Source prediction file created.")

if __name__ == "__main__":
    main()
