from __future__ import annotations
import os

GDC_BASE_URL: str = os.environ.get("GDC_BASE_URL", "https://api.gdc.cancer.gov")
# Where to look for the auth token by default
GDC_TOKEN_ENV: str = os.environ.get("GDC_TOKEN_ENV", "GDC_TOKEN")

# Default fields we pull for files; include nested case + sample info for easy CSVs
DEFAULT_FILE_FIELDS = [
    "id",
    "file_id",
    "file_name",
    "md5sum",
    "state",
    "file_size",
    "data_category",
    "data_type",
    "data_format",
    "experimental_strategy",
    "cases.case_id",
    "cases.submitter_id",
    "cases.project.project_id",
    "cases.project.name",
    "cases.disease_type",
    "cases.primary_site",
    "cases.diagnoses.age_at_diagnosis",
    "cases.diagnoses.vital_status",
    "cases.diagnoses.days_to_death",
    "cases.diagnoses.days_to_last_follow_up",
    "cases.demographic.gender",
    "cases.demographic.race",
    "cases.demographic.ethnicity",
    "cases.samples.sample_id",
    "cases.samples.submitter_id",
    "cases.samples.sample_type",
    "cases.samples.portions.portion_id",
]

# Minimal fields for the cases endpoint (for clinical annotations)
DEFAULT_CASE_FIELDS = [
    "case_id",
    "submitter_id",
    "project.project_id",
    "project.name",
    "disease_type",
    "primary_site",
    "demographic.gender",
    "demographic.race",
    "demographic.ethnicity",
    "demographic.year_of_birth",
]

# Extended clinical/survival/treatment outcome fields (best-effort; missing fields are tolerated).
CLINICAL_FIELDS = [
    # Diagnoses (survival/outcomes often hang here)
    "diagnoses.primary_diagnosis",
    "diagnoses.morphology",
    "diagnoses.tumor_stage",
    "diagnoses.tumor_grade",
    "diagnoses.vital_status",
    "diagnoses.days_to_death",
    "diagnoses.days_to_last_follow_up",
    # Treatments
    "treatments.treatment_type",
    "treatments.therapeutic_agents",
    "treatments.measure_of_response",
    "treatments.days_to_treatment",
    # Follow-ups / outcomes
    "follow_ups.days_to_last_follow_up",
    "follow_ups.vital_status",
    "follow_ups.progression_or_recurrence",
    "follow_ups.days_to_recurrence",
    # Exposures (risk factors)
    "exposures.cigarettes_per_day",
    "exposures.alcohol_history",
]

# Focused diagnosis/subtyping fields
DIAGNOSIS_FIELDS = [
    "diagnoses.primary_diagnosis",
    "diagnoses.morphology",
    "diagnoses.tumor_stage",
    "diagnoses.tumor_grade",
]

# Categories/types used for molecular and reports selection
MOLECULAR_CATEGORIES = [
    "Simple Nucleotide Variation",
    "Transcriptome Profiling",
    "Copy Number Variation",
    "DNA Methylation",
    "Somatic Structural Variation", 
    "Proteome Profiling",
    "Sequencing Reads",
]

REPORT_DATA_TYPES = [
    "Pathology Report",
    "Clinical Supplement",
]

# Very light-weight mapping from common extensions to preferred criteria.
# Preference order: data_format -> data_type -> filename suffix fallback.
FILETYPE_PREFERENCES = {
    ".svs": {"data_format": ["SVS"], "data_type": ["Diagnostic Slide Image"]},
    ".ndpi": {"data_format": ["NDPI"]},
    ".bcr.xml": {"data_format": ["BCR XML"], "data_category": ["Clinical"]},
    ".xml": {"data_format": ["BCR XML"], "data_category": ["Clinical"]},
    ".bam": {"data_format": ["BAM"]},
    ".vcf": {"data_format": ["VCF"]},
    ".maf": {"data_format": ["MAF"]},
    ".txt": {"data_format": ["TSV", "TXT"]},
    ".tsv": {"data_format": ["TSV"]},
}

# Grouping: map TCGA sample_type to a coarse group
SAMPLE_TYPE_TO_GROUP = {
    "Primary Tumor": "tumor",
    "Metastatic": "tumor",
    "Recurrent Tumor": "tumor",
    "Solid Tissue Normal": "normal",
    "Blood Derived Normal": "normal",
}
