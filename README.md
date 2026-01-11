
# TCGA-Tools
**TCGA-Tools** is a Python package that provides a clean, modular interface for downloading and organizing datasets from the NCI Genomic Data Commons (GDC) portal. It allows you to fetch raw data (e.g., `.svs` diagnostic slides, sequencing data) together with directly usable annotations (clinical, molecular, diagnostic reports, etc.), and automatically groups data at the **patient level** for easy analysis.

<p align="center">
  <img src="tcga_tools.png" width="300"/>
</p>

## Goals
- Simple one-liner to fetch project files (e.g., whole-slide images / `.svs`).
- Write analysis-ready CSVs with file metadata and patient grouping.
- Emit optional annotation CSVs: clinical (survival/outcomes/treatments), molecular (DNA/RNA/CNV/methylation), free-text reports, and diagnosis/subtype.
- Be resilient to missing or sparse fields across projects.

## 🚀 Features
- Clean, modular architecture with explicit ports/adapters for GDC and TCIA.
- GDC and TCIA downloads built on top of the [`gdc-api-wrapper`](https://github.com/histolab/gdc-api-wrapper).
- Multi-dataset support (download one or multiple TCGA projects at once).
- Annotation options:  
  - `clinical`: survival, treatment outcomes, patient metadata  
  - `molecular`: genomic, transcriptomic, and methylation data  
  - `report`: free-text pathology or clinical reports  
  - `diagnosis`: tumor subtype and diagnostic information  
  - `all`: fetch everything available
- Progress bars for downloads.
- Logging of all transformations for reproducibility.
- `raw=True` option for “dry runs” (inspect available data without downloading).
- Optional **statistics** and **visualizations**: class distributions, survival curves, annotation summaries.

## 📦 Installation

### From PyPI (pip)
```bash
pip install tcga-tools
```

### From PyPI (uv)
```bash
uv pip install tcga-tools
```

### Optional Pathology Dependencies (TCIA + GDC wrapper)
```bash
pip install gdc-api-wrapper
```

### From Source
```bash
git clone https://github.com/LUMCPathAI/TCGA-Tools.git
cd TCGA-Tools
pip install -e .
```

## Quickstart
```python
import tcga_tools as tt

tt.Download(
    dataset_name="TCGA-LUSC",
    filetypes=[".svs"],
    datatype=["WSI"],
    annotations=["clinical", "molecular", "report"],
    output_dir="./TCGA-LUSC",
    statistics=True,
    visualizations=True
)

#Download multiple datasets
tt.Download(
    dataset_name=["TCGA-LUSC", "TCGA-LUAD", "TCGA-BRCA"],  # list of datasets
    filetypes=[".svs", ".maf"],                            # multiple file types
    annotations="all",                                     # fetch everything
    output_dir="./TCGA",
)
```

## 🧬 Pathology Portal (TCGA + TCIA)
Use the high-level portal to query pathology metadata and download slides from
TCGA (GDC) and TCIA using clean, modular services.

```python
from tcga_tools.pathology import PathologyDataPortal
from tcga_tools.services.tcia_pathology import TciaSeriesQuery

portal = PathologyDataPortal()

# --- TCIA: SOP Instance lookup and downloads ---
query = TciaSeriesQuery(series_instance_uid="uid.series.instance", format_="JSON")
sop_result = portal.list_tcia_sop_instance_uids(query)
portal.download_tcia_series(series_instance_uid="uid.series.instance", output_dir="./TCIA")

# --- TCGA: download pathology files via GDC wrapper ---
tcga_files = portal.download_tcga_project(
    project_id="TCGA-LUSC",
    filetypes=[".svs"],
    output_dir="./TCGA-LUSC",
)
```

## 📚 TCIA Pathology Workflows
TCIA endpoints supported via the wrapper:
- SOPInstanceUID lookup for a SeriesInstanceUID (`sop_instance_uids`)
- Single-image download for a SeriesInstanceUID + SOPInstanceUID
- Series download as a zip file

## ✅ Usage Examples

### 1) Download all TCGA lung slides with subtype labels + clinical endpoints
```python
import tcga_tools as tt

tt.Download(
    dataset_name=["TCGA-LUAD", "TCGA-LUSC"],
    filetypes=[".svs"],
    annotations=["clinical", "diagnosis"],
    output_dir="./TCGA-LUNG",
)
```

### 2) Download all TCGA-SKCM slides with genetic information
```python
import tcga_tools as tt

tt.Download(
    dataset_name="TCGA-SKCM",
    filetypes=[".svs"],
    annotations=["molecular"],
    output_dir="./TCGA-SKCM",
)
```

### 3) Download all TCIA radiology and pathology images from a dataset
```python
from tcga_tools.pathology import PathologyDataPortal
from tcga_tools.services.tcia_pathology import TciaSeriesQuery

portal = PathologyDataPortal()

# Suppose you already have SeriesInstanceUIDs for a TCIA collection
series_uids = [
    "uid.series.instance.1",
    "uid.series.instance.2",
]

for series_uid in series_uids:
    sop_payload = portal.list_tcia_sop_instance_uids(
        TciaSeriesQuery(series_instance_uid=series_uid, format_="JSON")
    )
    portal.download_tcia_series(series_instance_uid=series_uid, output_dir="./TCIA-DATASET")
```

## 📊 Example Outputs (with statistics=True, visualizations=True)

- Summary log of transformations and queries

- Distributions of diagnosis categories

- Survival curves based on clinical annotations

- Counts per file type and annotation

## Outputs 
- `data/` (downloads)
- `files_metadata.csv` (flattened file + case/sample fields)
- `groups.csv` (per-case: paired / tumor_only / normal_only)
- `clinical.csv`, `molecular_index.csv`, `reports_index.csv`, `diagnosis.csv` (if requested)
- `gdc_manifest.tsv` (for the GDC Transfer Tool)

## Authentication
If you need controlled-access files, set an environment variable with your token:
```bash
export GDC_TOKEN="<your token>"
```
## Checking available datasets
```python
import tcga_tools as tt
tt.list_datasets()
```
## Annotations argument
Pass any subset of:
- `"clinical"` — survival/clinical outcome/treatment effect (diagnoses, treatments, follow-ups, exposures)
- `"molecular"` — DNA/RNA/CNV/Methylation file index
- `"report"` — free-text/clinical/pathology reports (XML/PDF)
- `"diagnosis"` — diagnostic subtype, morphology, stage/grade
- `"all"` — everything above

## Handling missing data
GDC projects vary in completeness. TCGA-Tools is defensive:
- Broad field requests; if the API rejects fields (HTTP 400), it **retries without fields** to maximize returned content.
- JSON is **flattened** into wide CSVs; absent fields simply do not appear, or appear with empty values.
- Grouping logic remains robust even if sample types are missing.

## CLI
```bash
python -m tcga_tools --dataset TCGA-LUSC --filetypes .svs \
  --annotations clinical molecular report diagnosis --out ./TCGA-LUSC
```

## Requirements
- Python ≥ 3.9
- Tested on Linux, macOS, Windows
- Dependencies are listed in `pyproject.toml` and installed automatically.

## Logging
All downloads and transformations are logged to `download.log` in your output directory for reproducibility.

## Raw mode
Preview available data without downloading:
```python
tt.Download(dataset_name="TCGA-LUSC", raw=True)
```

## Testing
Run unit tests:
```bash
pytest tests/
```

## Notes
- For very large downloads, prefer the emitted `gdc_manifest.tsv` with the GDC Data Transfer Tool.
- Extend `config.py` to add/modify field lists or filetype preferences as needed.

## License

Apache 2.0 — free for research and commercial use.

## Contributing

Contributions are welcome! Please open an issue or PR on GitHub.
