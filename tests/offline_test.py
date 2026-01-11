import os
import tempfile
import pandas as pd
from typing import Optional

import tcga_tools
from tcga_tools import downloader as _dl
from tcga_tools.ports import DownloadResult


class FakeClient:
    def __init__(self, *args, **kwargs):
        pass

    def paged_query(self, endpoint, filters, fields, size=5000):
        if endpoint == "files":
            return [
                {
                    "id": "F1",
                    "file_name": "slide1.svs",
                    "data_format": "SVS",
                    "cases.case_id": "C1",                 # flattened
                    "cases.submitter_id": "TCGA-XX-0001", # flattened
                    "cases.project.project_id": "TCGA-LUSC",
                    "cases.samples.sample_type": "Primary Tumor",  # flattened
                },
                {
                    "id": "F2",
                    "file_name": "slide2.svs",
                    "data_format": "SVS",
                    "cases.case_id": "C2",
                    "cases.submitter_id": "TCGA-XX-0002",
                    "cases.project.project_id": "TCGA-LUSC",
                    "cases.samples.sample_type": "Solid Tissue Normal",
                },
            ]
        return []

    def cases_query(self, filters, fields, size=5000):
        # Return a mixture of clinical + diagnosis content w/ some missing fields
        return [
            {
                "case_id": "C1",
                "submitter_id": "TCGA-XX-0001",
                "diagnoses": [{
                    "primary_diagnosis": "Lung Squamous Cell Carcinoma",
                    "tumor_stage": "Stage II",
                    "vital_status": "Alive",
                    "days_to_last_follow_up": 1200,
                }],
                "treatments": [{"treatment_type": "Radiation", "measure_of_response": "PR"}],
            },
            {
                "case_id": "C2",
                "submitter_id": "TCGA-XX-0002",
                "diagnoses": [{
                    "primary_diagnosis": "Lung Squamous Cell Carcinoma",
                    "tumor_stage": None,
                    "vital_status": "Dead",
                    "days_to_death": 800,
                }],
                # No treatments / follow-ups for this case -> tests missing handling
            },
        ]

    def download_single(self, uuid: str, *, target_path: str, related_files: bool = False) -> str:
        # Create an empty file to simulate a download
        open(target_path, "wb").close()
        return target_path

    def download_tar(self, uuids, *, target_path: str, uncompressed: bool = False) -> str:
        open(target_path, "wb").close()
        return target_path

    def download_manifest_for_query(self, filters, *, manifest_path: str) -> str:
        open(manifest_path, "wb").write(b"file_id\tfile_name\n")
        return manifest_path


class FakeDownloader:
    def download_single(self, uuid: str, *, path: str, name: Optional[str] = None) -> DownloadResult:
        os.makedirs(path, exist_ok=True)
        filename = name or uuid
        target = os.path.join(path, filename)
        open(target, "wb").close()
        return DownloadResult(path=target, filename=filename)

    def download_multiple(self, uuids, *, path: str) -> DownloadResult:
        os.makedirs(path, exist_ok=True)
        target = os.path.join(path, "gdc_download.tar.gz")
        open(target, "wb").close()
        return DownloadResult(path=target, filename="gdc_download.tar.gz")


def test_download_with_all_annotations(monkeypatch):
    # Patch the real client in the downloader to use our FakeClient
    monkeypatch.setattr(_dl, "GDCClient", FakeClient)
    monkeypatch.setattr(_dl, "GdcApiWrapperDownloader", FakeDownloader)

    with tempfile.TemporaryDirectory() as d:
        out = tcga_tools.Download(
            dataset_name="TCGA-LUSC",
            filetypes=[".svs"],
            annotations=["all"],
            output_dir=d,
        )
        # Files
        assert (out["files_csv"]).exists()
        assert (out["groups_csv"]).exists()
        assert (out["manifest_tsv"]).exists()
        # Annotation CSVs
        assert (out["clinical_csv"]).exists()
        assert (out["molecular_csv"]).exists() or out["molecular_csv"] is None  # may be empty with fake client
        assert (out["report_csv"]).exists() or out["report_csv"] is None
        assert (out["diagnosis_csv"]).exists()

        # Downloads directory
        assert (out["data_dir"]).exists()

        print("Successfully verified data directory creation.")
