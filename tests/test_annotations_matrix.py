import tempfile
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
                    "data_category": "Simple Nucleotide Variation",
                    "data_type": "Aggregated Somatic Mutation",
                    "cases.case_id": "C1",
                    "cases.submitter_id": "TCGA-XX-0001",
                    "cases.project.project_id": "TCGA-LUSC",
                    "cases.samples.sample_type": "Primary Tumor",
                }
            ]
        return []

    def cases_query(self, filters, fields, size=5000):
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
            }
        ]

    def download_manifest_for_query(self, filters, *, manifest_path: str) -> str:
        open(manifest_path, "wb").write(b"file_id\tfile_name\n")
        return manifest_path


class FakeDownloader:
    def download_single(self, uuid: str, *, path: str, name: Optional[str] = None) -> DownloadResult:
        filename = name or uuid
        target = f"{path}/{filename}"
        open(target, "wb").close()
        return DownloadResult(path=target, filename=filename)

    def download_multiple(self, uuids, *, path: str) -> DownloadResult:
        target = f"{path}/gdc_download.tar.gz"
        open(target, "wb").close()
        return DownloadResult(path=target, filename="gdc_download.tar.gz")


def _run_download(tmp_dir: str, annotations):
    return tcga_tools.Download(
        dataset_name="TCGA-LUSC",
        filetypes=[".svs"],
        annotations=annotations,
        output_dir=tmp_dir,
        raw=True,
    )


def test_download_annotations_matrix(monkeypatch):
    monkeypatch.setattr(_dl, "GDCClient", FakeClient)
    monkeypatch.setattr(_dl, "GdcApiWrapperDownloader", FakeDownloader)

    cases = [
        (None, {"clinical_csv": None, "molecular_csv": None, "report_csv": None, "diagnosis_csv": None}),
        (["clinical"], {"clinical_csv": "clinical.csv"}),
        (["molecular"], {"molecular_csv": "molecular_index.csv"}),
        (["report"], {"report_csv": "reports_index.csv"}),
        (["diagnosis"], {"diagnosis_csv": "diagnosis.csv"}),
        (["all"], {
            "clinical_csv": "clinical.csv",
            "molecular_csv": "molecular_index.csv",
            "report_csv": "reports_index.csv",
            "diagnosis_csv": "diagnosis.csv",
        }),
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        for annotations, expected in cases:
            artifacts = _run_download(tmp_dir, annotations)
            for key, expected_name in expected.items():
                if expected_name is None:
                    assert key not in artifacts or artifacts[key] is None
                else:
                    assert key in artifacts
                    assert artifacts[key] is not None
                    assert artifacts[key].name == expected_name
                    assert artifacts[key].exists()
