import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

import tcga_tools
from tcga_tools import downloader as _dl
from tcga_tools import patho_bench as _patho_bench
from tcga_tools import tcia as _tcia


def _contains_value(node, target):
    if isinstance(node, dict):
        return any(_contains_value(value, target) for value in node.values())
    if isinstance(node, list):
        return any(_contains_value(value, target) for value in node)
    return node == target


class FakeClient:
    def __init__(self, *args, **kwargs):
        self.last_queries = []
        self.downloaded_ids = []

    def paged_query(self, endpoint, filters, fields, size=5000):
        self.last_queries.append(
            {
                "endpoint": endpoint,
                "filters": filters,
                "requested_fields": list(fields) if fields else None,
            }
        )
        if endpoint == "files":
            if _contains_value(filters, "Gene Expression Quantification"):
                return [
                    {
                        "id": "R1",
                        "file_name": "rna_seq.tsv",
                        "data_format": "TSV",
                        "data_type": "Gene Expression Quantification",
                        "cases.case_id": "C1",
                        "cases.submitter_id": "TCGA-XX-0001",
                        "cases.project.project_id": "TCGA-LUSC",
                        "cases.samples.sample_type": "Primary Tumor",
                    }
                ]
            return [
                {
                    "id": "F1",
                    "file_name": "slide1.svs",
                    "data_format": "SVS",
                    "data_type": "Slide Image",
                    "cases.case_id": "C1",
                    "cases.submitter_id": "TCGA-XX-0001",
                    "cases.project.project_id": "TCGA-LUSC",
                    "cases.samples.sample_type": "Primary Tumor",
                },
                {
                    "id": "F2",
                    "file_name": "slide2.svs",
                    "data_format": "SVS",
                    "data_type": "Slide Image",
                    "cases.case_id": "C2",
                    "cases.submitter_id": "TCGA-XX-0002",
                    "cases.project.project_id": "TCGA-LUSC",
                    "cases.samples.sample_type": "Solid Tissue Normal",
                },
            ]
        return []

    def cases_query(self, filters, fields, size=5000):
        return [
            {
                "case_id": "C1",
                "submitter_id": "TCGA-XX-0001",
                "diagnoses": [
                    {
                        "primary_diagnosis": "Lung Squamous Cell Carcinoma",
                        "tumor_stage": "Stage II",
                        "vital_status": "Alive",
                        "days_to_last_follow_up": 1200,
                    }
                ],
                "treatments": [
                    {"treatment_type": "Radiation", "measure_of_response": "PR"}
                ],
            },
            {
                "case_id": "C2",
                "submitter_id": "TCGA-XX-0002",
                "diagnoses": [
                    {
                        "primary_diagnosis": "Lung Squamous Cell Carcinoma",
                        "tumor_stage": None,
                        "vital_status": "Dead",
                        "days_to_death": 800,
                    }
                ],
            },
        ]

    def download_single(
        self, uuid: str, *, target_path: str, related_files: bool = False
    ) -> str:
        self.downloaded_ids.append(uuid)
        Path(target_path).write_bytes(b"")
        return target_path

    def download_tar(
        self, uuids, *, target_path: str, uncompressed: bool = False
    ) -> str:
        Path(target_path).write_bytes(b"")
        return target_path

    def download_manifest_for_query(self, filters, *, manifest_path: str) -> str:
        Path(manifest_path).write_text("file_id\tfile_name\n")
        return manifest_path


class FakeTCIAClient:
    def __init__(self, *args, **kwargs):
        self.last_queries = []
        self.downloaded_series = []

    def list_collections(self):
        self.last_queries.append({"endpoint": "getCollectionValues"})
        return [{"Collection": "TCGA-BRCA"}, {"Collection": "CPTAC-CCRCC"}]

    def list_modalities(self, *, collection=None):
        self.last_queries.append(
            {"endpoint": "getModalityValues", "params": {"Collection": collection}}
        )
        if collection == "TCGA-BRCA":
            return [{"Modality": "MR"}, {"Modality": "MG"}]
        return [{"Modality": "CT"}]

    def get_patient_studies(
        self,
        *,
        collection,
        modalities=None,
        patient_id=None,
        study_instance_uid=None,
    ):
        self.last_queries.append(
            {
                "endpoint": "getPatientStudy",
                "params": {
                    "Collection": collection,
                    "Modality": list(modalities or []),
                    "PatientID": patient_id,
                    "StudyInstanceUID": study_instance_uid,
                },
            }
        )
        return [
            {
                "StudyInstanceUID": "1.2.3",
                "StudyDate": "2001-11-21 00:00:00.0",
                "StudyDescription": "*MRI - BREAST",
                "PatientAge": "049Y",
                "PatientID": patient_id or "TCGA-AR-A1AQ",
                "PatientName": "1^TCGA-BRCA",
                "PatientSex": "F",
                "Collection": collection,
                "SeriesCount": 2,
            }
        ]

    def get_series(
        self,
        *,
        collection,
        modalities=None,
        patient_id=None,
        study_instance_uid=None,
        series_instance_uid=None,
        body_part=None,
        manufacturer=None,
    ):
        self.last_queries.append(
            {
                "endpoint": "getSeries",
                "params": {
                    "Collection": collection,
                    "Modality": list(modalities or []),
                    "PatientID": patient_id,
                    "StudyInstanceUID": study_instance_uid,
                    "SeriesInstanceUID": series_instance_uid,
                    "BodyPartExamined": body_part,
                    "Manufacturer": manufacturer,
                },
            }
        )
        return [
            {
                "SeriesInstanceUID": series_instance_uid or "9.8.7",
                "StudyInstanceUID": study_instance_uid or "1.2.3",
                "Modality": (list(modalities or ["MR"])[0]),
                "SeriesDate": "2001-11-21 00:00:00.0",
                "SeriesDescription": "LT SAG FSE T2",
                "BodyPartExamined": body_part or "BREAST",
                "SeriesNumber": 4,
                "Collection": collection,
                "PatientID": patient_id or "TCGA-AR-A1AQ",
                "Manufacturer": manufacturer or "GE MEDICAL SYSTEMS",
                "ManufacturerModelName": "SIGNA EXCITE",
                "SoftwareVersions": "11",
                "ImageCount": 36,
                "TimeStamp": "2011-12-01 07:41:03.0",
                "LicenseName": "CC BY 3.0",
                "LicenseURI": "http://creativecommons.org/licenses/by/3.0/",
                "CollectionURI": "https://doi.org/example",
                "FileSize": 4971672,
                "DateReleased": "2011-12-01 07:41:03.0",
                "StudyDate": "2001-11-21 00:00:00.0",
            }
        ]

    def download_series(
        self, series_instance_uid: str, *, target_path: str, include_md5: bool = False
    ) -> str:
        self.downloaded_series.append(series_instance_uid)
        Path(target_path).write_bytes(b"ZIP")
        return target_path


class FakePathoBenchClient:
    def __init__(self, *args, **kwargs):
        self.last_queries = []

    def fetch_available_splits(self):
        self.last_queries.append({"endpoint": "available_splits"})
        return {
            "cptac_coad": ["KRAS_mutation", "TP53_mutation"],
            "cptac_brca": ["PIK3CA_mutation"],
            "bracs": ["slidelevel_fine"],
        }

    def list_repo_tree(self, *, path_in_repo=None, recursive=False):
        self.last_queries.append(
            {
                "endpoint": "tree",
                "path_in_repo": path_in_repo,
                "recursive": recursive,
            }
        )
        repo_files = {
            "cptac_coad": [
                {"type": "file", "path": "cptac_coad/KRAS_mutation/config.yaml"},
                {"type": "file", "path": "cptac_coad/KRAS_mutation/k=all.tsv"},
                {"type": "file", "path": "cptac_coad/TP53_mutation/config.yaml"},
                {"type": "file", "path": "cptac_coad/TP53_mutation/k=all.tsv"},
            ],
            "bracs": [
                {"type": "file", "path": "bracs/slidelevel_fine/config.yaml"},
                {"type": "file", "path": "bracs/slidelevel_fine/k=all.tsv"},
            ],
        }
        return repo_files[path_in_repo]

    def download_file(self, repo_path: str, *, target_path: Path):
        self.last_queries.append({"endpoint": "resolve", "repo_path": repo_path})
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if repo_path.endswith(".tsv"):
            target_path.write_text("sample_id\tslide_id\tlabel\nA\tS1\t1\n")
        elif repo_path.endswith(".yaml"):
            target_path.write_text("key: value\n")
        else:
            target_path.write_text("")
        return target_path


def test_download_with_all_annotations(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(_dl, "GDCClient", FakeClient)

    out = tcga_tools.download(
        dataset_name="TCGA-LUSC",
        filetypes=[".svs"],
        annotations=["all"],
        output_dir=tmp_path,
    )

    assert out["files_csv"].exists()
    assert out["groups_csv"].exists()
    assert out["clinical_csv"].exists()
    assert out["diagnosis_csv"].exists()
    assert out["data_dir"].exists()

    summary = tcga_tools.validate_output_dir(
        tmp_path / "TCGA-LUSC", expected_dataset="TCGA-LUSC"
    )
    assert summary["file_rows"] == 2
    assert summary["patient_count"] == 2


def test_rna_seq_raw_download_does_not_inject_none_filetype(
    monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(_dl, "GDCClient", FakeClient)

    tcga_tools.download(
        dataset_name="TCGA-LUSC",
        filetypes=None,
        datatype=["rna-seq"],
        annotations=None,
        output_dir=tmp_path,
        raw=True,
    )

    files_csv = tmp_path / "TCGA-LUSC" / "files_metadata.csv"
    files_df = pd.read_csv(files_csv)
    assert len(files_df) == 1
    assert files_df["data_type"].iloc[0] == "Gene Expression Quantification"

    run_log = json.loads((tmp_path / "run_log.json").read_text())
    assert run_log["filetypes"] == []
    assert run_log["datatype"] == ["rna-seq"]
    assert not _contains_value(run_log["queries"], "*none")


def test_cli_validate_output(tmp_path: Path):
    dataset_dir = tmp_path / "TCGA-LUSC"
    dataset_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "patient": "TCGA-XX-0001",
                "cases.project.project_id": "TCGA-LUSC",
                "file_name": "slide1.svs",
            }
        ]
    ).to_csv(dataset_dir / "files_metadata.csv", index=False)
    (tmp_path / "run_log.json").write_text(json.dumps({"datasets": ["TCGA-LUSC"]}))

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tcga_tools",
            "validate-output",
            str(tmp_path),
            "--dataset",
            "TCGA-LUSC",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    summary = json.loads(completed.stdout)
    assert summary["file_rows"] == 1
    assert summary["patient_count"] == 1


def test_tcia_dataset_and_datatype_listing(monkeypatch):
    monkeypatch.setattr(_tcia, "TCIAClient", FakeTCIAClient)

    datasets = tcga_tools.list_datasets(source="tcia", include_data_types=True)
    assert set(datasets["collection"]) == {"TCGA-BRCA", "CPTAC-CCRCC"}
    assert datasets.loc[datasets["collection"] == "TCGA-BRCA", "data_types"].iloc[
        0
    ] == [
        "MR",
        "MG",
    ]

    data_types = tcga_tools.list_data_types(source="tcia", dataset_name="TCGA-BRCA")
    assert set(data_types["data_type"]) == {"MR", "MG"}


def test_tcia_raw_download_with_patient_metadata(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(_tcia, "TCIAClient", FakeTCIAClient)

    out = tcga_tools.download(
        dataset_name="TCGA-BRCA",
        source="tcia",
        datatype=["MR"],
        annotations=["clinical"],
        output_dir=tmp_path,
        raw=True,
        patient_id="TCGA-AR-A1AQ",
    )

    assert out["files_csv"].exists()
    assert out["clinical_csv"].exists()
    assert out["patient_studies_csv"].exists()

    files_df = pd.read_csv(out["files_csv"])
    assert files_df["Modality"].iloc[0] == "MR"
    assert files_df["patient"].iloc[0] == "TCGA-AR-A1AQ"

    clinical_df = pd.read_csv(out["clinical_csv"])
    assert clinical_df["PatientID"].iloc[0] == "TCGA-AR-A1AQ"

    summary = tcga_tools.validate_output_dir(
        tmp_path / "TCGA-BRCA", expected_dataset="TCGA-BRCA"
    )
    assert summary["file_rows"] == 1


def test_gdc_parallel_download_records_worker_count(monkeypatch, tmp_path: Path):
    fake_client = FakeClient()
    monkeypatch.setattr(_dl, "GDCClient", lambda *args, **kwargs: fake_client)

    out = tcga_tools.download(
        dataset_name="TCGA-LUSC",
        filetypes=[".svs"],
        output_dir=tmp_path,
        download_workers=2,
    )

    assert out["data_dir"].exists()
    assert sorted(fake_client.downloaded_ids) == ["F1", "F2"]

    run_log = json.loads((tmp_path / "run_log.json").read_text())
    assert run_log["download_workers"] == 2


def test_tcia_parallel_download_records_worker_count(monkeypatch, tmp_path: Path):
    fake_client = FakeTCIAClient()
    monkeypatch.setattr(_tcia, "TCIAClient", lambda *args, **kwargs: fake_client)

    out = tcga_tools.download(
        dataset_name="TCGA-BRCA",
        source="tcia",
        datatype=["MR"],
        output_dir=tmp_path,
        raw=False,
        download_workers=3,
    )

    assert out["data_dir"].exists()
    assert fake_client.downloaded_series == ["9.8.7"]

    run_log = json.loads((tmp_path / "run_log.json").read_text())
    assert run_log["download_workers"] == 3


def test_patho_bench_listing_and_tasks(monkeypatch):
    monkeypatch.setattr(_patho_bench, "PathoBenchClient", FakePathoBenchClient)

    datasets = tcga_tools.list_datasets(source="patho-bench", include_data_types=True)
    assert "cptac_coad" in set(datasets["dataset_name"])
    tasks = datasets.loc[datasets["dataset_name"] == "cptac_coad", "tasks"].iloc[0]
    assert tasks == ["KRAS_mutation", "TP53_mutation"]

    task_rows = tcga_tools.list_data_types(
        source="patho-bench", dataset_name="cptac_coad"
    )
    assert set(task_rows["task_name"]) == {"KRAS_mutation", "TP53_mutation"}


def test_patho_bench_metadata_download(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(_patho_bench, "PathoBenchClient", FakePathoBenchClient)

    out = tcga_tools.download(
        dataset_name="cptac_coad",
        source="patho-bench",
        task_name="KRAS_mutation",
        output_dir=tmp_path,
    )

    assert out["files_csv"].exists()
    assert out["raw_data_manifest_json"].exists()

    files_df = pd.read_csv(out["files_csv"])
    assert set(files_df["repo_path"]) == {
        "available_splits.yaml",
        "cptac_coad/KRAS_mutation/config.yaml",
        "cptac_coad/KRAS_mutation/k=all.tsv",
    }

    manifest = json.loads(out["raw_data_manifest_json"].read_text())
    assert manifest["display_name"] == "CPTAC-COAD"
    assert manifest["raw_data_source"] == "tcia"

    summary = tcga_tools.validate_output_dir(tmp_path / "cptac_coad")
    assert summary["file_rows"] == 3
    assert summary["patient_count"] is None


def test_patho_bench_tcia_delegation(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(_patho_bench, "PathoBenchClient", FakePathoBenchClient)
    delegated = {}

    def _fake_download_tcia(**kwargs):
        delegated.update(kwargs)
        raw_dir = Path(kwargs["output_dir"])
        raw_dir.mkdir(parents=True, exist_ok=True)
        index = raw_dir / "multi_index.json"
        index.write_text('{"delegated": true}')
        return {"multi_index_json": index}

    monkeypatch.setattr(_patho_bench, "download_tcia", _fake_download_tcia)

    out = tcga_tools.download(
        dataset_name="cptac_coad",
        source="patho-bench",
        output_dir=tmp_path,
        download_raw_data=True,
    )

    assert delegated["dataset_name"] == "CPTAC-COAD"
    assert out["delegated_raw_index_json"].exists()
