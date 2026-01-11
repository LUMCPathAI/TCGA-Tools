import pandas as pd

from tcga_tools.services.tcga_pathology import TcgaPathologyService
from tcga_tools.ports import DownloadResult, FileDownloadPort, GdcMetadataPort


class FakeMetadataClient(GdcMetadataPort):
    def __init__(self):
        self.last = None

    def paged_query(self, endpoint: str, filters: dict, fields):
        self.last = {"endpoint": endpoint, "filters": filters, "fields": list(fields) if fields else None}
        return [
            {
                "id": "F1",
                "file_name": "slide1.svs",
                "cases.case_id": "C1",
                "cases.project.project_id": "TCGA-LUSC",
            }
        ]

    def download_manifest(self, filters: dict, *, manifest_path: str) -> str:
        raise NotImplementedError


class FakeDownloader(FileDownloadPort):
    def __init__(self):
        self.last = None

    def download_single(self, uuid: str, *, path: str, name=None) -> DownloadResult:
        raise NotImplementedError

    def download_multiple(self, uuids, *, path: str) -> DownloadResult:
        self.last = {"uuids": list(uuids), "path": path}
        return DownloadResult(path=f"{path}/bundle.tar.gz", filename="bundle.tar.gz")


def test_tcga_pathology_service_list_and_download():
    metadata = FakeMetadataClient()
    downloader = FakeDownloader()
    service = TcgaPathologyService(metadata_client=metadata, downloader=downloader)

    df = service.list_files(project_id="TCGA-LUSC", filetypes=[".svs"], fields=["id", "file_name"])
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "file_name"] == "slide1.svs"

    service.download_files(uuids=df["id"].tolist(), output_dir="/tmp")
    assert downloader.last == {"uuids": ["F1"], "path": "/tmp"}
