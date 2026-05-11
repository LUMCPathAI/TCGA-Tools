from typing import Optional

import tcga_tools.pathology as pathology
from tcga_tools.ports import DownloadResult, SopInstanceResult
from tcga_tools.services.tcia_pathology import TciaPathologyService, TciaSeriesQuery


class FakeTciaClient:
    def sop_instance_uids(
        self,
        series_instance_uid: str,
        *,
        format_: str = "JSON",
        path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> SopInstanceResult:
        payload = {"series": series_instance_uid, "format": format_}
        return SopInstanceResult(series_instance_uid=series_instance_uid, format=format_, payload=payload)

    def download_single_image(
        self,
        series_instance_uid: str,
        sop_instance_uid: str,
        *,
        path: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        filename = name or f"{sop_instance_uid}.dcm"
        return DownloadResult(path=f"{path}/{filename}", filename=filename)

    def download_series_images(
        self,
        series_instance_uid: str,
        *,
        path: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        filename = name or f"{series_instance_uid}.zip"
        return DownloadResult(path=f"{path}/{filename}", filename=filename)


def test_tcia_service_roundtrip():
    service = TciaPathologyService(client=FakeTciaClient())
    query = TciaSeriesQuery(series_instance_uid="series-uid")
    result = service.get_sop_instance_uids(query)
    assert result.payload["series"] == "series-uid"

    single = service.download_single_image(
        series_instance_uid="series-uid",
        sop_instance_uid="sop-uid",
        output_dir="/tmp",
    )
    assert single.filename.endswith(".dcm")

    series = service.download_series_images(series_instance_uid="series-uid", output_dir="/tmp")
    assert series.filename.endswith(".zip")


def test_pathology_portal_tcia(monkeypatch):
    monkeypatch.setattr(pathology, "TciaApiWrapperClient", FakeTciaClient)
    portal = pathology.PathologyDataPortal()

    payload = portal.list_tcia_sop_instance_uids(
        TciaSeriesQuery(series_instance_uid="series-uid", format_="JSON")
    )
    assert payload["series"] == "series-uid"

    single_path = portal.download_tcia_single_image(
        series_instance_uid="series-uid",
        sop_instance_uid="sop-uid",
        output_dir="/tmp",
        name="example.dcm",
    )
    assert single_path.endswith("example.dcm")

    series_path = portal.download_tcia_series(
        series_instance_uid="series-uid",
        output_dir="/tmp",
        name="example.zip",
    )
    assert series_path.endswith("example.zip")
