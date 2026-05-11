from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Sequence

from .datasets import list_data_types, list_datasets
from .downloader import download
from .validation import validate_output_dir

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TCGA-Tools: download, inspect, and validate GDC/TCGA datasets."
    )
    subparsers = parser.add_subparsers(dest="command")

    download_parser = subparsers.add_parser(
        "download", help="Download one or more TCGA datasets."
    )
    download_parser.add_argument(
        "--source",
        default="gdc",
        choices=["gdc", "tcia", "patho-bench"],
        help="Data source backend: gdc, tcia, or patho-bench",
    )
    download_parser.add_argument(
        "--dataset",
        nargs="+",
        required=True,
        help="Dataset or collection ID(s), for example TCGA-LUSC or TCGA-BRCA",
    )
    download_parser.add_argument(
        "--filetypes",
        nargs="+",
        default=None,
        help="Optional extensions, for example .svs .maf",
    )
    download_parser.add_argument(
        "--datatype",
        nargs="+",
        default=None,
        help="Optional data-type filters, for example WSI rna-seq or MR CT",
    )
    download_parser.add_argument(
        "--annotations",
        nargs="*",
        default=None,
        help="Optional annotations: clinical molecular report diagnosis all",
    )
    download_parser.add_argument(
        "--task",
        default=None,
        help="Patho-Bench only: task name within one benchmark dataset, or '*' for all tasks",
    )
    download_parser.add_argument("--out", default=".", help="Output directory")
    download_parser.add_argument(
        "--tar", action="store_true", help="Download as one tar.gz archive"
    )
    download_parser.add_argument(
        "--raw", action="store_true", help="Preview only; do not download data files"
    )
    download_parser.add_argument(
        "--statistics", action="store_true", help="Compute and save dataset statistics"
    )
    download_parser.add_argument(
        "--visualizations", action="store_true", help="Also save plots"
    )
    download_parser.add_argument(
        "--download-workers",
        type=int,
        default=4,
        help="Bounded parallel worker count for file or series downloads",
    )
    download_parser.add_argument(
        "--patient-id",
        default=None,
        help="TCIA only: restrict download to one patient identifier",
    )
    download_parser.add_argument(
        "--study-instance-uid",
        default=None,
        help="TCIA only: restrict download to one DICOM study UID",
    )
    download_parser.add_argument(
        "--series-instance-uid",
        default=None,
        help="TCIA only: restrict download to one DICOM series UID",
    )
    download_parser.add_argument(
        "--body-part",
        default=None,
        help="TCIA only: restrict download to one BodyPartExamined value",
    )
    download_parser.add_argument(
        "--manufacturer",
        default=None,
        help="TCIA only: restrict download to one scanner manufacturer",
    )
    download_parser.add_argument(
        "--download-raw-data",
        action="store_true",
        help="Patho-Bench only: additionally attempt raw-data download when a delegated backend is supported",
    )

    datasets_parser = subparsers.add_parser(
        "list-datasets", help="List datasets available for a GDC program."
    )
    datasets_parser.add_argument(
        "--source",
        default="gdc",
        choices=["gdc", "tcia", "patho-bench"],
        help="Data source backend: gdc, tcia, or patho-bench",
    )
    datasets_parser.add_argument(
        "--program", default="TCGA", help="Program name, default: TCGA"
    )
    datasets_parser.add_argument(
        "--include-data-types",
        action="store_true",
        help="Include per-dataset data types when supported by the backend",
    )
    datasets_parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of a table"
    )

    datatypes_parser = subparsers.add_parser(
        "list-data-types",
        help="List available data-type values for one backend or dataset.",
    )
    datatypes_parser.add_argument(
        "--source",
        default="gdc",
        choices=["gdc", "tcia", "patho-bench"],
        help="Data source backend: gdc, tcia, or patho-bench",
    )
    datatypes_parser.add_argument(
        "--dataset",
        default=None,
        help="Optional dataset or collection identifier",
    )
    datatypes_parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of a table"
    )

    validate_parser = subparsers.add_parser(
        "validate-output",
        help="Validate a previously generated output directory offline.",
    )
    validate_parser.add_argument(
        "output_dir", help="Root output directory or per-dataset subdirectory"
    )
    validate_parser.add_argument(
        "--dataset",
        dest="expected_dataset",
        default=None,
        help="Expected TCGA project identifier",
    )
    validate_parser.add_argument(
        "--allow-empty-files",
        action="store_true",
        help="Allow empty files_metadata.csv",
    )

    return parser


def _run_download(args: argparse.Namespace) -> int:
    artifacts = download(
        dataset_name=args.dataset,
        filetypes=args.filetypes,
        datatype=args.datatype,
        annotations=args.annotations,
        output_dir=args.out,
        tar_archives=args.tar,
        raw=args.raw,
        statistics=args.statistics,
        visualizations=args.visualizations,
        source=args.source,
        patient_id=args.patient_id,
        study_instance_uid=args.study_instance_uid,
        series_instance_uid=args.series_instance_uid,
        body_part=args.body_part,
        manufacturer=args.manufacturer,
        download_workers=args.download_workers,
        task_name=args.task,
        download_raw_data=args.download_raw_data,
    )
    print(
        json.dumps(
            {key: str(value) for key, value in artifacts.items()},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _run_list_datasets(args: argparse.Namespace) -> int:
    datasets_df = list_datasets(
        program=args.program,
        source=args.source,
        include_data_types=args.include_data_types,
    )
    if args.json:
        print(datasets_df.to_json(orient="records", indent=2))
    else:
        print(datasets_df.to_string(index=False))
    return 0


def _run_list_data_types(args: argparse.Namespace) -> int:
    data_types_df = list_data_types(dataset_name=args.dataset, source=args.source)
    if args.json:
        print(data_types_df.to_json(orient="records", indent=2))
    else:
        print(data_types_df.to_string(index=False))
    return 0


def _run_validate_output(args: argparse.Namespace) -> int:
    summary = validate_output_dir(
        args.output_dir,
        expected_dataset=args.expected_dataset,
        allow_empty_files=args.allow_empty_files,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    normalized_argv = list(argv) if argv is not None else sys.argv[1:]

    # Backward compatibility with the previous CLI:
    # `python -m tcga_tools --dataset TCGA-LUSC ...`
    if normalized_argv and normalized_argv[0].startswith("-"):
        normalized_argv = ["download", *normalized_argv]

    args = parser.parse_args(normalized_argv)
    if args.command in (None, "download"):
        return _run_download(args)
    if args.command == "list-datasets":
        return _run_list_datasets(args)
    if args.command == "list-data-types":
        return _run_list_data_types(args)
    if args.command == "validate-output":
        return _run_validate_output(args)
    parser.error(f"Unknown command: {args.command}")
    return 2
