from __future__ import annotations
import argparse
import logging

from .downloader import download

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def main():
    p = argparse.ArgumentParser(description="TCGA-Tools: quick downloads + annotations from GDC")
    p.add_argument("--dataset", nargs="+", required=True, help="Project ID(s), e.g., TCGA-LUSC TCGA-LUAD")
    p.add_argument("--filetypes", nargs="+", default=[".svs"], help="Extensions, e.g., .svs .bam")
    p.add_argument("--datatype", nargs="+", default=[], help="Data type, e.g., WSI")
    p.add_argument("--annotations", nargs="*", default=[], help="clinical molecular report diagnosis all")
    p.add_argument("--out", default=".", help="Output directory")
    p.add_argument("--tar", action="store_true", help="Download as one tar.gz archive")
    p.add_argument("--raw", action="store_true", help="Preview only; do not download data files")
    p.add_argument("--statistics", action="store_true", help="Compute and save dataset statistics")
    p.add_argument("--visualizations", action="store_true", help="Also save plots (requires matplotlib; lifelines optional)")
    args = p.parse_args()


    download(
    dataset_name=args.dataset,
    filetypes=args.filetypes,
    datatype=args.datatype,
    annotations=args.annotations,
    output_dir=args.out,
    tar_archives=args.tar,
    raw=args.raw,
    statistics=args.statistics,
    visualizations=args.visualizations,
    )


if __name__ == "__main__":
    main()
