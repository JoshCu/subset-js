# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb",
# ]
# ///
"""
Create optimized parquet files from the hydrofabric GeoPackage and upload to S3.

Each table is sorted by its primary filter column and written with the
row group size determined by benchmarking.

Usage:
    uv run create_parquet.py
    uv run create_parquet.py --gpkg /path/to/conus_nextgen.gpkg
    uv run create_parquet.py --skip-upload
    uv run create_parquet.py --tables divides flowpaths
"""

import argparse
import subprocess
import time
from pathlib import Path

import duckdb

S3_BUCKET = "s3://communityhydrofabric/parquet"

# table_name -> (sort_col, row_group_size or None for default)
TABLES = {
    "divides": ("divide_id", 5_000),
    "divide-attributes": ("divide_id", 10_000),
    "flowpaths": ("id", 5_000),
    "flowpath-attributes": ("id", 10_000),
    "flowpath-attributes-ml": ("id", 10_000),
    "hydrolocations": ("id", 10_000),
    "pois": ("id", 5_000),
    "lakes": ("poi_id", None),
    "network": ("id", 20_000),
}


def create_and_upload(gpkg_path: str, output_dir: Path, tables: list[str], upload: bool):
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    for table_name in tables:
        sort_col, rg_size = TABLES[table_name]
        out_file = output_dir / f"{table_name}.parquet"

        rg_label = str(rg_size) if rg_size else "default"
        print(
            f"{table_name}.parquet  (sort: {sort_col}, row_group: {rg_label}) ... ",
            end="",
            flush=True,
        )
        t0 = time.perf_counter()

        rg_clause = f", ROW_GROUP_SIZE {rg_size}" if rg_size else ""
        con.execute(f"""
            COPY (
                SELECT * FROM st_read('{gpkg_path}', layer='{table_name}')
                ORDER BY "{sort_col}"
            )
            TO '{out_file}' (FORMAT PARQUET{rg_clause})
        """)

        elapsed = time.perf_counter() - t0
        size_mb = out_file.stat().st_size / (1024 * 1024)
        print(f"{size_mb:.1f} MB, {elapsed:.1f}s")

        if upload:
            dest = f"{S3_BUCKET}/{table_name}.parquet"
            print(f"  uploading -> {dest} ... ", end="", flush=True)
            subprocess.run(["aws", "s3", "cp", str(out_file), dest], check=True)
            print("done")

    con.close()
    print("\nAll done.")


def main():
    all_tables = list(TABLES.keys())

    parser = argparse.ArgumentParser(description="Create optimized hydrofabric parquet files")
    parser.add_argument(
        "--gpkg", type=str, default=str(Path.home() / ".ngiab/hydrofabric/v2.2/conus_nextgen.gpkg")
    )
    parser.add_argument("--output-dir", type=str, default="./parquet_output")
    parser.add_argument("--tables", nargs="+", default=all_tables, choices=all_tables)
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    create_and_upload(args.gpkg, Path(args.output_dir), args.tables, upload=not args.skip_upload)


if __name__ == "__main__":
    main()
