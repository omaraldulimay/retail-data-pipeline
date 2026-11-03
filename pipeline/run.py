"""Loads all sources into the warehouse.

`python -m pipeline.run` talks to the source APIs at SOURCE_API_URL. With `--in-process`
the mock APIs run inside this process, so no server needs to be started.
"""

import argparse
import logging
import os
from pathlib import Path

import httpx

from pipeline import extract
from pipeline.warehouse import connect

SOURCES_DIR = Path(
    os.getenv("SOURCES_DIR", Path(__file__).resolve().parent.parent / "data/sources")
)
SOURCE_API_URL = os.getenv("SOURCE_API_URL", "http://127.0.0.1:8100")


def source_client(in_process: bool) -> httpx.Client:
    if in_process:
        from fastapi.testclient import TestClient

        from sources.app import app

        return TestClient(app)
    return httpx.Client(base_url=SOURCE_API_URL, timeout=30)


def load_all(in_process: bool = False) -> list[extract.LoadResult]:
    with source_client(in_process) as client:
        con = connect()
        try:
            return [
                extract.load_products(con, client),
                extract.load_orders(con, client),
                extract.load_stock_snapshots(con, client),
                extract.load_logistics_files(con, SOURCES_DIR / "logistics"),
            ]
        finally:
            con.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in-process", action="store_true")
    args = parser.parse_args()
    for result in load_all(args.in_process):
        print(
            f"{result.source:22} received {result.received:6}  new {result.new:6}  {result.detail}"
        )


if __name__ == "__main__":
    main()
