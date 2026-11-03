"""Keeps the DuckDB warehouse in Azure Blob Storage: `python -m pipeline.cloud run|download`.

`run` downloads the warehouse (if it exists), runs the daily Dagster job, checks the result and
uploads the file again. Authentication uses DefaultAzureCredential (managed identity in Azure).
"""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from pipeline import check
from pipeline.warehouse import WAREHOUSE_PATH

BLOB_NAME = "warehouse.duckdb"


def _container():
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(
        account_url=os.environ["AZURE_STORAGE_ACCOUNT_URL"], credential=DefaultAzureCredential()
    )
    return service.get_container_client(os.getenv("WAREHOUSE_CONTAINER", "warehouse"))


def download_warehouse(path: Path | str = WAREHOUSE_PATH) -> bool:
    """Returns False when there is no warehouse in storage yet (first run)."""
    from azure.core.exceptions import ResourceNotFoundError

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Temp file next to the target, so os.replace stays on one file system.
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".part", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        try:
            tmp.write(_container().download_blob(BLOB_NAME).readall())
        except ResourceNotFoundError:
            tmp_path.unlink()
            return False
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    os.replace(tmp_path, path)
    return True


def upload_warehouse(path: Path | str = WAREHOUSE_PATH) -> None:
    with open(path, "rb") as f:
        _container().upload_blob(BLOB_NAME, f, overwrite=True)


def run() -> None:
    print("Downloaded warehouse" if download_warehouse() else "No warehouse in storage yet")
    dagster = str(Path(sys.executable).with_name("dagster"))
    subprocess.run(
        [dagster, "job", "execute", "-m", "orchestration.definitions", "-j", "daily_pipeline"],
        check=True,
    )
    check.main()
    upload_warehouse()
    print("Uploaded warehouse")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run", "download"])
    args = parser.parse_args()
    if args.command == "run":
        run()
    else:
        print("Downloaded" if download_warehouse() else "Nothing to download")


if __name__ == "__main__":
    main()
