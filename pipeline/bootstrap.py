"""Builds a complete demo warehouse from scratch: generate, load, model.

Used by the dashboard on hosts without a pipeline run (Streamlit Community Cloud), where
`data/` does not exist. The warehouse is built under a temporary name and moved into place at
the end, so a half-built file is never read.
"""

import fcntl
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from pipeline.run import SOURCES_DIR
from pipeline.warehouse import WAREHOUSE_PATH, connect

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "dbt_project"
SEED = 42
logger = logging.getLogger(__name__)


def bootstrap_enabled() -> bool:
    """True with DEMO_BOOTSTRAP=1 or on Streamlit Community Cloud (repo is mounted there)."""
    return os.getenv("DEMO_BOOTSTRAP") == "1" or str(ROOT).startswith("/mount/src/")


def build_warehouse(warehouse: Path = WAREHOUSE_PATH, sources_dir: Path = SOURCES_DIR) -> None:
    """Generates the sources, loads them in process and runs `dbt build`."""
    # Imported here: the dashboard imports this module on every run, but rarely builds.
    from fastapi.testclient import TestClient

    import sources.app
    from generator.generate import write_all
    from pipeline import extract

    warehouse.parent.mkdir(parents=True, exist_ok=True)
    if not (sources_dir / "shop" / "orders.jsonl").exists():
        write_all(sources_dir, SEED)

    building = warehouse.with_name(warehouse.name + ".building")
    building.unlink(missing_ok=True)
    previous = (sources.app.SOURCES_DIR, sources.app.failure_rate)
    sources.app.SOURCES_DIR, sources.app.failure_rate = sources_dir, 0.0
    sources.app.load_orders.cache_clear()
    sources.app.load_snapshots.cache_clear()
    con = connect(building)
    try:
        with TestClient(sources.app.app) as client:
            extract.load_products(con, client)
            extract.load_orders(con, client, page_size=1000)
            extract.load_stock_snapshots(con, client)
        extract.load_logistics_files(con, sources_dir / "logistics")
    finally:
        con.close()
        sources.app.SOURCES_DIR, sources.app.failure_rate = previous
        sources.app.load_orders.cache_clear()
        sources.app.load_snapshots.cache_clear()

    with tempfile.TemporaryDirectory() as scratch:
        result = subprocess.run(
            [
                str(Path(sys.executable).with_name("dbt")),
                "build",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
            ],
            env={
                **os.environ,
                "WAREHOUSE_PATH": str(building),
                "DBT_TARGET_PATH": str(Path(scratch) / "target"),
                "DBT_LOG_PATH": str(Path(scratch) / "logs"),
            },
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
    if result.returncode != 0:
        building.unlink(missing_ok=True)
        raise RuntimeError("dbt build failed:\n" + result.stdout[-3000:] + result.stderr[-1000:])
    building.replace(warehouse)


def ensure_warehouse(warehouse: Path = WAREHOUSE_PATH, sources_dir: Path = SOURCES_DIR) -> bool:
    """Builds the warehouse unless it exists; returns True if it did build.

    A file lock makes concurrent callers (several Streamlit sessions or processes) wait for the
    first one instead of building twice.
    """
    if warehouse.exists():
        return False
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    with warehouse.with_name(warehouse.name + ".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if warehouse.exists():
            return False
        logger.info("Building demo warehouse at %s", warehouse)
        build_warehouse(warehouse, sources_dir)
        return True
