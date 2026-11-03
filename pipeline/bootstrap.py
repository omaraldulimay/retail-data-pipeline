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
import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import httpx

from pipeline.run import SOURCES_DIR
from pipeline.warehouse import WAREHOUSE_PATH, connect

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "dbt_project"
SEED = 42
DEFAULT_WAREHOUSE_URL = (
    "https://github.com/omaraldulimay/retail-data-pipeline/releases/download/demo-data/"
    "warehouse.duckdb"
)
DOWNLOAD_TIMEOUT_S = 60
DBT_TIMEOUT_S = 600
LOCK_TIMEOUT_S = 900  # a bit more than a full build (generate + extract + dbt)
DBT_TAIL_LINES = 40
logger = logging.getLogger(__name__)


def bootstrap_enabled() -> bool:
    """True with DEMO_BOOTSTRAP=1 or on Streamlit Community Cloud (repo is mounted there)."""
    return os.getenv("DEMO_BOOTSTRAP") == "1" or str(ROOT).startswith("/mount/src/")


def _log(message: str) -> None:
    """Prints to stdout (flushed) so the steps show up in the Streamlit Cloud logs."""
    print(f"[bootstrap] {message}", flush=True)


def _tail(text: str, lines: int = DBT_TAIL_LINES) -> str:
    return "\n".join(text.strip().splitlines()[-lines:])


def build_warehouse(
    warehouse: Path = WAREHOUSE_PATH,
    sources_dir: Path = SOURCES_DIR,
    progress: Callable[[str], None] | None = None,
) -> None:
    """Generates the sources, loads them in process and runs `dbt build`.

    `progress` is called with a short German message after each finished step.
    """
    # Imported here: the dashboard imports this module on every run, but rarely builds.
    from fastapi.testclient import TestClient

    import sources.app
    from generator.generate import write_all
    from pipeline import extract

    def step_done(name: str, started: float, message: str) -> None:
        _log(f"{name} done in {time.monotonic() - started:.1f}s")
        if progress:
            progress(message)

    warehouse.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    _log("generate: start")
    if not (sources_dir / "shop" / "orders.jsonl").exists():
        write_all(sources_dir, SEED)
    step_done("generate", started, "Quelldaten erzeugt.")

    building = warehouse.with_name(warehouse.name + ".building")
    building.unlink(missing_ok=True)
    previous = (sources.app.SOURCES_DIR, sources.app.failure_rate)
    sources.app.SOURCES_DIR, sources.app.failure_rate = sources_dir, 0.0
    sources.app.load_orders.cache_clear()
    sources.app.load_snapshots.cache_clear()
    started = time.monotonic()
    _log("extract: start")
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
    step_done("extract", started, "Daten geladen.")

    started = time.monotonic()
    _log("dbt build: start")
    with tempfile.TemporaryDirectory() as scratch:
        try:
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
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=DBT_TIMEOUT_S,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            building.unlink(missing_ok=True)
            partial = (
                exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else exc.stdout
            )
            raise RuntimeError(
                f"dbt build timed out after {DBT_TIMEOUT_S}s. Last output:\n{_tail(partial or '')}"
            ) from exc
    if result.returncode != 0:
        building.unlink(missing_ok=True)
        raise RuntimeError(
            f"dbt build failed (exit code {result.returncode}). Last {DBT_TAIL_LINES} lines:\n"
            + _tail(result.stdout or "")
        )
    building.replace(warehouse)
    step_done("dbt build", started, "Modelle mit dbt gebaut.")


def _acquire_lock(lock, timeout: float) -> None:
    """Polls a non-blocking flock; raises instead of waiting forever on a hung builder."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Another build held the lock for more than {timeout:.0f}s; giving up."
                ) from None
            time.sleep(1)


def download_warehouse(warehouse: Path, url: str) -> None:
    """Downloads a prebuilt warehouse (built by CI) and moves it into place after validating it.

    Raises on any problem; the caller falls back to the local build.
    """
    part = warehouse.with_name(warehouse.name + ".part")
    part.unlink(missing_ok=True)
    try:
        with httpx.stream(
            "GET", url, follow_redirects=True, timeout=DOWNLOAD_TIMEOUT_S
        ) as response:
            response.raise_for_status()
            with part.open("wb") as out:
                for chunk in response.iter_bytes():
                    out.write(chunk)
        con = duckdb.connect(str(part), read_only=True)
        try:
            rows = con.execute("select count(*) from marts.fct_orders").fetchone()[0]
        finally:
            con.close()
        if rows == 0:
            raise ValueError("marts.fct_orders is empty")
        os.replace(part, warehouse)
    finally:
        part.unlink(missing_ok=True)


def ensure_warehouse(
    warehouse: Path = WAREHOUSE_PATH,
    sources_dir: Path = SOURCES_DIR,
    progress: Callable[[str], None] | None = None,
    lock_timeout: float = LOCK_TIMEOUT_S,
) -> bool:
    """Builds the warehouse unless it exists; returns True if it did build.

    A file lock makes concurrent callers (several Streamlit sessions or processes) wait for the
    first one instead of building twice, but only up to `lock_timeout` seconds.
    """
    if warehouse.exists():
        return False
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    with warehouse.with_name(warehouse.name + ".lock").open("w") as lock:
        _acquire_lock(lock, lock_timeout)
        if warehouse.exists():
            return False
        # The lock is ours, so a .building file is left over from a dead build.
        warehouse.with_name(warehouse.name + ".building").unlink(missing_ok=True)
        started = time.monotonic()
        url = os.getenv("DEMO_WAREHOUSE_URL", DEFAULT_WAREHOUSE_URL)
        if url:  # an empty value switches the download off
            _log(f"download: start from {url}")
            if progress:
                progress("Lade fertige Demo-Daten …")
            try:
                download_warehouse(warehouse, url)
            except Exception as exc:
                _log(f"download failed ({type(exc).__name__}: {exc}); building locally")
                if progress:
                    progress("Download nicht möglich, die Daten werden lokal aufgebaut …")
            else:
                _log(f"end: downloaded warehouse in {time.monotonic() - started:.1f}s")
                return True
        logger.info("Building demo warehouse at %s", warehouse)
        _log(f"start: building warehouse at {warehouse}")
        started = time.monotonic()
        build_warehouse(warehouse, sources_dir, progress)
        _log(f"end: warehouse ready after {time.monotonic() - started:.1f}s")
        return True
