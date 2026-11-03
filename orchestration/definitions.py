"""What runs, in which order, when, and who is told when something breaks.

Start the UI with `dagster dev -m orchestration.definitions`.
"""

import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    RunFailureSensorContext,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    multi_asset,
    run_failure_sensor,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from pipeline import extract
from pipeline.run import SOURCES_DIR, source_client
from pipeline.warehouse import WAREHOUSE_PATH, connect

ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)

# dbt runs inside dbt_project/, so it gets the absolute warehouse path via its profile.
os.environ.setdefault("WAREHOUSE_PATH", str(WAREHOUSE_PATH))

dbt_project = DbtProject(project_dir=ROOT / "dbt_project", profiles_dir=ROOT / "dbt_project")
dbt_project.prepare_if_dev()

# Without SOURCE_API_URL the mock source systems run inside the pipeline process.
IN_PROCESS_SOURCES = os.getenv("SOURCE_API_URL") is None
# Share of rejected rows per source above which the data quality check fails.
MAX_REJECTED_SHARE = float(os.getenv("MAX_REJECTED_SHARE", "0.02"))


def _metadata(result: extract.LoadResult) -> dict:
    return {
        "rows_received": result.received,
        "rows_new": result.new,
        "detail": result.detail,
    }


def _load_from_api(loader) -> MaterializeResult:
    con = connect()
    try:
        with source_client(IN_PROCESS_SOURCES) as client:
            return MaterializeResult(metadata=_metadata(loader(con, client)))
    finally:
        con.close()


@asset(
    key=AssetKey(["raw", "shop_products"]),
    group_name="ingestion",
    kinds={"python", "duckdb"},
)
def shop_products() -> MaterializeResult:
    """Product catalogue from the shop API."""
    return _load_from_api(extract.load_products)


@asset(
    key=AssetKey(["raw", "shop_orders"]),
    group_name="ingestion",
    kinds={"python", "duckdb"},
)
def shop_orders() -> MaterializeResult:
    """Orders changed since the last run, paged with a cursor."""
    return _load_from_api(extract.load_orders)


@asset(
    key=AssetKey(["raw", "wms_stock_snapshots"]),
    group_name="ingestion",
    kinds={"python", "duckdb"},
)
def wms_stock_snapshots() -> MaterializeResult:
    """Daily stock per SKU from the warehouse system."""
    return _load_from_api(extract.load_stock_snapshots)


@multi_asset(
    specs=[
        AssetSpec(
            ["raw", "logistics_events"],
            description="Events from the partner's daily CSV files.",
        ),
        AssetSpec(
            ["meta", "loaded_files"],
            description="One row per loaded file with its checksum.",
        ),
    ],
    group_name="ingestion",
)
def logistics_events():
    """Daily CSV files from the logistics partner's drop folder."""
    con = connect()
    try:
        result = extract.load_logistics_files(con, SOURCES_DIR / "logistics")
    finally:
        con.close()
    yield MaterializeResult(
        asset_key=AssetKey(["raw", "logistics_events"]), metadata=_metadata(result)
    )
    yield MaterializeResult(asset_key=AssetKey(["meta", "loaded_files"]))


class FolderGroups(DagsterDbtTranslator):
    """Groups dbt models by their folder (staging, quality, marts) in the Dagster UI."""

    def get_group_name(self, dbt_resource_props):
        fqn = dbt_resource_props.get("fqn", [])
        if dbt_resource_props.get("resource_type") == "model" and len(fqn) > 2:
            return fqn[1]
        return super().get_group_name(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=FolderGroups())
def retail_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Staging, quarantine and reporting tables, built and tested by dbt."""
    yield from dbt.cli(["build"], context=context).stream()


@asset_check(
    asset=AssetKey(["raw", "logistics_events"]),
    description="Jeden Tag ist eine Versanddatei angekommen.",
)
def logistics_files_complete() -> AssetCheckResult:
    con = connect()
    try:
        names = [
            row[0] for row in con.execute("select file_name from meta.loaded_files").fetchall()
        ]
    finally:
        con.close()
    days = sorted(
        date.fromisoformat(name.removeprefix("versand_").removesuffix(".csv")) for name in names
    )
    expected = (
        {days[0] + timedelta(days=offset) for offset in range((days[-1] - days[0]).days + 1)}
        if days
        else set()
    )
    missing = sorted(expected - set(days))
    return AssetCheckResult(
        passed=not missing,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "missing_days": [day.isoformat() for day in missing],
            "files_loaded": len(days),
        },
    )


@asset_check(
    asset=AssetKey(["raw", "shop_orders"]),
    description=f"Höchstens {MAX_REJECTED_SHARE:.0%} der Zeilen je Quelle in Quarantäne.",
    additional_deps=[AssetKey(["quality", "dq_rejected_rows"])],
)
def rejected_share_below_threshold() -> AssetCheckResult:
    con = connect()
    try:
        rejected = dict(
            con.execute(
                "select source, count(*) from quality.dq_rejected_rows group by all"
            ).fetchall()
        )
        totals = {
            "shop": con.execute("select count(*) from staging.stg_order_lines").fetchall()[0][0],
            "warehouse": con.execute("select count(*) from raw.wms_stock_snapshots").fetchall()[0][
                0
            ],
            "logistics": con.execute("select count(*) from raw.logistics_events").fetchall()[0][0],
        }
    finally:
        con.close()
    shares = {source: rejected.get(source, 0) / total for source, total in totals.items() if total}
    return AssetCheckResult(
        passed=all(share <= MAX_REJECTED_SHARE for share in shares.values()),
        severity=AssetCheckSeverity.ERROR,
        metadata={f"share_{source}": round(share, 4) for source, share in shares.items()},
    )


daily_pipeline = define_asset_job("daily_pipeline", selection=AssetSelection.all())

daily_schedule = ScheduleDefinition(
    job=daily_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.RUNNING,
)


@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING)
def notify_on_failure(context: RunFailureSensorContext) -> None:
    """Posts failed runs to a chat webhook (Teams, Slack, …) when ALERT_WEBHOOK_URL is set."""
    job = context.dagster_run.job_name
    message = f"Pipeline-Lauf {job} fehlgeschlagen: {context.failure_event.message}"
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if webhook:
        httpx.post(webhook, json={"text": message}, timeout=10)
    else:
        context.log.warning("No ALERT_WEBHOOK_URL set. %s", message)


defs = Definitions(
    assets=[
        shop_products,
        shop_orders,
        wms_stock_snapshots,
        logistics_events,
        retail_models,
    ],
    asset_checks=[logistics_files_complete, rejected_share_below_threshold],
    jobs=[daily_pipeline],
    schedules=[daily_schedule],
    sensors=[notify_on_failure],
    # dbt is installed next to the Python that runs the pipeline, e.g. in .venv/bin.
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            dbt_executable=str(Path(sys.executable).with_name("dbt")),
        )
    },
    # DuckDB allows one writer at a time, so steps run one after another in one process.
    executor=in_process_executor,
)
