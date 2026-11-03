"""What runs, in which order, when, and who is told when something breaks.

Start the UI with `dagster dev -m orchestration.definitions`.
"""

import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    In,
    MaterializeResult,
    Nothing,
    OpExecutionContext,
    RunFailureSensorContext,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    job,
    multi_asset,
    op,
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
# Hours after which the last successful load counts as stale.
FRESHNESS_HOURS = float(os.getenv("FRESHNESS_HOURS", "26"))
# A source may lag the newest source by this many days.
MAX_SOURCE_LAG_DAYS = 1


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


@asset_check(
    asset=AssetKey(["marts", "alert_sales_anomalies"]),
    description=(
        "Der letzte Tag in den Daten ist kein unerwarteter Umsatz-Ausreißer "
        "(Tage in bekannten Kalenderereignissen zählen nicht)."
    ),
    additional_deps=[AssetKey(["marts", "agg_daily_sales"])],
)
def latest_day_not_anomalous() -> AssetCheckResult:
    con = connect()
    try:
        latest = con.execute("select max(order_date) from marts.agg_daily_sales").fetchall()[0][0]
        rows = con.execute(
            "select revenue, expected_revenue, deviation_pct, direction "
            "from marts.alert_sales_anomalies where order_date = ? and not is_expected",
            [latest],
        ).fetchall()
    finally:
        con.close()
    metadata = {"latest_day": str(latest), "is_anomaly": bool(rows)}
    if rows:
        revenue, expected, deviation, direction = rows[0]
        metadata |= {
            "revenue": float(revenue),
            "expected_revenue": float(expected),
            "deviation_pct": float(deviation),
            "direction": direction,
        }
    return AssetCheckResult(passed=not rows, severity=AssetCheckSeverity.WARN, metadata=metadata)


@asset_check(
    asset=AssetKey(["raw", "shop_orders"]),
    description=(
        f"Alle Quellen liefern Daten bis höchstens {MAX_SOURCE_LAG_DAYS} Tag hinter der "
        f"neuesten Quelle, und der letzte erfolgreiche Load ist jünger als {FRESHNESS_HOURS:g} h."
    ),
    additional_deps=[
        AssetKey(["raw", "wms_stock_snapshots"]),
        AssetKey(["raw", "logistics_events"]),
    ],
)
def sources_fresh() -> AssetCheckResult:
    con = connect()
    try:
        return _check_sources_fresh(con)
    finally:
        con.close()


def _check_sources_fresh(con, now: datetime | None = None) -> AssetCheckResult:
    newest = {
        "shop_orders": con.execute(
            "select max(cast(payload ->> 'created_at' as timestamp)::date) from raw.shop_orders"
        ).fetchall()[0][0],
        "wms_stock_snapshots": con.execute(
            "select max(cast(snapshot_date as date)) from raw.wms_stock_snapshots"
        ).fetchall()[0][0],
        "logistics": max(
            (
                date.fromisoformat(name[0].removeprefix("versand_").removesuffix(".csv"))
                for name in con.execute("select file_name from meta.loaded_files").fetchall()
            ),
            default=None,
        ),
    }
    # Rows in load_log are written only after a load finished, so they are all successful.
    last_load = con.execute("select max(finished_at) from meta.load_log").fetchall()[0][0]
    reference = max((day for day in newest.values() if day), default=None)
    lagging = {
        source: (reference - day).days
        for source, day in newest.items()
        if day is None or reference - day > timedelta(days=MAX_SOURCE_LAG_DAYS)
    }
    age_hours = ((now or datetime.now()) - last_load).total_seconds() / 3600 if last_load else None
    load_stale = age_hours is None or age_hours > FRESHNESS_HOURS
    metadata = {f"newest_{source}": str(day) for source, day in newest.items()}
    metadata |= {
        "lagging_sources": sorted(lagging),
        "last_load_finished_at": str(last_load),
        "last_load_age_hours": round(age_hours, 1) if age_hours is not None else None,
    }
    return AssetCheckResult(
        passed=not lagging and not load_stale,
        severity=AssetCheckSeverity.WARN,
        metadata=metadata,
    )


daily_pipeline = define_asset_job("daily_pipeline", selection=AssetSelection.all())

daily_schedule = ScheduleDefinition(
    job=daily_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.RUNNING,
)


# Backfill of one day. An op job, because partitioned and unpartitioned assets cannot share a job.
logistics_days = DailyPartitionsDefinition(start_date="2025-11-01", end_offset=0)


@op(description="Reloads the partition's logistics CSV file, even if its checksum is unchanged.")
def reload_logistics_file(context: OpExecutionContext):
    con = connect()
    try:
        result = extract.reload_logistics_day(con, context.partition_key, SOURCES_DIR / "logistics")
    finally:
        con.close()
    context.log.info("Reloaded %s rows (%s)", result.received, result.detail)


@op(ins={"after_reload": In(Nothing)}, description="Rebuilds all dbt models and tests.")
def build_models(dbt: DbtCliResource) -> None:
    dbt.cli(["build"], manifest=dbt_project.manifest_path).wait()


@job(partitions_def=logistics_days, executor_def=in_process_executor)
def reload_logistics_day():
    build_models(after_reload=reload_logistics_file())


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
    asset_checks=[
        logistics_files_complete,
        rejected_share_below_threshold,
        latest_day_not_anomalous,
        sources_fresh,
    ],
    jobs=[daily_pipeline, reload_logistics_day],
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
