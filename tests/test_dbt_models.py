import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

import sources.app
from pipeline import extract
from pipeline.warehouse import connect

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "dbt_project"
NEGATIVE_STOCK_ROWS = 4


@pytest.fixture(scope="session")
def built_warehouse(sources_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Loads the session data into a temp warehouse and runs `dbt build` once on it."""
    workdir = tmp_path_factory.mktemp("dbt")
    warehouse = workdir / "warehouse.duckdb"

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sources.app, "SOURCES_DIR", sources_dir)
        patch.setattr(sources.app, "failure_rate", 0.0)
        sources.app.load_orders.cache_clear()
        sources.app.load_snapshots.cache_clear()
        con = connect(warehouse)
        try:
            with TestClient(sources.app.app) as client:
                extract.load_products(con, client)
                extract.load_orders(con, client, page_size=1000)
                extract.load_stock_snapshots(con, client)
            extract.load_logistics_files(con, sources_dir / "logistics")
        finally:
            con.close()
        sources.app.load_orders.cache_clear()
        sources.app.load_snapshots.cache_clear()

    dbt = Path(sys.executable).with_name("dbt")
    result = subprocess.run(
        [
            str(dbt),
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        env={
            **os.environ,
            "WAREHOUSE_PATH": str(warehouse),
            "DBT_TARGET_PATH": str(workdir / "target"),
            "DBT_LOG_PATH": str(workdir / "logs"),
        },
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    yield warehouse


@pytest.fixture
def wh(built_warehouse: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect(str(built_warehouse), read_only=True)
    yield con
    con.close()


def scalar(con, sql: str):
    return con.execute(sql).fetchall()[0][0]


def expected_order_line_rules(order_records: list[dict], sources_dir: Path) -> dict[str, int]:
    """Applies the quarantine rules (in the models' precedence) to the raw source files."""
    skus = {
        product["sku"] for product in json.loads((sources_dir / "shop/products.json").read_text())
    }
    orders = {order["order_id"]: order for order in order_records}.values()
    rules = {"Menge kleiner oder gleich 0": 0, "Negativer Preis": 0, "Unbekannter Artikel": 0}
    for order in orders:
        for line in order["lines"]:
            if line["quantity"] <= 0:
                rules["Menge kleiner oder gleich 0"] += 1
            elif line["unit_price"] < 0:
                rules["Negativer Preis"] += 1
            elif line["sku"] not in skus:
                rules["Unbekannter Artikel"] += 1
    return rules


def test_quarantine_counts_match_injected_errors(wh, order_records, sources_dir):
    distinct = len({order["order_id"] for order in order_records})
    rejected = dict(
        wh.execute(
            "select rule, count(*) from quality.dq_rejected_rows "
            "where source in ('shop', 'warehouse') group by rule"
        ).fetchall()
    )
    assert rejected == {
        "Menge kleiner oder gleich 0": distinct // 400,
        "Unbekannter Artikel": distinct // 600,
        "Negativer Preis": distinct // 900,
        "Negativer Bestand": NEGATIVE_STOCK_ROWS,
    }
    order_rules = {k: v for k, v in rejected.items() if k != "Negativer Bestand"}
    assert order_rules == expected_order_line_rules(order_records, sources_dir)


def test_only_august_14_is_missing(wh):
    rows = wh.execute("select missing_date, expected_file from quality.dq_missing_files").fetchall()
    assert [(str(day), name) for day, name in rows] == [("2026-08-14", "versand_2026-08-14.csv")]


def test_both_timestamp_formats_are_parsed(wh):
    assert (
        scalar(wh, "select count(*) from staging.stg_logistics_events where event_at is null") == 0
    )
    assert (
        scalar(wh, "select count(*) from quality.dq_rejected_rows where rule = 'Ungültiges Datum'")
        == 0
    )
    formats = scalar(
        wh,
        "select count(distinct event_at >= timestamp '2026-06-01') "
        "from staging.stg_logistics_events",
    )
    assert formats == 2


def test_cancelled_orders_are_excluded_from_daily_sales(wh):
    cancelled = scalar(wh, "select count(*) from marts.fct_orders where status = 'cancelled'")
    assert cancelled > 0
    active = scalar(wh, "select count(*) from marts.fct_orders where status <> 'cancelled'")
    assert scalar(wh, "select sum(orders) from marts.agg_daily_sales") == active


def test_fct_orders_has_one_row_per_order(wh, order_records):
    distinct = len({order["order_id"] for order in order_records})
    assert scalar(wh, "select count(*) from marts.fct_orders") == distinct
    assert scalar(wh, "select count(distinct order_id) from marts.fct_orders") == distinct


def test_fct_order_lines_are_clean(wh):
    assert scalar(wh, "select count(*) from marts.fct_order_lines") > 0
    bad = scalar(
        wh,
        "select count(*) from marts.fct_order_lines where quantity <= 0 or unit_price < 0",
    )
    assert bad == 0


def test_stock_coverage_has_one_row_per_product(wh):
    products = scalar(wh, "select count(*) from staging.stg_products")
    assert products == 35
    assert scalar(wh, "select count(*) from marts.agg_stock_coverage") == products
    assert scalar(wh, "select count(distinct sku) from marts.agg_stock_coverage") == products
    assert scalar(wh, "select min(on_hand) from marts.agg_stock_coverage") >= 0
