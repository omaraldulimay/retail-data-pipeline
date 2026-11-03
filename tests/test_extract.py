import hashlib
import json
import random
import shutil
from datetime import date
from pathlib import Path

import pytest

import pipeline.extract
import sources.app
from pipeline import extract
from pipeline.http import get_json


def count(con, sql: str) -> int:
    return con.execute(sql).fetchall()[0][0]


def distinct_order_ids(order_records: list[dict]) -> set[str]:
    return {order["order_id"] for order in order_records}


def test_full_load_counts(con, api, sources_dir: Path, order_records, sleeps):
    products = extract.load_products(con, api)
    orders = extract.load_orders(con, api)
    snapshots = extract.load_stock_snapshots(con, api)
    logistics = extract.load_logistics_files(con, sources_dir / "logistics")

    snapshot_lines = len((sources_dir / "wms" / "stock_snapshots.jsonl").read_text().splitlines())
    csv_files = len(list((sources_dir / "logistics").glob("versand_*.csv")))
    assert products.new == 35
    assert count(con, "select count(*) from raw.shop_products") == 35
    assert orders.new == len(distinct_order_ids(order_records))
    assert count(con, "select count(*) from raw.shop_orders") == orders.new
    assert snapshots.new == snapshot_lines
    assert count(con, "select count(*) from raw.wms_stock_snapshots") == snapshot_lines
    assert logistics.detail == f"{csv_files} files"
    assert count(con, "select count(*) from meta.loaded_files") == csv_files
    assert count(con, "select count(*) from raw.logistics_events") == logistics.new
    assert count(con, "select count(*) from meta.load_log") == 4
    assert sleeps == []


def test_second_run_loads_nothing_new(con, api, sources_dir: Path, sleeps):
    extract.load_products(con, api)
    extract.load_orders(con, api)
    extract.load_stock_snapshots(con, api)
    extract.load_logistics_files(con, sources_dir / "logistics")
    before = {
        table: count(con, f"select count(*) from {table}")
        for table in (
            "raw.shop_products",
            "raw.shop_orders",
            "raw.wms_stock_snapshots",
            "raw.logistics_events",
        )
    }

    assert extract.load_products(con, api).new == 0
    orders = extract.load_orders(con, api)
    assert (orders.received, orders.new) == (0, 0)
    snapshots = extract.load_stock_snapshots(con, api)
    assert (snapshots.received, snapshots.new) == (0, 0)
    logistics = extract.load_logistics_files(con, sources_dir / "logistics")
    assert (logistics.received, logistics.new) == (0, 0)
    after = {table: count(con, f"select count(*) from {table}") for table in before}
    assert after == before


def test_duplicate_order_line_is_stored_once(con, api, order_records, sleeps):
    duplicate = order_records[-1]
    assert order_records.count(duplicate) == 2
    result = extract.load_orders(con, api)
    assert result.received == len(order_records)
    assert result.new == len(order_records) - 1
    stored = count(
        con, f"select count(*) from raw.shop_orders where order_id = '{duplicate['order_id']}'"
    )
    assert stored == 1


def test_orders_resume_after_interruption(con, api, order_records, tmp_path: Path, monkeypatch):
    calls = 0

    def dying_get_json(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls > 3:
            raise RuntimeError("source went away")
        return get_json(*args, **kwargs, sleep=lambda delay: None)

    monkeypatch.setattr(pipeline.extract, "get_json", dying_get_json)
    with pytest.raises(RuntimeError, match="went away"):
        extract.load_orders(con, api, page_size=1000)
    partial = count(con, "select count(*) from raw.shop_orders")
    assert partial == 3000
    cursor = con.execute("select cursor from meta.extract_state").fetchall()[0][0]
    assert cursor

    monkeypatch.setattr(
        pipeline.extract,
        "get_json",
        lambda *args, **kwargs: get_json(*args, **kwargs, sleep=lambda delay: None),
    )
    resumed = extract.load_orders(con, api, page_size=1000)
    expected = len(distinct_order_ids(order_records))
    assert count(con, "select count(*) from raw.shop_orders") == expected
    assert count(con, "select count(distinct order_id) from raw.shop_orders") == expected
    # The resumed run did not start from scratch.
    assert resumed.received < len(order_records) - partial + 1


def test_full_load_survives_flaky_sources(con, api, sources_dir: Path, order_records, sleeps):
    random.seed(1234)
    sources.app.failure_rate = 0.3
    extract.load_products(con, api)
    extract.load_orders(con, api, page_size=1000)
    extract.load_stock_snapshots(con, api)
    sources.app.failure_rate = 0.0

    snapshot_lines = len((sources_dir / "wms" / "stock_snapshots.jsonl").read_text().splitlines())
    assert count(con, "select count(*) from raw.shop_products") == 35
    assert count(con, "select count(*) from raw.shop_orders") == len(
        distinct_order_ids(order_records)
    )
    assert count(con, "select count(*) from raw.wms_stock_snapshots") == snapshot_lines
    assert len(sleeps) > 0  # retries really happened


def rows_for_file(con, name: str) -> int:
    return count(con, f"select count(*) from raw.logistics_events where file_name = '{name}'")


def test_logistics_unchanged_files_are_skipped(con, logistics_copy: Path):
    first = extract.load_logistics_files(con, logistics_copy)
    assert first.received > 0
    second = extract.load_logistics_files(con, logistics_copy)
    assert (second.received, second.new, second.detail) == (0, 0, "0 files")


def test_logistics_corrected_redelivery_replaces_rows(con, logistics_copy: Path):
    extract.load_logistics_files(con, logistics_copy)
    name = "versand_2026-03-10.csv"
    path = logistics_copy / name
    old_rows = rows_for_file(con, name)
    total_before = count(con, "select count(*) from raw.logistics_events")

    lines = path.read_bytes().decode("cp1252").splitlines(keepends=True)
    assert len(lines) - 1 == old_rows
    path.write_bytes("".join(lines[:-1]).encode("cp1252"))

    result = extract.load_logistics_files(con, logistics_copy)
    assert result.detail == "1 files"
    assert rows_for_file(con, name) == old_rows - 1
    assert count(con, "select count(*) from raw.logistics_events") == total_before - 1
    checksum_rows = con.execute(
        "select row_count from meta.loaded_files where file_name = ?", [name]
    ).fetchall()
    assert checksum_rows == [(old_rows - 1,)]


def test_logistics_new_file_is_picked_up(con, logistics_copy: Path):
    extract.load_logistics_files(con, logistics_copy)
    source = logistics_copy / "versand_2026-03-10.csv"
    new_name = "versand_2026-11-01.csv"
    shutil.copy(source, logistics_copy / new_name)
    expected = rows_for_file(con, source.name)

    result = extract.load_logistics_files(con, logistics_copy)
    assert result.detail == "1 files"
    assert rows_for_file(con, new_name) == expected


def test_new_order_version_is_kept_as_second_row(con, api_for, sources_dir: Path, tmp_path: Path):
    shop = tmp_path / "src" / "shop"
    shop.mkdir(parents=True)
    lines = (sources_dir / "shop" / "orders.jsonl").read_text().splitlines()[:2000]
    (shop / "orders.jsonl").write_text("\n".join(lines) + "\n")
    client = api_for(tmp_path / "src")

    extract.load_orders(con, client, page_size=1000)
    original = json.loads(lines[10])
    assert original["status"] != "cancelled"
    assert (
        count(
            con, f"select count(*) from raw.shop_orders where order_id = '{original['order_id']}'"
        )
        == 1
    )

    newer = {**original, "status": "cancelled", "updated_at": "2099-01-01T00:00:00"}
    with (shop / "orders.jsonl").open("a") as handle:
        handle.write(json.dumps(newer) + "\n")
    sources.app.load_orders.cache_clear()

    result = extract.load_orders(con, client, page_size=1000)
    assert result.new == 1
    versions = con.execute(
        "select payload ->> 'status' from raw.shop_orders where order_id = ? order by updated_at",
        [original["order_id"]],
    ).fetchall()
    assert versions == [(original["status"],), ("cancelled",)]


def test_reload_logistics_day_replaces_rows_without_duplicates(con, logistics_copy: Path):
    extract.load_logistics_files(con, logistics_copy)
    name = "versand_2026-08-13.csv"
    path = logistics_copy / name
    total_before = rows_for_file(con, name)

    # The file changes, but the stored checksum says it is already loaded: the normal load skips it.
    path.write_bytes(b"".join(path.read_bytes().splitlines(keepends=True)[:-1]))
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    con.execute("update meta.loaded_files set checksum = ? where file_name = ?", [checksum, name])
    assert extract.load_logistics_files(con, logistics_copy).received == 0
    assert rows_for_file(con, name) == total_before

    result = extract.reload_logistics_day(con, "2026-08-13", logistics_copy)
    assert result.received == total_before - 1
    assert rows_for_file(con, name) == total_before - 1
    distinct_lines = count(
        con, f"select count(distinct line_no) from raw.logistics_events where file_name = '{name}'"
    )
    assert distinct_lines == total_before - 1
    assert con.execute(
        "select row_count from meta.loaded_files where file_name = ?", [name]
    ).fetchall() == [(total_before - 1,)]


def test_reload_logistics_day_is_idempotent(con, logistics_copy: Path):
    extract.load_logistics_files(con, logistics_copy)
    first = extract.reload_logistics_day(con, date(2026, 8, 13), logistics_copy)
    total = count(con, "select count(*) from raw.logistics_events")
    second = extract.reload_logistics_day(con, date(2026, 8, 13), logistics_copy)
    assert first.received == second.received
    assert count(con, "select count(*) from raw.logistics_events") == total
    assert count(con, "select count(*) from meta.loaded_files") == len(
        list(logistics_copy.glob("versand_*.csv"))
    )


def test_reload_logistics_day_without_file_raises_and_changes_nothing(con, logistics_copy: Path):
    extract.load_logistics_files(con, logistics_copy)
    total = count(con, "select count(*) from raw.logistics_events")
    with pytest.raises(FileNotFoundError, match="2026-08-14"):
        extract.reload_logistics_day(con, "2026-08-14", logistics_copy)
    assert count(con, "select count(*) from raw.logistics_events") == total
