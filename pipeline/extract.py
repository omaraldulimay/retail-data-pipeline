"""Loads the three sources into the raw schema. Every load is incremental and safe to repeat."""

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import httpx

from pipeline.http import get_json
from pipeline.warehouse import get_cursor, insert_rows, log_load, set_cursor

LOGISTICS_COLUMNS = [
    "file_name",
    "line_no",
    "order_number",
    "tracking_number",
    "carrier",
    "event",
    "event_time",
    "return_reason",
]


@dataclass(frozen=True)
class LoadResult:
    source: str
    received: int
    new: int
    detail: str = ""


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"select count(*) from {table}").fetchall()[0][0]


def load_products(con: duckdb.DuckDBPyConnection, client: httpx.Client) -> LoadResult:
    started = datetime.now()
    products = get_json(client, "/shop/products")
    before = _count(con, "raw.shop_products")
    insert_rows(
        con,
        "raw.shop_products",
        ["sku", "payload"],
        [(product["sku"], json.dumps(product, ensure_ascii=False)) for product in products],
        conflict="replace",
    )
    result = LoadResult("shop_products", len(products), _count(con, "raw.shop_products") - before)
    log_load(con, result.source, started, result.received, result.new)
    return result


def load_orders(
    con: duckdb.DuckDBPyConnection, client: httpx.Client, page_size: int = 500
) -> LoadResult:
    """Pages through orders changed since the stored cursor.

    Every distinct version of an order is kept; an exact repeat of a version is ignored.
    The cursor is saved after each page, so an interrupted run continues where it stopped.
    """
    started = datetime.now()
    cursor = get_cursor(con, "shop_orders")
    before = _count(con, "raw.shop_orders")
    received = 0
    while True:
        params = {"limit": page_size}
        if cursor:
            params["cursor"] = cursor
        page = get_json(client, "/shop/orders", params=params)
        rows = []
        for order in page["orders"]:
            payload = json.dumps(order, ensure_ascii=False, sort_keys=True)
            digest = hashlib.sha256(payload.encode()).hexdigest()
            rows.append((order["order_id"], order["updated_at"], payload, digest))
        if rows:
            insert_rows(
                con,
                "raw.shop_orders",
                ["order_id", "updated_at", "payload", "payload_hash"],
                rows,
            )
            received += len(rows)
            last = page["orders"][-1]
            cursor = f"{last['updated_at']}|{last['order_id']}"
            set_cursor(con, "shop_orders", cursor)
        if not page["next_cursor"]:
            break
    result = LoadResult("shop_orders", received, _count(con, "raw.shop_orders") - before)
    log_load(con, result.source, started, result.received, result.new)
    return result


def load_stock_snapshots(con: duckdb.DuckDBPyConnection, client: httpx.Client) -> LoadResult:
    """Loads every snapshot day that is not in the warehouse yet."""
    started = datetime.now()
    available = get_json(client, "/wms/snapshot-dates")
    loaded = {
        row[0]
        for row in con.execute(
            "select distinct snapshot_date from raw.wms_stock_snapshots"
        ).fetchall()
    }
    received = new = 0
    for snapshot_date in sorted(set(available) - loaded):
        rows = get_json(client, "/wms/stock-snapshots", params={"snapshot_date": snapshot_date})
        insert_rows(
            con,
            "raw.wms_stock_snapshots",
            ["snapshot_date", "sku", "on_hand"],
            [(row["snapshot_date"], row["sku"], row["on_hand"]) for row in rows],
            conflict="replace",
        )
        received += len(rows)
        new += len(rows)
    result = LoadResult(
        "wms_stock_snapshots", received, new, f"{len(set(available) - loaded)} days"
    )
    log_load(con, result.source, started, result.received, result.new, result.detail)
    return result


def _parse_logistics_csv(file_name: str, content: bytes) -> list[tuple]:
    reader = csv.reader(io.StringIO(content.decode("cp1252")), delimiter=";")
    next(reader, None)  # header
    return [
        (file_name, line_no, *(row + [""] * 6)[:6])
        for line_no, row in enumerate(reader, start=2)
        if any(cell.strip() for cell in row)
    ]


def _replace_file(
    con: duckdb.DuckDBPyConnection, file_name: str, checksum: str, rows: list[tuple]
) -> None:
    """Swaps the rows and the checksum entry of one file in a single transaction."""
    con.execute("begin")
    try:
        con.execute("delete from raw.logistics_events where file_name = ?", [file_name])
        con.execute("delete from meta.loaded_files where file_name = ?", [file_name])
        insert_rows(con, "raw.logistics_events", LOGISTICS_COLUMNS, rows)
        con.execute(
            "insert into meta.loaded_files values (?, ?, ?, current_timestamp)",
            [file_name, checksum, len(rows)],
        )
    except BaseException:
        con.execute("rollback")
        raise
    con.execute("commit")


def load_logistics_files(con: duckdb.DuckDBPyConnection, directory: Path) -> LoadResult:
    """Loads new or changed CSV files from the logistics partner's drop folder.

    A file is identified by its name and checksum: an unchanged file is skipped, a corrected
    re-delivery under the same name replaces the earlier rows.
    """
    started = datetime.now()
    known = dict(con.execute("select file_name, checksum from meta.loaded_files").fetchall())
    received = files = 0
    for path in sorted(directory.glob("versand_*.csv")):
        content = path.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()
        if known.get(path.name) == checksum:
            continue
        rows = _parse_logistics_csv(path.name, content)
        _replace_file(con, path.name, checksum, rows)
        received += len(rows)
        files += 1
    result = LoadResult("logistics_events", received, received, f"{files} files")
    log_load(con, result.source, started, result.received, result.new, result.detail)
    return result


def reload_logistics_day(
    con: duckdb.DuckDBPyConnection, day: date | str, directory: Path
) -> LoadResult:
    """Force-reloads one day's CSV file, even if name and checksum are unchanged.

    Deletes the day's rows and its loaded_files entry and loads the file again in one
    transaction. Raises FileNotFoundError if the partner's file for that day does not exist.
    """
    day = date.fromisoformat(str(day))
    path = directory / f"versand_{day.isoformat()}.csv"
    if not path.is_file():
        raise FileNotFoundError(f"No logistics file for {day.isoformat()}: {path} does not exist")
    started = datetime.now()
    content = path.read_bytes()
    rows = _parse_logistics_csv(path.name, content)
    _replace_file(con, path.name, hashlib.sha256(content).hexdigest(), rows)
    result = LoadResult("logistics_events", len(rows), len(rows), f"reloaded {path.name}")
    log_load(con, result.source, started, result.received, result.new, result.detail)
    return result
