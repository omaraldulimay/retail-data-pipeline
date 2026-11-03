import os
from pathlib import Path

import duckdb
import pyarrow as pa

# Paths default to the project folder, so it does not matter where a tool is started from.
WAREHOUSE_PATH = Path(
    os.getenv("WAREHOUSE_PATH", Path(__file__).resolve().parent.parent / "data/warehouse.duckdb")
)

# Raw tables keep data exactly as delivered (plus load metadata), so every number in the
# dashboard can be traced back to what a source actually sent.
SCHEMA = """
create schema if not exists raw;
create schema if not exists meta;

create table if not exists raw.shop_products (
    sku varchar primary key,
    payload json,
    _loaded_at timestamp
);

create table if not exists raw.shop_orders (
    order_id varchar,
    updated_at varchar,
    payload json,
    payload_hash varchar,
    _loaded_at timestamp,
    primary key (order_id, payload_hash)
);

create table if not exists raw.wms_stock_snapshots (
    snapshot_date varchar,
    sku varchar,
    on_hand integer,
    _loaded_at timestamp,
    primary key (snapshot_date, sku)
);

create table if not exists raw.logistics_events (
    file_name varchar,
    line_no integer,
    order_number varchar,
    tracking_number varchar,
    carrier varchar,
    event varchar,
    event_time varchar,
    return_reason varchar,
    _loaded_at timestamp,
    primary key (file_name, line_no)
);

create table if not exists meta.loaded_files (
    file_name varchar primary key,
    checksum varchar,
    row_count integer,
    loaded_at timestamp
);

create table if not exists meta.extract_state (
    source varchar primary key,
    cursor varchar,
    updated_at timestamp
);

create table if not exists meta.load_log (
    source varchar,
    started_at timestamp,
    finished_at timestamp,
    rows_received integer,
    rows_new integer,
    detail varchar
);
"""


def connect(path: Path | str = WAREHOUSE_PATH) -> duckdb.DuckDBPyConnection:
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def insert_rows(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    rows: list[tuple],
    *,
    conflict: str = "ignore",
) -> None:
    """Inserts many rows in one statement; row-by-row inserts are slow in DuckDB."""
    if not rows:
        return
    batch = pa.Table.from_pylist([dict(zip(columns, row, strict=True)) for row in rows])
    con.register("_batch", batch)
    try:
        con.execute(
            f"insert or {conflict} into {table} ({', '.join(columns)}, _loaded_at) "
            f"select {', '.join(columns)}, current_timestamp from _batch"
        )
    finally:
        con.unregister("_batch")


def get_cursor(con: duckdb.DuckDBPyConnection, source: str) -> str | None:
    rows = con.execute(
        "select cursor from meta.extract_state where source = ?", [source]
    ).fetchall()
    return rows[0][0] if rows else None


def set_cursor(con: duckdb.DuckDBPyConnection, source: str, cursor: str) -> None:
    con.execute(
        "insert or replace into meta.extract_state values (?, ?, current_timestamp)",
        [source, cursor],
    )


def log_load(
    con, source: str, started_at, rows_received: int, rows_new: int, detail: str = ""
) -> None:
    con.execute(
        "insert into meta.load_log values (?, ?, current_timestamp, ?, ?, ?)",
        [source, started_at, rows_received, rows_new, detail],
    )
