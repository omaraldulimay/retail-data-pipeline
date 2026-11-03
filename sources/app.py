"""Mock shop and warehouse APIs that serve the generated source data.

Run with `uvicorn sources.app:app --port 8100`. Set SOURCE_FAILURE_RATE (0..1) to make the
APIs fail randomly with 503, which the extractors are expected to survive.
"""

import json
import os
import random
from functools import cache
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query

SOURCES_DIR = Path(
    os.getenv("SOURCES_DIR", Path(__file__).resolve().parent.parent / "data/sources")
)
failure_rate = float(os.getenv("SOURCE_FAILURE_RATE", "0"))

app = FastAPI(title="Source systems (mock)")


def flaky() -> None:
    if random.random() < failure_rate:
        raise HTTPException(status_code=503, detail="Temporarily unavailable")


@cache
def load_orders() -> list[dict]:
    with (SOURCES_DIR / "shop" / "orders.jsonl").open() as handle:
        orders = [json.loads(line) for line in handle]
    # Paging is stable when records are sorted by (updated_at, order_id).
    return sorted(orders, key=lambda order: (order["updated_at"], order["order_id"]))


@cache
def load_snapshots() -> dict[str, list[dict]]:
    by_date: dict[str, list[dict]] = {}
    with (SOURCES_DIR / "wms" / "stock_snapshots.jsonl").open() as handle:
        for line in handle:
            row = json.loads(line)
            by_date.setdefault(row["snapshot_date"], []).append(row)
    return by_date


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/shop/products", dependencies=[Depends(flaky)])
def products() -> list[dict]:
    return json.loads((SOURCES_DIR / "shop" / "products.json").read_text())


@app.get("/shop/orders", dependencies=[Depends(flaky)])
def orders(
    updated_since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(500, ge=1, le=1000),
) -> dict:
    """Orders changed since a timestamp, in pages. `cursor` continues where a page stopped."""
    rows = load_orders()
    if cursor:
        after = tuple(cursor.split("|", 1))
        rows = [row for row in rows if (row["updated_at"], row["order_id"]) > after]
    elif updated_since:
        rows = [row for row in rows if row["updated_at"] > updated_since]
    page = rows[:limit]
    next_cursor = f"{page[-1]['updated_at']}|{page[-1]['order_id']}" if len(rows) > limit else None
    return {"orders": page, "next_cursor": next_cursor}


@app.get("/wms/stock-snapshots", dependencies=[Depends(flaky)])
def stock_snapshots(snapshot_date: str) -> list[dict]:
    rows = load_snapshots().get(snapshot_date)
    if rows is None:
        raise HTTPException(status_code=404, detail=f"No snapshot for {snapshot_date}")
    return rows


@app.get("/wms/snapshot-dates")
def snapshot_dates() -> list[str]:
    return sorted(load_snapshots())
