"""Generates one year of realistic, slightly messy source data for the pipeline.

Run with `python -m generator.generate`. The output is deterministic for a given seed.
"""

import argparse
import csv
import json
import math
import random
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

from generator.catalog import Product, build_catalog

START = date(2025, 11, 1)
END = date(2026, 10, 31)

CHANNELS = [("webshop", 0.68), ("app", 0.17), ("marketplace", 0.15)]
COUNTRIES = [("DE", 0.85), ("AT", 0.08), ("CH", 0.04), ("NL", 0.03)]
CARRIERS = [("DHL", 0.6), ("DPD", 0.25), ("GLS", 0.15)]
RETURN_REASONS = [
    "Gefällt nicht",
    "Beschädigt",
    "Farbe weicht ab",
    "Falscher Artikel",
    "Zu spät",
]
# Typical reasons differ per category: ceramics break, textiles look different on screen.
REASON_WEIGHTS = {
    "Kerzen": [40, 20, 15, 10, 15],
    "Keramik": [20, 50, 10, 10, 10],
    "Textilien": [35, 5, 40, 10, 10],
    "Deko": [40, 25, 15, 10, 10],
    "Küche": [35, 30, 10, 15, 10],
}
# Days from shipping to delivery: DHL is fastest, GLS slowest.
CARRIER_DAYS = {"DHL": [55, 33, 9, 3], "DPD": [45, 37, 13, 5], "GLS": [32, 38, 21, 9]}

# The logistics partner switched its export format on this day, as partners do.
FORMAT_SWITCH = date(2026, 6, 1)
# On this day the partner's export failed, so no file arrives.
MISSING_FILE_DAY = date(2026, 8, 14)


@dataclass
class OrderLine:
    sku: str
    quantity: int
    unit_price: float


@dataclass
class Order:
    order_id: str
    created_at: str
    updated_at: str
    status: str
    channel: str
    customer_id: str
    country: str
    lines: list[OrderLine] = field(default_factory=list)


def pick(rng: random.Random, weighted: list[tuple[str, float]]) -> str:
    return rng.choices([value for value, _ in weighted], [weight for _, weight in weighted])[0]


def demand_factor(day: date) -> float:
    """Relative order volume for a day: weekday rhythm, seasons, campaigns and growth."""
    weekday = [1.0, 0.95, 0.95, 1.0, 1.05, 0.9, 1.2][day.weekday()]
    season = 1.0
    black_friday = date(day.year, 11, 28)
    if day.month == 11 and abs((day - black_friday).days) <= 3:
        season = 2.8
    elif day.month == 12 and day.day <= 20:
        season = 1.9
    elif day.month == 12:
        season = 0.6
    elif day.month == 1 and day.day <= 21:
        season = 1.35  # January sale
    elif day.month in (6, 7, 8):
        season = 0.8
    growth = 1 + 0.2 * (day - START).days / (END - START).days
    return weekday * season * growth


def is_sale(day: date) -> bool:
    black_friday = date(day.year, 11, 28)
    return (day.month == 11 and abs((day - black_friday).days) <= 3) or (
        day.month == 1 and day.day <= 21
    )


def business_days_after(start: date, days: int) -> date:
    current = start
    while days > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days -= 1
    return current


def poisson(rng: random.Random, mean: float) -> int:
    # Knuth's method is fine for the small daily means used here.
    limit, k, p = math.exp(-mean), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def generate(seed: int = 42) -> dict:
    rng = random.Random(seed)
    catalog = build_catalog()
    popularity = [rng.uniform(0.4, 1.6) for _ in catalog]
    orders: list[Order] = []
    events: list[dict] = []
    units_shipped: dict[tuple[date, str], int] = {}
    customers = [f"C{100000 + i}" for i in range(9000)]

    day = START
    while day <= END:
        for _ in range(poisson(rng, 62 * demand_factor(day))):
            order = make_order(rng, day, len(orders) + 1, catalog, popularity, customers)
            orders.append(order)
            events.extend(make_shipping_events(rng, order, catalog, units_shipped))
        day += timedelta(days=1)

    inject_order_errors(rng, orders)
    snapshots = simulate_stock(rng, catalog, units_shipped)
    return {
        "catalog": catalog,
        "orders": orders,
        "events": events,
        "snapshots": snapshots,
    }


def make_order(rng, day, number, catalog, popularity, customers) -> Order:
    created = datetime.combine(
        day, time(rng.randint(6, 23), rng.randint(0, 59), rng.randint(0, 59))
    )
    discount = 0.8 if is_sale(day) else 1.0
    lines = []
    for product in rng.choices(
        catalog, popularity, k=rng.choices([1, 2, 3, 4], [55, 28, 12, 5])[0]
    ):
        if any(line.sku == product.sku for line in lines):
            continue
        lines.append(
            OrderLine(
                product.sku,
                rng.choices([1, 2, 3], [80, 15, 5])[0],
                round(product.price * discount, 2),
            )
        )
    order = Order(
        order_id=f"NH-{day.year}-{number:06d}",
        created_at=created.isoformat(),
        updated_at=created.isoformat(),
        status="paid",
        channel=pick(rng, CHANNELS),
        customer_id=rng.choice(customers),
        country=pick(rng, COUNTRIES),
        lines=lines,
    )
    # A few orders are cancelled later on, which the pipeline must pick up as an update.
    if rng.random() < 0.025:
        order.status = "cancelled"
        order.updated_at = (created + timedelta(hours=rng.randint(1, 30))).isoformat()
    return order


def make_shipping_events(rng, order: Order, catalog: list[Product], units_shipped) -> list[dict]:
    if order.status == "cancelled":
        return []
    created = datetime.fromisoformat(order.created_at)
    carrier = pick(rng, CARRIERS)
    tracking = f"{carrier}{rng.randint(10**11, 10**12 - 1)}"
    shipped = datetime.combine(
        business_days_after(created.date(), rng.choices([0, 1, 2, 3], [20, 55, 18, 7])[0]),
        time(rng.randint(14, 19), rng.randint(0, 59)),
    )
    transit_days = rng.choices([1, 2, 3, 4], CARRIER_DAYS[carrier])[0]
    # Carriers are overloaded in the weeks before Christmas.
    if shipped.month == 12 and shipped.day <= 23 and rng.random() < 0.45:
        transit_days += rng.randint(1, 2)
    delivered = datetime.combine(
        business_days_after(shipped.date(), transit_days),
        time(rng.randint(8, 20), rng.randint(0, 59)),
    )
    base = {"order_id": order.order_id, "tracking_id": tracking, "carrier": carrier}
    events = [
        {**base, "event": "VERSENDET", "at": shipped, "reason": ""},
        {**base, "event": "ZUGESTELLT", "at": delivered, "reason": ""},
    ]
    by_sku = {product.sku: product for product in catalog}
    for line in order.lines:
        key = (shipped.date(), line.sku)
        units_shipped[key] = units_shipped.get(key, 0) + line.quantity
    riskiest = max(
        (by_sku[line.sku] for line in order.lines),
        key=lambda product: product.return_rate,
    )
    if rng.random() < riskiest.return_rate * 1.3:
        returned = delivered + timedelta(days=rng.randint(3, 14), hours=rng.randint(0, 8))
        reason = rng.choices(RETURN_REASONS, REASON_WEIGHTS[riskiest.category])[0]
        events.append({**base, "event": "RETOURE_EINGANG", "at": returned, "reason": reason})
    return events


def inject_order_errors(rng, orders: list[Order]) -> None:
    """Typical upstream data problems that the quality checks must catch."""
    for order in rng.sample(orders, k=len(orders) // 400):
        order.lines[0].quantity = 0
    for order in rng.sample(orders, k=len(orders) // 600):
        order.lines.append(OrderLine("LEGACY-0815", 1, 9.99))
    for order in rng.sample(orders, k=len(orders) // 900):
        order.lines[0].unit_price = -abs(order.lines[0].unit_price)


def simulate_stock(rng, catalog: list[Product], units_shipped) -> list[dict]:
    snapshots = []
    stock = {product.sku: rng.randint(80, 220) for product in catalog}
    incoming: dict[date, dict[str, int]] = {}
    day = START
    while day <= END:
        for sku, quantity in incoming.pop(day, {}).items():
            stock[sku] += quantity
        for product in catalog:
            stock[product.sku] = max(
                0, stock[product.sku] - units_shipped.get((day, product.sku), 0)
            )
            reorder_pending = any(product.sku in batch for batch in incoming.values())
            if stock[product.sku] < 40 and not reorder_pending:
                arrival = day + timedelta(days=rng.randint(7, 21))
                incoming.setdefault(arrival, {})[product.sku] = rng.randint(150, 300)
            snapshots.append(
                {
                    "snapshot_date": day.isoformat(),
                    "sku": product.sku,
                    "on_hand": stock[product.sku],
                }
            )
        day += timedelta(days=1)
    # A warehouse system bug reports negative stock on a few rows.
    for row in rng.sample(snapshots, k=4):
        row["on_hand"] = -rng.randint(1, 12)
    return snapshots


def write_logistics_files(rng, events: list[dict], directory: Path) -> int:
    """Daily semicolon CSVs in the Windows encoding German partners typically send."""
    directory.mkdir(parents=True, exist_ok=True)
    by_day: dict[date, list[dict]] = {}
    for event in events:
        if event["at"].date() <= END:
            by_day.setdefault(event["at"].date(), []).append(event)

    written = 0
    for day, rows in sorted(by_day.items()):
        if day == MISSING_FILE_DAY:
            continue
        rows.sort(key=lambda row: row["at"])
        # The partner occasionally sends the same row twice and references unknown orders.
        rows += rng.sample(rows, k=max(1, len(rows) // 150))
        if rng.random() < 0.15:
            rows.append({**rows[0], "order_id": f"NH-{day.year}-999{rng.randint(100, 999)}"})
        path = directory / f"versand_{day.isoformat()}.csv"
        with path.open("w", encoding="cp1252", newline="") as handle:
            writer = csv.writer(handle, delimiter=";")
            writer.writerow(
                [
                    "Auftragsnummer",
                    "Sendungsnummer",
                    "Spediteur",
                    "Ereignis",
                    "Zeitpunkt",
                    "Retourengrund",
                ]
            )
            for row in rows:
                stamp = row["at"].strftime(
                    "%Y-%m-%d %H:%M" if day < FORMAT_SWITCH else "%d.%m.%Y %H:%M"
                )
                writer.writerow(
                    [
                        row["order_id"],
                        row["tracking_id"],
                        row["carrier"],
                        row["event"],
                        stamp,
                        row["reason"],
                    ]
                )
        written += 1
    return written


def write_all(output: Path, seed: int) -> dict:
    data = generate(seed)
    rng = random.Random(seed + 1)
    (output / "shop").mkdir(parents=True, exist_ok=True)
    (output / "wms").mkdir(parents=True, exist_ok=True)

    (output / "shop" / "products.json").write_text(
        json.dumps(
            [asdict(product) for product in data["catalog"]],
            ensure_ascii=False,
            indent=1,
        )
    )
    orders = data["orders"]
    # The shop sends one order twice with a newer timestamp, as webhooks sometimes do.
    duplicate = json.loads(json.dumps(asdict(orders[len(orders) // 2])))
    with (output / "shop" / "orders.jsonl").open("w") as handle:
        for order in orders:
            handle.write(json.dumps(asdict(order), ensure_ascii=False) + "\n")
        handle.write(json.dumps(duplicate, ensure_ascii=False) + "\n")
    with (output / "wms" / "stock_snapshots.jsonl").open("w") as handle:
        for row in data["snapshots"]:
            handle.write(json.dumps(row) + "\n")
    files = write_logistics_files(rng, data["events"], output / "logistics")
    return {
        "products": len(data["catalog"]),
        "orders": len(orders),
        "order_lines": sum(len(order.lines) for order in orders),
        "stock_snapshots": len(data["snapshots"]),
        "logistics_files": files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/sources"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    print(json.dumps(write_all(args.output, args.seed), indent=1))


if __name__ == "__main__":
    main()
