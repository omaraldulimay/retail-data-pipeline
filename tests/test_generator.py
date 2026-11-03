import csv
import re
from collections import Counter
from datetime import date
from pathlib import Path

import pytest

from generator.generate import FORMAT_SWITCH, MISSING_FILE_DAY, write_all


def read_jsonl(path: Path) -> list[str]:
    return path.read_text().splitlines()


def distinct_orders(order_records: list[dict]) -> list[dict]:
    return list({order["order_id"]: order for order in order_records}.values())


def test_same_seed_gives_identical_orders(tmp_path: Path, sources_dir: Path):
    write_all(tmp_path, 42)
    assert (tmp_path / "shop" / "orders.jsonl").read_bytes() == (
        sources_dir / "shop" / "orders.jsonl"
    ).read_bytes()


def test_different_seed_gives_different_orders(tmp_path: Path, sources_dir: Path):
    write_all(tmp_path, 7)
    assert (tmp_path / "shop" / "orders.jsonl").read_bytes() != (
        sources_dir / "shop" / "orders.jsonl"
    ).read_bytes()


def test_last_order_line_is_an_exact_duplicate(order_records: list[dict]):
    assert order_records[-1] in order_records[:-1]
    assert len(distinct_orders(order_records)) == len(order_records) - 1


def test_injected_order_errors(order_records: list[dict]):
    orders = distinct_orders(order_records)
    lines = [line for order in orders for line in order["lines"]]
    assert sum(line["quantity"] == 0 for line in lines) == len(orders) // 400
    assert sum(line["sku"] == "LEGACY-0815" for line in lines) == len(orders) // 600
    assert sum(line["unit_price"] < 0 for line in lines) == len(orders) // 900


def test_exactly_four_negative_stock_snapshots(sources_dir: Path):
    rows = read_jsonl(sources_dir / "wms" / "stock_snapshots.jsonl")
    assert sum('"on_hand": -' in row for row in rows) == 4


def logistics_files(sources_dir: Path) -> dict[date, Path]:
    return {
        date.fromisoformat(path.stem.removeprefix("versand_")): path
        for path in (sources_dir / "logistics").glob("versand_*.csv")
    }


def test_missing_logistics_file(sources_dir: Path):
    files = logistics_files(sources_dir)
    assert date(2026, 8, 14) == MISSING_FILE_DAY
    assert MISSING_FILE_DAY not in files
    assert not (sources_dir / "logistics" / "versand_2026-08-14.csv").exists()
    # Neighbouring days exist, so the gap is a real hole and not the end of the range.
    assert date(2026, 8, 13) in files
    assert date(2026, 8, 15) in files


def test_logistics_files_are_cp1252_semicolon_csv(sources_dir: Path):
    for path in logistics_files(sources_dir).values():
        text = path.read_bytes().decode("cp1252")
        rows = list(csv.reader(text.splitlines(), delimiter=";"))
        assert rows[0][0] == "Auftragsnummer"
        assert all(len(row) == 6 for row in rows)


def test_timestamp_format_switches_on_1_june_2026(sources_dir: Path):
    iso = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}")
    german = re.compile(r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}")
    files = logistics_files(sources_dir)
    assert any(day < FORMAT_SWITCH for day in files)
    assert any(day >= FORMAT_SWITCH for day in files)
    for day, path in files.items():
        pattern = iso if day < FORMAT_SWITCH else german
        text = path.read_bytes().decode("cp1252")
        for row in list(csv.reader(text.splitlines(), delimiter=";"))[1:]:
            assert pattern.fullmatch(row[4]), (path.name, row[4])


def test_december_has_more_orders_per_day_than_july(order_records: list[dict]):
    per_day = Counter(order["created_at"][:10] for order in distinct_orders(order_records))
    december = [count for day, count in per_day.items() if day.startswith("2025-12")]
    july = [count for day, count in per_day.items() if day.startswith("2026-07")]
    assert len(december) == 31
    assert len(july) == 31
    assert sum(december) / len(december) > sum(july) / len(july)


@pytest.mark.parametrize("name", ["shop/products.json", "shop/orders.jsonl"])
def test_expected_files_exist(sources_dir: Path, name: str):
    assert (sources_dir / name).is_file()
