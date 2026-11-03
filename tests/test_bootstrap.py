from pathlib import Path

import duckdb
import pytest

from pipeline import bootstrap


@pytest.mark.slow
def test_ensure_warehouse_builds_once(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse.duckdb"
    sources_dir = tmp_path / "sources"

    assert bootstrap.ensure_warehouse(warehouse, sources_dir) is True
    assert not warehouse.with_name("warehouse.duckdb.building").exists()
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        assert con.execute("select count(*) from marts.agg_daily_sales").fetchone()[0] > 300
        assert con.execute("select count(*) from marts.rec_reorder").fetchone() is not None
    finally:
        con.close()

    # A second call finds the finished warehouse and does nothing.
    assert bootstrap.ensure_warehouse(warehouse, sources_dir) is False


def test_bootstrap_enabled_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEMO_BOOTSTRAP", raising=False)
    assert bootstrap.bootstrap_enabled() is False
    monkeypatch.setenv("DEMO_BOOTSTRAP", "1")
    assert bootstrap.bootstrap_enabled() is True
