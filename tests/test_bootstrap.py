import contextlib
import fcntl
import subprocess
from pathlib import Path

import duckdb
import pytest

from pipeline import bootstrap


@pytest.fixture(autouse=True)
def no_download(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests build locally unless they set a download URL themselves."""
    monkeypatch.setenv("DEMO_WAREHOUSE_URL", "")


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


def _stub_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skips generator and extract so only the dbt step is under test."""
    import generator.generate
    from pipeline import extract

    monkeypatch.setattr(generator.generate, "write_all", lambda *a, **k: None)
    for name in ("load_products", "load_orders", "load_stock_snapshots", "load_logistics_files"):
        monkeypatch.setattr(extract, name, lambda *a, **k: None)


def test_dbt_failure_includes_output_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_sources(monkeypatch)
    output = "\n".join(f"line {i}" for i in range(100))

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 2, stdout=output, stderr=None)

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)
    steps: list[str] = []
    with pytest.raises(RuntimeError) as err:
        bootstrap.ensure_warehouse(tmp_path / "w.duckdb", tmp_path / "src", steps.append)

    assert "line 99" in str(err.value)
    assert "line 60" in str(err.value)
    assert "line 59" not in str(err.value)
    assert steps == ["Quelldaten erzeugt.", "Daten geladen."]
    assert "[bootstrap] extract done" in capsys.readouterr().out
    assert not (tmp_path / "w.duckdb.building").exists()


def test_lock_wait_times_out(tmp_path: Path) -> None:
    warehouse = tmp_path / "w.duckdb"
    with warehouse.with_name("w.duckdb.lock").open("w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(TimeoutError):
            bootstrap.ensure_warehouse(warehouse, tmp_path / "src", lock_timeout=0.2)


def _small_warehouse(path: Path, rows: int) -> bytes:
    con = duckdb.connect(str(path))
    con.execute("create schema marts")
    con.execute("create table marts.fct_orders as select * from range(?) t(order_id)", [rows])
    con.close()
    return path.read_bytes()


def _serve(monkeypatch: pytest.MonkeyPatch, handler) -> None:
    """Routes the download through a MockTransport instead of the network."""

    @contextlib.contextmanager
    def stream(method: str, url: str, **kwargs: object):
        transport = bootstrap.httpx.MockTransport(handler)
        with (
            bootstrap.httpx.Client(transport=transport) as client,
            client.stream(method, url, **kwargs) as response,
        ):
            yield response

    monkeypatch.setattr(bootstrap.httpx, "stream", stream)


def test_download_success_skips_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _small_warehouse(tmp_path / "remote.duckdb", rows=5)
    monkeypatch.setenv("DEMO_WAREHOUSE_URL", "https://example.test/warehouse.duckdb")
    _serve(monkeypatch, lambda request: bootstrap.httpx.Response(200, content=payload))
    monkeypatch.setattr(bootstrap, "build_warehouse", lambda *a, **k: pytest.fail("built"))
    steps: list[str] = []

    warehouse = tmp_path / "w.duckdb"
    assert bootstrap.ensure_warehouse(warehouse, tmp_path / "src", steps.append) is True

    con = duckdb.connect(str(warehouse), read_only=True)
    assert con.execute("select count(*) from marts.fct_orders").fetchone()[0] == 5
    con.close()
    assert steps == ["Lade fertige Demo-Daten …"]
    assert not warehouse.with_name("w.duckdb.part").exists()


@pytest.mark.parametrize("case", ["http_error", "empty_table", "not_a_database"])
def test_download_failure_falls_back_to_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    payload = {
        "http_error": b"",
        "empty_table": _small_warehouse(tmp_path / "remote.duckdb", rows=0),
        "not_a_database": b"<html>not found</html>",
    }[case]
    status = 404 if case == "http_error" else 200
    monkeypatch.setenv("DEMO_WAREHOUSE_URL", "https://example.test/warehouse.duckdb")
    _serve(monkeypatch, lambda request: bootstrap.httpx.Response(status, content=payload))
    built: list[Path] = []
    monkeypatch.setattr(bootstrap, "build_warehouse", lambda wh, *a, **k: built.append(wh))
    steps: list[str] = []

    warehouse = tmp_path / "w.duckdb"
    assert bootstrap.ensure_warehouse(warehouse, tmp_path / "src", steps.append) is True

    assert built == [warehouse]
    assert steps[0] == "Lade fertige Demo-Daten …"
    assert steps[1].startswith("Download nicht möglich")
    assert not warehouse.with_name("w.duckdb.part").exists()
