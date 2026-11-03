import json
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import pipeline.extract
import sources.app
from generator.generate import write_all
from pipeline.http import get_json
from pipeline.warehouse import connect

SEED = 42


def point_app_at(monkeypatch: pytest.MonkeyPatch, directory: Path) -> TestClient:
    """Makes the mock source API serve `directory` and returns a client for it."""
    monkeypatch.setattr(sources.app, "SOURCES_DIR", directory)
    monkeypatch.setattr(sources.app, "failure_rate", 0.0)
    sources.app.load_orders.cache_clear()
    sources.app.load_snapshots.cache_clear()
    return TestClient(sources.app.app)


@pytest.fixture(scope="session")
def sources_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One generated year of source data, shared (read-only) by the whole session."""
    directory = tmp_path_factory.mktemp("sources")
    write_all(directory, SEED)
    return directory


@pytest.fixture(scope="session")
def order_records(sources_dir: Path) -> list[dict]:
    """Every line of orders.jsonl, including the one duplicate line at the end."""
    with (sources_dir / "shop" / "orders.jsonl").open() as handle:
        return [json.loads(line) for line in handle]


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch, sources_dir: Path) -> Iterator[TestClient]:
    """httpx client for the mock APIs serving the session data."""
    client = point_app_at(monkeypatch, sources_dir)
    with client:
        yield client
    sources.app.load_orders.cache_clear()
    sources.app.load_snapshots.cache_clear()


@pytest.fixture
def api_for(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[Path], TestClient]]:
    """Factory that points the mock APIs at another sources directory."""
    clients: list[TestClient] = []

    def make(directory: Path) -> TestClient:
        client = point_app_at(monkeypatch, directory)
        clients.append(client)
        return client

    yield make
    for client in clients:
        client.close()
    sources.app.load_orders.cache_clear()
    sources.app.load_snapshots.cache_clear()


@pytest.fixture
def con(tmp_path: Path) -> Iterator:
    connection = connect(tmp_path / "warehouse.duckdb")
    yield connection
    connection.close()


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replaces the extractors' backoff sleep; returns the recorded delays.

    The default `sleep=time.sleep` is bound when get_json is defined, so patching time.sleep
    would not help; the extractors' reference to get_json is wrapped instead.
    """
    delays: list[float] = []

    def get_json_without_sleep(*args, **kwargs):
        return get_json(*args, **kwargs, sleep=delays.append)

    monkeypatch.setattr(pipeline.extract, "get_json", get_json_without_sleep)
    return delays


@pytest.fixture
def logistics_copy(sources_dir: Path, tmp_path: Path) -> Path:
    destination = tmp_path / "logistics"
    shutil.copytree(sources_dir / "logistics", destination)
    return destination
