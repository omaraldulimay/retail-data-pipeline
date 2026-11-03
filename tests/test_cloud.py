import pytest
from azure.core.exceptions import ResourceNotFoundError

from pipeline import cloud


class FakeContainer:
    def __init__(self, blob: bytes | None = None):
        self.blob = blob
        self.uploads: list[tuple[str, bytes, bool]] = []

    def download_blob(self, name):
        if self.blob is None:
            raise ResourceNotFoundError("missing")
        data = self.blob
        return type("Downloader", (), {"readall": lambda self: data})()

    def upload_blob(self, name, data, overwrite=False):
        self.uploads.append((name, data.read(), overwrite))


@pytest.fixture
def fake(monkeypatch):
    container = FakeContainer(b"duckdb-bytes")
    monkeypatch.setattr(cloud, "_container", lambda: container)
    return container


def test_download_writes_blob(fake, tmp_path):
    target = tmp_path / "sub" / "warehouse.duckdb"
    assert cloud.download_warehouse(target) is True
    assert target.read_bytes() == b"duckdb-bytes"
    assert [p.name for p in target.parent.iterdir()] == ["warehouse.duckdb"]


def test_download_missing_blob(fake, tmp_path):
    fake.blob = None
    target = tmp_path / "warehouse.duckdb"
    assert cloud.download_warehouse(target) is False
    assert list(tmp_path.iterdir()) == []


def test_upload_overwrites(fake, tmp_path):
    source = tmp_path / "warehouse.duckdb"
    source.write_bytes(b"new-data")
    cloud.upload_warehouse(source)
    assert fake.uploads == [("warehouse.duckdb", b"new-data", True)]
