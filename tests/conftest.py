import sys
from pathlib import Path

import fakeredis
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import storage, worker  # noqa: E402


@pytest.fixture
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def store(server):
    """Async store used by the API, backed by in-memory Redis."""
    return storage.JobStore(fakeredis.FakeAsyncRedis(server=server, decode_responses=True))


@pytest.fixture
def sync_redis(server, monkeypatch):
    """Sync client the Celery task uses; shares data with `store` through the same FakeServer."""
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    monkeypatch.setattr(worker, '_client', client)
    return client
