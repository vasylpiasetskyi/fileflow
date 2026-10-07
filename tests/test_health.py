"""/health unit tests: real FastAPI app, fake dependency probes.

httpx.ASGITransport does not run the lifespan, so no real connections are
opened; the probes dependency is overridden instead.
"""

import asyncio

import httpx
import pytest

from app.api import deps
from app.api.main import create_app
from app.api.routes import health


async def _up() -> None:
    return None


async def _down() -> None:
    raise ConnectionRefusedError("connection refused")


async def _hang() -> None:
    await asyncio.sleep(60)


def _client(probes: dict) -> httpx.AsyncClient:
    app = create_app()
    app.dependency_overrides[deps.get_health_probes] = lambda: probes
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_all_up_returns_200():
    async with _client({"postgres": _up, "minio": _up, "kafka": _up}) as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert {name: c["status"] for name, c in body["checks"].items()} == {
        "postgres": "up",
        "minio": "up",
        "kafka": "up",
    }


async def test_one_down_returns_503_and_names_the_culprit():
    async with _client({"postgres": _down, "minio": _up, "kafka": _up}) as client:
        resp = await client.get("/health")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"]["status"] == "down"
    assert "ConnectionRefusedError" in body["checks"]["postgres"]["error"]
    assert body["checks"]["minio"]["status"] == "up"


async def test_hung_probe_times_out(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(health, "PROBE_TIMEOUT_S", 0.1)
    async with _client({"postgres": _up, "minio": _hang, "kafka": _up}) as client:
        resp = await client.get("/health")

    assert resp.status_code == 503
    assert resp.json()["checks"]["minio"]["error"] == "TimeoutError"


async def test_probes_run_concurrently(monkeypatch: pytest.MonkeyPatch):
    async def slow() -> None:
        await asyncio.sleep(0.2)

    async with _client({"a": slow, "b": slow, "c": slow}) as client:
        loop = asyncio.get_running_loop()
        started = loop.time()
        resp = await client.get("/health")
        elapsed = loop.time() - started

    assert resp.status_code == 200
    # Sequential would be ~0.6s.
    assert elapsed < 0.45


async def test_liveness_does_not_touch_dependencies():
    # Probes that would hang must not matter: /health/live checks nothing.
    async with _client({"postgres": _hang, "minio": _hang, "kafka": _hang}) as client:
        resp = await client.get("/health/live")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
