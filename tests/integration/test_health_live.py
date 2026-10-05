"""Runs the real app (with lifespan) against the compose infrastructure.

docker compose up -d
uv run pytest -m integration
"""

import pytest
from fastapi.testclient import TestClient

from app.api.main import create_app

pytestmark = pytest.mark.integration


def test_health_against_real_infrastructure():
    with TestClient(create_app()) as client:  # context manager runs the lifespan
        resp = client.get("/health")

    assert resp.status_code == 200, resp.json()
    assert resp.json()["status"] == "ok"
