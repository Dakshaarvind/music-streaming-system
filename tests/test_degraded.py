"""Fault-handling tests: these stop and restart real containers, so they
require the stack to be running via `docker compose up --build` (not
local uvicorn processes). See README.md for details.

- Stopping catalog-service should make playlist responses "degraded"
  (track IDs only) instead of failing.
- Stopping user-service should make the gateway return 503.
"""
import subprocess
import time

import httpx
import pytest

GATEWAY_URL = "http://localhost:8000"


@pytest.fixture(autouse=True, scope="module")
def require_stack_running():
    try:
        httpx.get(f"{GATEWAY_URL}/health", timeout=2.0)
    except httpx.ConnectError:
        pytest.skip("Stack is not running. Start it with `docker compose up --build`.")


def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def _wait_for_health(url: str, expect_up: bool, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=1.0)
            if expect_up and response.status_code == 200:
                return
        except httpx.TransportError:
            if not expect_up:
                return
        time.sleep(0.5)
    if expect_up:
        raise TimeoutError(f"{url} did not come back up in time")


@pytest.fixture
def catalog_service_down():
    _compose("stop", "catalog-service")
    _wait_for_health("http://localhost:8003/health", expect_up=False)
    try:
        yield
    finally:
        _compose("start", "catalog-service")
        _wait_for_health("http://localhost:8003/health", expect_up=True)


@pytest.fixture
def user_service_down():
    _compose("stop", "user-service")
    _wait_for_health("http://localhost:8001/health", expect_up=False)
    try:
        yield
    finally:
        _compose("start", "user-service")
        _wait_for_health("http://localhost:8001/health", expect_up=True)


def test_playlists_degrade_gracefully_when_catalog_is_down(catalog_service_down):
    response = httpx.get(f"{GATEWAY_URL}/users/42/playlists", timeout=10.0)
    assert response.status_code == 200

    body = response.json()
    assert body["degraded"] is True
    playlist = body["playlists"][0]
    assert all("title" not in t for t in playlist["tracks"])
    assert all("id" in t for t in playlist["tracks"])


def test_gateway_returns_503_when_user_service_is_down(user_service_down):
    response = httpx.get(f"{GATEWAY_URL}/users/42/playlists", timeout=10.0)
    assert response.status_code == 503
