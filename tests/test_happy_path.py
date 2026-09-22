"""End-to-end happy-path tests through the gateway.

These are integration tests: they assume the full stack is already running
(via `docker compose up --build` or four local uvicorn processes) on the
default ports. See README.md for how to start the stack before running
pytest.
"""
import httpx
import pytest

GATEWAY_URL = "http://localhost:8000"


@pytest.fixture(autouse=True, scope="module")
def require_stack_running():
    try:
        httpx.get(f"{GATEWAY_URL}/health", timeout=2.0)
    except httpx.ConnectError:
        pytest.skip("Stack is not running. Start it with `docker compose up --build`.")


def test_all_services_healthy():
    for url in [
        "http://localhost:8000/health",
        "http://localhost:8001/health",
        "http://localhost:8002/health",
        "http://localhost:8003/health",
    ]:
        response = httpx.get(url, timeout=2.0)
        assert response.status_code == 200
        assert response.json()["status"] == "ok"


def test_get_playlists_for_existing_user():
    response = httpx.get(f"{GATEWAY_URL}/users/42/playlists", timeout=5.0)
    assert response.status_code == 200

    body = response.json()
    assert body["degraded"] is False
    assert len(body["playlists"]) >= 3

    playlist = body["playlists"][0]
    assert playlist["owner_id"] == 42
    assert "tracks" in playlist
    assert all("title" in t for t in playlist["tracks"])


def test_get_playlists_for_unknown_user_returns_404():
    response = httpx.get(f"{GATEWAY_URL}/users/9999/playlists", timeout=5.0)
    assert response.status_code == 404


def test_create_playlist_for_existing_user():
    response = httpx.post(
        f"{GATEWAY_URL}/users/42/playlists",
        json={"name": "Test Playlist", "track_ids": [1, 2]},
        timeout=5.0,
    )
    assert response.status_code == 201

    body = response.json()
    assert body["owner_id"] == 42
    assert body["name"] == "Test Playlist"
    assert len(body["tracks"]) == 2


def test_correlation_id_is_propagated():
    request_id = "test-fixed-correlation-id-0001"
    response = httpx.get(
        f"{GATEWAY_URL}/users/42/playlists",
        headers={"X-Request-ID": request_id},
        timeout=5.0,
    )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == request_id


def test_get_track_through_gateway():
    response = httpx.get(f"{GATEWAY_URL}/tracks/1", timeout=5.0)
    assert response.status_code == 200
    assert response.json()["id"] == 1
