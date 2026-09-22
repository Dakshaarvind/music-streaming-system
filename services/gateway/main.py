"""API Gateway.

Single entry point for clients. Assigns a correlation ID to requests that
don't already carry one, routes to the User/Playlist/Catalog services, and
logs every hop. This is the centerpiece of the Milestone 1 demo: a single
client request fans out across three backend services and returns a
combined response.
"""
import os

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common.http_client import ServiceClient
from common.logging_utils import RequestLoggingMiddleware, ServiceLogger

SERVICE_NAME = "gateway"
log = ServiceLogger(SERVICE_NAME)

app = FastAPI(title="API Gateway")
app.add_middleware(RequestLoggingMiddleware, service_logger=log)

USER_SERVICE_URL = os.environ.get("USER_SERVICE_URL", "http://localhost:8001")
PLAYLIST_SERVICE_URL = os.environ.get("PLAYLIST_SERVICE_URL", "http://localhost:8002")
CATALOG_SERVICE_URL = os.environ.get("CATALOG_SERVICE_URL", "http://localhost:8003")

user_client = ServiceClient(log, peer_name="user-service", base_url=USER_SERVICE_URL)
playlist_client = ServiceClient(log, peer_name="playlist-service", base_url=PLAYLIST_SERVICE_URL)
catalog_client = ServiceClient(log, peer_name="catalog-service", base_url=CATALOG_SERVICE_URL)


class CreatePlaylistBody(BaseModel):
    name: str
    track_ids: list[int] = []


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/users/{user_id}/playlists")
async def get_user_playlists(user_id: int):
    await _require_user_exists(user_id)

    try:
        response = await playlist_client.get("/playlists", params={"owner_id": user_id})
    except (httpx.ConnectError, httpx.TimeoutException):
        raise HTTPException(status_code=503, detail="playlist-service unavailable")

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail="playlist-service error")

    return response.json()


@app.post("/users/{user_id}/playlists", status_code=201)
async def create_user_playlist(user_id: int, body: CreatePlaylistBody):
    await _require_user_exists(user_id)

    try:
        response = await playlist_client.post(
            "/playlists",
            json={"owner_id": user_id, "name": body.name, "track_ids": body.track_ids},
        )
    except (httpx.ConnectError, httpx.TimeoutException):
        raise HTTPException(status_code=503, detail="playlist-service unavailable")

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail="playlist-service error")

    return response.json()


@app.get("/tracks/{track_id}")
async def get_track(track_id: int):
    try:
        response = await catalog_client.get(f"/tracks/{track_id}")
    except (httpx.ConnectError, httpx.TimeoutException):
        raise HTTPException(status_code=503, detail="catalog-service unavailable")

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Track not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail="catalog-service error")

    return response.json()


async def _require_user_exists(user_id: int) -> None:
    try:
        response = await user_client.get(f"/users/{user_id}")
    except (httpx.ConnectError, httpx.TimeoutException):
        raise HTTPException(status_code=503, detail="user-service unavailable")

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="User not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail="user-service error")
