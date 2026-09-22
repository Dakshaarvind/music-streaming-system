"""Playlist Service.

Owns personal and collaborative playlists -- the main shared mutable
resource in the full design (target for later coordination/replication
milestones). For Milestone 1 it stores playlists in memory and calls the
Catalog Service to hydrate track names. If the catalog is unreachable it
degrades gracefully and returns track IDs only, flagging the response as
degraded rather than failing the request.
"""
import itertools
import os

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from common.http_client import ServiceClient
from common.logging_utils import RequestLoggingMiddleware, ServiceLogger

SERVICE_NAME = "playlist-service"
log = ServiceLogger(SERVICE_NAME)

app = FastAPI(title="Playlist Service")
app.add_middleware(RequestLoggingMiddleware, service_logger=log)

CATALOG_SERVICE_URL = os.environ.get("CATALOG_SERVICE_URL", "http://localhost:8003")
catalog_client = ServiceClient(log, peer_name="catalog-service", base_url=CATALOG_SERVICE_URL)

PLAYLISTS = {
    1: {"id": 1, "owner_id": 42, "name": "Focus Flow", "track_ids": [1, 3, 5], "editor_ids": [42]},
    2: {"id": 2, "owner_id": 42, "name": "Late Night Coding", "track_ids": [7, 8, 10], "editor_ids": [42]},
    3: {"id": 3, "owner_id": 42, "name": "Group Road Trip", "track_ids": [2, 4, 6, 9], "editor_ids": [42, 7, 13]},
    4: {"id": 4, "owner_id": 7, "name": "Morning Warmup", "track_ids": [1, 2], "editor_ids": [7]},
    5: {"id": 5, "owner_id": 7, "name": "Chill Set", "track_ids": [3, 6, 9], "editor_ids": [7]},
    6: {"id": 6, "owner_id": 13, "name": "Deep Work", "track_ids": [5, 8, 10], "editor_ids": [13]},
    7: {"id": 7, "owner_id": 13, "name": "Weekend Mix", "track_ids": [2, 4, 7], "editor_ids": [13]},
}
_id_counter = itertools.count(max(PLAYLISTS) + 1)


class CreatePlaylistRequest(BaseModel):
    owner_id: int
    name: str
    track_ids: list[int] = []


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/playlists")
async def list_playlists(owner_id: int = Query(...)):
    matches = [p for p in PLAYLISTS.values() if p["owner_id"] == owner_id]
    hydrated, degraded = await _hydrate_all(matches)
    return {"playlists": hydrated, "degraded": degraded}


@app.get("/playlists/{playlist_id}")
async def get_playlist(playlist_id: int):
    playlist = PLAYLISTS.get(playlist_id)
    if playlist is None:
        raise HTTPException(status_code=404, detail="Playlist not found")
    hydrated, degraded = await _hydrate_all([playlist])
    return {**hydrated[0], "degraded": degraded}


@app.post("/playlists", status_code=201)
async def create_playlist(body: CreatePlaylistRequest):
    playlist_id = next(_id_counter)
    playlist = {
        "id": playlist_id,
        "owner_id": body.owner_id,
        "name": body.name,
        "track_ids": body.track_ids,
        "editor_ids": [body.owner_id],
    }
    PLAYLISTS[playlist_id] = playlist
    hydrated, degraded = await _hydrate_all([playlist])
    return {**hydrated[0], "degraded": degraded}


async def _hydrate_all(playlists: list[dict]) -> tuple[list[dict], bool]:
    """Attach track metadata from the catalog. Falls back to bare IDs
    (with degraded=True) if the catalog service cannot be reached."""
    all_ids = sorted({tid for p in playlists for tid in p["track_ids"]})
    if not all_ids:
        return [{**p, "tracks": []} for p in playlists], False

    try:
        response = await catalog_client.get("/tracks", params={"ids": ",".join(map(str, all_ids))})
        response.raise_for_status()
        tracks_by_id = {t["id"]: t for t in response.json()["tracks"]}
        degraded = False
    except Exception:
        tracks_by_id = {}
        degraded = True

    hydrated = []
    for p in playlists:
        if degraded:
            tracks = [{"id": tid} for tid in p["track_ids"]]
        else:
            tracks = [tracks_by_id[tid] for tid in p["track_ids"] if tid in tracks_by_id]
        hydrated.append({**p, "tracks": tracks})

    return hydrated, degraded
