"""Catalog Service.

Owns track/album/artist metadata. Read-heavy in the full design (would be
partitioned by track ID in a later milestone). For Milestone 1 it serves an
in-memory seed catalog and has no downstream dependencies.
"""
from fastapi import FastAPI, HTTPException

from common.logging_utils import RequestLoggingMiddleware, ServiceLogger

SERVICE_NAME = "catalog-service"
log = ServiceLogger(SERVICE_NAME)

app = FastAPI(title="Catalog Service")
app.add_middleware(RequestLoggingMiddleware, service_logger=log)

TRACKS = {
    1: {"id": 1, "title": "Midnight Drive", "artist": "The Wavelengths", "duration_sec": 214},
    2: {"id": 2, "title": "Static Bloom", "artist": "Nova Rush", "duration_sec": 187},
    3: {"id": 3, "title": "Glass Horizon", "artist": "Kite Season", "duration_sec": 231},
    4: {"id": 4, "title": "Paper Tides", "artist": "The Wavelengths", "duration_sec": 198},
    5: {"id": 5, "title": "Low Orbit", "artist": "Nova Rush", "duration_sec": 245},
    6: {"id": 6, "title": "Amber Static", "artist": "Kite Season", "duration_sec": 176},
    7: {"id": 7, "title": "Slow Code", "artist": "Deadlock", "duration_sec": 203},
    8: {"id": 8, "title": "Loopback", "artist": "Deadlock", "duration_sec": 220},
    9: {"id": 9, "title": "Quiet Signal", "artist": "The Wavelengths", "duration_sec": 192},
    10: {"id": 10, "title": "Fault Tolerant", "artist": "Deadlock", "duration_sec": 208},
}


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/tracks/{track_id}")
def get_track(track_id: int):
    track = TRACKS.get(track_id)
    if track is None:
        raise HTTPException(status_code=404, detail="Track not found")
    return track


@app.get("/tracks")
def get_tracks(ids: str):
    """Bulk lookup: ids is a comma-separated list, e.g. ids=1,2,3."""
    try:
        track_ids = [int(x) for x in ids.split(",") if x]
    except ValueError:
        raise HTTPException(status_code=400, detail="ids must be a comma-separated list of integers")

    return {"tracks": [TRACKS[tid] for tid in track_ids if tid in TRACKS]}
