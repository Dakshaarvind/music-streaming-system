"""User Service.

Owns account records: identity and subscription tier. For Milestone 1 it
only exposes a lookup used by the gateway to validate that a user exists
before fanning out to other services. Auth is out of scope for this
milestone.
"""
from fastapi import FastAPI, HTTPException

from common.logging_utils import RequestLoggingMiddleware, ServiceLogger

SERVICE_NAME = "user-service"
log = ServiceLogger(SERVICE_NAME)

app = FastAPI(title="User Service")
app.add_middleware(RequestLoggingMiddleware, service_logger=log)

USERS = {
    42: {"id": 42, "username": "daksha", "tier": "premium"},
    7: {"id": 7, "username": "alex", "tier": "free"},
    13: {"id": 13, "username": "morgan", "tier": "premium"},
    99: {"id": 99, "username": "sam", "tier": "free"},
}


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/users/{user_id}")
def get_user(user_id: int):
    user = USERS.get(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user
