"""Shared JSON logging: one structured line per inbound/outbound message.

Every log line is a single JSON object written to stdout and to
logs/<service>.log, tagged with the request's correlation ID so that
scripts/trace.py can reconstruct a request's full path across services.
"""
import json
import logging
import os
import sys
import time
import uuid
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler

from starlette.middleware.base import BaseHTTPMiddleware

REQUEST_ID_HEADER = "X-Request-ID"

_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")


def get_current_request_id() -> str:
    return _request_id_ctx.get()


def _build_logger(service_name: str) -> logging.Logger:
    logger = logging.getLogger(f"dsms.{service_name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    os.makedirs("logs", exist_ok=True)

    formatter = logging.Formatter("%(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    file_handler = RotatingFileHandler(
        f"logs/{service_name}.log", maxBytes=5_000_000, backupCount=2
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


class ServiceLogger:
    """Wraps a stdlib logger and emits DS-flavored JSON log records."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._logger = _build_logger(service_name)

    def _emit(self, **fields):
        record = {
            "timestamp": _utc_now_iso(),
            "service": self.service_name,
            "request_id": get_current_request_id(),
            **fields,
        }
        self._logger.info(json.dumps(record))

    def recv_request(self, peer: str, method: str, path: str):
        self._emit(direction="RECV_REQUEST", peer=peer, method=method, path=path)

    def send_request(self, peer: str, method: str, path: str):
        self._emit(direction="SEND_REQUEST", peer=peer, method=method, path=path)

    def recv_response(
        self, peer: str, method: str, path: str, status: int, latency_ms: float
    ):
        self._emit(
            direction="RECV_RESPONSE",
            peer=peer,
            method=method,
            path=path,
            status=status,
            latency_ms=round(latency_ms, 2),
        )

    def send_response(
        self, peer: str, method: str, path: str, status: int, latency_ms: float
    ):
        self._emit(
            direction="SEND_RESPONSE",
            peer=peer,
            method=method,
            path=path,
            status=status,
            latency_ms=round(latency_ms, 2),
        )


def _utc_now_iso() -> str:
    now = time.time()
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now))
    millis = int((now % 1) * 1000)
    return f"{base}.{millis:03d}Z"


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Inbound logging: assigns/propagates the request ID, logs RECV/SEND."""

    def __init__(self, app, service_logger: ServiceLogger):
        super().__init__(app)
        self.log = service_logger

    async def dispatch(self, request, call_next):
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        token = _request_id_ctx.set(request_id)

        peer = request.headers.get("X-Peer-Service", "client")
        start = time.monotonic()
        self.log.recv_request(peer=peer, method=request.method, path=request.url.path)

        try:
            response = await call_next(request)
        finally:
            pass

        latency_ms = (time.monotonic() - start) * 1000
        response.headers[REQUEST_ID_HEADER] = request_id
        self.log.send_response(
            peer=peer,
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            latency_ms=latency_ms,
        )
        _request_id_ctx.reset(token)
        return response
