"""Shared httpx wrapper for service-to-service calls.

Propagates the correlation ID, applies a bounded timeout with limited
retries on GET requests, and logs every outbound call (SEND_REQUEST /
RECV_RESPONSE) so the call shows up in the owning service's log file.
"""
import time

import httpx

from common.logging_utils import (
    REQUEST_ID_HEADER,
    ServiceLogger,
    get_current_request_id,
)

DEFAULT_TIMEOUT_SECONDS = 2.0
DEFAULT_GET_RETRIES = 2


class ServiceClient:
    """A named client for calling one downstream service."""

    def __init__(
        self,
        service_logger: ServiceLogger,
        peer_name: str,
        base_url: str,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        self.log = service_logger
        self.peer_name = peer_name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def get(self, path: str, params: dict | None = None) -> httpx.Response:
        return await self._request("GET", path, params=params, retries=DEFAULT_GET_RETRIES)

    async def post(self, path: str, json: dict | None = None) -> httpx.Response:
        # POST is not idempotent, so it is not retried automatically.
        return await self._request("POST", path, json_body=json, retries=0)

    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
        retries: int = 0,
    ) -> httpx.Response:
        url = f"{self.base_url}{path}"
        headers = {
            REQUEST_ID_HEADER: get_current_request_id(),
            "X-Peer-Service": _my_service_name(self.log),
        }

        attempt = 0
        last_exc: Exception | None = None

        while attempt <= retries:
            attempt += 1
            self.log.send_request(peer=self.peer_name, method=method, path=path)
            start = time.monotonic()
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(
                        method, url, params=params, json=json_body, headers=headers
                    )
                latency_ms = (time.monotonic() - start) * 1000
                self.log.recv_response(
                    peer=self.peer_name,
                    method=method,
                    path=path,
                    status=response.status_code,
                    latency_ms=latency_ms,
                )
                return response
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                last_exc = exc
                latency_ms = (time.monotonic() - start) * 1000
                self.log.recv_response(
                    peer=self.peer_name,
                    method=method,
                    path=path,
                    status=0,
                    latency_ms=latency_ms,
                )
                if attempt <= retries:
                    continue
                raise

        raise last_exc  # pragma: no cover - unreachable, satisfies type checkers


def _my_service_name(log: ServiceLogger) -> str:
    return log.service_name
