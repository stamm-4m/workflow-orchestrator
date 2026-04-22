"""
STAMM backend API client.

Single entry point used by every DAG to talk to the STAMM backend
(FastAPI + PostgreSQL). This is a stub: the backend is under active
development (see docs/dags.md). Methods raise NotImplementedError until
the matching endpoint is available.

Base URL is read from STAMM_API_BASE.
"""

from __future__ import annotations

import os
from typing import Any

import requests


class StammAPIError(RuntimeError):
    pass


class StammAPIClient:
    def __init__(self, base_url: str | None = None, timeout: float = 30.0):
        self.base_url = (base_url or os.environ["STAMM_API_BASE"]).rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path if path.startswith('/') else '/' + path}"

    def ping(self) -> bool:
        r = self._session.get(self._url("/health"), timeout=self.timeout)
        r.raise_for_status()
        return r.json().get("status") == "ok"

    def get_active_snapshots(self) -> list[dict[str, Any]]:
        raise NotImplementedError("GET /experiments/active/snapshots — pending backend")

    def get_experiment_models(self, experiment_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError("GET /experiments/{id}/models — pending backend")

    def post_predictions(self, experiment_id: str, predictions: list[dict[str, Any]]) -> None:
        raise NotImplementedError("POST /experiments/{id}/predictions — pending backend")
