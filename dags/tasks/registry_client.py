"""
registry_client.py — Shared authenticated HTTP client for the STAMM Model
Registry API.

Every DAG task that talks to the Model Registry (predictions, sensor data,
experiment status) goes through here so there is exactly one place that
knows how to log in, cache the token, and retry on expiry.

Env vars:
    MODEL_REGISTRY_API_BASE
    MODEL_REGISTRY_TIMEOUT_SECONDS
    MODEL_REGISTRY_VERIFY_TLS
    MODEL_REGISTRY_SERVICE_EMAIL
    MODEL_REGISTRY_SERVICE_PASSWORD
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

API_BASE: str = os.getenv("MODEL_REGISTRY_API_BASE", "").rstrip("/")
TIMEOUT: float = float(os.getenv("MODEL_REGISTRY_TIMEOUT_SECONDS", "30"))
VERIFY_TLS: bool = os.getenv("MODEL_REGISTRY_VERIFY_TLS", "true").lower() == "true"

SERVICE_EMAIL: str = os.getenv("MODEL_REGISTRY_SERVICE_EMAIL", "")
SERVICE_PASSWORD: str = os.getenv("MODEL_REGISTRY_SERVICE_PASSWORD", "")

_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": 0.0}


def _login() -> Optional[str]:
    if not SERVICE_EMAIL or not SERVICE_PASSWORD:
        log.warning("No MODEL_REGISTRY_SERVICE_EMAIL/PASSWORD configured — calling API unauthenticated.")
        return None

    url = f"{API_BASE}/auth/login-json"
    try:
        resp = requests.post(
            url,
            json={"email": SERVICE_EMAIL, "password": SERVICE_PASSWORD},
            timeout=TIMEOUT,
            verify=VERIFY_TLS,
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["access_token"] = data["access_token"]
        # Refresh a bit before actual expiry (access tokens live 60 min server-side).
        _token_cache["expires_at"] = time.time() + 50 * 60
        return _token_cache["access_token"]
    except Exception as exc:
        log.error(f"[auth] login failed @ {url}: {exc}")
        return None


def auth_headers() -> Dict[str, str]:
    """Return {"Authorization": "Bearer ..."} , logging in (or refreshing) as needed."""
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"]:
        return {"Authorization": f"Bearer {_token_cache['access_token']}"}

    token = _login()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _url(path: str) -> str:
    return f"{API_BASE}/{path.lstrip('/')}"


def get(path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    return requests.get(_url(path), headers=auth_headers(), params=params, timeout=TIMEOUT, verify=VERIFY_TLS)


def post(path: str, json_body: Optional[Dict[str, Any]] = None) -> requests.Response:
    headers = {"Content-Type": "application/json", **auth_headers()}
    return requests.post(_url(path), headers=headers, json=json_body, timeout=TIMEOUT, verify=VERIFY_TLS)


def get_all_pages(path: str, params: Optional[Dict[str, Any]] = None, page_size: int = 1000) -> List[Dict[str, Any]]:
    """Page through a CRUD list endpoint (offset/limit only, no server-side filtering)."""
    out: List[Dict[str, Any]] = []
    offset = 0
    base_params = dict(params or {})
    while True:
        resp = get(path, params={**base_params, "offset": offset, "limit": page_size})
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out
