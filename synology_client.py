import json
from datetime import datetime
from typing import Dict, Optional

import requests


class SynologyError(Exception):
    pass


def _request_json(method: str, url: str, **kwargs) -> dict:
    resp = requests.request(method, url, timeout=10, **kwargs)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success", False):
        raise SynologyError(data)
    return data


def discover_endpoints(base_url: str) -> Dict[str, dict]:
    info_url = f"{base_url}/webapi/query.cgi"
    data = _request_json(
        "GET",
        info_url,
        params={
            "api": "SYNO.API.Info",
            "version": 1,
            "method": "query",
            "query": "SYNO.API.Auth,SYNO.Cal.Event",
        },
    )
    return data.get("data", {})


def login(base_url: str, account: str, password: str, auth_path: str) -> str:
    auth_url = f"{base_url}{auth_path}"
    data = _request_json(
        "POST",
        auth_url,
        data={
            "api": "SYNO.API.Auth",
            "method": "login",
            "version": 3,
            "account": account,
            "passwd": password,
            "session": "Calendar",
            "format": "sid",
        },
    )
    return data["data"]["sid"]


def create_event(
    base_url: str,
    sid: str,
    event_path: str,
    calendar_id: str,
    title: str,
    description: str,
    start_dt: datetime,
    end_dt: datetime,
) -> dict:
    url = f"{base_url}{event_path}"
    event_payload = {
        "summary": title,
        "description": description,
        "start": {"time": start_dt.isoformat(), "tz": "UTC"},
        "end": {"time": end_dt.isoformat(), "tz": "UTC"},
        "all_day": False,
    }
    return _request_json(
        "POST",
        url,
        data={
            "api": "SYNO.Cal.Event",
            "method": "create",
            "version": 3,
            "calendar_id": calendar_id,
            "event": json.dumps(event_payload),
            "_sid": sid,
        },
    )


def push_synology_event(
    base_url: str,
    username: str,
    password: str,
    calendar_id: str,
    title: str,
    description: str,
    start_dt: datetime,
    end_dt: datetime,
) -> Optional[dict]:
    """High-level helper to create an event on Synology Calendar via REST."""
    endpoints = discover_endpoints(base_url)
    auth_info = endpoints.get("SYNO.API.Auth", {})
    event_info = endpoints.get("SYNO.Cal.Event", {})
    auth_path = auth_info.get("path", "/webapi/auth.cgi")
    event_path = event_info.get("path", "/webapi/entry.cgi")
    sid = login(base_url, username, password, auth_path)
    return create_event(base_url, sid, event_path, calendar_id, title, description, start_dt, end_dt)
