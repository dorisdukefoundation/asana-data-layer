#!/usr/bin/env python3
import csv
import datetime as dt
import hashlib
import hmac
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response


ASANA_API_BASE = "https://app.asana.com/api/1.0"
AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_META_API_BASE = "https://api.airtable.com/v0/meta"

DEFAULT_ASANA_GOAL_FIELDS = ",".join(
    [
        "gid",
        "name",
        "resource_type",
        "permalink_url",
        "archived",
        "created_at",
        "notes",
        "html_notes",
        "due_on",
        "owner.gid",
        "owner.name",
        "owner.email",
        "team.gid",
        "team.name",
        "time_period.start_on",
        "time_period.end_on",
        "time_period.period",
        "current_status.title",
        "current_status.text",
        "current_status.color",
        "current_status.created_at",
        "current_status.author.name",
        "metric.initial_number_value",
        "metric.current_number_value",
        "metric.target_number_value",
        "metric.unit",
        "metric.currency_code",
        "metric.precision",
    ]
)


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    asana_access_token: str
    airtable_token: str
    airtable_base_id: str
    airtable_table_name: str
    airtable_merge_field: str
    public_base_url: str
    webhook_secret_store: Path
    csv_path: Path
    asana_goal_fields: str
    admin_api_key: Optional[str]
    filter_to_existing_airtable_fields: bool

    @classmethod
    def from_env(cls) -> "Settings":
        asana_access_token = os.environ.get("ASANA_ACCESS_TOKEN", "").strip()
        airtable_token = os.environ.get("AIRTABLE_TOKEN", "").strip()
        public_base_url = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")

        if not asana_access_token:
            raise RuntimeError("Missing ASANA_ACCESS_TOKEN")
        if not airtable_token:
            raise RuntimeError("Missing AIRTABLE_TOKEN")
        if not public_base_url:
            raise RuntimeError("Missing PUBLIC_BASE_URL")

        return cls(
            asana_access_token=asana_access_token,
            airtable_token=airtable_token,
            airtable_base_id=os.environ.get("AIRTABLE_BASE_ID", "app7STgvpfUa3K20Y").strip(),
            airtable_table_name=os.environ.get("AIRTABLE_TABLE_NAME", "Asana Goals").strip(),
            airtable_merge_field=os.environ.get("AIRTABLE_MERGE_FIELD", "Asana Goal GID").strip(),
            public_base_url=public_base_url,
            webhook_secret_store=Path(
                os.environ.get("WEBHOOK_SECRET_STORE", "webhook_secrets.json")
            ),
            csv_path=Path(
                os.environ.get("GOALS_CSV_PATH", "goals_export_2026-04-13.csv")
            ),
            asana_goal_fields=os.environ.get(
                "ASANA_GOAL_OPT_FIELDS", DEFAULT_ASANA_GOAL_FIELDS
            ).strip(),
            admin_api_key=os.environ.get("ADMIN_API_KEY", "").strip() or None,
            filter_to_existing_airtable_fields=env_bool(
                "AIRTABLE_FILTER_TO_EXISTING_FIELDS", True
            ),
        )


class SecretStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _read(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        with self.path.open(encoding="utf-8") as infile:
            return json.load(infile)

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            return self._read().get(key)

    def set(self, key: str, value: str) -> None:
        with self._lock:
            payload = self._read()
            payload[key] = value
            with self.path.open("w", encoding="utf-8") as outfile:
                json.dump(payload, outfile, indent=2, sort_keys=True)


class AsanaClient:
    def __init__(self, token: str, goal_fields: str) -> None:
        self.token = token
        self.goal_fields = goal_fields

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params)

        body = None
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        if data is not None:
            body = json.dumps({"data": data}).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(
            f"{ASANA_API_BASE}{path}{query}",
            data=body,
            headers=headers,
            method=method,
        )
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)

    def get_goal(self, goal_gid: str) -> dict[str, Any]:
        payload = self._request(
            "GET",
            f"/goals/{goal_gid}",
            params={"opt_fields": self.goal_fields},
        )
        return payload["data"]

    def get_parent_goals(self, goal_gid: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"/goals/{goal_gid}/parentGoals",
            params={"opt_fields": "gid,name"},
        )
        return payload.get("data", [])

    def create_webhook(self, resource_gid: str, target: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/webhooks",
            data={
                "resource": resource_gid,
                "target": target,
                "filters": [
                    {"resource_type": "goal", "action": action}
                    for action in ("added", "removed", "deleted", "undeleted", "changed")
                ],
            },
        )


class AirtableClient:
    def __init__(self, token: str, base_id: str, table_name: str) -> None:
        self.token = token
        self.base_id = base_id
        self.table_name = table_name

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(
            path,
            data=body,
            headers=headers,
            method=method,
        )
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)

    def get_existing_field_names(self) -> Optional[set[str]]:
        path = f"{AIRTABLE_META_API_BASE}/bases/{self.base_id}/tables"
        try:
            payload = self._request("GET", path)
        except urllib.error.HTTPError:
            return None

        for table in payload.get("tables", []):
            if table.get("name") == self.table_name:
                return {field["name"] for field in table.get("fields", [])}
        return None

    def upsert_records(
        self,
        records: list[dict[str, Any]],
        merge_field: str,
    ) -> dict[str, Any]:
        table = urllib.parse.quote(self.table_name, safe="")
        path = f"{AIRTABLE_API_BASE}/{self.base_id}/{table}"
        payload = {
            "performUpsert": {"fieldsToMergeOn": [merge_field]},
            "typecast": True,
            "records": [{"fields": fields} for fields in records],
        }
        return self._request("PATCH", path, data=payload)


settings = Settings.from_env()
secret_store = SecretStore(settings.webhook_secret_store)
asana_client = AsanaClient(settings.asana_access_token, settings.asana_goal_fields)
airtable_client = AirtableClient(
    settings.airtable_token,
    settings.airtable_base_id,
    settings.airtable_table_name,
)
app = FastAPI(title="Asana Goals Sync API")


def require_admin_key(x_admin_key: Optional[str]) -> None:
    if settings.admin_api_key and x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")


def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    expected = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def maybe_filter_fields(fields: dict[str, Any]) -> dict[str, Any]:
    if not settings.filter_to_existing_airtable_fields:
        return fields

    existing = airtable_client.get_existing_field_names()
    if not existing:
        return fields
    return {key: value for key, value in fields.items() if key in existing}


def stringify_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, sort_keys=True)


def goal_to_airtable_fields(
    goal: dict[str, Any],
    parent_goals: list[dict[str, Any]],
    event: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    owner = goal.get("owner") or {}
    team = goal.get("team") or {}
    metric = goal.get("metric") or {}
    time_period = goal.get("time_period") or {}
    current_status = goal.get("current_status") or {}
    status_author = current_status.get("author") or {}

    fields: dict[str, Any] = {
        settings.airtable_merge_field: goal.get("gid"),
        "Goal Name": goal.get("name"),
        "Resource Type": goal.get("resource_type"),
        "Permalink URL": goal.get("permalink_url"),
        "Archived": bool(goal.get("archived")),
        "Created At": goal.get("created_at"),
        "Due On": goal.get("due_on"),
        "Notes": goal.get("notes"),
        "HTML Notes": goal.get("html_notes"),
        "Owner GID": owner.get("gid"),
        "Owner Name": owner.get("name"),
        "Owner Email": owner.get("email"),
        "Team GID": team.get("gid"),
        "Team Name": team.get("name"),
        "Time Period Start On": time_period.get("start_on"),
        "Time Period End On": time_period.get("end_on"),
        "Time Period": time_period.get("period"),
        "Status Title": current_status.get("title"),
        "Status Text": current_status.get("text"),
        "Status Color": current_status.get("color"),
        "Status Created At": current_status.get("created_at"),
        "Status Author": status_author.get("name"),
        "Metric Initial Value": metric.get("initial_number_value"),
        "Metric Current Value": metric.get("current_number_value"),
        "Metric Target Value": metric.get("target_number_value"),
        "Metric Unit": metric.get("unit"),
        "Metric Currency Code": metric.get("currency_code"),
        "Metric Precision": metric.get("precision"),
        "Parent Goal GIDs": ";".join(parent["gid"] for parent in parent_goals),
        "Parent Goal Names": ";".join(parent["name"] for parent in parent_goals),
        "Last Synced At": event.get("received_at") if event else None,
        "Last Event Action": event.get("action") if event else None,
        "Last Event At": event.get("created_at") if event else None,
        "Raw Goal JSON": stringify_json(goal),
    }
    return {key: value for key, value in fields.items() if value is not None}


def deleted_goal_fields(goal_gid: str, event: dict[str, Any]) -> dict[str, Any]:
    resource = event.get("resource") or {}
    fields = {
        settings.airtable_merge_field: goal_gid,
        "Goal Name": resource.get("name"),
        "Deleted": True,
        "Last Event Action": event.get("action"),
        "Last Event At": event.get("created_at"),
        "Last Synced At": event.get("received_at"),
        "Raw Goal JSON": stringify_json(event),
    }
    return {key: value for key, value in fields.items() if value is not None}


def sync_goal(goal_gid: str, event: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if event is None:
        event = {"received_at": dt.datetime.now(dt.timezone.utc).isoformat()}

    try:
        goal = asana_client.get_goal(goal_gid)
        parent_goals = asana_client.get_parent_goals(goal_gid)
        fields = goal_to_airtable_fields(goal, parent_goals, event)
    except urllib.error.HTTPError as exc:
        if exc.code != 404 or event is None:
            raise
        fields = deleted_goal_fields(goal_gid, event)

    filtered_fields = maybe_filter_fields(fields)
    if settings.airtable_merge_field not in filtered_fields:
        raise RuntimeError(
            f"Required merge field {settings.airtable_merge_field!r} is not present in Airtable."
        )

    result = airtable_client.upsert_records([filtered_fields], settings.airtable_merge_field)
    return {
        "goal_gid": goal_gid,
        "airtable_result": result,
        "field_count": len(filtered_fields),
    }


def sync_goals_from_csv(csv_path: Path) -> dict[str, Any]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    results = []
    with csv_path.open(newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            goal_gid = (row.get("goal_gid") or "").strip()
            if not goal_gid:
                continue
            results.append(sync_goal(goal_gid))
    return {"synced": len(results), "results": results}


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhooks/asana/{goal_gid}")
async def receive_asana_webhook(
    goal_gid: str,
    request: Request,
    x_hook_secret: Optional[str] = Header(default=None),
    x_hook_signature: Optional[str] = Header(default=None),
) -> Response:
    body = await request.body()

    if x_hook_secret:
        secret_store.set(goal_gid, x_hook_secret)
        return Response(status_code=204, headers={"X-Hook-Secret": x_hook_secret})

    stored_secret = secret_store.get(goal_gid)
    if not stored_secret:
        raise HTTPException(status_code=412, detail="No stored webhook secret for goal")
    if not x_hook_signature or not verify_signature(body, x_hook_signature, stored_secret):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = json.loads(body.decode("utf-8") or "{}")
    events = payload.get("events", [])
    processed = []
    for event in events:
        resource = event.get("resource") or {}
        if resource.get("gid") and resource["gid"] != goal_gid:
            continue
        event["received_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        processed.append(sync_goal(goal_gid, event))

    return JSONResponse({"processed": len(processed), "results": processed})


@app.post("/admin/sync-goal/{goal_gid}")
def admin_sync_goal(
    goal_gid: str,
    x_admin_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    return sync_goal(goal_gid)


@app.post("/admin/sync-csv")
def admin_sync_csv(
    x_admin_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    return sync_goals_from_csv(settings.csv_path)


@app.get("/admin/config")
def admin_config(
    x_admin_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    return {
        "airtable_base_id": settings.airtable_base_id,
        "airtable_table_name": settings.airtable_table_name,
        "airtable_merge_field": settings.airtable_merge_field,
        "public_base_url": settings.public_base_url,
        "csv_path": str(settings.csv_path),
        "webhook_secret_store": str(settings.webhook_secret_store),
    }
