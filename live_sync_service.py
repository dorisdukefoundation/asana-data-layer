#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import socket
import threading
import time
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

GOAL_FIELDS = ",".join(
    [
        "gid",
        "name",
        "resource_type",
        "permalink_url",
        "archived",
        "created_at",
        "modified_at",
        "due_on",
        "notes",
        "html_notes",
        "owner.gid",
        "owner.name",
        "team.gid",
        "team.name",
        "time_period.gid",
        "time_period.start_on",
        "time_period.end_on",
        "time_period.period",
        "current_status.title",
        "current_status.text",
        "current_status.color",
        "current_status.created_at",
        "current_status.author.gid",
        "current_status.author.name",
        "metric.gid",
        "metric.initial_number_value",
        "metric.current_number_value",
        "metric.target_number_value",
        "metric.unit",
        "metric.currency_code",
        "metric.precision",
    ]
)

PROJECT_FIELDS = ",".join(
    [
        "gid",
        "name",
        "resource_type",
        "permalink_url",
        "archived",
        "completed",
        "completed_at",
        "created_at",
        "modified_at",
        "start_on",
        "due_on",
        "due_date",
        "notes",
        "html_notes",
        "color",
        "public",
        "default_view",
        "layout",
        "owner.gid",
        "owner.name",
        "team.gid",
        "team.name",
        "current_status.title",
        "current_status.text",
        "current_status.color",
        "current_status.created_at",
        "current_status.author.gid",
        "current_status.author.name",
        "members.gid",
        "members.name",
        "followers.gid",
        "followers.name",
        "custom_field_settings.gid",
        "custom_field_settings.custom_field.gid",
        "custom_field_settings.custom_field.name",
    ]
)

TASK_FIELDS = ",".join(
    [
        "gid",
        "name",
        "resource_type",
        "resource_subtype",
        "permalink_url",
        "created_at",
        "modified_at",
        "completed",
        "completed_at",
        "due_on",
        "due_at",
        "start_on",
        "start_at",
        "assignee.gid",
        "assignee.name",
        "assignee_status",
        "parent.gid",
        "parent.name",
        "projects.gid",
        "projects.name",
        "memberships.project.gid",
        "memberships.project.name",
        "memberships.section.gid",
        "memberships.section.name",
        "tags.gid",
        "tags.name",
        "followers.gid",
        "followers.name",
        "workspace.gid",
        "workspace.name",
        "notes",
        "html_notes",
        "num_subtasks",
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
    workspace_gid: str
    public_base_url: str
    admin_api_key: Optional[str]
    webhook_secret_store: Path
    goals_table: str
    projects_table: str
    tasks_table: str
    enable_tasks: bool
    auto_create_tasks_table: bool

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
            airtable_base_id=os.environ.get("AIRTABLE_BASE_ID", "app3mkbiuKcaANHa7").strip(),
            workspace_gid=os.environ.get("ASANA_WORKSPACE_GID", "1204848198937008").strip(),
            public_base_url=public_base_url,
            admin_api_key=os.environ.get("ADMIN_API_KEY", "").strip() or None,
            webhook_secret_store=Path(
                os.environ.get("WEBHOOK_SECRET_STORE", "live_sync_webhook_secrets.json")
            ),
            goals_table=os.environ.get("AIRTABLE_GOALS_TABLE", "Asana Goals").strip(),
            projects_table=os.environ.get("AIRTABLE_PROJECTS_TABLE", "Asana Projects").strip(),
            tasks_table=os.environ.get("AIRTABLE_TASKS_TABLE", "Asana Tasks").strip(),
            enable_tasks=env_bool("ENABLE_TASKS_SYNC", True),
            auto_create_tasks_table=env_bool("AUTO_CREATE_TASKS_TABLE", True),
        )


class SecretStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def _load(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        with self.path.open(encoding="utf-8") as infile:
            return json.load(infile)

    def get(self, key: str) -> Optional[str]:
        with self.lock:
            return self._load().get(key)

    def set(self, key: str, value: str) -> None:
        with self.lock:
            payload = self._load()
            payload[key] = value
            with self.path.open("w", encoding="utf-8") as outfile:
                json.dump(payload, outfile, indent=2, sort_keys=True)


class AsanaClient:
    def __init__(self, token: str, workspace_gid: str) -> None:
        self.token = token
        self.workspace_gid = workspace_gid

    def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            url = path_or_url
            if params:
                url = f"{url}{'&' if '?' in url else '?'}{urllib.parse.urlencode(params)}"
        else:
            query = ""
            if params:
                query = "?" + urllib.parse.urlencode(params)
            url = f"{ASANA_API_BASE}{path_or_url}{query}"

        body = None
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        if data is not None:
            body = json.dumps({"data": data}).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        attempts = 0
        while True:
            attempts += 1
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.load(resp)
            except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout):
                if attempts >= 5:
                    raise
                time.sleep(min(2 ** attempts, 20))

    def paginate(self, path: str, *, params: Optional[dict[str, Any]] = None) -> list[dict]:
        payload = self._request("GET", path, params=params)
        data = list(payload.get("data", []))
        next_page = payload.get("next_page")
        while next_page and next_page.get("uri"):
            payload = self._request("GET", next_page["uri"])
            data.extend(payload.get("data", []))
            next_page = payload.get("next_page")
        return data

    def list_projects(self, archived: bool) -> list[dict]:
        return self.paginate(
            f"/workspaces/{self.workspace_gid}/projects",
            params={
                "limit": 100,
                "archived": str(archived).lower(),
                "opt_fields": PROJECT_FIELDS,
            },
        )

    def list_goals(self, archived: bool) -> list[dict]:
        return self.paginate(
            "/goals",
            params={
                "workspace": self.workspace_gid,
                "limit": 100,
                "archived": str(archived).lower(),
            },
        )

    def get_goal(self, goal_gid: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/goals/{goal_gid}",
            params={"opt_fields": GOAL_FIELDS},
        )["data"]

    def get_goal_parents(self, goal_gid: str) -> list[dict[str, Any]]:
        return self._request(
            "GET",
            f"/goals/{goal_gid}/parentGoals",
            params={"opt_fields": "gid,name"},
        ).get("data", [])

    def get_project(self, project_gid: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/projects/{project_gid}",
            params={"opt_fields": PROJECT_FIELDS},
        )["data"]

    def get_task(self, task_gid: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/tasks/{task_gid}",
            params={"opt_fields": TASK_FIELDS},
        )["data"]

    def list_tasks_for_project(self, project_gid: str) -> list[dict[str, Any]]:
        return self.paginate(
            f"/projects/{project_gid}/tasks",
            params={
                "limit": 100,
                "completed_since": "1970-01-01T00:00:00.000Z",
                "opt_fields": TASK_FIELDS,
            },
        )

    def create_webhook(self, resource_gid: str, target: str, filters: list[dict[str, str]]) -> dict:
        return self._request(
            "POST",
            "/webhooks",
            data={"resource": resource_gid, "target": target, "filters": filters},
        )["data"]


class AirtableClient:
    def __init__(self, token: str, base_id: str) -> None:
        self.token = token
        self.base_id = base_id
        self._schema_cache: Optional[dict[str, dict[str, Any]]] = None

    def _request(
        self,
        method: str,
        url: str,
        *,
        data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        body = None
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)

    def list_tables(self, refresh: bool = False) -> dict[str, dict[str, Any]]:
        if self._schema_cache is not None and not refresh:
            return self._schema_cache

        url = f"{AIRTABLE_META_API_BASE}/bases/{self.base_id}/tables"
        payload = self._request("GET", url)
        self._schema_cache = {table["name"]: table for table in payload.get("tables", [])}
        return self._schema_cache

    def get_field_map(self, table_name: str) -> Optional[dict[str, dict[str, Any]]]:
        table = self.list_tables().get(table_name)
        if not table:
            return None
        return {field["name"]: field for field in table.get("fields", [])}

    def create_table(self, table_name: str, fields: list[dict[str, Any]]) -> dict[str, Any]:
        url = f"{AIRTABLE_META_API_BASE}/bases/{self.base_id}/tables"
        payload = self._request("POST", url, data={"name": table_name, "fields": fields})
        self.list_tables(refresh=True)
        return payload

    def upsert_records(
        self,
        table_name: str,
        merge_field: str,
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        encoded_table = urllib.parse.quote(table_name, safe="")
        url = f"{AIRTABLE_API_BASE}/{self.base_id}/{encoded_table}"
        payload = {
            "performUpsert": {"fieldsToMergeOn": [merge_field]},
            "typecast": True,
            "records": [{"fields": row} for row in rows],
        }
        return self._request("PATCH", url, data=payload)

    def find_records_by_value(self, table_name: str, field_name: str, value: Any) -> list[dict[str, Any]]:
        encoded_table = urllib.parse.quote(table_name, safe="")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            formula = f"{{{field_name}}}={value}"
        else:
            escaped = str(value).replace("'", "\\'")
            formula = f"{{{field_name}}}='{escaped}'"
        url = (
            f"{AIRTABLE_API_BASE}/{self.base_id}/{encoded_table}?"
            + urllib.parse.urlencode({"filterByFormula": formula})
        )
        payload = self._request("GET", url)
        return payload.get("records", [])

    def delete_records(self, table_name: str, record_ids: list[str]) -> None:
        if not record_ids:
            return
        encoded_table = urllib.parse.quote(table_name, safe="")
        chunks = [record_ids[i : i + 10] for i in range(0, len(record_ids), 10)]
        for chunk in chunks:
            query = urllib.parse.urlencode([("records[]", rid) for rid in chunk])
            url = f"{AIRTABLE_API_BASE}/{self.base_id}/{encoded_table}?{query}"
            self._request("DELETE", url)


settings = Settings.from_env()
asana = AsanaClient(settings.asana_access_token, settings.workspace_gid)
airtable = AirtableClient(settings.airtable_token, settings.airtable_base_id)
secrets = SecretStore(settings.webhook_secret_store)
app = FastAPI(title="Asana Airtable Live Sync")


def require_admin_key(x_admin_key: Optional[str]) -> None:
    if settings.admin_api_key and x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")


def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def dedupe_by_gid(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for item in items:
        gid = item.get("gid")
        if gid:
            seen[gid] = item
    return list(seen.values())


def join_names(items: list[dict[str, Any]]) -> str:
    return ";".join(item.get("name", "") for item in items if item.get("name"))


def join_gids(items: list[dict[str, Any]]) -> str:
    return ";".join(item.get("gid", "") for item in items if item.get("gid"))


def maybe_task_table() -> None:
    if not settings.enable_tasks:
        return
    if airtable.get_field_map(settings.tasks_table):
        return
    if not settings.auto_create_tasks_table:
        return

    airtable.create_table(
        settings.tasks_table,
        [
            {"name": "task_gid", "type": "singleLineText"},
            {"name": "workspace_gid", "type": "singleLineText"},
            {"name": "task_name", "type": "multilineText"},
            {"name": "resource_type", "type": "singleLineText"},
            {"name": "resource_subtype", "type": "singleLineText"},
            {"name": "permalink_url", "type": "multilineText"},
            {"name": "completed", "type": "checkbox"},
            {"name": "completed_at", "type": "dateTime"},
            {"name": "created_at", "type": "dateTime"},
            {"name": "modified_at", "type": "dateTime"},
            {"name": "due_on", "type": "date"},
            {"name": "due_at", "type": "dateTime"},
            {"name": "start_on", "type": "date"},
            {"name": "start_at", "type": "dateTime"},
            {"name": "assignee_gid", "type": "singleLineText"},
            {"name": "assignee_name", "type": "singleLineText"},
            {"name": "assignee_status", "type": "singleLineText"},
            {"name": "parent_task_gid", "type": "singleLineText"},
            {"name": "parent_task_name", "type": "multilineText"},
            {"name": "project_gids", "type": "multilineText"},
            {"name": "project_names", "type": "multilineText"},
            {"name": "membership_project_gids", "type": "multilineText"},
            {"name": "membership_project_names", "type": "multilineText"},
            {"name": "membership_section_gids", "type": "multilineText"},
            {"name": "membership_section_names", "type": "multilineText"},
            {"name": "tag_gids", "type": "multilineText"},
            {"name": "tag_names", "type": "multilineText"},
            {"name": "follower_gids", "type": "multilineText"},
            {"name": "follower_names", "type": "multilineText"},
            {"name": "num_subtasks", "type": "number", "options": {"precision": 0}},
            {"name": "notes", "type": "multilineText"},
            {"name": "html_notes", "type": "multilineText"},
            {"name": "raw_task_json", "type": "multilineText"},
        ],
    )


def coerce_for_airtable(field_meta: dict[str, Any], value: Any) -> Any:
    if value is None or value == "":
        return None

    field_type = field_meta.get("type")
    if field_type == "checkbox":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "yes"}
    if field_type == "number":
        text = str(value).strip()
        if text == "":
            return None
        return float(text) if "." in text else int(text)
    if field_type in {"singleLineText", "multilineText", "singleSelect"}:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)
    return value


def filter_for_existing_fields(table_name: str, row: dict[str, Any]) -> dict[str, Any]:
    field_map = airtable.get_field_map(table_name)
    if not field_map:
        raise RuntimeError(f"Airtable table {table_name!r} not found")

    filtered: dict[str, Any] = {}
    for field_name, value in row.items():
        meta = field_map.get(field_name)
        if not meta:
            continue
        coerced = coerce_for_airtable(meta, value)
        if coerced is None:
            continue
        filtered[field_name] = coerced
    return filtered


def delete_from_airtable(table_name: str, merge_field: str, merge_value: Any) -> int:
    field_map = airtable.get_field_map(table_name) or {}
    field_meta = field_map.get(merge_field)
    value = merge_value
    if field_meta and field_meta.get("type") == "number" and isinstance(merge_value, str):
        value = float(merge_value) if "." in merge_value else int(merge_value)
    records = airtable.find_records_by_value(table_name, merge_field, value)
    record_ids = [record["id"] for record in records]
    airtable.delete_records(table_name, record_ids)
    return len(record_ids)


def sync_goal(goal_gid: str) -> dict[str, Any]:
    try:
        goal = asana.get_goal(goal_gid)
        parents = asana.get_goal_parents(goal_gid)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            deleted = delete_from_airtable(settings.goals_table, "goal_gid", goal_gid)
            return {"goal_gid": goal_gid, "deleted_records": deleted}
        raise

    owner = goal.get("owner") or {}
    team = goal.get("team") or {}
    time_period = goal.get("time_period") or {}
    status = goal.get("current_status") or {}
    status_author = status.get("author") or {}
    metric = goal.get("metric") or {}
    row = {
        "workspace_gid": settings.workspace_gid,
        "goal_gid": goal.get("gid"),
        "goal_name": goal.get("name"),
        "resource_type": goal.get("resource_type"),
        "permalink_url": goal.get("permalink_url"),
        "archived": goal.get("archived"),
        "archived_query_source": goal.get("archived"),
        "created_at": goal.get("created_at"),
        "modified_at": goal.get("modified_at"),
        "due_on": goal.get("due_on"),
        "owner_gid": owner.get("gid"),
        "owner_name": owner.get("name"),
        "team_gid": team.get("gid"),
        "team_name": team.get("name"),
        "time_period_gid": time_period.get("gid"),
        "time_period_start_on": time_period.get("start_on"),
        "time_period_end_on": time_period.get("end_on"),
        "time_period_period": time_period.get("period"),
        "status_title": status.get("title"),
        "status_text": status.get("text"),
        "status_color": status.get("color"),
        "status_created_at": status.get("created_at"),
        "status_author_gid": status_author.get("gid"),
        "status_author_name": status_author.get("name"),
        "metric_gid": metric.get("gid"),
        "metric_initial_number_value": metric.get("initial_number_value"),
        "metric_current_number_value": metric.get("current_number_value"),
        "metric_target_number_value": metric.get("target_number_value"),
        "metric_unit": metric.get("unit"),
        "metric_currency_code": metric.get("currency_code"),
        "metric_precision": metric.get("precision"),
        "parent_goal_gids": ";".join(parent.get("gid", "") for parent in parents),
        "parent_goal_names": ";".join(parent.get("name", "") for parent in parents),
        "notes": goal.get("notes"),
        "html_notes": goal.get("html_notes"),
        "raw_goal_json": json.dumps(goal, ensure_ascii=False, sort_keys=True),
    }
    filtered = filter_for_existing_fields(settings.goals_table, row)
    airtable.upsert_records(settings.goals_table, "goal_gid", [filtered])
    return {"goal_gid": goal_gid, "field_count": len(filtered)}


def sync_project(project_gid: str) -> dict[str, Any]:
    try:
        project = asana.get_project(project_gid)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            deleted = delete_from_airtable(settings.projects_table, "project_gid", project_gid)
            return {"project_gid": project_gid, "deleted_records": deleted}
        raise

    owner = project.get("owner") or {}
    team = project.get("team") or {}
    status = project.get("current_status") or {}
    status_author = status.get("author") or {}
    members = project.get("members") or []
    followers = project.get("followers") or []
    custom_field_settings = project.get("custom_field_settings") or []
    row = {
        "workspace_gid": settings.workspace_gid,
        "project_gid": project.get("gid"),
        "project_name": project.get("name"),
        "resource_type": project.get("resource_type"),
        "permalink_url": project.get("permalink_url"),
        "archived": project.get("archived"),
        "archived_query_source": project.get("archived"),
        "completed": project.get("completed"),
        "completed_at": project.get("completed_at"),
        "created_at": project.get("created_at"),
        "modified_at": project.get("modified_at"),
        "start_on": project.get("start_on"),
        "due_on": project.get("due_on"),
        "due_date": project.get("due_date"),
        "color": project.get("color"),
        "public": project.get("public"),
        "default_view": project.get("default_view"),
        "layout": project.get("layout"),
        "owner_gid": owner.get("gid"),
        "owner_name": owner.get("name"),
        "team_gid": team.get("gid"),
        "team_name": team.get("name"),
        "status_title": status.get("title"),
        "status_text": status.get("text"),
        "status_color": status.get("color"),
        "status_created_at": status.get("created_at"),
        "status_author_gid": status_author.get("gid"),
        "status_author_name": status_author.get("name"),
        "member_gids": join_gids(members),
        "member_names": join_names(members),
        "follower_gids": join_gids(followers),
        "follower_names": join_names(followers),
        "custom_field_setting_gids": ";".join(
            cfs.get("gid", "") for cfs in custom_field_settings if cfs.get("gid")
        ),
        "custom_field_gids": ";".join(
            (cfs.get("custom_field") or {}).get("gid", "")
            for cfs in custom_field_settings
            if (cfs.get("custom_field") or {}).get("gid")
        ),
        "custom_field_names": ";".join(
            (cfs.get("custom_field") or {}).get("name", "")
            for cfs in custom_field_settings
            if (cfs.get("custom_field") or {}).get("name")
        ),
        "notes": project.get("notes"),
        "html_notes": project.get("html_notes"),
        "raw_project_json": json.dumps(project, ensure_ascii=False, sort_keys=True),
    }
    filtered = filter_for_existing_fields(settings.projects_table, row)
    airtable.upsert_records(settings.projects_table, "project_gid", [filtered])
    return {"project_gid": project_gid, "field_count": len(filtered)}


def sync_task(task_gid: str) -> dict[str, Any]:
    maybe_task_table()
    if not settings.enable_tasks:
        return {"task_gid": task_gid, "skipped": True, "reason": "Tasks sync disabled"}

    try:
        task = asana.get_task(task_gid)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            deleted = delete_from_airtable(settings.tasks_table, "task_gid", task_gid)
            return {"task_gid": task_gid, "deleted_records": deleted}
        raise

    assignee = task.get("assignee") or {}
    parent = task.get("parent") or {}
    projects = task.get("projects") or []
    memberships = task.get("memberships") or []
    tags = task.get("tags") or []
    followers = task.get("followers") or []
    workspace = task.get("workspace") or {}
    membership_projects = [(membership.get("project") or {}) for membership in memberships]
    membership_sections = [(membership.get("section") or {}) for membership in memberships]
    row = {
        "workspace_gid": workspace.get("gid") or settings.workspace_gid,
        "task_gid": task.get("gid"),
        "task_name": task.get("name"),
        "resource_type": task.get("resource_type"),
        "resource_subtype": task.get("resource_subtype"),
        "permalink_url": task.get("permalink_url"),
        "completed": task.get("completed"),
        "completed_at": task.get("completed_at"),
        "created_at": task.get("created_at"),
        "modified_at": task.get("modified_at"),
        "due_on": task.get("due_on"),
        "due_at": task.get("due_at"),
        "start_on": task.get("start_on"),
        "start_at": task.get("start_at"),
        "assignee_gid": assignee.get("gid"),
        "assignee_name": assignee.get("name"),
        "assignee_status": task.get("assignee_status"),
        "parent_task_gid": parent.get("gid"),
        "parent_task_name": parent.get("name"),
        "project_gids": join_gids(projects),
        "project_names": join_names(projects),
        "membership_project_gids": join_gids(membership_projects),
        "membership_project_names": join_names(membership_projects),
        "membership_section_gids": join_gids(membership_sections),
        "membership_section_names": join_names(membership_sections),
        "tag_gids": join_gids(tags),
        "tag_names": join_names(tags),
        "follower_gids": join_gids(followers),
        "follower_names": join_names(followers),
        "num_subtasks": task.get("num_subtasks"),
        "notes": task.get("notes"),
        "html_notes": task.get("html_notes"),
        "raw_task_json": json.dumps(task, ensure_ascii=False, sort_keys=True),
    }
    filtered = filter_for_existing_fields(settings.tasks_table, row)
    airtable.upsert_records(settings.tasks_table, "task_gid", [filtered])
    return {"task_gid": task_gid, "field_count": len(filtered)}


WORKSPACE_WEBHOOK_FILTERS = [
    {"resource_type": "goal", "action": action}
    for action in ("added", "removed", "deleted", "undeleted", "changed")
] + [
    {"resource_type": "project", "action": action}
    for action in ("added", "removed", "deleted", "undeleted", "changed")
]

PROJECT_TASK_WEBHOOK_FILTERS = [
    {"resource_type": "task", "action": action}
    for action in ("added", "removed", "deleted", "undeleted", "changed")
]


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "workspace_gid": settings.workspace_gid,
        "base_id": settings.airtable_base_id,
        "enable_tasks": settings.enable_tasks,
    }


@app.post("/webhooks/asana/workspace")
async def asana_workspace_webhook(
    request: Request,
    x_hook_secret: Optional[str] = Header(default=None),
    x_hook_signature: Optional[str] = Header(default=None),
) -> Response:
    key = f"workspace:{settings.workspace_gid}"
    body = await request.body()

    if x_hook_secret:
        secrets.set(key, x_hook_secret)
        return Response(status_code=204, headers={"X-Hook-Secret": x_hook_secret})

    stored = secrets.get(key)
    if not stored or not x_hook_signature or not verify_signature(body, x_hook_signature, stored):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = json.loads(body.decode("utf-8") or "{}")
    results = []
    seen = set()
    for event in payload.get("events", []):
        resource = event.get("resource") or {}
        resource_gid = resource.get("gid")
        resource_type = resource.get("resource_type")
        if not resource_gid or (resource_type, resource_gid) in seen:
            continue
        seen.add((resource_type, resource_gid))
        if resource_type == "goal":
            results.append(sync_goal(resource_gid))
        elif resource_type == "project":
            results.append(sync_project(resource_gid))

    return JSONResponse({"processed": len(results), "results": results})


@app.post("/webhooks/asana/project/{project_gid}")
async def asana_project_webhook(
    project_gid: str,
    request: Request,
    x_hook_secret: Optional[str] = Header(default=None),
    x_hook_signature: Optional[str] = Header(default=None),
) -> Response:
    key = f"project:{project_gid}"
    body = await request.body()

    if x_hook_secret:
        secrets.set(key, x_hook_secret)
        return Response(status_code=204, headers={"X-Hook-Secret": x_hook_secret})

    stored = secrets.get(key)
    if not stored or not x_hook_signature or not verify_signature(body, x_hook_signature, stored):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    if not settings.enable_tasks:
        return JSONResponse({"processed": 0, "skipped": "Tasks sync disabled"})

    payload = json.loads(body.decode("utf-8") or "{}")
    results = []
    seen: set[str] = set()
    for event in payload.get("events", []):
        resource = event.get("resource") or {}
        if resource.get("resource_type") != "task":
            continue
        task_gid = resource.get("gid")
        if not task_gid or task_gid in seen:
            continue
        seen.add(task_gid)
        results.append(sync_task(task_gid))

    return JSONResponse({"processed": len(results), "results": results})


@app.post("/admin/bootstrap")
def bootstrap_sync(x_admin_key: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    maybe_task_table()
    workspace_webhook = asana.create_webhook(
        settings.workspace_gid,
        f"{settings.public_base_url}/webhooks/asana/workspace",
        WORKSPACE_WEBHOOK_FILTERS,
    )

    project_refs = dedupe_by_gid(asana.list_projects(archived=False) + asana.list_projects(archived=True))
    created_task_webhooks = []
    if settings.enable_tasks:
        for project in project_refs:
            created_task_webhooks.append(
                asana.create_webhook(
                    project["gid"],
                    f"{settings.public_base_url}/webhooks/asana/project/{project['gid']}",
                    PROJECT_TASK_WEBHOOK_FILTERS,
                )
            )
            time.sleep(0.03)

    return {
        "workspace_webhook_gid": workspace_webhook.get("gid"),
        "task_webhook_count": len(created_task_webhooks),
    }


@app.post("/admin/backfill/goals")
def backfill_goals(x_admin_key: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    goal_refs = dedupe_by_gid(asana.list_goals(False) + asana.list_goals(True))
    results = [sync_goal(goal["gid"]) for goal in goal_refs]
    return {"synced": len(results), "results": results}


@app.post("/admin/backfill/projects")
def backfill_projects(x_admin_key: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    project_refs = dedupe_by_gid(asana.list_projects(False) + asana.list_projects(True))
    results = [sync_project(project["gid"]) for project in project_refs]
    return {"synced": len(results), "results": results}


@app.post("/admin/backfill/tasks")
def backfill_tasks(
    project_gid: Optional[str] = None,
    x_admin_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    maybe_task_table()

    project_gids = [project_gid] if project_gid else [
        project["gid"] for project in dedupe_by_gid(asana.list_projects(False) + asana.list_projects(True))
    ]
    seen_tasks: set[str] = set()
    results = []
    for current_project_gid in project_gids:
        for task in asana.list_tasks_for_project(current_project_gid):
            task_gid = task.get("gid")
            if not task_gid or task_gid in seen_tasks:
                continue
            seen_tasks.add(task_gid)
            results.append(sync_task(task_gid))
    return {"synced": len(results), "results": results}


@app.post("/admin/backfill/all")
def backfill_all(x_admin_key: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    goals = backfill_goals(x_admin_key)
    projects = backfill_projects(x_admin_key)
    tasks = backfill_tasks(None, x_admin_key) if settings.enable_tasks else {"synced": 0}
    return {"goals": goals["synced"], "projects": projects["synced"], "tasks": tasks["synced"]}


@app.get("/admin/config")
def config(x_admin_key: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin_key(x_admin_key)
    maybe_task_table()
    return {
        "workspace_gid": settings.workspace_gid,
        "base_id": settings.airtable_base_id,
        "goals_table": settings.goals_table,
        "projects_table": settings.projects_table,
        "tasks_table": settings.tasks_table,
        "public_base_url": settings.public_base_url,
        "enable_tasks": settings.enable_tasks,
        "tables_present": list(airtable.list_tables().keys()),
    }
