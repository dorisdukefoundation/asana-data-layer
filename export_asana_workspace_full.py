#!/usr/bin/env python3
import csv
import json
import os
import socket
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional


API_BASE = "https://app.asana.com/api/1.0"
WORKSPACE_GID = os.environ.get("ASANA_WORKSPACE_GID", "1204848198937008")

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


class AsanaClient:
    def __init__(self, token: str) -> None:
        self.token = token

    def get_json(self, path_or_url: str, *, params: Optional[dict[str, Any]] = None) -> dict:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            url = path_or_url
            if params:
                url = f"{url}{'&' if '?' in url else '?'}{urllib.parse.urlencode(params)}"
        else:
            query = ""
            if params:
                query = "?" + urllib.parse.urlencode(params)
            url = f"{API_BASE}{path_or_url}{query}"

        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            },
        )
        attempts = 0
        while True:
            attempts += 1
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.load(resp)
            except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout) as exc:
                if attempts >= 5:
                    raise
                delay = min(2 ** attempts, 20)
                print(f"Retrying after error on {url}: {exc} (attempt {attempts})")
                time.sleep(delay)

    def paginate(self, path: str, *, params: Optional[dict[str, Any]] = None) -> list[dict]:
        payload = self.get_json(path, params=params)
        items = list(payload.get("data", []))
        next_page = payload.get("next_page")
        while next_page and next_page.get("uri"):
            payload = self.get_json(next_page["uri"])
            items.extend(payload.get("data", []))
            next_page = payload.get("next_page")
        return items


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def join_people(items: Iterable[dict]) -> str:
    return ";".join(item.get("name", "") for item in items if item.get("name"))


def join_people_gids(items: Iterable[dict]) -> str:
    return ";".join(item.get("gid", "") for item in items if item.get("gid"))


def flatten_goal(goal: dict, archived_source: bool, parent_goals: list[dict]) -> dict[str, str]:
    owner = goal.get("owner") or {}
    team = goal.get("team") or {}
    time_period = goal.get("time_period") or {}
    status = goal.get("current_status") or {}
    status_author = status.get("author") or {}
    metric = goal.get("metric") or {}
    return {
        "workspace_gid": WORKSPACE_GID,
        "goal_gid": stringify(goal.get("gid")),
        "goal_name": stringify(goal.get("name")),
        "resource_type": stringify(goal.get("resource_type")),
        "permalink_url": stringify(goal.get("permalink_url")),
        "archived": stringify(goal.get("archived")),
        "archived_query_source": stringify(archived_source),
        "created_at": stringify(goal.get("created_at")),
        "modified_at": stringify(goal.get("modified_at")),
        "due_on": stringify(goal.get("due_on")),
        "owner_gid": stringify(owner.get("gid")),
        "owner_name": stringify(owner.get("name")),
        "team_gid": stringify(team.get("gid")),
        "team_name": stringify(team.get("name")),
        "time_period_gid": stringify(time_period.get("gid")),
        "time_period_start_on": stringify(time_period.get("start_on")),
        "time_period_end_on": stringify(time_period.get("end_on")),
        "time_period_period": stringify(time_period.get("period")),
        "status_title": stringify(status.get("title")),
        "status_text": stringify(status.get("text")),
        "status_color": stringify(status.get("color")),
        "status_created_at": stringify(status.get("created_at")),
        "status_author_gid": stringify(status_author.get("gid")),
        "status_author_name": stringify(status_author.get("name")),
        "metric_gid": stringify(metric.get("gid")),
        "metric_initial_number_value": stringify(metric.get("initial_number_value")),
        "metric_current_number_value": stringify(metric.get("current_number_value")),
        "metric_target_number_value": stringify(metric.get("target_number_value")),
        "metric_unit": stringify(metric.get("unit")),
        "metric_currency_code": stringify(metric.get("currency_code")),
        "metric_precision": stringify(metric.get("precision")),
        "parent_goal_gids": ";".join(parent.get("gid", "") for parent in parent_goals),
        "parent_goal_names": ";".join(parent.get("name", "") for parent in parent_goals),
        "notes": stringify(goal.get("notes")),
        "html_notes": stringify(goal.get("html_notes")),
        "raw_goal_json": json.dumps(goal, ensure_ascii=False, sort_keys=True),
    }


def flatten_project(project: dict, archived_source: bool) -> dict[str, str]:
    owner = project.get("owner") or {}
    team = project.get("team") or {}
    status = project.get("current_status") or {}
    status_author = status.get("author") or {}
    members = project.get("members") or []
    followers = project.get("followers") or []
    custom_field_settings = project.get("custom_field_settings") or []

    return {
        "workspace_gid": WORKSPACE_GID,
        "project_gid": stringify(project.get("gid")),
        "project_name": stringify(project.get("name")),
        "resource_type": stringify(project.get("resource_type")),
        "permalink_url": stringify(project.get("permalink_url")),
        "archived": stringify(project.get("archived")),
        "archived_query_source": stringify(archived_source),
        "completed": stringify(project.get("completed")),
        "completed_at": stringify(project.get("completed_at")),
        "created_at": stringify(project.get("created_at")),
        "modified_at": stringify(project.get("modified_at")),
        "start_on": stringify(project.get("start_on")),
        "due_on": stringify(project.get("due_on")),
        "due_date": stringify(project.get("due_date")),
        "color": stringify(project.get("color")),
        "public": stringify(project.get("public")),
        "default_view": stringify(project.get("default_view")),
        "layout": stringify(project.get("layout")),
        "owner_gid": stringify(owner.get("gid")),
        "owner_name": stringify(owner.get("name")),
        "team_gid": stringify(team.get("gid")),
        "team_name": stringify(team.get("name")),
        "status_title": stringify(status.get("title")),
        "status_text": stringify(status.get("text")),
        "status_color": stringify(status.get("color")),
        "status_created_at": stringify(status.get("created_at")),
        "status_author_gid": stringify(status_author.get("gid")),
        "status_author_name": stringify(status_author.get("name")),
        "member_gids": join_people_gids(members),
        "member_names": join_people(members),
        "follower_gids": join_people_gids(followers),
        "follower_names": join_people(followers),
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
        "notes": stringify(project.get("notes")),
        "html_notes": stringify(project.get("html_notes")),
        "raw_project_json": json.dumps(project, ensure_ascii=False, sort_keys=True),
    }


def dedupe_by_gid(items: Iterable[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for item in items:
        gid = item.get("gid")
        if gid:
            seen[gid] = item
    return list(seen.values())


def export_goals(client: AsanaClient, output_path: Path) -> int:
    listed = []
    for archived in (False, True):
        listed.extend(
            client.paginate(
                "/goals",
                params={
                    "workspace": WORKSPACE_GID,
                    "limit": 100,
                    "archived": str(archived).lower(),
                },
            )
        )

    goal_refs = dedupe_by_gid(listed)
    rows = []
    for index, goal_ref in enumerate(goal_refs, start=1):
        goal = client.get_json(
            f"/goals/{goal_ref['gid']}",
            params={"opt_fields": GOAL_FIELDS},
        )["data"]
        parent_goals = client.get_json(
            f"/goals/{goal_ref['gid']}/parentGoals",
            params={"opt_fields": "gid,name"},
        ).get("data", [])
        rows.append(flatten_goal(goal, bool(goal.get("archived")), parent_goals))
        if index % 25 == 0 or index == len(goal_refs):
            print(f"Goals: {index}/{len(goal_refs)}")
        if index < len(goal_refs):
            time.sleep(0.12)

    fieldnames = list(rows[0].keys()) if rows else []
    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def export_projects(client: AsanaClient, output_path: Path) -> int:
    listed = []
    for archived in (False, True):
        listed.extend(
            client.paginate(
                f"/workspaces/{WORKSPACE_GID}/projects",
                params={
                    "limit": 100,
                    "archived": str(archived).lower(),
                    "opt_fields": PROJECT_FIELDS,
                },
            )
        )

    project_refs = dedupe_by_gid(listed)
    rows = []
    for index, project in enumerate(project_refs, start=1):
        rows.append(flatten_project(project, bool(project.get("archived"))))
        if index % 25 == 0 or index == len(project_refs):
            print(f"Projects: {index}/{len(project_refs)}")

    fieldnames = list(rows[0].keys()) if rows else []
    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    token = os.environ.get("ASANA_ACCESS_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set ASANA_ACCESS_TOKEN before running this script.")

    today = date.today().isoformat()
    goals_output = Path(f"asana_workspace_goals_{today}.csv")
    projects_output = Path(f"asana_workspace_projects_{today}.csv")

    client = AsanaClient(token)
    export_kind = os.environ.get("ASANA_EXPORT_KIND", "both").strip().lower()

    goal_count = 0
    project_count = 0
    if export_kind in {"both", "goals"}:
        goal_count = export_goals(client, goals_output)
    if export_kind in {"both", "projects"}:
        project_count = export_projects(client, projects_output)

    print(
        json.dumps(
            {
                "workspace_gid": WORKSPACE_GID,
                "goals_output": str(goals_output),
                "goal_count": goal_count,
                "projects_output": str(projects_output),
                "project_count": project_count,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
