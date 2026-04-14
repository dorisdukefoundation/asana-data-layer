#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


API_BASE = "https://app.asana.com/api/1.0"
DEFAULT_GOAL_FIELDS = ",".join(
    [
        "gid",
        "name",
        "resource_type",
        "permalink_url",
        "archived",
        "created_at",
        "modified_at",
        "notes",
        "html_notes",
        "due_on",
        "owner.gid",
        "owner.name",
        "team.gid",
        "team.name",
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


def asana_get(path: str, token: str) -> dict:
    req = urllib.request.Request(
        f"{API_BASE}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def fetch_goal(goal_gid: str, token: str) -> dict:
    query = urllib.parse.urlencode({"opt_fields": DEFAULT_GOAL_FIELDS})
    data = asana_get(f"/goals/{goal_gid}?{query}", token)
    return data.get("data", {})


def fetch_parent_goals(goal_gid: str, token: str) -> list[dict]:
    query = urllib.parse.urlencode({"opt_fields": "gid,name"})
    data = asana_get(f"/goals/{goal_gid}/parentGoals?{query}", token)
    return data.get("data", [])


def flatten_goal(goal: dict, parent_goals: list[dict]) -> dict[str, str]:
    owner = goal.get("owner") or {}
    team = goal.get("team") or {}
    time_period = goal.get("time_period") or {}
    current_status = goal.get("current_status") or {}
    status_author = current_status.get("author") or {}
    metric = goal.get("metric") or {}

    return {
        "goal_gid": goal.get("gid", ""),
        "goal_name": goal.get("name", ""),
        "resource_type": goal.get("resource_type", ""),
        "permalink_url": goal.get("permalink_url", ""),
        "archived": str(goal.get("archived", "")),
        "created_at": goal.get("created_at", ""),
        "modified_at": goal.get("modified_at", ""),
        "due_on": goal.get("due_on", ""),
        "owner_gid": owner.get("gid", ""),
        "owner_name": owner.get("name", ""),
        "team_gid": team.get("gid", ""),
        "team_name": team.get("name", ""),
        "time_period_start_on": time_period.get("start_on", ""),
        "time_period_end_on": time_period.get("end_on", ""),
        "time_period_period": time_period.get("period", ""),
        "status_title": current_status.get("title", ""),
        "status_text": current_status.get("text", ""),
        "status_color": current_status.get("color", ""),
        "status_created_at": current_status.get("created_at", ""),
        "status_author_gid": status_author.get("gid", ""),
        "status_author_name": status_author.get("name", ""),
        "metric_gid": metric.get("gid", ""),
        "metric_initial_number_value": metric.get("initial_number_value", ""),
        "metric_current_number_value": metric.get("current_number_value", ""),
        "metric_target_number_value": metric.get("target_number_value", ""),
        "metric_unit": metric.get("unit", ""),
        "metric_currency_code": metric.get("currency_code", ""),
        "metric_precision": metric.get("precision", ""),
        "parent_goal_gids": ";".join(parent.get("gid", "") for parent in parent_goals),
        "parent_goal_names": ";".join(parent.get("name", "") for parent in parent_goals),
        "notes": goal.get("notes", ""),
        "html_notes": goal.get("html_notes", ""),
        "raw_goal_json": json.dumps(goal, ensure_ascii=False, sort_keys=True),
    }


def export_goals(input_path: Path, output_path: Path, token: str) -> None:
    with input_path.open(newline="", encoding="utf-8") as infile:
        rows = list(csv.DictReader(infile))

    flat_rows: list[dict[str, str]] = []
    for idx, row in enumerate(rows, start=1):
        goal_gid = row["goal_gid"]
        try:
            goal = fetch_goal(goal_gid, token)
            parent_goals = fetch_parent_goals(goal_gid, token)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(
                f"Failed fetching goal details for {goal_gid}: HTTP {exc.code}\n{body}"
            ) from exc

        flat_rows.append(flatten_goal(goal, parent_goals))

        if idx < len(rows):
            time.sleep(0.15)

    fieldnames = list(flat_rows[0].keys()) if flat_rows else []
    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat_rows)


def main() -> None:
    token = os.environ.get("ASANA_ACCESS_TOKEN")
    if not token:
        raise SystemExit(
            "Set ASANA_ACCESS_TOKEN with a token that has goals:read, then rerun."
        )

    default_input = Path("goals_export_2026-04-13.csv")
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_input
    output_path = (
        Path(sys.argv[2])
        if len(sys.argv) > 2
        else input_path.with_name(f"{input_path.stem}_full_details{input_path.suffix}")
    )

    export_goals(input_path, output_path, token)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
