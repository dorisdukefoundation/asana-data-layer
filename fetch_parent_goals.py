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


def fetch_parent_goals(goal_gid: str, token: str) -> list[dict]:
    query = urllib.parse.urlencode({"opt_fields": "gid,name"})
    data = asana_get(f"/goals/{goal_gid}/parentGoals?{query}", token)
    return data.get("data", [])


def enrich_csv(input_path: Path, output_path: Path, token: str) -> None:
    with input_path.open(newline="", encoding="utf-8") as infile:
        rows = list(csv.DictReader(infile))

    for idx, row in enumerate(rows, start=1):
        goal_gid = row["goal_gid"]
        try:
            parents = fetch_parent_goals(goal_gid, token)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(
                f"Failed fetching parent goals for {goal_gid}: HTTP {exc.code}\n{body}"
            ) from exc

        row["parent_goal_gids"] = ";".join(parent["gid"] for parent in parents)
        row["parent_goal_names"] = ";".join(parent["name"] for parent in parents)

        if idx < len(rows):
            time.sleep(0.15)

    fieldnames = list(rows[0].keys()) if rows else [
        "goal_gid",
        "goal_name",
        "owner_gid",
        "team_gid",
        "permalink_url",
        "parent_goal_gids",
        "parent_goal_names",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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
        else input_path.with_name(f"{input_path.stem}_with_parents{input_path.suffix}")
    )

    enrich_csv(input_path, output_path, token)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
