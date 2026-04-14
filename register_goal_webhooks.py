#!/usr/bin/env python3
import csv
import json
import os
import urllib.request
from pathlib import Path


ASANA_API_BASE = "https://app.asana.com/api/1.0"


def asana_post(path: str, token: str, data: dict) -> dict:
    req = urllib.request.Request(
        f"{ASANA_API_BASE}{path}",
        data=json.dumps({"data": data}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def goal_ids_from_csv(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as infile:
        return [
            row["goal_gid"].strip()
            for row in csv.DictReader(infile)
            if (row.get("goal_gid") or "").strip()
        ]


def main() -> None:
    token = os.environ.get("ASANA_ACCESS_TOKEN", "").strip()
    public_base_url = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    csv_path = Path(os.environ.get("GOALS_CSV_PATH", "goals_export_2026-04-13.csv"))

    if not token:
        raise SystemExit("Set ASANA_ACCESS_TOKEN before running this script.")
    if not public_base_url:
        raise SystemExit("Set PUBLIC_BASE_URL before running this script.")

    created = []
    for goal_gid in goal_ids_from_csv(csv_path):
        target = f"{public_base_url}/webhooks/asana/{goal_gid}"
        payload = asana_post(
            "/webhooks",
            token,
            {
                "resource": goal_gid,
                "target": target,
                "filters": [
                    {"resource_type": "goal", "action": action}
                    for action in ("added", "removed", "deleted", "undeleted", "changed")
                ],
            },
        )
        data = payload.get("data", {})
        created.append(
            {
                "goal_gid": goal_gid,
                "webhook_gid": data.get("gid"),
                "target": data.get("target"),
            }
        )

    print(json.dumps({"created": len(created), "webhooks": created}, indent=2))


if __name__ == "__main__":
    main()
