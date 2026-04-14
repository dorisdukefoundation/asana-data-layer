# Asana Data Layer

This project contains the scripts and services that sync Asana data into Airtable so the `Asana Data Layer` base can stay durable and queryable outside Asana.

## What it syncs

The API fetches the goal directly from Asana after each webhook event, then flattens the goal into Airtable-friendly fields. The default mapping includes:

- Goal name and GID
- Permalink URL
- archived flag
- owner and team metadata
- due date
- notes and HTML notes
- time period metadata
- current status title, text, color, author, and timestamp
- metric values and unit metadata
- parent goal IDs and names
- raw goal JSON for anything else we may need later

## Files

- `app.py`: FastAPI service with webhook and admin sync endpoints
- `register_goal_webhooks.py`: creates one webhook per goal in the CSV export
- `goals_export_2026-04-13.csv`: source list of goals to watch

## Setup

1. Create a virtualenv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy `.env.example` into your runtime environment.

3. Make sure the Airtable table already contains a merge field named `Asana Goal GID`, or update `AIRTABLE_MERGE_FIELD`.

4. Run the API:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

5. Expose the service publicly, for example with ngrok:

```bash
ngrok http 8000
```

6. Set `PUBLIC_BASE_URL` to the public HTTPS URL and register the goal webhooks:

```bash
python3 register_goal_webhooks.py
```

## Endpoints

- `GET /health`
- `POST /webhooks/asana/{goal_gid}`
- `POST /admin/sync-goal/{goal_gid}`
- `POST /admin/sync-csv`
- `GET /admin/config`

If `ADMIN_API_KEY` is set, pass it as `X-Admin-Key` for the admin endpoints.

## Airtable fields

The service can filter outgoing fields to only the columns that already exist in Airtable if `AIRTABLE_FILTER_TO_EXISTING_FIELDS=true`. That lets us include richer goal metadata without breaking if your table is still catching up.

## Live Sync Service

`live_sync_service.py` is the long-running FastAPI service for the `Asana Data Layer` base (`app3mkbiuKcaANHa7`).

It does three things:

- syncs goal changes from a workspace webhook into `Asana Goals`
- syncs project changes from the same workspace webhook into `Asana Projects`
- syncs task changes from per-project webhooks into `Asana Tasks`

If `Asana Tasks` does not exist yet and `AUTO_CREATE_TASKS_TABLE=true`, the service will create it through the Airtable metadata API the first time it needs it.

### Live sync setup

1. Set these environment variables:

```bash
export ASANA_ACCESS_TOKEN='...'
export AIRTABLE_TOKEN='...'
export AIRTABLE_BASE_ID='app3mkbiuKcaANHa7'
export ASANA_WORKSPACE_GID='1204848198937008'
export PUBLIC_BASE_URL='https://your-public-url.example.com'
export ADMIN_API_KEY='choose-a-random-secret'
```

2. Run the API:

```bash
uvicorn live_sync_service:app --host 0.0.0.0 --port 8000
```

3. Expose it publicly, for example:

```bash
ngrok http 8000
```

4. Bootstrap webhooks and, if needed, the tasks table:

```bash
curl -X POST \
  -H "X-Admin-Key: $ADMIN_API_KEY" \
  "$PUBLIC_BASE_URL/admin/bootstrap"
```

5. Backfill the current Asana data into Airtable:

```bash
curl -X POST -H "X-Admin-Key: $ADMIN_API_KEY" "$PUBLIC_BASE_URL/admin/backfill/goals"
curl -X POST -H "X-Admin-Key: $ADMIN_API_KEY" "$PUBLIC_BASE_URL/admin/backfill/projects"
curl -X POST -H "X-Admin-Key: $ADMIN_API_KEY" "$PUBLIC_BASE_URL/admin/backfill/tasks"
```

### Live sync endpoints

- `GET /health`
- `POST /webhooks/asana/workspace`
- `POST /webhooks/asana/project/{project_gid}`
- `POST /admin/bootstrap`
- `POST /admin/backfill/goals`
- `POST /admin/backfill/projects`
- `POST /admin/backfill/tasks`
- `POST /admin/backfill/all`
- `GET /admin/config`
