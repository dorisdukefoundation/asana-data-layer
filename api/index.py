from fastapi import FastAPI

try:
    from live_sync_service import app as app
except Exception as exc:  # pragma: no cover - fallback for misconfigured deployments
    app = FastAPI(title="Asana Airtable Live Sync")

    @app.get("/")
    def root() -> dict[str, str]:
        return {
            "status": "misconfigured",
            "error": str(exc),
        }

    @app.get("/health")
    def health() -> dict[str, str]:
        return {
            "status": "misconfigured",
            "error": str(exc),
        }
