import os
import json
import datetime
import requests
from dateutil import tz

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY secrets.")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

VILNIUS = tz.gettz("Europe/Vilnius")

def now_vilnius():
    return datetime.datetime.now(tz=VILNIUS)

def should_run(now: datetime.datetime) -> bool:
    # Run only Mon/Thu/Sat at 12:00-12:15 local time
    # weekday(): Mon=0 ... Sun=6
    allowed_weekdays = {0, 3, 5}  # Mon, Thu, Sat
    if now.weekday() not in allowed_weekdays:
        return False
    if now.hour != 12:
        return False
    if not (0 <= now.minute <= 15):
        return False
    return True

def get_queued_job():
    # Use PostgREST to fetch the newest queued job
    # GET /rest/v1/collector_jobs?status=eq.queued&order=requested_at.desc&limit=1
    url = f"{SUPABASE_URL}/rest/v1/collector_jobs"
    params = {
        "status": "eq.queued",
        "order": "requested_at.desc",
        "limit": "1",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else None

def update_job(job_id: str, patch: dict):
    url = f"{SUPABASE_URL}/rest/v1/collector_jobs"
    params = {"id": f"eq.{job_id}"}
    r = requests.patch(url, headers=HEADERS, params=params, data=json.dumps(patch), timeout=30)
    r.raise_for_status()

def update_run(run_id: str, patch: dict):
    url = f"{SUPABASE_URL}/rest/v1/runs"
    params = {"id": f"eq.{run_id}"}
    r = requests.patch(url, headers=HEADERS, params=params, data=json.dumps(patch), timeout=30)
    r.raise_for_status()

def main():
    now = now_vilnius()
    print(f"[worker] Now (Vilnius): {now.isoformat()}")

    # If you want: always run when triggered manually
    manual = os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch"

    if not manual and not should_run(now):
        print("[worker] Not in scheduled window. Exiting.")
        return

    job = get_queued_job()
    if not job:
        print("[worker] No queued jobs found. Exiting.")
        return

    job_id = job["id"]
    run_id = job["run_id"]
    print(f"[worker] Picked job_id={job_id}, run_id={run_id}")

    try:
        # Mark running
        update_job(job_id, {"status": "running", "started_at": now.isoformat()})

        # TODO: Here you will implement real scraping for:
        # Lidl, Maxima, Rimi, Norfa, IKI
        # and insert into public.offers (upsert/dedupe).
        #
        # For now we simulate success:
        offers_count = 0
        stores_ok = 0

        finished = now_vilnius()

        update_run(run_id, {
            "status": "ok",
            "stores_ok": stores_ok,
            "offers_count": offers_count,
            "finished_at": finished.isoformat(),
            "errors": None,
            "notes": "Worker ran successfully (placeholder; scraping not yet implemented)."
        })

        update_job(job_id, {"status": "done", "finished_at": finished.isoformat(), "error": None})

        print("[worker] Completed successfully.")

    except Exception as e:
        finished = now_vilnius()
        err = str(e)
        print(f"[worker] Failed: {err}")

        # Mark run fail + job failed
        update_run(run_id, {
            "status": "fail",
            "finished_at": finished.isoformat(),
            "errors": {"worker": err},
            "notes": "Worker failed."
        })
        update_job(job_id, {"status": "failed", "finished_at": finished.isoformat(), "error": err})

        raise

if __name__ == "__main__":
    main()
