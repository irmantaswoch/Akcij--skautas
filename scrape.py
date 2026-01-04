import os
import re
import json
import datetime
import requests
from dateutil import tz
from playwright.sync_api import sync_playwright

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY secrets.")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

VILNIUS = tz.gettz("Europe/Vilnius")

LIDL_URL = "https://www.lidl.lt/c/kainu-leidiniai/s10020254"

def now_vilnius():
    return datetime.datetime.now(tz=VILNIUS)

def should_run(now: datetime.datetime) -> bool:
    # Mon/Thu/Sat 12:00-12:15 Europe/Vilnius
    allowed_weekdays = {0, 3, 5}
    return (now.weekday() in allowed_weekdays) and (now.hour == 12) and (0 <= now.minute <= 15)

def supa_get(path, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def supa_patch(path, where_params: dict, patch: dict):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.patch(url, headers=HEADERS, params=where_params, data=json.dumps(patch), timeout=60)
    r.raise_for_status()

def supa_insert_offers(rows):
    if not rows:
        return
    # upsert by a reasonable conflict set
    url = f"{SUPABASE_URL}/rest/v1/offers?on_conflict=week_id,store,title,pack_value,pack_unit,price"
    headers = dict(HEADERS)
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    r = requests.post(url, headers=headers, data=json.dumps(rows), timeout=120)
    r.raise_for_status()

def get_queued_job():
    rows = supa_get("collector_jobs", params={
        "status": "eq.queued",
        "order": "requested_at.desc",
        "limit": "1",
    })
    return rows[0] if rows else None

def update_job(job_id: str, patch: dict):
    supa_patch("collector_jobs", {"id": f"eq.{job_id}"}, patch)

def update_run(run_id: str, patch: dict):
    supa_patch("runs", {"id": f"eq.{run_id}"}, patch)

# -------- Lidl HTML extraction --------

PRICE_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[.,]\d{2}))\s*€")
PACK_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|vnt)\b", re.IGNORECASE)

def normalize_price(s: str):
    s = s.replace(" ", "").replace("\xa0", "")
    m = PRICE_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except:
        return None

def compute_unit_price(price: float, pack_value: float, pack_unit: str):
    pack_unit = pack_unit.lower()
    if pack_unit == "g":
        kg = pack_value / 1000.0
        if kg > 0:
            return round(price / kg, 4), "EUR/kg"
    if pack_unit == "kg":
        if pack_value > 0:
            return round(price / pack_value, 4), "EUR/kg"
    if pack_unit == "ml":
        l = pack_value / 1000.0
        if l > 0:
            return round(price / l, 4), "EUR/l"
    if pack_unit == "l":
        if pack_value > 0:
            return round(price / pack_value, 4), "EUR/l"
    if pack_unit == "vnt":
        if pack_value > 0:
            return round(price / pack_value, 4), "EUR/vnt"
    return None, None

def extract_lidl_offers_html(week_id: str):
    """
    MVP strategy:
    - Render Lidl page
    - Grab visible text blocks
    - Identify product-like chunks containing €
    - Heuristically extract title + price (+ pack if present)
    This works even when DOM structure changes, because it uses text heuristics.
    """
    offers = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(LIDL_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2500)

        # Scroll to force lazy content
        for _ in range(8):
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(600)

        # Collect candidate text blocks from the page
        # We take div/li/section articles; if Lidl changes DOM, text approach still catches.
        candidates = page.locator("div, li, section, article").all()
        texts = []
        for el in candidates:
            try:
                t = el.inner_text(timeout=1000)
                if t and "€" in t:
                    t = re.sub(r"\s+\n", "\n", t)
                    t = re.sub(r"\n{3,}", "\n\n", t)
                    texts.append(t.strip())
            except:
                continue

        browser.close()

    # Flatten and parse
    for block in texts:
        # Take lines and attempt to form product entries
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        # We look for lines that contain €; title usually is same line or previous line
        for i, ln in enumerate(lines):
            if "€" not in ln:
                continue
            price = normalize_price(ln)
            if price is None:
                continue

            title = None
            # Prefer previous line as title if it looks like a name
            if i > 0 and len(lines[i-1]) >= 3 and "€" not in lines[i-1]:
                title = lines[i-1]
            else:
                # fallback: remove price token from same line
                title = PRICE_RE.sub("", ln).strip(" -–•|")
            title = re.sub(r"\s{2,}", " ", (title or "")).strip()
            if len(title) < 3:
                continue

            pack_value = None
            pack_unit = None
            m_pack = PACK_RE.search(" ".join(lines[max(0, i-2): i+2]))
            if m_pack:
                try:
                    pack_value = float(m_pack.group(1).replace(",", "."))
                    pack_unit = m_pack.group(2).lower()
                except:
                    pack_value = None
                    pack_unit = None

            unit_price = None
            unit_price_unit = None
            if pack_value and pack_unit:
                unit_price, unit_price_unit = compute_unit_price(price, pack_value, pack_unit)

            key = (title.lower(), price, pack_value, pack_unit)
            if key in seen:
                continue
            seen.add(key)

            offers.append({
                "week_id": week_id,
                "store": "lidl",
                "title": title,
                "category": None,
                "price": round(price, 2),
                "old_price": None,
                "currency": "EUR",
                "pack_value": pack_value,
                "pack_unit": pack_unit,
                "unit_price": unit_price,
                "unit_price_unit": unit_price_unit,
                "discount_pct": None,
                "valid_from": None,
                "valid_to": None,
                "source_url": LIDL_URL,
                "source_type": "html",
            })

    return offers

def main():
    now = now_vilnius()
    print(f"[worker] Now (Vilnius): {now.isoformat()}")

    event_name = os.getenv("GITHUB_EVENT_NAME", "")
    manual = (event_name == "workflow_dispatch")

    if not manual and not should_run(now):
        print("[worker] Not in scheduled window. Exiting.")
        return

    job = get_queued_job()
    if not job:
        print("[worker] No queued jobs found. Exiting.")
        return

    job_id = job["id"]
    run_id = job["run_id"]
    week_id = job.get("week_id") or f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
    print(f"[worker] Picked job_id={job_id}, run_id={run_id}, week_id={week_id}")

    try:
        update_job(job_id, {"status": "running", "started_at": now.isoformat()})

        offers = extract_lidl_offers_html(week_id=week_id)
        print(f"[lidl-html] Extracted offers: {len(offers)}")

        supa_insert_offers(offers)

        finished = now_vilnius()
        update_run(run_id, {
            "status": "ok",
            "stores_ok": 1 if len(offers) > 0 else 0,
            "offers_count": len(offers),
            "finished_at": finished.isoformat(),
            "errors": None,
            "notes": f"Lidl HTML stage completed. offers={len(offers)}"
        })
        update_job(job_id, {"status": "done", "finished_at": finished.isoformat(), "error": None})
        print("[worker] Completed successfully.")

    except Exception as e:
        finished = now_vilnius()
        err = str(e)
        print(f"[worker] Failed: {err}")

        update_run(run_id, {
            "status": "fail",
            "finished_at": finished.isoformat(),
            "errors": {"worker": err},
            "notes": "Worker failed in Lidl HTML stage."
        })
        update_job(job_id, {"status": "failed", "finished_at": finished.isoformat(), "error": err})
        raise

if __name__ == "__main__":
    main()
