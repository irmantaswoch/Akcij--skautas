import os
import re
import json
import datetime
from io import BytesIO
import requests
import pdfplumber
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

LIDL_LEAFLETS_URL = "https://www.lidl.lt/c/kainu-leidiniai/s10020254"

def now_vilnius():
    return datetime.datetime.now(tz=VILNIUS)

def iso_week_id(d=None):
    d = d or now_vilnius()
    # ISO week id: YYYY-Www
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"

def should_run(now: datetime.datetime) -> bool:
    # Run only Mon/Thu/Sat at 12:00-12:15 local time
    allowed_weekdays = {0, 3, 5}  # Mon, Thu, Sat
    if now.weekday() not in allowed_weekdays:
        return False
    if now.hour != 12:
        return False
    return 0 <= now.minute <= 15

# --- Supabase REST helpers ---
def supa_get(path, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def supa_patch(path, where_params: dict, patch: dict):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.patch(url, headers=HEADERS, params=where_params, data=json.dumps(patch), timeout=60)
    r.raise_for_status()

def supa_insert_offers(rows: list[dict]):
    if not rows:
        return
    # Upsert using the unique index columns (must match on_conflict columns)
    # offers_dedupe_uq: (week_id, store, title, coalesce(pack_value,-1), coalesce(pack_unit,''), price)
    # PostgREST on_conflict must use real columns; pack_value/pack_unit may be null,
    # still works for most cases; scraper also dedupes in Python.
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

# --- Lidl collector ---
def find_lidl_pdf_links() -> list[str]:
    """
    Use Playwright to render the leaflets page and collect all unique PDF URLs.
    """
    pdfs = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(LIDL_LEAFLETS_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)

        # Collect all hrefs that look like pdf
        anchors = page.locator("a").all()
        for a in anchors:
            href = a.get_attribute("href")
            if not href:
                continue
            if ".pdf" in href.lower():
                # Normalize relative links
                if href.startswith("/"):
                    href = "https://www.lidl.lt" + href
                pdfs.add(href)

        browser.close()

    return sorted(pdfs)

_price_re = re.compile(r"(?<!\d)(\d{1,3}(?:[.,]\d{2}))\s*€")
_pack_re = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|vnt)\b", re.IGNORECASE)

def parse_pdf_offers(pdf_bytes: bytes, source_url: str, week_id: str) -> list[dict]:
    """
    Generic PDF text extraction:
    - Extract lines from PDF
    - Identify lines containing prices
    - Build a minimal offer record: title + price (+ pack if detected)
    This is MVP. Lidl PDF layout varies; later we’ll refine extraction rules.
    """
    offers = []
    seen = set()

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

            for ln in lines:
                m_price = _price_re.search(ln)
                if not m_price:
                    continue

                price_str = m_price.group(1).replace(",", ".")
                try:
                    price = float(price_str)
                except:
                    continue

                # Title heuristic: remove price token, keep remaining text
                title = _price_re.sub("", ln).strip(" -–•|")
                title = re.sub(r"\s{2,}", " ", title).strip()
                if len(title) < 3:
                    continue

                pack_value = None
                pack_unit = None
                m_pack = _pack_re.search(ln)
                if m_pack:
                    pv = m_pack.group(1).replace(",", ".")
                    try:
                        pack_value = float(pv)
                        pack_unit = m_pack.group(2).lower()
                    except:
                        pack_value = None
                        pack_unit = None

                unit_price = None
                unit_price_unit = None
                if pack_value and pack_unit:
                    unit_price, unit_price_unit = compute_unit_price(price, pack_value, pack_unit)

                # Dedup key (local)
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
                    "source_url": source_url,
                    "source_type": "pdf",
                })

    return offers

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

def download(url: str) -> bytes:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content

def main():
    now = now_vilnius()
    print(f"[worker] Now (Vilnius): {now.isoformat()}")

    # If manually triggered, allow run regardless of time window:
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
    week_id = job.get("week_id") or iso_week_id()
    print(f"[worker] Picked job_id={job_id}, run_id={run_id}, week_id={week_id}")

    try:
        update_job(job_id, {"status": "running", "started_at": now.isoformat()})

        # ---- LIDL ONLY (Stage 1) ----
        pdf_links = find_lidl_pdf_links()
        print(f"[lidl] Found PDF links: {len(pdf_links)}")

        all_offers = []
        for pdf_url in pdf_links:
            try:
                pdf_bytes = download(pdf_url)
                offers = parse_pdf_offers(pdf_bytes, source_url=pdf_url, week_id=week_id)
                print(f"[lidl] {pdf_url} -> offers extracted: {len(offers)}")
                all_offers.extend(offers)
            except Exception as e:
                print(f"[lidl] PDF parse failed: {pdf_url} :: {e}")

        # Insert into offers
        supa_insert_offers(all_offers)

        finished = now_vilnius()
        stores_ok = 1
        offers_count = len(all_offers)

        update_run(run_id, {
            "status": "ok",
            "stores_ok": stores_ok,
            "offers_count": offers_count,
            "finished_at": finished.isoformat(),
            "errors": None,
            "notes": f"Lidl stage completed. PDFs={len(pdf_links)} offers={offers_count}"
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
            "notes": "Worker failed in Lidl stage."
        })
        update_job(job_id, {"status": "failed", "finished_at": finished.isoformat(), "error": err})
        raise

if __name__ == "__main__":
    main()
