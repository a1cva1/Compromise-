"""
nid.zip → Cloudflare D1 (fast parallel uploader)
"""
import os
import sys
import json
import glob
import zipfile
import tempfile
import logging
import threading
import requests
import json5
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("uploader")

# ---------- Credentials ----------
CF_ACCOUNT_ID = os.environ["CF_ACCOUNT_ID"]
CF_DATABASE_ID = os.environ["CF_DATABASE_ID"]
CF_API_TOKEN = os.environ["CF_API_TOKEN"]

D1_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/"
    f"{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query"
)

ZIP_PATH = os.environ.get("ZIP_PATH", "nid.zip")

# ---------- Tuning ----------
# D1: max 100 bound params per query. Each row = 3 params → max 33.
# Use 25 to be extra safe (25×3 = 75 params).
BATCH_SIZE = 25
MAX_WORKERS = 6

# ---------- D1 helpers ----------
def d1_post(sql: str, params: list) -> dict:
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    r = requests.post(
        D1_URL,
        headers=headers,
        json={"sql": sql, "params": params},
        timeout=120,
    )
    if not r.ok:
        # Show the REAL error body from Cloudflare
        log.error("🚨 D1 %d response: %s", r.status_code, r.text[:500])
        r.raise_for_status()
    return r.json()


def insert_chunk(records: list[dict]) -> int:
    """
    এক statement-এ সব row insert করে:
    INSERT OR REPLACE INTO users (number, nid, dob) VALUES (?,?,?),(?,?,?),...
    """
    if not records:
        return 0

    placeholders = ",".join(["(?, ?, ?)"] * len(records))
    sql = f"INSERT OR REPLACE INTO users (number, nid, dob) VALUES {placeholders}"

    params = []
    for r in records:
        params.extend([r["number"], r["nid"], r["dob"]])

    d1_post(sql, params)
    return len(records)


# ---------- Tolerant JSON parser ----------
def parse_records(raw_text: str) -> list[dict]:
    raw_text = raw_text.strip()
    if not raw_text:
        return []

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        data = json5.loads(raw_text)

    # single object → list
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                data = v
                break
        else:
            data = [data]

    if not isinstance(data, list):
        raise ValueError("record array পাওয়া যায়নি")

    cleaned = []
    for i, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            continue
        number = str(item.get("number", "")).strip()
        nid = str(item.get("nid", "")).strip()
        dob = str(item.get("dob", "")).strip()
        if not (number and nid and dob):
            continue
        cleaned.append({"number": number, "nid": nid, "dob": dob})
    return cleaned


# ---------- Parallel insert ----------
progress_lock = threading.Lock()
_abort = threading.Event()


def insert_parallel(records: list[dict]) -> tuple[int, int]:
    total = len(records)
    if total == 0:
        return 0, 0

    chunks = [records[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    log.info("📤 %d row → %d batch (প্রতি batch-এ %d row), %d parallel worker",
             total, len(chunks), BATCH_SIZE, MAX_WORKERS)

    inserted = 0
    failed = 0
    first_error_shown = False

    def submit_one(chunk):
        # abort signal এলে skip
        if _abort.is_set():
            return 0
        return insert_chunk(chunk)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(submit_one, c): c for c in chunks}
        for f in as_completed(futures):
            chunk = futures[f]
            try:
                n = f.result()
                with progress_lock:
                    inserted += n
            except Exception as e:
                with progress_lock:
                    failed += len(chunk)
                    # প্রথম error-এ abort flag set — বাকিগুলো থেমে যাবে
                    if not first_error_shown:
                        first_error_shown = True
                        log.error("❌ প্রথম batch fail — বাকি সব বাতিল। কারণ: %s", e)
                        _abort.set()

            with progress_lock:
                done = inserted + failed
                pct = int(done / total * 100)
                bar = "█" * (pct // 2) + "░" * (50 - pct // 2)
                log.info(f"  [{bar}] {pct}% ({done}/{total}) | ok={inserted} fail={failed}")

    return inserted, failed


# ---------- Main ----------
def main():
    if not os.path.exists(ZIP_PATH):
        log.error("❌ %s ফাইল পাওয়া যায়নি (repo root-এ রাখুন)", ZIP_PATH)
        sys.exit(1)

    size_mb = os.path.getsize(ZIP_PATH) / 1024 / 1024
    log.info("📦 Zip: %s (%.2f MB)", ZIP_PATH, size_mb)

    grand_inserted = 0
    grand_failed = 0
    grand_total = 0
    file_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Extract
        try:
            with zipfile.ZipFile(ZIP_PATH, "r") as zf:
                zf.extractall(tmpdir)
                log.info("✅ %d ফাইল extract হয়েছে", len(zf.namelist()))
        except Exception as e:
            log.error("❌ Zip extract fail: %s", e)
            sys.exit(1)

        json_files = sorted(
            glob.glob(os.path.join(tmpdir, "**", "*.json"), recursive=True)
        )
        if not json_files:
            log.error("❌ Zip-এ কোনো .json ফাইল নেই")
            sys.exit(1)

        log.info("📂 %d টি JSON ফাইল পাওয়া গেছে", len(json_files))

        for fp in json_files:
            rel = os.path.relpath(fp, tmpdir)
            log.info("➡️  %s", rel)

            try:
                with open(fp, "r", encoding="utf-8") as f:
                    records = parse_records(f.read())
            except Exception as e:
                log.error("  ❌ Parse fail: %s", e)
                continue

            if not records:
                log.warning("  ⚠️  কোনো record নেই")
                continue

            log.info("  📥 %d record", len(records))
            ins, fail = insert_parallel(records)

            grand_inserted += ins
            grand_failed += fail
            grand_total += len(records)
            file_count += 1

            # প্রথম ফাইলেই বড় fail হলে বন্ধ করি
            if _abort.is_set():
                log.error("🛑 প্রথম ফাইলেই error — বাকি ফাইল skip")
                break

    log.info("=" * 55)
    log.info("🎉 সম্পন্ন!")
    log.info("   📁 ফাইল প্রসেস: %d", file_count)
    log.info("   📊 মোট record: %d", grand_total)
    log.info("   ✔️  Inserted: %d", grand_inserted)
    log.info("   ❌ Failed: %d", grand_failed)

    if grand_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
