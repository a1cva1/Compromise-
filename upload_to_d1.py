"""
nid.zip থেকে JSON ফাইল বের করে সরাসরি Cloudflare D1-এ insert করে।
GitHub Actions-এ চলে — কোনো HTTP server দরকার নেই।
"""
import os
import sys
import json
import time
import glob
import zipfile
import tempfile
import logging
import requests
import json5

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("uploader")

# ---------- Credentials (GitHub Secrets থেকে) ----------
CF_ACCOUNT_ID = os.environ["CF_ACCOUNT_ID"]
CF_DATABASE_ID = os.environ["CF_DATABASE_ID"]
CF_API_TOKEN = os.environ["CF_API_TOKEN"]

D1_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/"
    f"{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query"
)

ZIP_PATH = os.environ.get("ZIP_PATH", "nid.zip")

# ---------- D1 API helpers ----------
def d1_batch(statements: list) -> dict:
    """একসাথে অনেক statement পাঠায় (batch API)।"""
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = [{"sql": s["sql"], "params": s["params"]} for s in statements]
    r = requests.post(D1_URL, headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()


def d1_single(sql: str, params: list) -> dict:
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    r = requests.post(
        D1_URL, headers=headers,
        json={"sql": sql, "params": params}, timeout=60,
    )
    r.raise_for_status()
    return r.json()


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
            log.warning("  Row %d বাদ — অসম্পূর্ণ: %s", i, item)
            continue
        cleaned.append({"number": number, "nid": nid, "dob": dob})
    return cleaned


# ---------- Batch insert ----------
BATCH_SIZE = 50
SQL = "INSERT OR REPLACE INTO users (number, nid, dob) VALUES (?, ?, ?)"


def insert_records(records: list[dict]) -> tuple[int, int]:
    inserted, failed = 0, 0
    total = len(records)
    for i in range(0, total, BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        statements = [
            {"sql": SQL, "params": [r["number"], r["nid"], r["dob"]]}
            for r in chunk
        ]
        try:
            d1_batch(statements)
            inserted += len(chunk)
        except Exception as e:
            log.error("  Batch fail: %s — single retry", e)
            for s in statements:
                try:
                    d1_single(s["sql"], s["params"])
                    inserted += 1
                except Exception as e2:
                    failed += 1
                    log.error("  Single fail: %s", e2)

        pct = int(inserted / total * 100) if total else 100
        bar = "█" * (pct // 2) + "░" * (50 - pct // 2)
        log.info(f"  [{bar}] {pct}% ({inserted}/{total})")

    return inserted, failed


# ---------- Main ----------
def main():
    if not os.path.exists(ZIP_PATH):
        log.error("❌ %s ফাইল পাওয়া যায়নি (repo root-এ রাখুন)", ZIP_PATH)
        sys.exit(1)

    log.info("📦 Zip: %s (%.2f MB)", ZIP_PATH, os.path.getsize(ZIP_PATH) / 1024 / 1024)

    grand_inserted = 0
    grand_failed = 0
    grand_total = 0
    file_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Zip extract
        try:
            with zipfile.ZipFile(ZIP_PATH, "r") as zf:
                zf.extractall(tmpdir)
                log.info("✅ %d ফাইল extract হয়েছে", len(zf.namelist()))
        except Exception as e:
            log.error("❌ Zip extract fail: %s", e)
            sys.exit(1)

        # সব .json খুঁজি (recursive)
        json_files = sorted(glob.glob(os.path.join(tmpdir, "**", "*.json"), recursive=True))
        if not json_files:
            log.error("❌ Zip-এ কোনো .json ফাইল নেই")
            sys.exit(1)

        log.info("📂 %d টি JSON ফাইল পাওয়া গেছে", len(json_files))

        for fp in json_files:
            rel = os.path.relpath(fp, tmpdir)
            log.info("➡️  প্রসেস: %s", rel)

            try:
                with open(fp, "r", encoding="utf-8") as f:
                    raw = f.read()
                records = parse_records(raw)
            except Exception as e:
                log.error("  ❌ Parse fail: %s", e)
                continue

            if not records:
                log.warning("  ⚠️  কোনো record নেই")
                continue

            log.info("  📥 %d record পাওয়া গেছে", len(records))
            inserted, failed = insert_records(records)

            grand_inserted += inserted
            grand_failed += failed
            grand_total += len(records)
            file_count += 1

            time.sleep(0.5)  # rate limit নিরাপদ

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
