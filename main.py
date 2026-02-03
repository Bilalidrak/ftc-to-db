import os
import csv
import time
import signal
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
import json
import requests

# =========================================================
# CONFIG
# =========================================================
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASS = os.getenv("MONGO_PASS", "admin123")
MONGO_DB = os.getenv("MONGO_DB", "mydb")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "mycollection")
MONGO_HOST = os.getenv("MONGO_HOST", "mongo_server")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "10"))
LIVE_PROGRESS_INTERVAL = 100000  # print every 100k rows
COOLDOWN = 7 * 3600  # 7 hours in seconds

CSV_DIR = "/app/csv_files"
LOG_DIR = "/app/logs"
OFFSET_FILE = os.path.join(LOG_DIR, "offset.json")
HEALTH_FILE = os.path.join(LOG_DIR, "health.ok")
PROGRESS_LOG = os.path.join(LOG_DIR, "progress.log")

os.makedirs(LOG_DIR, exist_ok=True)

# =========================================================
# LOGGING
# =========================================================
logger = logging.getLogger("CSVImporter")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(PROGRESS_LOG, maxBytes=5*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# =========================================================
# BITRIX ALERT FUNCTION
# =========================================================
BITRIX_WEBHOOK_URL = "https://idrakai.bitrix24.com/rest/1/n5p8fvongc3zjpq7/im.message.add.json"
BITRIX_CHAT_ID = "chat2249"

def send_bitrix_alert(title: str, message: str, status_icon: str = "ℹ️"):
    full_message = f"[B]{status_icon} {title}[/B]\n[HR]\n{message}"
    try:
        requests.get(
            BITRIX_WEBHOOK_URL,
            params={
                "DIALOG_ID": BITRIX_CHAT_ID,
                "MESSAGE": full_message,
                "SYSTEM": "Y"
            },
            timeout=5
        )
    except Exception as e:
        logger.warning(f"Failed to send Bitrix alert: {e}")

# =========================================================
# PRODUCTION-READY ALERT (Pakistan Time)
# =========================================================
def send_bitrix_pro_alert(csv_file, total_rows, inserted, skipped, elapsed_seconds, status="Successful"):
    pst = timezone(timedelta(hours=5))  # Pakistan Standard Time UTC+5
    now_pst = datetime.now(pst).strftime("%Y-%m-%d %H:%M:%S")
    host = os.uname()[1]
    message = (
        f"📦 CSV Import Report\n"
        f"🕐 Time: {now_pst} PKT\n"
        f"👤 Host: {host}\n"
        f"📄 File: {csv_file}\n"
        f"📊 Total Rows: {total_rows:,}\n"
        f"💾 Inserted: {inserted:,}\n"
        f"⚠️ Skipped: {skipped:,}\n"
        f"⏱️ Time Elapsed: {elapsed_seconds}s\n"
        f"✅ Status: {status}"
    )
    send_bitrix_alert(f"CSV Import Report: {csv_file}", message)

# =========================================================
# SIGNAL HANDLING
# =========================================================
running = True

def shutdown_handler(signum, frame):
    global running
    logger.info("Shutdown signal received. Finishing current batch...")
    running = False

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# =========================================================
# MONGO CONNECTION
# =========================================================
mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}?authSource=admin"
client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000, retryWrites=True)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# =========================================================
# OFFSET HANDLING
# =========================================================
def read_offsets() -> dict:
    if not os.path.exists(OFFSET_FILE):
        logger.info("Offset file not found. Starting from scratch")
        return {}
    try:
        with open(OFFSET_FILE) as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to read offset file: {e}")
        return {}

def write_offsets(offsets: dict):
    with open(OFFSET_FILE, "w") as f:
        json.dump(offsets, f)

# =========================================================
# UTILITIES
# =========================================================
def normalize_row(row: dict) -> dict:
    phone = row.get("company-phone-number", "").strip()
    if phone and not phone.startswith("1"):
        phone = "1" + phone
    return {
        "company_phone_number": phone,
        "created_date": row.get("created-date"),
        "violation_date": row.get("violation-date"),
        "consumer_city": row.get("consumer-city"),
        "consumer_state": row.get("consumer-state"),
        "consumer_area_code": row.get("consumer-area-code"),
        "subject": row.get("subject") or "Other",
        "tag": "ftc_dnc",
        "ingested_at": datetime.now(timezone.utc),
    }

def flush_csv(csv_path):
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    if lines:
        header = lines[0]
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(header)

# =========================================================
# FLUSH BATCH
# =========================================================
def flush_batch_return(batch):
    try:
        result = collection.bulk_write(batch, ordered=False)
        inserted = result.inserted_count
        skipped = 0
    except BulkWriteError as e:
        inserted = sum(1 for err in e.details.get("writeErrors", []) if err.get("code") != 11000)
        skipped = sum(1 for err in e.details.get("writeErrors", []) if err.get("code") == 11000)
    return inserted, skipped

# =========================================================
# TRIM LOG
# =========================================================
def trim_progress_log():
    if not os.path.exists(PROGRESS_LOG):
        return
    with open(PROGRESS_LOG, "r", encoding="utf-8") as f:
        lines = f.readlines()
    import_lines = [i for i, l in enumerate(lines) if "Import completed:" in l]
    if len(import_lines) <= 5:
        return
    keep_from = import_lines[-5]
    new_lines = lines[keep_from:]
    with open(PROGRESS_LOG, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

# =========================================================
# IMPORT FILE
# =========================================================
def import_file(csv_path: str, offsets: dict):
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as f:
        reader = list(csv.DictReader(f))
        total_rows = len(reader)

    offset = offsets.get(csv_path, 0)
    if offset >= total_rows:
        offset = 0
        offsets[csv_path] = 0
        write_offsets(offsets)

    if total_rows <= 1:
        return

    processed = offset
    batch = []
    start_time = time.time()
    total_inserted = 0
    total_skipped = 0

    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if not running:
                break
            if i < offset:
                continue
            batch.append(InsertOne(normalize_row(row)))
            processed += 1
            if len(batch) >= BATCH_SIZE:
                inserted, skipped = flush_batch_return(batch)
                total_inserted += inserted
                total_skipped += skipped
                batch.clear()
                offsets[csv_path] = processed
                write_offsets(offsets)

    if batch:
        inserted, skipped = flush_batch_return(batch)
        total_inserted += inserted
        total_skipped += skipped
        offsets[csv_path] = processed
        write_offsets(offsets)

    flush_csv(csv_path)
    offsets[csv_path] = 0
    write_offsets(offsets)

    with open(HEALTH_FILE, "w") as hf:
        hf.write(f"OK {datetime.now(timezone.utc).isoformat()}")

    elapsed = int(time.time() - start_time)
    logger.info(f"Import completed: {os.path.basename(csv_path)} | Total rows: {total_rows} | Inserted: {total_inserted} | Skipped: {total_skipped} | Time elapsed: {elapsed}s")

    send_bitrix_pro_alert(
        csv_file=os.path.basename(csv_path),
        total_rows=total_rows,
        inserted=total_inserted,
        skipped=total_skipped,
        elapsed_seconds=elapsed,
        status="Successful"
    )

    trim_progress_log()

# =========================================================
# MAIN LOOP
# =========================================================
def main():
    offsets = read_offsets()
    last_import_time = 0

    # Start alert
    logger.info("Starting CSV importer...")
    send_bitrix_alert("CSV Importer Started", "CSV Importer process has started on host.")

    while running:
        now = time.time()
        if now - last_import_time < COOLDOWN:
            time.sleep(CHECK_INTERVAL)
            continue

        csv_files = sorted(
            [f for f in os.listdir(CSV_DIR) if f.endswith("_session.csv")],
            key=lambda x: os.path.getmtime(os.path.join(CSV_DIR, x)),
            reverse=True
        )

        if not csv_files:
            time.sleep(CHECK_INTERVAL)
            continue

        latest_csv = os.path.join(CSV_DIR, csv_files[0])

        try:
            import_file(latest_csv, offsets)
            last_import_time = time.time()
        except Exception as e:
            logger.exception("Importer crashed")
            send_bitrix_pro_alert(
                csv_file=os.path.basename(latest_csv),
                total_rows=0,
                inserted=0,
                skipped=0,
                elapsed_seconds=0,
                status="Failed"
            )

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
