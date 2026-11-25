"""
================================================================================
WEEKLY UPLOADER — ARCHIVING AND CLEANUP (via cron, Monday 3:00 UTC)
================================================================================
1. Moves past-day files (< today UTC) to staging/.
2. Compresses staging via native tar (zero RAM blowup).
3. Uploads to Backblaze B2.
4. On success: deletes local archive + staging. On failure: keeps all for retry.
5. On failure (compression or upload): ALERT via Telegram (same SMTP as collector).
================================================================================
"""
import os, shutil, subprocess, logging, traceback
from pathlib import Path
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from b2sdk.v2 import InMemoryAccountInfo, B2Api

BASE_DIR = Path("/root/crypto_bot")
DB_DIR = BASE_DIR / "crypto_db"
STAGING_DIR = BASE_DIR / "staging"
ARCHIVE_DIR = BASE_DIR / "archives"
LOG_FILE = BASE_DIR / "logs" / "uploader.log"

os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)
os.makedirs(LOG_FILE.parent, exist_ok=True)

_h = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger = logging.getLogger("uploader"); logger.setLevel(logging.INFO); logger.addHandler(_h)

load_dotenv(BASE_DIR / ".env")
B2_KEY_ID = os.getenv("B2_KEY_ID")
B2_APP_KEY = os.getenv("B2_APP_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

def send_alert(subject, body):
    """Sends a Telegram alert via HTTPS API (port 443). Sync, stdlib only."""
    import urllib.parse, urllib.request
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        logger.warning("Telegram alert not sent: TELEGRAM_TOKEN/CHAT_ID not configured.")
        return
    text = f"[CryptoBot-Uploader] {subject}\n\n{body}"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",
                                  data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info(f"Telegram alert sent: {subject}")
            else:
                logger.error(f"Telegram HTTP error {resp.status}")
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")

def get_b2_bucket():
    info = InMemoryAccountInfo(); api = B2Api(info)
    api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    return api.get_bucket_by_name(B2_BUCKET_NAME)

def main():
    logger.info("=== Archive starting ===")
    today_str = datetime.now(timezone.utc).date().isoformat()

    # 1. STAGING: move past-day .parquet files
    moved = 0
    for root, _, files in os.walk(DB_DIR):
        for f in files:
            if not f.endswith(".parquet"): continue
            if f[:10] < today_str:                       # filename = YYYY-MM-DD_....parquet
                src = Path(root) / f
                dst = STAGING_DIR / src.relative_to(DB_DIR)
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst)); moved += 1

    if moved == 0:
        logger.info("No files to archive.")
        return
    logger.info(f"{moved} files moved to staging.")

    archive_name = f"crypto_data_until_{today_str}.tar.gz"
    archive_path = ARCHIVE_DIR / archive_name

    # 2. COMPRESSION
    try:
        subprocess.run(["tar", "-czf", str(archive_path), "-C", str(STAGING_DIR), "."], check=True)
        logger.info(f"Compression OK: {archive_name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Compression error: {e}")
        send_alert("Compression failed", f"tar failed: {e}\nStaging retained for retry.")
        return

    # 3. B2 UPLOAD
    try:
        bucket = get_b2_bucket()
        bucket.upload_local_file(local_file=str(archive_path), file_name=f"archives/{archive_name}")
        logger.info("B2 upload succeeded.")
    except Exception as e:
        logger.error(f"B2 upload error: {e}")
        send_alert("B2 upload failed",
                   f"Upload failed: {e}\n\nThe archive and staging are RETAINED for "
                   f"retry next week. Disk fill risk if this repeats.\n\n{traceback.format_exc()}")
        return

    # 4. CLEANUP (only if upload succeeded)
    try:
        os.remove(archive_path)
        shutil.rmtree(STAGING_DIR); os.makedirs(STAGING_DIR)
        os.system(f"find {DB_DIR} -type d -empty -delete")
        logger.info("Local cleanup complete. === Archive OK ===")
        send_alert("B2 upload complete",
                   f"{moved} files archived ({archive_name}).\nDisk freed, staging cleaned.")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Uploader fatal error: {e}")
        send_alert("Uploader fatal error", traceback.format_exc())
