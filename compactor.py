"""
DAILY COMPACTOR — merges part-files from previous days into a single
Parquet file per day, sorted by local_ts.

Triggered via cron: daily at 00:05 UTC (5 min after midnight).
The current day's files are never touched (the collector is still writing to them).
"""
import os, logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import pandas as pd

load_dotenv("/root/crypto_bot/.env")
BASE_DIR = Path("/root/crypto_bot/crypto_db")
LOG_FILE = Path("/root/crypto_bot/logs/compactor.log")
os.makedirs(LOG_FILE.parent, exist_ok=True)
_h = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger = logging.getLogger("compactor"); logger.setLevel(logging.INFO); logger.addHandler(_h)

def send_alert(subject, body):
    """Sends a Telegram alert via HTTPS API (port 443). Sync, stdlib only."""
    import urllib.parse, urllib.request
    token = os.getenv("TELEGRAM_TOKEN"); chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id): return
    text = f"[CryptoBot-Compactor] {subject}\n\n{body}"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",
                                  data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10): pass
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")

def compact_dir(leaf: Path, today: str) -> int:
    """Merges part-files from previous days in a leaf directory.
    Returns the number of days compacted."""
    by_date = defaultdict(list)
    for f in leaf.glob("*.parquet"):
        by_date[f.name[:10]].append(f)

    done = 0
    for date_str, files in sorted(by_date.items()):
        if date_str >= today: continue          # never touch the current day
        final = leaf / f"{date_str}.parquet"
        # Already compacted: a single file named YYYY-MM-DD.parquet (no timestamp)
        if len(files) == 1 and files[0] == final: continue
        try:
            df = pd.concat([pd.read_parquet(f) for f in sorted(files)], ignore_index=True)
            if "local_ts" in df.columns:
                df = df.sort_values("local_ts").reset_index(drop=True)
            # Atomic write: temp file then rename
            tmp = final.with_suffix(".tmp.parquet")
            df.to_parquet(tmp, engine="pyarrow", compression="zstd", index=False)
            tmp.rename(final)
            # Remove part-files (unless already final)
            for f in files:
                if f != final: f.unlink()
            logger.info(f"Compacted {leaf.relative_to(BASE_DIR)}/{date_str} "
                        f"({len(files)} files -> 1, {len(df)} rows)")
            done += 1
        except Exception as e:
            logger.error(f"Error {leaf}/{date_str}: {e}")
    return done

def main():
    today = datetime.now(timezone.utc).date().isoformat()
    logger.info(f"=== Compaction started — days < {today} ===")
    total = 0
    for root, _, files in os.walk(BASE_DIR):
        if any(f.endswith(".parquet") for f in files):
            total += compact_dir(Path(root), today)
    logger.info(f"=== Compaction complete: {total} days compacted. ===")
    if total:
        send_alert("Daily compaction complete", f"{total} day(s) merged into 1 file/day.")

if __name__ == "__main__":
    main()
