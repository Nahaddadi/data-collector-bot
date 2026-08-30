"""
================================================================================
TELEGRAM BOT (INTERACTIVE) - Control and monitoring for the collection VM
================================================================================
Runs as a systemd service (cryptobot-telegram.service). Listens for commands via
Telegram long-polling (outbound port 443 only) and responds in real time.

SECURITY: responds ONLY to the configured chat_id (TELEGRAM_CHAT_ID from .env).
All other chats are ignored. Only predefined commands are accepted; no arbitrary
shell execution is performed.

Commands: /help /status /health /db /logs <venue> /errors /restart <venue> /upload
Auto     : disk-space alerts at 70/80/90% thresholds.
================================================================================
"""
import asyncio, os, shutil, logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import aiohttp

load_dotenv("/root/crypto_bot/.env")
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID") or "")
API = f"https://api.telegram.org/bot{TOKEN}"
CRYPTO_DB = Path("/root/crypto_bot/crypto_db")
LOG_DIR = Path("/root/crypto_bot/logs")
VENV_PY = "/root/crypto_bot/venv/bin/python"
SRC = "/root/crypto_bot/src"
VENUES = ["binance", "bybit", "okx", "kucoin", "hyperliquid", "dydx"]

LOG_FILE = LOG_DIR / "bot.log"
os.makedirs(LOG_DIR, exist_ok=True)
_h = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger = logging.getLogger("bot"); logger.setLevel(logging.INFO); logger.addHandler(_h)

HELP = (
    "🤖 CryptoBot — available commands\n\n"
    "/status — CPU, RAM, disk, file count, uptime\n"
    "/health — status of 6 venues\n"
    "/db — database size per venue\n"
    "/logs <venue> — last 20 log lines\n"
    "/errors — last errors across venues\n"
    "/restart <venue> — restart a venue\n"
    "/upload — trigger B2 archiving now\n"
    "/help — this help\n\n"
    "Venues: binance, bybit, okx, kucoin, hyperliquid, dydx"
)

def human(n):
    n = float(n)
    for u in ["B", "K", "M", "G", "T"]:
        if n < 1024: return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}P"

async def tg_send(session, text):
    try:
        await session.post(f"{API}/sendMessage", data={"chat_id": CHAT_ID, "text": text[:3900]})
    except Exception as e:
        logger.error(f"tg_send: {e}")

async def run_cmd(*args):
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        out, _ = await proc.communicate()
        return out.decode(errors="replace")
    except Exception as e:
        return f"(command error: {e})"

def _scan_db():
    total, n, by_ex = 0, 0, {}
    for f in CRYPTO_DB.rglob("*.parquet"):
        try: size = f.stat().st_size
        except OSError: continue
        total += size; n += 1
        ex = next((p.split("=", 1)[1] for p in f.parts if p.startswith("exchange=")), "?")
        by_ex[ex] = by_ex.get(ex, 0) + size
    return total, n, by_ex

# ---------------- COMMAND HANDLERS ----------------
async def cmd_status():
    la = os.getloadavg(); cores = os.cpu_count() or 1
    mem = {}
    for line in open("/proc/meminfo"):
        parts = line.split()
        if parts: mem[parts[0].rstrip(":")] = int(parts[1]) * 1024
    ram_total = mem.get("MemTotal", 0); ram_avail = mem.get("MemAvailable", 0)
    ram_used = ram_total - ram_avail
    du = shutil.disk_usage("/")
    total, n, _ = await asyncio.to_thread(_scan_db)
    up = float(open("/proc/uptime").read().split()[0])
    d, hh = int(up // 86400), int((up % 86400) // 3600)
    return (f"📊 STATUS\n"
            f"CPU : {la[0]/cores*100:.0f}%  (load {la[0]:.2f} / {cores} cores)\n"
            f"RAM : {human(ram_used)} / {human(ram_total)}  ({ram_used/ram_total*100:.0f}%)\n"
            f"Disk : {human(du.used)} / {human(du.total)}  ({du.used/du.total*100:.0f}%)\n"
            f"DB : {human(total)}  —  {n} files\n"
            f"VM uptime : {d}d {hh}h")

async def cmd_health():
    lines = ["🩺 SERVICES"]
    for v in VENUES:
        st = (await run_cmd("systemctl", "is-active", f"cryptobot@{v}")).strip()
        lines.append(f"{'✅' if st == 'active' else '🔴'} {v} : {st}")
    return "\n".join(lines)

async def cmd_db():
    total, n, by_ex = await asyncio.to_thread(_scan_db)
    lines = [f"💾 DB : {human(total)} ({n} files)"]
    for ex in sorted(by_ex, key=lambda k: -by_ex[k]):
        lines.append(f"  {ex} : {human(by_ex[ex])}")
    return "\n".join(lines)

async def cmd_logs(venue):
    if venue not in VENUES:
        return f"Unknown venue. Choices: {', '.join(VENUES)}"
    f = LOG_DIR / f"collector_{venue}.log"
    if not f.exists(): return f"No log file for {venue} yet."
    out = await run_cmd("tail", "-n", "20", str(f))
    return f"📜 {venue} (last 20 lines):\n\n{out.strip() or '(empty)'}"

async def cmd_errors():
    out = await run_cmd("bash", "-c",
                        f"grep -h ERROR {LOG_DIR}/collector_*.log 2>/dev/null | tail -20")
    return f"⚠️ Last errors:\n\n{out.strip() or '(no recent errors)'}"

async def cmd_restart(venue):
    if venue not in VENUES:
        return f"Unknown venue. Choices: {', '.join(VENUES)}"
    await run_cmd("systemctl", "restart", f"cryptobot@{venue}")
    await asyncio.sleep(2)
    st = (await run_cmd("systemctl", "is-active", f"cryptobot@{venue}")).strip()
    return f"🔄 {venue} restarted — status: {st}"

async def cmd_upload():
    asyncio.create_task(run_cmd(VENV_PY, f"{SRC}/uploader.py"))
    return "📤 B2 archiving started. A notification will be sent upon completion."

async def handle(text):
    parts = text.split()
    cmd = parts[0].lower().lstrip("/").split("@")[0]   # handles /status@MonBot
    arg = parts[1] if len(parts) > 1 else None
    if cmd in ("help", "start"): return HELP
    if cmd == "status": return await cmd_status()
    if cmd == "health": return await cmd_health()
    if cmd == "db": return await cmd_db()
    if cmd == "logs": return await cmd_logs(arg) if arg else "Usage: /logs <venue>"
    if cmd == "errors": return await cmd_errors()
    if cmd == "restart": return await cmd_restart(arg) if arg else "Usage: /restart <venue>"
    if cmd == "upload": return await cmd_upload()
    return "Unknown command. /help for the list."

# ---------------- DISK MONITORING ----------------
async def disk_monitor(session):
    thresholds = [70, 80, 90]
    armed = {t: True for t in thresholds}
    while True:
        try:
            du = shutil.disk_usage("/")
            pct = du.used / du.total * 100
            for t in thresholds:
                if pct >= t and armed[t]:
                    crit = "  🚨 CRITICAL — archive/free space recommended!" if t >= 90 else ""
                    await tg_send(session, f"⚠️ Disk at {pct:.0f}% (threshold {t}%){crit}")
                    armed[t] = False
                elif pct < t - 5 and not armed[t]:
                    armed[t] = True   # rearmed with hysteresis (-5%)
        except Exception as e:
            logger.error(f"disk_monitor: {e}")
        await asyncio.sleep(600)   # every 10 min

# ---------------- MAIN LOOP ----------------
async def main():
    if not (TOKEN and CHAT_ID):
        logger.error("TELEGRAM_TOKEN/CHAT_ID missing from .env — exiting.")
        return
    offset = None
    async with aiohttp.ClientSession() as session:
        # Clear pending updates so old commands are not replayed on restart
        try:
            async with session.get(f"{API}/getUpdates", params={"offset": -1}) as r:
                for upd in (await r.json()).get("result", []):
                    offset = upd["update_id"] + 1
        except Exception: pass

        await tg_send(session, "🤖 Bot started and listening. /help for commands.")
        asyncio.create_task(disk_monitor(session))
        logger.info("Bot started.")

        while True:
            try:
                params = {"timeout": 30}
                if offset is not None: params["offset"] = offset
                async with session.get(f"{API}/getUpdates", params=params,
                                        timeout=aiohttp.ClientTimeout(total=40)) as r:
                    data = await r.json()
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("edited_message") or {}
                    chat = str(msg.get("chat", {}).get("id", ""))
                    text = (msg.get("text") or "").strip()
                    if chat != CHAT_ID:          # SECURITY: only the configured chat_id is served
                        logger.warning(f"Message ignored from unauthorized chat: {chat}")
                        continue
                    if not text.startswith("/"): continue
                    logger.info(f"Command received: {text}")
                    try:
                        reply = await handle(text)
                    except Exception as e:
                        reply = f"Processing error: {e}"
                        logger.error(f"handle({text}): {e}")
                    if reply: await tg_send(session, reply)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"main loop: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
