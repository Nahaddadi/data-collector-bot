"""
================================================================================
LIVE COLLECTOR — 24/7 production (multi-process: one process per venue)
================================================================================
Launch: python collector.py <venue>
  venue in {binance, bybit, okx, kucoin, hyperliquid, dydx}

Each venue runs in its OWN process (systemd template), uses 2 cores, isolates
dYdX (the torrent), and a crash affects only one venue.

CEX (USDT): SPOT (book+trades) + FUTURES (book+trades+funding)
  + USDC spot BTC/ETH on Binance & OKX (stablecoin regime / depeg monitoring)
DEX (native quote): Hyperliquid (coin), dYdX (-USD): book+trades+funding
Book: L20, snapshot every 100ms (throttle). Trades: full tick-by-tick.
================================================================================
"""
import asyncio, json, time, uuid, logging, signal, os, sys
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import aiohttp
from datetime import datetime, timezone
from pathlib import Path
import websockets
import pandas as pd

# --- VENUE (required argument) ---
VENUES = {"binance", "bybit", "okx", "kucoin", "hyperliquid", "dydx"}
VENUE = sys.argv[1] if len(sys.argv) > 1 else None
if VENUE not in VENUES:
    print(f"usage: python collector.py <{'|'.join(sorted(VENUES))}>")
    sys.exit(1)

load_dotenv()   # reads /root/crypto_bot/.env (or traverses up)

# --- CONFIGURATION ---
DEPTH = 20
THROTTLE_MS = 100          # max 1 book snapshot per 100ms (sampling rate)
FLUSH_INTERVAL = 60        # flush at most every 60s (bounds crash loss)
FLUSH_MAX = 20000          # ... or immediately when 20k rows accumulated (bounds RAM)
WATCHDOG_SEC = 300         # alert if a book feed is silent for 5 min
BASE_DIR = Path("/root/crypto_bot/crypto_db")
LOG_FILE = Path(f"/root/crypto_bot/logs/collector_{VENUE}.log")
WRITE_PARQUET = True

SPOT = {
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BTCUSDC", "ETHUSDC"],
    "bybit":   ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "okx":     ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BTC-USDC", "ETH-USDC"],
    "kucoin":  ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
}
FUT = {
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "bybit":   ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "okx":     ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"],
    "kucoin":  ["XBTUSDTM", "ETHUSDTM", "SOLUSDTM"],
}
DEX = {
    "hyperliquid": ["BTC", "ETH", "SOL"],
    "dydx":        ["BTC-USD", "ETH-USD", "SOL-USD"],
}

os.makedirs(LOG_FILE.parent, exist_ok=True)
_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5,
                                encoding="utf-8")
_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger = logging.getLogger(VENUE)
logger.setLevel(logging.WARNING)   # INFO and DEBUG silenced — clean logs for long runs
logger.addHandler(_handler)

is_running = True
def shutdown_handler(sig, frame):
    global is_running
    logger.warning(f"Signal {sig} received — initiating clean shutdown.")
    is_running = False
signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# --------------------------------------------------------------------------- #
class Book:
    def __init__(self): self.bids, self.asks, self.ready = {}, {}, False
    def set_snapshot(self, bids, asks):
        self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
        self.ready = True
    def apply(self, bids, asks):
        for p, q in bids:
            p, q = float(p), float(q)
            self.bids.pop(p, None) if q == 0 else self.bids.__setitem__(p, q)
        for p, q in asks:
            p, q = float(p), float(q)
            self.asks.pop(p, None) if q == 0 else self.asks.__setitem__(p, q)
    def prune_crossed(self):
        if not self.bids or not self.asks: return
        bb, ba = max(self.bids), min(self.asks)
        if bb < ba: return
        for p in [q for q in self.asks if q <= bb]: self.asks.pop(p, None)
        for p in [q for q in self.bids if q >= ba]: self.bids.pop(p, None)
    def top_row(self, depth, **meta):
        if not self.ready: return None
        b = sorted(self.bids.items(), key=lambda x: -x[0])[:depth]
        a = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        if len(b) < depth or len(a) < depth: return None   # only emit complete L20 snapshots
        row = {"local_ts": int(time.time()*1000), **meta}
        for i in range(depth):
            row[f"bid_{i+1}"], row[f"bid_{i+1}_qty"] = b[i]
            row[f"ask_{i+1}"], row[f"ask_{i+1}_qty"] = a[i]
        return row

class Sink:
    def __init__(self, dt, asset, ex, pair):
        self.meta = (dt, asset, ex, pair)
        self.rows = []; self.last_ts = 0; self.last_flush = time.time(); self.total = 0
    def should_update(self):
        """Returns True if the sink would accept a new book snapshot.
        Used to skip the expensive top_row sort when the throttle would reject
        the result anyway (critical for KuCoin spot level2)."""
        if self.meta[0] != "book": return True
        return int(time.time()*1000) - self.last_ts >= THROTTLE_MS

    def add(self, row):
        self.total += 1
        if self.meta[0] == "book":
            now = row.get("local_ts", int(time.time()*1000))
            if now - self.last_ts < THROTTLE_MS: return
            self.last_ts = now
        self.rows.append(row)

def write_batch(meta, rows):
    if not (WRITE_PARQUET and rows): return
    dt, asset, ex, pair = meta
    pair_c = pair.replace("/", "-").replace(":", "-")
    d = datetime.now(timezone.utc).date()
    out = (BASE_DIR / f"data={dt}" / f"asset={asset}" / f"exchange={ex}"
           / f"pair={pair_c}" / f"year={d.year:04d}" / f"month={d.month:02d}")
    out.mkdir(parents=True, exist_ok=True)
    fp = out / f"{d.isoformat()}_{int(time.time()*1000)}.parquet"
    try:
        pd.DataFrame(rows).to_parquet(fp, engine="pyarrow", compression="zstd", index=False)
        logger.info(f"flush {len(rows)} -> {fp.name}")
    except Exception as e:
        logger.error(f"flush err {fp}: {e}")

def _norm(side):
    out = []
    for lvl in side or []:
        if isinstance(lvl, dict): out.append((lvl.get("price"), lvl.get("size")))
        else: out.append((lvl[0], lvl[1]))
    return out

async def _ws(url, ping_interval=None):
    return websockets.connect(url, ping_interval=ping_interval, max_queue=None, open_timeout=15)

async def _until(ws):
    return await asyncio.wait_for(ws.recv(), timeout=20.0)

# --------------------------------------------------------------------------- #
#                                PARSING                                      #
# --------------------------------------------------------------------------- #
def bybit_book_parse(m):
    if not str(m.get("topic", "")).startswith("orderbook."): return None
    d = m.get("data", {}); return m.get("type"), d.get("b", []), d.get("a", [])
def okx_book_parse(d):
    return [(l[0], l[1]) for l in d.get("bids", [])], [(l[0], l[1]) for l in d.get("asks", [])]
def kucoin_l2_parse(m):
    if m.get("subject") != "trade.l2update": return None
    d = m["data"]; ch = d["changes"]
    return (int(d["sequenceStart"]), int(d["sequenceEnd"]),
            [(c[0], c[1]) for c in ch.get("bids", [])], [(c[0], c[1]) for c in ch.get("asks", [])])
def kucoin_fut_depth_parse(m):
    if "level2Depth50" not in str(m.get("topic", "")): return None
    d = m.get("data", {})
    return ([(l[0], l[1]) for l in d.get("bids", [])], [(l[0], l[1]) for l in d.get("asks", [])])

def _trade(price, amount, side, ts, tid, exchange, pair):
    return {"local_ts": int(time.time()*1000), "exchange_ts": ts, "exchange": exchange,
            "pair": pair, "trade_id": str(tid) if tid is not None else None,
            "price": float(price), "amount": float(amount), "side": str(side).lower()}
def binance_trades_parse(m, exchange, pair):
    if m.get("e") not in ("trade", "aggTrade"): return []
    side = "sell" if m.get("m") else "buy"
    return [_trade(m["p"], m["q"], side, m.get("T"), m.get("t", m.get("a")), exchange, pair)]
def bybit_trades_parse(m, exchange, pair):
    if not str(m.get("topic", "")).startswith("publicTrade."): return []
    return [_trade(d["p"], d["v"], d["S"], d.get("T"), d.get("i"), exchange, pair) for d in m.get("data", [])]
def okx_trades_parse(data, exchange, pair):
    return [_trade(d["px"], d["sz"], d["side"], int(d["ts"]), d.get("tradeId"), exchange, pair) for d in data]
def kucoin_spot_trades_parse(m, exchange, pair):
    if m.get("subject") != "trade.l3match": return []
    d = m["data"]; return [_trade(d["price"], d["size"], d["side"], d.get("time"), d.get("tradeId"), exchange, pair)]
def kucoin_fut_trades_parse(m, exchange, pair):
    if m.get("subject") != "match": return []
    d = m["data"]; return [_trade(d["price"], d.get("size", d.get("matchSize")), d["side"],
                                   d.get("ts", d.get("time")), d.get("tradeId"), exchange, pair)]
def hl_trades_parse_one(t, exchange, pair):
    s = t.get("side"); side = {"B": "buy", "A": "sell"}.get(s, str(s).lower())
    return _trade(t["px"], t["sz"], side, t.get("time"), t.get("tid"), exchange, pair)
def dydx_trades_parse(m, exchange, pair):
    if m.get("channel") != "v4_trades": return []
    return [_trade(t["price"], t["size"], t["side"], t.get("createdAt"), t.get("id"), exchange, pair)
            for t in m.get("contents", {}).get("trades", [])]

def _fund(exchange, pair, rate, **extra):
    return {"local_ts": int(time.time()*1000), "exchange": exchange, "pair": pair,
            "funding_rate": float(rate) if rate not in (None, "") else None, **extra}
def binance_fund_parse(m, exchange, pair):
    if m.get("e") != "markPriceUpdate": return None
    return _fund(exchange, pair, m.get("r"), next_funding_ts=m.get("T"), mark_price=m.get("p"), index_price=m.get("i"))
def bybit_fund_parse(m, exchange, pair):
    if not str(m.get("topic", "")).startswith("tickers."): return None
    d = m.get("data", {})
    if "fundingRate" not in d: return None
    return _fund(exchange, pair, d.get("fundingRate"), next_funding_ts=d.get("nextFundingTime"),
                 mark_price=d.get("markPrice"), index_price=d.get("indexPrice"))
def okx_fund_parse(data, exchange, pair):
    d = data[0]
    return _fund(exchange, pair, d.get("fundingRate"), funding_ts=d.get("fundingTime"), next_funding_ts=d.get("nextFundingTime"))
def kucoin_fut_fund_parse(m, exchange, pair):
    if m.get("subject") != "funding.rate": return None
    d = m["data"]; return _fund(exchange, pair, d.get("fundingRate"), funding_ts=d.get("timestamp"))
def hl_fund_parse_ctx(ctx, exchange, pair):
    return _fund(exchange, pair, ctx.get("funding"), mark_price=ctx.get("markPx"),
                 oracle_price=ctx.get("oraclePx"), open_interest=ctx.get("openInterest"))
def dydx_fund_parse(m, exchange, pair):
    if m.get("channel") != "v4_markets": return None
    c = m.get("contents", {}); markets = c.get("markets", c)
    mk = markets.get(pair) if isinstance(markets, dict) else None
    if not mk or ("nextFundingRate" not in mk and "oraclePrice" not in mk): return None
    return _fund(exchange, pair, mk.get("nextFundingRate"), oracle_price=mk.get("oraclePrice"))

# --------------------------------------------------------------------------- #
#                              COLLECTORS                                     #
# --------------------------------------------------------------------------- #
async def binance_multi(futures, sinks):
    syms = [s.lower() for s in sinks]
    if futures:
        depth = "/".join(f"{x}@depth20@100ms" for x in syms)
        mkt = "/".join([f"{x}@aggTrade" for x in syms] + [f"{x}@markPrice@1s" for x in syms])
        urls = [f"wss://fstream.binance.com/public/stream?streams={depth}",
                f"wss://fstream.binance.com/market/stream?streams={mkt}"]
    else:
        st = "/".join([f"{x}@depth20@100ms" for x in syms] + [f"{x}@trade" for x in syms])
        urls = [f"wss://stream.binance.com:9443/stream?streams={st}"]
    await asyncio.gather(*[_binance_conn(u, sinks, futures) for u in urls])

async def _binance_conn(url, sinks, futures):
    tag = url.split("/")[3] if futures else "spot"
    while is_running:
        try:
            async with await _ws(url) as ws:
                while is_running:
                    msg = json.loads(await _until(ws))
                    sym = msg.get("stream", "").split("@")[0].upper()
                    sk = sinks.get(sym)
                    if not sk: continue
                    data = msg.get("data", {})
                    if "bids" in data and "asks" in data:            # spot partial depth (snapshot)
                        if sk["book"].should_update():
                            bk = Book(); bk.set_snapshot(data["bids"], data["asks"])
                            r = bk.top_row(DEPTH, exchange="binance", pair=sym)
                            if r: sk["book"].add(r)
                    elif data.get("e") == "depthUpdate":             # futures partial depth (top 20 absolute)
                        if sk["book"].should_update():
                            bk = Book(); bk.set_snapshot(data.get("b", []), data.get("a", []))
                            r = bk.top_row(DEPTH, exchange="binance", pair=sym)
                            if r: sk["book"].add(r)
                    elif data.get("e") in ("trade", "aggTrade"):
                        for t in binance_trades_parse(data, "binance", sym): sk["trade"].add(t)
                    elif data.get("e") == "markPriceUpdate" and sk.get("fund") is not None:
                        f = binance_fund_parse(data, "binance", sym)
                        if f: sk["fund"].add(f)
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"binance({'fut '+tag if futures else 'spot'}): {e!r}"); await asyncio.sleep(2)

async def bybit_multi(category, sinks, want_fund):
    url = f"wss://stream.bybit.com/v5/public/{category}"
    while is_running:
        books = {s: Book() for s in sinks}
        try:
            async with await _ws(url) as ws:
                args = []
                for s in sinks:
                    args += [f"orderbook.50.{s}", f"publicTrade.{s}"]
                    if want_fund: args.append(f"tickers.{s}")
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async def ping():
                    while is_running:
                        await asyncio.sleep(20)
                        try: await ws.send(json.dumps({"op": "ping"}))
                        except Exception: return
                pt = asyncio.create_task(ping())
                try:
                    while is_running:
                        m = json.loads(await _until(ws))
                        sym = str(m.get("topic", "")).split(".")[-1]
                        sk = sinks.get(sym)
                        if not sk: continue
                        pb = bybit_book_parse(m)
                        if pb:
                            action, b, a = pb; bk = books[sym]
                            bk.set_snapshot(b, a) if action == "snapshot" else bk.apply(b, a)
                            if bk.ready and sk["book"].should_update():
                                r = bk.top_row(DEPTH, exchange="bybit", pair=sym)
                                if r: sk["book"].add(r)
                            continue
                        for t in bybit_trades_parse(m, "bybit", sym): sk["trade"].add(t)
                        if want_fund and sk.get("fund") is not None:
                            f = bybit_fund_parse(m, "bybit", sym)
                            if f: sk["fund"].add(f)
                finally: pt.cancel()
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"bybit({category}): {e!r}"); await asyncio.sleep(2)

async def okx_multi(sinks, want_fund):
    while is_running:
        books = {s: Book() for s in sinks}; last = {s: -1 for s in sinks}
        try:
            async with await _ws("wss://ws.okx.com:8443/ws/v5/public") as ws:
                chans = []
                for s in sinks:
                    chans += [{"channel": "books", "instId": s}, {"channel": "trades", "instId": s}]
                    if want_fund: chans.append({"channel": "funding-rate", "instId": s})
                sub = {"op": "subscribe", "args": chans}
                await ws.send(json.dumps(sub))
                async def ping():
                    while is_running:
                        await asyncio.sleep(15)
                        try: await ws.send("ping")
                        except Exception: return
                pt = asyncio.create_task(ping())
                try:
                    while is_running:
                        raw = await _until(ws)
                        if raw == "pong": continue
                        m = json.loads(raw)
                        ch = m.get("arg", {}).get("channel"); sym = m.get("arg", {}).get("instId")
                        data = m.get("data"); sk = sinks.get(sym)
                        if not (data and sk): continue
                        if ch == "books":
                            d = data[0]; b, a = okx_book_parse(d)
                            seqid, prev = int(d.get("seqId", -1)), int(d.get("prevSeqId", -1))
                            if m.get("action") == "snapshot":
                                books[sym].set_snapshot(b, a); last[sym] = seqid
                            else:
                                if prev != last[sym]:
                                    books[sym] = Book(); last[sym] = -1; await ws.send(json.dumps(sub)); continue
                                books[sym].apply(b, a); last[sym] = seqid
                            if sk["book"].should_update():
                                r = books[sym].top_row(DEPTH, exchange="okx", pair=sym, seq=last[sym])
                                if r: sk["book"].add(r)
                        elif ch == "trades":
                            for t in okx_trades_parse(data, "okx", sym): sk["trade"].add(t)
                        elif ch == "funding-rate" and sk.get("fund") is not None:
                            sk["fund"].add(okx_fund_parse(data, "okx", sym))
                finally: pt.cancel()
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"okx: {e!r}"); await asyncio.sleep(2)

async def kucoin_spot_multi(sinks):
    while is_running:
        books = {s: Book() for s in sinks}; last = {s: 0 for s in sinks}; q = asyncio.Queue()
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api.kucoin.com/api/v1/bullet-public") as r:
                    tok = (await r.json())["data"]
                url = f"{tok['instanceServers'][0]['endpoint']}?token={tok['token']}&connectId={uuid.uuid4().hex}"
                async with await _ws(url) as ws:
                    for i, sym in enumerate(sinks):
                        await ws.send(json.dumps({"id": int(time.time()*1000)+i, "type": "subscribe",
                                                  "topic": f"/market/level2:{sym}", "response": True}))
                        await ws.send(json.dumps({"id": int(time.time()*1000)+1000+i, "type": "subscribe",
                                                  "topic": f"/market/match:{sym}", "response": True}))
                    async def rd():
                        try:
                            async for raw in ws: await q.put(json.loads(raw))
                        except Exception: pass
                    async def ping():
                        while is_running:
                            await asyncio.sleep(15)
                            try: await ws.send(json.dumps({"id": int(time.time()*1000), "type": "ping"}))
                            except Exception: return
                    rt, pt = asyncio.create_task(rd()), asyncio.create_task(ping())
                    await asyncio.sleep(1.0)
                    for sym in sinks:
                        async with s.get("https://api.kucoin.com/api/v1/market/orderbook/level2_100",
                                         params={"symbol": sym}) as r:
                            snap = (await r.json())["data"]
                        books[sym].set_snapshot(snap["bids"], snap["asks"]); last[sym] = int(snap["sequence"])
                    try:
                        while is_running:
                            m = await asyncio.wait_for(q.get(), timeout=20.0)
                            sym = str(m.get("topic", "")).split(":")[-1]; sk = sinks.get(sym)
                            if not sk: continue
                            pl = kucoin_l2_parse(m)
                            if pl:
                                sS, sE, b, a = pl
                                if sE <= last[sym]: continue
                                books[sym].apply(b, a); last[sym] = sE
                                if sk["book"].should_update():
                                    r = books[sym].top_row(DEPTH, exchange="kucoin", pair=sym, seq=sE)
                                    if r: sk["book"].add(r)
                                continue
                            for t in kucoin_spot_trades_parse(m, "kucoin", sym): sk["trade"].add(t)
                    finally: rt.cancel(); pt.cancel()
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"kucoin_spot: {e!r}"); await asyncio.sleep(2)

async def kucoin_fut_multi(sinks):
    while is_running:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api-futures.kucoin.com/api/v1/bullet-public") as r:
                    tok = (await r.json())["data"]
                url = f"{tok['instanceServers'][0]['endpoint']}?token={tok['token']}&connectId={uuid.uuid4().hex}"
                async with await _ws(url) as ws:
                    i = 0
                    for sym in sinks:
                        for topic in (f"/contractMarket/level2Depth50:{sym}",
                                      f"/contractMarket/execution:{sym}",
                                      f"/contract/instrument:{sym}"):
                            await ws.send(json.dumps({"id": int(time.time()*1000)+i, "type": "subscribe",
                                                      "topic": topic, "response": True})); i += 1
                    async def ping():
                        while is_running:
                            await asyncio.sleep(15)
                            try: await ws.send(json.dumps({"id": int(time.time()*1000), "type": "ping"}))
                            except Exception: return
                    pt = asyncio.create_task(ping())
                    try:
                        while is_running:
                            m = json.loads(await _until(ws))
                            sym = str(m.get("topic", "")).split(":")[-1]; sk = sinks.get(sym)
                            if not sk: continue
                            pb = kucoin_fut_depth_parse(m)
                            if pb:
                                bk = Book(); bk.set_snapshot(*pb)
                                if sk["book"].should_update():
                                    r = bk.top_row(DEPTH, exchange="kucoin", pair=sym)
                                    if r: sk["book"].add(r)
                                continue
                            for t in kucoin_fut_trades_parse(m, "kucoin", sym): sk["trade"].add(t)
                            f = kucoin_fut_fund_parse(m, "kucoin", sym)
                            if f and sk.get("fund") is not None: sk["fund"].add(f)
                    finally: pt.cancel()
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"kucoin_fut: {e!r}"); await asyncio.sleep(2)

async def hyperliquid_multi(sinks):
    while is_running:
        try:
            async with await _ws("wss://api.hyperliquid.xyz/ws") as ws:
                for coin in sinks:
                    for sub in ({"type": "l2Book", "coin": coin, "nSigFigs": 5, "nLevels": 20},
                                {"type": "trades", "coin": coin},
                                {"type": "activeAssetCtx", "coin": coin}):
                        await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
                async def ping():
                    while is_running:
                        await asyncio.sleep(30)
                        try: await ws.send(json.dumps({"method": "ping"}))
                        except Exception: return
                pt = asyncio.create_task(ping())
                try:
                    while is_running:
                        m = json.loads(await _until(ws))
                        chan = m.get("channel")
                        if chan == "l2Book":
                            d = m["data"]; coin = d.get("coin"); sk = sinks.get(coin)
                            if not sk: continue
                            lv = d["levels"]
                            bk = Book(); bk.set_snapshot([(x["px"], x["sz"]) for x in lv[0]], [(x["px"], x["sz"]) for x in lv[1]])
                            if sk["book"].should_update():
                                r = bk.top_row(DEPTH, exchange="hyperliquid", pair=coin)
                                if r: sk["book"].add(r)
                        elif chan == "trades":
                            for t in m.get("data", []):
                                sk = sinks.get(t.get("coin"))
                                if sk: sk["trade"].add(hl_trades_parse_one(t, "hyperliquid", t.get("coin")))
                        elif chan == "activeAssetCtx":
                            d = m["data"]; coin = d.get("coin"); sk = sinks.get(coin)
                            if sk and sk.get("fund") is not None:
                                f = hl_fund_parse_ctx(d.get("ctx", {}), "hyperliquid", coin)
                                if f["funding_rate"] is not None: sk["fund"].add(f)
                finally: pt.cancel()
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"hyperliquid: {e!r}"); await asyncio.sleep(2)

async def dydx_multi(sinks):
    while is_running:
        books = {s: Book() for s in sinks}
        try:
            async with await _ws("wss://indexer.dydx.trade/v4/ws") as ws:
                while is_running:
                    m = json.loads(await _until(ws))
                    if m.get("type") == "connected":
                        for tk in sinks:
                            await ws.send(json.dumps({"type": "subscribe", "channel": "v4_orderbook", "id": tk}))
                            await ws.send(json.dumps({"type": "subscribe", "channel": "v4_trades", "id": tk}))
                        await ws.send(json.dumps({"type": "subscribe", "channel": "v4_markets"}))
                        continue
                    chan = m.get("channel")
                    if chan == "v4_orderbook":
                        tk = m.get("id"); sk = sinks.get(tk)
                        if not sk: continue
                        c = m.get("contents", {}); typ = "snapshot" if m.get("type") == "subscribed" else "delta"
                        b, a = _norm(c.get("bids")), _norm(c.get("asks")); bk = books[tk]
                        if typ == "snapshot": bk.set_snapshot(b, a)
                        else:
                            for p, q in b:
                                if float(q) > 0: bk.asks.pop(float(p), None)
                            for p, q in a:
                                if float(q) > 0: bk.bids.pop(float(p), None)
                            bk.apply(b, a)
                        bk.prune_crossed()
                        if sk["book"].should_update():
                            r = bk.top_row(DEPTH, exchange="dydx", pair=tk)
                            if r: sk["book"].add(r)
                    elif chan == "v4_trades":
                        tk = m.get("id"); sk = sinks.get(tk)
                        if sk:
                            for t in dydx_trades_parse(m, "dydx", tk): sk["trade"].add(t)
                    elif chan == "v4_markets":
                        for tk in sinks:
                            if sinks[tk].get("fund") is None: continue
                            f = dydx_fund_parse(m, "dydx", tk)
                            if f and f["funding_rate"] is not None: sinks[tk]["fund"].add(f)
        except (asyncio.TimeoutError, TimeoutError): pass
        except Exception as e:
            if is_running: logger.error(f"dydx: {e!r}"); await asyncio.sleep(2)

# --------------------------------------------------------------------------- #
#                       PERIODIC FLUSH + WATCHDOG                            #
# --------------------------------------------------------------------------- #
async def flusher(S):
    while is_running:
        await asyncio.sleep(5)
        now = time.time()
        for s in S.values():
            if s.rows and (len(s.rows) >= FLUSH_MAX or now - s.last_flush >= FLUSH_INTERVAL):
                batch = s.rows; s.rows = []; s.last_flush = now
                try: await asyncio.to_thread(write_batch, s.meta, batch)   # write off the event loop
                except Exception as e: logger.error(f"flush thread err: {e}")

async def send_alert(subject: str, body: str):
    """Sends a Telegram alert via HTTPS API (port 443).
    Reads TELEGRAM_TOKEN and TELEGRAM_CHAT_ID from .env. Silent if absent."""
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        logger.warning("Telegram alert not sent: TELEGRAM_TOKEN/TELEGRAM_CHAT_ID missing")
        return
    text = f"[CryptoBot-{VENUE}] {subject}\n\n{body}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                     data={"chat_id": chat_id, "text": text}) as r:
                if r.status == 200:
                    logger.warning(f"Telegram alert sent: {subject}")
                else:
                    txt = await r.text()
                    logger.error(f"Telegram HTTP error {r.status}: {txt[:200]}")
    except Exception as e:
        logger.error(f"Telegram send error ({subject}): {e}")

async def watchdog(S):
    """Checks every WATCHDOG_SEC that each book feed is receiving data.
    Sends one alert on silence detection, one on recovery. No spam."""
    prev = {k: 0 for k in S}
    alerted = {k: False for k in S}

    while is_running:
        for _ in range(WATCHDOG_SEC):
            if not is_running: return
            await asyncio.sleep(1)

        for k, s in S.items():
            if s.meta[0] != "book": continue
            dt, asset, ex, pair = s.meta
            label = f"{ex}/{asset}/{pair}"

            if s.total == prev.get(k, 0):           # silence detected
                if not alerted[k]:
                    logger.warning(f"WATCHDOG silence: {label}")
                    msg = (f"Venue  : {ex}\nFeed   : {label}\n"
                           f"Silent for >{WATCHDOG_SEC}s\n"
                           f"Log    : /root/crypto_bot/logs/collector_{ex}.log")
                    await send_alert(f"Book feed silent: {label}", msg)
                    alerted[k] = True
            else:                                    # recovery
                if alerted[k]:
                    logger.warning(f"WATCHDOG recovery: {label}")
                    await send_alert(f"Recovery: {label}", f"Feed restored: {label}")
                    alerted[k] = False

            prev[k] = s.total

# --------------------------------------------------------------------------- #
#                              ORCHESTRATION                                  #
# --------------------------------------------------------------------------- #
async def main():
    logger.warning(f"Collector starting — venue={VENUE}")
    S = {}
    def sink(dt, at, ex, pair): S[(dt, at, ex, pair)] = Sink(dt, at, ex, pair); return S[(dt, at, ex, pair)]
    def mksinks(symbols, asset_t, with_fund):
        d = {}
        for sym in symbols:
            d[sym] = {"book": sink("book", asset_t, VENUE, sym), "trade": sink("trade", asset_t, VENUE, sym),
                      "fund": sink("funding", asset_t, VENUE, sym) if with_fund else None}
        return d

    tasks = []
    if VENUE == "binance":
        tasks += [binance_multi(False, mksinks(SPOT["binance"], "spot", False)),
                  binance_multi(True, mksinks(FUT["binance"], "future", True))]
    elif VENUE == "bybit":
        tasks += [bybit_multi("spot", mksinks(SPOT["bybit"], "spot", False), False),
                  bybit_multi("linear", mksinks(FUT["bybit"], "future", True), True)]
    elif VENUE == "okx":
        tasks += [okx_multi(mksinks(SPOT["okx"], "spot", False), False),
                  okx_multi(mksinks(FUT["okx"], "future", True), True)]
    elif VENUE == "kucoin":
        tasks += [kucoin_spot_multi(mksinks(SPOT["kucoin"], "spot", False)),
                  kucoin_fut_multi(mksinks(FUT["kucoin"], "future", True))]
    elif VENUE == "hyperliquid":
        tasks += [hyperliquid_multi(mksinks(DEX["hyperliquid"], "future", True))]
    elif VENUE == "dydx":
        tasks += [dydx_multi(mksinks(DEX["dydx"], "future", True))]

    tasks += [flusher(S), watchdog(S)]
    g = asyncio.gather(*tasks, return_exceptions=True)
    while is_running:
        await asyncio.sleep(1)

    logger.info("Shutdown requested — performing final flush.")
    g.cancel()
    for s in S.values():
        write_batch(s.meta, s.rows)
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
