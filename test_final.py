"""
FINAL TEST (60s) — Production-identical config, all venues, verdict per asset.
Validates locally before deploying to the VM.

INSTALL: pip install websockets aiohttp pandas pyarrow
RUN   : python test_final.py
"""
import asyncio, json, time, uuid, os, sys
from datetime import datetime, timezone
from pathlib import Path

DEPTH = 20
THROTTLE_MS = 100
DURATION = 60
BASE_DIR = Path("./crypto_db_test")
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

is_running = True
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
        if len(b) < depth or len(a) < depth: return None
        row = {"local_ts": int(time.time()*1000), **meta}
        for i in range(depth):
            row[f"bid_{i+1}"], row[f"bid_{i+1}_qty"] = b[i]
            row[f"ask_{i+1}"], row[f"ask_{i+1}_qty"] = a[i]
        return row

class Sink:
    def __init__(self, dt, asset, ex, pair):
        self.meta = (dt, asset, ex, pair); self.rows = []; self.last_ts = 0; self.total = 0
    def add(self, row):
        self.total += 1
        if self.meta[0] == "book":
            now = row.get("local_ts", int(time.time()*1000))
            if now - self.last_ts < THROTTLE_MS: return
            self.last_ts = now
        self.rows.append(row)

def write_hive(sink):
    if not (WRITE_PARQUET and sink.rows): return None
    import pandas as pd
    dt, asset, ex, pair = sink.meta
    d = datetime.now(timezone.utc).date()
    out = (BASE_DIR / f"data={dt}" / f"asset={asset}" / f"exchange={ex}"
           / f"pair={pair.replace('/','~').replace(':','~')}" / f"year={d.year:04d}" / f"month={d.month:02d}")
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(sink.rows).to_parquet(out / f"{d.isoformat()}.parquet",
        engine="pyarrow", compression="zstd", index=False)

def _norm(side):
    out = []
    for lvl in side or []:
        if isinstance(lvl, dict): out.append((lvl.get("price"), lvl.get("size")))
        else: out.append((lvl[0], lvl[1]))
    return out

async def _ws(url, ping_interval=None):
    import websockets
    return websockets.connect(url, ping_interval=ping_interval, max_queue=None, open_timeout=15)
async def _until(ws):
    return await asyncio.wait_for(ws.recv(), timeout=20.0)

# ----------- PARSING (identical to production) -----------
def bybit_book_parse(m):
    if not str(m.get("topic","")).startswith("orderbook."): return None
    d=m.get("data",{}); return m.get("type"),d.get("b",[]),d.get("a",[])
def okx_book_parse(d):
    return [(l[0],l[1]) for l in d.get("bids",[])],[(l[0],l[1]) for l in d.get("asks",[])]
def kucoin_l2_parse(m):
    if m.get("subject")!="trade.l2update": return None
    d=m["data"]; ch=d["changes"]
    return (int(d["sequenceStart"]),int(d["sequenceEnd"]),[(c[0],c[1]) for c in ch.get("bids",[])],[(c[0],c[1]) for c in ch.get("asks",[])])
def kucoin_fut_depth_parse(m):
    if "level2Depth50" not in str(m.get("topic","")): return None
    d=m.get("data",{}); return ([(l[0],l[1]) for l in d.get("bids",[])],[(l[0],l[1]) for l in d.get("asks",[])])
def _trade(price,amount,side,ts,tid,exchange,pair):
    return {"local_ts":int(time.time()*1000),"exchange_ts":ts,"exchange":exchange,"pair":pair,
            "trade_id":str(tid) if tid is not None else None,"price":float(price),"amount":float(amount),"side":str(side).lower()}
def binance_trades_parse(m,ex,pair):
    if m.get("e") not in ("trade","aggTrade"): return []
    return [_trade(m["p"],m["q"],"sell" if m.get("m") else "buy",m.get("T"),m.get("t",m.get("a")),ex,pair)]
def bybit_trades_parse(m,ex,pair):
    if not str(m.get("topic","")).startswith("publicTrade."): return []
    return [_trade(d["p"],d["v"],d["S"],d.get("T"),d.get("i"),ex,pair) for d in m.get("data",[])]
def okx_trades_parse(data,ex,pair):
    return [_trade(d["px"],d["sz"],d["side"],int(d["ts"]),d.get("tradeId"),ex,pair) for d in data]
def kucoin_spot_trades_parse(m,ex,pair):
    if m.get("subject")!="trade.l3match": return []
    d=m["data"]; return [_trade(d["price"],d["size"],d["side"],d.get("time"),d.get("tradeId"),ex,pair)]
def kucoin_fut_trades_parse(m,ex,pair):
    if m.get("subject")!="match": return []
    d=m["data"]; return [_trade(d["price"],d.get("size",d.get("matchSize")),d["side"],d.get("ts",d.get("time")),d.get("tradeId"),ex,pair)]
def hl_trades_parse_one(t,ex,pair):
    s=t.get("side"); return _trade(t["px"],t["sz"],{"B":"buy","A":"sell"}.get(s,str(s).lower()),t.get("time"),t.get("tid"),ex,pair)
def dydx_trades_parse(m,ex,pair):
    if m.get("channel")!="v4_trades": return []
    return [_trade(t["price"],t["size"],t["side"],t.get("createdAt"),t.get("id"),ex,pair) for t in m.get("contents",{}).get("trades",[])]
def _fund(ex,pair,rate,**kw):
    return {"local_ts":int(time.time()*1000),"exchange":ex,"pair":pair,"funding_rate":float(rate) if rate not in (None,"") else None,**kw}
def binance_fund_parse(m,ex,pair):
    if m.get("e")!="markPriceUpdate": return None
    return _fund(ex,pair,m.get("r"),next_funding_ts=m.get("T"),mark_price=m.get("p"),index_price=m.get("i"))
def bybit_fund_parse(m,ex,pair):
    if not str(m.get("topic","")).startswith("tickers."): return None
    d=m.get("data",{});
    if "fundingRate" not in d: return None
    return _fund(ex,pair,d.get("fundingRate"),next_funding_ts=d.get("nextFundingTime"),mark_price=d.get("markPrice"))
def okx_fund_parse(data,ex,pair):
    d=data[0]; return _fund(ex,pair,d.get("fundingRate"),funding_ts=d.get("fundingTime"),next_funding_ts=d.get("nextFundingTime"))
def kucoin_fut_fund_parse(m,ex,pair):
    if m.get("subject")!="funding.rate": return None
    d=m["data"]; return _fund(ex,pair,d.get("fundingRate"),funding_ts=d.get("timestamp"))
def hl_fund_parse_ctx(ctx,ex,pair):
    return _fund(ex,pair,ctx.get("funding"),mark_price=ctx.get("markPx"),oracle_price=ctx.get("oraclePx"))
def dydx_fund_parse(m,ex,pair):
    if m.get("channel")!="v4_markets": return None
    c=m.get("contents",{}); markets=c.get("markets",c)
    mk=markets.get(pair) if isinstance(markets,dict) else None
    if not mk or ("nextFundingRate" not in mk and "oraclePrice" not in mk): return None
    return _fund(ex,pair,mk.get("nextFundingRate"),oracle_price=mk.get("oraclePrice"))

# ----------- COLLECTORS (identical to production, is_running) -----------
async def binance_multi(futures,sinks):
    import aiohttp
    syms=[s.lower() for s in sinks]
    if futures:
        depth="/".join(f"{x}@depth20@100ms" for x in syms)
        mkt="/".join([f"{x}@aggTrade" for x in syms]+[f"{x}@markPrice@1s" for x in syms])
        urls=[f"wss://fstream.binance.com/public/stream?streams={depth}",
              f"wss://fstream.binance.com/market/stream?streams={mkt}"]
    else:
        st="/".join([f"{x}@depth20@100ms" for x in syms]+[f"{x}@trade" for x in syms])
        urls=[f"wss://stream.binance.com:9443/stream?streams={st}"]
    await asyncio.gather(*[_binance_conn(u,sinks,futures) for u in urls])
async def _binance_conn(url,sinks,futures):
    while is_running:
        try:
            async with await _ws(url) as ws:
                while is_running:
                    msg=json.loads(await _until(ws)); sym=msg.get("stream","").split("@")[0].upper()
                    sk=sinks.get(sym)
                    if not sk: continue
                    data=msg.get("data",{})
                    if "bids" in data and "asks" in data:
                        bk=Book();bk.set_snapshot(data["bids"],data["asks"]);r=bk.top_row(DEPTH,exchange="binance",pair=sym)
                        if r: sk["book"].add(r)
                    elif data.get("e")=="depthUpdate":
                        bk=Book();bk.set_snapshot(data.get("b",[]),data.get("a",[]));r=bk.top_row(DEPTH,exchange="binance",pair=sym)
                        if r: sk["book"].add(r)
                    elif data.get("e") in ("trade","aggTrade"):
                        for t in binance_trades_parse(data,"binance",sym): sk["trade"].add(t)
                    elif data.get("e")=="markPriceUpdate" and sk.get("fund") is not None:
                        f=binance_fund_parse(data,"binance",sym)
                        if f: sk["fund"].add(f)
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  binance: {e!r}"); await asyncio.sleep(2)

async def bybit_multi(category,sinks,want_fund):
    while is_running:
        books={s:Book() for s in sinks}
        try:
            async with await _ws(f"wss://stream.bybit.com/v5/public/{category}") as ws:
                args=[]
                for s in sinks: args+=[f"orderbook.50.{s}",f"publicTrade.{s}"]; (args.append(f"tickers.{s}") if want_fund else None)
                await ws.send(json.dumps({"op":"subscribe","args":args}))
                async def ping():
                    while is_running:
                        await asyncio.sleep(20)
                        try: await ws.send(json.dumps({"op":"ping"}))
                        except: return
                pt=asyncio.create_task(ping())
                try:
                    while is_running:
                        m=json.loads(await _until(ws)); sym=str(m.get("topic","")).split(".")[-1]; sk=sinks.get(sym)
                        if not sk: continue
                        pb=bybit_book_parse(m)
                        if pb:
                            a,b,c=pb; bk=books[sym]; (bk.set_snapshot(b,c) if a=="snapshot" else bk.apply(b,c))
                            r=bk.top_row(DEPTH,exchange="bybit",pair=sym)
                            if r: sk["book"].add(r); continue
                        for t in bybit_trades_parse(m,"bybit",sym): sk["trade"].add(t)
                        if want_fund and sk.get("fund"):
                            f=bybit_fund_parse(m,"bybit",sym)
                            if f: sk["fund"].add(f)
                finally: pt.cancel()
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  bybit({category}): {e!r}"); await asyncio.sleep(2)

async def okx_multi(sinks,want_fund):
    while is_running:
        books={s:Book() for s in sinks}; last={s:-1 for s in sinks}
        try:
            async with await _ws("wss://ws.okx.com:8443/ws/v5/public") as ws:
                chans=[]
                for s in sinks:
                    chans+=[{"channel":"books","instId":s},{"channel":"trades","instId":s}]
                    if want_fund: chans.append({"channel":"funding-rate","instId":s})
                sub={"op":"subscribe","args":chans}; await ws.send(json.dumps(sub))
                async def ping():
                    while is_running:
                        await asyncio.sleep(15)
                        try: await ws.send("ping")
                        except: return
                pt=asyncio.create_task(ping())
                try:
                    while is_running:
                        raw=await _until(ws)
                        if raw=="pong": continue
                        m=json.loads(raw); ch=m.get("arg",{}).get("channel"); sym=m.get("arg",{}).get("instId")
                        data=m.get("data"); sk=sinks.get(sym)
                        if not (data and sk): continue
                        if ch=="books":
                            d=data[0]; b,a=okx_book_parse(d); seqid,prev=int(d.get("seqId",-1)),int(d.get("prevSeqId",-1))
                            if m.get("action")=="snapshot": books[sym].set_snapshot(b,a); last[sym]=seqid
                            else:
                                if prev!=last[sym]: books[sym]=Book(); last[sym]=-1; await ws.send(json.dumps(sub)); continue
                                books[sym].apply(b,a); last[sym]=seqid
                            r=books[sym].top_row(DEPTH,exchange="okx",pair=sym)
                            if r: sk["book"].add(r)
                        elif ch=="trades":
                            for t in okx_trades_parse(data,"okx",sym): sk["trade"].add(t)
                        elif ch=="funding-rate" and sk.get("fund") is not None:
                            sk["fund"].add(okx_fund_parse(data,"okx",sym))
                finally: pt.cancel()
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  okx: {e!r}"); await asyncio.sleep(2)

async def kucoin_spot_multi(sinks):
    import aiohttp
    while is_running:
        books={s:Book() for s in sinks}; last={s:0 for s in sinks}; q=asyncio.Queue()
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api.kucoin.com/api/v1/bullet-public") as r: tok=(await r.json())["data"]
                url=f"{tok['instanceServers'][0]['endpoint']}?token={tok['token']}&connectId={uuid.uuid4().hex}"
                async with await _ws(url) as ws:
                    for i,sym in enumerate(sinks):
                        await ws.send(json.dumps({"id":int(time.time()*1000)+i,"type":"subscribe","topic":f"/market/level2:{sym}","response":True}))
                        await ws.send(json.dumps({"id":int(time.time()*1000)+1000+i,"type":"subscribe","topic":f"/market/match:{sym}","response":True}))
                    async def rd():
                        try:
                            async for raw in ws: await q.put(json.loads(raw))
                        except: pass
                    async def ping():
                        while is_running:
                            await asyncio.sleep(15)
                            try: await ws.send(json.dumps({"id":int(time.time()*1000),"type":"ping"}))
                            except: return
                    rt,pt=asyncio.create_task(rd()),asyncio.create_task(ping())
                    await asyncio.sleep(1.0)
                    for sym in sinks:
                        async with s.get("https://api.kucoin.com/api/v1/market/orderbook/level2_100",params={"symbol":sym}) as r:
                            snap=(await r.json())["data"]
                        books[sym].set_snapshot(snap["bids"],snap["asks"]); last[sym]=int(snap["sequence"])
                    try:
                        while is_running:
                            m=await asyncio.wait_for(q.get(),timeout=20.0); sym=str(m.get("topic","")).split(":")[-1]; sk=sinks.get(sym)
                            if not sk: continue
                            pl=kucoin_l2_parse(m)
                            if pl:
                                sS,sE,b,a=pl
                                if sE<=last[sym]: continue
                                books[sym].apply(b,a); last[sym]=sE; r=books[sym].top_row(DEPTH,exchange="kucoin",pair=sym,seq=sE)
                                if r: sk["book"].add(r); continue
                            for t in kucoin_spot_trades_parse(m,"kucoin",sym): sk["trade"].add(t)
                    finally: rt.cancel(); pt.cancel()
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  kucoin_spot: {e!r}"); await asyncio.sleep(2)

async def kucoin_fut_multi(sinks):
    import aiohttp
    while is_running:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api-futures.kucoin.com/api/v1/bullet-public") as r: tok=(await r.json())["data"]
                url=f"{tok['instanceServers'][0]['endpoint']}?token={tok['token']}&connectId={uuid.uuid4().hex}"
                async with await _ws(url) as ws:
                    i=0
                    for sym in sinks:
                        for topic in (f"/contractMarket/level2Depth50:{sym}",f"/contractMarket/execution:{sym}",f"/contract/instrument:{sym}"):
                            await ws.send(json.dumps({"id":int(time.time()*1000)+i,"type":"subscribe","topic":topic,"response":True})); i+=1
                    async def ping():
                        while is_running:
                            await asyncio.sleep(15)
                            try: await ws.send(json.dumps({"id":int(time.time()*1000),"type":"ping"}))
                            except: return
                    pt=asyncio.create_task(ping())
                    try:
                        while is_running:
                            m=json.loads(await _until(ws)); sym=str(m.get("topic","")).split(":")[-1]; sk=sinks.get(sym)
                            if not sk: continue
                            pb=kucoin_fut_depth_parse(m)
                            if pb:
                                bk=Book();bk.set_snapshot(*pb);r=bk.top_row(DEPTH,exchange="kucoin",pair=sym)
                                if r: sk["book"].add(r); continue
                            for t in kucoin_fut_trades_parse(m,"kucoin",sym): sk["trade"].add(t)
                            f=kucoin_fut_fund_parse(m,"kucoin",sym)
                            if f and sk.get("fund") is not None: sk["fund"].add(f)
                    finally: pt.cancel()
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  kucoin_fut: {e!r}"); await asyncio.sleep(2)

async def hyperliquid_multi(sinks):
    while is_running:
        try:
            async with await _ws("wss://api.hyperliquid.xyz/ws") as ws:
                for coin in sinks:
                    for sub in ({"type":"l2Book","coin":coin,"nSigFigs":5,"nLevels":20},{"type":"trades","coin":coin},{"type":"activeAssetCtx","coin":coin}):
                        await ws.send(json.dumps({"method":"subscribe","subscription":sub}))
                async def ping():
                    while is_running:
                        await asyncio.sleep(30)
                        try: await ws.send(json.dumps({"method":"ping"}))
                        except: return
                pt=asyncio.create_task(ping())
                try:
                    while is_running:
                        m=json.loads(await _until(ws)); chan=m.get("channel")
                        if chan=="l2Book":
                            d=m["data"];coin=d.get("coin");sk=sinks.get(coin)
                            if not sk: continue
                            lv=d["levels"]; bk=Book(); bk.set_snapshot([(x["px"],x["sz"]) for x in lv[0]],[(x["px"],x["sz"]) for x in lv[1]])
                            r=bk.top_row(DEPTH,exchange="hyperliquid",pair=coin)
                            if r: sk["book"].add(r)
                        elif chan=="trades":
                            for t in m.get("data",[]):
                                sk=sinks.get(t.get("coin")); (sk["trade"].add(hl_trades_parse_one(t,"hyperliquid",t.get("coin"))) if sk else None)
                        elif chan=="activeAssetCtx":
                            d=m["data"];coin=d.get("coin");sk=sinks.get(coin)
                            if sk and sk.get("fund"):
                                f=hl_fund_parse_ctx(d.get("ctx",{}),"hyperliquid",coin)
                                if f["funding_rate"] is not None: sk["fund"].add(f)
                finally: pt.cancel()
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  hyperliquid: {e!r}"); await asyncio.sleep(2)

async def dydx_multi(sinks):
    while is_running:
        books={s:Book() for s in sinks}
        try:
            async with await _ws("wss://indexer.dydx.trade/v4/ws") as ws:
                while is_running:
                    m=json.loads(await _until(ws))
                    if m.get("type")=="connected":
                        for tk in sinks:
                            await ws.send(json.dumps({"type":"subscribe","channel":"v4_orderbook","id":tk}))
                            await ws.send(json.dumps({"type":"subscribe","channel":"v4_trades","id":tk}))
                        await ws.send(json.dumps({"type":"subscribe","channel":"v4_markets"}))
                        continue
                    chan=m.get("channel")
                    if chan=="v4_orderbook":
                        tk=m.get("id");sk=sinks.get(tk)
                        if not sk: continue
                        c=m.get("contents",{}); typ="snapshot" if m.get("type")=="subscribed" else "delta"
                        b,a=_norm(c.get("bids")),_norm(c.get("asks")); bk=books[tk]
                        if typ=="snapshot": bk.set_snapshot(b,a)
                        else:
                            for p,q in b:
                                if float(q)>0: bk.asks.pop(float(p),None)
                            for p,q in a:
                                if float(q)>0: bk.bids.pop(float(p),None)
                            bk.apply(b,a)
                        bk.prune_crossed(); r=bk.top_row(DEPTH,exchange="dydx",pair=tk)
                        if r: sk["book"].add(r)
                    elif chan=="v4_trades":
                        tk=m.get("id");sk=sinks.get(tk)
                        if sk:
                            for t in dydx_trades_parse(m,"dydx",tk): sk["trade"].add(t)
                    elif chan=="v4_markets":
                        for tk in sinks:
                            if not sinks[tk].get("fund"): continue
                            f=dydx_fund_parse(m,"dydx",tk)
                            if f and f["funding_rate"] is not None: sinks[tk]["fund"].add(f)
        except (asyncio.TimeoutError,TimeoutError): pass
        except Exception as e:
            if is_running: print(f"  dydx: {e!r}"); await asyncio.sleep(2)

# ----------- VERDICT -----------
def verdict(S):
    import pandas as pd
    groups = {}
    for (dt,asset,ex,pair),s in S.items():
        key = (ex, asset, dt)
        groups.setdefault(key, {})[pair] = len(s.rows)
    for section,label in [("book","BOOK"),("trade","TRADES"),("funding","FUNDING")]:
        print(f"\n{'='*20} {label} {'='*20}")
        for (ex,asset,dt),pairs in sorted(groups.items()):
            if dt != section: continue
            parts = " ".join(f"{p}={n}" for p,n in sorted(pairs.items()))
            flag = "✅" if all(n > 0 for n in pairs.values()) else "⚠️ "
            print(f"  {flag} {ex:14s} {asset:7s}: {parts}")

# ----------- MAIN -----------
async def main():
    global is_running
    S = {}
    def sink(dt,at,ex,pair): S[(dt,at,ex,pair)]=Sink(dt,at,ex,pair); return S[(dt,at,ex,pair)]
    def mksinks(symbols,asset_t,ex,with_fund):
        d={}
        for sym in symbols:
            d[sym]={"book":sink("book",asset_t,ex,sym),"trade":sink("trade",asset_t,ex,sym),
                    "fund":sink("funding",asset_t,ex,sym) if with_fund else None}
        return d

    print(f"⏳ Final test {DURATION}s — production config (L20, 100ms throttle, USDT+USDC)...\n")
    tasks = []
    for ex in ("binance","bybit","okx","kucoin"):
        sp=mksinks(SPOT[ex],"spot",ex,False); fu=mksinks(FUT[ex],"future",ex,True)
        if ex=="binance": tasks+=[binance_multi(False,sp),binance_multi(True,fu)]
        elif ex=="bybit": tasks+=[bybit_multi("spot",sp,False),bybit_multi("linear",fu,True)]
        elif ex=="okx": tasks+=[okx_multi(sp,False),okx_multi(fu,True)]
        elif ex=="kucoin": tasks+=[kucoin_spot_multi(sp),kucoin_fut_multi(fu)]
    tasks+=[hyperliquid_multi(mksinks(DEX["hyperliquid"],"future","hyperliquid",True)),
            dydx_multi(mksinks(DEX["dydx"],"future","dydx",True))]

    async def timer():
        global is_running; await asyncio.sleep(DURATION); is_running = False
    tasks.append(timer())

    await asyncio.gather(*tasks, return_exceptions=True)

    verdict(S)
    if WRITE_PARQUET:
        n=sum(1 for s in S.values() if write_hive(s) is not None or len(s.rows)==0)
        print(f"\n{sum(1 for s in S.values() if s.rows)} feeds written under {BASE_DIR}/")
    print("\nDone.")

if __name__ == "__main__":
    asyncio.run(main())
