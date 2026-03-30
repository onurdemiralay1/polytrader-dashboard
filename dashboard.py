#!/usr/bin/env python3
"""
Live Dashboard for Polymarket Trading Bots
Usage: python3 dashboard.py [port]
"""

import json
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
import threading
import time
import sys

BOTS = {
    "iphone1": {"address": "0x1f19c48AEe80Ec95396d91f0d21Ac249b8a7f57a", "color": "#4ade80", "label": "DO Amsterdam"},
    "AWS":     {"address": "0xD4f619457619BbAc110BA5f983f12C4636EBc1a9", "color": "#60a5fa", "label": "AWS Original"},
    "Stop4":   {"address": "0xb43aA6ADc20c12a17B403480bc9422BbBC9E484D", "color": "#fb923c", "label": "AWS Stop@4"},
    "BSO":     {"address": "0x46b2f8a865a3572a435813e747e22cce0e835977", "color": "#f472b6", "label": "Onur BSO"},
}

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
RPC_URLS = ["https://polygon.gateway.tenderly.co", "https://polygon.drpc.org"]

cache = {}
cache_lock = threading.Lock()
CACHE_TTL = 30


def fetch_activity(address, limit=500, max_pages=8):
    all_events = []
    for page in range(max_pages):
        url = f"https://data-api.polymarket.com/activity?user={address}&limit={limit}&offset={page*limit}"
        req = urllib.request.Request(url, headers={"User-Agent": "PolyDash/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            if not data:
                break
            all_events.extend(data)
            if len(data) < limit:
                break
        except Exception as e:
            print(f"  page {page} error: {e}")
            break
    return all_events


def fetch_positions(address):
    """Fetch current positions from Polymarket data API."""
    url = f"https://data-api.polymarket.com/positions?user={address}"
    req = urllib.request.Request(url, headers={"User-Agent": "PolyDash/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  positions error: {e}")
        return []


# Cache token IDs per slug (fetched from gamma once per period)
_token_cache = {"slug": None, "up_token": None, "dn_token": None, "title": ""}


def _ensure_tokens(slug):
    """Fetch token IDs from gamma API if slug changed."""
    if _token_cache["slug"] == slug:
        return True
    url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
    req = urllib.request.Request(url, headers={"User-Agent": "PolyDash/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        if data:
            m = data[0]
            tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
            _token_cache["slug"] = slug
            _token_cache["up_token"] = tokens[0]
            _token_cache["dn_token"] = tokens[1]
            _token_cache["title"] = m.get("question", "")
            return True
    except Exception as e:
        print(f"  gamma token fetch error: {e}")
    return False


def _clob_midpoint(token_id):
    """Fetch midpoint from CLOB API (real-time)."""
    url = f"https://clob.polymarket.com/midpoint?token_id={token_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "PolyDash/1.0"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read())
    return float(data["mid"])


def fetch_live_prices():
    """Fetch live Up/Down prices using CLOB midpoint (real-time)."""
    now = int(time.time())
    period_start = (now // 900) * 900
    slug = f"btc-updown-15m-{period_start}"
    if not _ensure_tokens(slug):
        return None
    try:
        up_price = _clob_midpoint(_token_cache["up_token"])
        dn_price = _clob_midpoint(_token_cache["dn_token"])
        return {"slug": slug, "up": up_price, "down": dn_price,
                "title": _token_cache["title"], "ts": now, "period_start": period_start}
    except Exception as e:
        print(f"  clob price error: {e}")
        return None


def fetch_balances():
    """Batch fetch USDC.e balances for all bots via single RPC call."""
    batch = []
    bot_list = list(BOTS.items())
    for i, (name, bot) in enumerate(bot_list):
        addr_padded = "000000000000000000000000" + bot["address"][2:].lower()
        data = "0x70a08231" + addr_padded
        batch.append({"jsonrpc": "2.0", "method": "eth_call",
                       "params": [{"to": USDC_E, "data": data}, "latest"], "id": i})
    payload = json.dumps(batch).encode()
    for rpc_url in RPC_URLS:
        try:
            req = urllib.request.Request(rpc_url, data=payload,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                results = json.loads(resp.read())
            balances = {}
            for r in results:
                name = bot_list[r["id"]][0]
                balances[name] = round(int(r["result"], 16) / 1e6, 2)
            return balances
        except Exception as e:
            print(f"  balance RPC error ({rpc_url}): {e}")
    return {}


def ensure_period(periods, slug, ts, title):
    if slug not in periods:
        periods[slug] = {
            "buy": 0, "sell": 0, "redeem": 0, "trades": 0,
            "ts": ts, "title": title, "trade_list": []
        }


def process_bot_data(events):
    now = time.time()
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    h24 = now - 86400
    h1 = now - 3600

    buckets = {k: {"buy": 0, "sell": 0, "redeem": 0, "rebate": 0, "trades": 0, "redeems": 0}
               for k in ("all", "24h", "today", "1h")}
    periods = {}
    recent_trades = []
    last_active = 0

    for e in events:
        ts = e.get("timestamp", 0)
        usdc = float(e.get("usdcSize", 0) or 0)
        etype = e.get("type", "")
        slug = e.get("slug", "") or e.get("eventSlug", "")
        title = e.get("title", "")

        if ts > last_active:
            last_active = ts

        active = ["all"]
        if ts >= h24: active.append("24h")
        if ts >= today_start: active.append("today")
        if ts >= h1: active.append("1h")

        if etype == "TRADE":
            side = e.get("side", "")
            size = float(e.get("size", 0) or 0)
            price = float(e.get("price", 0) or 0)
            outcome = e.get("outcome", "")
            for b in active:
                buckets[b]["trades"] += 1
                buckets[b]["buy" if side == "BUY" else "sell"] += usdc

            if slug:
                ensure_period(periods, slug, ts, title)
                p = periods[slug]
                p["trades"] += 1
                p["buy" if side == "BUY" else "sell"] += usdc
                if p["ts"] < ts:
                    p["ts"] = ts
                p["trade_list"].append({
                    "t": ts, "side": side, "size": size,
                    "price": price, "usdc": usdc, "outcome": outcome,
                })

            if len(recent_trades) < 30:
                recent_trades.append({
                    "t": ts, "side": side, "size": size,
                    "price": price, "usdc": usdc, "outcome": outcome,
                    "title": title,
                })

        elif etype == "REDEEM":
            for b in active:
                buckets[b]["redeem"] += usdc
                buckets[b]["redeems"] += 1
            if slug:
                ensure_period(periods, slug, ts, title)
                periods[slug]["redeem"] += usdc

        elif etype == "MAKER_REBATE":
            for b in active:
                buckets[b]["rebate"] += usdc

    metrics = {}
    for bname, bd in buckets.items():
        pnl = -bd["buy"] + bd["sell"] + bd["redeem"] + bd["rebate"]
        metrics[bname] = {
            "pnl": round(pnl, 2), "trades": bd["trades"],
            "redeems": bd["redeems"], "volume": round(bd["buy"] + bd["sell"], 2),
            "rebates": round(bd["rebate"], 2),
        }

    period_list = []
    for slug, p in sorted(periods.items(), key=lambda x: x[1]["ts"], reverse=True):
        ppnl = -p["buy"] + p["sell"] + p["redeem"]
        try:
            period_start = int(slug.split("-")[-1])
        except:
            period_start = 0
        resolved = p["redeem"] > 0 or (period_start > 0 and now - period_start > 1200)
        entry = {
            "slug": slug, "title": p["title"], "pnl": round(ppnl, 2),
            "trades": p["trades"], "ts": p["ts"], "resolved": resolved,
            "period_start": period_start, "won": p["redeem"] > 0,
        }
        if len(period_list) < 3:
            entry["trade_list"] = sorted(p["trade_list"], key=lambda x: x["t"])
        period_list.append(entry)

    return {
        "metrics": metrics,
        "recent_trades": recent_trades,
        "last_active": last_active,
        "events": len(events),
        "periods": period_list[:80],
    }


def process_positions(positions):
    """Extract useful position data: portfolio value, per-market positions."""
    total_value = 0
    total_cost = 0
    active = []
    for p in positions:
        size = float(p.get("size", 0) or 0)
        if size <= 0:
            continue
        cur_val = float(p.get("currentValue", 0) or 0)
        init_val = float(p.get("initialValue", 0) or 0)
        total_value += cur_val
        total_cost += init_val
        active.append({
            "outcome": p.get("outcome", ""),
            "slug": p.get("slug", ""),
            "title": p.get("title", ""),
            "size": round(size, 2),
            "avgPrice": round(float(p.get("avgPrice", 0) or 0), 4),
            "curPrice": float(p.get("curPrice", 0) or 0),
            "value": round(cur_val, 2),
            "cost": round(init_val, 2),
            "pnl": round(float(p.get("cashPnl", 0) or 0), 2),
            "redeemable": p.get("redeemable", False),
        })
    # PnL by slug for unresolved period correction
    pnl_by_slug = {}
    for p in positions:
        slug = p.get("slug", "")
        if slug and float(p.get("size", 0) or 0) > 0:
            pnl_by_slug[slug] = pnl_by_slug.get(slug, 0) + float(p.get("cashPnl", 0) or 0)
    return {
        "portfolio_value": round(total_value, 2),
        "portfolio_cost": round(total_cost, 2),
        "unrealized_pnl": round(total_value - total_cost, 2),
        "positions": active,
        "pnl_by_slug": {k: round(v, 2) for k, v in pnl_by_slug.items()},
    }


def refresh_all():
    new_cache = {"_ts": time.time(), "_bots": {}}

    # Fetch USDC balances (single batch call)
    balances = fetch_balances()

    for name, bot in BOTS.items():
        try:
            events = fetch_activity(bot["address"])
            data = process_bot_data(events)
            data["label"] = bot["label"]
            data["color"] = bot["color"]
            data["address"] = bot["address"]

            # Positions
            raw_pos = fetch_positions(bot["address"])
            pos_data = process_positions(raw_pos)
            data["positions"] = pos_data["positions"]
            data["portfolio_value"] = pos_data["portfolio_value"]
            data["unrealized_pnl"] = pos_data["unrealized_pnl"]
            data["pos_pnl"] = pos_data["pnl_by_slug"]

            # Cash balance
            data["cash"] = balances.get(name)

            new_cache["_bots"][name] = data
            m24 = data["metrics"]["24h"]
            cash_str = f"${data['cash']:.0f}" if data["cash"] is not None else "?"
            print(f"  {name}: 24h=${m24['pnl']}, cash={cash_str}, portfolio=${pos_data['portfolio_value']:.0f}")
        except Exception as e:
            print(f"  {name} ERROR: {e}")
            new_cache["_bots"][name] = {
                "error": str(e), "label": bot["label"],
                "color": bot["color"], "address": bot["address"]
            }
    with cache_lock:
        cache.update(new_cache)
    print(f"  refreshed at {datetime.now().strftime('%H:%M:%S')}")


def bg_refresh():
    while True:
        try:
            refresh_all()
        except Exception as e:
            print(f"refresh error: {e}")
        time.sleep(CACHE_TTL)


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Polytrader Live</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#08080c;--surface:#111118;--border:#1a1a25;--border2:#252535;
  --text:#d4d4dc;--dim:#555568;--green:#4ade80;--red:#f87171;
  --blue:#60a5fa;--orange:#fb923c;--pink:#f472b6;
}
body{background:var(--bg);color:var(--text);font-family:'SF Mono','Fira Code',monospace;font-size:12px;padding:16px 20px}

.hdr{display:flex;justify-content:space-between;align-items:center;padding-bottom:12px;border-bottom:1px solid var(--border);margin-bottom:14px}
.hdr h1{font-size:15px;font-weight:600;letter-spacing:-0.3px}
.hdr .st{color:var(--dim);font-size:11px;display:flex;align-items:center;gap:8px}
.hdr .dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.rbtn{background:none;border:1px solid var(--border);color:var(--dim);padding:3px 10px;border-radius:3px;cursor:pointer;font:inherit;font-size:10px}
.rbtn:hover{color:var(--text);border-color:var(--border2)}

.tabs{display:flex;gap:3px;margin-bottom:14px}
.tabs button{background:var(--surface);border:1px solid var(--border);color:var(--dim);padding:5px 12px;border-radius:4px;cursor:pointer;font:inherit;font-size:11px}
.tabs button:hover{color:var(--text)}
.tabs button.on{background:var(--border);color:var(--text);border-color:var(--border2)}

.strip{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.scard{background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:12px 14px;position:relative;overflow:hidden}
.scard::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--c)}
.scard .nm{font-size:11px;color:var(--dim);margin-bottom:2px;display:flex;justify-content:space-between;align-items:center}
.scard .nm .badge{font-size:9px;padding:1px 5px;border-radius:3px}
.scard .nm .badge.active{background:rgba(74,222,128,.15);color:var(--green)}
.scard .nm .badge.stale{background:rgba(251,146,60,.15);color:var(--orange)}
.scard .nm .badge.offline{background:rgba(248,113,113,.15);color:var(--red)}
.scard .pnl{font-size:22px;font-weight:700;margin:4px 0}
.scard .row{display:flex;gap:12px;font-size:10px;color:var(--dim);flex-wrap:wrap}
.scard .row span b{color:var(--text);font-weight:500}
.scard .bal-row{display:flex;gap:12px;font-size:10px;color:var(--dim);margin-top:4px;padding-top:4px;border-top:1px solid var(--border)}
.scard .bal-row span b{color:var(--text);font-weight:500}

.pos{color:var(--green)}.neg{color:var(--red)}

.cur-section{background:var(--surface);border:1px solid var(--border);border-radius:6px;margin-bottom:14px;overflow:hidden}
.cur-hdr{padding:10px 14px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
.cur-hdr .title{font-weight:600;font-size:12px}
.cur-hdr .timer{color:var(--dim);font-size:11px}
.cur-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border)}
.cur-bot{background:var(--surface);padding:10px 12px}
.cur-bot .bn{font-size:10px;color:var(--dim);margin-bottom:6px;display:flex;align-items:center;gap:6px}
.cur-bot .bn .dot{width:6px;height:6px;border-radius:50%}
.cur-bot .chart-wrap{height:60px;margin-bottom:6px;position:relative}
.cur-bot .chart-wrap svg{width:100%;height:100%}
.cur-bot .stats{font-size:10px;color:var(--dim);line-height:1.6}
.cur-bot .stats b{color:var(--text);font-weight:500}
.cur-bot .stats .pos-line{display:flex;justify-content:space-between}

.ptable-wrap{background:var(--surface);border:1px solid var(--border);border-radius:6px;overflow:hidden;margin-bottom:14px}
.ptable-hdr{padding:10px 14px;border-bottom:1px solid var(--border);font-weight:600;font-size:12px}
.ptable{width:100%;border-collapse:collapse}
.ptable th{text-align:left;padding:6px 10px;font-size:9px;text-transform:uppercase;letter-spacing:.5px;color:var(--dim);background:var(--surface);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:10;box-shadow:0 1px 0 var(--border)}
.ptable td{padding:5px 10px;border-bottom:1px solid rgba(255,255,255,.02);font-size:11px;white-space:nowrap}
.ptable tr:hover{background:rgba(255,255,255,.015)}
.ptable .winner{background:rgba(74,222,128,.06)}
.ptable .per-name{color:var(--dim);max-width:180px;overflow:hidden;text-overflow:ellipsis}
.ptable .per-copy{cursor:pointer;transition:color .15s}
.ptable .per-copy:hover{color:var(--text)}
.ptable .per-copy.copied{color:var(--green)!important}
.ptable .t-count{color:var(--dim);font-size:9px;margin-left:4px}
.ptable-scroll{max-height:520px;overflow-y:auto}
.ptable-scroll::-webkit-scrollbar{width:4px}
.ptable-scroll::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
.note{color:var(--dim);font-size:10px;padding:6px 14px;border-top:1px solid var(--border)}

.trades-section{margin-top:14px}
.trades-section .sec-hdr{font-weight:600;font-size:12px;margin-bottom:10px}
.trades-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px}
.trades-card{background:var(--surface);border:1px solid var(--border);border-radius:6px;overflow:hidden}
.trades-card .th{padding:8px 12px;border-bottom:1px solid var(--border);font-size:11px;font-weight:600;display:flex;align-items:center;gap:6px}
.trades-card .th .dot{width:6px;height:6px;border-radius:50%}
.trow{display:flex;padding:3px 12px;font-size:10px;gap:6px;align-items:center;border-bottom:1px solid rgba(255,255,255,.015)}
.trow:hover{background:rgba(255,255,255,.015)}
.trow .tm{color:var(--dim);width:48px;flex-shrink:0}
.trow .sd{width:28px;font-weight:600;flex-shrink:0}
.trow .sd.BUY{color:var(--green)}.trow .sd.SELL{color:var(--red)}
.trow .dt{flex:1;color:var(--dim);overflow:hidden;text-overflow:ellipsis}
.trow .am{width:55px;text-align:right;flex-shrink:0}

.loading{text-align:center;padding:60px;color:var(--dim)}
.err{background:rgba(248,113,113,.08);border:1px solid rgba(248,113,113,.2);color:var(--red);padding:10px 14px;border-radius:6px;font-size:11px}
.date-range{display:flex;align-items:center;gap:8px;margin-bottom:14px;font-size:11px;color:var(--dim)}
.date-range input{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:4px 8px;border-radius:4px;font:inherit;font-size:11px}
.date-range input::-webkit-calendar-picker-indicator{filter:invert(0.7)}
.date-range .clear-btn{background:none;border:1px solid var(--border);color:var(--dim);padding:3px 8px;border-radius:3px;cursor:pointer;font:inherit;font-size:10px}
.date-range .clear-btn:hover{color:var(--text);border-color:var(--border2)}
.live-dot{display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--green);animation:pulse 1s infinite;margin-right:4px}
</style>
</head>
<body>

<div class="hdr">
  <h1>Polytrader Live</h1>
  <div class="st">
    <span class="dot"></span>
    <span id="st-txt">Loading...</span>
    <button class="rbtn" onclick="doFetch()">Refresh</button>
  </div>
</div>
<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px">
  <div class="tabs" id="tabs" style="margin-bottom:0">
    <button class="on" data-p="24h">24h</button>
    <button data-p="today">Today</button>
    <button data-p="1h">1h</button>
    <button data-p="all">All Data</button>
    <button data-p="custom">Custom</button>
  </div>
  <div class="date-range" id="date-range" style="display:none">
    <span>From (UTC)</span>
    <input type="datetime-local" id="dt-from">
    <span>To (UTC)</span>
    <input type="datetime-local" id="dt-to">
    <button class="clear-btn" id="dt-clear">Clear</button>
  </div>
</div>
<div id="app"><div class="loading">Fetching data from Polymarket API...</div></div>

<script>
const BOT_ORDER = ['iphone1','AWS','Stop4','BSO'];
const COLORS = {iphone1:'#4ade80',AWS:'#60a5fa',Stop4:'#fb923c',BSO:'#f472b6'};
let D = null, period = '24h';
let cmpA = 'iphone1', cmpB = 'BSO';
let livePrices = null; // {slug, up, down, ts}
let customFrom = null, customTo = null;

// Helper: get adjusted PnL for a bot
function getAdjustedPnl(b, period) {
  if (!b || !b.metrics) return 0;
  if (period === 'custom') {
    // Sum PnL from periods within custom range
    if (!b.periods || !customFrom || !customTo) return 0;
    let pnl = 0;
    const fromTs = customFrom.getTime()/1000;
    const toTs = customTo.getTime()/1000;
    b.periods.forEach(p => {
      if (p.period_start >= fromTs && p.period_start <= toTs) {
        pnl += getPeriodPnl(b, p.slug, p.pnl, p.resolved);
      }
    });
    return pnl;
  }
  if (!b.metrics[period]) return 0;
  let pnl = b.metrics[period].pnl;
  if (b.pos_pnl && b.periods) {
    b.periods.forEach(p => {
      if (!p.resolved && b.pos_pnl[p.slug] != null) {
        pnl += (b.pos_pnl[p.slug] - p.pnl);
      }
    });
  }
  return pnl;
}

// Custom range trade count
function getCustomTrades(b) {
  if (!b.periods || !customFrom || !customTo) return 0;
  const fromTs = customFrom.getTime()/1000, toTs = customTo.getTime()/1000;
  let t = 0;
  b.periods.forEach(p => { if (p.period_start >= fromTs && p.period_start <= toTs) t += p.trades; });
  return t;
}

// Helper: get PnL for a specific period (use positions data if unresolved)
function getPeriodPnl(b, slug, activityPnl, resolved) {
  if (!resolved && b && b.pos_pnl && b.pos_pnl[slug] != null) {
    return b.pos_pnl[slug];
  }
  return activityPnl;
}

document.getElementById('tabs').onclick = e => {
  if (e.target.tagName !== 'BUTTON') return;
  document.querySelectorAll('#tabs button').forEach(b=>b.classList.remove('on'));
  e.target.classList.add('on');
  period = e.target.dataset.p;
  document.getElementById('date-range').style.display = period === 'custom' ? 'flex' : 'none';
  if (D) render();
};
// Date range inputs — interpret as UTC
document.addEventListener('change', e => {
  if (e.target.id === 'dt-from') { customFrom = e.target.value ? new Date(e.target.value + 'Z') : null; if(D) render(); }
  if (e.target.id === 'dt-to') { customTo = e.target.value ? new Date(e.target.value + 'Z') : null; if(D) render(); }
});
document.addEventListener('click', e => {
  // Copy period timestamp
  const copyEl = e.target.closest('.per-copy');
  if (copyEl && copyEl.dataset.ts) {
    navigator.clipboard.writeText(copyEl.dataset.ts).then(() => {
      const orig = copyEl.textContent;
      copyEl.textContent = 'Copied ' + copyEl.dataset.ts;
      copyEl.classList.add('copied');
      setTimeout(() => { copyEl.textContent = orig; copyEl.classList.remove('copied'); }, 1200);
    });
    return;
  }
  if (e.target.id === 'dt-clear') {
    customFrom = customTo = null;
    const f = document.getElementById('dt-from'), t = document.getElementById('dt-to');
    if(f) f.value = ''; if(t) t.value = '';
    if(D) render();
  }
});

async function doFetch() {
  try {
    const r = await fetch('/api/data');
    D = await r.json();
    render();
    document.getElementById('st-txt').textContent =
      `Updated ${new Date(D._ts*1000).toLocaleTimeString()} · auto 30s`;
  } catch(e) {
    document.getElementById('app').innerHTML = `<div class="err">${e.message}</div>`;
  }
}

const $ = v => v >= 0 ? '+' : '';
const fmt = v => $(v) + '$' + Math.abs(v).toFixed(2);
const fmtBig = v => $(v) + '$' + Math.abs(v).toFixed(v >= 100 || v <= -100 ? 0 : 2);
const cls = v => v >= 0 ? 'pos' : 'neg';
const hm = ts => new Date(ts*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
const ago = ts => {
  const d = Date.now()/1000 - ts;
  if (d < 60) return Math.floor(d)+'s';
  if (d < 3600) return Math.floor(d/60)+'m';
  if (d < 86400) return Math.floor(d/3600)+'h';
  return Math.floor(d/86400)+'d';
};
const status = ts => {
  const d = Date.now()/1000 - ts;
  if (d < 300) return ['active','Active'];
  if (d < 1800) return ['stale','Stale'];
  return ['offline','Off'];
};

function buildChart(trades, periodStart) {
  if (!trades || !trades.length) return '<svg viewBox="0 0 200 60"><text x="100" y="33" text-anchor="middle" fill="#555568" font-size="10">No trades</text></svg>';
  const W = 200, H = 60, PAD = 2, duration = 900;
  let pos = 0, maxPos = 0, minPos = 0;
  const points = [{x: 0, pos: 0}];
  trades.forEach(t => {
    const x = Math.max(0, Math.min(1, (t.t - periodStart) / duration));
    const delta = t.side === 'BUY' ? t.size : -t.size;
    pos += delta;
    if (pos > maxPos) maxPos = pos;
    if (pos < minPos) minPos = pos;
    points.push({x, pos, side: t.side, size: t.size, outcome: t.outcome});
  });
  const range = Math.max(maxPos - minPos, 10);
  const toY = v => PAD + (H - 2*PAD) * (1 - (v - minPos) / range);
  const toX = frac => PAD + frac * (W - 2*PAD);
  const zeroY = toY(0);
  let path = `M${toX(0)},${zeroY.toFixed(1)}`;
  points.forEach(p => { path += ` L${toX(p.x).toFixed(1)},${toY(p.pos).toFixed(1)}`; });
  const now = Date.now()/1000;
  const nowFrac = Math.min(1, Math.max(0, (now - periodStart) / duration));
  path += ` L${toX(nowFrac).toFixed(1)},${toY(pos).toFixed(1)}`;
  let markers = '';
  points.forEach(p => {
    if (p.side) {
      const color = p.side === 'BUY' ? '#4ade80' : '#f87171';
      const r = Math.min(3.5, Math.max(1.5, p.size / 8));
      markers += `<circle cx="${toX(p.x).toFixed(1)}" cy="${toY(p.pos).toFixed(1)}" r="${r}" fill="${color}" opacity="0.7"/>`;
    }
  });
  let grid = '';
  for (let m = 0; m <= 15; m += 5) {
    const x = toX(m/15).toFixed(1);
    grid += `<line x1="${x}" y1="0" x2="${x}" y2="${H}" stroke="#1a1a25" stroke-width="0.5"/>`;
    if (m > 0 && m < 15) grid += `<text x="${x}" y="${H-1}" text-anchor="middle" fill="#555568" font-size="7">${m}m</text>`;
  }
  const nowX = toX(nowFrac).toFixed(1);
  const nowLine = nowFrac < 1 ? `<line x1="${nowX}" y1="0" x2="${nowX}" y2="${H}" stroke="#ffffff20" stroke-width="0.5" stroke-dasharray="2,2"/>` : '';
  return `<svg viewBox="0 0 ${W} ${H}">
    ${grid}
    <line x1="${PAD}" y1="${zeroY.toFixed(1)}" x2="${W-PAD}" y2="${zeroY.toFixed(1)}" stroke="#252535" stroke-width="0.5"/>
    ${nowLine}
    <path d="${path}" fill="none" stroke="#8888aa" stroke-width="1.2" opacity="0.6"/>
    ${markers}
    <text x="${W-PAD}" y="${toY(pos).toFixed(1)}" text-anchor="end" fill="#d4d4dc" font-size="8" dy="-3">${pos > 0 ? '+' : ''}${pos.toFixed(0)}</text>
  </svg>`;
}

function render() {
  const bots = D._bots;
  let h = '';

  // === Summary cards ===
  h += '<div class="strip">';
  BOT_ORDER.forEach(name => {
    const b = bots[name];
    if (!b || b.error) {
      h += `<div class="scard" style="--c:${COLORS[name]}"><div class="nm">${name}</div><div class="err">${b?.error||'No data'}</div></div>`;
      return;
    }
    const m = (period !== 'custom' && b.metrics[period]) ? b.metrics[period] : {pnl:0,trades:0,volume:0,rebates:0,redeems:0};
    const adjPnl = getAdjustedPnl(b, period);
    const displayTrades = period === 'custom' ? getCustomTrades(b) : m.trades;
    const [sc, st] = status(b.last_active);
    const cash = b.cash != null ? `$${b.cash.toFixed(0)}` : '?';
    const portVal = b.portfolio_value != null ? `$${b.portfolio_value.toFixed(0)}` : '?';
    const totalVal = (b.cash != null && b.portfolio_value != null) ? `$${(b.cash + b.portfolio_value).toFixed(0)}` : '?';

    h += `<div class="scard" style="--c:${b.color}">
      <div class="nm">${name} <span class="badge ${sc}">${st} ${ago(b.last_active)}</span></div>
      <div class="pnl ${cls(adjPnl)}">${fmtBig(adjPnl)}</div>
      <div class="row">
        <span><b>${displayTrades}</b> trades</span>
        <span><b>${m.redeems}</b> redeems</span>
        <span>vol <b>$${m.volume.toFixed(0)}</b></span>
      </div>
      <div class="bal-row">
        <span>Cash <b>${cash}</b></span>
        <span>Pos <b>${portVal}</b></span>
        <span>Total <b>${totalVal}</b></span>
      </div>
    </div>`;
  });
  h += '</div>';

  // === Current market section ===
  // Find most recent period across all bots
  let currentSlug = null, currentTitle = '', currentStart = 0;
  BOT_ORDER.forEach(name => {
    const b = bots[name];
    if (!b || !b.periods || !b.periods.length) return;
    b.periods.forEach(p => {
      if (p.period_start > currentStart) {
        currentStart = p.period_start;
        currentSlug = p.slug;
        currentTitle = p.title || currentSlug;
      }
    });
  });

  if (currentSlug) {
    const elapsed = Math.max(0, Date.now()/1000 - currentStart);
    const remaining = Math.max(0, 900 - elapsed);
    const mins = Math.floor(remaining/60);
    const secs = Math.floor(remaining%60);
    const timerTxt = remaining > 0 ? `${mins}:${secs.toString().padStart(2,'0')} left` : 'Resolved';
    const shortTitle = currentTitle.replace('Bitcoin Up or Down - ','').replace(/ ET$/,'');

    const lpGlobal = livePrices && livePrices.slug === currentSlug ? livePrices : null;
    const priceLabel = lpGlobal ? `<span class="live-dot"></span>Up ${lpGlobal.up.toFixed(3)} / Down ${lpGlobal.down.toFixed(3)}` : '';

    h += `<div class="cur-section">
      <div class="cur-hdr">
        <span class="title">Current: ${shortTitle}</span>
        <span id="live-price-hdr" style="font-size:11px;color:var(--dim)">${priceLabel}</span>
        <span class="timer" id="timer">${timerTxt}</span>
      </div>
      <div class="cur-grid">`;

    BOT_ORDER.forEach(name => {
      const b = bots[name];
      const pp = b?.periods?.find(p => p.slug === currentSlug);
      const trades = pp?.trade_list || [];

      // Get position data from positions API for this market
      const posUp = b?.positions?.find(p => p.slug === currentSlug && p.outcome === 'Up');
      const posDn = b?.positions?.find(p => p.slug === currentSlug && p.outcome === 'Down');

      const upSize = posUp ? posUp.size : 0;
      const dnSize = posDn ? posDn.size : 0;
      const upAvg = posUp ? posUp.avgPrice : 0;
      const dnAvg = posDn ? posDn.avgPrice : 0;
      // Use live prices if available for this slug, otherwise fall back to positions API
      const lp = livePrices && livePrices.slug === currentSlug ? livePrices : null;
      const upCur = lp ? lp.up : (posUp ? posUp.curPrice : 0);
      const dnCur = lp ? lp.down : (posDn ? posDn.curPrice : 0);
      // Recalc PnL with live prices if available
      const upValue = upSize * upCur;
      const dnValue = dnSize * dnCur;
      const upCost = posUp ? posUp.cost : 0;
      const dnCost = posDn ? posDn.cost : 0;
      const upPnl = upValue - upCost;
      const dnPnl = dnValue - dnCost;
      const totalPnl = upPnl + dnPnl;
      const totalCost = upCost + dnCost;
      const totalValue = upValue + dnValue;

      h += `<div class="cur-bot">
        <div class="bn"><span class="dot" style="background:${COLORS[name]}"></span>${name}`;
      if (totalCost > 0) {
        h += ` <span id="cur-tot-pnl-${name}" style="margin-left:auto;font-weight:600" class="${cls(totalPnl)}">${fmt(totalPnl)}</span>`;
      }
      h += `</div>
        <div class="chart-wrap">${buildChart(trades, currentStart)}</div>
        <div class="stats">`;

      if (upSize > 0 || dnSize > 0) {
        if (upSize > 0) {
          h += `<div class="pos-line">
            <span>Up: <b>${upSize.toFixed(0)}</b> sh</span>
            <span>avg <b>${upAvg.toFixed(2)}</b></span>
            <span>now <b id="cur-up-now-${name}">${upCur.toFixed(2)}</b></span>
            <span id="cur-up-pnl-${name}" class="${cls(upPnl)}">${fmt(upPnl)}</span>
          </div>`;
        }
        if (dnSize > 0) {
          h += `<div class="pos-line">
            <span>Dn: <b>${dnSize.toFixed(0)}</b> sh</span>
            <span>avg <b>${dnAvg.toFixed(2)}</b></span>
            <span>now <b id="cur-dn-now-${name}">${dnCur.toFixed(2)}</b></span>
            <span id="cur-dn-pnl-${name}" class="${cls(dnPnl)}">${fmt(dnPnl)}</span>
          </div>`;
        }
        h += `<div class="pos-line" style="border-top:1px solid var(--border);padding-top:3px;margin-top:2px">
          <span>Cost: <b>$${totalCost.toFixed(1)}</b></span>
          <span>Value: <b id="cur-tot-val-${name}">$${totalValue.toFixed(1)}</b></span>
        </div>`;
      } else {
        h += '<span>No position</span>';
      }
      h += '</div></div>';
    });
    h += '</div></div>';
  }

  // === Period PnL table ===
  const allPeriods = {};
  BOT_ORDER.forEach(name => {
    const b = bots[name];
    if (!b || !b.periods) return;
    b.periods.forEach(p => {
      if (!allPeriods[p.slug]) allPeriods[p.slug] = {slug: p.slug, title: p.title, ts: p.ts, start: p.period_start, bots: {}};
      allPeriods[p.slug].bots[name] = p;
      if (p.ts > allPeriods[p.slug].ts) allPeriods[p.slug].ts = p.ts;
    });
  });
  const sorted = Object.values(allPeriods).sort((a,b) => b.start - a.start);

  // Compare selector
  h += `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
    <span style="font-weight:600;font-size:12px">Period PnL Comparison</span>
    <span style="color:var(--dim);font-size:10px;margin-left:auto">Compare:</span>
    <select id="cmpA" style="background:var(--surface);border:1px solid var(--border);color:var(--text);padding:3px 6px;border-radius:3px;font:inherit;font-size:10px">
      ${BOT_ORDER.map(n => `<option value="${n}" ${n===cmpA?'selected':''}>${n}</option>`).join('')}
    </select>
    <span style="color:var(--dim);font-size:10px">vs</span>
    <select id="cmpB" style="background:var(--surface);border:1px solid var(--border);color:var(--text);padding:3px 6px;border-radius:3px;font:inherit;font-size:10px">
      ${BOT_ORDER.map(n => `<option value="${n}" ${n===cmpB?'selected':''}>${n}</option>`).join('')}
    </select>
  </div>`;

  h += `<div class="ptable-wrap">
    <div class="ptable-scroll">
    <table class="ptable">
    <tr><th style="width:160px">Period</th>`;
  BOT_ORDER.forEach(n => h += `<th style="color:${COLORS[n]}">${n}</th>`);
  h += `<th>Best</th><th style="color:var(--blue)">${cmpA} - ${cmpB}</th></tr>`;

  let cmpTotal = 0, cmpCount = 0;
  // Filter periods by custom date range if active
  let filteredPeriods = sorted;
  if (period === 'custom' && customFrom && customTo) {
    const fromTs = customFrom.getTime()/1000, toTs = customTo.getTime()/1000;
    filteredPeriods = sorted.filter(pp => pp.start >= fromTs && pp.start <= toTs);
  }
  filteredPeriods.slice(0, 80).forEach(pp => {
    const short = (pp.title || pp.slug).replace('Bitcoin Up or Down - ','').replace(/ ET$/,'');
    let bestName = '', bestPnl = -Infinity, anyResolved = false;

    // Compute display PnL per bot (use positions data for unresolved)
    const displayPnl = {};
    BOT_ORDER.forEach(n => {
      const bp = pp.bots[n];
      if (!bp) return;
      const b = bots[n];
      displayPnl[n] = getPeriodPnl(b, pp.slug, bp.pnl, bp.resolved);
      if ((bp.resolved || (b?.pos_pnl && b.pos_pnl[pp.slug] != null)) && displayPnl[n] > bestPnl) {
        bestPnl = displayPnl[n]; bestName = n; anyResolved = true;
      }
    });

    h += `<tr><td class="per-name per-copy" data-ts="${pp.start}" title="Click to copy: ${pp.start}">${short}</td>`;
    BOT_ORDER.forEach(n => {
      const bp = pp.bots[n];
      if (!bp) { h += '<td style="color:var(--dim)">-</td>'; return; }
      const dpnl = displayPnl[n] != null ? displayPnl[n] : bp.pnl;
      const isWinner = anyResolved && n === bestName;
      const hasPos = !bp.resolved && bots[n]?.pos_pnl && bots[n].pos_pnl[pp.slug] != null;
      const c = (bp.resolved || hasPos) ? cls(dpnl) : '';
      const star = bp.resolved ? '' : (hasPos ? '' : ' *');
      h += `<td class="${c}${isWinner?' winner':''}">${fmt(dpnl)}${star}<span class="t-count">${bp.trades}t</span></td>`;
    });

    // Best column
    if (anyResolved) {
      h += `<td style="color:${COLORS[bestName]};font-weight:600">${bestName}</td>`;
    } else {
      h += '<td style="color:var(--dim)">-</td>';
    }

    // Compare column
    const pA = displayPnl[cmpA], pB = displayPnl[cmpB];
    if (pA != null && pB != null) {
      const diff = pA - pB;
      cmpTotal += diff;
      cmpCount++;
      h += `<td class="${cls(diff)}" style="font-weight:500">${fmt(diff)}</td>`;
    } else {
      h += '<td style="color:var(--dim)">-</td>';
    }
    h += '</tr>';
  });

  // Totals row
  h += `<tr style="border-top:2px solid var(--border2);font-weight:700">
    <td>TOTAL (${cmpCount} periods)</td>`;
  BOT_ORDER.forEach(n => {
    const b = bots[n];
    if (!b || !b.periods) { h += '<td>-</td>'; return; }
    let total = 0;
    filteredPeriods.slice(0, 80).forEach(pp => {
      const bp = pp.bots[n];
      if (!bp) return;
      total += getPeriodPnl(b, pp.slug, bp.pnl, bp.resolved);
    });
    h += `<td class="${cls(total)}">${fmt(total)}</td>`;
  });
  h += `<td></td><td class="${cls(cmpTotal)}" style="font-weight:700">${fmt(cmpTotal)}</td>`;
  h += '</tr>';

  h += '</table></div>';
  h += '<div class="note">PnL uses positions API for unresolved periods (live market value).</div>';
  h += '</div>';

  // === Cumulative PnL sparkline per bot ===
  h += '<div class="ptable-wrap"><div class="ptable-hdr">Cumulative PnL (resolved periods)</div><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border)">';
  BOT_ORDER.forEach(name => {
    const b = bots[name];
    if (!b || !b.periods) { h += '<div style="background:var(--surface);padding:16px;color:var(--dim)">No data</div>'; return; }
    const resolved = b.periods.filter(p => p.resolved).reverse();
    if (!resolved.length) { h += `<div style="background:var(--surface);padding:16px;color:var(--dim)">${name}: no resolved</div>`; return; }
    let cum = 0;
    const pts = resolved.map(p => { cum += p.pnl; return {pnl: cum}; });
    const maxC = Math.max(...pts.map(p=>p.pnl), 0);
    const minC = Math.min(...pts.map(p=>p.pnl), 0);
    const range = Math.max(maxC - minC, 1);
    const W = 200, H = 50, PAD = 2;
    const toX = i => PAD + (i / Math.max(pts.length-1, 1)) * (W - 2*PAD);
    const toY = v => PAD + (H - 2*PAD) * (1 - (v - minC) / range);
    let pathD = `M${toX(0).toFixed(1)},${toY(pts[0].pnl).toFixed(1)}`;
    pts.forEach((p, i) => { if(i>0) pathD += ` L${toX(i).toFixed(1)},${toY(p.pnl).toFixed(1)}`; });
    let areaD = pathD + ` L${toX(pts.length-1).toFixed(1)},${toY(0).toFixed(1)} L${toX(0).toFixed(1)},${toY(0).toFixed(1)} Z`;
    const finalPnl = pts[pts.length-1].pnl;
    const fillColor = finalPnl >= 0 ? COLORS[name] : '#f87171';
    h += `<div style="background:var(--surface);padding:10px 12px">
      <div style="font-size:10px;color:var(--dim);margin-bottom:4px;display:flex;justify-content:space-between">
        <span style="color:${COLORS[name]}">${name}</span>
        <span class="${cls(finalPnl)}">${fmt(finalPnl)} (${resolved.length}p)</span>
      </div>
      <svg viewBox="0 0 ${W} ${H}" style="width:100%;height:50px">
        <line x1="${PAD}" y1="${toY(0).toFixed(1)}" x2="${W-PAD}" y2="${toY(0).toFixed(1)}" stroke="#252535" stroke-width="0.5"/>
        <path d="${areaD}" fill="${fillColor}" opacity="0.1"/>
        <path d="${pathD}" fill="none" stroke="${fillColor}" stroke-width="1.5" opacity="0.8"/>
      </svg>
    </div>`;
  });
  h += '</div></div>';

  // === Recent trades ===
  h += '<div class="trades-section"><div class="sec-hdr">Recent Trades</div><div class="trades-grid">';
  BOT_ORDER.forEach(name => {
    const b = bots[name];
    if (!b || !b.recent_trades) return;
    h += `<div class="trades-card"><div class="th"><span class="dot" style="background:${COLORS[name]}"></span>${name}</div>`;
    b.recent_trades.slice(0, 15).forEach(t => {
      h += `<div class="trow">
        <span class="tm">${hm(t.t)}</span>
        <span class="sd ${t.side}">${t.side[0]}</span>
        <span class="dt">${t.size.toFixed(1)} ${t.outcome} @${t.price.toFixed(2)}</span>
        <span class="am">$${t.usdc.toFixed(2)}</span>
      </div>`;
    });
    h += '</div>';
  });
  h += '</div></div>';

  document.getElementById('app').innerHTML = h;

  // Bind compare selectors (re-bind after each render since innerHTML replaces them)
  const selA = document.getElementById('cmpA');
  const selB = document.getElementById('cmpB');
  if (selA) selA.onchange = e => { cmpA = e.target.value; render(); };
  if (selB) selB.onchange = e => { cmpB = e.target.value; render(); };
}

// Timer countdown
setInterval(() => {
  const el = document.getElementById('timer');
  if (!el || !D) return;
  let cs = 0;
  BOT_ORDER.forEach(n => {
    const b = D._bots[n];
    if (!b?.periods) return;
    b.periods.forEach(p => { if (p.period_start > cs) cs = p.period_start; });
  });
  if (!cs) return;
  const rem = Math.max(0, 900 - (Date.now()/1000 - cs));
  if (rem <= 0) { el.textContent = 'Resolved'; return; }
  const m = Math.floor(rem/60), s = Math.floor(rem%60);
  el.textContent = m + ':' + s.toString().padStart(2,'0') + ' left';
}, 1000);

doFetch();
setInterval(doFetch, 30000);

// Live price polling every 5s — update DOM directly, no full re-render
async function pollPrices() {
  try {
    const r = await fetch('/api/prices');
    const p = await r.json();
    if (p && p.slug) {
      const changed = !livePrices || livePrices.up !== p.up || livePrices.down !== p.down || livePrices.slug !== p.slug;
      livePrices = p;
      if (!changed) return;
      // Update header prices
      const hdr = document.getElementById('live-price-hdr');
      if (hdr) hdr.innerHTML = `<span class="live-dot"></span>Up ${p.up.toFixed(3)} / Down ${p.down.toFixed(3)}`;
      // Update each bot's "now" prices and PnL in current market
      if (D) {
        BOT_ORDER.forEach(name => {
          const b = D._bots[name];
          if (!b) return;
          const posUp = b.positions?.find(x => x.slug === p.slug && x.outcome === 'Up');
          const posDn = b.positions?.find(x => x.slug === p.slug && x.outcome === 'Down');
          const upSize = posUp ? posUp.size : 0, dnSize = posDn ? posDn.size : 0;
          const upCost = posUp ? posUp.cost : 0, dnCost = posDn ? posDn.cost : 0;
          const upVal = upSize * p.up, dnVal = dnSize * p.down;
          const upPnl = upVal - upCost, dnPnl = dnVal - dnCost, totPnl = upPnl + dnPnl;
          // Update DOM elements by id
          const el = (id) => document.getElementById(id);
          const e1 = el('cur-up-now-'+name); if(e1) e1.textContent = p.up.toFixed(2);
          const e2 = el('cur-dn-now-'+name); if(e2) e2.textContent = p.down.toFixed(2);
          const e3 = el('cur-up-pnl-'+name); if(e3) { e3.textContent = fmt(upPnl); e3.className = cls(upPnl); }
          const e4 = el('cur-dn-pnl-'+name); if(e4) { e4.textContent = fmt(dnPnl); e4.className = cls(dnPnl); }
          const e5 = el('cur-tot-pnl-'+name); if(e5) { e5.textContent = fmt(totPnl); e5.className = cls(totPnl); }
          const e6 = el('cur-tot-val-'+name); if(e6) e6.textContent = '$'+(upVal+dnVal).toFixed(1);
        });
      }
    }
  } catch(e) {}
}
pollPrices();
setInterval(pollPrices, 5000);
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())
        elif self.path == '/api/data':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            with cache_lock:
                self.wfile.write(json.dumps(cache).encode())
        elif self.path == '/api/prices':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            prices = fetch_live_prices()
            self.wfile.write(json.dumps(prices or {}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *a):
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    print(f"Polytrader Dashboard — http://localhost:{port}")
    print(f"{len(BOTS)} bots, refresh every {CACHE_TTL}s\n")
    print("Fetching initial data...")
    refresh_all()
    threading.Thread(target=bg_refresh, daemon=True).start()
    server = HTTPServer(('0.0.0.0', port), Handler)
    print(f"\nReady: http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


if __name__ == '__main__':
    main()
