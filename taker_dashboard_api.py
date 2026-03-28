#!/usr/bin/env python3
"""
Dashboard API for 5-min taker bot.
Reads paper trade CSVs and snapshot CSVs, computes simulated positions & PnL.
Also proxies competitor wallet data from Polymarket.
Run alongside the bot on stop4: python3 taker_dashboard_api.py
"""
import csv
import glob
import json
import os
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from bisect import bisect_left
from urllib.parse import urlparse, parse_qs

TRADES_DIR = "/home/ubuntu/bot_stop4/trades_taker_5m"
ORDER_SIZE = 10.0
MAX_NET_POSITION = 100.0
START_BALANCE = 300.0
PORT = 8765
CACHE_TTL = 2
COMP_CACHE_TTL = 15
DEFAULT_LATENCY_MS = 150  # Onur: BSO fiber Tokyo→Amsterdam ~85ms + precomputed order ~40ms + matching ~25ms

# Polymarket crypto category fee formula (until 2026-03-29)
FEE_RATE = 0.25
FEE_EXPONENT = 2


def calc_fee(price, num_shares):
    """Polymarket crypto taker fee. Returns total fee in USDC."""
    if price <= 0 or price >= 1:
        return 0.0
    fee_per_share = price * FEE_RATE * (price * (1 - price)) ** FEE_EXPONENT
    return num_shares * fee_per_share

COMPETITOR_WALLETS = [
    {"name": "Onur", "addr": "0xe0229E10A858860218B6132F4234602C47bD6603"},
]

# On-chain balance lookup
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
RPC_URLS = ["https://polygon.gateway.tenderly.co", "https://polygon.drpc.org"]


def fetch_usdc_balance(address):
    """Fetch USDC.e balance via Polygon RPC."""
    addr_padded = "000000000000000000000000" + address[2:].lower()
    data = "0x70a08231" + addr_padded
    payload = json.dumps({"jsonrpc": "2.0", "method": "eth_call",
                          "params": [{"to": USDC_E, "data": data}, "latest"], "id": 1}).encode()
    for rpc_url in RPC_URLS:
        try:
            req = urllib.request.Request(rpc_url, data=payload,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
            return round(int(result["result"], 16) / 1e6, 2)
        except Exception:
            pass
    return None


def fetch_positions(address):
    """Fetch current positions from Polymarket data API."""
    url = f"https://data-api.polymarket.com/positions?user={address}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception:
        return []


def calc_portfolio_value(positions):
    """Sum currentValue of all active positions."""
    total = 0.0
    for p in positions:
        size = float(p.get("size", 0) or 0)
        if size > 0:
            total += float(p.get("currentValue", 0) or 0)
    return round(total, 2)

_cache = {"data": None, "ts": 0}
_comp_cache = {"data": None, "ts": 0}


def get_resolutions():
    """Load K per period and compute resolutions."""
    period_k = {}
    for f in sorted(glob.glob(os.path.join(TRADES_DIR, "snapshots_*.csv"))):
        pid = int(os.path.basename(f).replace("snapshots_", "").replace(".csv", ""))
        try:
            with open(f) as fh:
                first = next(csv.DictReader(fh), None)
                if first:
                    period_k[pid] = float(first["price_to_hit"])
        except Exception:
            pass
    resolutions = {}
    for pid in sorted(period_k):
        nxt = pid + 300
        if nxt in period_k:
            resolutions[pid] = "Up" if period_k[nxt] > period_k[pid] else "Down"
    return period_k, resolutions


def build_book_timeline(snapshots, sigs):
    """Merge snapshots + paper signals into a sorted timeline of book state.

    Each entry: (timestamp_ms, up_ask, down_ask, up_ask_size, down_ask_size)
    Used for latency look-ahead: find the book state at signal_time + latency.
    """
    timeline = []  # list of (ts_ms, up_ask, dn_ask, up_sz, dn_sz)

    for r in snapshots:
        try:
            ts = int(r["timestamp_ms"])
            up_a = float(r.get("up_best_ask") or 0)
            dn_a = float(r.get("down_best_ask") or 0)
            up_sz = float(r.get("up_ask_size", 0))
            dn_sz = float(r.get("down_ask_size", 0))
            if up_a > 0 or dn_a > 0:
                timeline.append((ts, up_a, dn_a, up_sz, dn_sz))
        except (ValueError, KeyError):
            pass

    for s in sigs:
        try:
            ts = int(s["timestamp_ms"])
            up_a = float(s.get("up_ask") or s.get("up_best_ask") or 0)
            dn_a = float(s.get("down_ask") or s.get("down_best_ask") or 0)
            up_sz = float(s.get("up_ask_size", 0))
            dn_sz = float(s.get("down_ask_size", 0))
            if up_a > 0 or dn_a > 0:
                timeline.append((ts, up_a, dn_a, up_sz, dn_sz))
        except (ValueError, KeyError):
            pass

    timeline.sort(key=lambda x: x[0])
    return timeline


def lookup_book(timeline, ts_keys, target_ts):
    """Find book state at or just after target_ts using binary search.

    ts_keys is a pre-extracted list of timestamps for bisect.
    Returns (up_ask, dn_ask, up_sz, dn_sz) or None if no data at/after target.
    """
    idx = bisect_left(ts_keys, target_ts)
    if idx >= len(timeline):
        return None
    return timeline[idx][1:]  # (up_ask, dn_ask, up_sz, dn_sz)


def simulate_period(sigs, snapshots=None, latency_ms=DEFAULT_LATENCY_MS):
    """Replay paper signals with fees and simulated fill latency.

    When latency_ms > 0, uses snapshot + signal book data to find the ask
    price at signal_time + latency_ms (what you'd actually fill at).
    """
    trades = []
    up_pos = down_pos = 0.0  # cost accumulators
    up_shares = down_shares = 0.0  # share accumulators for net position limit
    total_fees = 0.0

    # Build book timeline for latency look-ahead
    timeline = build_book_timeline(snapshots or [], sigs) if latency_ms > 0 else []
    ts_keys = [t[0] for t in timeline]

    for s in sigs:
        side = s["side"]
        signal_ts = int(s["timestamp_ms"])

        # Determine fill price: either instant or latency-delayed
        if latency_ms > 0 and timeline:
            book = lookup_book(timeline, ts_keys, signal_ts + latency_ms)
            if book is None:
                continue  # no book data at fill time — skip
            up_ask, dn_ask, up_sz, dn_sz = book
            ask = up_ask if side == "UP" else dn_ask
            sz = up_sz if side == "UP" else dn_sz
        else:
            try:
                ask = float(s["market_best_ask"])
            except (ValueError, KeyError):
                continue
            sz_key = "up_ask_size" if side == "UP" else "down_ask_size"
            sz = float(s.get(sz_key, 0))

        if ask <= 0:
            continue
        if sz < ORDER_SIZE:
            continue

        # Net position check (in shares)
        net_shares = up_shares - down_shares
        if side == "UP" and net_shares >= MAX_NET_POSITION:
            continue
        if side == "DOWN" and net_shares <= -MAX_NET_POSITION:
            continue

        tokens = ORDER_SIZE  # ORDER_SIZE is in shares (like Onur's 60-share batches)
        fee = calc_fee(ask, tokens)
        cost = tokens * ask + fee
        total_fees += fee

        if side == "UP":
            up_pos += cost
            up_shares += tokens
        else:
            down_pos += cost
            down_shares += tokens

        sig_remaining = float(s.get("remaining", 0))
        fill_ts = signal_ts + latency_ms
        fill_remaining = max(0, sig_remaining - latency_ms / 1000.0)

        trades.append({
            "ts": signal_ts,
            "fill_ts": fill_ts,
            "side": side,
            "ask": round(ask, 4),
            "cost": round(cost, 4),
            "fee": round(fee, 4),
            "tokens": round(tokens, 4),
            "remaining": round(sig_remaining, 1),
            "fill_remaining": round(fill_remaining, 1),
            "dp": round(float(s.get("dp", 0)), 4),
        })

    return trades, round(total_fees, 4)


def build_state(latency_ms=DEFAULT_LATENCY_MS):
    now = time.time()
    cache_key = latency_ms
    if _cache.get("key") == cache_key and _cache["data"] and now - _cache["ts"] < CACHE_TTL:
        return _cache["data"]

    period_k, resolutions = get_resolutions()

    # Load full snapshots for current period chart
    snapshot_rows = {}
    for f in sorted(glob.glob(os.path.join(TRADES_DIR, "snapshots_*.csv"))):
        pid = int(os.path.basename(f).replace("snapshots_", "").replace(".csv", ""))
        try:
            with open(f) as fh:
                rows = list(csv.DictReader(fh))
                if rows:
                    snapshot_rows[pid] = rows
        except Exception:
            pass

    # Load paper signals
    signals_by_period = {}
    for f in sorted(glob.glob(os.path.join(TRADES_DIR, "paper_*.csv"))):
        pid = int(os.path.basename(f).replace("paper_", "").replace(".csv", ""))
        try:
            with open(f) as fh:
                signals_by_period[pid] = list(csv.DictReader(fh))
        except Exception:
            pass

    all_pids = sorted(set(list(period_k.keys()) + list(signals_by_period.keys())))
    current_pid = all_pids[-1] if all_pids else 0

    # Simulate each period
    history = []
    balance = START_BALANCE
    total_pnl = total_buys = total_wins = 0
    cum_pnl = peak = 0.0
    max_dd = 0.0

    total_fees_all = 0.0
    for pid in all_pids:
        sigs = signals_by_period.get(pid, [])
        snaps = snapshot_rows.get(pid, [])
        trades, period_fees = simulate_period(sigs, snapshots=snaps, latency_ms=latency_ms)
        total_fees_all += period_fees

        cost = sum(t["cost"] for t in trades)
        up_tokens = sum(t["tokens"] for t in trades if t["side"] == "UP")
        down_tokens = sum(t["tokens"] for t in trades if t["side"] == "DOWN")
        up_pos = sum(t["cost"] for t in trades if t["side"] == "UP")
        down_pos = sum(t["cost"] for t in trades if t["side"] == "DOWN")

        res = resolutions.get(pid)
        if res:
            revenue = (up_tokens if res == "Up" else down_tokens) * 1.0
            pnl = revenue - cost
            cum_pnl += pnl
            total_pnl += pnl
            balance += pnl
            total_buys += len(trades)
            peak = max(peak, cum_pnl)
            max_dd = min(max_dd, cum_pnl - peak)
            if pnl > 0:
                total_wins += 1
        else:
            pnl = None

        dt = datetime.fromtimestamp(pid, tz=timezone.utc).strftime("%H:%M")
        history.append({
            "period_ts": pid,
            "time": dt,
            "resolution": res,
            "buys": len(trades),
            "up_pos": round(up_pos, 1),
            "down_pos": round(down_pos, 1),
            "up_tokens": round(up_tokens, 4),
            "down_tokens": round(down_tokens, 4),
            "cost": round(cost, 1),
            "fees": round(period_fees, 2),
            "pnl": round(pnl, 2) if pnl is not None else None,
            "cum_pnl": round(cum_pnl, 2),
            "balance": round(balance, 2),
            "trades": trades,
        })

    # Current period snapshots
    current_snaps = []
    if current_pid in snapshot_rows:
        for r in snapshot_rows[current_pid]:
            try:
                current_snaps.append({
                    "ts": int(r["timestamp_ms"]),
                    "remaining": round(float(r["remaining"]), 1),
                    "btc_raw": round(float(r["btc_raw"]), 2),
                    "btc_ema": round(float(r["btc_ema"]), 2),
                    "p_up": round(float(r["p_up"]), 4),
                    "price_to_hit": round(float(r["price_to_hit"]), 2),
                    "sigma": round(float(r["sigma"]), 3),
                    "premium": round(float(r["premium"]), 2),
                    "up_ask": float(r["up_best_ask"]) if r.get("up_best_ask") else None,
                    "down_ask": float(r["down_best_ask"]) if r.get("down_best_ask") else None,
                    "up_ask_size": round(float(r.get("up_ask_size", 0)), 1),
                    "down_ask_size": round(float(r.get("down_ask_size", 0)), 1),
                })
            except Exception:
                pass

    latest = current_snaps[-1] if current_snaps else {}
    current_entry = next((h for h in history if h["period_ts"] == current_pid), None)
    resolved_count = sum(1 for h in history if h["resolution"])

    result = {
        "ts": int(now * 1000),
        "period_ts": current_pid,
        "period_time": datetime.fromtimestamp(current_pid, tz=timezone.utc).strftime("%H:%M") if current_pid else "",
        "remaining": latest.get("remaining", 0),
        "mode": "PAPER",
        "btc_raw": latest.get("btc_raw"),
        "btc_ema": latest.get("btc_ema"),
        "price_to_hit": latest.get("price_to_hit"),
        "sigma": latest.get("sigma"),
        "p_up": latest.get("p_up"),
        "premium": latest.get("premium"),
        "up_best_ask": latest.get("up_ask"),
        "down_best_ask": latest.get("down_ask"),
        "up_ask_size": latest.get("up_ask_size"),
        "down_ask_size": latest.get("down_ask_size"),
        "current_period": current_entry,
        "snapshots": current_snaps,
        "history": list(reversed(history)),
        "balance": round(balance, 2),
        "start_balance": START_BALANCE,
        "total_pnl": round(total_pnl, 2),
        "total_buys": total_buys,
        "periods_resolved": resolved_count,
        "periods_total": len(history),
        "win_rate": round(100 * total_wins / resolved_count, 1) if resolved_count else 0,
        "max_drawdown": round(max_dd, 2),
        "total_fees": round(total_fees_all, 2),
        "latency_ms": latency_ms,
    }

    _cache["data"] = result
    _cache["ts"] = now
    _cache["key"] = cache_key
    return result


def build_competitors():
    now = time.time()
    if _comp_cache["data"] and now - _comp_cache["ts"] < COMP_CACHE_TTL:
        return _comp_cache["data"]

    _, resolutions = get_resolutions()
    result = []

    for w in COMPETITOR_WALLETS:
        try:
            url = f"https://data-api.polymarket.com/activity?user={w['addr']}&limit=1000"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=10)
            data = json.loads(resp.read())
        except Exception as e:
            result.append({"name": w["name"], "addr": w["addr"][:10] + "...", "error": str(e)})
            continue

        trades = [e for e in data if e.get("type") == "TRADE" and "btc-updown-5m" in e.get("slug", "")]

        by_period = defaultdict(list)
        for t in trades:
            try:
                pid = int(t["slug"].split("-")[-1])
                by_period[pid].append(t)
            except (ValueError, IndexError):
                pass

        periods = {}
        cum_pnl = 0.0
        total_cost = 0.0
        wins = 0
        peak = 0.0
        max_dd = 0.0

        for pid in sorted(by_period):
            tt = by_period[pid]
            ups = [t for t in tt if t.get("outcome") == "Up"]
            dns = [t for t in tt if t.get("outcome") == "Down"]
            up_sh = sum(float(t.get("size", 0)) for t in ups)
            dn_sh = sum(float(t.get("size", 0)) for t in dns)
            up_cost = sum(float(t.get("usdcSize", 0)) for t in ups)
            dn_cost = sum(float(t.get("usdcSize", 0)) for t in dns)
            cost = up_cost + dn_cost
            total_cost += cost

            res = resolutions.get(pid)
            pnl = None
            if res:
                revenue = up_sh if res == "Up" else dn_sh
                pnl = round(revenue - cost, 2)
                cum_pnl += pnl
                if pnl > 0:
                    wins += 1
                peak = max(peak, cum_pnl)
                max_dd = min(max_dd, cum_pnl - peak)

            # Individual trades for chart overlay
            trade_list = []
            for t in sorted(tt, key=lambda x: x.get("timestamp", 0)):
                ts_unix = t.get("timestamp", 0)
                rem = max(0, (pid + 300) - ts_unix)
                trade_list.append({
                    "ts": int(ts_unix * 1000),
                    "side": "UP" if t.get("outcome") == "Up" else "DOWN",
                    "ask": round(float(t.get("price", 0)), 4),
                    "cost": round(float(t.get("usdcSize", 0)), 2),
                    "shares": round(float(t.get("size", 0)), 1),
                    "remaining": round(rem, 1),
                })

            dt = datetime.fromtimestamp(pid, tz=timezone.utc).strftime("%H:%M")
            periods[str(pid)] = {
                "time": dt,
                "n": len(tt),
                "up_cost": round(up_cost, 1),
                "dn_cost": round(dn_cost, 1),
                "cost": round(cost, 1),
                "up_sh": round(up_sh, 1),
                "dn_sh": round(dn_sh, 1),
                "resolution": res,
                "pnl": pnl,
                "cum_pnl": round(cum_pnl, 2),
                "trades": trade_list,
            }

        resolved = sum(1 for p in periods.values() if p.get("resolution"))
        last_ts = max(by_period.keys()) if by_period else 0
        last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if last_ts else "?"

        # Wallet value: cash + positions
        cash = fetch_usdc_balance(w["addr"])
        raw_positions = fetch_positions(w["addr"])
        portfolio_value = calc_portfolio_value(raw_positions)

        result.append({
            "name": w["name"],
            "addr": w["addr"][:10] + "...",
            "full_addr": w["addr"],
            "error": None,
            "n_trades": len(trades),
            "n_periods": len(periods),
            "resolved": resolved,
            "total_pnl": round(cum_pnl, 2),
            "total_cost": round(total_cost, 0),
            "win_rate": round(100 * wins / resolved, 1) if resolved else 0,
            "max_drawdown": round(max_dd, 2),
            "last_active": last_dt,
            "cash": cash,
            "portfolio_value": portfolio_value,
            "wallet_value": round((cash or 0) + portfolio_value, 2),
            "periods": periods,
        })

    _comp_cache["data"] = result
    _comp_cache["ts"] = now
    return result


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        if path in ("/", "/state"):
            latency = int(params.get("latency", [DEFAULT_LATENCY_MS])[0])
            self._send_json(build_state(latency_ms=latency))
        elif path == "/competitors":
            self._send_json(build_competitors())
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET")
        self.end_headers()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print(f"Taker 5min Dashboard API on :{PORT}")
    print(f"Reading from {TRADES_DIR}")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
