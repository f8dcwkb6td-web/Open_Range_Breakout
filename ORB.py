"""
==============================================================================
ORB  —  OPENING RANGE BREAKOUT  |  LIVE ENGINE v2  (M5)  — DEBUG BUILD
==============================================================================
"""

import os, sys, io, time, logging, datetime, bisect
import numpy as np
import pandas as pd
from logging.handlers import RotatingFileHandler

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    print("ERROR: MetaTrader5 not installed.  pip install MetaTrader5")
    sys.exit(1)

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("ORB_V2")
logger.setLevel(logging.DEBUG)          # DEBUG level to catch everything
_fh = RotatingFileHandler(
    "orb_live_v2.log", maxBytes=15_000_000, backupCount=5, encoding="utf-8"
)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
_fh.setLevel(logging.DEBUG)
logger.addHandler(_fh)
_sh = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
)
_sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
_sh.setLevel(logging.DEBUG)
logger.addHandler(_sh)

# ── MT5 connection ────────────────────────────────────────────────────────────
TERMINAL_PATH = os.environ.get(
    "MT5_TERMINAL_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe"
)
LOGIN    = int(os.environ.get("MT5_LOGIN",    0))
PASSWORD =     os.environ.get("MT5_PASSWORD", "")
SERVER   =     os.environ.get("MT5_SERVER",   "")

MAGIC   = 202603261
COMMENT = "ORB_V2"

RISK_PER_TRADE = 0.01
MAX_HOLD       = 48
ATR_PERIOD     = 14
ATR_PCT_THRESH = 0.30
WARMUP_M5      = 200

FETCH_BARS_M5 = 2500

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "US500": {"open_h": 13, "open_m": 30, "close_h": 20},
    "UK100": {"open_h":  8, "open_m":  0, "close_h": 16},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

ACTIVE_PARAMS = "OLD"

PARAMS_NEW = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "US500": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
    "UK100": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "GER40": {"or_minutes": 30, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

PARAMS_OLD = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 1.00,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "US500": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
    "UK100": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

BEST_PARAMS = PARAMS_NEW if ACTIVE_PARAMS == "NEW" else PARAMS_OLD

SYMBOL_ALIASES = {
    "US30":  ["US30", "DJ30", "DJIA", "WS30", "DOW30", "US30Cash"],
    "US500": ["US500", "SPX500", "SP500", "SPX", "US500Cash"],
    "UK100": ["UK100", "FTSE100", "FTSE", "UK100Cash", "UKX"],
    "GER40": ["GER40", "DAX40", "DAX", "GER30", "DE40", "GER40Cash"],
}

SYMBOLS = list(BEST_PARAMS.keys())

STATE_FLAT        = "FLAT"
STATE_IN_POSITION = "IN_POSITION"


# ==============================================================================
#  SECTION 1 — SYMBOL RESOLVER
# ==============================================================================

def resolve_symbol(canonical):
    all_broker = {s.name.upper(): s.name for s in (mt5.symbols_get() or [])}
    for alias in SYMBOL_ALIASES[canonical]:
        info = mt5.symbol_info(alias)
        if info is not None:
            if not info.visible:
                mt5.symbol_select(alias, True)
            logger.info(f"  {canonical} -> '{alias}'")
            return alias
        for up, name in all_broker.items():
            if up.startswith(alias.upper()):
                mt5.symbol_select(name, True)
                logger.info(f"  {canonical} -> '{name}' (prefix)")
                return name
    logger.warning(f"  {canonical}: not found on broker")
    return None


def build_symbol_map():
    sym_map = {}
    active  = []
    skipped = []
    for canon in SYMBOLS:
        broker = resolve_symbol(canon)
        if broker:
            sym_map[canon] = broker
            active.append(canon)
        else:
            skipped.append(canon)
    logger.info(f"Active symbols  ({len(active)}): {active}")
    if skipped:
        logger.warning(f"Skipped symbols ({len(skipped)}): {skipped}")
    return sym_map, active


# ==============================================================================
#  SECTION 2 — INDICATORS
# ==============================================================================

def atr_wilder(h, l, c):
    n  = len(h)
    tr = np.empty(n)
    tr[0]  = h[0] - l[0]
    tr[1:] = np.maximum(
        h[1:] - l[1:],
        np.maximum(np.abs(h[1:] - c[:-1]),
                   np.abs(l[1:] - c[:-1]))
    )
    out = np.full(n, np.nan)
    if n < ATR_PERIOD:
        return out
    out[ATR_PERIOD - 1] = tr[:ATR_PERIOD].mean()
    k = 1.0 / ATR_PERIOD
    for i in range(ATR_PERIOD, n):
        out[i] = out[i - 1] * (1.0 - k) + tr[i] * k
    return out


def expanding_pct_rank(arr):
    n    = len(arr)
    out  = np.full(n, np.nan)
    hist = []
    for i in range(WARMUP_M5, n):
        v = arr[i]
        if np.isnan(v):
            continue
        if hist:
            out[i] = bisect.bisect_left(hist, v) / len(hist)
        bisect.insort(hist, v)
    return out


# ==============================================================================
#  SECTION 3 — BUILD CACHE
# ==============================================================================

def build_cache(canonical, m5):
    cfg = SESSION[canonical]
    o   = m5["open"].values.astype(np.float64)
    h   = m5["high"].values.astype(np.float64)
    l   = m5["low"].values.astype(np.float64)
    c   = m5["close"].values.astype(np.float64)
    n   = len(c)
    atr14   = atr_wilder(h, l, c)
    atr_pct = expanding_pct_rank(atr14)
    utc_h   = m5["utc_hour"].values.astype(np.int32)
    utc_m   = m5["utc_minute"].values.astype(np.int32)
    dates   = m5["date"].values
    times   = m5["time_utc"].values
    in_session = np.array([
        (utc_h[i] > cfg["open_h"] or
         (utc_h[i] == cfg["open_h"] and utc_m[i] >= cfg["open_m"]))
        and utc_h[i] < cfg["close_h"]
        for i in range(n)
    ])
    is_open_bar = (utc_h == cfg["open_h"]) & (utc_m == cfg["open_m"])
    return {
        "sym":         canonical,
        "n":           n,
        "o": o, "h": h, "l": l, "c": c,
        "atr14":       atr14,
        "atr_pct":     atr_pct,
        "utc_h":       utc_h,
        "utc_m":       utc_m,
        "dates":       dates,
        "times":       times,
        "in_session":  in_session,
        "is_open_bar": is_open_bar,
        "cfg":         cfg,
    }


# ==============================================================================
#  SECTION 4 — COMPUTE OR
# ==============================================================================

def compute_or(cache, or_bars):
    n       = cache["n"]
    h, l    = cache["h"], cache["l"]
    dates   = cache["dates"]
    is_open = cache["is_open_bar"]
    canon   = cache["sym"]

    day_start = {}
    for i in range(n):
        if is_open[i]:
            d = dates[i]
            if d not in day_start:
                day_start[d] = i

    day_or = {}
    for d, si in day_start.items():
        ei = si + or_bars
        if ei <= n:
            day_or[d] = (h[si:ei].max(), l[si:ei].min())

    or_high = np.full(n, np.nan)
    or_low  = np.full(n, np.nan)
    for i in range(n):
        if not cache["in_session"][i]:
            continue
        d = dates[i]
        if d not in day_or or d not in day_start:
            continue
        if i < day_start[d] + or_bars:
            continue
        or_high[i], or_low[i] = day_or[d]

    # DEBUG: log last bar's OR state and today's day_start/day_or coverage
    i_last = n - 1
    last_date = dates[i_last]
    logger.debug(
        f"[{canon}] compute_or({or_bars}bars): "
        f"total day_start={len(day_start)} day_or={len(day_or)} "
        f"last_date={last_date} "
        f"last_date_in_day_start={last_date in day_start} "
        f"last_date_in_day_or={last_date in day_or} "
        f"or_high[last]={or_high[i_last]} or_low[last]={or_low[i_last]}"
    )
    if last_date in day_start:
        si = day_start[last_date]
        logger.debug(
            f"[{canon}] today day_start[{last_date}]=bar#{si} "
            f"time={cache['times'][si]} "
            f"utc={cache['utc_h'][si]:02d}:{cache['utc_m'][si]:02d} "
            f"need i>={si+or_bars} current_i={i_last} "
            f"past_or_window={i_last >= si+or_bars}"
        )

    return or_high, or_low


# ==============================================================================
#  SECTION 5 — SIGNAL DETECTION ON LAST BAR
# ==============================================================================

def detect_signal_last_bar(cache, params, bars_since_last, day_trades_today):
    canon   = cache["sym"]
    or_bars = OR_BARS[params["or_minutes"]]
    or_high, or_low = compute_or(cache, or_bars)

    i = cache["n"] - 1

    # ── DEBUG: log every filter result on the last bar ──────────────────────
    atr_pct_val  = cache["atr_pct"][i]
    in_sess      = cache["in_session"][i]
    or_high_val  = or_high[i]
    or_low_val   = or_low[i]
    atr14_val    = cache["atr14"][i]
    c_cur        = cache["c"][i]
    o_cur        = cache["o"][i]
    body         = abs(c_cur - o_cur)
    utc_h        = cache["utc_h"][i]
    utc_m        = cache["utc_m"][i]
    last_time    = cache["times"][i]
    cfg          = cache["cfg"]

    logger.debug(
        f"[{canon}] detect_signal bar#{i} "
        f"time={last_time} "
        f"utc={utc_h:02d}:{utc_m:02d} "
        f"date={cache['dates'][i]} "
        f"c={c_cur:.5f} o={o_cur:.5f} body={body:.5f}"
    )
    logger.debug(
        f"[{canon}]   filters: "
        f"warmup_ok={i >= WARMUP_M5} "
        f"atr_pct={atr_pct_val:.4f}(need>={ATR_PCT_THRESH}) "
        f"atr_pct_ok={not np.isnan(atr_pct_val) and atr_pct_val >= ATR_PCT_THRESH} "
        f"in_session={in_sess} "
        f"or_high={'NaN' if np.isnan(or_high_val) else f'{or_high_val:.5f}'} "
        f"or_low={'NaN' if np.isnan(or_low_val) else f'{or_low_val:.5f}'} "
        f"or_valid={not np.isnan(or_high_val)} "
        f"cooldown={bars_since_last}>={params['cooldown_bars']}={bars_since_last >= params['cooldown_bars']} "
        f"day_trades={day_trades_today}<{params['max_trades_day']}={day_trades_today < params['max_trades_day']}"
    )

    # session window detail
    logger.debug(
        f"[{canon}]   session_cfg: open={cfg['open_h']:02d}:{cfg['open_m']:02d} "
        f"close={cfg['close_h']:02d}:00 "
        f"in_session_calc: "
        f"(utc_h>{cfg['open_h']} or (utc_h=={cfg['open_h']} and utc_m>={cfg['open_m']})) "
        f"= ({utc_h}>{cfg['open_h']} or ({utc_h}=={cfg['open_h']} and {utc_m}>={cfg['open_m']})) "
        f"= {utc_h > cfg['open_h'] or (utc_h == cfg['open_h'] and utc_m >= cfg['open_m'])} "
        f"AND utc_h<close: {utc_h}<{cfg['close_h']}={utc_h < cfg['close_h']}"
    )

    # ── Actual filter chain ──────────────────────────────────────────────────
    if i < WARMUP_M5:
        logger.debug(f"[{canon}]   REJECTED: warmup (i={i} < {WARMUP_M5})")
        return None, None, None, None, None, None

    if np.isnan(atr_pct_val) or atr_pct_val < ATR_PCT_THRESH:
        logger.debug(f"[{canon}]   REJECTED: atr_pct={atr_pct_val:.4f} < {ATR_PCT_THRESH}")
        return None, None, None, None, None, None

    if not in_sess:
        logger.debug(f"[{canon}]   REJECTED: not in_session")
        return None, None, None, None, None, None

    if np.isnan(or_high_val) or np.isnan(or_low_val):
        logger.debug(f"[{canon}]   REJECTED: OR is NaN")
        return None, None, None, None, None, None

    if bars_since_last < params["cooldown_bars"]:
        logger.debug(
            f"[{canon}]   REJECTED: cooldown "
            f"bars_since_last={bars_since_last} < {params['cooldown_bars']}"
        )
        return None, None, None, None, None, None

    if day_trades_today >= params["max_trades_day"]:
        logger.debug(
            f"[{canon}]   REJECTED: daily cap "
            f"day_trades={day_trades_today} >= {params['max_trades_day']}"
        )
        return None, None, None, None, None, None

    if np.isnan(atr14_val) or atr14_val <= 0:
        logger.debug(f"[{canon}]   REJECTED: atr14={atr14_val}")
        return None, None, None, None, None, None

    breaks_up   = c_cur > or_high_val
    breaks_down = c_cur < or_low_val

    logger.debug(
        f"[{canon}]   break_check: "
        f"c={c_cur:.5f} or_high={or_high_val:.5f} or_low={or_low_val:.5f} "
        f"breaks_up={breaks_up} breaks_down={breaks_down}"
    )

    if params["min_break_atr"] > 0:
        strong      = body >= params["min_break_atr"] * atr14_val
        logger.debug(
            f"[{canon}]   min_break_atr filter: "
            f"body={body:.5f} >= {params['min_break_atr']}*atr={params['min_break_atr']*atr14_val:.5f} "
            f"strong={strong}"
        )
        breaks_up   = breaks_up   and strong
        breaks_down = breaks_down and strong

    or_size = or_high_val - or_low_val
    sl_dist = max(params["sl_range_mult"] * or_size, atr14_val * 0.05)

    if breaks_up and not breaks_down:
        logger.debug(f"[{canon}]   SIGNAL: LONG")
        return "long",  or_high_val, or_low_val, atr14_val, or_size, sl_dist
    if breaks_down and not breaks_up:
        logger.debug(f"[{canon}]   SIGNAL: SHORT")
        return "short", or_high_val, or_low_val, atr14_val, or_size, sl_dist

    logger.debug(f"[{canon}]   NO SIGNAL: no break (or both sides broke simultaneously)")
    return None, None, None, None, None, None


# ==============================================================================
#  SECTION 6 — DATA FETCH
# ==============================================================================

def fetch_m5(broker_sym, n=FETCH_BARS_M5):
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 0, n + 1)
    if rates is None or len(rates) < WARMUP_M5 + 50:
        logger.warning(
            f"[{broker_sym}] M5 fetch failed / insufficient: "
            f"{len(rates) if rates is not None else 0}"
        )
        return None
    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[
        ["time", "open", "high", "low", "close"]
    ].copy()
    df["time_utc"]   = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df["utc_hour"]   = df["time_utc"].dt.hour
    df["utc_minute"] = df["time_utc"].dt.minute
    df["date"]       = df["time_utc"].dt.date
    df = df.iloc[:-1].reset_index(drop=True)   # drop still-forming bar
    logger.debug(
        f"[{broker_sym}] fetch_m5: {len(df)} bars "
        f"{df['time_utc'].iloc[0]} -> {df['time_utc'].iloc[-1]}"
    )
    return df


# ==============================================================================
#  SECTION 7 — POSITION SIZING
# ==============================================================================

def compute_lot_size(broker_sym, entry_price, sl_price, balance):
    info = mt5.symbol_info(broker_sym)
    if info is None:
        logger.error(f"[{broker_sym}] symbol_info returned None")
        return None

    sl_dist = abs(entry_price - sl_price)
    if sl_dist < 1e-9:
        logger.error(f"[{broker_sym}] SL distance is zero")
        return None

    if info.trade_tick_size <= 0:
        logger.error(f"[{broker_sym}] trade_tick_size <= 0")
        return None

    tick_value_per_lot = info.trade_tick_value / info.trade_tick_size
    risk_amount        = balance * RISK_PER_TRADE
    raw_lot            = risk_amount / (sl_dist * tick_value_per_lot)

    step    = info.volume_step if info.volume_step > 0 else 0.01
    vol_min = info.volume_min  if info.volume_min  > 0 else 0.01
    vol_max = info.volume_max  if info.volume_max  > 0 else 100.0

    lot = max(vol_min, min(vol_max, round(raw_lot / step) * step))
    lot = round(lot, 8)

    logger.info(
        f"[{broker_sym}] lot_calc: balance={balance:.2f} "
        f"risk={risk_amount:.2f} sl_dist={sl_dist:.5f} "
        f"tick_val/lot={tick_value_per_lot:.5f} "
        f"raw={raw_lot:.4f} -> lot={lot}"
    )
    return lot


# ==============================================================================
#  SECTION 8 — ORDER EXECUTION
# ==============================================================================

def send_market_order(broker_sym, direction, lot, sl_price, comment):
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{broker_sym}] Tick unavailable")
        return None, None

    price = tick.ask if direction == "long" else tick.bid
    otype = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       broker_sym,
        "volume":       lot,
        "type":         otype,
        "price":        price,
        "sl":           sl_price,
        "tp":           0.0,
        "deviation":    20,
        "magic":        MAGIC,
        "comment":      comment,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(req)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        code = getattr(result, "retcode", None)
        msg  = getattr(result, "comment", "")
        logger.error(f"[{broker_sym}] Entry FAILED retcode={code} msg={msg}")
        return None, None

    logger.info(
        f"[{broker_sym}] ENTRY {direction.upper()} "
        f"lot={lot} price={price:.5f} sl={sl_price:.5f} ticket={result.order}"
    )
    return result.order, price


def modify_sl(broker_sym, ticket, new_sl):
    req = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "symbol":   broker_sym,
        "position": ticket,
        "sl":       new_sl,
        "tp":       0.0,
    }
    result = mt5.order_send(req)
    if result is None or result.retcode not in (
        mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_NO_CHANGES
    ):
        code = getattr(result, "retcode", None)
        logger.warning(
            f"[{broker_sym}] SL modify failed retcode={code} "
            f"new_sl={new_sl:.5f}"
        )
        return False
    return True


def send_close_order(broker_sym, position):
    otype = (
        mt5.ORDER_TYPE_SELL
        if position.type == mt5.ORDER_TYPE_BUY
        else mt5.ORDER_TYPE_BUY
    )
    tick  = mt5.symbol_info_tick(broker_sym)
    price = tick.bid if position.type == mt5.ORDER_TYPE_BUY else tick.ask

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       broker_sym,
        "volume":       position.volume,
        "type":         otype,
        "position":     position.ticket,
        "price":        price,
        "deviation":    20,
        "magic":        MAGIC,
        "comment":      "orb_exit",
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(req)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        code = getattr(result, "retcode", None)
        logger.error(f"[{broker_sym}] Close FAILED retcode={code}")
        return False
    logger.info(
        f"[{broker_sym}] CLOSED ticket={position.ticket} price={price:.5f}"
    )
    return True


# ==============================================================================
#  SECTION 9 — PER-SYMBOL STATE
# ==============================================================================

def make_symbol_state():
    return {
        "state":            STATE_FLAT,
        "ticket":           None,
        "direction":        None,
        "entry_price":      None,
        "sl_dist":          None,
        "be_active":        False,
        "current_sl":       None,
        "hold_count":       0,
        "entry_date":       None,
        "entry_atr":        None,
        "bars_since_last":  9999,
        "day_trades_date":  None,
        "day_trades_count": 0,
    }


def _reset_after_close(sym_st, today):
    day_trades_date  = sym_st["day_trades_date"]
    day_trades_count = sym_st["day_trades_count"]
    bars_since_last  = sym_st["bars_since_last"]

    sym_st["state"]        = STATE_FLAT
    sym_st["ticket"]       = None
    sym_st["direction"]    = None
    sym_st["entry_price"]  = None
    sym_st["sl_dist"]      = None
    sym_st["be_active"]    = False
    sym_st["current_sl"]   = None
    sym_st["hold_count"]   = 0
    sym_st["entry_date"]   = None
    sym_st["entry_atr"]    = None

    sym_st["day_trades_date"]  = day_trades_date
    sym_st["day_trades_count"] = day_trades_count
    sym_st["bars_since_last"]  = bars_since_last


def _reset_daily_counter(sym_st, today):
    if sym_st["day_trades_date"] != today:
        logger.debug(
            f"  daily counter reset: {sym_st['day_trades_date']} -> {today} "
            f"(was {sym_st['day_trades_count']} trades)"
        )
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0


def reconstruct_position_state(canon, position):
    entry_time = datetime.datetime.fromtimestamp(
        position.time, tz=datetime.timezone.utc
    )
    now_utc    = datetime.datetime.now(tz=datetime.timezone.utc)
    hold_count = max(0, int((now_utc - entry_time).total_seconds() / 300))

    direction = "long" if position.type == mt5.ORDER_TYPE_BUY else "short"
    ep        = position.price_open
    sl_price  = position.sl or 0.0
    sl_dist   = abs(ep - sl_price) if sl_price > 0 else 0.01

    be_active = False
    if sl_price > 0:
        if direction == "long"  and sl_price >= ep: be_active = True
        if direction == "short" and sl_price <= ep: be_active = True

    logger.info(
        f"[{canon}] RECOVERED: dir={direction} ep={ep:.5f} "
        f"sl={sl_price:.5f} hold~{hold_count}bars be={be_active}"
    )
    today = entry_time.date()
    return {
        "state":            STATE_IN_POSITION,
        "ticket":           position.ticket,
        "direction":        direction,
        "entry_price":      ep,
        "sl_dist":          sl_dist,
        "be_active":        be_active,
        "current_sl":       sl_price,
        "hold_count":       hold_count,
        "entry_date":       today,
        "entry_atr":        None,
        "bars_since_last":  0,
        "day_trades_date":  today,
        "day_trades_count": 1,
    }


# ==============================================================================
#  SECTION 10 — PER-BAR PROCESSING
# ==============================================================================

def process_symbol(canon, broker_sym, sym_st, params, balance, today):
    # 1. Fetch
    df_m5 = fetch_m5(broker_sym)
    if df_m5 is None:
        logger.warning(f"[{canon}] M5 fetch failed — skipping bar")
        return

    # 2. Build cache
    cache = build_cache(canon, df_m5)

    # 3. New day reset
    _reset_daily_counter(sym_st, today)

    # 4. Advance cooldown every bar
    sym_st["bars_since_last"] += 1

    # 5. Broker cross-check
    positions = mt5.positions_get(symbol=broker_sym) or []
    positions = [p for p in positions if p.magic == MAGIC]

    broker_has_pos = len(positions) > 0
    state_in_pos   = sym_st["state"] == STATE_IN_POSITION

    logger.debug(
        f"[{canon}] state={sym_st['state']} "
        f"broker_has_pos={broker_has_pos} "
        f"bars_since_last={sym_st['bars_since_last']} "
        f"day_trades={sym_st['day_trades_count']} "
        f"today={today} day_trades_date={sym_st['day_trades_date']}"
    )

    if broker_has_pos and not state_in_pos:
        logger.warning(
            f"[{canon}] Desync: broker has position, "
            f"state={sym_st['state']} — recovering"
        )
        sym_st.update(reconstruct_position_state(canon, positions[0]))
        state_in_pos = True

    if not broker_has_pos and state_in_pos:
        logger.info(f"[{canon}] Position closed server-side (SL hit)")
        _log_close(canon, sym_st)
        _reset_after_close(sym_st, today)
        return

    # 6. IN_POSITION -> manage
    if sym_st["state"] == STATE_IN_POSITION:
        _manage_position(canon, broker_sym, sym_st, params, cache,
                         positions, today)
        return

    # 7. FLAT -> detect signal
    if sym_st["state"] == STATE_FLAT:
        direction, or_high_val, or_low_val, atr_val, or_size, sl_dist = \
            detect_signal_last_bar(
                cache, params,
                sym_st["bars_since_last"],
                sym_st["day_trades_count"],
            )

        if direction is None:
            return

        logger.info(
            f"[{canon}] SIGNAL {direction.upper()} "
            f"or_high={or_high_val:.5f} or_low={or_low_val:.5f} "
            f"or_size={or_size:.5f} sl_dist={sl_dist:.5f} "
            f"atr={atr_val:.5f} "
            f"(day_trades={sym_st['day_trades_count']+1}/"
            f"{params['max_trades_day']} "
            f"bars_since_last={sym_st['bars_since_last']})"
        )

        sym_st["bars_since_last"]  = 0
        sym_st["day_trades_count"] += 1

        _execute_entry(canon, broker_sym, sym_st, params,
                       balance, today, direction, sl_dist, atr_val)


def _execute_entry(canon, broker_sym, sym_st, params,
                   balance, today, direction, sl_dist, atr_val):
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{canon}] Tick unavailable — entry cancelled")
        return

    ep = tick.ask if direction == "long" else tick.bid

    min_sl_dist = 0.05 * atr_val
    if sl_dist < min_sl_dist:
        sl_dist = min_sl_dist

    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist

    if direction == "long"  and sl_price >= ep:
        sl_price = ep - min_sl_dist
        sl_dist  = min_sl_dist
    if direction == "short" and sl_price <= ep:
        sl_price = ep + min_sl_dist
        sl_dist  = min_sl_dist

    lot = compute_lot_size(broker_sym, ep, sl_price, balance)
    if lot is None:
        logger.error(f"[{canon}] Lot calc failed — entry cancelled")
        return

    ticket, _ = send_market_order(
        broker_sym, direction, lot, sl_price, f"{COMMENT}_{canon}"
    )
    if ticket is None:
        logger.error(f"[{canon}] Order failed — entry cancelled")
        return

    time.sleep(0.5)
    filled = [p for p in (mt5.positions_get(symbol=broker_sym) or [])
              if p.magic == MAGIC and p.ticket == ticket]

    if filled:
        actual_ep = filled[0].price_open
        actual_sl = filled[0].sl
        sl_dist   = abs(actual_ep - actual_sl)
        if sl_dist < 1e-9:
            sl_dist = min_sl_dist
    else:
        actual_ep = ep
        actual_sl = sl_price

    sym_st["state"]        = STATE_IN_POSITION
    sym_st["ticket"]       = ticket
    sym_st["direction"]    = direction
    sym_st["entry_price"]  = actual_ep
    sym_st["sl_dist"]      = sl_dist
    sym_st["be_active"]    = False
    sym_st["current_sl"]   = actual_sl
    sym_st["hold_count"]   = 0
    sym_st["entry_date"]   = today
    sym_st["entry_atr"]    = atr_val

    logger.info(
        f"[{canon}] ENTERED {direction.upper()} "
        f"ep={actual_ep:.5f} sl={actual_sl:.5f} "
        f"sl_dist={sl_dist:.5f} lot={lot} ticket={ticket}"
    )


def _manage_position(canon, broker_sym, sym_st, params, cache, positions, today):
    i         = cache["n"] - 1
    direction = sym_st["direction"]
    ep        = sym_st["entry_price"]
    sl_dist   = sym_st["sl_dist"]
    atr_cur   = cache["atr14"][i]
    bar_h     = cache["h"][i]
    bar_l     = cache["l"][i]
    cfg       = cache["cfg"]

    sym_st["hold_count"] += 1
    hc  = sym_st["hold_count"]
    pos = positions[0] if positions else None

    current_utc_hour = datetime.datetime.utcnow().hour
    logger.debug(
        f"[{canon}] manage: hold={hc}/{MAX_HOLD} "
        f"entry_date={sym_st['entry_date']} today={today} "
        f"utc_hour={current_utc_hour} close_h={cfg['close_h']} "
        f"be={sym_st['be_active']} cur_sl={sym_st['current_sl']:.5f}"
    )

    # EOD exit
    if sym_st["entry_date"] != today or current_utc_hour >= cfg["close_h"]:
        logger.info(
            f"[{canon}] EOD exit "
            f"(entry_date={sym_st['entry_date']} today={today} "
            f"utc_hour={current_utc_hour} close_h={cfg['close_h']})"
        )
        if pos:
            send_close_order(broker_sym, pos)
        _log_close(canon, sym_st)
        _reset_after_close(sym_st, today)
        return

    # Max hold
    if hc >= MAX_HOLD:
        logger.info(f"[{canon}] MAX HOLD ({MAX_HOLD} bars) — closing at market")
        if pos:
            send_close_order(broker_sym, pos)
        _log_close(canon, sym_st)
        _reset_after_close(sym_st, today)
        return

    # BE trigger
    one_r = ep + (sl_dist if direction == "long" else -sl_dist)
    if not sym_st["be_active"]:
        triggered = (
            (direction == "long"  and bar_h >= one_r) or
            (direction == "short" and bar_l <= one_r)
        )
        if triggered:
            sym_st["be_active"]  = True
            sym_st["current_sl"] = ep
            logger.info(f"[{canon}] BE triggered — SL -> {ep:.5f}")

    # Trail
    if sym_st["be_active"]:
        ta = atr_cur
        if np.isnan(ta) or ta <= 0:
            ta = sym_st["entry_atr"] or sl_dist
        trail_mult = params["trail_atr_mult"]
        if direction == "long":
            sym_st["current_sl"] = max(sym_st["current_sl"],
                                       bar_h - trail_mult * ta)
        else:
            sym_st["current_sl"] = min(sym_st["current_sl"],
                                       bar_l + trail_mult * ta)

    # Update broker SL
    if pos is not None:
        broker_sl = pos.sl or 0.0
        new_sl    = sym_st["current_sl"]
        sym_info  = mt5.symbol_info(broker_sym)
        pip       = (10 ** (-sym_info.digits + 1)) if sym_info else 0.0001
        if abs(new_sl - broker_sl) >= pip:
            if modify_sl(broker_sym, sym_st["ticket"], new_sl):
                logger.info(
                    f"[{canon}] SL updated {broker_sl:.5f} -> {new_sl:.5f} "
                    f"(hold={hc} be={sym_st['be_active']})"
                )


def _log_close(canon, sym_st):
    ticket = sym_st.get("ticket")
    if not ticket:
        return
    try:
        deals = mt5.history_deals_get(position=ticket)
        if deals and len(deals) >= 2:
            ep      = sym_st.get("entry_price", 0)
            sl_dist = sym_st.get("sl_dist", 1)
            close_p = deals[-1].price
            sign    = 1 if sym_st.get("direction") == "long" else -1
            r_val   = sign * (close_p - ep) / sl_dist if sl_dist > 0 else 0.0
            logger.info(f"[{canon}] CLOSED price={close_p:.5f} R={r_val:+.3f}")
        else:
            logger.info(f"[{canon}] CLOSED (deal history unavailable)")
    except Exception as e:
        logger.info(f"[{canon}] CLOSED (history error: {e})")


# ==============================================================================
#  SECTION 11 — BAR CLOCK
# ==============================================================================

_CLOCK_SYM_BROKER = None


def get_last_closed_bar_time():
    if _CLOCK_SYM_BROKER is None:
        return None
    rates = mt5.copy_rates_from_pos(_CLOCK_SYM_BROKER, mt5.TIMEFRAME_M5, 0, 2)
    if rates is not None and len(rates) >= 2:
        return pd.Timestamp(rates[1]["time"], unit="s")
    return None


def wait_for_new_bar(last_bar_time):
    while True:
        t = get_last_closed_bar_time()
        if t is not None and t > last_bar_time:
            return t
        time.sleep(10)


# ==============================================================================
#  SECTION 12 — METRICS
# ==============================================================================

class Metrics:
    def __init__(self, active_symbols):
        self.stats  = {s: {"trades": 0, "wins": 0, "total_r": 0.0}
                       for s in active_symbols}
        self.peak   = None
        self.max_dd = 0.0
        self.last_h = None

    def report(self, balance):
        tot_t = sum(d["trades"] for d in self.stats.values())
        tot_w = sum(d["wins"]   for d in self.stats.values())
        tot_r = sum(d["total_r"] for d in self.stats.values())
        wr    = tot_w / tot_t if tot_t else 0.0
        exp   = tot_r / tot_t if tot_t else 0.0
        logger.info(
            f"\n{'='*70}\n[HOURLY REPORT — ORB V2]\n"
            f"  Params set   : {ACTIVE_PARAMS}\n"
            f"  Total trades : {tot_t}\n"
            f"  Win rate     : {wr:.1%}\n"
            f"  Expectancy   : {exp:+.3f}R\n"
            f"  Total R      : {tot_r:+.1f}\n"
            f"  Max DD       : {self.max_dd:.1%}\n"
            f"  Equity       : {balance:,.2f}\n"
        )
        for canon, d in sorted(
            self.stats.items(), key=lambda x: -x[1]["total_r"]
        ):
            if d["trades"] > 0:
                logger.info(
                    f"  {canon:<8} n={d['trades']:>4} "
                    f"WR={d['wins']/d['trades']:.1%} "
                    f"E={d['total_r']/d['trades']:+.3f}R "
                    f"totalR={d['total_r']:+.1f}"
                )
        logger.info("=" * 70)

    def check_hourly(self, balance):
        if self.peak is None or balance > self.peak:
            self.peak = balance
        if self.peak and self.peak > 0:
            self.max_dd = max(self.max_dd, (self.peak - balance) / self.peak)
        h = datetime.datetime.now(datetime.timezone.utc).hour
        if self.last_h is None:
            self.last_h = h
        if h != self.last_h:
            self.last_h = h
            self.report(balance)


# ==============================================================================
#  SECTION 13 — MAIN LOOP
# ==============================================================================

def run_live():
    global _CLOCK_SYM_BROKER

    if not mt5.initialize(
        path=TERMINAL_PATH, login=LOGIN, password=PASSWORD, server=SERVER
    ):
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")

    acct = mt5.account_info()
    logger.info(
        f"MT5 connected | account={acct.login} | "
        f"balance={acct.balance:.2f} | currency={acct.currency}"
    )
    logger.info(f"Engine: ORB V2 | Magic: {MAGIC} | Comment: {COMMENT} | "
                f"Params: {ACTIVE_PARAMS}")

    logger.info("=== SYMBOL DIAGNOSTIC ===")
    sym_map, active_symbols = build_symbol_map()

    for canon in active_symbols:
        broker = sym_map[canon]
        info   = mt5.symbol_info(broker)
        tick   = mt5.symbol_info_tick(broker)
        tvpl   = (info.trade_tick_value / info.trade_tick_size
                  if info and info.trade_tick_size > 0 else "N/A")
        logger.info(
            f"  {canon} ({broker}): digits={info.digits} "
            f"tick_val/lot={tvpl} "
            f"vol_min={info.volume_min} vol_step={info.volume_step} "
            f"params={BEST_PARAMS[canon]}"
        )
    logger.info("=== END DIAGNOSTIC ===")

    if not active_symbols:
        logger.error("No symbols available — shutting down")
        mt5.shutdown()
        return

    _CLOCK_SYM_BROKER = sym_map[active_symbols[0]]

    sym_states = {canon: make_symbol_state() for canon in active_symbols}

    logger.info("=== STARTUP RECOVERY ===")
    for canon in active_symbols:
        broker    = sym_map[canon]
        positions = mt5.positions_get(symbol=broker) or []
        positions = [p for p in positions if p.magic == MAGIC]
        if positions:
            pos = positions[0]
            if COMMENT in (pos.comment or ""):
                sym_states[canon].update(
                    reconstruct_position_state(canon, pos)
                )
            else:
                logger.warning(
                    f"  {canon}: ticket={pos.ticket} "
                    f"comment='{pos.comment}' — not from this engine, skipping"
                )
        else:
            logger.info(f"  {canon}: no open position")
    logger.info("=== END RECOVERY ===")

    logger.info("=== ACTIVE PARAMS ===")
    for canon in active_symbols:
        logger.info(f"  {canon}: {BEST_PARAMS[canon]}")
    logger.info(
        f"  RISK={RISK_PER_TRADE:.2%}  MAX_HOLD={MAX_HOLD}bars  "
        f"ATR_PCT_THRESH={ATR_PCT_THRESH}  WARMUP={WARMUP_M5}bars"
    )
    logger.info("=== END PARAMS ===")

    metrics   = Metrics(active_symbols)
    bar_count = 0

    last_bar_time = get_last_closed_bar_time()
    if last_bar_time is None:
        last_bar_time = pd.Timestamp.utcnow()
    logger.info(f"Seeded bar time: {last_bar_time} UTC — waiting for next M5 close...")

    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1
            today         = datetime.datetime.utcnow().date()

            logger.info(
                f"-- BAR {bar_count} | {new_bar_time} UTC "
                f"----------------------------------------"
            )

            balance = mt5.account_info().balance

            for canon in active_symbols:
                broker = sym_map[canon]
                params = BEST_PARAMS[canon]
                try:
                    process_symbol(
                        canon, broker, sym_states[canon],
                        params, balance, today
                    )
                except Exception as sym_err:
                    logger.exception(f"[{canon}] Error in process_symbol: {sym_err}")

            metrics.check_hourly(balance)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down ORB V2")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(60)

    mt5.shutdown()
    logger.info("MT5 disconnected. ORB V2 stopped.")


if __name__ == "__main__":
    run_live()
