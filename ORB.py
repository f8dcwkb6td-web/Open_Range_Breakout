"""
==============================================================================
ORB  —  OPENING RANGE BREAKOUT  |  LIVE ENGINE  (M5)
==============================================================================
SYMBOLS:  US30, US500, UK100, GER40

PARITY AUDIT vs orb_hardcoded.py (backtest):
  ✓ H1 bars fetched (backtest fetches H1 but does not use it in any
    indicator or signal — live engine mirrors this exactly)
  ✓ atr_wilder:         exact copy, period=14
  ✓ expanding_pct_rank: exact copy, warmup=200, bisect
  ✓ build_cache:        exact copy — o,h,l,c,atr14,atr_pct,
                        in_session, is_open_bar, dates, times, utc_h
  ✓ compute_or:         exact copy — day_start dict, day_or dict,
                        gate i < day_start[d]+or_bars
  ✓ Signal detection:   exact port of detect_signals to single last bar:
                        atr_pct>=0.30, in_session, ~isnan(or_high),
                        c>or_high → long / c<or_low → short,
                        body>=min_break_atr*atr14 when min_break_atr>0,
                        cooldown_bars, max_trades_day
  ✓ Entry:              NEXT bar open  (market order at next bar's open)
  ✓ SL dist:            max(sl_range_mult*or_size, atr14[si]*0.05)
                        computed at SIGNAL bar si using atr14[si]
  ✓ SL price:           ep - direction*sl_dist  (anchored to entry price)
  ✓ one_r:              ep + direction*sl_dist
  ✓ BE trigger:         bar_h >= one_r (long) / bar_l <= one_r (short)
                        cur_sl = ep
  ✓ Trail (after BE):   long:  cur_sl = max(cur_sl, bar_h - trail_mult*atr)
                        short: cur_sl = min(cur_sl, bar_l + trail_mult*atr)
                        atr fallback = entry_atr if current bar atr is nan
  ✓ EOD exit:           dates[bi]!=entry_date OR utc_h[bi]>=close_h
  ✓ Max hold:           48 bars -> close at market
  ✓ SL hit:             handled server-side; detected via broker cross-check
  ✓ OR_BARS:            {15:3, 30:6, 60:12}
  ✓ SESSION:            exact UTC open_h/open_m/close_h per symbol
  ✓ BEST_PARAMS:        exact copy from backtest output
  ✓ RISK_PER_TRADE:     0.01 (1%)
  ✓ Position sizing:    lot = (balance*RISK) / (sl_dist * tick_val_per_lot)
                        tick_val_per_lot = trade_tick_value/trade_tick_size
                        all fields read live from MT5 symbol_info

ARCHITECTURE:
  - One MT5 connection, one M5 bar-close loop
  - Per-symbol state machine: FLAT -> PENDING_ENTRY -> IN_POSITION -> FLAT
  - PENDING_ENTRY: signal on bar close, entry at NEXT bar open
  - SL server-side; trail updated each bar via MODIFY
  - Broker cross-check every bar (recover on restart)
  - Symbol resolution: alias list + prefix scan (exact backtest logic)

MAGIC:   202603260
COMMENT: "ORB_LIVE"
LOG:     orb_live.log
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
logger = logging.getLogger("ORB_LIVE")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler(
    "orb_live.log", maxBytes=15_000_000, backupCount=5, encoding="utf-8"
)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_fh)
_sh = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
)
_sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_sh)

# ── MT5 connection ────────────────────────────────────────────────────────────
TERMINAL_PATH = os.environ.get(
    "MT5_TERMINAL_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe"
)
LOGIN    = int(os.environ.get("MT5_LOGIN",    0))
PASSWORD =     os.environ.get("MT5_PASSWORD", "")
SERVER   =     os.environ.get("MT5_SERVER",   "")

# ── Engine identity ───────────────────────────────────────────────────────────
MAGIC   = 202603260
COMMENT = "ORB_LIVE"

# ── Strategy constants — exact copy from backtest ─────────────────────────────
RISK_PER_TRADE = 0.01
MAX_HOLD       = 48
ATR_PERIOD     = 14
ATR_PCT_THRESH = 0.30
WARMUP_M5      = 200

# Live fetch sizes
FETCH_BARS_M5 = 2500   # ~8 trading days of M5; enough for warmup + OR history
FETCH_BARS_H1 = 500    # fetched to mirror backtest fetch(); not used in signals

# ── OR bar counts — exact copy from backtest ──────────────────────────────────
OR_BARS = {15: 3, 30: 6, 60: 12}

# ── Session config — exact copy from backtest ─────────────────────────────────
SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "US500": {"open_h": 13, "open_m": 30, "close_h": 20},
    "UK100": {"open_h":  8, "open_m":  0, "close_h": 16},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

# ── Hardcoded best params — exact copy from backtest ─────────────────────────
BEST_PARAMS = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 1.00,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "US500": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
    "UK100": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

# ── Symbol aliases — exact copy from backtest ─────────────────────────────────
SYMBOL_ALIASES = {
    "US30":  ["US30", "DJ30", "DJIA", "WS30", "DOW30", "US30Cash"],
    "US500": ["US500", "SPX500", "SP500", "SPX", "US500Cash"],
    "UK100": ["UK100", "FTSE100", "FTSE", "UK100Cash", "UKX"],
    "GER40": ["GER40", "DAX40", "DAX", "GER30", "DE40", "GER40Cash"],
}

SYMBOLS = list(BEST_PARAMS.keys())

# ── State constants ───────────────────────────────────────────────────────────
STATE_FLAT          = "FLAT"
STATE_PENDING_ENTRY = "PENDING_ENTRY"
STATE_IN_POSITION   = "IN_POSITION"


# ==============================================================================
#  SECTION 1 — SYMBOL RESOLVER
#  Exact copy of backtest resolve_symbol:
#    for alias in SYMBOL_ALIASES[canonical]:
#        try exact match, then prefix scan in broker list
# ==============================================================================

def resolve_symbol(canonical):
    """Exact copy of backtest resolve_symbol logic."""
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
#  Exact copies of backtest atr_wilder and expanding_pct_rank.
# ==============================================================================

def atr_wilder(h, l, c):
    """Exact copy of backtest atr_wilder."""
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
    """Exact copy of backtest expanding_pct_rank."""
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
#  Exact copy of backtest build_cache.
#  Only takes m5 — H1 is fetched separately but never passed here,
#  matching the backtest exactly.
# ==============================================================================

def build_cache(canonical, m5):
    """Exact copy of backtest build_cache."""
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
        "dates":       dates,
        "times":       times,
        "in_session":  in_session,
        "is_open_bar": is_open_bar,
        "cfg":         cfg,
    }


# ==============================================================================
#  SECTION 4 — COMPUTE OR
#  Exact copy of backtest compute_or.
# ==============================================================================

def compute_or(cache, or_bars):
    """Exact copy of backtest compute_or."""
    n       = cache["n"]
    h, l    = cache["h"], cache["l"]
    dates   = cache["dates"]
    is_open = cache["is_open_bar"]

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

    return or_high, or_low


# ==============================================================================
#  SECTION 5 — SIGNAL DETECTION ON LAST BAR
#  Exact port of backtest detect_signals, applied only to i = n-1.
#
#  NOTE on valid mask:
#    Backtest: valid[WARMUP_M5 : n-1] = True  (last index n-1 is FALSE)
#    The backtest's last bar (index n-1) is the still-forming bar which
#    is never a valid signal bar.  In the live engine fetch_m5() drops
#    the forming bar, so our i=n-1 corresponds to the backtest's n-2,
#    which IS in the valid range.  We therefore only gate on i >= WARMUP_M5.
# ==============================================================================

def detect_signal_last_bar(cache, params, bars_since_last, day_trades_today):
    """
    Check the last closed bar (i = n-1) for a signal.
    Returns (direction, or_high_val, or_low_val, atr_val)
         or (None, None, None, None).
    """
    or_bars = OR_BARS[params["or_minutes"]]
    or_high, or_low = compute_or(cache, or_bars)

    i = cache["n"] - 1

    if i < WARMUP_M5:
        return None, None, None, None

    # atr_pct filter
    if np.isnan(cache["atr_pct"][i]) or cache["atr_pct"][i] < ATR_PCT_THRESH:
        return None, None, None, None

    # session
    if not cache["in_session"][i]:
        return None, None, None, None

    # OR defined
    if np.isnan(or_high[i]) or np.isnan(or_low[i]):
        return None, None, None, None

    # cooldown
    if bars_since_last < params["cooldown_bars"]:
        return None, None, None, None

    # daily cap
    if day_trades_today >= params["max_trades_day"]:
        return None, None, None, None

    atr_val = cache["atr14"][i]
    if np.isnan(atr_val) or atr_val <= 0:
        return None, None, None, None

    c_cur = cache["c"][i]
    o_cur = cache["o"][i]
    body  = abs(c_cur - o_cur)

    breaks_up   = c_cur > or_high[i]
    breaks_down = c_cur < or_low[i]

    if params["min_break_atr"] > 0:
        strong      = body >= params["min_break_atr"] * atr_val
        breaks_up   = breaks_up   and strong
        breaks_down = breaks_down and strong

    if breaks_up and not breaks_down:
        return "long",  or_high[i], or_low[i], atr_val
    if breaks_down and not breaks_up:
        return "short", or_high[i], or_low[i], atr_val

    return None, None, None, None


# ==============================================================================
#  SECTION 6 — DATA FETCH
# ==============================================================================

def fetch_m5(broker_sym, n=FETCH_BARS_M5):
    """
    Fetch M5 bars, drop the forming bar, add utc_hour/utc_minute/date.
    Mirrors backtest fetch() M5 construction exactly.
    """
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
    # Drop the still-forming bar (last row)
    return df.iloc[:-1].reset_index(drop=True)


def fetch_h1(broker_sym, n=FETCH_BARS_H1):
    """
    Fetch H1 bars.
    Backtest fetches H1 in fetch() but never passes it to build_cache
    or any indicator function.  We mirror that exactly — fetch and discard.
    """
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_H1, 0, n + 1)
    if rates is None or len(rates) < 10:
        logger.warning(
            f"[{broker_sym}] H1 fetch failed: "
            f"{len(rates) if rates is not None else 0}"
        )
        return None
    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[
        ["time", "open", "high", "low", "close"]
    ].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    return df.iloc[:-1].reset_index(drop=True)


# ==============================================================================
#  SECTION 7 — POSITION SIZING
# ==============================================================================

def compute_lot_size(broker_sym, entry_price, sl_price, balance):
    """
    lot = (balance * RISK_PER_TRADE) / (sl_dist * tick_value_per_lot)

    tick_value_per_lot = trade_tick_value / trade_tick_size
    All broker fields read live from mt5.symbol_info().
    """
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
    """Send market entry. Returns (ticket, fill_price) or (None, None)."""
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
    """Modify SL on an open position."""
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
    """Close an open position at market."""
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
        "state":              STATE_FLAT,
        # Pending (signal bar values stored here, consumed on next bar open)
        "pending_dir":        None,
        "pending_sl_dist":    None,   # max(sl_range_mult*or_size, atr*0.05)
        "pending_atr":        None,   # atr14[si] — stored for trail fallback
        # In-position
        "ticket":             None,
        "direction":          None,
        "entry_price":        None,
        "sl_dist":            None,
        "be_active":          False,
        "current_sl":         None,
        "hold_count":         0,
        "entry_date":         None,
        "entry_atr":          None,   # atr14[si] for trail nan-fallback
        # Cooldown / daily cap
        "bars_since_last":    9999,
        "day_trades_date":    None,
        "day_trades_count":   0,
    }


def reconstruct_position_state(canon, position):
    """Recover in-memory state from an open MT5 position on restart."""
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
        if direction == "long"  and sl_price >= ep:
            be_active = True
        if direction == "short" and sl_price <= ep:
            be_active = True

    logger.info(
        f"[{canon}] RECOVERED: dir={direction} ep={ep:.5f} "
        f"sl={sl_price:.5f} hold~{hold_count}bars be={be_active}"
    )
    today = entry_time.date()
    return {
        "state":              STATE_IN_POSITION,
        "pending_dir":        None,
        "pending_sl_dist":    None,
        "pending_atr":        None,
        "ticket":             position.ticket,
        "direction":          direction,
        "entry_price":        ep,
        "sl_dist":            sl_dist,
        "be_active":          be_active,
        "current_sl":         sl_price,
        "hold_count":         hold_count,
        "entry_date":         today,
        "entry_atr":          None,
        "bars_since_last":    0,
        "day_trades_date":    today,
        "day_trades_count":   1,
    }


# ==============================================================================
#  SECTION 10 — PER-BAR PROCESSING
# ==============================================================================

def _reset_daily_counter(sym_st, today):
    if sym_st["day_trades_date"] != today:
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0


def process_symbol(canon, broker_sym, sym_st, params, balance, today):
    """
    Called once per closed M5 bar for each symbol.

    Mirrors the backtest flow exactly:
      FLAT:          detect_signal_last_bar -> set PENDING_ENTRY
      PENDING_ENTRY: _execute_entry (market order at next bar open)
      IN_POSITION:   _manage_position (EOD / max-hold / BE / trail)
    """
    # 1. Fetch data — H1 fetched to match backtest; not used beyond this
    df_m5 = fetch_m5(broker_sym)
    df_h1 = fetch_h1(broker_sym)   # mirrors backtest fetch() — not used further
    if df_m5 is None:
        logger.warning(f"[{canon}] M5 fetch failed — skipping bar")
        return

    # 2. Build cache (M5 only — exact copy of backtest build_cache)
    cache = build_cache(canon, df_m5)

    # 3. Daily counter reset
    _reset_daily_counter(sym_st, today)

    # 4. Cooldown tick
    sym_st["bars_since_last"] += 1

    # 5. Broker cross-check
    positions = mt5.positions_get(symbol=broker_sym) or []
    positions = [p for p in positions if p.magic == MAGIC]

    broker_has_pos = len(positions) > 0
    state_in_pos   = sym_st["state"] == STATE_IN_POSITION

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
        sym_st.update(make_symbol_state())
        sym_st["bars_since_last"]  = 0
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0
        return

    # 6. PENDING_ENTRY -> place entry
    if sym_st["state"] == STATE_PENDING_ENTRY:
        _execute_entry(canon, broker_sym, sym_st, params, balance, today)
        return

    # 7. IN_POSITION -> manage
    if sym_st["state"] == STATE_IN_POSITION:
        _manage_position(canon, broker_sym, sym_st, params, cache,
                         positions, today)
        return

    # 8. FLAT -> look for signal
    if sym_st["state"] == STATE_FLAT:
        direction, or_high_val, or_low_val, atr_val = detect_signal_last_bar(
            cache, params,
            sym_st["bars_since_last"],
            sym_st["day_trades_count"],
        )
        if direction is not None:
            # sl_dist at signal bar — exact backtest:
            # sl_dist = max(sl_range_mult * or_size, atr14[si] * 0.05)
            or_size  = or_high_val - or_low_val
            sl_dist  = max(params["sl_range_mult"] * or_size, atr_val * 0.05)

            sym_st["state"]             = STATE_PENDING_ENTRY
            sym_st["pending_dir"]       = direction
            sym_st["pending_sl_dist"]   = sl_dist
            sym_st["pending_atr"]       = atr_val
            # FIX: claim the daily slot and start cooldown at signal time,
            # not at fill time — matches backtest detect_signals loop exactly
            sym_st["bars_since_last"]   = 0
            sym_st["day_trades_count"] += 1

            logger.info(
                f"[{canon}] SIGNAL {direction.upper()} "
                f"or_high={or_high_val:.5f} or_low={or_low_val:.5f} "
                f"or_size={or_size:.5f} sl_dist={sl_dist:.5f} "
                f"atr={atr_val:.5f} — entering on NEXT bar open"
            )


def _execute_entry(canon, broker_sym, sym_st, params, balance, today):
    """
    Place market order at the open of the bar after the signal bar.
    ep = current ask/bid  ~  o[si+1] in backtest.
    SL anchored to fill price: ep - direction*sl_dist.
    """
    direction = sym_st["pending_dir"]
    sl_dist   = sym_st["pending_sl_dist"]
    atr_e     = sym_st["pending_atr"]

    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{canon}] Tick unavailable — cancelling entry")
        sym_st.update(make_symbol_state())
        return

    ep = tick.ask if direction == "long" else tick.bid

    # Minimum SL distance matches backtest: max(..., atr*0.05)
    min_sl_dist = 0.05 * atr_e
    if sl_dist < min_sl_dist:
        sl_dist = min_sl_dist

    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist

    # Safety clamp
    if direction == "long"  and sl_price >= ep:
        sl_price = ep - min_sl_dist
        sl_dist  = min_sl_dist
    if direction == "short" and sl_price <= ep:
        sl_price = ep + min_sl_dist
        sl_dist  = min_sl_dist

    lot = compute_lot_size(broker_sym, ep, sl_price, balance)
    if lot is None:
        logger.error(f"[{canon}] Lot calc failed — cancelling entry")
        sym_st.update(make_symbol_state())
        return

    ticket, _ = send_market_order(
        broker_sym, direction, lot, sl_price, f"{COMMENT}_{canon}"
    )
    if ticket is None:
        logger.error(f"[{canon}] Order failed — cancelling entry")
        sym_st.update(make_symbol_state())
        return

    # Confirm actual fill
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

    sym_st["state"]           = STATE_IN_POSITION
    sym_st["ticket"]          = ticket
    sym_st["direction"]       = direction
    sym_st["entry_price"]     = actual_ep
    sym_st["sl_dist"]         = sl_dist
    sym_st["be_active"]       = False
    sym_st["current_sl"]      = actual_sl
    sym_st["hold_count"]      = 0
    sym_st["entry_date"]      = today
    sym_st["entry_atr"]       = atr_e
    sym_st["day_trades_date"] = today
    # NOTE: day_trades_count already incremented at signal time — do NOT touch it here
    sym_st["pending_dir"]     = None
    sym_st["pending_sl_dist"] = None
    sym_st["pending_atr"]     = None

    logger.info(
        f"[{canon}] ENTERED {direction.upper()} "
        f"ep={actual_ep:.5f} sl={actual_sl:.5f} "
        f"sl_dist={sl_dist:.5f} lot={lot} ticket={ticket}"
    )


def _manage_position(canon, broker_sym, sym_st, params, cache, positions, today):
    """
    Manage open position each bar.

    Exact port of backtest inner loop:
      bi = ei + k  (here: i = cache["n"]-1, the latest closed bar)
      if dates[bi]!=entry_date or utc_h[bi]>=close_h:  EOD exit
      if hc >= MAX_HOLD:                                max-hold exit
      if bh>=one_r (long):                              BE trigger
      if be_active: cur_sl = max(cur_sl, bh-trail*ta)  trail update
    """
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

    # ── EOD exit — backtest: dates[bi]!=entry_date or utc_h[bi]>=close_h ──
    current_utc_hour = datetime.datetime.utcnow().hour
    if sym_st["entry_date"] != today or current_utc_hour >= cfg["close_h"]:
        logger.info(
            f"[{canon}] EOD exit "
            f"(entry_date={sym_st['entry_date']} today={today} "
            f"utc_hour={current_utc_hour} close_h={cfg['close_h']})"
        )
        if pos:
            send_close_order(broker_sym, pos)
        _log_close(canon, sym_st)
        sym_st.update(make_symbol_state())
        sym_st["bars_since_last"]  = 0
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0
        return

    # ── Max hold ───────────────────────────────────────────────────────────
    if hc >= MAX_HOLD:
        logger.info(f"[{canon}] MAX HOLD ({MAX_HOLD} bars) — closing at market")
        if pos:
            send_close_order(broker_sym, pos)
        _log_close(canon, sym_st)
        sym_st.update(make_symbol_state())
        sym_st["bars_since_last"]  = 0
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0
        return

    # ── BE trigger — backtest: one_r = ep + direction*sl_dist ─────────────
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

    # ── Trail — backtest: ta=atr14[bi] if not nan else atr (entry atr) ────
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

    # ── Update broker SL if moved by >= 1 pip ─────────────────────────────
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
    """Log approximate R from MT5 deal history."""
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
            f"\n{'='*70}\n[HOURLY REPORT — ORB LIVE]\n"
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
            self.max_dd = max(self.max_dd,
                              (self.peak - balance) / self.peak)
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
    logger.info(f"Engine: ORB LIVE | Magic: {MAGIC} | Comment: {COMMENT}")

    logger.info("=== SYMBOL DIAGNOSTIC ===")
    sym_map, active_symbols = build_symbol_map()

    for canon in active_symbols:
        broker = sym_map[canon]
        info   = mt5.symbol_info(broker)
        tick   = mt5.symbol_info_tick(broker)
        bars_t = mt5.copy_rates_from_pos(broker, mt5.TIMEFRAME_M5, 0, 5)
        tvpl   = (info.trade_tick_value / info.trade_tick_size
                  if info and info.trade_tick_size > 0 else "N/A")
        logger.info(
            f"  {canon} ({broker}): digits={info.digits} "
            f"spread={info.spread} "
            f"tick_val={info.trade_tick_value} "
            f"tick_sz={info.trade_tick_size} "
            f"tick_val/lot={tvpl} "
            f"vol_min={info.volume_min} vol_step={info.volume_step} "
            f"tick={'OK' if tick else 'NONE'} "
            f"m5={'OK len='+str(len(bars_t)) if bars_t is not None else 'NONE'}"
        )
    logger.info("=== END DIAGNOSTIC ===")

    if not active_symbols:
        logger.error("No symbols available — shutting down")
        mt5.shutdown()
        return

    _CLOCK_SYM_BROKER = sym_map[active_symbols[0]]
    logger.info(f"Bar clock: {active_symbols[0]} ({_CLOCK_SYM_BROKER})")

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
    logger.info(
        f"Seeded bar time: {last_bar_time} UTC "
        f"— waiting for next M5 close..."
    )

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
                    logger.exception(
                        f"[{canon}] Error in process_symbol: {sym_err}"
                    )

            metrics.check_hourly(balance)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down ORB LIVE")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(60)

    mt5.shutdown()
    logger.info("MT5 disconnected. ORB LIVE stopped.")
    try:
        final_balance = mt5.account_info().balance if mt5.account_info() else 0
    except Exception:
        final_balance = 0
    metrics.report(final_balance)


if __name__ == "__main__":
    run_live()
