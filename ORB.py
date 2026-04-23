"""
==============================================================================
ORB  —  OPENING RANGE BREAKOUT  |  LIVE ENGINE v3  (M5)
==============================================================================
SYMBOLS:  US30, US500, UK100, GER40

CHANGES vs v2:
  + US500 added.
  + GRID param set added (locked from grid search 2024-10-21 -> 2026-04-17).
    Set ACTIVE_PARAMS = "GRID" / "NEW" / "OLD" to switch.
  + Full BT parity audit applied.

PARITY vs BACKTEST (script5.py) — every live/BT touchpoint:
------------------------------------------------------------------------------
  1. TODAY — bar timestamp, not wall clock
     today = cache["dates"][-1]
     Matches BT: dates[bi] in resolve_trade().

  2. EOD DATE CHECK — bar timestamp date
     if entry_date != today  (both from bar timestamp)
     Matches BT: dates[bi] != entry_date.

  3. EOD HOUR CHECK — bar timestamp hour
     current_bar_hour = int(cache["utc_h"][-1])
     if current_bar_hour >= cfg["close_h"]
     Matches BT: utc_h[bi] >= cfg["close_h"].

  4. ENTRY PRICE — current ask/bid tick ~ open of next bar
     Long  -> tick.ask  |  Short -> tick.bid
     Matches BT: ep = o[ei] (+spread for long in BT, broker handles live).

  5. SL DISTANCE — identical formula to BT resolve_trade()
     In detect_signal_last_bar():
         sl_dist = max(sl_range_mult * or_size, atr * 0.05)
     In _execute_entry() after tick price known:
         min_sl_dist = 0.05 * atr
         if sl_dist < min_sl_dist: sl_dist = min_sl_dist
     Matches BT exactly.

  6. SIGNAL LOGIC — identical to BT build_cache_and_signals()
     Same ATR pct filter, in_session, OR validity, cooldown, daily cap,
     min_break_atr body filter.
     FIX vs v2: sl_dist < 3.0 rejection REMOVED.  BT never applies this
     check (BT checks sl_dist < 0.0 which is impossible).  This was the
     only remaining signal-count divergence between live and BT.

  7. BREAKEVEN — identical to BT
     one_r = ep + direction * sl_dist
     if long  and bar_h >= one_r: be = True, cur_sl = ep
     if short and bar_l <= one_r: be = True, cur_sl = ep

  8. TRAILING STOP — identical to BT
     if long:  cur_sl = max(cur_sl, bar_h - trail_mult * atr)
     if short: cur_sl = min(cur_sl, bar_l + trail_mult * atr)

  9. RISK GUARD — identical threshold to BT compute_lot_aware()
     rejected if actual_loss > MAX_RISK_MULTIPLE * intended_risk

  10. SPREAD — broker handles natively in live; no arithmetic needed.

LOG:     orb_live_v3.log
MAGIC:   202603262
COMMENT: "ORB_V3"
==============================================================================
"""

import os, sys, io, time, logging, datetime, bisect
import threading
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
logger = logging.getLogger("ORB_V3")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler(
    "orb_live_v3.log", maxBytes=15_000_000, backupCount=5, encoding="utf-8"
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
MAGIC   = 202603262
COMMENT = "ORB_V3"

# ── Strategy constants ────────────────────────────────────────────────────────
RISK_PER_TRADE    = 0.01
MAX_HOLD          = 48
ATR_PERIOD        = 14
ATR_PCT_THRESH    = 0.30
WARMUP_M5         = 200
MAX_RISK_MULTIPLE = 2.0

FETCH_BARS_M5 = 600   # warmup(200) + OR(12) + ATR(14) + generous buffer — nothing more needed live

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "US500": {"open_h": 13, "open_m": 30, "close_h": 20},
    "UK100": {"open_h":  8, "open_m":  0, "close_h": 16},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

# ── THREE PARAM SETS — set ACTIVE_PARAMS to switch ───────────────────────────
ACTIVE_PARAMS = "GRID"   # "GRID" | "NEW" | "OLD"

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

# Locked from grid search (2024-10-21 -> 2026-04-17)
PARAMS_GRID = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.0},
    "US500": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "UK100": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.0},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

_PARAM_MAP = {"NEW": PARAMS_NEW, "OLD": PARAMS_OLD, "GRID": PARAMS_GRID}
if ACTIVE_PARAMS not in _PARAM_MAP:
    raise ValueError(
        f"ACTIVE_PARAMS must be 'NEW', 'OLD', or 'GRID', got '{ACTIVE_PARAMS}'"
    )
BEST_PARAMS = _PARAM_MAP[ACTIVE_PARAMS]

# ── Symbol aliases ────────────────────────────────────────────────────────────
SYMBOL_ALIASES = {
    "US30":  ["US30", "DJ30", "DJIA", "WS30", "DOW30", "US30Cash"],
    "US500": ["US500", "SPX500", "SP500", "SPX", "US500Cash"],
    "UK100": ["UK100", "FTSE100", "FTSE", "UK100Cash", "UKX"],
    "GER40": ["GER40", "DAX40", "DAX", "GER30", "DE40", "GER40Cash"],
}

SYMBOLS = list(SESSION.keys())

# Tick values cached at startup — symbol_info is slow, no need to call every bar
_tick_value_cache: dict = {}


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
#  SECTION 2 — INDICATORS  (identical to backtest)
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
#  SECTION 3 — BUILD CACHE  (identical to backtest)
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
        "dates":       dates,
        "times":       times,
        "in_session":  in_session,
        "is_open_bar": is_open_bar,
        "cfg":         cfg,
    }


# ==============================================================================
#  SECTION 4 — COMPUTE OR  (identical to backtest)
# ==============================================================================

def compute_or(cache, or_bars):
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
#
#  Matches BT build_cache_and_signals() signal logic exactly.
#  Key fix vs v2: sl_dist < 3.0 rejection removed — BT does not apply it.
# ==============================================================================

def detect_signal_last_bar(cache, params, bars_since_last, day_trades_today):
    or_bars = OR_BARS[params["or_minutes"]]
    or_high, or_low = compute_or(cache, or_bars)

    i = cache["n"] - 1

    if i < WARMUP_M5:
        return None, None, None, None, None, None

    if np.isnan(cache["atr_pct"][i]) or cache["atr_pct"][i] < ATR_PCT_THRESH:
        return None, None, None, None, None, None

    if not cache["in_session"][i]:
        return None, None, None, None, None, None

    if np.isnan(or_high[i]) or np.isnan(or_low[i]):
        return None, None, None, None, None, None

    if bars_since_last < params["cooldown_bars"]:
        return None, None, None, None, None, None

    if day_trades_today >= params["max_trades_day"]:
        return None, None, None, None, None, None

    atr_val = cache["atr14"][i]
    if np.isnan(atr_val) or atr_val <= 0:
        return None, None, None, None, None, None

    c_cur = cache["c"][i]
    o_cur = cache["o"][i]
    body  = abs(c_cur - o_cur)

    breaks_up   = c_cur > or_high[i]
    breaks_down = c_cur < or_low[i]

    if params["min_break_atr"] > 0:
        strong      = body >= params["min_break_atr"] * atr_val
        breaks_up   = breaks_up   and strong
        breaks_down = breaks_down and strong

    or_size = or_high[i] - or_low[i]

    # SL distance — identical formula to BT resolve_trade():
    #   sl_dist = max(sl_range_mult * or_size, atr * 0.05)
    # min_sl_dist floor (0.05 * atr) applied later in _execute_entry,
    # same as BT applies it inside resolve_trade() after entry price known.
    # NO sl_dist < 3.0 check — BT only rejects sl_dist < 0.0 (impossible).
    sl_dist = max(params["sl_range_mult"] * or_size, atr_val * 0.05)

    if breaks_up and not breaks_down:
        return "long",  or_high[i], or_low[i], atr_val, or_size, sl_dist
    if breaks_down and not breaks_up:
        return "short", or_high[i], or_low[i], atr_val, or_size, sl_dist

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
    # Drop incomplete live bar — only look at fully closed bars.
    # Matches BT: valid[WARMUP_M5 : n-1] = True (last bar excluded).
    return df.iloc[:-1].reset_index(drop=True)


# ==============================================================================
#  SECTION 7 — POSITION SIZING
# ==============================================================================

def compute_lot_size(broker_sym, sl_dist, balance):
    """
    Lot = balance * risk / (sl_dist * tick_value_per_lot), floored to vol_min.
    Identical to BT compute_lot_aware().
    Uses cached tick value and symbol constraints — no symbol_info call per bar.
    Returns (lot, tick_value_per_lot) or (None, None) on error.
    """
    cached = _tick_value_cache.get(broker_sym)
    if cached is None:
        logger.error(f"[{broker_sym}] not in tick_value_cache — was startup skipped?")
        return None, None

    tick_value_per_lot = cached["tick_value_per_lot"]
    vol_min            = cached["vol_min"]
    vol_max            = cached["vol_max"]
    step               = cached["vol_step"]

    if sl_dist < 1e-9:
        logger.error(f"[{broker_sym}] sl_dist ~ 0")
        return None, None

    risk_amount = balance * RISK_PER_TRADE
    raw_lot     = risk_amount / (sl_dist * tick_value_per_lot)

    lot = max(vol_min, min(vol_max, round(raw_lot / step) * step))
    lot = round(lot, 8)

    logger.info(
        f"[{broker_sym}] lot_calc: balance={balance:.2f} "
        f"risk={risk_amount:.2f} sl_dist={sl_dist:.5f} "
        f"tick_val/lot={tick_value_per_lot:.5f} "
        f"raw={raw_lot:.4f} -> lot={lot}"
    )
    return lot, tick_value_per_lot


# ==============================================================================
#  SECTION 7b — PRE-ENTRY RISK GUARD
# ==============================================================================

def check_actual_risk(canon, lot, sl_dist, balance, tick_value_per_lot):
    """
    Reject if actual monetary loss at SL > MAX_RISK_MULTIPLE * intended risk.
    Mirrors BT compute_lot_aware(): rejected = risk_multiple > MAX_RISK_MULTIPLE
    """
    intended_risk = balance * RISK_PER_TRADE
    actual_loss   = lot * sl_dist * tick_value_per_lot
    risk_multiple = (actual_loss / intended_risk
                     if intended_risk > 0 else float("inf"))

    status = "OK" if risk_multiple <= MAX_RISK_MULTIPLE else "REJECTED"
    logger.info(
        f"[{canon}] RISK_AUDIT [{status}] "
        f"balance={balance:.2f} "
        f"intended={intended_risk:.2f} ({RISK_PER_TRADE:.1%}) "
        f"actual={actual_loss:.2f} ({actual_loss/balance:.2%}) "
        f"multiple={risk_multiple:.2f}x "
        f"lot={lot} sl_dist={sl_dist:.5f} "
        f"tick_val/lot={tick_value_per_lot:.5f} "
        f"max_allowed={MAX_RISK_MULTIPLE}x"
    )

    if risk_multiple > MAX_RISK_MULTIPLE:
        logger.warning(
            f"[{canon}] TRADE REJECTED — actual risk {actual_loss:.2f} "
            f"is {risk_multiple:.2f}x intended {intended_risk:.2f} "
            f"(limit {MAX_RISK_MULTIPLE}x). Cause: vol_min floor."
        )
        return False

    return True


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
#
#  The BT allows overlapping positions on the same symbol (up to
#  max_trades_day per day).  The live engine must match: sym_st holds a list
#  of open position dicts ("positions") rather than a single state.
#  Shared counters (bars_since_last, day_trades_count) live at the symbol
#  level, exactly as the BT signal pre-computation uses them.
# ==============================================================================

def make_symbol_state():
    return {
        "positions":        [],   # list of open position dicts for this symbol
        "bars_since_last":  9999,
        "day_trades_date":  None,
        "day_trades_count": 0,
    }


def _make_position_record(ticket, direction, entry_price, sl_dist,
                           current_sl, entry_date, entry_atr):
    return {
        "ticket":      ticket,
        "direction":   direction,
        "entry_price": entry_price,
        "sl_dist":     sl_dist,
        "be_active":   False,
        "current_sl":  current_sl,
        "hold_count":  0,
        "entry_date":  entry_date,
        "entry_atr":   entry_atr,
    }


def _reset_daily_counter(sym_st, today):
    """today must be cache["dates"][-1], not utcnow().date()."""
    if sym_st["day_trades_date"] != today:
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0


def _reconstruct_position_record(canon, position):
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
        f"[{canon}] RECOVERED ticket={position.ticket}: "
        f"dir={direction} ep={ep:.5f} "
        f"sl={sl_price:.5f} hold~{hold_count}bars be={be_active}"
    )
    rec = _make_position_record(
        ticket=position.ticket,
        direction=direction,
        entry_price=ep,
        sl_dist=sl_dist,
        current_sl=sl_price,
        entry_date=entry_time.date(),
        entry_atr=None,
    )
    rec["be_active"]   = be_active
    rec["hold_count"]  = hold_count
    return rec


# ==============================================================================
#  SECTION 10 — PER-BAR PROCESSING
# ==============================================================================

def process_symbol(canon, broker_sym, sym_st, params, balance):
    """
    Process one bar for one symbol.

    Supports concurrent open positions on the same symbol (matches BT which
    allows overlapping trades up to max_trades_day).

    today derived from bar timestamp — never wall clock.
    """
    df_m5 = fetch_m5(broker_sym)
    if df_m5 is None:
        logger.warning(f"[{canon}] M5 fetch failed — skipping bar")
        return

    cache = build_cache(canon, df_m5)

    # Bar-derived date — matches BT dates[bi]
    today = cache["dates"][-1]

    _reset_daily_counter(sym_st, today)
    sym_st["bars_since_last"] += 1

    # All broker positions for this symbol owned by this engine
    broker_positions = mt5.positions_get(symbol=broker_sym) or []
    broker_positions = [p for p in broker_positions if p.magic == MAGIC]
    broker_tickets   = {p.ticket for p in broker_positions}

    # ── Desync recovery: broker has positions we don't know about ────────────
    known_tickets = {pr["ticket"] for pr in sym_st["positions"]}
    for bp in broker_positions:
        if bp.ticket not in known_tickets:
            logger.warning(
                f"[{canon}] Desync: unknown ticket={bp.ticket} — recovering"
            )
            rec = _reconstruct_position_record(canon, bp)
            sym_st["positions"].append(rec)
            sym_st["day_trades_count"] = min(
                sym_st["day_trades_count"] + 1, params["max_trades_day"]
            )

    # ── Detect positions closed server-side (SL hit) ─────────────────────────
    closed_records = [pr for pr in sym_st["positions"]
                      if pr["ticket"] not in broker_tickets]
    for pr in closed_records:
        logger.info(
            f"[{canon}] ticket={pr['ticket']} closed server-side (SL hit)"
        )
        _log_close_record(canon, pr)
        sym_st["positions"].remove(pr)

    # ── Manage each open position ────────────────────────────────────────────
    broker_pos_map = {p.ticket: p for p in broker_positions}
    for pr in list(sym_st["positions"]):
        bp = broker_pos_map.get(pr["ticket"])
        _manage_position_record(canon, broker_sym, pr, params, cache,
                                bp, today, sym_st)

    # ── Check for new entry signal ───────────────────────────────────────────
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

    # Claim slot before risk guard so counters are always truthful
    sym_st["bars_since_last"]  = 0
    sym_st["day_trades_count"] += 1

    _execute_entry(canon, broker_sym, sym_st, params,
                   balance, today, direction, sl_dist, atr_val)


def _execute_entry(canon, broker_sym, sym_st, params,
                   balance, today, direction, sl_dist, atr_val):
    """
    Place market order on signal bar close.
    On success, appends a new position record to sym_st["positions"].
    """
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{canon}] Tick unavailable — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    ep = tick.ask if direction == "long" else tick.bid

    # min_sl_dist floor — mirrors BT resolve_trade()
    min_sl_dist = 0.05 * atr_val
    if sl_dist < min_sl_dist:
        sl_dist = min_sl_dist

    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist

    # Edge-case guard
    if direction == "long"  and sl_price >= ep:
        sl_price = ep - min_sl_dist
        sl_dist  = min_sl_dist
    if direction == "short" and sl_price <= ep:
        sl_price = ep + min_sl_dist
        sl_dist  = min_sl_dist

    lot, tick_value_per_lot = compute_lot_size(broker_sym, sl_dist, balance)
    if lot is None:
        logger.error(f"[{canon}] Lot calc failed — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    # Risk guard — mirrors BT compute_lot_aware() rejection
    if not check_actual_risk(canon, lot, sl_dist, balance, tick_value_per_lot):
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    ticket, _ = send_market_order(
        broker_sym, direction, lot, sl_price, f"{COMMENT}_{canon}"
    )
    if ticket is None:
        logger.error(f"[{canon}] Order failed — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    # Fast fill confirm — poll up to 300ms instead of unconditional 500ms sleep
    filled = []
    for _ in range(6):
        time.sleep(0.05)
        filled = [p for p in (mt5.positions_get(symbol=broker_sym) or [])
                  if p.magic == MAGIC and p.ticket == ticket]
        if filled:
            break

    if filled:
        actual_ep = filled[0].price_open
        actual_sl = filled[0].sl
        sl_dist   = abs(actual_ep - actual_sl)
        if sl_dist < 1e-9:
            sl_dist = min_sl_dist
    else:
        actual_ep = ep
        actual_sl = sl_price

    rec = _make_position_record(
        ticket=ticket,
        direction=direction,
        entry_price=actual_ep,
        sl_dist=sl_dist,
        current_sl=actual_sl,
        entry_date=today,
        entry_atr=atr_val,
    )
    sym_st["positions"].append(rec)

    logger.info(
        f"[{canon}] ENTERED {direction.upper()} ticket={ticket} "
        f"ep={actual_ep:.5f} sl={actual_sl:.5f} "
        f"sl_dist={sl_dist:.5f} lot={lot} "
        f"open_positions={len(sym_st['positions'])}"
    )


def _manage_position_record(canon, broker_sym, pr, params, cache,
                             broker_pos, today, sym_st):
    """
    Manage one open position record for one bar.
    Identical EOD, breakeven, and trailing logic to BT resolve_trade().
    Removes the record from sym_st["positions"] on close.
    """
    i         = cache["n"] - 1
    direction = pr["direction"]
    ep        = pr["entry_price"]
    sl_dist   = pr["sl_dist"]
    atr_cur   = cache["atr14"][i]
    bar_h     = cache["h"][i]
    bar_l     = cache["l"][i]
    cfg       = cache["cfg"]

    pr["hold_count"] += 1
    hc = pr["hold_count"]

    # ── EOD — bar timestamp date and bar timestamp hour ──────────────────────
    # Matches BT: dates[bi] != entry_date or utc_h[bi] >= cfg["close_h"]
    current_bar_hour = int(cache["utc_h"][i])
    if pr["entry_date"] != today or current_bar_hour >= cfg["close_h"]:
        logger.info(
            f"[{canon}] EOD exit ticket={pr['ticket']} "
            f"(entry_date={pr['entry_date']} bar_date={today} "
            f"bar_utc_hour={current_bar_hour} close_h={cfg['close_h']})"
        )
        if broker_pos:
            send_close_order(broker_sym, broker_pos)
        _log_close_record(canon, pr)
        sym_st["positions"].remove(pr)
        return

    if hc >= MAX_HOLD:
        logger.info(
            f"[{canon}] MAX HOLD ({MAX_HOLD} bars) ticket={pr['ticket']} — closing"
        )
        if broker_pos:
            send_close_order(broker_sym, broker_pos)
        _log_close_record(canon, pr)
        sym_st["positions"].remove(pr)
        return

    # ── Breakeven — identical to BT ──────────────────────────────────────────
    one_r = ep + (sl_dist if direction == "long" else -sl_dist)
    if not pr["be_active"]:
        triggered = (
            (direction == "long"  and bar_h >= one_r) or
            (direction == "short" and bar_l <= one_r)
        )
        if triggered:
            pr["be_active"]  = True
            pr["current_sl"] = ep
            logger.info(
                f"[{canon}] BE triggered ticket={pr['ticket']} — SL -> {ep:.5f}"
            )

    # ── Trailing stop — identical to BT ─────────────────────────────────────
    if pr["be_active"]:
        ta = atr_cur
        if np.isnan(ta) or ta <= 0:
            ta = pr["entry_atr"] or sl_dist
        trail_mult = params["trail_atr_mult"]
        if direction == "long":
            pr["current_sl"] = max(pr["current_sl"], bar_h - trail_mult * ta)
        else:
            pr["current_sl"] = min(pr["current_sl"], bar_l + trail_mult * ta)

    # ── Push updated SL to broker ────────────────────────────────────────────
    if broker_pos is not None:
        broker_sl = broker_pos.sl or 0.0
        new_sl    = pr["current_sl"]
        pip       = _tick_value_cache.get(broker_sym, {}).get("pip", 0.0001)
        if abs(new_sl - broker_sl) >= pip:
            if modify_sl(broker_sym, pr["ticket"], new_sl):
                logger.info(
                    f"[{canon}] SL updated ticket={pr['ticket']} "
                    f"{broker_sl:.5f} -> {new_sl:.5f} "
                    f"(hold={hc} be={pr['be_active']})"
                )


def _log_close_record(canon, pr):
    ticket = pr.get("ticket")
    if not ticket:
        return
    try:
        deals = mt5.history_deals_get(position=ticket)
        if deals and len(deals) >= 2:
            ep      = pr.get("entry_price", 0)
            sl_dist = pr.get("sl_dist", 1)
            close_p = deals[-1].price
            sign    = 1 if pr.get("direction") == "long" else -1
            r_val   = sign * (close_p - ep) / sl_dist if sl_dist > 0 else 0.0
            logger.info(
                f"[{canon}] CLOSED ticket={ticket} "
                f"price={close_p:.5f} R={r_val:+.3f}"
            )
        else:
            logger.info(f"[{canon}] CLOSED ticket={ticket} "
                        f"(deal history unavailable)")
    except Exception as e:
        logger.info(f"[{canon}] CLOSED ticket={ticket} (history error: {e})")


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
            f"\n{'='*70}\n[HOURLY REPORT — ORB V3]\n"
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


def _process_symbol_safe(canon, broker, sym_st, params, balance):
    """Thread wrapper — catches exceptions so one symbol never kills others."""
    try:
        process_symbol(canon, broker, sym_st, params, balance)
    except Exception as e:
        logger.exception(f"[{canon}] Error in process_symbol: {e}")


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
    logger.info(
        f"Engine: ORB V3 | Magic: {MAGIC} | Comment: {COMMENT} | "
        f"Params: {ACTIVE_PARAMS}"
    )

    logger.info("=== SYMBOL DIAGNOSTIC ===")
    sym_map, active_symbols = build_symbol_map()

    for canon in active_symbols:
        broker = sym_map[canon]
        info   = mt5.symbol_info(broker)
        if info is None:
            logger.error(f"  {canon}: symbol_info None — skipping")
            continue
        tvpl = (info.trade_tick_value / info.trade_tick_size
                if info.trade_tick_size > 0 else 1.0)
        pip  = 10 ** (-info.digits + 1)
        _tick_value_cache[broker] = {
            "tick_value_per_lot": tvpl,
            "vol_min":  info.volume_min  if info.volume_min  > 0 else 0.01,
            "vol_max":  info.volume_max  if info.volume_max  > 0 else 100.0,
            "vol_step": info.volume_step if info.volume_step > 0 else 0.01,
            "pip":      pip,
        }
        logger.info(
            f"  {canon} ({broker}): digits={info.digits} "
            f"tick_val/lot={tvpl:.6f} "
            f"vol_min={info.volume_min} vol_step={info.volume_step} "
            f"pip={pip} params={BEST_PARAMS[canon]}"
        )
    logger.info(f"  Risk guard: MAX_RISK_MULTIPLE={MAX_RISK_MULTIPLE}x")
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
            for pos in positions:
                if COMMENT in (pos.comment or ""):
                    rec = _reconstruct_position_record(canon, pos)
                    sym_states[canon]["positions"].append(rec)
                    sym_states[canon]["day_trades_count"] = min(
                        sym_states[canon]["day_trades_count"] + 1,
                        BEST_PARAMS[canon]["max_trades_day"]
                    )
                else:
                    logger.warning(
                        f"  {canon}: ticket={pos.ticket} "
                        f"comment='{pos.comment}' — not from this engine, skipping"
                    )
            logger.info(
                f"  {canon}: recovered "
                f"{len(sym_states[canon]['positions'])} position(s)"
            )
        else:
            logger.info(f"  {canon}: no open positions")
    logger.info("=== END RECOVERY ===")

    logger.info("=== ACTIVE PARAMS ===")
    for canon in active_symbols:
        logger.info(f"  {canon}: {BEST_PARAMS[canon]}")
    logger.info(
        f"  RISK={RISK_PER_TRADE:.2%}  MAX_HOLD={MAX_HOLD}bars  "
        f"ATR_PCT_THRESH={ATR_PCT_THRESH}  WARMUP={WARMUP_M5}bars  "
        f"MAX_RISK_MULTIPLE={MAX_RISK_MULTIPLE}x"
    )
    logger.info("=== END PARAMS ===")

    metrics   = Metrics(active_symbols)
    bar_count = 0

    last_bar_time = get_last_closed_bar_time()
    if last_bar_time is None:
        last_bar_time = pd.Timestamp.utcnow()
    logger.info(
        f"Seeded bar time: {last_bar_time} UTC — "
        f"waiting for next M5 close..."
    )

    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1

            # Wall clock for log label only — never used for EOD decisions
            wall_today = datetime.datetime.utcnow().date()

            logger.info(
                f"-- BAR {bar_count} | {new_bar_time} UTC "
                f"(wall={wall_today}) "
                f"----------------------------------------"
            )

            balance = mt5.account_info().balance

            threads = []
            for canon in active_symbols:
                broker = sym_map[canon]
                params = BEST_PARAMS[canon]
                t = threading.Thread(
                    target=_process_symbol_safe,
                    args=(canon, broker, sym_states[canon], params, balance),
                    daemon=True,
                )
                threads.append(t)

            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=25)  # hard cap per bar — never block the clock

            metrics.check_hourly(balance)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down ORB V3")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(60)

    mt5.shutdown()
    logger.info("MT5 disconnected. ORB V3 stopped.")


if __name__ == "__main__":
    run_live()
