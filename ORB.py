"""
==============================================================================
ORB  —  OPENING RANGE BREAKOUT  |  LIVE ENGINE v4  (M5)
==============================================================================
SYMBOLS:  US30, GER40

CHANGES vs v3:
  + 500k-bar STARTUP FETCH seeds full ATR / expanding-pct-rank history.
    Per-bar loop only fetches the single most-recent closed bar and
    appends it — O(1) data work per bar instead of re-fetching thousands.
  + ATR (Wilder) and expanding-pct-rank updated incrementally each bar.
  + OR recomputed from cached arrays — no full recompute.
  + FTMO symbol aliases added (.cash suffix variants).
  + Daily loss budget uses STARTING_BALANCE (fixed $1,187.50 for $25k),
    not current balance — matches BT exactly.
  + Vol-max cap applied only when per-trade lot would exceed the daily
    budget floor; vol_min floor still always applies.
  + Full parity audit carried forward from v3.

STARTUP FLOW:
  1. fetch_m5_full()  — 500k bars per symbol, build full cache + ATR history
  2. update_atr_incremental() — appends one new bar, updates ATR in-place
  3. process_symbol() — signal detection on last bar of cache, same as v3

DAILY BUDGET LOGIC (matches BT run_eval_simulation / compute_vol_max_cap):
  DAILY_LOSS_BUDGET = STARTING_BALANCE * 4.75%  = $1,187.50  (fixed)
  per_trade_allowance = DAILY_LOSS_BUDGET / max_trades_day_combo
  vol_max_cap = per_trade_allowance / (sl_dist * tick_val)
  vol_max_cap is a CEILING — only bites when the 1% risk lot would exceed
  the per-trade budget.  vol_min floor always applies regardless.

PARITY vs BACKTEST (script5.py) — every live/BT touchpoint:
------------------------------------------------------------------------------
  1. TODAY           — bar timestamp cache["dates"][-1], not wall clock
  2. EOD DATE CHECK  — bar timestamp date  (dates[-1] != entry_date)
  3. EOD HOUR CHECK  — bar timestamp hour  (utc_h[-1] >= close_h)
  4. ENTRY PRICE     — current ask/bid tick ~ open of next bar
  5. SL DISTANCE     — max(sl_range_mult * or_size, atr * 0.05)
                       min floor 0.05*atr applied in _execute_entry
  6. SIGNAL LOGIC    — ATR pct filter, in_session, OR validity,
                       cooldown, daily cap, min_break_atr body filter
  7. BREAKEVEN       — one_r = ep ± sl_dist; triggers SL -> ep
  8. TRAILING STOP   — SL trails bar high/low - trail_mult * atr
  9. RISK GUARD      — actual_loss > MAX_RISK_MULTIPLE * intended → reject
  10. SPREAD         — broker handles natively; no arithmetic in live

LOG:     orb_live_v4.log
MAGIC:   202603262
COMMENT: "ORB_V4"
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
logger = logging.getLogger("ORB_V4")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler(
    "orb_live_v4.log", maxBytes=15_000_000, backupCount=5, encoding="utf-8"
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
COMMENT = "ORB_V4"

# ── Broker / risk constants ───────────────────────────────────────────────────
STARTING_BALANCE  = 25_000.0          # fixed — used for daily budget calc
RISK_PER_TRADE    = 0.01
MAX_RISK_MULTIPLE = 2.0

# Daily loss budget — fixed to STARTING_BALANCE, matches BT exactly.
# vol_max_cap = per_trade_allowance / (sl_dist * tick_val)
# This is a ceiling on lot size; vol_min floor always applies.
DAILY_LOSS_CAP_PCT = 0.0475
DAILY_LOSS_BUDGET  = STARTING_BALANCE * DAILY_LOSS_CAP_PCT   # $1,187.50

# ── Strategy constants ────────────────────────────────────────────────────────
FETCH_BARS_STARTUP = 50000   # full history fetch at startup
MAX_HOLD           = 48
ATR_PERIOD         = 14
ATR_PCT_THRESH     = 0.30
WARMUP_M5          = 200

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

# ── Param sets ────────────────────────────────────────────────────────────────
ACTIVE_PARAMS = "GRID"   # "GRID" | "NEW" | "OLD"

PARAMS_NEW = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "GER40": {"or_minutes": 30, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

PARAMS_OLD = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 1.00,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

PARAMS_GRID = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.0},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

_PARAM_MAP = {"NEW": PARAMS_NEW, "OLD": PARAMS_OLD, "GRID": PARAMS_GRID}
if ACTIVE_PARAMS not in _PARAM_MAP:
    raise ValueError(f"ACTIVE_PARAMS must be 'NEW'/'OLD'/'GRID', got '{ACTIVE_PARAMS}'")
BEST_PARAMS = _PARAM_MAP[ACTIVE_PARAMS]

# max_trades_day_combo — sum across all active symbols, used for budget split
# Computed after symbol resolution; placeholder updated in run_live().
_MAX_TRADES_DAY_COMBO = 0

# ── Symbol aliases — includes FTMO .cash suffix variants ─────────────────────
SYMBOL_ALIASES = {
    "US30":  ["US30.cash", "US30",  "DJ30",    "DJIA",   "WS30",   "DOW30",  "US30Cash"],
    "GER40": ["GER40.cash","GER40", "DAX40",   "DAX",    "GER30",  "DE40",   "GER40Cash"],
}

SYMBOLS = list(SESSION.keys())

# Tick value and vol constraints — populated at startup, never re-fetched
_tick_value_cache: dict = {}

# Per-symbol bar cache — populated at startup, updated incrementally
# Structure per symbol:
#   {
#     "o","h","l","c": np.ndarray  (full history, grows by 1 each bar)
#     "utc_h","utc_m": np.ndarray int32
#     "dates":         np.ndarray  (object, datetime.date)
#     "times":         np.ndarray  (datetime64[ns])
#     "atr14":         np.ndarray  (Wilder ATR, updated incrementally)
#     "atr_wilder_prev": float     (previous ATR value for incremental update)
#     "atr_pct":       np.ndarray  (expanding pct rank, updated incrementally)
#     "atr_pct_hist":  list        (sorted history for bisect — expanding rank)
#     "last_bar_time": pd.Timestamp
#   }
_bar_cache: dict = {}


# ==============================================================================
#  SECTION 1 — SYMBOL RESOLVER
# ==============================================================================

def resolve_symbol(canonical):
    """Try each alias in order; return first broker name that exists."""
    all_broker = {s.name.upper(): s.name for s in (mt5.symbols_get() or [])}
    for alias in SYMBOL_ALIASES[canonical]:
        info = mt5.symbol_info(alias)
        if info is not None:
            if not info.visible:
                mt5.symbol_select(alias, True)
            logger.info(f"  {canonical} -> '{alias}'")
            return alias
        # prefix match fallback
        for up, name in all_broker.items():
            if up.startswith(alias.upper().replace(".CASH", "")):
                mt5.symbol_select(name, True)
                logger.info(f"  {canonical} -> '{name}' (prefix match)")
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

def atr_wilder_full(h, l, c):
    """Full Wilder ATR — called once at startup."""
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


def atr_wilder_update(prev_atr, new_h, new_l, prev_c):
    """
    Incremental Wilder ATR for one new bar.
    tr  = max(high-low, |high-prev_close|, |low-prev_close|)
    atr = prev_atr * (1 - 1/period) + tr * (1/period)
    """
    tr  = max(new_h - new_l,
              abs(new_h - prev_c),
              abs(new_l - prev_c))
    k   = 1.0 / ATR_PERIOD
    return prev_atr * (1.0 - k) + tr * k


def expanding_pct_rank_full(arr):
    """Full expanding pct rank — called once at startup."""
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
    return out, hist   # return hist so we can continue from it


def expanding_pct_rank_update(hist, new_atr_val):
    """
    Incremental pct rank for one new ATR value.
    Returns (rank, updated_hist).
    rank = position of new value in existing sorted history / len(history)
    Then inserts the new value for future bars.
    """
    if np.isnan(new_atr_val):
        return np.nan, hist
    rank = bisect.bisect_left(hist, new_atr_val) / len(hist) if hist else np.nan
    bisect.insort(hist, new_atr_val)
    return rank, hist


# ==============================================================================
#  SECTION 3 — STARTUP: FULL FETCH + CACHE BUILD
# ==============================================================================

def fetch_m5_full(broker_sym):
    """
    Fetch up to FETCH_BARS_STARTUP M5 bars at startup.
    Drops the last (incomplete live) bar.
    Returns DataFrame or None.
    """
    rates = mt5.copy_rates_from_pos(
        broker_sym, mt5.TIMEFRAME_M5, 0, FETCH_BARS_STARTUP + 1
    )
    if rates is None or len(rates) < WARMUP_M5 + ATR_PERIOD + 50:
        logger.error(
            f"[{broker_sym}] Startup fetch failed / insufficient: "
            f"{len(rates) if rates is not None else 0} bars"
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
    df = df.iloc[:-1].reset_index(drop=True)   # drop live incomplete bar
    return df


def build_bar_cache(canon, df):
    """
    Build full bar cache from startup DataFrame.
    Computes ATR and pct-rank in full, stores sorted history for incremental
    updates.
    """
    o = df["open"].values.astype(np.float64)
    h = df["high"].values.astype(np.float64)
    l = df["low"].values.astype(np.float64)
    c = df["close"].values.astype(np.float64)

    atr14          = atr_wilder_full(h, l, c)
    atr_pct, hist  = expanding_pct_rank_full(atr14)

    # Last valid ATR for incremental updates
    valid_atr = atr14[~np.isnan(atr14)]
    atr_prev  = float(valid_atr[-1]) if len(valid_atr) > 0 else 0.0

    n = len(c)
    _bar_cache[canon] = {
        "o":               o,
        "h":               h,
        "l":               l,
        "c":               c,
        "utc_h":           df["utc_hour"].values.astype(np.int32),
        "utc_m":           df["utc_minute"].values.astype(np.int32),
        "dates":           df["date"].values,
        "times":           df["time_utc"].values,
        "atr14":           atr14,
        "atr_wilder_prev": atr_prev,
        "atr_pct":         atr_pct,
        "atr_pct_hist":    hist,        # sorted list — kept alive for bisect
        "last_bar_time":   pd.Timestamp(df["time_utc"].iloc[-1]),
        "n":               n,
    }

    logger.info(
        f"  [{canon}] cache built: {n:,} bars  "
        f"{df['time_utc'].iloc[0].strftime('%Y-%m-%d')} -> "
        f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d')}  "
        f"ATR={atr_prev:.2f}  "
        f"atr_pct={atr_pct[-1]:.3f}  "
        f"pct_hist_len={len(hist):,}"
    )


# ==============================================================================
#  SECTION 4 — INCREMENTAL BAR UPDATE
# ==============================================================================

def fetch_latest_closed_bar(broker_sym):
    """
    Fetch only the 2 most recent bars.
    Bar[0] = most recent CLOSED bar (bar[1] is live/incomplete).
    Returns (time_utc, open, high, low, close) or None.
    """
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 0, 2)
    if rates is None or len(rates) < 2:
        return None
    r = rates[0]   # [0] = older closed bar when fetched from pos 0 with count 2
    # copy_rates_from_pos(sym, tf, 0, 2) returns [bar[-2], bar[-1]]
    # bar[-1] is the live bar; bar[-2] is the last closed bar
    # We want the last closed = rates[-2] index = rates[0] in a 2-bar fetch
    # Actually: pos=0 means most recent. rates[0]=most recent (live), rates[1]=prev closed
    # Correct: use rates[1] for last closed bar
    r = rates[1]
    t = pd.Timestamp(r["time"], unit="s")
    return t, float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])


def append_bar_to_cache(canon, bar_time, o, h, l, c):
    """
    Append one new bar to all cache arrays and update ATR incrementally.
    Called once per M5 bar after wait_for_new_bar() fires.
    """
    cache = _bar_cache[canon]

    # ── Duplicate guard ───────────────────────────────────────────────────────
    if bar_time <= cache["last_bar_time"]:
        return False   # already have this bar

    # ── Build scalar metadata ─────────────────────────────────────────────────
    dt     = bar_time.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
    utc_h  = np.int32(dt.hour)
    utc_m  = np.int32(dt.minute)
    date   = dt.date()
    ts_ns  = np.datetime64(bar_time.value, "ns")

    # ── Incremental ATR ───────────────────────────────────────────────────────
    prev_c   = cache["c"][-1]
    new_atr  = atr_wilder_update(cache["atr_wilder_prev"], h, l, prev_c)

    # ── Incremental pct rank ──────────────────────────────────────────────────
    new_pct, hist = expanding_pct_rank_update(cache["atr_pct_hist"], new_atr)

    # ── Append to arrays (np.append returns new array — reassign) ─────────────
    cache["o"]      = np.append(cache["o"],     o)
    cache["h"]      = np.append(cache["h"],     h)
    cache["l"]      = np.append(cache["l"],     l)
    cache["c"]      = np.append(cache["c"],     c)
    cache["utc_h"]  = np.append(cache["utc_h"], utc_h)
    cache["utc_m"]  = np.append(cache["utc_m"], utc_m)
    cache["dates"]  = np.append(cache["dates"], date)
    cache["times"]  = np.append(cache["times"], ts_ns)
    cache["atr14"]  = np.append(cache["atr14"], new_atr)
    cache["atr_pct"]= np.append(cache["atr_pct"], new_pct)

    cache["atr_wilder_prev"] = new_atr
    cache["atr_pct_hist"]    = hist
    cache["last_bar_time"]   = bar_time
    cache["n"]               = cache["n"] + 1

    return True


# ==============================================================================
#  SECTION 5 — OR COMPUTATION (from cache, no re-fetch)
# ==============================================================================

def compute_or_from_cache(canon, or_bars):
    """
    Compute OR high/low for all bars using the cached arrays.
    Identical logic to BT build_cache_and_signals() OR section.
    Returns (or_high_arr, or_low_arr) — only last element is needed for signal.
    """
    cache   = _bar_cache[canon]
    cfg     = SESSION[canon]
    n       = cache["n"]
    h       = cache["h"]
    l_arr   = cache["l"]
    utc_h   = cache["utc_h"]
    utc_m   = cache["utc_m"]
    dates   = cache["dates"]

    in_session = np.array([
        (utc_h[i] > cfg["open_h"] or
         (utc_h[i] == cfg["open_h"] and utc_m[i] >= cfg["open_m"]))
        and utc_h[i] < cfg["close_h"]
        for i in range(n)
    ])
    is_open_bar = (utc_h == cfg["open_h"]) & (utc_m == cfg["open_m"])

    day_start = {}
    for i in range(n):
        if is_open_bar[i]:
            d = dates[i]
            if d not in day_start:
                day_start[d] = i

    day_or = {}
    for d, si in day_start.items():
        ei = si + or_bars
        if ei <= n:
            day_or[d] = (h[si:ei].max(), l_arr[si:ei].min())

    # We only need the last bar's OR values for signal detection
    # but compute full arrays for correctness / future extension
    or_high = np.full(n, np.nan)
    or_low  = np.full(n, np.nan)
    for i in range(n):
        if not in_session[i]:
            continue
        d = dates[i]
        if d not in day_or or d not in day_start:
            continue
        if i < day_start[d] + or_bars:
            continue
        or_high[i], or_low[i] = day_or[d]

    return or_high, or_low, in_session


# ==============================================================================
#  SECTION 6 — SIGNAL DETECTION ON LAST BAR
# ==============================================================================

def detect_signal_last_bar(canon, params, bars_since_last, day_trades_today):
    """
    Run signal detection on the last (most recently appended) bar.
    Identical logic to BT build_cache_and_signals() signal section and
    v3 detect_signal_last_bar().
    """
    cache   = _bar_cache[canon]
    n       = cache["n"]
    i       = n - 1   # last bar index

    if i < WARMUP_M5:
        return None, None, None, None, None, None

    atr_pct_i = cache["atr_pct"][i]
    if np.isnan(atr_pct_i) or atr_pct_i < ATR_PCT_THRESH:
        return None, None, None, None, None, None

    cfg = SESSION[canon]
    utc_h_i = int(cache["utc_h"][i])
    utc_m_i = int(cache["utc_m"][i])
    in_sess  = (
        (utc_h_i > cfg["open_h"] or
         (utc_h_i == cfg["open_h"] and utc_m_i >= cfg["open_m"]))
        and utc_h_i < cfg["close_h"]
    )
    if not in_sess:
        return None, None, None, None, None, None

    or_bars = OR_BARS[params["or_minutes"]]
    or_high_arr, or_low_arr, _ = compute_or_from_cache(canon, or_bars)

    if np.isnan(or_high_arr[i]) or np.isnan(or_low_arr[i]):
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

    breaks_up   = c_cur > or_high_arr[i]
    breaks_down = c_cur < or_low_arr[i]

    if params["min_break_atr"] > 0:
        strong      = body >= params["min_break_atr"] * atr_val
        breaks_up   = breaks_up   and strong
        breaks_down = breaks_down and strong

    or_size = or_high_arr[i] - or_low_arr[i]
    sl_dist = max(params["sl_range_mult"] * or_size, atr_val * 0.05)

    if breaks_up and not breaks_down:
        return "long",  or_high_arr[i], or_low_arr[i], atr_val, or_size, sl_dist
    if breaks_down and not breaks_up:
        return "short", or_high_arr[i], or_low_arr[i], atr_val, or_size, sl_dist

    return None, None, None, None, None, None


# ==============================================================================
#  SECTION 7 — POSITION SIZING
# ==============================================================================

def compute_vol_max_cap(sl_dist, tick_value_per_lot):
    """
    Lot ceiling from daily budget — matches BT compute_vol_max_cap().
    DAILY_LOSS_BUDGET is fixed to STARTING_BALANCE * 4.75% = $1,187.50.
    per_trade_allowance = DAILY_LOSS_BUDGET / max_trades_day_combo.
    vol_max_cap = per_trade_allowance / (sl_dist * tick_val).
    This is a ceiling; vol_min floor always applies regardless.
    """
    global _MAX_TRADES_DAY_COMBO
    if sl_dist < 1e-9 or tick_value_per_lot <= 0 or _MAX_TRADES_DAY_COMBO == 0:
        return 250.0   # safety backstop
    per_trade = DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO
    cached    = list(_tick_value_cache.values())[0]   # vol_step same for all
    vol_step  = cached["vol_step"]
    vol_min   = cached["vol_min"]
    raw       = per_trade / (sl_dist * tick_value_per_lot)
    cap       = max(vol_min, round(raw / vol_step) * vol_step)
    return round(cap, 8)


def compute_lot_size(broker_sym, sl_dist, balance):
    """
    Lot = balance * 1% / (sl_dist * tick_val), capped by daily budget ceiling.
    Matches BT compute_lot_aware().
    Returns (lot, tick_value_per_lot) or (None, None).
    """
    cached = _tick_value_cache.get(broker_sym)
    if cached is None:
        logger.error(f"[{broker_sym}] not in tick_value_cache")
        return None, None

    tvpl     = cached["tick_value_per_lot"]
    vol_min  = cached["vol_min"]
    vol_step = cached["vol_step"]

    if sl_dist < 1e-9:
        logger.error(f"[{broker_sym}] sl_dist ~ 0")
        return None, None

    vol_max_cap   = compute_vol_max_cap(sl_dist, tvpl)
    risk_amount   = balance * RISK_PER_TRADE
    raw_lot       = risk_amount / (sl_dist * tvpl)
    lot           = max(vol_min, min(vol_max_cap, round(raw_lot / vol_step) * vol_step))
    lot           = round(lot, 8)

    logger.info(
        f"[{broker_sym}] lot_calc: balance={balance:.2f} "
        f"risk={risk_amount:.2f} sl_dist={sl_dist:.5f} "
        f"tvpl={tvpl:.5f} raw={raw_lot:.4f} "
        f"vol_max_cap={vol_max_cap} -> lot={lot}"
    )
    return lot, tvpl


# ==============================================================================
#  SECTION 7b — PRE-ENTRY RISK GUARD
# ==============================================================================

def check_actual_risk(canon, lot, sl_dist, balance, tvpl):
    """
    Reject if actual_loss > MAX_RISK_MULTIPLE * intended_risk.
    Mirrors BT compute_lot_aware() rejection logic.
    """
    intended   = balance * RISK_PER_TRADE
    actual     = lot * sl_dist * tvpl
    multiple   = actual / intended if intended > 0 else float("inf")
    status     = "OK" if multiple <= MAX_RISK_MULTIPLE else "REJECTED"

    logger.info(
        f"[{canon}] RISK_AUDIT [{status}] "
        f"intended={intended:.2f} actual={actual:.2f} "
        f"multiple={multiple:.2f}x lot={lot} "
        f"sl_dist={sl_dist:.5f} tvpl={tvpl:.5f}"
    )

    if multiple > MAX_RISK_MULTIPLE:
        logger.warning(
            f"[{canon}] TRADE REJECTED — actual risk {actual:.2f} "
            f"is {multiple:.2f}x intended {intended:.2f} "
            f"(limit {MAX_RISK_MULTIPLE}x)"
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
        logger.warning(f"[{broker_sym}] SL modify failed retcode={code} new_sl={new_sl:.5f}")
        return False
    return True


def send_close_order(broker_sym, position):
    otype = (
        mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY
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
    logger.info(f"[{broker_sym}] CLOSED ticket={position.ticket} price={price:.5f}")
    return True


# ==============================================================================
#  SECTION 9 — PER-SYMBOL STATE
# ==============================================================================

def make_symbol_state():
    return {
        "positions":        [],
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
    """today must be cache["dates"][-1] — never wall clock."""
    if sym_st["day_trades_date"] != today:
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0


def _reconstruct_position_record(canon, position):
    entry_time = datetime.datetime.fromtimestamp(
        position.time, tz=datetime.timezone.utc
    )
    now_utc    = datetime.datetime.now(tz=datetime.timezone.utc)
    hold_count = max(0, int((now_utc - entry_time).total_seconds() / 300))
    direction  = "long" if position.type == mt5.ORDER_TYPE_BUY else "short"
    ep         = position.price_open
    sl_price   = position.sl or 0.0
    sl_dist    = abs(ep - sl_price) if sl_price > 0 else 0.01
    be_active  = False
    if sl_price > 0:
        if direction == "long"  and sl_price >= ep: be_active = True
        if direction == "short" and sl_price <= ep: be_active = True
    logger.info(
        f"[{canon}] RECOVERED ticket={position.ticket}: "
        f"dir={direction} ep={ep:.5f} sl={sl_price:.5f} "
        f"hold~{hold_count}bars be={be_active}"
    )
    rec = _make_position_record(
        ticket=ticket, direction=direction, entry_price=ep,
        sl_dist=sl_dist, current_sl=sl_price,
        entry_date=entry_time.date(), entry_atr=None,
    )
    rec["be_active"]  = be_active
    rec["hold_count"] = hold_count
    return rec


# ==============================================================================
#  SECTION 10 — PER-BAR PROCESSING
# ==============================================================================

def process_symbol(canon, broker_sym, sym_st, params, balance):
    """
    Called once per M5 bar per symbol.
    Cache already updated by append_bar_to_cache() in main loop before
    this is called — no fetch needed here.
    """
    cache = _bar_cache[canon]
    today = cache["dates"][-1]   # bar-derived date — never wall clock

    _reset_daily_counter(sym_st, today)
    sym_st["bars_since_last"] += 1

    broker_positions = mt5.positions_get(symbol=broker_sym) or []
    broker_positions = [p for p in broker_positions if p.magic == MAGIC]
    broker_tickets   = {p.ticket for p in broker_positions}

    # Desync recovery
    known_tickets = {pr["ticket"] for pr in sym_st["positions"]}
    for bp in broker_positions:
        if bp.ticket not in known_tickets:
            logger.warning(f"[{canon}] Desync: unknown ticket={bp.ticket} — recovering")
            rec = _reconstruct_position_record(canon, bp)
            sym_st["positions"].append(rec)
            sym_st["day_trades_count"] = min(
                sym_st["day_trades_count"] + 1, params["max_trades_day"]
            )

    # Detect server-side closes (SL hit)
    closed_records = [pr for pr in sym_st["positions"]
                      if pr["ticket"] not in broker_tickets]
    for pr in closed_records:
        logger.info(f"[{canon}] ticket={pr['ticket']} closed server-side (SL hit)")
        _log_close_record(canon, pr)
        sym_st["positions"].remove(pr)

    # Manage each open position
    broker_pos_map = {p.ticket: p for p in broker_positions}
    for pr in list(sym_st["positions"]):
        bp = broker_pos_map.get(pr["ticket"])
        _manage_position_record(canon, broker_sym, pr, params, cache,
                                bp, today, sym_st)

    # Signal detection on last bar
    direction, or_high_val, or_low_val, atr_val, or_size, sl_dist = \
        detect_signal_last_bar(
            canon, params,
            sym_st["bars_since_last"],
            sym_st["day_trades_count"],
        )

    if direction is None:
        return

    logger.info(
        f"[{canon}] SIGNAL {direction.upper()} "
        f"or_high={or_high_val:.5f} or_low={or_low_val:.5f} "
        f"or_size={or_size:.5f} sl_dist={sl_dist:.5f} atr={atr_val:.5f} "
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
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    ep = tick.ask if direction == "long" else tick.bid

    min_sl_dist = 0.05 * atr_val
    if sl_dist < min_sl_dist:
        sl_dist = min_sl_dist

    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist
    if direction == "long"  and sl_price >= ep:
        sl_price = ep - min_sl_dist; sl_dist = min_sl_dist
    if direction == "short" and sl_price <= ep:
        sl_price = ep + min_sl_dist; sl_dist = min_sl_dist

    lot, tvpl = compute_lot_size(broker_sym, sl_dist, balance)
    if lot is None:
        logger.error(f"[{canon}] Lot calc failed — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    if not check_actual_risk(canon, lot, sl_dist, balance, tvpl):
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

    # Fast fill confirm — poll up to 300ms
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
        ticket=ticket, direction=direction, entry_price=actual_ep,
        sl_dist=sl_dist, current_sl=actual_sl,
        entry_date=today, entry_atr=atr_val,
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
    i         = cache["n"] - 1
    direction = pr["direction"]
    ep        = pr["entry_price"]
    sl_dist   = pr["sl_dist"]
    atr_cur   = cache["atr14"][i]
    bar_h     = cache["h"][i]
    bar_l     = cache["l"][i]
    cfg       = SESSION[canon]

    pr["hold_count"] += 1
    hc = pr["hold_count"]

    # EOD — bar timestamp date and bar timestamp hour
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
        logger.info(f"[{canon}] MAX HOLD ticket={pr['ticket']} — closing")
        if broker_pos:
            send_close_order(broker_sym, broker_pos)
        _log_close_record(canon, pr)
        sym_st["positions"].remove(pr)
        return

    # Breakeven
    one_r = ep + (sl_dist if direction == "long" else -sl_dist)
    if not pr["be_active"]:
        triggered = (
            (direction == "long"  and bar_h >= one_r) or
            (direction == "short" and bar_l <= one_r)
        )
        if triggered:
            pr["be_active"]  = True
            pr["current_sl"] = ep
            logger.info(f"[{canon}] BE triggered ticket={pr['ticket']} SL -> {ep:.5f}")

    # Trailing stop
    if pr["be_active"]:
        ta = atr_cur if (not np.isnan(atr_cur) and atr_cur > 0) \
             else (pr["entry_atr"] or sl_dist)
        trail_mult = params["trail_atr_mult"]
        if direction == "long":
            pr["current_sl"] = max(pr["current_sl"], bar_h - trail_mult * ta)
        else:
            pr["current_sl"] = min(pr["current_sl"], bar_l + trail_mult * ta)

    # Push updated SL to broker
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
            logger.info(f"[{canon}] CLOSED ticket={ticket} (deal history unavailable)")
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
            f"\n{'='*70}\n[HOURLY REPORT — ORB V4]\n"
            f"  Params set      : {ACTIVE_PARAMS}\n"
            f"  Starting balance: ${STARTING_BALANCE:,.0f}\n"
            f"  Daily budget    : ${DAILY_LOSS_BUDGET:.2f} "
            f"({DAILY_LOSS_CAP_PCT:.2%})\n"
            f"  Total trades    : {tot_t}\n"
            f"  Win rate        : {wr:.1%}\n"
            f"  Expectancy      : {exp:+.3f}R\n"
            f"  Total R         : {tot_r:+.1f}\n"
            f"  Max DD          : {self.max_dd:.1%}\n"
            f"  Equity          : {balance:,.2f}\n"
        )
        for canon, d in sorted(self.stats.items(), key=lambda x: -x[1]["total_r"]):
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
    try:
        process_symbol(canon, broker, sym_st, params, balance)
    except Exception as e:
        logger.exception(f"[{canon}] Error in process_symbol: {e}")


# ==============================================================================
#  SECTION 13 — MAIN LOOP
# ==============================================================================

def run_live():
    global _CLOCK_SYM_BROKER, _MAX_TRADES_DAY_COMBO

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
        f"Engine: ORB V4 | Magic: {MAGIC} | Comment: {COMMENT} | "
        f"Params: {ACTIVE_PARAMS}"
    )
    logger.info(
        f"Starting balance (for budget): ${STARTING_BALANCE:,.0f}  "
        f"Daily budget: ${DAILY_LOSS_BUDGET:.2f} ({DAILY_LOSS_CAP_PCT:.2%})"
    )

    # ── Symbol resolution ─────────────────────────────────────────────────────
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
            f"tvpl={tvpl:.6f} "
            f"vol_min={info.volume_min} vol_step={info.volume_step} "
            f"pip={pip} params={BEST_PARAMS[canon]}"
        )

    # Compute max_trades_day_combo across all active symbols
    _MAX_TRADES_DAY_COMBO = sum(
        BEST_PARAMS[s]["max_trades_day"] for s in active_symbols
    )
    per_trade_allowance = DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO if _MAX_TRADES_DAY_COMBO else 0
    logger.info(
        f"  max_trades_day_combo={_MAX_TRADES_DAY_COMBO}  "
        f"per_trade_allowance=${per_trade_allowance:.2f}"
    )
    logger.info(f"  Risk guard: MAX_RISK_MULTIPLE={MAX_RISK_MULTIPLE}x")
    logger.info("=== END DIAGNOSTIC ===")

    if not active_symbols:
        logger.error("No symbols available — shutting down")
        mt5.shutdown()
        return

    # ── Startup 500k-bar fetch — build full ATR cache ─────────────────────────
    logger.info(f"\n=== STARTUP FETCH ({FETCH_BARS_STARTUP:,} bars) ===")
    for canon in active_symbols:
        broker = sym_map[canon]
        logger.info(f"  [{canon}] fetching {FETCH_BARS_STARTUP:,} bars...")
        df = fetch_m5_full(broker)
        if df is None:
            logger.error(f"  [{canon}] startup fetch failed — cannot trade this symbol")
            active_symbols = [s for s in active_symbols if s != canon]
            continue
        build_bar_cache(canon, df)
    logger.info("=== END STARTUP FETCH ===\n")

    if not active_symbols:
        logger.error("No symbols with data — shutting down")
        mt5.shutdown()
        return

    _CLOCK_SYM_BROKER = sym_map[active_symbols[0]]

    sym_states = {canon: make_symbol_state() for canon in active_symbols}

    # ── Startup recovery ──────────────────────────────────────────────────────
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
                f"  {canon}: recovered {len(sym_states[canon]['positions'])} position(s)"
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
    logger.info("=== END PARAMS ===\n")

    metrics   = Metrics(active_symbols)
    bar_count = 0

    last_bar_time = get_last_closed_bar_time()
    if last_bar_time is None:
        last_bar_time = pd.Timestamp.utcnow()
    logger.info(
        f"Seeded bar time: {last_bar_time} UTC — "
        f"waiting for next M5 close..."
    )

    # ── Main bar loop ─────────────────────────────────────────────────────────
    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1

            wall_today = datetime.datetime.utcnow().date()
            logger.info(
                f"-- BAR {bar_count} | {new_bar_time} UTC "
                f"(wall={wall_today}) "
                f"----------------------------------------"
            )

            # ── Step 1: append new bar to each symbol's cache ─────────────────
            for canon in active_symbols:
                broker = sym_map[canon]
                bar = fetch_latest_closed_bar(broker)
                if bar is None:
                    logger.warning(f"[{canon}] bar fetch failed — skipping append")
                    continue
                bar_time, o, h, l, c = bar
                added = append_bar_to_cache(canon, bar_time, o, h, l, c)
                if not added:
                    logger.debug(f"[{canon}] duplicate bar {bar_time} — skipped")

            # ── Step 2: process signal + position management ───────────────────
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
                t.join(timeout=25)

            metrics.check_hourly(balance)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down ORB V4")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(60)

    mt5.shutdown()
    logger.info("MT5 disconnected. ORB V4 stopped.")


if __name__ == "__main__":
    run_live()
