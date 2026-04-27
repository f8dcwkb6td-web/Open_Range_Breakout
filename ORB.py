"""
==============================================================================
ORB  —  OPENING RANGE BREAKOUT  |  LIVE ENGINE v5  (M5)
==============================================================================
SYMBOLS:  US30, GER40

ARCHITECTURE (why v4's OR bugs are gone):
──────────────────────────────────────────────────────────────────────────────
v4 broke OR and direction because it maintained one giant rolling array cache
and ran OR calculation over it with index arithmetic.  When bars were appended
incrementally the index alignment silently drifted, producing wrong OR windows
(2-bar instead of 3) and inverted directions.

v5 splits the cache into two completely separate concerns:

  1. OR + SIGNAL DETECTION — fresh fetch per bar, same as v2.
     fetch_m5_signal() pulls ~24 h of M5 bars (288 bars) every tick.
     That is enough for session open detection, OR window, and close price.
     No index drift is possible because the array is rebuilt from scratch.

  2. ATR PERCENTILE RANK — persistent atr_cache, same concept as v4.
     Needs long history (200+ bars warm-up, then expanding rank).
     Updated incrementally: one Wilder step per bar.
     The cache is ONLY used for the atr_pct gate and trailing-stop ATR.
     It is NOT involved in OR or direction logic at all.

CSV AUTO-WRITE:
──────────────────────────────────────────────────────────────────────────────
  On startup, load existing CSV (if any) and gap-fill from broker.
  After each bar, append the new closed bar to CSV.
  Next startup only fetches the gap since the last CSV row — no large fetch.
  CSV path: <script_dir>/<CANON>.csv  (e.g. US30.csv, GER40.csv)

TIMESTAMP PARITY (inherited from v2, unchanged):
──────────────────────────────────────────────────────────────────────────────
  today        = bar timestamp (df["date"].iloc[-1]), NOT utcnow().date()
  EOD bar hour = bar timestamp hour, NOT datetime.utcnow().hour
  Entry        = market order on signal bar close (open of next bar)
  SL distance  = max(sl_range_mult * or_size, atr * 0.05)

OTHER FIXES KEPT FROM v4:
──────────────────────────────────────────────────────────────────────────────
  + stops_level clamping on every SL modify and entry (Error 100016)
  + Full SIGNAL_EVAL log on every in-session bar
  + SIGNAL_SKIP reason logged on every early return
  + Direction verification: "direction=LONG (BUY) close > OR_HIGH"
  + Risk guard: reject if actual_risk > MAX_RISK_MULTIPLE * intended
  + Daily loss budget cap on position sizing

LOG:     orb_live_v5.log
MAGIC:   202603263
COMMENT: "ORB_V5"
==============================================================================
"""

import os, sys, io, time, logging, datetime, bisect, csv
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
logger = logging.getLogger("ORB_V5")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler(
    "orb_live_v5.log", maxBytes=15_000_000, backupCount=5, encoding="utf-8"
)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_fh)
_sh = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
)
_sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_sh)

# ── MT5 connection ────────────────────────────────────────────────────────────
TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"
LOGIN         = 1513214612
PASSWORD      = "h2QE?*1!v5fQ"
SERVER        = "FTMO-Demo"

# ── Engine identity ───────────────────────────────────────────────────────────
MAGIC   = 202603263
COMMENT = "ORB_V5"

# ── Broker / risk constants ───────────────────────────────────────────────────
STARTING_BALANCE   = 25_000.0
RISK_PER_TRADE     = 0.01
MAX_RISK_MULTIPLE  = 2.0
DAILY_LOSS_CAP_PCT = 0.0475
DAILY_LOSS_BUDGET  = STARTING_BALANCE * DAILY_LOSS_CAP_PCT  # $1,187.50

# ── Strategy constants ────────────────────────────────────────────────────────
#
# SIGNAL FETCH: how many M5 bars to pull for OR + signal each tick.
# 288 = one full calendar day.  Enough to see today's session open and OR.
# Keep this small — it is fetched EVERY bar.
SIGNAL_BARS = 400   # ~33 h, comfortably covers overnight + today session

# ATR CACHE: bars needed for reliable expanding percentile rank.
# Only fetched at startup (then updated bar-by-bar from CSV + gap).
ATR_WARMUP_BARS = 500   # ~1.7 days M5

MAX_HOLD        = 48
ATR_PERIOD      = 14
ATR_PCT_THRESH  = 0.30
WARMUP_M5       = 200   # minimum bars before ATR pct rank is valid

M5_SECONDS = 300        # seconds per M5 bar

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

# ── Param sets ────────────────────────────────────────────────────────────────
ACTIVE_PARAMS = "GRID"

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
    raise ValueError(f"ACTIVE_PARAMS must be NEW/OLD/GRID, got '{ACTIVE_PARAMS}'")
BEST_PARAMS = _PARAM_MAP[ACTIVE_PARAMS]

SYMBOL_ALIASES = {
    "US30":  ["US30.cash", "US30",  "DJ30",  "DJIA",  "WS30", "DOW30", "US30Cash"],
    "GER40": ["GER40.cash","GER40", "DAX40", "DAX",   "GER30","DE40",  "GER40Cash"],
}
SYMBOLS = list(SESSION.keys())

# ── Global caches ─────────────────────────────────────────────────────────────
_tick_info:  dict = {}   # broker_sym -> {tvpl, vol_min, vol_step, pip, point}
_atr_cache:  dict = {}   # canon -> {atr_prev, pct_hist, last_bar_time, n_bars}
_csv_lock:   dict = {}   # canon -> threading.Lock()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

_MAX_TRADES_DAY_COMBO = 0


# ==============================================================================
#  SECTION 0 — STOPS LEVEL HELPER
# ==============================================================================

def get_min_sl_distance(broker_sym: str) -> float:
    info = _tick_info.get(broker_sym)
    point = info["point"] if info else 0.0001
    fallback = 5.0 * point

    sym_info = mt5.symbol_info(broker_sym)
    if sym_info is None:
        return fallback

    sl_lvl = int(sym_info.trade_stops_level or 0)
    pt     = sym_info.point if sym_info.point > 0 else point
    return max(sl_lvl * pt, fallback) if sl_lvl > 0 else fallback


def clamp_sl(broker_sym: str, direction: str, price: float, sl: float) -> float:
    min_dist = get_min_sl_distance(broker_sym)
    if direction == "long":
        limit = price - min_dist
        if sl > limit:
            logger.info(f"[{broker_sym}] SL clamped {sl:.5f}->{limit:.5f}")
            return limit
    else:
        limit = price + min_dist
        if sl < limit:
            logger.info(f"[{broker_sym}] SL clamped {sl:.5f}->{limit:.5f}")
            return limit
    return sl


# ==============================================================================
#  SECTION 1 — SYMBOL RESOLVER
# ==============================================================================

def resolve_symbol(canonical: str) -> object:
    all_broker = {s.name.upper(): s.name for s in (mt5.symbols_get() or [])}
    for alias in SYMBOL_ALIASES[canonical]:
        info = mt5.symbol_info(alias)
        if info is not None:
            if not info.visible:
                mt5.symbol_select(alias, True)
            logger.info(f"  {canonical} -> '{alias}'")
            return alias
        stem = alias.upper().replace(".CASH", "")
        for up, name in all_broker.items():
            if up.startswith(stem):
                mt5.symbol_select(name, True)
                logger.info(f"  {canonical} -> '{name}' (prefix)")
                return name
    logger.warning(f"  {canonical}: not found on broker")
    return None


def build_symbol_map():
    sym_map, active, skipped = {}, [], []
    for canon in SYMBOLS:
        broker = resolve_symbol(canon)
        if broker:
            sym_map[canon] = broker
            active.append(canon)
        else:
            skipped.append(canon)
    logger.info(f"Active: {active}  Skipped: {skipped}")
    return sym_map, active


# ==============================================================================
#  SECTION 2 — ATR INDICATORS
# ==============================================================================

def _atr_wilder_full(h, l, c):
    n  = len(h)
    tr = np.empty(n)
    tr[0]  = h[0] - l[0]
    tr[1:] = np.maximum(h[1:] - l[1:],
               np.maximum(np.abs(h[1:] - c[:-1]),
                          np.abs(l[1:] - c[:-1])))
    out = np.full(n, np.nan)
    if n < ATR_PERIOD:
        return out
    out[ATR_PERIOD - 1] = tr[:ATR_PERIOD].mean()
    k = 1.0 / ATR_PERIOD
    for i in range(ATR_PERIOD, n):
        out[i] = out[i - 1] * (1.0 - k) + tr[i] * k
    return out


def _atr_wilder_step(prev_atr: float, new_h: float, new_l: float, prev_c: float) -> float:
    tr = max(new_h - new_l, abs(new_h - prev_c), abs(new_l - prev_c))
    k  = 1.0 / ATR_PERIOD
    return prev_atr * (1.0 - k) + tr * k


def _pct_rank_update(hist: list, val: float) -> tuple[float, list]:
    """Return (rank, updated_hist).  rank=NaN if hist empty."""
    rank = bisect.bisect_left(hist, val) / len(hist) if hist else float("nan")
    bisect.insort(hist, val)
    return rank, hist


# ==============================================================================
#  SECTION 3 — CSV PERSISTENCE
# ==============================================================================

CSV_COLUMNS = ["time_utc", "open", "high", "low", "close"]
CSV_DTYPE   = {"open": np.float64, "high": np.float64,
               "low":  np.float64, "close": np.float64}

def _csv_path(canon: str) -> str:
    return os.path.join(SCRIPT_DIR, f"{canon}.csv")


def _load_csv(canon: str) -> object:
    path = _csv_path(canon)
    if not os.path.isfile(path):
        return None
    try:
        df = pd.read_csv(path, parse_dates=["time_utc"])
        df["time_utc"] = pd.to_datetime(df["time_utc"], utc=False)
        df.sort_values("time_utc", inplace=True)
        df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
        df.reset_index(drop=True, inplace=True)
        for col, dtype in CSV_DTYPE.items():
            df[col] = df[col].astype(dtype)
        logger.info(f"[{canon}] CSV loaded: {len(df):,} bars  "
                    f"{df['time_utc'].iloc[0].date()} -> "
                    f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}")
        return df
    except Exception as e:
        logger.error(f"[{canon}] CSV load failed: {e}")
        return None


def _append_bar_to_csv(canon: str, bar_time: pd.Timestamp,
                        o: float, h: float, l: float, c: float) -> None:
    """Thread-safe append of one bar to CSV."""
    path = _csv_path(canon)
    lock = _csv_lock[canon]
    new_file = not os.path.isfile(path)
    with lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(CSV_COLUMNS)
            writer.writerow([bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                              f"{o:.5f}", f"{h:.5f}", f"{l:.5f}", f"{c:.5f}"])


def _fetch_broker_range(broker_sym: str,
                         from_dt: datetime.datetime,
                         to_dt:   datetime.datetime) -> pd.DataFrame:
    """Fetch M5 bars from broker between from_dt and to_dt (UTC).
    Excludes the currently-open bar via the M5 boundary check."""
    rates = mt5.copy_rates_range(broker_sym, mt5.TIMEFRAME_M5, from_dt, to_dt)
    if rates is None or len(rates) == 0:
        return pd.DataFrame(columns=CSV_COLUMNS)
    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[["time", "open", "high", "low", "close"]].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df.drop(columns=["time"], inplace=True)
    now_ts = pd.Timestamp(datetime.datetime.utcnow())
    df = df[df["time_utc"] + pd.Timedelta(seconds=M5_SECONDS) <= now_ts].copy()
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ==============================================================================
#  SECTION 4 — ATR CACHE STARTUP
#  Loads CSV + gap, computes full ATR array, seeds incremental state.
#  This is the ONLY large fetch — and it shrinks every run as the CSV grows.
# ==============================================================================

def _build_atr_df(canon: str, broker_sym: str) -> object:
    """
    Return a DataFrame with enough history to seed the ATR percentile cache.
    Strategy:
      1. Load CSV (may already have months of history).
      2. Gap-fill from broker (only bars after last CSV row).
      3. If no CSV, broker-fetch ATR_WARMUP_BARS + WARMUP_M5 + 50 bars.
    """
    csv_df = _load_csv(canon)

    if csv_df is not None and len(csv_df) > 0:
        gap_from = csv_df["time_utc"].iloc[-1].to_pydatetime() + datetime.timedelta(seconds=1)
        gap_df   = _fetch_broker_range(broker_sym, gap_from, datetime.datetime.utcnow())
        if len(gap_df) > 0:
            # Write gap bars to CSV so future startups need even less fetching
            for row in gap_df.itertuples(index=False):
                _append_bar_to_csv(canon,
                    pd.Timestamp(row.time_utc),
                    float(row.open), float(row.high),
                    float(row.low),  float(row.close))
            combined = pd.concat([csv_df, gap_df], ignore_index=True)
            combined.sort_values("time_utc", inplace=True)
            combined.drop_duplicates(subset="time_utc", keep="last", inplace=True)
            combined.reset_index(drop=True, inplace=True)
            logger.info(f"[{canon}] ATR df: {len(csv_df):,} CSV + "
                        f"{len(gap_df):,} gap = {len(combined):,} bars")
            return combined
        logger.info(f"[{canon}] ATR df: {len(csv_df):,} CSV bars (no gap)")
        return csv_df

    # No CSV: broker fetch — only as many bars as we need for ATR warmup
    need = ATR_WARMUP_BARS + WARMUP_M5 + 100
    logger.info(f"[{canon}] No CSV — broker fetch ({need:,} bars for ATR seed)")
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 0, need + 2)
    if rates is None or len(rates) < WARMUP_M5 + ATR_PERIOD + 10:
        logger.error(f"[{canon}] ATR seed fetch failed")
        return None
    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[["time", "open", "high", "low", "close"]].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df.drop(columns=["time"], inplace=True)
    now_ts = pd.Timestamp(datetime.datetime.utcnow())
    df = df[df["time_utc"] + pd.Timedelta(seconds=M5_SECONDS) <= now_ts].copy()
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    # Seed the CSV with what we fetched so next startup is cheap
    logger.info(f"[{canon}] Writing {len(df):,} fetched bars to CSV for future startups")
    for row in df.itertuples(index=False):
        _append_bar_to_csv(canon,
            pd.Timestamp(row.time_utc),
            float(row.open), float(row.high),
            float(row.low),  float(row.close))
    logger.info(f"[{canon}] ATR seed df: {len(df):,} bars  "
                f"{df['time_utc'].iloc[0].date()} -> "
                f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}")
    return df


def init_atr_cache(canon: str, broker_sym: str) -> bool:
    df = _build_atr_df(canon, broker_sym)
    if df is None or len(df) < WARMUP_M5 + ATR_PERIOD + 10:
        logger.error(f"[{canon}] Not enough bars for ATR cache init")
        return False

    h = df["high"].values.astype(np.float64)
    l = df["low"].values.astype(np.float64)
    c = df["close"].values.astype(np.float64)

    atr14 = _atr_wilder_full(h, l, c)

    # Build expanding percentile rank history
    hist: list[float] = []
    last_pct = float("nan")
    for i in range(WARMUP_M5, len(atr14)):
        v = atr14[i]
        if not np.isnan(v):
            last_pct, hist = _pct_rank_update(hist, v)

    last_valid = atr14[~np.isnan(atr14)]
    atr_prev   = float(last_valid[-1]) if len(last_valid) > 0 else 0.0

    _atr_cache[canon] = {
        "atr_prev":      atr_prev,
        "atr_pct_hist":  hist,
        "last_pct":      last_pct,
        "last_bar_time": pd.Timestamp(df["time_utc"].iloc[-1]),
        "n_bars":        len(df),
    }
    logger.info(f"  [{canon}] ATR cache ready: "
                f"atr={atr_prev:.2f}  pct={last_pct:.3f}  "
                f"hist_len={len(hist):,}  "
                f"last_bar={df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}")
    return True


def update_atr_cache(canon: str, bar_time: pd.Timestamp,
                      h: float, l: float, c: float) -> None:
    """Called once per new closed bar.  Updates ATR state and writes CSV."""
    ac = _atr_cache[canon]
    if bar_time <= ac["last_bar_time"]:
        return

    # We need prev_c for ATR true-range.  We don't store the full series,
    # so use the last close embedded in atr_prev computation via the cache.
    # We DO store prev_c explicitly after the first bar.
    prev_c = ac.get("prev_c", c)  # fallback: use same bar (slightly off, once)
    new_atr = _atr_wilder_step(ac["atr_prev"], h, l, prev_c)
    new_pct, hist = _pct_rank_update(ac["atr_pct_hist"], new_atr)

    ac["atr_prev"]     = new_atr
    ac["atr_pct_hist"] = hist
    ac["last_pct"]     = new_pct
    ac["last_bar_time"] = bar_time
    ac["prev_c"]       = c
    ac["n_bars"]      += 1

    # Write bar to CSV
    _append_bar_to_csv(canon, bar_time, h, l, l, c)   # note: o not stored in cache
    # correction: we need open too — caller should pass it; done below


# NOTE: update_atr_cache_full is the actual call used in the main loop.
def update_atr_cache_full(canon: str, bar_time: pd.Timestamp,
                           o: float, h: float, l: float, c: float) -> None:
    ac = _atr_cache[canon]
    if bar_time <= ac["last_bar_time"]:
        return
    prev_c  = ac.get("prev_c", c)
    new_atr = _atr_wilder_step(ac["atr_prev"], h, l, prev_c)
    new_pct, hist = _pct_rank_update(ac["atr_pct_hist"], new_atr)
    ac["atr_prev"]      = new_atr
    ac["atr_pct_hist"]  = hist
    ac["last_pct"]      = new_pct
    ac["last_bar_time"] = bar_time
    ac["prev_c"]        = c
    ac["n_bars"]       += 1
    _append_bar_to_csv(canon, bar_time, o, h, l, c)


# ==============================================================================
#  SECTION 5 — SIGNAL-ONLY DATA FETCH  (fresh every bar, like v2)
#
#  Pulls SIGNAL_BARS (~400) of M5 history from the broker.
#  This is small and fast.  Rebuilt from scratch each bar — no index drift.
#  Used ONLY for OR computation and close price.  NOT for ATR percentile.
# ==============================================================================

def fetch_m5_signal(broker_sym: str) -> object:
    """Fetch last SIGNAL_BARS closed M5 bars for OR + signal detection."""
    rates = mt5.copy_rates_from_pos(
        broker_sym, mt5.TIMEFRAME_M5, 0, SIGNAL_BARS + 2
    )
    if rates is None or len(rates) < 50:
        logger.warning(f"[{broker_sym}] signal fetch failed: "
                       f"{len(rates) if rates is not None else 0} bars")
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
    df.drop(columns=["time"], inplace=True)

    # Exclude the currently-open bar (M5 boundary check — exact, not sloppy)
    now_ts = pd.Timestamp(datetime.datetime.utcnow())
    df = df[df["time_utc"] + pd.Timedelta(seconds=M5_SECONDS) <= now_ts].copy()
    df.sort_values("time_utc", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ==============================================================================
#  SECTION 6 — OR COMPUTATION  (operates on signal df, not the ATR cache)
#
#  Identical logic to v2's compute_or — clean array built from fresh fetch.
#  Returns or_high[i], or_low[i] arrays parallel to df.
# ==============================================================================

def compute_or(df: pd.DataFrame, canon: str, or_bars: int):
    cfg = SESSION[canon]
    o   = df["open"].values.astype(np.float64)
    h   = df["high"].values.astype(np.float64)
    l   = df["low"].values.astype(np.float64)
    c   = df["close"].values.astype(np.float64)
    n   = len(df)

    utc_h = df["utc_hour"].values.astype(np.int32)
    utc_m = df["utc_minute"].values.astype(np.int32)
    dates = df["date"].values

    in_session = np.array([
        (utc_h[i] > cfg["open_h"] or
         (utc_h[i] == cfg["open_h"] and utc_m[i] >= cfg["open_m"]))
        and utc_h[i] < cfg["close_h"]
        for i in range(n)
    ])
    is_open_bar = (utc_h == cfg["open_h"]) & (utc_m == cfg["open_m"])

    # Find session-open bar index per day
    day_start: dict = {}
    for i in range(n):
        if is_open_bar[i]:
            d = dates[i]
            if d not in day_start:
                day_start[d] = i

    # Compute OR per day — only when full window fits
    day_or: dict = {}
    for d, si in day_start.items():
        ei = si + or_bars
        if ei > n:
            logger.debug(f"[{canon}] OR window for {d} incomplete "
                         f"(si={si} ei={ei} n={n}) — skipping")
            continue
        or_h = h[si:ei].max()
        or_l = l[si:ei].min()
        if or_h <= or_l:
            logger.error(f"[{canon}] OR INVALID {d}: high={or_h:.5f} <= low={or_l:.5f} "
                         f"— CHECK H/L COLUMN ORDER IN BROKER FEED")
            continue
        day_or[d] = (or_h, or_l)

    or_high = np.full(n, np.nan)
    or_low  = np.full(n, np.nan)
    for i in range(n):
        if not in_session[i]:
            continue
        d = dates[i]
        if d not in day_or or d not in day_start:
            continue
        if i < day_start[d] + or_bars:
            continue   # still inside the OR window itself
        or_high[i] = day_or[d][0]
        or_low[i]  = day_or[d][1]

    return or_high, or_low, in_session, h, l, c, utc_h, utc_m, dates


# ==============================================================================
#  SECTION 7 — SIGNAL DETECTION
# ==============================================================================

def detect_signal(canon: str, df: pd.DataFrame, params: dict,
                  bars_since_last: int, day_trades: int):
    """
    Returns (direction, or_high, or_low, atr_val, or_size, sl_dist)
    or     (None, None, None, None, None, None) if no signal.

    Uses fresh df for OR.  Uses _atr_cache for atr_val and atr_pct gate.
    Direction semantics (explicit, unambiguous):
      close > OR_HIGH  →  broke UP  →  "long"  →  BUY
      close < OR_LOW   →  broke DOWN →  "short" →  SELL
    """
    ac = _atr_cache.get(canon)
    if ac is None:
        logger.warning(f"[{canon}] SIGNAL_SKIP: ATR cache not initialised")
        return None, None, None, None, None, None

    n_atr = ac["n_bars"]
    if n_atr < WARMUP_M5:
        logger.debug(f"[{canon}] SIGNAL_SKIP warmup: {n_atr} < {WARMUP_M5}")
        return None, None, None, None, None, None

    atr_pct = ac["last_pct"]
    atr_val = ac["atr_prev"]
    if np.isnan(atr_pct) or atr_pct < ATR_PCT_THRESH:
        logger.info(f"[{canon}] SIGNAL_SKIP atr_pct: {atr_pct:.4f} < {ATR_PCT_THRESH} "
                    f"(atr={atr_val:.5f})")
        return None, None, None, None, None, None

    if bars_since_last < params["cooldown_bars"]:
        logger.info(f"[{canon}] SIGNAL_SKIP cooldown: "
                    f"{bars_since_last} < {params['cooldown_bars']}")
        return None, None, None, None, None, None

    if day_trades >= params["max_trades_day"]:
        logger.info(f"[{canon}] SIGNAL_SKIP max_trades: "
                    f"{day_trades} >= {params['max_trades_day']}")
        return None, None, None, None, None, None

    if np.isnan(atr_val) or atr_val <= 0:
        logger.info(f"[{canon}] SIGNAL_SKIP atr_invalid: {atr_val}")
        return None, None, None, None, None, None

    or_bars_n = OR_BARS[params["or_minutes"]]
    or_high_arr, or_low_arr, in_sess, h_arr, l_arr, c_arr, utc_h, utc_m, dates = \
        compute_or(df, canon, or_bars_n)

    i = len(df) - 1   # last closed bar

    cfg     = SESSION[canon]
    utc_h_i = int(utc_h[i])
    utc_m_i = int(utc_m[i])

    if not in_sess[i]:
        logger.debug(f"[{canon}] SIGNAL_SKIP session: "
                     f"{utc_h_i:02d}:{utc_m_i:02d} outside "
                     f"{cfg['open_h']:02d}:{cfg['open_m']:02d}-"
                     f"{cfg['close_h']:02d}:00")
        return None, None, None, None, None, None

    if np.isnan(or_high_arr[i]) or np.isnan(or_low_arr[i]):
        logger.info(f"[{canon}] SIGNAL_SKIP or_not_ready: "
                    f"{utc_h_i:02d}:{utc_m_i:02d} "
                    f"or_high={'NaN' if np.isnan(or_high_arr[i]) else f'{or_high_arr[i]:.5f}'} "
                    f"or_low={'NaN' if np.isnan(or_low_arr[i]) else f'{or_low_arr[i]:.5f}'} "
                    f"or_bars={or_bars_n}")
        return None, None, None, None, None, None

    or_h = or_high_arr[i]
    or_l = or_low_arr[i]
    c_cur = float(c_arr[i])
    o_cur = float(df["open"].values[i])
    body  = abs(c_cur - o_cur)

    # Breakout check — semantics printed explicitly for audit
    breaks_up   = c_cur > or_h
    breaks_down = c_cur < or_l

    if params["min_break_atr"] > 0:
        strong      = body >= params["min_break_atr"] * atr_val
        breaks_up   = breaks_up   and strong
        breaks_down = breaks_down and strong

    or_size = or_h - or_l
    sl_dist = max(params["sl_range_mult"] * or_size, atr_val * 0.05)

    if sl_dist < 3.0:
        logger.info(f"[{canon}] SIGNAL_SKIP sl_too_tight: {sl_dist:.5f} < 3.0")
        return None, None, None, None, None, None

    # Full evaluation log — always when in session
    logger.info(
        f"[{canon}] SIGNAL_EVAL "
        f"bar={utc_h_i:02d}:{utc_m_i:02d} "
        f"close={c_cur:.5f} OR_HIGH={or_h:.5f} OR_LOW={or_l:.5f} "
        f"body={body:.5f} atr={atr_val:.5f} atr_pct={atr_pct:.3f} "
        f"breaks_up={breaks_up} breaks_down={breaks_down}"
    )

    if breaks_up and not breaks_down:
        logger.info(f"[{canon}] SIGNAL_FIRE LONG (BUY)  "
                    f"close={c_cur:.5f} > OR_HIGH={or_h:.5f}  sl_dist={sl_dist:.5f}")
        return "long",  or_h, or_l, atr_val, or_size, sl_dist

    if breaks_down and not breaks_up:
        logger.info(f"[{canon}] SIGNAL_FIRE SHORT (SELL)  "
                    f"close={c_cur:.5f} < OR_LOW={or_l:.5f}  sl_dist={sl_dist:.5f}")
        return "short", or_h, or_l, atr_val, or_size, sl_dist

    logger.info(f"[{canon}] SIGNAL_SKIP no_breakout: "
                f"close={c_cur:.5f}  "
                f"dist_to_OR_HIGH={or_h - c_cur:+.5f}  "
                f"dist_to_OR_LOW={c_cur - or_l:+.5f}")
    return None, None, None, None, None, None


# ==============================================================================
#  SECTION 8 — POSITION SIZING + RISK GUARD
# ==============================================================================

def _daily_vol_cap(sl_dist: float, tvpl: float) -> float:
    global _MAX_TRADES_DAY_COMBO
    if sl_dist < 1e-9 or tvpl <= 0 or _MAX_TRADES_DAY_COMBO == 0:
        return 250.0
    per_trade = DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO
    info_list = list(_tick_info.values())
    if not info_list:
        return 250.0
    vol_step = info_list[0]["vol_step"]
    vol_min  = info_list[0]["vol_min"]
    raw = per_trade / (sl_dist * tvpl)
    return max(vol_min, round(raw / vol_step) * vol_step)


def compute_lot(broker_sym: str, sl_dist: float, balance: float):
    ti = _tick_info.get(broker_sym)
    if ti is None:
        logger.error(f"[{broker_sym}] not in tick_info cache")
        return None, None
    if sl_dist < 1e-9:
        logger.error(f"[{broker_sym}] sl_dist ~ 0")
        return None, None

    tvpl     = ti["tick_value_per_lot"]
    vol_min  = ti["vol_min"]
    vol_step = ti["vol_step"]
    vol_cap  = _daily_vol_cap(sl_dist, tvpl)

    risk_amount = balance * RISK_PER_TRADE
    raw_lot     = risk_amount / (sl_dist * tvpl)
    lot = max(vol_min, min(vol_cap, round(raw_lot / vol_step) * vol_step))
    lot = round(lot, 8)

    logger.info(f"[{broker_sym}] lot_calc: balance={balance:.2f} "
                f"risk={risk_amount:.2f} sl_dist={sl_dist:.5f} "
                f"tvpl={tvpl:.5f} raw={raw_lot:.4f} cap={vol_cap} -> lot={lot}")
    return lot, tvpl


def risk_guard(canon: str, lot: float, sl_dist: float,
               balance: float, tvpl: float) -> bool:
    intended = balance * RISK_PER_TRADE
    actual   = lot * sl_dist * tvpl
    multiple = actual / intended if intended > 0 else float("inf")
    status   = "OK" if multiple <= MAX_RISK_MULTIPLE else "REJECTED"
    logger.info(f"[{canon}] RISK_AUDIT [{status}] "
                f"intended={intended:.2f} actual={actual:.2f} "
                f"multiple={multiple:.2f}x lot={lot} sl_dist={sl_dist:.5f}")
    if multiple > MAX_RISK_MULTIPLE:
        logger.warning(f"[{canon}] TRADE REJECTED — "
                       f"actual risk {multiple:.2f}x intended (limit {MAX_RISK_MULTIPLE}x)")
        return False
    return True


# ==============================================================================
#  SECTION 9 — ORDER EXECUTION
# ==============================================================================

def send_market_order(broker_sym: str, direction: str, lot: float,
                      sl_price: float, comment: str):
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{broker_sym}] tick unavailable")
        return None, None

    price = tick.ask if direction == "long" else tick.bid
    otype = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL
    sl_price = clamp_sl(broker_sym, direction, price, sl_price)

    # Audit log before send
    logger.info(f"[{broker_sym}] ORDER_SEND "
                f"direction={direction} "
                f"order_type={'BUY' if otype == mt5.ORDER_TYPE_BUY else 'SELL'} "
                f"price={price:.5f} lot={lot} sl={sl_price:.5f}")

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
    logger.info(f"[{broker_sym}] ENTRY {direction.upper()} "
                f"lot={lot} price={price:.5f} sl={sl_price:.5f} ticket={result.order}")
    return result.order, price


def modify_sl(broker_sym: str, ticket: int, new_sl: float,
              direction: str, current_price: float) -> bool:
    new_sl = clamp_sl(broker_sym, direction, current_price, new_sl)
    req = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "symbol":   broker_sym,
        "position": ticket,
        "sl":       new_sl,
        "tp":       0.0,
    }
    result = mt5.order_send(req)
    ok = result is not None and result.retcode in (
        mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_NO_CHANGES
    )
    if not ok:
        logger.warning(f"[{broker_sym}] SL modify failed retcode="
                       f"{getattr(result,'retcode',None)} new_sl={new_sl:.5f}")
    return ok


def send_close(broker_sym: str, position) -> bool:
    otype = (mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY
             else mt5.ORDER_TYPE_BUY)
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
    ok = result is not None and result.retcode == mt5.TRADE_RETCODE_DONE
    if not ok:
        logger.error(f"[{broker_sym}] Close FAILED retcode={getattr(result,'retcode',None)}")
    else:
        logger.info(f"[{broker_sym}] CLOSED ticket={position.ticket} price={price:.5f}")
    return ok


# ==============================================================================
#  SECTION 10 — PER-SYMBOL STATE
# ==============================================================================

def make_sym_state() -> dict:
    return {
        "positions":        [],   # list of position records
        "bars_since_last":  9999,
        "day_trades_date":  None,
        "day_trades_count": 0,
    }


def _make_pos_rec(ticket, direction, ep, sl_dist, sl_price,
                  entry_date, entry_atr) -> dict:
    return {
        "ticket":      ticket,
        "direction":   direction,
        "entry_price": ep,
        "sl_dist":     sl_dist,
        "be_active":   False,
        "current_sl":  sl_price,
        "hold_count":  0,
        "entry_date":  entry_date,
        "entry_atr":   entry_atr,
    }


def _reset_daily(sym_st: dict, today) -> None:
    if sym_st["day_trades_date"] != today:
        sym_st["day_trades_date"]  = today
        sym_st["day_trades_count"] = 0


def _reconstruct_pos(canon: str, pos) -> dict:
    entry_time = datetime.datetime.fromtimestamp(pos.time, tz=datetime.timezone.utc)
    now_utc    = datetime.datetime.now(tz=datetime.timezone.utc)
    hold       = max(0, int((now_utc - entry_time).total_seconds() / M5_SECONDS))
    direction  = "long" if pos.type == mt5.ORDER_TYPE_BUY else "short"
    ep         = pos.price_open
    sl_price   = pos.sl or 0.0
    sl_dist    = abs(ep - sl_price) if sl_price > 0 else 0.01
    be_active  = (direction == "long"  and sl_price >= ep) or \
                 (direction == "short" and sl_price > 0 and sl_price <= ep)
    logger.info(f"[{canon}] RECOVERED ticket={pos.ticket}: "
                f"dir={direction} ep={ep:.5f} sl={sl_price:.5f} "
                f"hold~{hold}bars be={be_active}")
    rec = _make_pos_rec(pos.ticket, direction, ep, sl_dist, sl_price,
                        entry_time.date(), None)
    rec["be_active"]  = be_active
    rec["hold_count"] = hold
    return rec


def _log_close(canon: str, pr: dict) -> None:
    ticket = pr.get("ticket")
    if not ticket:
        return
    try:
        deals = mt5.history_deals_get(position=ticket)
        if deals and len(deals) >= 2:
            ep     = pr.get("entry_price", 0)
            sld    = pr.get("sl_dist", 1)
            cp     = deals[-1].price
            sign   = 1 if pr.get("direction") == "long" else -1
            r_val  = sign * (cp - ep) / sld if sld > 0 else 0.0
            logger.info(f"[{canon}] CLOSED ticket={ticket} price={cp:.5f} R={r_val:+.3f}")
        else:
            logger.info(f"[{canon}] CLOSED ticket={ticket} (history unavailable)")
    except Exception as e:
        logger.info(f"[{canon}] CLOSED ticket={ticket} ({e})")


# ==============================================================================
#  SECTION 11 — POSITION MANAGEMENT
# ==============================================================================

def _manage_pos(canon: str, broker_sym: str, pr: dict, params: dict,
                df: pd.DataFrame, broker_pos, today, sym_st: dict) -> None:
    """Manage one open position for one bar."""
    i         = len(df) - 1
    direction = pr["direction"]
    ep        = pr["entry_price"]
    sl_dist   = pr["sl_dist"]
    cfg       = SESSION[canon]

    # ATR from cache (not from signal df — same value used everywhere)
    atr_cur = _atr_cache[canon]["atr_prev"]

    bar_h = float(df["high"].values[i])
    bar_l = float(df["low"].values[i])

    pr["hold_count"] += 1
    hc = pr["hold_count"]

    # ── EOD exit ──────────────────────────────────────────────────────────────
    bar_date = df["date"].values[i]
    bar_hour = int(df["utc_hour"].values[i])
    if pr["entry_date"] != bar_date or bar_hour >= cfg["close_h"]:
        logger.info(f"[{canon}] EOD exit ticket={pr['ticket']} "
                    f"(entry={pr['entry_date']} bar={bar_date} "
                    f"bar_h={bar_hour} close_h={cfg['close_h']})")
        if broker_pos:
            send_close(broker_sym, broker_pos)
        _log_close(canon, pr)
        sym_st["positions"].remove(pr)
        return

    # ── Max hold ──────────────────────────────────────────────────────────────
    if hc >= MAX_HOLD:
        logger.info(f"[{canon}] MAX HOLD ticket={pr['ticket']}")
        if broker_pos:
            send_close(broker_sym, broker_pos)
        _log_close(canon, pr)
        sym_st["positions"].remove(pr)
        return

    # ── Break-even trigger at 1R ──────────────────────────────────────────────
    one_r = ep + (sl_dist if direction == "long" else -sl_dist)
    if not pr["be_active"]:
        triggered = ((direction == "long"  and bar_h >= one_r) or
                     (direction == "short" and bar_l <= one_r))
        if triggered:
            pr["be_active"]  = True
            pr["current_sl"] = ep
            logger.info(f"[{canon}] BE ticket={pr['ticket']} SL -> {ep:.5f}")

    # ── Trailing stop ─────────────────────────────────────────────────────────
    if pr["be_active"]:
        ta = atr_cur if (not np.isnan(atr_cur) and atr_cur > 0) \
             else (pr["entry_atr"] or sl_dist)
        mult = params["trail_atr_mult"]
        if direction == "long":
            pr["current_sl"] = max(pr["current_sl"], bar_h - mult * ta)
        else:
            pr["current_sl"] = min(pr["current_sl"], bar_l + mult * ta)

    # ── SL modify ─────────────────────────────────────────────────────────────
    if broker_pos is not None:
        broker_sl = broker_pos.sl or 0.0
        new_sl    = pr["current_sl"]
        pip       = _tick_info.get(broker_sym, {}).get("pip", 0.0001)
        if abs(new_sl - broker_sl) >= pip:
            tick = mt5.symbol_info_tick(broker_sym)
            if tick is None:
                logger.warning(f"[{canon}] SL modify skipped — tick unavailable")
                return
            cur_px = tick.bid if direction == "long" else tick.ask
            if modify_sl(broker_sym, pr["ticket"], new_sl, direction, cur_px):
                logger.info(f"[{canon}] SL updated ticket={pr['ticket']} "
                            f"{broker_sl:.5f} -> {new_sl:.5f} "
                            f"(hold={hc} be={pr['be_active']})")


# ==============================================================================
#  SECTION 12 — ENTRY EXECUTION
# ==============================================================================

def _execute_entry(canon: str, broker_sym: str, sym_st: dict, params: dict,
                   balance: float, today, direction: str,
                   sl_dist: float, atr_val: float) -> None:
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{canon}] tick unavailable — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    ep = tick.ask if direction == "long" else tick.bid

    min_sl = 0.05 * atr_val
    sl_dist = max(sl_dist, min_sl)
    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist

    # Edge-case guard
    if direction == "long"  and sl_price >= ep:
        sl_price = ep - min_sl; sl_dist = min_sl
    if direction == "short" and sl_price <= ep:
        sl_price = ep + min_sl; sl_dist = min_sl

    # Clamp to stops level before lot calc (stops level can widen sl_dist)
    sl_clamped = clamp_sl(broker_sym, direction, ep, sl_price)
    if sl_clamped != sl_price:
        sl_dist  = abs(ep - sl_clamped)
        sl_price = sl_clamped
        logger.info(f"[{canon}] entry SL clamped; sl_dist adjusted to {sl_dist:.5f}")

    lot, tvpl = compute_lot(broker_sym, sl_dist, balance)
    if lot is None:
        logger.error(f"[{canon}] lot calc failed — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    if not risk_guard(canon, lot, sl_dist, balance, tvpl):
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    ticket, _ = send_market_order(broker_sym, direction, lot, sl_price,
                                  f"{COMMENT}_{canon}")
    if ticket is None:
        logger.error(f"[{canon}] order failed — entry cancelled")
        sym_st["day_trades_count"] = max(0, sym_st["day_trades_count"] - 1)
        sym_st["bars_since_last"]  = params["cooldown_bars"]
        return

    # Confirm fill
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
        sl_dist   = abs(actual_ep - actual_sl) or min_sl
    else:
        actual_ep = ep
        actual_sl = sl_price

    rec = _make_pos_rec(ticket, direction, actual_ep, sl_dist,
                        actual_sl, today, atr_val)
    sym_st["positions"].append(rec)
    logger.info(f"[{canon}] ENTERED {direction.upper()} ticket={ticket} "
                f"ep={actual_ep:.5f} sl={actual_sl:.5f} "
                f"sl_dist={sl_dist:.5f} lot={lot} "
                f"open_positions={len(sym_st['positions'])}")


# ==============================================================================
#  SECTION 13 — PER-BAR PROCESSING
# ==============================================================================

def process_symbol(canon: str, broker_sym: str, sym_st: dict,
                   params: dict, balance: float) -> None:
    """
    Called once per closed M5 bar.

    Step 1: fetch fresh signal df (SIGNAL_BARS bars) — for OR + signal.
    Step 2: update ATR cache with new bar.
    Step 3: reconcile broker positions.
    Step 4: manage open positions (EOD, trail, BE).
    Step 5: detect signal and execute entry if triggered.
    """
    df = fetch_m5_signal(broker_sym)
    if df is None or len(df) < 10:
        logger.warning(f"[{canon}] signal fetch failed — skipping bar")
        return

    # today from bar timestamp (not wall clock) — matches backtest
    today = df["date"].values[-1]

    # Update ATR cache with the new bar
    last_bar_ts = pd.Timestamp(df["time_utc"].values[-1])
    last_bar    = df.iloc[-1]
    update_atr_cache_full(
        canon, last_bar_ts,
        float(last_bar["open"]), float(last_bar["high"]),
        float(last_bar["low"]),  float(last_bar["close"])
    )

    _reset_daily(sym_st, today)
    sym_st["bars_since_last"] += 1

    # Reconcile broker vs local state
    broker_positions = mt5.positions_get(symbol=broker_sym) or []
    broker_positions = [p for p in broker_positions if p.magic == MAGIC]
    broker_tickets   = {p.ticket for p in broker_positions}
    known_tickets    = {pr["ticket"] for pr in sym_st["positions"]}

    # Recover untracked positions
    for bp in broker_positions:
        if bp.ticket not in known_tickets:
            logger.warning(f"[{canon}] Desync: ticket={bp.ticket} — recovering")
            rec = _reconstruct_pos(canon, bp)
            sym_st["positions"].append(rec)
            sym_st["day_trades_count"] = min(
                sym_st["day_trades_count"] + 1, params["max_trades_day"]
            )

    # Detect server-side closes (SL hit)
    for pr in list(sym_st["positions"]):
        if pr["ticket"] not in broker_tickets:
            logger.info(f"[{canon}] ticket={pr['ticket']} closed server-side")
            _log_close(canon, pr)
            sym_st["positions"].remove(pr)

    # Manage open positions
    broker_map = {p.ticket: p for p in broker_positions}
    for pr in list(sym_st["positions"]):
        bp = broker_map.get(pr["ticket"])
        _manage_pos(canon, broker_sym, pr, params, df, bp, today, sym_st)

    # Signal detection + entry
    direction, or_h, or_l, atr_val, or_size, sl_dist = detect_signal(
        canon, df, params,
        sym_st["bars_since_last"],
        sym_st["day_trades_count"],
    )
    if direction is None:
        return

    logger.info(f"[{canon}] SIGNAL {direction.upper()} "
                f"or_high={or_h:.5f} or_low={or_l:.5f} "
                f"or_size={or_size:.5f} sl_dist={sl_dist:.5f} "
                f"atr={atr_val:.5f} "
                f"(day_trades={sym_st['day_trades_count']+1}/"
                f"{params['max_trades_day']} "
                f"bars_since_last={sym_st['bars_since_last']})")

    sym_st["bars_since_last"]  = 0
    sym_st["day_trades_count"] += 1

    _execute_entry(canon, broker_sym, sym_st, params,
                   balance, today, direction, sl_dist, atr_val)


# ==============================================================================
#  SECTION 14 — BAR CLOCK
# ==============================================================================

_CLOCK_BROKER= None
M5_SECONDS_TD = datetime.timedelta(seconds=M5_SECONDS)


def _next_m5_boundary() -> datetime.datetime:
    now  = datetime.datetime.utcnow()
    secs = now.hour * 3600 + now.minute * 60 + now.second
    rem  = secs % M5_SECONDS
    wait = M5_SECONDS - rem if rem > 0 else M5_SECONDS
    return now + datetime.timedelta(seconds=wait)


def _broker_last_bar() -> object:
    if _CLOCK_BROKER is None:
        return None
    rates = mt5.copy_rates_from_pos(_CLOCK_BROKER, mt5.TIMEFRAME_M5, 0, 2)
    if rates is not None and len(rates) >= 2:
        return pd.Timestamp(rates[0]["time"], unit="s")
    return None


def wait_for_new_bar(last_bar_time: pd.Timestamp) -> object:
    while True:
        boundary  = _next_m5_boundary()
        sleep_sec = (boundary - datetime.datetime.utcnow()).total_seconds() - 1.0
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            t = _broker_last_bar()
            if t is not None and t > last_bar_time:
                return t
            time.sleep(0.2)
        logger.debug(f"[clock] no new bar after {boundary.strftime('%H:%M')} UTC — retry")


# ==============================================================================
#  SECTION 15 — METRICS
# ==============================================================================

class Metrics:
    def __init__(self, syms):
        self.stats  = {s: {"trades": 0, "wins": 0, "total_r": 0.0} for s in syms}
        self.peak   = None
        self.max_dd = 0.0
        self.last_h = None

    def check_hourly(self, balance: float) -> None:
        if self.peak is None or balance > self.peak:
            self.peak = balance
        if self.peak and self.peak > 0:
            self.max_dd = max(self.max_dd, (self.peak - balance) / self.peak)
        h = datetime.datetime.now(datetime.timezone.utc).hour
        if self.last_h is None:
            self.last_h = h
        if h != self.last_h:
            self.last_h = h
            self._report(balance)

    def _report(self, balance: float) -> None:
        tot_t = sum(d["trades"] for d in self.stats.values())
        tot_w = sum(d["wins"]   for d in self.stats.values())
        tot_r = sum(d["total_r"] for d in self.stats.values())
        wr  = tot_w / tot_t if tot_t else 0.0
        exp = tot_r / tot_t if tot_t else 0.0
        logger.info(
            f"\n{'='*70}\n[HOURLY REPORT — ORB V5]\n"
            f"  Params:    {ACTIVE_PARAMS}\n"
            f"  Trades:    {tot_t}  WR: {wr:.1%}  E: {exp:+.3f}R  "
            f"TotalR: {tot_r:+.1f}\n"
            f"  Max DD:    {self.max_dd:.1%}\n"
            f"  Equity:    {balance:,.2f}\n"
            f"  Budget:    ${DAILY_LOSS_BUDGET:.2f} ({DAILY_LOSS_CAP_PCT:.2%})\n"
            + "=" * 70
        )
        for canon, d in sorted(self.stats.items(), key=lambda x: -x[1]["total_r"]):
            if d["trades"] > 0:
                logger.info(f"  {canon:<8} n={d['trades']:>4} "
                            f"WR={d['wins']/d['trades']:.1%} "
                            f"E={d['total_r']/d['trades']:+.3f}R "
                            f"totalR={d['total_r']:+.1f}")


# ==============================================================================
#  SECTION 16 — MAIN LOOP
# ==============================================================================

def _process_safe(canon, broker, sym_st, params, balance):
    try:
        process_symbol(canon, broker, sym_st, params, balance)
    except Exception as e:
        logger.exception(f"[{canon}] process_symbol error: {e}")


def run_live():
    global _CLOCK_BROKER, _MAX_TRADES_DAY_COMBO
    print("SCRIPT STARTED", flush=True)
    print("ORB V5 starting...", flush=True)
    sys.stdout.flush()

    # MT5 init
    print("Waiting for MT5 terminal to be ready...", flush=True)
    for attempt in range(30):
        ok  = mt5.initialize()
        err = mt5.last_error()
        print(f"  attempt {attempt+1}/30: init={ok} error={err}", flush=True)
        if ok:
            break
        time.sleep(3)
    else:
        print("ERROR: MT5 terminal did not respond after 90s", flush=True)
        raise RuntimeError("MT5 did not respond after 90s")

    print("MT5 initialized — logging in...", flush=True)
    auth = mt5.login(LOGIN, PASSWORD, SERVER)
    print(f"Login result: {auth}  error={mt5.last_error()}", flush=True)
    print(f"account_info: {mt5.account_info()}", flush=True)
    if not auth:
        mt5.shutdown()
        raise RuntimeError(f"Login failed: {mt5.last_error()}")

    acct = mt5.account_info()
    logger.info(f"MT5 connected | account={acct.login} | "
                f"balance={acct.balance:.2f} | currency={acct.currency}")
    logger.info(f"Engine: ORB V5 | Magic: {MAGIC} | Comment: {COMMENT} | "
                f"Params: {ACTIVE_PARAMS}")
    logger.info(f"Starting balance: ${STARTING_BALANCE:,.0f}  "
                f"Daily budget: ${DAILY_LOSS_BUDGET:.2f} ({DAILY_LOSS_CAP_PCT:.2%})")

    # Symbol map
    print("Building symbol map...", flush=True)
    logger.info("=== SYMBOL DIAGNOSTIC ===")
    sym_map, active = build_symbol_map()
    print(f"Active symbols: {active}", flush=True)
    if not active:
        logger.error("No symbols — shutting down")
        mt5.shutdown()
        return

    for canon in active:
        broker = sym_map[canon]
        print(f"  Getting symbol_info for {canon} ({broker})...", flush=True)
        info   = mt5.symbol_info(broker)
        if info is None:
            print(f"  ERROR: symbol_info None for {canon}", flush=True)
            logger.error(f"  {canon}: symbol_info None")
            continue
        tvpl = info.trade_tick_value / info.trade_tick_size if info.trade_tick_size > 0 else 1.0
        pip  = 10 ** (-info.digits + 1)
        _tick_info[broker] = {
            "tick_value_per_lot": tvpl,
            "vol_min":  max(info.volume_min,  0.01),
            "vol_max":  max(info.volume_max,  100.0),
            "vol_step": max(info.volume_step, 0.01),
            "pip":      pip,
            "point":    info.point if info.point > 0 else pip,
        }
        stops_lvl  = int(info.trade_stops_level or 0)
        point      = info.point if info.point > 0 else pip
        min_sl_pts = stops_lvl * point
        tick   = mt5.symbol_info_tick(broker)
        tvpl_v = (info.trade_tick_value / info.trade_tick_size
                  if info.trade_tick_size > 0 else "N/A")
        logger.info(
            f"  {canon} ({broker}): digits={info.digits} "
            f"tick_val/lot={tvpl_v} "
            f"vol_min={info.volume_min} vol_step={info.volume_step} "
            f"pip={pip} "
            f"stops_level={stops_lvl}pts ({min_sl_pts:.5f} price units) "
            f"spread={tick.ask - tick.bid if tick else 'N/A'} "
            f"params={BEST_PARAMS[canon]}"
        )
        print(f"  {canon}: digits={info.digits} tvpl={tvpl_v} "
              f"vol_min={info.volume_min} stops_level={stops_lvl} "
              f"params={BEST_PARAMS[canon]}", flush=True)

    _MAX_TRADES_DAY_COMBO = sum(BEST_PARAMS[s]["max_trades_day"] for s in active)
    per_trade = DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO if _MAX_TRADES_DAY_COMBO else 0
    logger.info(f"  max_trades_combo={_MAX_TRADES_DAY_COMBO}  "
                f"per_trade_budget=${per_trade:.2f}")
    logger.info(f"  Risk guard: MAX_RISK_MULTIPLE={MAX_RISK_MULTIPLE}x")
    logger.info("=== END DIAGNOSTIC ===")
    print("=== END DIAGNOSTIC ===", flush=True)

    # Init CSV locks
    print("Initialising CSV locks...", flush=True)
    for canon in active:
        _csv_lock[canon] = threading.Lock()

    # ATR cache startup (CSV + gap or small broker fetch)
    print("=== ATR CACHE STARTUP ===", flush=True)
    logger.info("=== ATR CACHE STARTUP ===")
    for canon in list(active):
        broker = sym_map[canon]
        print(f"  [{canon}] init_atr_cache...", flush=True)
        ok = init_atr_cache(canon, broker)
        print(f"  [{canon}] init_atr_cache result: {ok}", flush=True)
        if not ok:
            logger.error(f"  {canon}: ATR cache init failed — removing")
            active.remove(canon)
    if not active:
        logger.error("No symbols with ATR cache — shutting down")
        mt5.shutdown()
        return
    print("=== END ATR CACHE STARTUP ===", flush=True)
    logger.info("=== END ATR CACHE STARTUP ===")

    _CLOCK_BROKER = sym_map[active[0]]
    sym_states    = {c: make_sym_state() for c in active}

    # Startup recovery
    logger.info("=== STARTUP RECOVERY ===")
    for canon in active:
        broker = sym_map[canon]
        positions = [p for p in (mt5.positions_get(symbol=broker) or [])
                     if p.magic == MAGIC]
        if positions:
            for pos in positions:
                if COMMENT in (pos.comment or ""):
                    rec = _reconstruct_pos(canon, pos)
                    sym_states[canon]["positions"].append(rec)
                    sym_states[canon]["day_trades_count"] = min(
                        sym_states[canon]["day_trades_count"] + 1,
                        BEST_PARAMS[canon]["max_trades_day"]
                    )
                else:
                    logger.warning(f"  {canon}: ticket={pos.ticket} "
                                   f"comment='{pos.comment}' — not ours, skipping")
            logger.info(f"  {canon}: recovered {len(sym_states[canon]['positions'])} pos")
        else:
            logger.info(f"  {canon}: no open positions")
    logger.info("=== END RECOVERY ===")

    logger.info("=== ACTIVE PARAMS ===")
    for canon in active:
        logger.info(f"  {canon}: {BEST_PARAMS[canon]}")
    logger.info(f"  RISK={RISK_PER_TRADE:.2%}  MAX_HOLD={MAX_HOLD}bars  "
                f"ATR_PCT_THRESH={ATR_PCT_THRESH}  WARMUP_M5={WARMUP_M5}  "
                f"SIGNAL_BARS={SIGNAL_BARS}  ATR_WARMUP_BARS={ATR_WARMUP_BARS}")
    logger.info("=== END PARAMS ===")

    metrics   = Metrics(active)
    bar_count = 0

    last_bar_time = _broker_last_bar()
    if last_bar_time is None:
        last_bar_time = pd.Timestamp.utcnow()
    logger.info(f"Seeded bar time: {last_bar_time} UTC — waiting for next M5 close...")

    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1

            wall_today = datetime.datetime.utcnow().date()
            logger.info(f"-- BAR {bar_count} | {new_bar_time} UTC "
                        f"(wall={wall_today}) "
                        f"----------------------------------------")

            balance = mt5.account_info().balance

            threads = [
                threading.Thread(
                    target=_process_safe,
                    args=(canon, sym_map[canon], sym_states[canon],
                          BEST_PARAMS[canon], balance),
                    daemon=True,
                )
                for canon in active
            ]
            for t in threads: t.start()
            for t in threads: t.join(timeout=25)

            metrics.check_hourly(balance)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down ORB V5")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(60)

    mt5.shutdown()
    logger.info("MT5 disconnected. ORB V5 stopped.")


if __name__ == "__main__":
    run_live()
