"""
==============================================================================
ORB  —  LIVE ENGINE v6
==============================================================================
CORE PRINCIPLE:
  This is a direct port of the backtest (orb_chrono_bt.py).
  OR computation, signal detection, ATR, and all indicator logic are
  IDENTICAL to build_cache_and_signals() + resolve_trade() in the BT.
  No rolling per-bar fetches. No index drift. No recomputing OR live.

HOW IT WORKS:
  STARTUP:
    1. Normalize CSV to engine canonical format if it is an MT5 export
       (tab-separated, <DATE>/<TIME> columns, YYYY.MM.DD dates).
    2. Load CSV history (same _load_csv as v5).
    3. Gap-fill from broker (bars after last CSV row).
    4. Run build_cache_and_signals() over the full array — same as BT.
    5. Cache is now a complete pre-computed state: or_high, or_low,
       atr14, atr_pct, signal arrays — all indexed by bar position.

  EACH NEW BAR:
    1. Fetch the single new closed bar from broker.
    2. Append it to the in-memory arrays.
    3. Recompute only the last element of each indicator (O(1) Wilder step).
    4. Check signal on the last bar index — same logic as BT signal[i].
    5. If signal: send market order immediately.
    6. Manage open positions: BE, trail, EOD — same as resolve_trade() forward sim.

  SL MODIFICATION:
    Retry loop on the SAME bar until success or timeout.
    Re-fetches current bid/ask before each attempt.

CSV:
  Engine canonical format: comma-separated, header = time_utc,open,high,low,close,
  ISO datetime (YYYY-MM-DD HH:MM:SS).
  MT5-exported files are normalised to canonical format once on startup.
  Path: <script_dir>/<CANON>.cash.csv

WHAT IS GONE vs v5:
  - fetch_m5_signal()          — replaced by single-bar append
  - compute_or() per bar       — replaced by pre-computed or_high/or_low arrays
  - _atr_cache incremental     — replaced by full Wilder array + single step
  - detect_signal()            — replaced by direct array lookup (same as BT)
  - All per-bar rolling fetches

LOG:    orb_live_v6.log
MAGIC:  202603264
==============================================================================
"""

import os, sys, io, time, logging, datetime, bisect, threading
import numpy as np
import pandas as pd
from logging.handlers import RotatingFileHandler

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    print("ERROR: MetaTrader5 not installed.")
    sys.exit(1)

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("ORB_V6")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler(
    "orb_live_v6.log", maxBytes=15_000_000, backupCount=5, encoding="utf-8"
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
MAGIC   = 202603264
COMMENT = "ORB_V6"

# ── Broker / risk constants ───────────────────────────────────────────────────
STARTING_BALANCE   = 25_000.0
RISK_PER_TRADE     = 0.0085
MAX_RISK_MULTIPLE  = 2.0
DAILY_LOSS_CAP_PCT = 0.0475
DAILY_LOSS_BUDGET  = STARTING_BALANCE * DAILY_LOSS_CAP_PCT

VOL_MIN  = 0.10
VOL_STEP = 0.01
VOL_MAX  = 250.0

# ── Strategy constants ────────────────────────────────────────────────────────
WARMUP_M5      = 200
ATR_PERIOD     = 14
ATR_PCT_THRESH = 0.30
MAX_HOLD       = 48
M5_SECONDS     = 300

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

# ── GRID params (matched to BT) ───────────────────────────────────────────────
BEST_PARAMS = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "min_break_atr": 0.0, "max_trades_day": 1, "cooldown_bars": 3},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.5,
              "min_break_atr": 0.0, "max_trades_day": 2, "cooldown_bars": 3},
}

SYMBOL_ALIASES = {
    "US30":  ["US30.cash", "US30",  "DJ30",  "DJIA",  "WS30", "DOW30", "US30Cash"],
    "GER40": ["GER40.cash","GER40", "DAX40", "DAX",   "GER30","DE40",  "GER40Cash"],
}
SYMBOLS = list(SESSION.keys())

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Global symbol info cache ──────────────────────────────────────────────────
_tick_info: dict = {}   # broker_sym -> {tvpl, vol_min, vol_step, pip, point}
_MAX_TRADES_DAY_COMBO = 0


# ==============================================================================
#  SECTION 1 — CSV PERSISTENCE
# ==============================================================================

# Canonical format written and appended by this engine:
#   - comma-separated
#   - header line: time_utc,open,high,low,close
#   - datetime column: YYYY-MM-DD HH:MM:SS  (ISO, space-separated date/time)
#
# MT5-exported files use a different layout:
#   - tab-separated
#   - header: <DATE>\t<TIME>\t<OPEN>\t<HIGH>\t<LOW>\t<CLOSE>\t...
#   - date column: YYYY.MM.DD  (dot-separated)
#
# On startup, _normalize_csv_if_needed() detects MT5 format and rewrites
# the file in canonical format so all subsequent appends are consistent.

CSV_COLUMNS = ["time_utc", "open", "high", "low", "close"]
CSV_DTYPE   = {"open": np.float64, "high": np.float64,
               "low":  np.float64, "close": np.float64}


def _csv_path(canon: str) -> str:
    for d in (SCRIPT_DIR, os.getcwd()):
        p = os.path.join(d, f"{canon}.cash.csv")
        if os.path.isfile(p):
            return p
    return os.path.join(SCRIPT_DIR, f"{canon}.cash.csv")


def _load_csv(canon: str):
    """
    Load and parse the CSV for canon.  Handles both the engine's own
    canonical format and raw MT5 exports (tab-sep, dot-date, extra cols).
    Returns a clean DataFrame[time_utc, open, high, low, close] sorted
    ascending, or None on failure / missing file.
    """
    path = _csv_path(canon)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()

        sep = "\t" if "\t" in first_line else ","
        df  = pd.read_csv(path, sep=sep, engine="python")

        # ── Normalise column names ─────────────────────────────────────────
        def _clean_col(c):
            c = c.strip().lower().replace("<", "").replace(">", "")
            # MT5 prefixes: tdate -> date, topen -> open, etc.
            if c.startswith("t") and c[1:] in (
                "date","time","open","high","low","close",
                "tickvol","vol","spread"
            ):
                c = c[1:]
            return c

        df.columns = [_clean_col(c) for c in df.columns]

        # ── Build time_utc column ──────────────────────────────────────────
        if "date" in df.columns and "time" in df.columns:
            # MT5 export: separate date + time columns, date may use dots
            combined = (df["date"].astype(str).str.strip()
                        + " "
                        + df["time"].astype(str).str.strip())
            fixed = combined.str.replace(
                r"(\d{4})\.(\d{2})\.(\d{2})",
                lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}",
                regex=True,
            )
            df["time_utc"] = pd.to_datetime(fixed)
        elif "time_utc" in df.columns:
            df["time_utc"] = pd.to_datetime(df["time_utc"].astype(str))
        elif "time" in df.columns:
            if pd.api.types.is_numeric_dtype(df["time"]):
                df["time_utc"] = pd.to_datetime(
                    df["time"].astype(np.int64), unit="s"
                )
            else:
                sample = str(df["time"].iloc[0]).strip()
                if "." in sample.split(" ")[0]:
                    fixed = df["time"].astype(str).str.replace(
                        r"(\d{4})\.(\d{2})\.(\d{2})",
                        lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}",
                        regex=True,
                    )
                    df["time_utc"] = pd.to_datetime(fixed)
                else:
                    df["time_utc"] = pd.to_datetime(df["time"].astype(str))
        else:
            raise ValueError(f"No usable time column. Found: {list(df.columns)}")

        df = df[["time_utc","open","high","low","close"]].copy()
        for col, dtype in CSV_DTYPE.items():
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
        df.dropna(inplace=True)
        df.sort_values("time_utc", inplace=True)
        df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
        df.reset_index(drop=True, inplace=True)
        logger.info(
            f"[{canon}] CSV: {len(df):,} bars  "
            f"{df['time_utc'].iloc[0].date()} -> "
            f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}"
        )
        return df
    except Exception as e:
        logger.error(f"[{canon}] CSV load failed: {e}", exc_info=True)
        return None


def _normalize_csv_if_needed(canon: str) -> None:
    """
    One-time idempotent conversion: if the CSV on disk is an MT5 export
    (detected by a header that does NOT start with 'time_utc'), parse it
    with _load_csv() and rewrite it in canonical engine format.

    After this call the file is guaranteed to be comma-separated with a
    'time_utc,open,high,low,close' header and ISO datetime values, so
    _append_csv() rows always match the existing content.
    """
    path = _csv_path(canon)
    if not os.path.isfile(path):
        return  # nothing to normalise yet

    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()
    except Exception as e:
        logger.warning(f"[{canon}] Could not read CSV header for normalisation: {e}")
        return

    # Already canonical — nothing to do
    if first_line.strip().lower().startswith("time_utc"):
        return

    logger.info(f"[{canon}] MT5 CSV detected — normalising to engine format ...")
    df = _load_csv(canon)
    if df is None or len(df) == 0:
        logger.warning(
            f"[{canon}] Normalisation: _load_csv returned empty — leaving file as-is"
        )
        return

    try:
        tmp_path = path + ".normalising"
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            f.write("time_utc,open,high,low,close\n")
            for _, row in df.iterrows():
                f.write(
                    f"{row['time_utc'].strftime('%Y-%m-%d %H:%M:%S')},"
                    f"{row['open']},{row['high']},{row['low']},{row['close']}\n"
                )
        # Atomic replace
        if os.path.isfile(path):
            os.replace(tmp_path, path)
        else:
            os.rename(tmp_path, path)
        logger.info(
            f"[{canon}] CSV normalised: {len(df):,} bars written to {path}"
        )
    except Exception as e:
        logger.error(f"[{canon}] CSV normalisation failed: {e}", exc_info=True)
        # Leave original file intact — live engine can still run from broker data
        if os.path.isfile(path + ".normalising"):
            try:
                os.remove(path + ".normalising")
            except Exception:
                pass


def _append_csv(canon: str, bar_time: pd.Timestamp,
                o: float, h: float, l: float, c: float) -> None:
    """
    Append one closed bar to the canonical CSV.
    File is guaranteed to be in canonical format by the time this is called
    (_normalize_csv_if_needed runs during load_history on startup).
    Creates the file with a header if it does not yet exist.
    """
    path = _csv_path(canon)
    row  = (
        f"{bar_time.strftime('%Y-%m-%d %H:%M:%S')},"
        f"{o},{h},{l},{c}\n"
    )
    if not os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("time_utc,open,high,low,close\n")
            f.write(row)
    else:
        with open(path, "a", encoding="utf-8") as f:
            f.write(row)


# ==============================================================================
#  SECTION 2 — SYMBOL RESOLVER
# ==============================================================================

def resolve_symbol(canonical: str):
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
#  SECTION 3 — INDICATORS  (identical to BT)
# ==============================================================================

def _atr_wilder_full(h, l, c):
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
        out[i] = out[i-1] * (1.0 - k) + tr[i] * k
    return out


def _expanding_pct_rank(arr):
    """Identical to BT expanding_pct_rank."""
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
#  SECTION 4 — BUILD CACHE  (identical to BT build_cache_and_signals)
# ==============================================================================

def build_cache(canon: str, df: pd.DataFrame, params: dict) -> dict:
    """
    Direct port of BT build_cache_and_signals().
    OR computation is fully vectorised over the complete history.
    Signal array is pre-computed — live engine just checks cache["signal"][-1].
    """
    cfg = SESSION[canon]

    o = df["open"].values.astype(np.float64)
    h = df["high"].values.astype(np.float64)
    l = df["low"].values.astype(np.float64)
    c = df["close"].values.astype(np.float64)
    n = len(c)

    times   = df["time_utc"].values                          # datetime64[ns]
    utc_h   = df["time_utc"].dt.hour.values.astype(np.int32)
    utc_m   = df["time_utc"].dt.minute.values.astype(np.int32)
    dates   = df["time_utc"].dt.date.values

    atr14   = _atr_wilder_full(h, l, c)
    atr_pct = _expanding_pct_rank(atr14)

    # ── in_session ────────────────────────────────────────────────────────────
    in_session = np.array([
        (utc_h[i] > cfg["open_h"] or
         (utc_h[i] == cfg["open_h"] and utc_m[i] >= cfg["open_m"]))
        and utc_h[i] < cfg["close_h"]
        for i in range(n)
    ])
    is_open_bar = (utc_h == cfg["open_h"]) & (utc_m == cfg["open_m"])

    # ── Opening Range — IDENTICAL to BT ──────────────────────────────────────
    or_bars_n = OR_BARS[params["or_minutes"]]
    day_start = {}
    for i in range(n):
        if is_open_bar[i]:
            d = dates[i]
            if d not in day_start:
                day_start[d] = i

    day_or = {}
    for d, si in day_start.items():
        ei = si + or_bars_n
        if ei <= n:
            day_or[d] = (h[si:ei].max(), l[si:ei].min())

    or_high = np.full(n, np.nan)
    or_low  = np.full(n, np.nan)
    for i in range(n):
        if not in_session[i]:
            continue
        d = dates[i]
        if d not in day_or or d not in day_start:
            continue
        if i < day_start[d] + or_bars_n:
            continue
        or_high[i], or_low[i] = day_or[d]

    # ── Signal detection — IDENTICAL to BT ───────────────────────────────────
    cooldown      = params["cooldown_bars"]
    max_t         = params["max_trades_day"]
    min_break_atr = params["min_break_atr"]

    atr_ok = (~np.isnan(atr_pct)) & (atr_pct >= ATR_PCT_THRESH)
    valid  = np.zeros(n, dtype=bool)
    valid[WARMUP_M5:n-1] = True
    base = in_session & atr_ok & valid & ~np.isnan(or_high)
    body = np.abs(c - o)

    breaks_up   = base & (c > or_high)
    breaks_down = base & (c < or_low)

    if min_break_atr > 0:
        strong      = ~np.isnan(atr14) & (body >= min_break_atr * atr14)
        breaks_up   = breaks_up   & strong
        breaks_down = breaks_down & strong

    signal   = np.zeros(n, dtype=np.int8)
    last_sig = -9999
    day_count: dict = {}
    candidates = sorted(
        [(i,  1) for i in np.where(breaks_up)[0]] +
        [(i, -1) for i in np.where(breaks_down)[0]]
    )
    for i, d in candidates:
        if i - last_sig < cooldown:
            continue
        day = dates[i]
        if day_count.get(day, 0) >= max_t:
            continue
        signal[i] = d
        last_sig   = i
        day_count[day] = day_count.get(day, 0) + 1

    logger.info(
        f"[{canon}] cache built: n={n:,}  "
        f"signals={int((signal != 0).sum()):,}  "
        f"last_bar={df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}"
    )

    return {
        "canon":      canon,
        "n":          n,
        "o": o, "h": h, "l": l, "c": c,
        "atr14":      atr14,
        "atr_pct":    atr_pct,
        "or_high":    or_high,
        "or_low":     or_low,
        "signal":     signal,
        "utc_h":      utc_h,
        "utc_m":      utc_m,
        "dates":      dates,
        "times":      times,
        "in_session": in_session,
        # Wilder state for O(1) incremental update
        "_atr_prev":  float(atr14[~np.isnan(atr14)][-1])
                      if not np.all(np.isnan(atr14)) else 0.0,
        "_atr_hist":  [],   # rebuilt in init_cache_incremental
        "_prev_c":    float(c[-1]),
        "params":     params,
        "day_start":  day_start,
        "day_or":     day_or,
        # Live state
        "_last_sig_bar": int(np.where(signal != 0)[0][-1])
                         if (signal != 0).any() else -9999,
        "_day_count":    dict(day_count),
    }


def _rebuild_atr_hist(atr14: np.ndarray) -> list:
    """Rebuild sorted hist list for expanding pct rank — needed for incremental update."""
    hist = []
    for i in range(WARMUP_M5, len(atr14)):
        v = atr14[i]
        if not np.isnan(v):
            bisect.insort(hist, v)
    return hist


# ==============================================================================
#  SECTION 5 — CACHE INCREMENTAL UPDATE
# ==============================================================================

def append_bar_to_cache(cache: dict, bar_time: pd.Timestamp,
                         o: float, h: float, l: float, c: float) -> None:
    """
    Append one new closed bar to all arrays.
    Recompute ATR, ATR pct, OR, signal for the new bar only.
    This is O(1) per bar — no full rebuild.
    """
    canon  = cache["canon"]
    cfg    = SESSION[canon]
    params = cache["params"]
    n      = cache["n"]

    # ── Extend OHLC + time arrays ─────────────────────────────────────────────
    cache["o"]      = np.append(cache["o"],      o)
    cache["h"]      = np.append(cache["h"],      h)
    cache["l"]      = np.append(cache["l"],      l)
    cache["c"]      = np.append(cache["c"],      c)
    cache["times"]  = np.append(cache["times"],  np.datetime64(bar_time))

    utc_h_new = bar_time.hour
    utc_m_new = bar_time.minute
    date_new  = bar_time.date()

    cache["utc_h"]  = np.append(cache["utc_h"],  utc_h_new)
    cache["utc_m"]  = np.append(cache["utc_m"],  utc_m_new)
    cache["dates"]  = np.append(cache["dates"],  date_new)

    # ── ATR (Wilder step) ─────────────────────────────────────────────────────
    prev_c   = cache["_prev_c"]
    prev_atr = cache["_atr_prev"]
    tr       = max(h - l, abs(h - prev_c), abs(l - prev_c))
    k        = 1.0 / ATR_PERIOD
    new_atr  = prev_atr * (1.0 - k) + tr * k
    cache["atr14"]     = np.append(cache["atr14"], new_atr)
    cache["_atr_prev"] = new_atr
    cache["_prev_c"]   = c

    # ── ATR pct rank (incremental bisect) ─────────────────────────────────────
    hist    = cache["_atr_hist"]
    new_n   = n + 1
    if new_n > WARMUP_M5 and len(hist) > 0:
        new_pct = bisect.bisect_left(hist, new_atr) / len(hist)
    else:
        new_pct = float("nan")
    bisect.insort(hist, new_atr)
    cache["atr_pct"] = np.append(cache["atr_pct"], new_pct)

    # ── in_session ────────────────────────────────────────────────────────────
    in_sess_new = (
        (utc_h_new > cfg["open_h"] or
         (utc_h_new == cfg["open_h"] and utc_m_new >= cfg["open_m"]))
        and utc_h_new < cfg["close_h"]
    )
    cache["in_session"] = np.append(cache["in_session"], in_sess_new)

    # ── OR update ─────────────────────────────────────────────────────────────
    or_bars_n = OR_BARS[params["or_minutes"]]

    # Check if this bar is the session open bar
    if utc_h_new == cfg["open_h"] and utc_m_new == cfg["open_m"]:
        if date_new not in cache["day_start"]:
            cache["day_start"][date_new] = new_n - 1   # index of this bar

    # Check if OR window just completed for today
    if date_new in cache["day_start"] and date_new not in cache["day_or"]:
        si = cache["day_start"][date_new]
        ei = si + or_bars_n
        if new_n >= ei:
            h_arr = cache["h"]
            l_arr = cache["l"]
            cache["day_or"][date_new] = (
                h_arr[si:ei].max(),
                l_arr[si:ei].min()
            )
            logger.info(
                f"[{canon}] OR set for {date_new}: "
                f"high={cache['day_or'][date_new][0]:.5f} "
                f"low={cache['day_or'][date_new][1]:.5f}"
            )

    # Set or_high/or_low for this bar
    i        = new_n - 1
    d        = date_new
    or_h_new = np.nan
    or_l_new = np.nan
    if (in_sess_new
            and d in cache["day_or"]
            and d in cache["day_start"]
            and i >= cache["day_start"][d] + or_bars_n):
        or_h_new, or_l_new = cache["day_or"][d]

    cache["or_high"] = np.append(cache["or_high"], or_h_new)
    cache["or_low"]  = np.append(cache["or_low"],  or_l_new)

    # ── Signal detection for this bar (same logic as BT) ─────────────────────
    sig_new     = np.int8(0)
    atr_pct_new = new_pct
    atr_new     = new_atr

    if (in_sess_new
            and not np.isnan(atr_pct_new)
            and atr_pct_new >= ATR_PCT_THRESH
            and new_n > WARMUP_M5
            and not np.isnan(or_h_new)):

        cooldown      = params["cooldown_bars"]
        max_t         = params["max_trades_day"]
        min_break_atr = params["min_break_atr"]
        last_sig_bar  = cache["_last_sig_bar"]

        if i - last_sig_bar >= cooldown:
            day_count = cache["_day_count"]
            if day_count.get(d, 0) < max_t:
                body = abs(c - o)
                bu   = c > or_h_new
                bd   = c < or_l_new
                if min_break_atr > 0:
                    strong = body >= min_break_atr * atr_new
                    bu = bu and strong
                    bd = bd and strong

                if bu and not bd:
                    sig_new = np.int8(1)
                    logger.info(
                        f"[{canon}] SIGNAL LONG  bar={bar_time} "
                        f"close={c:.5f} > OR_HIGH={or_h_new:.5f} "
                        f"atr={atr_new:.5f} atr_pct={atr_pct_new:.3f}"
                    )
                elif bd and not bu:
                    sig_new = np.int8(-1)
                    logger.info(
                        f"[{canon}] SIGNAL SHORT bar={bar_time} "
                        f"close={c:.5f} < OR_LOW={or_l_new:.5f} "
                        f"atr={atr_new:.5f} atr_pct={atr_pct_new:.3f}"
                    )

                if sig_new != 0:
                    cache["_last_sig_bar"] = i
                    cache["_day_count"][d] = day_count.get(d, 0) + 1

    cache["signal"] = np.append(cache["signal"], sig_new)
    cache["n"]      = new_n


# ==============================================================================
#  SECTION 6 — DATA LOADING  (CSV + broker gap)
# ==============================================================================

def load_history(canon: str, broker_sym: str) -> pd.DataFrame:
    """
    Normalise CSV format (MT5 -> canonical), load it, then gap-fill
    from the broker for any bars after the last CSV row.
    Returns full DataFrame ready for build_cache().
    """
    # One-time idempotent format normalisation
    _normalize_csv_if_needed(canon)

    csv_df = _load_csv(canon)

    if csv_df is not None and len(csv_df) > 0:
        gap_from = (
            csv_df["time_utc"].iloc[-1].to_pydatetime()
            + datetime.timedelta(seconds=1)
        )
        gap_df = _fetch_broker_range(
            broker_sym, gap_from, datetime.datetime.utcnow()
        )
        if len(gap_df) > 0:
            combined = pd.concat([csv_df, gap_df], ignore_index=True)
            combined.sort_values("time_utc", inplace=True)
            combined.drop_duplicates(subset="time_utc", keep="last", inplace=True)
            combined.reset_index(drop=True, inplace=True)
            logger.info(
                f"[{canon}] history: {len(csv_df):,} CSV + "
                f"{len(gap_df):,} gap = {len(combined):,} bars"
            )
            return combined
        logger.info(f"[{canon}] history: {len(csv_df):,} CSV (no gap)")
        return csv_df

    # No CSV — fetch enough history for warmup directly from broker
    need = WARMUP_M5 + ATR_PERIOD + 500
    logger.info(f"[{canon}] No CSV — fetching {need} bars from broker")
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 0, need + 2)
    if rates is None or len(rates) < WARMUP_M5 + ATR_PERIOD + 10:
        logger.error(f"[{canon}] broker fetch failed")
        return None
    df = pd.DataFrame(rates)[["time","open","high","low","close"]].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df.drop(columns=["time"], inplace=True)
    now_ts = pd.Timestamp(datetime.datetime.utcnow())
    df = df[df["time_utc"] + pd.Timedelta(seconds=M5_SECONDS) <= now_ts].copy()
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _last_closed_bar_open_time() -> datetime.datetime:
    """
    Return the open timestamp of the last fully closed M5 bar.
    = floor(utcnow to 5-min grid) minus one bar.
    Used as the safe upper bound for gap fills so the currently-forming
    bar is never included in history.
    """
    now          = datetime.datetime.utcnow()
    secs         = now.hour * 3600 + now.minute * 60 + now.second
    rem          = secs % M5_SECONDS
    forming_open = now - datetime.timedelta(seconds=rem)
    return forming_open - datetime.timedelta(seconds=M5_SECONDS)


def _fetch_broker_range(broker_sym: str,
                         from_dt: datetime.datetime,
                         to_dt:   datetime.datetime) -> pd.DataFrame:
    # Clamp to_dt so the currently-forming bar is never included.
    last_closed = _last_closed_bar_open_time()
    safe_to     = min(to_dt, last_closed)
    if safe_to < from_dt:
        return pd.DataFrame(columns=["time_utc","open","high","low","close"])

    rates = mt5.copy_rates_range(broker_sym, mt5.TIMEFRAME_M5, from_dt, safe_to)
    if rates is None or len(rates) == 0:
        return pd.DataFrame(columns=["time_utc","open","high","low","close"])
    df = pd.DataFrame(rates)[["time","open","high","low","close"]].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df.drop(columns=["time"], inplace=True)
    # Secondary guard: drop any bar whose close time hasn't arrived yet
    now_ts = pd.Timestamp(datetime.datetime.utcnow())
    df = df[df["time_utc"] + pd.Timedelta(seconds=M5_SECONDS) <= now_ts].copy()
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _fetch_single_closed_bar(broker_sym: str,
                              expected_bar_time: pd.Timestamp) -> tuple:
    """
    Fetch the single just-closed bar.
    copy_rates_from_pos(..., 1, 1) returns the last fully-closed bar
    (index 0 = currently forming, index 1 = last closed on MT5).
    Polls until the broker confirms the bar whose open time ==
    expected_bar_time, or returns whatever index-1 holds on timeout.
    Returns (bar_time, o, h, l, c) or None on total failure.
    """
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 1, 1)
        if rates is not None and len(rates) == 1:
            bt = pd.Timestamp(int(rates[0]["time"]), unit="s")
            if bt == expected_bar_time:
                r = rates[0]
                return (
                    bt,
                    float(r["open"]), float(r["high"]),
                    float(r["low"]),  float(r["close"])
                )
        time.sleep(0.1)

    # Timeout fallback: use whatever is at index 1
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 1, 1)
    if rates is not None and len(rates) == 1:
        r  = rates[0]
        bt = pd.Timestamp(int(r["time"]), unit="s")
        logger.warning(
            f"[{broker_sym}] bar time mismatch: "
            f"expected {expected_bar_time} got {bt}"
        )
        return (
            bt,
            float(r["open"]), float(r["high"]),
            float(r["low"]),  float(r["close"])
        )
    return None


# ==============================================================================
#  SECTION 7 — STOPS LEVEL + SL HELPERS
# ==============================================================================

def get_min_sl_distance(broker_sym: str) -> float:
    info     = _tick_info.get(broker_sym)
    point    = info["point"] if info else 0.0001
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
            return limit
    else:
        limit = price + min_dist
        if sl < limit:
            return limit
    return sl


# ==============================================================================
#  SECTION 8 — POSITION SIZING
# ==============================================================================

def compute_lot(broker_sym: str, sl_dist: float, balance: float):
    ti = _tick_info.get(broker_sym)
    if ti is None or sl_dist < 1e-9:
        return None, None

    tvpl     = ti["tick_value_per_lot"]
    vol_step = ti["vol_step"]
    vol_min  = ti["vol_min"]

    # Daily loss cap
    per_trade = (
        DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO
        if _MAX_TRADES_DAY_COMBO else DAILY_LOSS_BUDGET
    )
    vol_cap = max(
        vol_min,
        round((per_trade / (sl_dist * tvpl)) / vol_step) * vol_step
    )
    vol_cap = min(vol_cap, VOL_MAX)

    risk_amount = balance * RISK_PER_TRADE
    raw_lot     = risk_amount / (sl_dist * tvpl)
    lot = max(vol_min, min(vol_cap, round(raw_lot / vol_step) * vol_step))
    lot = round(lot, 8)

    # Risk guard
    intended = balance * RISK_PER_TRADE
    actual   = lot * sl_dist * tvpl
    multiple = actual / intended if intended > 0 else float("inf")
    if multiple > MAX_RISK_MULTIPLE:
        logger.warning(f"[{broker_sym}] RISK REJECTED {multiple:.2f}x")
        return None, None

    logger.info(
        f"[{broker_sym}] lot={lot} sl_dist={sl_dist:.5f} "
        f"tvpl={tvpl:.5f} risk={actual:.2f} ({multiple:.2f}x)"
    )
    return lot, tvpl


# ==============================================================================
#  SECTION 9 — ORDER EXECUTION
# ==============================================================================

def send_market_order(broker_sym: str, direction: str, lot: float,
                      sl_price: float, comment: str):
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{broker_sym}] tick unavailable")
        return None, None

    price    = tick.ask if direction == "long" else tick.bid
    otype    = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL
    sl_price = clamp_sl(broker_sym, direction, price, sl_price)

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
        logger.error(
            f"[{broker_sym}] Entry FAILED retcode="
            f"{getattr(result,'retcode',None)} "
            f"msg={getattr(result,'comment','')}"
        )
        return None, None
    logger.info(
        f"[{broker_sym}] ENTRY {direction.upper()} "
        f"lot={lot} price={price:.5f} sl={sl_price:.5f} "
        f"ticket={result.order}"
    )
    return result.order, price


def modify_sl_with_retry(broker_sym: str, ticket: int,
                          new_sl: float, direction: str,
                          timeout: float = None) -> bool:
    """
    Retry SL modification indefinitely until the broker confirms success.
    Re-fetches bid/ask and re-clamps before every attempt so the SL price
    is always valid against the current stops level.

    Only stops on:
      DONE / NO_CHANGES   — success, returns True.
      MARKET_CLOSED       — not retriable, returns False immediately.
      position gone       — ticket no longer exists, returns False.

    10016 (INVALID_STOPS) and all other transient errors are retried
    without limit. timeout parameter kept for call-site compatibility only.
    """
    attempt = 0

    while True:
        attempt += 1

        # ── Abort if position no longer exists ───────────────────────────────
        if not mt5.positions_get(ticket=ticket):
            logger.warning(
                f"[{broker_sym}] SL retry {attempt}: "
                f"ticket={ticket} no longer exists — aborting"
            )
            return False

        # ── Abort if market is closed/disabled ───────────────────────────────
        sym_info = mt5.symbol_info(broker_sym)
        if (sym_info is not None and
                sym_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED):
            logger.error(
                f"[{broker_sym}] SL modify aborted: market disabled "
                f"ticket={ticket}"
            )
            return False

        # ── Fetch fresh tick ─────────────────────────────────────────────────
        tick = mt5.symbol_info_tick(broker_sym)
        if tick is None:
            logger.warning(
                f"[{broker_sym}] SL retry {attempt}: tick unavailable — waiting"
            )
            time.sleep(0.5)
            continue

        cur_px     = tick.bid if direction == "long" else tick.ask
        sl_clamped = clamp_sl(broker_sym, direction, cur_px, new_sl)

        req = {
            "action":   mt5.TRADE_ACTION_SLTP,
            "symbol":   broker_sym,
            "position": ticket,
            "sl":       sl_clamped,
            "tp":       0.0,
        }
        result  = mt5.order_send(req)
        retcode = getattr(result, "retcode", None)

        # ── Success ──────────────────────────────────────────────────────────
        if retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_NO_CHANGES):
            logger.info(
                f"[{broker_sym}] SL modified ticket={ticket} "
                f"sl={sl_clamped:.5f} (attempt {attempt})"
            )
            return True

        # ── Log and retry ────────────────────────────────────────────────────
        logger.warning(
            f"[{broker_sym}] SL retry {attempt} retcode={retcode} "
            f"sl_target={sl_clamped:.5f} price={cur_px:.5f}"
        )
        time.sleep(0.2)


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
    if ok:
        logger.info(
            f"[{broker_sym}] CLOSED ticket={position.ticket} price={price:.5f}"
        )
    else:
        logger.error(
            f"[{broker_sym}] Close FAILED "
            f"retcode={getattr(result,'retcode',None)}"
        )
    return ok


# ==============================================================================
#  SECTION 10 — PER-SYMBOL STATE
# ==============================================================================

def make_sym_state() -> dict:
    return {
        "positions":        [],
        "day_trades_date":  None,
        "day_trades_count": 0,
    }


def _make_pos_rec(ticket, direction, ep, sl_dist,
                  sl_price, entry_date, entry_atr) -> dict:
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
    entry_time = datetime.datetime.fromtimestamp(
        pos.time, tz=datetime.timezone.utc
    )
    now_utc   = datetime.datetime.now(tz=datetime.timezone.utc)
    hold      = max(0, int((now_utc - entry_time).total_seconds() / M5_SECONDS))
    direction = "long" if pos.type == mt5.ORDER_TYPE_BUY else "short"
    ep        = pos.price_open
    sl_price  = pos.sl or 0.0
    sl_dist   = abs(ep - sl_price) if sl_price > 0 else 0.01
    be_active = (
        (direction == "long"  and sl_price >= ep) or
        (direction == "short" and sl_price > 0 and sl_price <= ep)
    )
    rec = _make_pos_rec(
        pos.ticket, direction, ep, sl_dist,
        sl_price, entry_time.date(), None
    )
    rec["be_active"]  = be_active
    rec["hold_count"] = hold
    logger.info(
        f"[{canon}] RECOVERED ticket={pos.ticket} "
        f"dir={direction} ep={ep:.5f} sl={sl_price:.5f} "
        f"hold~{hold}bars be={be_active}"
    )
    return rec


# ==============================================================================
#  SECTION 11 — POSITION MANAGEMENT
#  Mirrors resolve_trade() forward simulation from BT exactly.
# ==============================================================================

def manage_positions(canon: str, broker_sym: str, sym_st: dict,
                     cache: dict, params: dict, today) -> None:
    """
    For each open position: check EOD, max hold, BE, trail.
    Uses current bar (cache[-1]) — same logic as resolve_trade() inner loop.
    SL modifications use modify_sl_with_retry() — retries on same bar.
    """
    i        = cache["n"] - 1
    bar_h    = float(cache["h"][i])
    bar_l    = float(cache["l"][i])
    bar_date = cache["dates"][i]
    bar_hour = int(cache["utc_h"][i])
    cfg      = SESSION[canon]
    atr_cur  = float(cache["atr14"][i])

    broker_positions = mt5.positions_get(symbol=broker_sym) or []
    broker_positions = [p for p in broker_positions if p.magic == MAGIC]
    broker_map       = {p.ticket: p for p in broker_positions}
    broker_tickets   = set(broker_map.keys())

    # Detect server-side closes
    for pr in list(sym_st["positions"]):
        if pr["ticket"] not in broker_tickets:
            logger.info(
                f"[{canon}] ticket={pr['ticket']} closed server-side"
            )
            sym_st["positions"].remove(pr)

    for pr in list(sym_st["positions"]):
        bp        = broker_map.get(pr["ticket"])
        direction = pr["direction"]
        ep        = pr["entry_price"]
        sl_dist   = pr["sl_dist"]

        pr["hold_count"] += 1
        hc = pr["hold_count"]

        # ── EOD exit — same condition as resolve_trade() ──────────────────────
        if bar_date != pr["entry_date"] or bar_hour >= cfg["close_h"]:
            logger.info(
                f"[{canon}] EOD exit ticket={pr['ticket']} "
                f"bar={bar_date} {bar_hour:02d}h entry={pr['entry_date']}"
            )
            if bp:
                send_close(broker_sym, bp)
            sym_st["positions"].remove(pr)
            continue

        # ── Max hold ──────────────────────────────────────────────────────────
        if hc >= MAX_HOLD:
            logger.info(f"[{canon}] MAX HOLD ticket={pr['ticket']}")
            if bp:
                send_close(broker_sym, bp)
            sym_st["positions"].remove(pr)
            continue

        # ── Break-even at 1R — same as resolve_trade() ────────────────────────
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
                    f"[{canon}] BE ticket={pr['ticket']} SL -> {ep:.5f}"
                )

        # ── Trail — same as resolve_trade() ──────────────────────────────────
        if pr["be_active"]:
            ta   = (atr_cur if (not np.isnan(atr_cur) and atr_cur > 0)
                    else (pr["entry_atr"] or sl_dist))
            mult = params["trail_atr_mult"]
            if direction == "long":
                pr["current_sl"] = max(
                    pr["current_sl"], bar_h - mult * ta
                )
            else:
                pr["current_sl"] = min(
                    pr["current_sl"], bar_l + mult * ta
                )

        # ── SL modify with retry on same bar ──────────────────────────────────
        if bp is not None:
            broker_sl = bp.sl or 0.0
            new_sl    = pr["current_sl"]
            pip       = _tick_info.get(broker_sym, {}).get("pip", 0.0001)
            if abs(new_sl - broker_sl) >= pip:
                modify_sl_with_retry(
                    broker_sym, pr["ticket"], new_sl, direction, timeout=8.0
                )


# ==============================================================================
#  SECTION 12 — ENTRY EXECUTION
# ==============================================================================

def execute_entry(canon: str, broker_sym: str, sym_st: dict, params: dict,
                  balance: float, today, direction: str,
                  sl_dist: float, atr_val: float,
                  or_h: float, or_l: float) -> None:

    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{canon}] tick unavailable — entry cancelled")
        return

    ep      = tick.ask if direction == "long" else tick.bid
    min_sl  = 0.05 * atr_val
    sl_dist = max(sl_dist, min_sl)
    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist

    # Edge guard
    if direction == "long" and sl_price >= ep:
        sl_price = ep - min_sl
        sl_dist  = min_sl
    if direction == "short" and sl_price <= ep:
        sl_price = ep + min_sl
        sl_dist  = min_sl

    sl_clamped = clamp_sl(broker_sym, direction, ep, sl_price)
    if sl_clamped != sl_price:
        sl_dist  = abs(ep - sl_clamped)
        sl_price = sl_clamped

    lot, tvpl = compute_lot(broker_sym, sl_dist, balance)
    if lot is None:
        logger.error(f"[{canon}] lot calc failed — entry cancelled")
        return

    ticket, _ = send_market_order(
        broker_sym, direction, lot, sl_price, f"{COMMENT}_{canon}"
    )
    if ticket is None:
        return

    # Confirm fill
    filled = []
    for _ in range(10):
        time.sleep(0.05)
        filled = [
            p for p in (mt5.positions_get(symbol=broker_sym) or [])
            if p.magic == MAGIC and p.ticket == ticket
        ]
        if filled:
            break

    if filled:
        actual_ep = filled[0].price_open
        actual_sl = filled[0].sl
        sl_dist   = abs(actual_ep - actual_sl) or min_sl
    else:
        actual_ep = ep
        actual_sl = sl_price

    rec = _make_pos_rec(
        ticket, direction, actual_ep, sl_dist, actual_sl, today, atr_val
    )
    sym_st["positions"].append(rec)
    sym_st["day_trades_count"] += 1

    logger.info(
        f"[{canon}] ENTERED {direction.upper()} ticket={ticket} "
        f"ep={actual_ep:.5f} sl={actual_sl:.5f} "
        f"sl_dist={sl_dist:.5f} lot={lot} "
        f"OR_HIGH={or_h:.5f} OR_LOW={or_l:.5f}"
    )


# ==============================================================================
#  SECTION 13 — PER-BAR PROCESSING
# ==============================================================================

def process_bar(canon: str, broker_sym: str, sym_st: dict,
                cache: dict, params: dict, balance: float,
                new_bar_time: pd.Timestamp) -> None:
    """
    Called once per confirmed closed M5 bar.

    1. Fetch the closed bar OHLC from broker.
    2. Append to cache arrays + recompute indicators (O(1)).
    3. Write bar to CSV (always in canonical format).
    4. Manage open positions (EOD, BE, trail, SL retry).
    5. Check signal on last bar — if fired, execute entry immediately.
    """
    # ── 1. Fetch closed bar ───────────────────────────────────────────────────
    bar = _fetch_single_closed_bar(broker_sym, new_bar_time)
    if bar is None:
        logger.warning(
            f"[{canon}] could not fetch bar {new_bar_time} — skipping"
        )
        return
    bar_time, o, h, l, c = bar

    # ── 2. Append to cache ────────────────────────────────────────────────────
    append_bar_to_cache(cache, bar_time, o, h, l, c)

    # ── 3. Write to CSV (canonical comma-sep ISO format) ─────────────────────
    _append_csv(canon, bar_time, o, h, l, c)

    # ── 4. Manage positions ───────────────────────────────────────────────────
    today = cache["dates"][-1]
    _reset_daily(sym_st, today)
    manage_positions(canon, broker_sym, sym_st, cache, params, today)

    # ── 5. Signal check — direct array lookup, same as BT ────────────────────
    i   = cache["n"] - 1
    sig = int(cache["signal"][i])

    if sig == 0:
        return

    # Reconcile broker before entry — don't double-enter
    broker_positions = [
        p for p in (mt5.positions_get(symbol=broker_sym) or [])
        if p.magic == MAGIC
    ]
    if len(broker_positions) >= params["max_trades_day"]:
        logger.info(
            f"[{canon}] SIGNAL suppressed — already at max positions"
        )
        return

    direction = "long" if sig == 1 else "short"
    or_h      = float(cache["or_high"][i])
    or_l      = float(cache["or_low"][i])
    atr_val   = float(cache["atr14"][i])
    or_size   = or_h - or_l
    sl_dist   = max(params["sl_range_mult"] * or_size, atr_val * 0.05)

    if sl_dist < 3.0:
        logger.info(
            f"[{canon}] SIGNAL_SKIP sl_too_tight: {sl_dist:.5f}"
        )
        return

    logger.info(
        f"[{canon}] SIGNAL {direction.upper()} "
        f"OR_HIGH={or_h:.5f} OR_LOW={or_l:.5f} "
        f"close={float(cache['c'][i]):.5f} "
        f"sl_dist={sl_dist:.5f} atr={atr_val:.5f} "
        f"day_trades={sym_st['day_trades_count']}/{params['max_trades_day']}"
    )

    execute_entry(
        canon, broker_sym, sym_st, params, balance, today,
        direction, sl_dist, atr_val, or_h, or_l
    )


# ==============================================================================
#  SECTION 14 — BAR CLOCK
# ==============================================================================

_CLOCK_BROKER = None


def _next_m5_boundary() -> datetime.datetime:
    now  = datetime.datetime.utcnow()
    secs = now.hour * 3600 + now.minute * 60 + now.second
    rem  = secs % M5_SECONDS
    wait = M5_SECONDS - rem if rem > 0 else M5_SECONDS
    return now + datetime.timedelta(seconds=wait)


def _broker_last_bar_time() -> pd.Timestamp:
    if _CLOCK_BROKER is None:
        return None
    rates = mt5.copy_rates_from_pos(_CLOCK_BROKER, mt5.TIMEFRAME_M5, 1, 1)
    if rates is not None and len(rates) == 1:
        return pd.Timestamp(int(rates[0]["time"]), unit="s")
    return None


def wait_for_new_bar(last_bar_time: pd.Timestamp) -> pd.Timestamp:
    while True:
        boundary  = _next_m5_boundary()
        sleep_sec = (boundary - datetime.datetime.utcnow()).total_seconds() - 0.5
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            t = _broker_last_bar_time()
            if t is not None and t > last_bar_time:
                return t
            time.sleep(0.1)
        logger.debug(
            f"[clock] no new bar after {boundary.strftime('%H:%M')} — retry"
        )


# ==============================================================================
#  SECTION 15 — MAIN LOOP
# ==============================================================================

def _process_safe(canon, broker, sym_st, cache, params, balance, new_bar_time):
    try:
        process_bar(
            canon, broker, sym_st, cache, params, balance, new_bar_time
        )
    except Exception as e:
        logger.exception(f"[{canon}] process_bar error: {e}")


def run_live():
    global _CLOCK_BROKER, _MAX_TRADES_DAY_COMBO

    print("ORB V6 starting...", flush=True)

    # ── MT5 init ──────────────────────────────────────────────────────────────
    for attempt in range(30):
        ok  = mt5.initialize()
        err = mt5.last_error()
        print(f"  MT5 init attempt {attempt+1}/30: ok={ok} err={err}", flush=True)
        if ok:
            break
        time.sleep(3)
    else:
        raise RuntimeError("MT5 did not respond after 90s")

    auth = mt5.login(LOGIN, PASSWORD, SERVER)
    if not auth:
        mt5.shutdown()
        raise RuntimeError(f"Login failed: {mt5.last_error()}")

    acct = mt5.account_info()
    logger.info(
        f"MT5 connected | account={acct.login} | "
        f"balance={acct.balance:.2f} | currency={acct.currency}"
    )

    # ── Symbol map ────────────────────────────────────────────────────────────
    sym_map, active = build_symbol_map()
    if not active:
        mt5.shutdown()
        return

    for canon in active:
        broker = sym_map[canon]
        info   = mt5.symbol_info(broker)
        if info is None:
            continue
        tvpl = (info.trade_tick_value / info.trade_tick_size
                if info.trade_tick_size > 0 else 1.0)
        pip  = 10 ** (-info.digits + 1)
        _tick_info[broker] = {
            "tick_value_per_lot": tvpl,
            "vol_min":  max(info.volume_min,  VOL_MIN),
            "vol_max":  max(info.volume_max,  VOL_MAX),
            "vol_step": max(info.volume_step, VOL_STEP),
            "pip":      pip,
            "point":    info.point if info.point > 0 else pip,
        }
        logger.info(
            f"  {canon} ({broker}): digits={info.digits} "
            f"tvpl={tvpl:.6f} vol_min={info.volume_min} "
            f"stops_level={info.trade_stops_level} "
            f"params={BEST_PARAMS[canon]}"
        )

    _MAX_TRADES_DAY_COMBO = sum(
        BEST_PARAMS[s]["max_trades_day"] for s in active
    )

    # ── Build caches from CSV + gap ───────────────────────────────────────────
    logger.info("=== BUILDING CACHES ===")
    caches = {}
    for canon in list(active):
        broker = sym_map[canon]
        df = load_history(canon, broker)      # normalises CSV format internally
        if df is None or len(df) < WARMUP_M5 + ATR_PERIOD + 10:
            logger.error(f"[{canon}] not enough history — skipping")
            active.remove(canon)
            continue
        cache = build_cache(canon, df, BEST_PARAMS[canon])
        cache["_atr_hist"] = _rebuild_atr_hist(cache["atr14"])
        caches[canon] = cache

    if not active:
        mt5.shutdown()
        return
    logger.info("=== END CACHES ===")

    _CLOCK_BROKER = sym_map[active[0]]
    sym_states    = {c: make_sym_state() for c in active}

    # ── Startup recovery ──────────────────────────────────────────────────────
    logger.info("=== STARTUP RECOVERY ===")
    for canon in active:
        broker    = sym_map[canon]
        positions = [
            p for p in (mt5.positions_get(symbol=broker) or [])
            if p.magic == MAGIC
        ]
        for pos in positions:
            if COMMENT in (pos.comment or ""):
                rec = _reconstruct_pos(canon, pos)
                sym_states[canon]["positions"].append(rec)
                sym_states[canon]["day_trades_count"] = min(
                    sym_states[canon]["day_trades_count"] + 1,
                    BEST_PARAMS[canon]["max_trades_day"]
                )
        logger.info(
            f"  {canon}: recovered {len(sym_states[canon]['positions'])} pos"
        )
    logger.info("=== END RECOVERY ===")

    # ── Main loop ─────────────────────────────────────────────────────────────
    # Seed from the cache's own last bar, not the broker.
    # The broker's last-closed-bar time can be ahead of the cache if the gap
    # fill missed the most recent bar by a few milliseconds at startup — seeding
    # from the broker would skip that bar forever.  Seeding from the cache
    # guarantees wait_for_new_bar fires for every bar the cache hasn't seen.
    cache_last_times = [
        pd.Timestamp(int(caches[c]["times"][-1]))
        for c in active
    ]
    last_bar_time = min(cache_last_times)   # conservative: use earliest
    logger.info(
        f"Seeded bar time from cache: {last_bar_time} — "
        f"waiting for next M5 close..."
    )

    bar_count = 0
    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1

            balance = mt5.account_info().balance
            logger.info(
                f"-- BAR {bar_count} | {new_bar_time} UTC | "
                f"balance={balance:.2f} "
                f"------------------------------"
            )

            threads = [
                threading.Thread(
                    target=_process_safe,
                    args=(
                        canon, sym_map[canon], sym_states[canon],
                        caches[canon], BEST_PARAMS[canon], balance,
                        new_bar_time
                    ),
                    daemon=True,
                )
                for canon in active
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=25)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down ORB V6")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(60)

    mt5.shutdown()
    logger.info("MT5 disconnected. ORB V6 stopped.")


if __name__ == "__main__":
    run_live()
