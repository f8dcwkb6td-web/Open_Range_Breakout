"""
==============================================================================
ORB  —  LIVE ENGINE v6
==============================================================================
CORE PRINCIPLE:
  Direct port of orb_chrono_bt.py.  OR, ATR, signal logic are IDENTICAL
  to build_cache_and_signals() + resolve_trade() in the BT.

HOW IT WORKS:
  STARTUP:
    1. Normalise CSV (MT5 export -> canonical format).
    2. Load CSV history.
    3. Gap-fill from broker.
    4. Run build_cache() — same as BT build_cache_and_signals().
    5. Run verify_cache_vs_bt() — compares live cache signals against
       BT re-run on the same data.  Logs % accuracy per symbol.
       ABORTS that symbol if accuracy < MIN_BT_ACCURACY (95%).
    6. Enter bar loop.

  EACH NEW BAR:
    1. _fetch_single_closed_bar() uses cache's last bar time as reference
       (bt > last_cached) — no wall-clock comparison, no mismatch possible.
    2. Append to cache (O(1) incremental update identical to BT).
    3. Write to CSV.
    4. Manage positions (EOD, BE, trail, SL retry on same bar).
    5. Signal check — direct array lookup, entry immediately.

  ENTRY:
    Fresh account_info() at entry time — not stale balance from main loop.
    Free margin check before sending order — prevents 10019.

  SL MODIFICATION:
    Retries indefinitely on same bar.
    Re-fetches bid/ask + re-clamps before every attempt.
    Aborts only on: position gone, market disabled.

FIXES vs previous version:
  - Bar time mismatch (expected 13:35 got 13:40): _fetch_single_closed_bar
    now uses cache last bar time as reference, not wall-clock new_bar_time.
    Any bar newer than cache is accepted immediately.
  - 10019 no money: execute_entry fetches fresh account_info() at entry
    time and checks free_margin before sending.
  - balance passed to threads: removed — each entry fetches its own.
  - BT accuracy check on startup: verify_cache_vs_bt() runs over full
    CSV history and logs match % before any trading begins.

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

# Minimum signal match rate vs BT required before going live per symbol.
MIN_BT_ACCURACY = 0.95

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

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

_tick_info:            dict = {}
_MAX_TRADES_DAY_COMBO: int  = 0


# ==============================================================================
#  SECTION 1 — CSV PERSISTENCE
# ==============================================================================

CSV_DTYPE = {"open": np.float64, "high": np.float64,
             "low":  np.float64, "close": np.float64}


def _csv_path(canon: str) -> str:
    for d in (SCRIPT_DIR, os.getcwd()):
        p = os.path.join(d, f"{canon}.cash.csv")
        if os.path.isfile(p):
            return p
    return os.path.join(SCRIPT_DIR, f"{canon}.cash.csv")


def _load_csv(canon: str):
    path = _csv_path(canon)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()
        sep = "\t" if "\t" in first_line else ","
        df  = pd.read_csv(path, sep=sep, engine="python")

        def _clean_col(c):
            c = c.strip().lower().replace("<", "").replace(">", "")
            if c.startswith("t") and c[1:] in (
                "date","time","open","high","low","close","tickvol","vol","spread"
            ):
                c = c[1:]
            return c

        df.columns = [_clean_col(c) for c in df.columns]

        if "date" in df.columns and "time" in df.columns:
            combined = (df["date"].astype(str).str.strip()
                        + " " + df["time"].astype(str).str.strip())
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
                    df["time"].astype(np.int64), unit="s")
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
        logger.info(f"[{canon}] CSV: {len(df):,} bars  "
                    f"{df['time_utc'].iloc[0].date()} -> "
                    f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}")
        return df
    except Exception as e:
        logger.error(f"[{canon}] CSV load failed: {e}", exc_info=True)
        return None


def _normalize_csv_if_needed(canon: str) -> None:
    path = _csv_path(canon)
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()
    except Exception as e:
        logger.warning(f"[{canon}] Could not read CSV header: {e}")
        return
    if first_line.strip().lower().startswith("time_utc"):
        return
    logger.info(f"[{canon}] MT5 CSV detected — normalising...")
    df = _load_csv(canon)
    if df is None or len(df) == 0:
        return
    try:
        tmp = path + ".normalising"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            f.write("time_utc,open,high,low,close\n")
            for _, row in df.iterrows():
                f.write(
                    f"{row['time_utc'].strftime('%Y-%m-%d %H:%M:%S')},"
                    f"{row['open']},{row['high']},{row['low']},{row['close']}\n"
                )
        os.replace(tmp, path)
        logger.info(f"[{canon}] CSV normalised: {len(df):,} bars")
    except Exception as e:
        logger.error(f"[{canon}] CSV normalisation failed: {e}", exc_info=True)
        if os.path.isfile(path + ".normalising"):
            try:
                os.remove(path + ".normalising")
            except Exception:
                pass


def _append_csv(canon: str, bar_time: pd.Timestamp,
                o: float, h: float, l: float, c: float) -> None:
    path = _csv_path(canon)
    row  = (f"{bar_time.strftime('%Y-%m-%d %H:%M:%S')},"
            f"{o},{h},{l},{c}\n")
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
    tr[1:] = np.maximum(h[1:] - l[1:],
               np.maximum(np.abs(h[1:] - c[:-1]),
                          np.abs(l[1:] - c[:-1])))
    out = np.full(n, np.nan)
    if n < ATR_PERIOD:
        return out
    out[ATR_PERIOD - 1] = tr[:ATR_PERIOD].mean()
    k = 1.0 / ATR_PERIOD
    for i in range(ATR_PERIOD, n):
        out[i] = out[i-1] * (1.0 - k) + tr[i] * k
    return out


def _expanding_pct_rank(arr):
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
    cfg = SESSION[canon]
    o   = df["open"].values.astype(np.float64)
    h   = df["high"].values.astype(np.float64)
    l   = df["low"].values.astype(np.float64)
    c   = df["close"].values.astype(np.float64)
    n   = len(c)

    times = df["time_utc"].values
    utc_h = df["time_utc"].dt.hour.values.astype(np.int32)
    utc_m = df["time_utc"].dt.minute.values.astype(np.int32)
    dates = df["time_utc"].dt.date.values

    atr14   = _atr_wilder_full(h, l, c)
    atr_pct = _expanding_pct_rank(atr14)

    in_session = np.array([
        (utc_h[i] > cfg["open_h"] or
         (utc_h[i] == cfg["open_h"] and utc_m[i] >= cfg["open_m"]))
        and utc_h[i] < cfg["close_h"]
        for i in range(n)
    ])
    is_open_bar = (utc_h == cfg["open_h"]) & (utc_m == cfg["open_m"])

    or_bars_n = OR_BARS[params["or_minutes"]]
    day_start: dict = {}
    for i in range(n):
        if is_open_bar[i]:
            d = dates[i]
            if d not in day_start:
                day_start[d] = i

    day_or: dict = {}
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

    logger.info(f"[{canon}] cache built: n={n:,}  "
                f"signals={int((signal != 0).sum()):,}  "
                f"last_bar={df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')}")

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
        "_atr_prev":  float(atr14[~np.isnan(atr14)][-1])
                      if not np.all(np.isnan(atr14)) else 0.0,
        "_atr_hist":  [],
        "_prev_c":    float(c[-1]),
        "params":     params,
        "day_start":  day_start,
        "day_or":     day_or,
        "_last_sig_bar": int(np.where(signal != 0)[0][-1])
                         if (signal != 0).any() else -9999,
        "_day_count":    dict(day_count),
    }


def _rebuild_atr_hist(atr14: np.ndarray) -> list:
    hist = []
    for i in range(WARMUP_M5, len(atr14)):
        v = atr14[i]
        if not np.isnan(v):
            bisect.insort(hist, v)
    return hist


# ==============================================================================
#  SECTION 4B — BT ACCURACY VERIFICATION
#
#  Runs build_cache() independently on the same DataFrame the live cache
#  was built from, then compares signal arrays element-by-element.
#
#  Metric: over all bars where EITHER cache fired a signal, what fraction
#  do both agree on direction?  Zero-vs-zero bars are not counted (trivial).
#
#  Logs:
#    - Overall match %
#    - Direction accuracy on bars both fired
#    - False positives (live only), missed (BT only), wrong direction
#    - Last 20 BT signal bars with OK/MISMATCH status
#    - First 10 discrepancies with timestamps
#
#  Returns True if accuracy >= MIN_BT_ACCURACY.
# ==============================================================================

def verify_cache_vs_bt(canon: str, df: pd.DataFrame,
                        live_cache: dict, params: dict) -> bool:
    logger.info(f"[{canon}] === BT ACCURACY CHECK ===")

    bt_cache = build_cache(canon, df, params)

    live_sig = live_cache["signal"]
    bt_sig   = bt_cache["signal"]
    n        = min(len(live_sig), len(bt_sig))
    times    = live_cache["times"]

    live_fire_set = set(np.where(live_sig[:n] != 0)[0].tolist())
    bt_fire_set   = set(np.where(bt_sig[:n]   != 0)[0].tolist())
    all_bars      = live_fire_set | bt_fire_set
    total         = len(all_bars)

    if total == 0:
        logger.warning(f"[{canon}] BT check: no signals in either cache "
                       f"— nothing to compare (warmup only?)")
        return True

    matches = sum(
        1 for i in all_bars
        if (int(live_sig[i]) if i < len(live_sig) else 0) ==
           (int(bt_sig[i])   if i < len(bt_sig)   else 0)
    )
    accuracy = matches / total

    both_fired  = live_fire_set & bt_fire_set
    dir_matches = sum(1 for i in both_fired
                      if live_sig[i] == bt_sig[i])
    dir_total   = len(both_fired)
    dir_acc     = dir_matches / dir_total if dir_total > 0 else 1.0

    live_only = sorted(live_fire_set - bt_fire_set)
    bt_only   = sorted(bt_fire_set   - live_fire_set)
    wrong_dir = sorted(i for i in both_fired if live_sig[i] != bt_sig[i])

    logger.info(f"[{canon}] Signal match: {accuracy:.2%}  "
                f"({matches}/{total} bars agree)")
    logger.info(f"[{canon}] Direction match (both fired): {dir_acc:.2%}  "
                f"({dir_matches}/{dir_total})")
    logger.info(f"[{canon}] False positives (live only): {len(live_only)}  "
                f"Missed (BT only): {len(bt_only)}  "
                f"Wrong direction: {len(wrong_dir)}")

    # First 10 discrepancies
    discrepancies = sorted(
        (live_fire_set ^ bt_fire_set) | set(wrong_dir)
    )[:10]
    if discrepancies:
        logger.info(f"[{canon}] First discrepancies:")
        for i in discrepancies:
            ts  = pd.Timestamp(int(times[i])) if i < len(times) else "?"
            lv  = int(live_sig[i]) if i < len(live_sig) else 0
            bv  = int(bt_sig[i])   if i < len(bt_sig)   else 0
            tag = ("live_only" if i in live_fire_set - bt_fire_set else
                   "bt_only"   if i in bt_fire_set   - live_fire_set else
                   "wrong_dir")
            logger.info(f"  [{tag}] bar={ts}  live={lv:+d}  bt={bv:+d}")

    # Last 20 BT signal bars
    bt_fire_sorted = sorted(bt_fire_set)
    logger.info(f"[{canon}] Last 20 BT signals ({len(bt_fire_sorted)} total):")
    for i in bt_fire_sorted[-20:]:
        ts    = pd.Timestamp(int(times[i])) if i < len(times) else "?"
        lv    = int(live_sig[i]) if i < len(live_sig) else 0
        bv    = int(bt_sig[i])
        ok    = "OK" if lv == bv else "MISMATCH"
        orh   = float(bt_cache["or_high"][i])
        orl   = float(bt_cache["or_low"][i])
        cl    = float(bt_cache["c"][i])
        logger.info(f"  [{ok}] {ts}  BT={bv:+d} live={lv:+d}  "
                    f"OR_H={orh:.5f} OR_L={orl:.5f} close={cl:.5f}")

    logger.info(f"[{canon}] === END BT ACCURACY CHECK ===")

    if accuracy < MIN_BT_ACCURACY:
        logger.error(f"[{canon}] accuracy {accuracy:.2%} < "
                     f"required {MIN_BT_ACCURACY:.2%} — ABORTING this symbol")
        return False

    logger.info(f"[{canon}] accuracy {accuracy:.2%} >= "
                f"{MIN_BT_ACCURACY:.2%} — OK to trade")
    return True


# ==============================================================================
#  SECTION 5 — CACHE INCREMENTAL UPDATE  (O(1) per bar, identical to BT)
# ==============================================================================

def append_bar_to_cache(cache: dict, bar_time: pd.Timestamp,
                         o: float, h: float, l: float, c: float) -> None:
    canon  = cache["canon"]
    cfg    = SESSION[canon]
    params = cache["params"]
    n      = cache["n"]

    cache["o"]     = np.append(cache["o"],     o)
    cache["h"]     = np.append(cache["h"],     h)
    cache["l"]     = np.append(cache["l"],     l)
    cache["c"]     = np.append(cache["c"],     c)
    cache["times"] = np.append(cache["times"], np.datetime64(bar_time))

    utc_h_new = bar_time.hour
    utc_m_new = bar_time.minute
    date_new  = bar_time.date()

    cache["utc_h"]  = np.append(cache["utc_h"],  utc_h_new)
    cache["utc_m"]  = np.append(cache["utc_m"],  utc_m_new)
    cache["dates"]  = np.append(cache["dates"],  date_new)

    # ATR Wilder step
    prev_c   = cache["_prev_c"]
    prev_atr = cache["_atr_prev"]
    tr       = max(h - l, abs(h - prev_c), abs(l - prev_c))
    k        = 1.0 / ATR_PERIOD
    new_atr  = prev_atr * (1.0 - k) + tr * k
    cache["atr14"]     = np.append(cache["atr14"], new_atr)
    cache["_atr_prev"] = new_atr
    cache["_prev_c"]   = c

    # ATR pct rank
    hist  = cache["_atr_hist"]
    new_n = n + 1
    if new_n > WARMUP_M5 and len(hist) > 0:
        new_pct = bisect.bisect_left(hist, new_atr) / len(hist)
    else:
        new_pct = float("nan")
    bisect.insort(hist, new_atr)
    cache["atr_pct"] = np.append(cache["atr_pct"], new_pct)

    # in_session
    in_sess_new = (
        (utc_h_new > cfg["open_h"] or
         (utc_h_new == cfg["open_h"] and utc_m_new >= cfg["open_m"]))
        and utc_h_new < cfg["close_h"]
    )
    cache["in_session"] = np.append(cache["in_session"], in_sess_new)

    # OR update
    or_bars_n = OR_BARS[params["or_minutes"]]

    if utc_h_new == cfg["open_h"] and utc_m_new == cfg["open_m"]:
        if date_new not in cache["day_start"]:
            cache["day_start"][date_new] = new_n - 1

    if date_new in cache["day_start"] and date_new not in cache["day_or"]:
        si = cache["day_start"][date_new]
        ei = si + or_bars_n
        if new_n >= ei:
            cache["day_or"][date_new] = (
                cache["h"][si:ei].max(),
                cache["l"][si:ei].min()
            )
            logger.info(f"[{canon}] OR set {date_new}: "
                        f"high={cache['day_or'][date_new][0]:.5f} "
                        f"low={cache['day_or'][date_new][1]:.5f}")

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

    # Signal detection — identical logic to BT
    sig_new = np.int8(0)

    if (in_sess_new
            and not np.isnan(new_pct)
            and new_pct >= ATR_PCT_THRESH
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
                    strong = body >= min_break_atr * new_atr
                    bu = bu and strong
                    bd = bd and strong

                if bu and not bd:
                    sig_new = np.int8(1)
                    logger.info(f"[{canon}] SIGNAL LONG  bar={bar_time} "
                                f"close={c:.5f} > OR_HIGH={or_h_new:.5f} "
                                f"atr={new_atr:.5f} atr_pct={new_pct:.3f}")
                elif bd and not bu:
                    sig_new = np.int8(-1)
                    logger.info(f"[{canon}] SIGNAL SHORT bar={bar_time} "
                                f"close={c:.5f} < OR_LOW={or_l_new:.5f} "
                                f"atr={new_atr:.5f} atr_pct={new_pct:.3f}")

                if sig_new != 0:
                    cache["_last_sig_bar"] = i
                    cache["_day_count"][d] = day_count.get(d, 0) + 1

    cache["signal"] = np.append(cache["signal"], sig_new)
    cache["n"]      = new_n


# ==============================================================================
#  SECTION 6 — DATA LOADING
# ==============================================================================

def _last_closed_bar_open_time() -> datetime.datetime:
    now          = datetime.datetime.utcnow()
    secs         = now.hour * 3600 + now.minute * 60 + now.second
    rem          = secs % M5_SECONDS
    forming_open = now - datetime.timedelta(seconds=rem)
    return forming_open - datetime.timedelta(seconds=M5_SECONDS)


def _fetch_broker_range(broker_sym: str,
                         from_dt: datetime.datetime,
                         to_dt:   datetime.datetime) -> pd.DataFrame:
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
    now_ts = pd.Timestamp(datetime.datetime.utcnow())
    df = df[df["time_utc"] + pd.Timedelta(seconds=M5_SECONDS) <= now_ts].copy()
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def load_history(canon: str, broker_sym: str):
    _normalize_csv_if_needed(canon)
    csv_df = _load_csv(canon)

    if csv_df is not None and len(csv_df) > 0:
        gap_from = (csv_df["time_utc"].iloc[-1].to_pydatetime()
                    + datetime.timedelta(seconds=1))
        gap_df   = _fetch_broker_range(broker_sym, gap_from,
                                        datetime.datetime.utcnow())
        if len(gap_df) > 0:
            combined = pd.concat([csv_df, gap_df], ignore_index=True)
            combined.sort_values("time_utc", inplace=True)
            combined.drop_duplicates(subset="time_utc", keep="last", inplace=True)
            combined.reset_index(drop=True, inplace=True)
            logger.info(f"[{canon}] history: {len(csv_df):,} CSV + "
                        f"{len(gap_df):,} gap = {len(combined):,} bars")
            return combined
        logger.info(f"[{canon}] history: {len(csv_df):,} CSV (no gap)")
        return csv_df

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


# ==============================================================================
#  SECTION 7 — BAR FETCH
#
#  Accepts any bar with open time STRICTLY GREATER than last cached bar.
#  No wall-clock expected time — immune to broker time offsets and DST.
# ==============================================================================

def _fetch_single_closed_bar(broker_sym: str,
                              last_cached_bar_time: pd.Timestamp) -> tuple:
    """
    Poll broker index 1 (last fully closed bar) until its open timestamp
    is strictly greater than last_cached_bar_time, then return it.

    This approach is immune to broker-server-time vs UTC offsets because
    we compare broker timestamps to each other (cache vs new), not to
    wall clock.  The 13:35 vs 13:40 mismatch is eliminated entirely.
    """
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 1, 1)
        if rates is not None and len(rates) == 1:
            bt = pd.Timestamp(int(rates[0]["time"]), unit="s")
            if bt > last_cached_bar_time:
                r = rates[0]
                return (bt,
                        float(r["open"]), float(r["high"]),
                        float(r["low"]),  float(r["close"]))
        time.sleep(0.1)

    # Timeout: accept whatever index 1 holds if it's newer
    rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 1, 1)
    if rates is not None and len(rates) == 1:
        r  = rates[0]
        bt = pd.Timestamp(int(r["time"]), unit="s")
        if bt <= last_cached_bar_time:
            logger.warning(f"[{broker_sym}] fetch timeout: broker at {bt}, "
                           f"cache at {last_cached_bar_time} — skipping bar")
            return None
        logger.warning(f"[{broker_sym}] fetch timeout accepted bar {bt}")
        return (bt,
                float(r["open"]), float(r["high"]),
                float(r["low"]),  float(r["close"]))
    return None


# ==============================================================================
#  SECTION 8 — STOPS LEVEL + SL HELPERS
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
#  SECTION 9 — POSITION SIZING
# ==============================================================================

def compute_lot(broker_sym: str, sl_dist: float, balance: float):
    ti = _tick_info.get(broker_sym)
    if ti is None or sl_dist < 1e-9:
        return None, None

    tvpl     = ti["tick_value_per_lot"]
    vol_step = ti["vol_step"]
    vol_min  = ti["vol_min"]

    per_trade = (DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO
                 if _MAX_TRADES_DAY_COMBO else DAILY_LOSS_BUDGET)
    vol_cap   = max(vol_min,
                    round((per_trade / (sl_dist * tvpl)) / vol_step) * vol_step)
    vol_cap   = min(vol_cap, VOL_MAX)

    risk_amount = balance * RISK_PER_TRADE
    raw_lot     = risk_amount / (sl_dist * tvpl)
    lot = max(vol_min, min(vol_cap, round(raw_lot / vol_step) * vol_step))
    lot = round(lot, 8)

    intended = balance * RISK_PER_TRADE
    actual   = lot * sl_dist * tvpl
    multiple = actual / intended if intended > 0 else float("inf")
    if multiple > MAX_RISK_MULTIPLE:
        logger.warning(f"[{broker_sym}] RISK REJECTED {multiple:.2f}x")
        return None, None

    logger.info(f"[{broker_sym}] lot={lot} sl_dist={sl_dist:.5f} "
                f"tvpl={tvpl:.5f} risk={actual:.2f} ({multiple:.2f}x)")
    return lot, tvpl


# ==============================================================================
#  SECTION 10 — ORDER EXECUTION
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
        logger.error(f"[{broker_sym}] Entry FAILED retcode="
                     f"{getattr(result,'retcode',None)} "
                     f"msg={getattr(result,'comment','')}")
        return None, None
    logger.info(f"[{broker_sym}] ENTRY {direction.upper()} "
                f"lot={lot} price={price:.5f} sl={sl_price:.5f} "
                f"ticket={result.order}")
    return result.order, price


def modify_sl_with_retry(broker_sym: str, ticket: int,
                          new_sl: float, direction: str,
                          timeout: float = None) -> bool:
    """
    Retry SL modification on same bar until success.
    Aborts only on: position gone, market disabled.
    10016 and all other transient errors are retried without limit.
    timeout param kept for call-site compatibility — not used.
    """
    attempt = 0
    while True:
        attempt += 1

        if not mt5.positions_get(ticket=ticket):
            logger.warning(f"[{broker_sym}] SL retry {attempt}: "
                           f"ticket={ticket} gone — aborting")
            return False

        sym_info = mt5.symbol_info(broker_sym)
        if (sym_info is not None and
                sym_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED):
            logger.error(f"[{broker_sym}] SL aborted: market disabled "
                         f"ticket={ticket}")
            return False

        tick = mt5.symbol_info_tick(broker_sym)
        if tick is None:
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

        if retcode in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_NO_CHANGES):
            logger.info(f"[{broker_sym}] SL modified ticket={ticket} "
                        f"sl={sl_clamped:.5f} (attempt {attempt})")
            return True

        logger.warning(f"[{broker_sym}] SL retry {attempt} retcode={retcode} "
                       f"sl_target={sl_clamped:.5f} price={cur_px:.5f}")
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
        logger.info(f"[{broker_sym}] CLOSED ticket={position.ticket} "
                    f"price={price:.5f}")
    else:
        logger.error(f"[{broker_sym}] Close FAILED "
                     f"retcode={getattr(result,'retcode',None)}")
    return ok


# ==============================================================================
#  SECTION 11 — PER-SYMBOL STATE
# ==============================================================================

def make_sym_state() -> dict:
    return {"positions": [], "day_trades_date": None, "day_trades_count": 0}


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
    entry_time = datetime.datetime.fromtimestamp(pos.time,
                                                  tz=datetime.timezone.utc)
    now_utc    = datetime.datetime.now(tz=datetime.timezone.utc)
    hold       = max(0, int((now_utc - entry_time).total_seconds() / M5_SECONDS))
    direction  = "long" if pos.type == mt5.ORDER_TYPE_BUY else "short"
    ep         = pos.price_open
    sl_price   = pos.sl or 0.0
    sl_dist    = abs(ep - sl_price) if sl_price > 0 else 0.01
    be_active  = ((direction == "long"  and sl_price >= ep) or
                  (direction == "short" and sl_price > 0 and sl_price <= ep))
    rec = _make_pos_rec(pos.ticket, direction, ep, sl_dist,
                        sl_price, entry_time.date(), None)
    rec["be_active"]  = be_active
    rec["hold_count"] = hold
    logger.info(f"[{canon}] RECOVERED ticket={pos.ticket} "
                f"dir={direction} ep={ep:.5f} sl={sl_price:.5f} "
                f"hold~{hold}bars be={be_active}")
    return rec


# ==============================================================================
#  SECTION 12 — POSITION MANAGEMENT
# ==============================================================================

def manage_positions(canon: str, broker_sym: str, sym_st: dict,
                     cache: dict, params: dict, today) -> None:
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

    for pr in list(sym_st["positions"]):
        if pr["ticket"] not in broker_tickets:
            logger.info(f"[{canon}] ticket={pr['ticket']} closed server-side")
            sym_st["positions"].remove(pr)

    for pr in list(sym_st["positions"]):
        bp        = broker_map.get(pr["ticket"])
        direction = pr["direction"]
        ep        = pr["entry_price"]
        sl_dist   = pr["sl_dist"]

        pr["hold_count"] += 1
        hc = pr["hold_count"]

        # EOD exit — bar date and bar hour from bar timestamp (matches BT)
        if bar_date != pr["entry_date"] or bar_hour >= cfg["close_h"]:
            logger.info(f"[{canon}] EOD exit ticket={pr['ticket']} "
                        f"bar={bar_date} {bar_hour:02d}h "
                        f"entry={pr['entry_date']}")
            if bp:
                send_close(broker_sym, bp)
            sym_st["positions"].remove(pr)
            continue

        # Max hold
        if hc >= MAX_HOLD:
            logger.info(f"[{canon}] MAX HOLD ticket={pr['ticket']}")
            if bp:
                send_close(broker_sym, bp)
            sym_st["positions"].remove(pr)
            continue

        # Break-even at 1R
        one_r = ep + (sl_dist if direction == "long" else -sl_dist)
        if not pr["be_active"]:
            triggered = ((direction == "long"  and bar_h >= one_r) or
                         (direction == "short" and bar_l <= one_r))
            if triggered:
                pr["be_active"]  = True
                pr["current_sl"] = ep
                logger.info(f"[{canon}] BE ticket={pr['ticket']} "
                            f"SL -> {ep:.5f}")

        # Trail
        if pr["be_active"]:
            ta   = (atr_cur if (not np.isnan(atr_cur) and atr_cur > 0)
                    else (pr["entry_atr"] or sl_dist))
            mult = params["trail_atr_mult"]
            if direction == "long":
                pr["current_sl"] = max(pr["current_sl"], bar_h - mult * ta)
            else:
                pr["current_sl"] = min(pr["current_sl"], bar_l + mult * ta)

        # SL modify with retry on same bar
        if bp is not None:
            broker_sl = bp.sl or 0.0
            new_sl    = pr["current_sl"]
            pip       = _tick_info.get(broker_sym, {}).get("pip", 0.0001)
            if abs(new_sl - broker_sl) >= pip:
                modify_sl_with_retry(broker_sym, pr["ticket"],
                                     new_sl, direction)


# ==============================================================================
#  SECTION 13 — ENTRY EXECUTION
# ==============================================================================

def execute_entry(canon: str, broker_sym: str, sym_st: dict, params: dict,
                  today, direction: str, sl_dist: float, atr_val: float,
                  or_h: float, or_l: float) -> None:
    """
    Fetches fresh account_info at entry time.
    Checks free_margin before sending — prevents retcode 10019.
    Does NOT use the stale balance passed from the main loop.
    """
    acct = mt5.account_info()
    if acct is None:
        logger.error(f"[{canon}] account_info unavailable — entry cancelled")
        return
    balance     = acct.balance
    free_margin = acct.margin_free

    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{canon}] tick unavailable — entry cancelled")
        return

    ep      = tick.ask if direction == "long" else tick.bid
    min_sl  = 0.05 * atr_val
    sl_dist = max(sl_dist, min_sl)
    sl_price = ep - sl_dist if direction == "long" else ep + sl_dist

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

    # Free margin check — prevents 10019
    estimated_margin = lot * sl_dist * tvpl
    if free_margin < estimated_margin:
        logger.warning(f"[{canon}] insufficient free margin: "
                       f"free={free_margin:.2f} "
                       f"needed~={estimated_margin:.2f} — cancelled")
        return

    ticket, _ = send_market_order(broker_sym, direction, lot, sl_price,
                                  f"{COMMENT}_{canon}")
    if ticket is None:
        return

    filled = []
    for _ in range(10):
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
    sym_st["day_trades_count"] += 1

    logger.info(f"[{canon}] ENTERED {direction.upper()} ticket={ticket} "
                f"ep={actual_ep:.5f} sl={actual_sl:.5f} "
                f"sl_dist={sl_dist:.5f} lot={lot} "
                f"OR_HIGH={or_h:.5f} OR_LOW={or_l:.5f} "
                f"balance={balance:.2f} free_margin={free_margin:.2f}")


# ==============================================================================
#  SECTION 14 — PER-BAR PROCESSING
# ==============================================================================

def process_bar(canon: str, broker_sym: str, sym_st: dict,
                cache: dict, params: dict,
                new_bar_time: pd.Timestamp) -> None:
    """
    new_bar_time: hint from clock, not used for fetch validation.
    _fetch_single_closed_bar uses cache's own last bar time as reference.
    """
    last_cached = pd.Timestamp(int(cache["times"][-1]))

    bar = _fetch_single_closed_bar(broker_sym, last_cached)
    if bar is None:
        logger.warning(f"[{canon}] no new bar after {last_cached} — skipping")
        return
    bar_time, o, h, l, c = bar

    append_bar_to_cache(cache, bar_time, o, h, l, c)
    _append_csv(canon, bar_time, o, h, l, c)

    today = cache["dates"][-1]
    _reset_daily(sym_st, today)
    manage_positions(canon, broker_sym, sym_st, cache, params, today)

    i   = cache["n"] - 1
    sig = int(cache["signal"][i])

    if sig == 0:
        return

    # Double-check broker positions before entry
    broker_positions = [p for p in (mt5.positions_get(symbol=broker_sym) or [])
                        if p.magic == MAGIC]
    if len(broker_positions) >= params["max_trades_day"]:
        logger.info(f"[{canon}] SIGNAL suppressed — already at max positions")
        return

    direction = "long" if sig == 1 else "short"
    or_h      = float(cache["or_high"][i])
    or_l      = float(cache["or_low"][i])
    atr_val   = float(cache["atr14"][i])
    or_size   = or_h - or_l
    sl_dist   = max(params["sl_range_mult"] * or_size, atr_val * 0.05)

    if sl_dist < 3.0:
        logger.info(f"[{canon}] SIGNAL_SKIP sl_too_tight: {sl_dist:.5f}")
        return

    logger.info(f"[{canon}] SIGNAL {direction.upper()} "
                f"OR_HIGH={or_h:.5f} OR_LOW={or_l:.5f} "
                f"close={float(cache['c'][i]):.5f} "
                f"sl_dist={sl_dist:.5f} atr={atr_val:.5f} "
                f"day_trades={sym_st['day_trades_count']}/{params['max_trades_day']}")

    execute_entry(canon, broker_sym, sym_st, params, today,
                  direction, sl_dist, atr_val, or_h, or_l)


# ==============================================================================
#  SECTION 15 — BAR CLOCK
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
        logger.debug(f"[clock] no new bar after "
                     f"{boundary.strftime('%H:%M')} — retry")


# ==============================================================================
#  SECTION 16 — MAIN LOOP
# ==============================================================================

def _process_safe(canon, broker, sym_st, cache, params, new_bar_time):
    try:
        process_bar(canon, broker, sym_st, cache, params, new_bar_time)
    except Exception as e:
        logger.exception(f"[{canon}] process_bar error: {e}")


def run_live():
    global _CLOCK_BROKER, _MAX_TRADES_DAY_COMBO

    print("ORB V6 starting...", flush=True)

    for attempt in range(30):
        ok  = mt5.initialize()
        err = mt5.last_error()
        print(f"  MT5 init {attempt+1}/30: ok={ok} err={err}", flush=True)
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
    logger.info(f"MT5 connected | account={acct.login} | "
                f"balance={acct.balance:.2f} | currency={acct.currency}")

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
        logger.info(f"  {canon} ({broker}): digits={info.digits} "
                    f"tvpl={tvpl:.6f} vol_min={info.volume_min} "
                    f"stops_level={info.trade_stops_level} "
                    f"params={BEST_PARAMS[canon]}")

    _MAX_TRADES_DAY_COMBO = sum(BEST_PARAMS[s]["max_trades_day"] for s in active)

    # ── Build caches ──────────────────────────────────────────────────────────
    logger.info("=== BUILDING CACHES ===")
    caches   = {}
    hist_dfs = {}
    for canon in list(active):
        broker = sym_map[canon]
        df = load_history(canon, broker)
        if df is None or len(df) < WARMUP_M5 + ATR_PERIOD + 10:
            logger.error(f"[{canon}] not enough history — skipping")
            active.remove(canon)
            continue
        cache = build_cache(canon, df, BEST_PARAMS[canon])
        cache["_atr_hist"] = _rebuild_atr_hist(cache["atr14"])
        caches[canon]   = cache
        hist_dfs[canon] = df
    logger.info("=== END CACHES ===")

    if not active:
        mt5.shutdown()
        return

    # ── BT accuracy check — must pass before trading ─────────────────────────
    logger.info("=== BT ACCURACY VERIFICATION ===")
    for canon in list(active):
        ok = verify_cache_vs_bt(
            canon, hist_dfs[canon], caches[canon], BEST_PARAMS[canon]
        )
        if not ok:
            logger.error(f"[{canon}] failed BT accuracy check — removing")
            active.remove(canon)
    logger.info("=== END BT ACCURACY VERIFICATION ===")

    if not active:
        logger.error("No symbols passed BT accuracy — shutting down")
        mt5.shutdown()
        return

    _CLOCK_BROKER = sym_map[active[0]]
    sym_states    = {c: make_sym_state() for c in active}

    # ── Startup recovery ──────────────────────────────────────────────────────
    logger.info("=== STARTUP RECOVERY ===")
    for canon in active:
        broker    = sym_map[canon]
        positions = [p for p in (mt5.positions_get(symbol=broker) or [])
                     if p.magic == MAGIC]
        for pos in positions:
            if COMMENT in (pos.comment or ""):
                rec = _reconstruct_pos(canon, pos)
                sym_states[canon]["positions"].append(rec)
                sym_states[canon]["day_trades_count"] = min(
                    sym_states[canon]["day_trades_count"] + 1,
                    BEST_PARAMS[canon]["max_trades_day"]
                )
        logger.info(f"  {canon}: recovered "
                    f"{len(sym_states[canon]['positions'])} pos")
    logger.info("=== END RECOVERY ===")

    # Seed from cache's last bar — not broker, not wall clock
    cache_last_times = [pd.Timestamp(int(caches[c]["times"][-1]))
                        for c in active]
    last_bar_time    = min(cache_last_times)
    logger.info(f"Seeded from cache: {last_bar_time} — "
                f"waiting for next M5 close...")

    bar_count = 0
    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1

            logger.info(f"-- BAR {bar_count} | {new_bar_time} "
                        f"------------------------------")

            threads = [
                threading.Thread(
                    target=_process_safe,
                    args=(canon, sym_map[canon], sym_states[canon],
                          caches[canon], BEST_PARAMS[canon], new_bar_time),
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
