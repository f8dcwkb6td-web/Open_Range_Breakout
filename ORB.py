import sys
print("SCRIPT STARTED", flush=True)
sys.stdout.flush()
"""
==============================================================================
ORB  —  OPENING RANGE BREAKOUT  |  LIVE ENGINE v4  (M5)
==============================================================================
SYMBOLS:  US30, GER40

FIXES vs previous version:
  + Bar clock: precision sleep-to-boundary + aggressive poll — no missed ticks
  + Gap fill:  on every bar, fetches last_bar_time→now via copy_rates_range
               and appends every missing bar in sequence before processing
  + ATR:       incremental atr_wilder_update called per bar in gap fill so
               the Wilder chain stays valid across weekend / missed bars
  + Weekend gap detection: logs gap size and fills automatically

PATCH (this version):
  + Error 100016 fix: fetch stops_level from symbol_info before every SL
    modify and every entry — clamp SL to respect broker minimum distance
    from current price.  stops_level of 0 is treated as a safe fallback
    of 5 * pip so we never send a zero-distance SL.
  + Signal rejection logging: every early-return path in
    detect_signal_last_bar now logs WHY it rejected, so missed trades
    (like US30 not firing today) are diagnosable from the log.
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
TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"
LOGIN    = 1513214612
PASSWORD = "h2QE?*1!v5fQ"
SERVER   = "FTMO-Demo"

# ── Engine identity ───────────────────────────────────────────────────────────
MAGIC   = 202603262
COMMENT = "ORB_V4"

# ── Broker / risk constants ───────────────────────────────────────────────────
STARTING_BALANCE  = 25_000.0
RISK_PER_TRADE    = 0.01
MAX_RISK_MULTIPLE = 2.0

DAILY_LOSS_CAP_PCT = 0.0475
DAILY_LOSS_BUDGET  = STARTING_BALANCE * DAILY_LOSS_CAP_PCT   # $1,187.50

# ── Strategy constants ────────────────────────────────────────────────────────
FETCH_BARS_STARTUP = 99_999
CACHE_MAX_BARS     = 500_000
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
    raise ValueError(f"ACTIVE_PARAMS must be 'NEW'/'OLD'/'GRID', got '{ACTIVE_PARAMS}'")
BEST_PARAMS = _PARAM_MAP[ACTIVE_PARAMS]

_MAX_TRADES_DAY_COMBO = 0

SYMBOL_ALIASES = {
    "US30":  ["US30.cash", "US30",  "DJ30",    "DJIA",   "WS30",   "DOW30",  "US30Cash"],
    "GER40": ["GER40.cash","GER40", "DAX40",   "DAX",    "GER30",  "DE40",   "GER40Cash"],
}

SYMBOLS = list(SESSION.keys())

_tick_value_cache: dict = {}
_bar_cache: dict = {}


# ==============================================================================
#  SECTION 0 — STOPS LEVEL HELPER  (NEW — fixes error 100016)
# ==============================================================================

def get_min_sl_distance(broker_sym: str) -> float:
    """
    Return the broker's minimum SL distance in price units for broker_sym.

    MT5 gives stops_level in POINTS (integer).  Convert to price via:
        min_dist = stops_level * point

    If stops_level == 0 (broker reports no minimum) we fall back to
    5 * point so we never send a zero-distance SL.

    We also apply a hard floor of 5 * pip from _tick_value_cache so even
    if symbol_info is briefly unavailable the function returns something safe.
    """
    cached_pip = _tick_value_cache.get(broker_sym, {}).get("pip", 0.0001)
    fallback   = 5.0 * cached_pip

    info = mt5.symbol_info(broker_sym)
    if info is None:
        logger.warning(
            f"[{broker_sym}] get_min_sl_distance: symbol_info None "
            f"— using fallback {fallback:.6f}"
        )
        return fallback

    point = info.point if info.point > 0 else cached_pip
    sl    = int(info.trade_stops_level or 0)

    if sl <= 0:
        # Broker says 0 — use a safe minimum of 5 points
        min_dist = max(5.0 * point, fallback)
        logger.debug(
            f"[{broker_sym}] stops_level=0 "
            f"— using safe floor {min_dist:.6f}"
        )
    else:
        min_dist = sl * point

    logger.debug(
        f"[{broker_sym}] stops_level={sl} point={point:.8f} "
        f"min_sl_dist={min_dist:.6f}"
    )
    return min_dist


def clamp_sl_to_stops_level(
    broker_sym: str,
    direction: str,
    current_price: float,
    sl_price: float,
) -> float:
    """
    Ensure sl_price respects the broker minimum stop distance from
    current_price.  Adjusts away from price if too close.

    direction='long'  → SL must be BELOW price by at least min_dist
    direction='short' → SL must be ABOVE price by at least min_dist
    """
    min_dist = get_min_sl_distance(broker_sym)

    if direction == "long":
        max_allowed = current_price - min_dist
        if sl_price > max_allowed:
            logger.info(
                f"[{broker_sym}] SL clamped (long): "
                f"{sl_price:.5f} -> {max_allowed:.5f} "
                f"(price={current_price:.5f} min_dist={min_dist:.5f})"
            )
            return max_allowed
    else:  # short
        min_allowed = current_price + min_dist
        if sl_price < min_allowed:
            logger.info(
                f"[{broker_sym}] SL clamped (short): "
                f"{sl_price:.5f} -> {min_allowed:.5f} "
                f"(price={current_price:.5f} min_dist={min_dist:.5f})"
            )
            return min_allowed

    return sl_price


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
    tr = max(new_h - new_l,
             abs(new_h - prev_c),
             abs(new_l - prev_c))
    k  = 1.0 / ATR_PERIOD
    return prev_atr * (1.0 - k) + tr * k


def expanding_pct_rank_full(arr):
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
    return out, hist


def expanding_pct_rank_update(hist, new_atr_val):
    if np.isnan(new_atr_val):
        return np.nan, hist
    rank = bisect.bisect_left(hist, new_atr_val) / len(hist) if hist else np.nan
    bisect.insort(hist, new_atr_val)
    return rank, hist


# ==============================================================================
#  SECTION 3 — STARTUP DATA LOAD
# ==============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def _find_csv(filename):
    for d in (SCRIPT_DIR, os.getcwd()):
        p = os.path.join(d, filename)
        if os.path.isfile(p):
            return p
    return os.path.join(SCRIPT_DIR, filename)

CSV_FILES = {
    "US30":  _find_csv("US30.cash.csv"),
    "GER40": _find_csv("GER40.cash.csv"),
}


def _df_add_derived_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["utc_hour"]   = df["time_utc"].dt.hour
    df["utc_minute"] = df["time_utc"].dt.minute
    df["date"]       = df["time_utc"].dt.date
    return df


def _parse_csv(csv_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path, sep="\t")
        if len(df.columns) < 4:
            df = pd.read_csv(csv_path)
    except Exception:
        df = pd.read_csv(csv_path)

    def _clean_col(c):
        c = c.strip().lower().replace("<", "").replace(">", "")
        if c.startswith("t") and c[1:] in (
            "date", "time", "open", "high", "low", "close",
            "tickvol", "vol", "spread"
        ):
            c = c[1:]
        return c

    df.columns = [_clean_col(c) for c in df.columns]

    if "date" in df.columns and "time" in df.columns:
        combined = df["date"].astype(str).str.strip() + " " + df["time"].astype(str).str.strip()
        fixed = combined.str.replace(
            r"(\d{4})\.(\d{2})\.(\d{2})",
            lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}",
            regex=True
        )
        df["time_utc"] = pd.to_datetime(fixed)
        logger.info(f"  CSV: combined date+time columns, first bar: {df['time_utc'].iloc[0]}")

    elif "time" in df.columns:
        if pd.api.types.is_numeric_dtype(df["time"]):
            df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
        else:
            sample = str(df["time"].iloc[0]).strip()
            if "." in sample.split(" ")[0]:
                fixed = df["time"].astype(str).str.replace(
                    r"(\d{4})\.(\d{2})\.(\d{2})",
                    lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}",
                    regex=True
                )
                df["time_utc"] = pd.to_datetime(fixed)
            else:
                df["time_utc"] = pd.to_datetime(df["time"].astype(str))
        if hasattr(df["time_utc"].dt, "tz") and df["time_utc"].dt.tz is not None:
            df["time_utc"] = df["time_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    else:
        raise ValueError(f"CSV has no usable time column. Columns found: {list(df.columns)}")

    df = df[["time_utc", "open", "high", "low", "close"]].copy()
    df["open"]  = pd.to_numeric(df["open"],  errors="coerce").astype(np.float64)
    df["high"]  = pd.to_numeric(df["high"],  errors="coerce").astype(np.float64)
    df["low"]   = pd.to_numeric(df["low"],   errors="coerce").astype(np.float64)
    df["close"] = pd.to_numeric(df["close"], errors="coerce").astype(np.float64)

    df.dropna(inplace=True)
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = _df_add_derived_cols(df)
    return df


def _fetch_gap_df(broker_sym: str, from_dt: datetime.datetime) -> pd.DataFrame:
    to_dt = datetime.datetime.utcnow()
    logger.info(
        f"[{broker_sym}] gap fetch: "
        f"{from_dt.strftime('%Y-%m-%d %H:%M')} UTC -> now"
    )
    rates = mt5.copy_rates_range(broker_sym, mt5.TIMEFRAME_M5, from_dt, to_dt)
    if rates is None or len(rates) == 0:
        logger.warning(
            f"[{broker_sym}] gap fetch returned 0 bars  "
            f"MT5 error: {mt5.last_error()}"
        )
        return pd.DataFrame()

    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[
        ["time", "open", "high", "low", "close"]
    ].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df.drop(columns=["time"], inplace=True)
    df.sort_values("time_utc", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = _df_add_derived_cols(df)
    logger.info(f"[{broker_sym}] gap fetch: {len(df):,} bars received")
    return df


def _finalise_df(df: pd.DataFrame, broker_sym: str):
    df = df.iloc[:-1].reset_index(drop=True)

    if len(df) > CACHE_MAX_BARS:
        logger.info(
            f"[{broker_sym}] trimming {len(df):,} -> {CACHE_MAX_BARS:,} bars "
            f"(keeping newest)"
        )
        df = df.iloc[-CACHE_MAX_BARS:].reset_index(drop=True)

    min_needed = WARMUP_M5 + ATR_PERIOD + 50
    if len(df) < min_needed:
        logger.error(
            f"[{broker_sym}] only {len(df):,} bars after assembly — "
            f"need at least {min_needed:,} — cannot trade"
        )
        return None

    logger.info(
        f"[{broker_sym}] cache range: "
        f"{df['time_utc'].iloc[0].strftime('%Y-%m-%d')} -> "
        f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')} "
        f"({len(df):,} closed bars)"
    )
    return df


def load_csv_or_fetch(canon: str, broker_sym: str):
    csv_path = CSV_FILES.get(canon, "")

    logger.info(f"[{canon}] data load start | script_dir={SCRIPT_DIR} | cwd={os.getcwd()}")
    logger.info(f"[{canon}] CSV path resolved to: {csv_path} | exists={os.path.isfile(csv_path)}")

    if csv_path and os.path.isfile(csv_path):
        try:
            csv_df = _parse_csv(csv_path)
            logger.info(
                f"[{canon}] CSV parsed OK: {len(csv_df):,} bars  "
                f"{csv_df['time_utc'].iloc[0].strftime('%Y-%m-%d')} -> "
                f"{csv_df['time_utc'].iloc[-1].strftime('%Y-%m-%d %H:%M')} UTC"
            )
        except Exception as e:
            logger.error(f"[{canon}] CSV parse FAILED: {e}")
            logger.info(f"[{canon}] falling back to broker fetch")
            return fetch_m5_full(broker_sym)

        if len(csv_df) == 0:
            logger.warning(f"[{canon}] CSV parsed but empty — falling back to broker fetch")
            return fetch_m5_full(broker_sym)

        gap_from = csv_df["time_utc"].iloc[-1].to_pydatetime()
        gap_df   = _fetch_gap_df(broker_sym, gap_from)

        if len(gap_df) > 0:
            combined = pd.concat([csv_df, gap_df], ignore_index=True)
            combined.sort_values("time_utc", inplace=True)
            combined.drop_duplicates(subset="time_utc", keep="last", inplace=True)
            combined.reset_index(drop=True, inplace=True)
            logger.info(
                f"[{canon}] combined: {len(csv_df):,} CSV + "
                f"{len(gap_df):,} gap = {len(combined):,} bars"
            )
        else:
            combined = csv_df.copy()
            logger.info(f"[{canon}] no gap bars — using CSV only ({len(combined):,} bars)")

        result = _finalise_df(combined, broker_sym)
        if result is None:
            logger.warning(f"[{canon}] CSV+gap finalise failed — falling back to broker fetch")
            return fetch_m5_full(broker_sym)
        return result

    logger.info(f"[{canon}] no CSV — broker fetch (target {FETCH_BARS_STARTUP:,} bars)")
    return fetch_m5_full(broker_sym)


def fetch_m5_full(broker_sym):
    info = mt5.symbol_info(broker_sym)
    if info is None:
        logger.error(f"[{broker_sym}] symbol_info returned None — MT5 error: {mt5.last_error()}")
        return None

    if not info.visible:
        logger.info(f"[{broker_sym}] not visible — selecting...")
        if not mt5.symbol_select(broker_sym, True):
            logger.error(f"[{broker_sym}] symbol_select failed.  MT5 error: {mt5.last_error()}")
            return None
        time.sleep(3)

    logger.info(f"[{broker_sym}] pre-warming history (copy_rates_range 2010->now)...")
    seed_from = datetime.datetime(2010, 1, 1)
    seed_to   = datetime.datetime.utcnow()
    rates_probe = mt5.copy_rates_range(broker_sym, mt5.TIMEFRAME_M5, seed_from, seed_to)
    n_probe = len(rates_probe) if rates_probe is not None else 0
    logger.info(f"[{broker_sym}] pre-warm returned {n_probe:,} bars — sleeping 15s...")
    time.sleep(15)

    attempts = [FETCH_BARS_STARTUP, 500_000, 300_000, 200_000, 100_000, 50_000]
    seen = set()
    attempts = [x for x in attempts if not (x in seen or seen.add(x))]
    rates = None

    for n_bars in attempts:
        rates = mt5.copy_rates_from_pos(broker_sym, mt5.TIMEFRAME_M5, 0, n_bars + 1)
        n_got = len(rates) if rates is not None else 0
        err   = mt5.last_error()

        if rates is not None and n_got >= WARMUP_M5 + ATR_PERIOD + 50:
            oldest_ts = pd.Timestamp(rates[0]["time"], unit="s")
            days_back = (pd.Timestamp.utcnow() - oldest_ts).days
            logger.info(
                f"[{broker_sym}] fetch {n_bars:,}: got {n_got:,} bars | "
                f"oldest={oldest_ts.date()} ({days_back}d back)"
            )
            if n_got < int(n_bars * 0.8):
                logger.warning(
                    f"[{broker_sym}] got {n_got:,}/{n_bars:,} bars — "
                    f"using them anyway (terminal may still be syncing)"
                )
            break
        else:
            logger.warning(
                f"[{broker_sym}] fetch attempt {n_bars:,} bars -> "
                f"got {n_got} bars  MT5 error: {err}"
            )
            time.sleep(2)

    if rates is None or len(rates) < WARMUP_M5 + ATR_PERIOD + 50:
        logger.error(f"[{broker_sym}] all fetch attempts failed.  Last MT5 error: {mt5.last_error()}")
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
    df = df.iloc[:-1].reset_index(drop=True)

    if len(df) > CACHE_MAX_BARS:
        logger.info(f"[{broker_sym}] trimming {len(df):,} -> {CACHE_MAX_BARS:,} bars (keeping newest)")
        df = df.iloc[-CACHE_MAX_BARS:].reset_index(drop=True)

    logger.info(
        f"[{broker_sym}] cache range: "
        f"{df['time_utc'].iloc[0].strftime('%Y-%m-%d')} -> "
        f"{df['time_utc'].iloc[-1].strftime('%Y-%m-%d')} "
        f"({len(df):,} closed bars)"
    )
    return df


def build_bar_cache(canon, df):
    o = df["open"].values.astype(np.float64)
    h = df["high"].values.astype(np.float64)
    l = df["low"].values.astype(np.float64)
    c = df["close"].values.astype(np.float64)

    atr14         = atr_wilder_full(h, l, c)
    atr_pct, hist = expanding_pct_rank_full(atr14)

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
        "atr_pct_hist":    hist,
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

def _trim_cache(cache):
    n = cache["n"]
    if n <= CACHE_MAX_BARS:
        return
    drop = n - CACHE_MAX_BARS
    for key in ("o", "h", "l", "c", "utc_h", "utc_m",
                "dates", "times", "atr14", "atr_pct"):
        cache[key] = cache[key][drop:]
    cache["n"] = CACHE_MAX_BARS


def append_bar_to_cache(canon, bar_time, o, h, l, c):
    cache = _bar_cache[canon]

    if bar_time <= cache["last_bar_time"]:
        return False

    dt    = bar_time.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
    utc_h = np.int32(dt.hour)
    utc_m = np.int32(dt.minute)
    date  = dt.date()
    ts_ns = np.datetime64(bar_time.value, "ns")

    prev_c  = cache["c"][-1]
    new_atr = atr_wilder_update(cache["atr_wilder_prev"], h, l, prev_c)
    new_pct, hist = expanding_pct_rank_update(cache["atr_pct_hist"], new_atr)

    cache["o"]       = np.append(cache["o"],      o)
    cache["h"]       = np.append(cache["h"],      h)
    cache["l"]       = np.append(cache["l"],      l)
    cache["c"]       = np.append(cache["c"],      c)
    cache["utc_h"]   = np.append(cache["utc_h"],  utc_h)
    cache["utc_m"]   = np.append(cache["utc_m"],  utc_m)
    cache["dates"]   = np.append(cache["dates"],  date)
    cache["times"]   = np.append(cache["times"],  ts_ns)
    cache["atr14"]   = np.append(cache["atr14"],  new_atr)
    cache["atr_pct"] = np.append(cache["atr_pct"], new_pct)

    cache["atr_wilder_prev"] = new_atr
    cache["atr_pct_hist"]    = hist
    cache["last_bar_time"]   = bar_time
    cache["n"]               = cache["n"] + 1

    _trim_cache(cache)
    return True


# ==============================================================================
#  SECTION 4b — GAP FILL
# ==============================================================================

def fill_gap_for_symbol(canon: str, broker_sym: str) -> int:
    cache   = _bar_cache[canon]
    now_utc = datetime.datetime.utcnow()

    from_dt = cache["last_bar_time"].to_pydatetime() + datetime.timedelta(seconds=1)
    to_dt   = now_utc

    rates = mt5.copy_rates_range(broker_sym, mt5.TIMEFRAME_M5, from_dt, to_dt)
    if rates is None or len(rates) == 0:
        return 0

    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[["time", "open", "high", "low", "close"]].copy()
    df["time_utc"] = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df.sort_values("time_utc", inplace=True)
    df.drop_duplicates(subset="time_utc", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)

    live_cutoff = pd.Timestamp(now_utc) - pd.Timedelta(minutes=5)
    df = df[df["time_utc"] <= live_cutoff].copy()

    if len(df) == 0:
        return 0

    appended   = 0
    gap_warned = False

    for row in df.itertuples(index=False):
        bar_time = pd.Timestamp(row.time_utc)

        if not gap_warned and appended == 0:
            gap = bar_time - cache["last_bar_time"]
            if gap > pd.Timedelta(minutes=6):
                logger.warning(
                    f"[{canon}] GAP DETECTED: {gap} "
                    f"(cache_end={cache['last_bar_time']} "
                    f"first_new={bar_time}) — filling {len(df)} bars..."
                )
                gap_warned = True

        added = append_bar_to_cache(
            canon, bar_time,
            float(row.open), float(row.high),
            float(row.low),  float(row.close)
        )
        if added:
            appended += 1

    if appended > 0:
        logger.info(
            f"[{canon}] gap fill: {appended} bar(s) appended  "
            f"cache now ends {cache['last_bar_time']} UTC"
        )

    return appended


# ==============================================================================
#  SECTION 5 — OR COMPUTATION
# ==============================================================================

def compute_or_from_cache(canon, or_bars):
    cache      = _bar_cache[canon]
    cfg        = SESSION[canon]
    n          = cache["n"]
    h          = cache["h"]
    l_arr      = cache["l"]
    utc_h      = cache["utc_h"]
    utc_m      = cache["utc_m"]
    dates      = cache["dates"]

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
#  SECTION 6 — SIGNAL DETECTION  (PATCHED: detailed rejection logging)
# ==============================================================================

def detect_signal_last_bar(canon, params, bars_since_last, day_trades_today):
    cache = _bar_cache[canon]
    n     = cache["n"]
    i     = n - 1

    if i < WARMUP_M5:
        logger.debug(
            f"[{canon}] SIGNAL_SKIP warmup: bar_idx={i} < WARMUP_M5={WARMUP_M5}"
        )
        return None, None, None, None, None, None

    atr_pct_i = cache["atr_pct"][i]
    if np.isnan(atr_pct_i) or atr_pct_i < ATR_PCT_THRESH:
        logger.info(
            f"[{canon}] SIGNAL_SKIP atr_pct: "
            f"atr_pct={atr_pct_i:.4f} < threshold={ATR_PCT_THRESH} "
            f"(atr={cache['atr14'][i]:.5f})"
        )
        return None, None, None, None, None, None

    cfg     = SESSION[canon]
    utc_h_i = int(cache["utc_h"][i])
    utc_m_i = int(cache["utc_m"][i])
    in_sess = (
        (utc_h_i > cfg["open_h"] or
         (utc_h_i == cfg["open_h"] and utc_m_i >= cfg["open_m"]))
        and utc_h_i < cfg["close_h"]
    )
    if not in_sess:
        logger.debug(
            f"[{canon}] SIGNAL_SKIP session: "
            f"bar_utc={utc_h_i:02d}:{utc_m_i:02d} "
            f"session={cfg['open_h']:02d}:{cfg['open_m']:02d}-"
            f"{cfg['close_h']:02d}:00"
        )
        return None, None, None, None, None, None

    or_bars_n = OR_BARS[params["or_minutes"]]
    or_high_arr, or_low_arr, _ = compute_or_from_cache(canon, or_bars_n)

    if np.isnan(or_high_arr[i]) or np.isnan(or_low_arr[i]):
        logger.info(
            f"[{canon}] SIGNAL_SKIP or_not_ready: "
            f"bar_utc={utc_h_i:02d}:{utc_m_i:02d} "
            f"or_high={'NaN' if np.isnan(or_high_arr[i]) else f'{or_high_arr[i]:.5f}'} "
            f"or_low={'NaN' if np.isnan(or_low_arr[i]) else f'{or_low_arr[i]:.5f}'} "
            f"(or_minutes={params['or_minutes']} or_bars={or_bars_n})"
        )
        return None, None, None, None, None, None

    if bars_since_last < params["cooldown_bars"]:
        logger.info(
            f"[{canon}] SIGNAL_SKIP cooldown: "
            f"bars_since_last={bars_since_last} < cooldown={params['cooldown_bars']}"
        )
        return None, None, None, None, None, None

    if day_trades_today >= params["max_trades_day"]:
        logger.info(
            f"[{canon}] SIGNAL_SKIP max_trades: "
            f"day_trades={day_trades_today} >= max={params['max_trades_day']}"
        )
        return None, None, None, None, None, None

    atr_val = cache["atr14"][i]
    if np.isnan(atr_val) or atr_val <= 0:
        logger.info(
            f"[{canon}] SIGNAL_SKIP atr_invalid: atr={atr_val}"
        )
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

    # Log the full signal evaluation when inside session — always useful
    logger.info(
        f"[{canon}] SIGNAL_EVAL "
        f"bar={utc_h_i:02d}:{utc_m_i:02d} "
        f"c={c_cur:.5f} o={o_cur:.5f} body={body:.5f} "
        f"or_high={or_high_arr[i]:.5f} or_low={or_low_arr[i]:.5f} "
        f"or_size={or_size:.5f} atr={atr_val:.5f} "
        f"min_break_atr={params['min_break_atr']} "
        f"breaks_up={breaks_up} breaks_down={breaks_down}"
    )

    if breaks_up and not breaks_down:
        return "long",  or_high_arr[i], or_low_arr[i], atr_val, or_size, sl_dist
    if breaks_down and not breaks_up:
        return "short", or_high_arr[i], or_low_arr[i], atr_val, or_size, sl_dist

    # Neither condition met — log the exact gap to breakout
    logger.info(
        f"[{canon}] SIGNAL_SKIP no_breakout: "
        f"c={c_cur:.5f}  "
        f"dist_to_or_high={or_high_arr[i]-c_cur:+.5f}  "
        f"dist_to_or_low={c_cur-or_low_arr[i]:+.5f}  "
        f"body={body:.5f} min_body={params['min_break_atr']*atr_val:.5f}"
    )
    return None, None, None, None, None, None


# ==============================================================================
#  SECTION 7 — POSITION SIZING
# ==============================================================================

def compute_vol_max_cap(sl_dist, tick_value_per_lot):
    global _MAX_TRADES_DAY_COMBO
    if sl_dist < 1e-9 or tick_value_per_lot <= 0 or _MAX_TRADES_DAY_COMBO == 0:
        return 250.0
    per_trade = DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO
    cached    = list(_tick_value_cache.values())[0]
    vol_step  = cached["vol_step"]
    vol_min   = cached["vol_min"]
    raw       = per_trade / (sl_dist * tick_value_per_lot)
    cap       = max(vol_min, round(raw / vol_step) * vol_step)
    return round(cap, 8)


def compute_lot_size(broker_sym, sl_dist, balance):
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

    vol_max_cap = compute_vol_max_cap(sl_dist, tvpl)
    risk_amount = balance * RISK_PER_TRADE
    raw_lot     = risk_amount / (sl_dist * tvpl)
    lot         = max(vol_min, min(vol_max_cap,
                                   round(raw_lot / vol_step) * vol_step))
    lot         = round(lot, 8)

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
    intended = balance * RISK_PER_TRADE
    actual   = lot * sl_dist * tvpl
    multiple = actual / intended if intended > 0 else float("inf")
    status   = "OK" if multiple <= MAX_RISK_MULTIPLE else "REJECTED"

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
#  SECTION 8 — ORDER EXECUTION  (PATCHED: stops_level clamping on entry + modify)
# ==============================================================================

def send_market_order(broker_sym, direction, lot, sl_price, comment):
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{broker_sym}] Tick unavailable")
        return None, None
    price = tick.ask if direction == "long" else tick.bid
    otype = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL

    # ── PATCH: clamp SL to broker stops_level before sending ─────────────────
    sl_price = clamp_sl_to_stops_level(broker_sym, direction, price, sl_price)
    # ─────────────────────────────────────────────────────────────────────────

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


def modify_sl(broker_sym, ticket, new_sl, direction: str, current_price: float):
    """
    PATCHED: accepts direction + current_price so we can clamp new_sl
    to the broker's stops_level before sending.  Callers updated below.
    """
    # ── PATCH: clamp to stops_level ──────────────────────────────────────────
    new_sl = clamp_sl_to_stops_level(broker_sym, direction, current_price, new_sl)
    # ─────────────────────────────────────────────────────────────────────────

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
            f"new_sl={new_sl:.5f} current_price={current_price:.5f}"
        )
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
        ticket=position.ticket, direction=direction, entry_price=ep,
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
    cache = _bar_cache[canon]
    today = cache["dates"][-1]

    _reset_daily_counter(sym_st, today)
    sym_st["bars_since_last"] += 1

    broker_positions = mt5.positions_get(symbol=broker_sym) or []
    broker_positions = [p for p in broker_positions if p.magic == MAGIC]
    broker_tickets   = {p.ticket for p in broker_positions}

    known_tickets = {pr["ticket"] for pr in sym_st["positions"]}
    for bp in broker_positions:
        if bp.ticket not in known_tickets:
            logger.warning(f"[{canon}] Desync: unknown ticket={bp.ticket} — recovering")
            rec = _reconstruct_position_record(canon, bp)
            sym_st["positions"].append(rec)
            sym_st["day_trades_count"] = min(
                sym_st["day_trades_count"] + 1, params["max_trades_day"]
            )

    closed_records = [pr for pr in sym_st["positions"]
                      if pr["ticket"] not in broker_tickets]
    for pr in closed_records:
        logger.info(f"[{canon}] ticket={pr['ticket']} closed server-side (SL hit)")
        _log_close_record(canon, pr)
        sym_st["positions"].remove(pr)

    broker_pos_map = {p.ticket: p for p in broker_positions}
    for pr in list(sym_st["positions"]):
        bp = broker_pos_map.get(pr["ticket"])
        _manage_position_record(canon, broker_sym, pr, params, cache,
                                bp, today, sym_st)

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

    # ── PATCH: clamp SL to broker stops_level before lot sizing ──────────────
    # We clamp here (pre-lot) AND inside send_market_order (post-price).
    # Pre-lot clamp ensures sl_dist used for sizing is consistent with what
    # will actually be sent.
    sl_price_clamped = clamp_sl_to_stops_level(broker_sym, direction, ep, sl_price)
    if sl_price_clamped != sl_price:
        sl_dist = abs(ep - sl_price_clamped)
        sl_price = sl_price_clamped
        logger.info(
            f"[{canon}] entry SL clamped to stops_level: "
            f"sl_dist adjusted to {sl_dist:.5f}"
        )
    # ─────────────────────────────────────────────────────────────────────────

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

    if pr["be_active"]:
        ta = atr_cur if (not np.isnan(atr_cur) and atr_cur > 0) \
             else (pr["entry_atr"] or sl_dist)
        trail_mult = params["trail_atr_mult"]
        if direction == "long":
            pr["current_sl"] = max(pr["current_sl"], bar_h - trail_mult * ta)
        else:
            pr["current_sl"] = min(pr["current_sl"], bar_l + trail_mult * ta)

    if broker_pos is not None:
        broker_sl    = broker_pos.sl or 0.0
        new_sl       = pr["current_sl"]
        pip          = _tick_value_cache.get(broker_sym, {}).get("pip", 0.0001)
        if abs(new_sl - broker_sl) >= pip:
            # ── PATCH: get current price for stops_level clamping ─────────────
            tick = mt5.symbol_info_tick(broker_sym)
            if tick is None:
                logger.warning(
                    f"[{canon}] SL modify skipped — tick unavailable "
                    f"ticket={pr['ticket']}"
                )
                return
            current_price = tick.bid if direction == "long" else tick.ask
            # ─────────────────────────────────────────────────────────────────
            if modify_sl(broker_sym, pr["ticket"], new_sl,
                         direction, current_price):
                logger.info(
                    f"[{canon}] SL updated ticket={pr['ticket']} "
                    f"{broker_sl:.5f} -> {pr['current_sl']:.5f} "
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
M5_SECONDS        = 300


def _next_m5_boundary_utc() -> datetime.datetime:
    now  = datetime.datetime.utcnow()
    secs = now.hour * 3600 + now.minute * 60 + now.second
    rem  = secs % M5_SECONDS
    wait = M5_SECONDS - rem if rem > 0 else M5_SECONDS
    return now + datetime.timedelta(seconds=wait)


def _get_broker_bar_time() -> pd.Timestamp | None:
    if _CLOCK_SYM_BROKER is None:
        return None
    rates = mt5.copy_rates_from_pos(_CLOCK_SYM_BROKER, mt5.TIMEFRAME_M5, 0, 2)
    if rates is not None and len(rates) >= 2:
        return pd.Timestamp(rates[0]["time"], unit="s")
    return None


def wait_for_new_bar(last_bar_time: pd.Timestamp) -> pd.Timestamp:
    while True:
        boundary = _next_m5_boundary_utc()
        sleep_to = (boundary - datetime.datetime.utcnow()).total_seconds() - 1.0
        if sleep_to > 0:
            time.sleep(sleep_to)

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            t = _get_broker_bar_time()
            if t is not None and t > last_bar_time:
                return t
            time.sleep(0.2)

        logger.debug(
            f"[clock] no new bar after boundary {boundary.strftime('%H:%M')} UTC — "
            f"retrying next boundary"
        )


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
        for canon, d in sorted(self.stats.items(),
                                key=lambda x: -x[1]["total_r"]):
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
    print("run_live() started", flush=True)

    print("Waiting for MT5 terminal to be ready...", flush=True)
    for _attempt in range(30):
        init_ok = mt5.initialize()
        err     = mt5.last_error()
        print(f"  attempt {_attempt+1}/30: init={init_ok} error={err}", flush=True)
        if init_ok:
            break
        time.sleep(3)
    else:
        raise RuntimeError("MT5 terminal did not respond after 90s")

    print("MT5 initialized — logging in...", flush=True)
    authorized = mt5.login(LOGIN, PASSWORD, SERVER)
    print(f"Login result: {authorized} error={mt5.last_error()}", flush=True)
    print(f"account_info: {mt5.account_info()}", flush=True)

    if not authorized:
        mt5.shutdown()
        raise RuntimeError(f"MT5 login failed: {mt5.last_error()}")

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
        # ── PATCH: log stops_level at startup ─────────────────────────────────
        stops_lvl  = int(info.trade_stops_level or 0)
        point      = info.point if info.point > 0 else pip
        min_sl_pts = stops_lvl * point
        logger.info(
            f"  {canon} ({broker}): digits={info.digits} "
            f"tvpl={tvpl:.6f} "
            f"vol_min={info.volume_min} vol_step={info.volume_step} "
            f"pip={pip} "
            f"stops_level={stops_lvl}pts ({min_sl_pts:.5f} price units) "
            f"params={BEST_PARAMS[canon]}"
        )

    _MAX_TRADES_DAY_COMBO = sum(
        BEST_PARAMS[s]["max_trades_day"] for s in active_symbols
    )
    per_trade_allowance = (DAILY_LOSS_BUDGET / _MAX_TRADES_DAY_COMBO
                           if _MAX_TRADES_DAY_COMBO else 0)
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

    logger.info(
        f"\n=== STARTUP DATA LOAD (CSV+gap or broker fetch, "
        f"cap {CACHE_MAX_BARS:,}) ==="
    )
    for canon in active_symbols[:]:
        broker   = sym_map[canon]
        csv_path = CSV_FILES.get(canon, "")
        if os.path.isfile(csv_path):
            logger.info(f"  [{canon}] CSV found: {csv_path}")
        else:
            logger.info(
                f"  [{canon}] no CSV — broker fetch fallback "
                f"(target {FETCH_BARS_STARTUP:,} bars)"
            )
        df = load_csv_or_fetch(canon, broker)
        if df is None:
            logger.error(f"  [{canon}] data load failed — removing from active symbols")
            active_symbols.remove(canon)
            continue
        build_bar_cache(canon, df)
    logger.info("=== END DATA LOAD ===\n")

    if not active_symbols:
        logger.error("No symbols with data — shutting down")
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
        f"MAX_RISK_MULTIPLE={MAX_RISK_MULTIPLE}x  "
        f"CACHE_MAX_BARS={CACHE_MAX_BARS:,}  "
        f"FETCH_BARS_STARTUP={FETCH_BARS_STARTUP:,}"
    )
    logger.info("=== END PARAMS ===\n")

    metrics   = Metrics(active_symbols)
    bar_count = 0

    last_bar_time = _get_broker_bar_time()
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

            wall_today = datetime.datetime.utcnow().date()
            logger.info(
                f"-- BAR {bar_count} | {new_bar_time} UTC "
                f"(wall={wall_today}) "
                f"----------------------------------------"
            )

            for canon in active_symbols:
                broker = sym_map[canon]
                n_new  = fill_gap_for_symbol(canon, broker)
                if n_new == 0:
                    logger.debug(f"[{canon}] no new bars from gap fill")

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
