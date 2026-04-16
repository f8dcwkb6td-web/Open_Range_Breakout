"""
==============================================================================
ORB  —  LIVE ENGINE  (chronological, broker-aware)
==============================================================================

ENTRY TIMING — THE CORE PROBLEM THIS ENGINE SOLVES:
──────────────────────────────────────────────────────────────────────────────
  In the backtest (BT):
    bar closes at 12:00 → signal detected → entry at o[si+1] = 12:00 open
    (historical data: close of bar N and open of bar N+1 share the same
     timestamp — there is ZERO latency between signal and entry)

  Naive live engine (WRONG):
    bar closes at 12:00 → detected at ~12:01 → waits for "next bar open"
    → enters at 12:05 open — 5 bars / 5 minutes late, completely different price

  THIS engine (CORRECT):
    bar closes at 12:00 → detected at ~12:01 → MARKET ORDER SENT NOW
    → fills at ~12:01 market price ≈ 12:05 bar open (the first available
      price after the signal bar closes — exactly what the BT models)

  The BT's "o[si+1]" is simply the first price available after bar si closes.
  In live trading that first price is the current market — so send it NOW.

ARCHITECTURE:
──────────────────────────────────────────────────────────────────────────────
  • One SymbolState per symbol holds the rolling M5 buffer + open trade state.
  • Main loop polls every POLL_INTERVAL seconds.
  • On each poll, per-symbol:
      1. Fetch latest closed M5 bars from MT5  (drop the still-forming bar)
      2. For each newly closed bar (in order):
         a. Run the EXACT same indicator + signal logic as the BT
         b. If signal[last_bar] != 0 → compute lot → MARKET ORDER NOW
      3. Manage open trade: breakeven, trailing SL, session-close exit
  • Hard SL placed on the order; BE / trail modify the position SL via
    TRADE_ACTION_SLTP.

SIGNAL LOGIC:
──────────────────────────────────────────────────────────────────────────────
  Copied verbatim from BT (atr_wilder, expanding_pct_rank, OR construction,
  cooldown, day_count, min_break_atr).  No changes.  Any future BT param
  change must be mirrored here.

OUTPUTS:
  orb_live_trades.csv   — completed trade log
  orb_live.log          — rotating log
==============================================================================
"""

from __future__ import annotations

import os, sys, io, csv, time, bisect, logging, traceback
from datetime import datetime, date, timezone
from pathlib import Path
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd

try:
    import MetaTrader5 as mt5
except ImportError:
    print("MetaTrader5 not installed.  Run:  pip install MetaTrader5")
    sys.exit(1)

# ==============================================================================
#  CONFIGURATION  —  must match BT constants exactly
# ==============================================================================

TERMINAL_PATH = os.environ.get("MT5_TERMINAL_PATH",
                                r"C:\Program Files\MetaTrader 5\terminal64.exe")
LOGIN    = int(os.environ.get("MT5_LOGIN",    0))
PASSWORD =     os.environ.get("MT5_PASSWORD", "")
SERVER   =     os.environ.get("MT5_SERVER",   "")

# ── Risk / broker ─────────────────────────────────────────────────────────
RISK_PER_TRADE    = 0.01
VOL_MIN           = 0.10
VOL_STEP          = 0.10
VOL_MAX           = 250.0
MAX_RISK_MULTIPLE = 2.0

# ── Strategy (must match BT) ──────────────────────────────────────────────
FETCH_BARS_M5  = 10000
WARMUP_M5      = 200
ATR_PERIOD     = 14
ATR_PCT_THRESH = 0.30
MAX_HOLD       = 48      # bars

OR_BARS = {15: 3, 30: 6, 60: 12}

SESSION = {
    "US30":  {"open_h": 13, "open_m": 30, "close_h": 20},
    "US500": {"open_h": 13, "open_m": 30, "close_h": 20},
    "UK100": {"open_h":  8, "open_m":  0, "close_h": 16},
    "GER40": {"open_h":  8, "open_m":  0, "close_h": 17},
}

PARAMS_ACTIVE = {
    "US30":  {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 1.00,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "US500": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
    "UK100": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 1, "min_break_atr": 0.3},
    "GER40": {"or_minutes": 15, "sl_range_mult": 0.5, "trail_atr_mult": 0.75,
              "cooldown_bars": 3, "max_trades_day": 2, "min_break_atr": 0.0},
}

SYMBOL_ALIASES = {
    "US30":  ["US30", "DJ30", "DJIA", "WS30", "DOW30", "US30Cash"],
    "US500": ["US500", "SPX500", "SP500", "SPX", "US500Cash"],
    "UK100": ["UK100", "FTSE100", "FTSE", "UK100Cash", "UKX"],
    "GER40": ["GER40", "DAX40", "DAX", "GER30", "DE40", "GER40Cash"],
}
SYMBOLS = list(SESSION.keys())

TICK_VALUE_FALLBACK = {"US30": 1.0, "US500": 1.0, "UK100": 1.0, "GER40": 1.0}

POLL_INTERVAL = 5    # seconds between bar-close checks
MAGIC         = 20250101
TRADES_CSV    = Path("orb_live_trades.csv")

# ==============================================================================
#  LOGGING
# ==============================================================================

logger = logging.getLogger("ORB_LIVE")
logger.setLevel(logging.DEBUG)

_fh = RotatingFileHandler("orb_live.log", maxBytes=15_000_000, backupCount=5,
                           encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_fh)

_sh = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace"))
_sh.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
_sh.setLevel(logging.INFO)
logger.addHandler(_sh)


# ==============================================================================
#  SECTION 1 — MT5 HELPERS
# ==============================================================================

def resolve_symbol(canonical: str) -> str | None:
    all_broker = {s.name.upper(): s.name for s in (mt5.symbols_get() or [])}
    for alias in SYMBOL_ALIASES[canonical]:
        info = mt5.symbol_info(alias)
        if info is not None:
            if not info.visible:
                mt5.symbol_select(alias, True)
            return alias
        for up, name in all_broker.items():
            if up.startswith(alias.upper()):
                mt5.symbol_select(name, True)
                return name
    return None


def get_tick_value(canonical: str) -> float:
    broker = resolve_symbol(canonical)
    if broker is None:
        return TICK_VALUE_FALLBACK.get(canonical, 1.0)
    info = mt5.symbol_info(broker)
    if info is None or info.trade_tick_size <= 0:
        return TICK_VALUE_FALLBACK.get(canonical, 1.0)
    return info.trade_tick_value / info.trade_tick_size


def get_account_balance() -> float:
    info = mt5.account_info()
    return info.balance if info else 0.0


def fetch_m5(canonical: str, count: int) -> pd.DataFrame | None:
    """
    Returns the last `count` CLOSED M5 bars.

    MT5's copy_rates_from_pos(0) returns bars from the current forming bar
    backwards.  Bar index 0 is the still-forming bar — we drop it so we only
    ever act on confirmed closed bars.  This matches the BT which only ever
    reads historical (closed) bars.
    """
    broker = resolve_symbol(canonical)
    if broker is None:
        return None
    # Request one extra so after dropping bar[0] we still have `count` closed bars
    rates = mt5.copy_rates_from_pos(broker, mt5.TIMEFRAME_M5, 0, count + 1)
    if rates is None or len(rates) < 10:
        return None
    cols = ["time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume"]
    df = pd.DataFrame(rates, columns=cols)[
        ["time", "open", "high", "low", "close"]].copy()
    df = df.iloc[:-1].copy()   # drop the still-forming bar
    df["time_utc"]   = pd.to_datetime(df["time"].astype(np.int64), unit="s")
    df["utc_hour"]   = df["time_utc"].dt.hour
    df["utc_minute"] = df["time_utc"].dt.minute
    df["date"]       = df["time_utc"].dt.date
    return df.reset_index(drop=True)


# ==============================================================================
#  SECTION 2 — LOT SIZING  (verbatim from BT)
# ==============================================================================

def compute_lot_aware(balance: float, sl_dist: float,
                       tvpl: float) -> tuple:
    if sl_dist < 1e-9 or tvpl <= 0:
        return None, 0.0, 0.0, 0.0, True
    intended  = balance * RISK_PER_TRADE
    raw_lot   = intended / (sl_dist * tvpl)
    lot = max(VOL_MIN, min(VOL_MAX, round(raw_lot / VOL_STEP) * VOL_STEP))
    lot = round(lot, 8)
    actual    = lot * sl_dist * tvpl
    risk_mult = actual / intended if intended > 0 else float("inf")
    rejected  = risk_mult > MAX_RISK_MULTIPLE
    return lot, intended, actual, risk_mult, rejected


# ==============================================================================
#  SECTION 3 — INDICATORS  (verbatim from BT)
# ==============================================================================

def atr_wilder(h: np.ndarray, l: np.ndarray, c: np.ndarray) -> np.ndarray:
    n  = len(h)
    tr = np.empty(n)
    tr[0]  = h[0] - l[0]
    tr[1:] = np.maximum(
        h[1:] - l[1:],
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


def expanding_pct_rank(arr: np.ndarray) -> np.ndarray:
    n    = len(arr)
    out  = np.full(n, np.nan)
    hist: list[float] = []
    for i in range(WARMUP_M5, n):
        v = arr[i]
        if np.isnan(v):
            continue
        if hist:
            out[i] = bisect.bisect_left(hist, v) / len(hist)
        bisect.insort(hist, v)
    return out


# ==============================================================================
#  SECTION 4 — SIGNAL COMPUTATION  (verbatim from BT's build_cache_and_signals)
# ==============================================================================

def run_signal_on_bars(canonical: str, df: pd.DataFrame, params: dict) -> dict:
    """
    Identical signal pipeline to BT.  Operates on a closed-bar DataFrame.
    signal[i] != 0  means bar i closed with a valid OR breakout.

    In the BT: entry = o[i+1]  (open of next bar)
    In live:   entry = MARKET ORDER NOW  (same economic intent)
    """
    cfg = SESSION[canonical]
    o   = df["open"].values.astype(np.float64)
    h   = df["high"].values.astype(np.float64)
    l   = df["low"].values.astype(np.float64)
    c   = df["close"].values.astype(np.float64)
    n   = len(c)

    atr14   = atr_wilder(h, l, c)
    atr_pct = expanding_pct_rank(atr14)

    utc_h = df["utc_hour"].values.astype(np.int32)
    utc_m = df["utc_minute"].values.astype(np.int32)
    dates = df["date"].values
    times = df["time_utc"].values

    in_session = np.array([
        (utc_h[i] > cfg["open_h"] or
         (utc_h[i] == cfg["open_h"] and utc_m[i] >= cfg["open_m"]))
        and utc_h[i] < cfg["close_h"]
        for i in range(n)
    ])
    is_open_bar = (utc_h == cfg["open_h"]) & (utc_m == cfg["open_m"])

    or_bars   = OR_BARS[params["or_minutes"]]
    day_start: dict = {}
    for i in range(n):
        if is_open_bar[i]:
            d = dates[i]
            if d not in day_start:
                day_start[d] = i

    day_or: dict = {}
    for d, si_d in day_start.items():
        ei = si_d + or_bars
        if ei <= n:
            day_or[d] = (h[si_d:ei].max(), l[si_d:ei].min())

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

    cooldown      = params["cooldown_bars"]
    max_t         = params["max_trades_day"]
    min_break_atr = params["min_break_atr"]

    atr_ok = (~np.isnan(atr_pct)) & (atr_pct >= ATR_PCT_THRESH)
    valid  = np.zeros(n, dtype=bool)
    # BT gate: valid[WARMUP_M5 : n-1]
    # In live we check valid[n-1] == True, so we need n-1 >= WARMUP_M5
    valid[WARMUP_M5:n - 1] = True
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
        last_sig  = i
        day_count[day] = day_count.get(day, 0) + 1

    return {
        "n": n, "o": o, "h": h, "l": l, "c": c,
        "atr14": atr14, "or_high": or_high, "or_low": or_low,
        "signal": signal, "dates": dates, "times": times,
        "utc_h": utc_h,
    }


# ==============================================================================
#  SECTION 5 — ENTRY PARAM COMPUTATION  (mirrors BT's resolve_trade setup)
# ==============================================================================

def compute_entry_params(cache: dict, si: int, params: dict) -> dict | None:
    """
    Mirrors the first half of BT's resolve_trade(): compute sl_dist and
    validate the setup.  Does NOT touch lot sizing or order sending.
    """
    atr     = float(cache["atr14"][si])
    or_h    = float(cache["or_high"][si])
    or_l    = float(cache["or_low"][si])
    or_size = or_h - or_l

    if np.isnan(atr) or atr <= 0:
        return None
    if np.isnan(or_size) or or_size <= 0:
        return None

    sl_dist = max(params["sl_range_mult"] * or_size, atr * 0.05)
    if sl_dist < 0.05 * atr:
        sl_dist = 0.05 * atr

    return {
        "direction": int(cache["signal"][si]),
        "sl_dist":   sl_dist,
        "atr":       atr,
        "or_high":   or_h,
        "or_low":    or_l,
    }


# ==============================================================================
#  SECTION 6 — MT5 ORDER / POSITION HELPERS
# ==============================================================================

def send_market_order(broker_sym: str, direction: int, lot: float,
                       sl_price: float) -> int | None:
    order_type = mt5.ORDER_TYPE_BUY if direction == 1 else mt5.ORDER_TYPE_SELL
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        logger.error(f"[{broker_sym}] no tick")
        return None
    price = tick.ask if direction == 1 else tick.bid
    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       broker_sym,
        "volume":       lot,
        "type":         order_type,
        "price":        price,
        "sl":           round(sl_price, 5),
        "deviation":    20,
        "magic":        MAGIC,
        "comment":      "ORB",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    res = mt5.order_send(req)
    if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
        logger.error(f"[{broker_sym}] order_send fail retcode="
                     f"{res.retcode if res else 'None'}")
        return None
    logger.info(f"[{broker_sym}] FILLED ticket={res.order} "
                f"price={res.price:.5f} lot={lot} sl={sl_price:.5f}")
    return res.order


def modify_sl(ticket: int, broker_sym: str, new_sl: float) -> bool:
    pos = _get_pos(ticket, broker_sym)
    if pos is None:
        return False
    res = mt5.order_send({
        "action":   mt5.TRADE_ACTION_SLTP,
        "position": ticket,
        "symbol":   broker_sym,
        "sl":       round(new_sl, 5),
        "tp":       pos.tp,
    })
    return res is not None and res.retcode == mt5.TRADE_RETCODE_DONE


def close_position(ticket: int, broker_sym: str, lot: float, direction: int) -> bool:
    close_type = mt5.ORDER_TYPE_SELL if direction == 1 else mt5.ORDER_TYPE_BUY
    tick = mt5.symbol_info_tick(broker_sym)
    if tick is None:
        return False
    price = tick.bid if direction == 1 else tick.ask
    res = mt5.order_send({
        "action":       mt5.TRADE_ACTION_DEAL,
        "position":     ticket,
        "symbol":       broker_sym,
        "volume":       lot,
        "type":         close_type,
        "price":        price,
        "deviation":    20,
        "magic":        MAGIC,
        "comment":      "ORB_CLOSE",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    })
    return res is not None and res.retcode == mt5.TRADE_RETCODE_DONE


def _get_pos(ticket: int, broker_sym: str):
    positions = mt5.positions_get(symbol=broker_sym)
    if positions:
        for p in positions:
            if p.ticket == ticket and p.magic == MAGIC:
                return p
    return None


# ==============================================================================
#  SECTION 7 — STATE DATACLASSES
# ==============================================================================

@dataclass
class OpenTrade:
    ticket:          int
    direction:       int
    lot:             float
    entry_price:     float
    sl_dist:         float
    cur_sl:          float
    be_active:       bool
    entry_bar_abs:   int     # buffer index at entry time (for MAX_HOLD count)
    entry_time:      str
    signal_bar_time: str
    broker_sym:      str
    atr_at_entry:    float


@dataclass
class SymbolState:
    canonical:       str
    broker_sym:      str
    tick_value:      float
    params:          dict
    cfg:             dict
    df:              pd.DataFrame = field(default_factory=pd.DataFrame)
    last_bar_time:   object = None   # pd.Timestamp of last processed closed bar
    today:           object = None   # date
    day_trade_count: int = 0
    last_sig_bar:    int = -9999     # buffer index of last signal (for cooldown)
    open_trade:      OpenTrade | None = None


# ==============================================================================
#  SECTION 8 — ENGINE
# ==============================================================================

class ORBLiveEngine:

    def __init__(self):
        self.states: dict[str, SymbolState] = {}
        self._init_csv()

    # ── CSV header ────────────────────────────────────────────────────────────

    def _init_csv(self):
        if not TRADES_CSV.exists():
            with open(TRADES_CSV, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=[
                    "symbol", "signal_bar_time", "entry_time", "exit_time",
                    "direction", "lot", "entry_price", "exit_price",
                    "sl_dist", "tick_val_lot", "intended_risk",
                    "actual_risk", "risk_multiple", "outcome_r",
                    "pnl", "balance_after", "exit_reason",
                ]).writeheader()

    # ── Initialisation ────────────────────────────────────────────────────────

    def initialise(self):
        logger.info("Initialising...")
        for sym in SYMBOLS:
            broker = resolve_symbol(sym)
            if broker is None:
                logger.warning(f"[{sym}] not found — skipping")
                continue
            df = fetch_m5(sym, FETCH_BARS_M5)
            if df is None or len(df) < WARMUP_M5 + 10:
                logger.warning(f"[{sym}] insufficient history — skipping")
                continue
            st = SymbolState(
                canonical=sym,
                broker_sym=broker,
                tick_value=get_tick_value(sym),
                params=PARAMS_ACTIVE[sym],
                cfg=SESSION[sym],
                df=df,
                last_bar_time=df["time_utc"].iloc[-1],
            )
            self.states[sym] = st
            logger.info(f"[{sym}] ready  bars={len(df)}  "
                        f"broker={broker}  tv={st.tick_value:.6f}  "
                        f"last_closed={st.last_bar_time}")
        logger.info(f"Active: {list(self.states.keys())}")

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self):
        self.initialise()
        logger.info("Engine running — Ctrl-C to stop\n")
        while True:
            try:
                for sym, st in self.states.items():
                    self._process_symbol(st)
            except KeyboardInterrupt:
                logger.info("Stopping.")
                break
            except Exception:
                logger.error(traceback.format_exc())
            time.sleep(POLL_INTERVAL)

    # ── Per-symbol: detect new closed bars → signal check → market order ─────

    def _process_symbol(self, st: SymbolState):

        # 1. Fetch latest closed bars
        df_new = fetch_m5(st.canonical, FETCH_BARS_M5)
        if df_new is None or df_new.empty:
            return

        # 2. Which bars are new since we last checked?
        if st.last_bar_time is not None:
            new_mask = df_new["time_utc"] > st.last_bar_time
            new_bars = df_new[new_mask]
        else:
            new_bars = df_new.iloc[-1:]

        # Update rolling buffer and watermark
        st.df = df_new
        st.last_bar_time = df_new["time_utc"].iloc[-1]

        if new_bars.empty:
            self._manage_open_trade(st)
            return

        # 3. Process each newly closed bar in time order
        #
        # For each newly closed bar at absolute buffer position abs_idx,
        # we slice the buffer up to and including it, run the full signal
        # pipeline, and check if signal fired on the last bar of that slice
        # (equivalent to BT's signal[si]).
        #
        # If signal fires → MARKET ORDER IMMEDIATELY.
        # This is the correct live equivalent of BT's "entry at o[si+1]".

        n_total = len(df_new)
        n_new   = len(new_bars)

        for k in range(n_new):
            abs_idx = n_total - n_new + k      # this bar's index in df_new
            sub     = df_new.iloc[:abs_idx + 1].copy().reset_index(drop=True)
            si      = len(sub) - 1             # last bar = the just-closed bar

            bar_time  = sub["time_utc"].iloc[si]
            bar_date  = sub["date"].iloc[si]

            # Reset per-day counters on new date
            if bar_date != st.today:
                st.today           = bar_date
                st.day_trade_count = 0
                logger.debug(f"[{st.canonical}] new date {bar_date}")

            # Skip if trade already open
            if st.open_trade is not None:
                continue

            # BT warmup gate: need at least WARMUP_M5 bars
            if si < WARMUP_M5:
                continue

            # Run full BT signal logic
            cache = run_signal_on_bars(st.canonical, sub, st.params)

            # Check signal on THIS bar (the one that just closed)
            direction = int(cache["signal"][si])
            if direction == 0:
                continue

            # Cooldown (using absolute buffer index to match BT's bar-index cooldown)
            if abs_idx - st.last_sig_bar < st.params["cooldown_bars"]:
                logger.debug(f"[{st.canonical}] cooldown — skip {bar_time}")
                continue

            # Day trade count
            if st.day_trade_count >= st.params["max_trades_day"]:
                logger.debug(f"[{st.canonical}] max_trades_day — skip {bar_time}")
                continue

            # Compute SL distance (mirrors BT's resolve_trade setup)
            ep_params = compute_entry_params(cache, si, st.params)
            if ep_params is None:
                continue

            sl_dist = ep_params["sl_dist"]
            balance = get_account_balance()

            # Lot sizing — identical to BT
            lot, intended, actual, risk_mult, rejected = \
                compute_lot_aware(balance, sl_dist, st.tick_value)
            if rejected:
                logger.warning(
                    f"[{st.canonical}] RISK-GUARD REJECT "
                    f"risk_mult={risk_mult:.2f}x sl={sl_dist:.5f} bal={balance:.2f}"
                )
                continue

            # ── MARKET ORDER NOW ──────────────────────────────────────────────
            #
            # The BT enters at o[si+1] which is the FIRST price after bar si
            # closes.  In live the first available price is the current market.
            # Waiting for bar si+1 to OPEN would mean waiting up to 5 minutes
            # and entering at a completely different price (12:05 instead of
            # the ~12:00 price that the BT modelled).
            #
            # Current market ≈ o[si+1] for liquid indices with no overnight gap.
            # Any remaining difference (slippage/spread) is unavoidable and
            # already priced in by live trading realities.
            #
            tick_now  = mt5.symbol_info_tick(st.broker_sym)
            fill_est  = tick_now.ask if direction == 1 else tick_now.bid
            sl_price  = fill_est - direction * sl_dist

            logger.info(
                f"[{st.canonical}] SIGNAL bar_closed={bar_time}  "
                f"{'LONG' if direction==1 else 'SHORT'}  "
                f"sl_dist={sl_dist:.5f}  lot={lot}  "
                f"→ MARKET ORDER NOW (not waiting for next bar open)"
            )

            ticket = send_market_order(st.broker_sym, direction, lot, sl_price)
            if ticket is None:
                continue

            # Read actual fill price from position
            time.sleep(0.3)
            pos        = _get_pos(ticket, st.broker_sym)
            fill_price = pos.price_open if pos else fill_est

            st.open_trade = OpenTrade(
                ticket=ticket,
                direction=direction,
                lot=lot,
                entry_price=fill_price,
                sl_dist=sl_dist,
                cur_sl=sl_price,
                be_active=False,
                entry_bar_abs=abs_idx,
                entry_time=str(datetime.now(timezone.utc)),
                signal_bar_time=str(bar_time),
                broker_sym=st.broker_sym,
                atr_at_entry=float(cache["atr14"][si]),
            )
            st.last_sig_bar    = abs_idx
            st.day_trade_count += 1

            logger.info(
                f"[{st.canonical}] OPEN: ticket={ticket} "
                f"fill={fill_price:.5f} sl={sl_price:.5f} "
                f"1R={fill_price + direction * sl_dist:.5f} "
                f"intended={intended:.4f} actual={actual:.4f} "
                f"risk_mult={risk_mult:.3f}x"
            )

        # 4. Manage any open trade
        self._manage_open_trade(st)

    # ── Open trade management  (mirrors BT's resolve_trade loop) ─────────────

    def _manage_open_trade(self, st: SymbolState):
        """
        Polls open trade state and applies:
          - Hard SL already on broker — we don't re-check manually
          - Breakeven: move SL to entry when price reaches 1R target
          - Trailing stop: widen SL as price moves in our favour
          - Session close: force-close at session end
          - MAX_HOLD: force-close after 48 bars
        """
        if st.open_trade is None:
            return
        tr = st.open_trade

        # ── Check if position still exists (closed by broker SL/TP) ──────────
        pos = _get_pos(tr.ticket, tr.broker_sym)
        if pos is None:
            self._record_trade(st, tr, exit_reason="SL_HIT")
            st.open_trade = None
            return

        # Need current bar data
        if st.df.empty:
            return
        last   = st.df.iloc[-1]
        bh     = float(last["high"])
        bl     = float(last["low"])
        bc     = float(last["close"])
        bdate  = last["date"]
        butc_h = int(last["utc_hour"])

        # ── Session close or date rollover ────────────────────────────────────
        if butc_h >= st.cfg["close_h"] or bdate != st.today:
            logger.info(f"[{st.canonical}] SESSION END — closing ticket={tr.ticket}")
            close_position(tr.ticket, tr.broker_sym, tr.lot, tr.direction)
            self._record_trade(st, tr, exit_reason="SESSION_CLOSE", exit_price=bc)
            st.open_trade = None
            return

        # ── MAX_HOLD bar limit ────────────────────────────────────────────────
        bars_held = len(st.df) - 1 - tr.entry_bar_abs
        if bars_held >= MAX_HOLD:
            logger.info(f"[{st.canonical}] MAX_HOLD — closing ticket={tr.ticket}")
            close_position(tr.ticket, tr.broker_sym, tr.lot, tr.direction)
            self._record_trade(st, tr, exit_reason="MAX_HOLD", exit_price=bc)
            st.open_trade = None
            return

        ep      = tr.entry_price
        sl_dist = tr.sl_dist

        # ── Breakeven activation (mirrors BT) ─────────────────────────────────
        one_r = ep + tr.direction * sl_dist
        if not tr.be_active:
            hit = (tr.direction == 1 and bh >= one_r) or \
                  (tr.direction == -1 and bl <= one_r)
            if hit:
                if modify_sl(tr.ticket, tr.broker_sym, ep):
                    tr.cur_sl    = ep
                    tr.be_active = True
                    logger.info(f"[{st.canonical}] BREAKEVEN sl→{ep:.5f}")

        # ── Trailing stop (mirrors BT) ─────────────────────────────────────────
        if tr.be_active:
            cache = run_signal_on_bars(st.canonical, st.df, st.params)
            last_atr = cache["atr14"][-1]
            ta = float(last_atr) if not np.isnan(last_atr) else tr.atr_at_entry
            tm = st.params["trail_atr_mult"]

            if tr.direction == 1:
                proposed = bh - tm * ta
                if proposed > tr.cur_sl:
                    if modify_sl(tr.ticket, tr.broker_sym, proposed):
                        logger.debug(f"[{st.canonical}] TRAIL UP "
                                     f"{tr.cur_sl:.5f}→{proposed:.5f}")
                        tr.cur_sl = proposed
            else:
                proposed = bl + tm * ta
                if proposed < tr.cur_sl:
                    if modify_sl(tr.ticket, tr.broker_sym, proposed):
                        logger.debug(f"[{st.canonical}] TRAIL DOWN "
                                     f"{tr.cur_sl:.5f}→{proposed:.5f}")
                        tr.cur_sl = proposed

    # ── Trade log ─────────────────────────────────────────────────────────────

    def _record_trade(self, st: SymbolState, tr: OpenTrade,
                       exit_reason: str, exit_price: float | None = None):
        # Try to get actual fill from deal history
        now_ts = int(datetime.now(timezone.utc).timestamp())
        deals  = mt5.history_deals_get(now_ts - 3600, now_ts) or []
        ep_out = exit_price or tr.entry_price
        for d in reversed(deals):
            if d.magic == MAGIC and d.symbol == tr.broker_sym:
                ep_out = d.price
                break

        outcome_r = tr.direction * (ep_out - tr.entry_price) / tr.sl_dist
        pnl       = outcome_r * tr.lot * tr.sl_dist * st.tick_value
        bal       = get_account_balance()

        row = {
            "symbol":         st.canonical,
            "signal_bar_time": tr.signal_bar_time,
            "entry_time":     tr.entry_time,
            "exit_time":      str(datetime.now(timezone.utc)),
            "direction":      "LONG" if tr.direction == 1 else "SHORT",
            "lot":            tr.lot,
            "entry_price":    round(tr.entry_price, 5),
            "exit_price":     round(ep_out, 5),
            "sl_dist":        round(tr.sl_dist, 5),
            "tick_val_lot":   round(st.tick_value, 6),
            "intended_risk":  round(bal * RISK_PER_TRADE, 4),
            "actual_risk":    round(tr.lot * tr.sl_dist * st.tick_value, 4),
            "risk_multiple":  round(
                (tr.lot * tr.sl_dist * st.tick_value)
                / max(bal * RISK_PER_TRADE, 1e-9), 3),
            "outcome_r":      round(outcome_r, 4),
            "pnl":            round(pnl, 4),
            "balance_after":  round(bal, 4),
            "exit_reason":    exit_reason,
        }
        with open(TRADES_CSV, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=list(row.keys())).writerow(row)

        logger.info(f"[{st.canonical}] CLOSED {exit_reason} "
                    f"outcome={outcome_r:+.4f}R pnl={pnl:+.4f} bal={bal:.2f}")


# ==============================================================================
#  ENTRY POINT
# ==============================================================================

def main():
    if not mt5.initialize(path=TERMINAL_PATH, login=LOGIN,
                          password=PASSWORD, server=SERVER):
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
    logger.info("MT5 connected")
    engine = ORBLiveEngine()
    try:
        engine.run()
    finally:
        mt5.shutdown()
        logger.info("Done.")


if __name__ == "__main__":
    main()
