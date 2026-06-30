"""
Discord Stock Watchlist & Technical Analysis Bot (merged)

Foundation: SQLite-backed commands.Bot (yours).
Folded in from your friend's bot: market-bias/EMA/RSI scoring engine,
trade-plan builder (entry/stop/target/R:R), 52-week high / breakout
alerts, market heat map, and weekly earnings task. All new state is
persisted in SQLite instead of in-memory globals, and the RSI
calculation is shared (deduped) across both feature sets.

Features:
- Watchlist (SQLite), dynamic via !add / !addmany / !remove
- Price drop alerts with re-alert protection and auto-reset on recovery
- Per-symbol custom drop thresholds
- Trailing-peak drop tracking (alert measured from recent high)
- Golden Cross / Death Cross alerts (20/50 MA) with proper flag resets
- RSI overbought / oversold alerts
- RSI, MACD, Trend analysis (!analyze)
- Fibonacci retracement command (!fib)
- Quick !price command
- Market bias (SPY EMA/RSI/VIX) command (!bias)
- Setup scoring engine (0-10) + trade plan (entry/stop/target/R:R) (!score, auto-alerts)
- 52-week high "approaching" + "confirmed breakout" alerts
- Daily market heat map (after close)
- Weekly earnings-this-week summary (Mondays)
- History caching to reduce API calls
- Market-hours awareness
- Failed-fetch counter to surface dead/delisted symbols
- Custom !help command
- Persistent alert/state tracking in SQLite (survives restarts)
- Error handling in background tasks
"""

import os
import io
import time
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

import pandas as pd
import yfinance as yf

import matplotlib
matplotlib.use("Agg")  # headless backend, no display needed
import matplotlib.pyplot as plt
import squarify

# -------------------------------------------------
# Config
# -------------------------------------------------

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

# Who to ping on alerts. ALERT_MENTION_TYPE can be:
#   "everyone" - pings @everyone (no ID needed; bot needs Mention Everyone permission)
#   "role"     - pings the role whose ID is in ALERT_MENTION_ID
#   "user"     - pings the user whose ID is in ALERT_MENTION_ID
# Leave ALERT_MENTION_TYPE unset/blank (or omit the ID for role/user) to disable.
ALERT_MENTION_ID = os.getenv("ALERT_MENTION_ID")
ALERT_MENTION_TYPE = os.getenv("ALERT_MENTION_TYPE", "role")  # "everyone", "role", or "user"

DB_FILE = "watchlist.db"
DEFAULT_DROP_THRESHOLD = 0.15  # 15%

# RSI alert bounds
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# How long a cached history result stays fresh (seconds)
HISTORY_CACHE_TTL = 600  # 10 minutes

# How many consecutive failed fetches before we flag a symbol as likely dead
FAIL_LIMIT = 5

# Set to False to run the background loop regardless of market hours
RESPECT_MARKET_HOURS = True

# --- scoring engine / trade-plan settings (from friend's bot) ---
SCORE_ALERT_THRESHOLD = 6      # min score to consider a setup "strong"
SCORE_ALERT_COOLDOWN = 900     # 15 min between repeat score alerts per symbol
DEBUG = True

# heatmap posts once per day, at/after this hour (ET)
HEATMAP_HOUR_ET = 16

def log(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")

# -------------------------------------------------
# Bot Setup
# -------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

# remove the default help so we can supply our own
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# -------------------------------------------------
# History Cache
# -------------------------------------------------

# symbol -> (timestamp, dataframe)
_history_cache = {}

# Tracks the last known market open/closed state so we announce only on change.
# None until the first loop run establishes a baseline.
_market_open_state = None

# -------------------------------------------------
# Mention Helper
# -------------------------------------------------

def alert_prefix():
    """Return a mention string to prepend to alerts, or empty string if disabled."""
    if ALERT_MENTION_TYPE == "everyone":
        return "@everyone "
    if not ALERT_MENTION_ID:
        return ""
    if ALERT_MENTION_TYPE == "user":
        return f"<@{ALERT_MENTION_ID}> "
    return f"<@&{ALERT_MENTION_ID}> "

# Discord suppresses @everyone/role/user pings by default; this opts them back in.
ALERT_ALLOWED = discord.AllowedMentions(everyone=True, roles=True, users=True)

async def send_alert(channel, symbol, content, **kwargs):
    """Send an alert message, tagging whichever users are subscribed to
    this symbol, with mentions enabled so the pings actually notify."""
    ping = mention_for_symbol(symbol)
    return await channel.send(f"{ping}{content}", allowed_mentions=ALERT_ALLOWED, **kwargs)

async def send_broadcast(channel, content, **kwargs):
    """Send a non-symbol-specific alert (e.g. market open/close) using the
    configured role/user/everyone ping rather than per-symbol subscribers."""
    ping = alert_prefix()
    return await channel.send(f"{ping}{content}", allowed_mentions=ALERT_ALLOWED, **kwargs)

# -------------------------------------------------
# Market Hours
# -------------------------------------------------

def market_is_open():
    """
    Rough check for US regular trading hours (9:30-16:00 ET, Mon-Fri).
    ET is UTC-5 (EST) or UTC-4 (EDT). We approximate DST as Mar-Nov to avoid
    a tz dependency; this is good enough for skipping nights and weekends.
    Does not account for market holidays.
    """
    now_utc = datetime.now(timezone.utc)

    is_dst = 3 <= now_utc.month <= 11
    offset = -4 if is_dst else -5
    et = now_utc + timedelta(hours=offset)

    if et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False

    open_minutes = 9 * 60 + 30
    close_minutes = 16 * 60
    now_minutes = et.hour * 60 + et.minute

    return open_minutes <= now_minutes <= close_minutes

# -------------------------------------------------
# Database
# -------------------------------------------------

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                baseline REAL NOT NULL,
                threshold REAL DEFAULT 0.15,
                peak REAL,
                alerted INTEGER DEFAULT 0,
                golden_alerted INTEGER DEFAULT 0,
                death_alerted INTEGER DEFAULT 0,
                rsi_high_alerted INTEGER DEFAULT 0,
                rsi_low_alerted INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0,
                last_signal TEXT,
                last_score REAL,
                last_score_time REAL,
                last_score_alert_time REAL,
                pre_break_alerted INTEGER DEFAULT 0,
                breakout_alerted INTEGER DEFAULT 0,
                added_by TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                symbol TEXT NOT NULL,
                user_id TEXT NOT NULL,
                PRIMARY KEY (symbol, user_id)
            )
        """)

        # Migrate older databases that predate newer columns.
        cols = [r[1] for r in conn.execute("PRAGMA table_info(watchlist)").fetchall()]
        migrations = {
            "threshold": "ALTER TABLE watchlist ADD COLUMN threshold REAL DEFAULT 0.15",
            "peak": "ALTER TABLE watchlist ADD COLUMN peak REAL",
            "rsi_high_alerted": "ALTER TABLE watchlist ADD COLUMN rsi_high_alerted INTEGER DEFAULT 0",
            "rsi_low_alerted": "ALTER TABLE watchlist ADD COLUMN rsi_low_alerted INTEGER DEFAULT 0",
            "fail_count": "ALTER TABLE watchlist ADD COLUMN fail_count INTEGER DEFAULT 0",
            "last_signal": "ALTER TABLE watchlist ADD COLUMN last_signal TEXT",
            "last_score": "ALTER TABLE watchlist ADD COLUMN last_score REAL",
            "last_score_time": "ALTER TABLE watchlist ADD COLUMN last_score_time REAL",
            "last_score_alert_time": "ALTER TABLE watchlist ADD COLUMN last_score_alert_time REAL",
            "pre_break_alerted": "ALTER TABLE watchlist ADD COLUMN pre_break_alerted INTEGER DEFAULT 0",
            "breakout_alerted": "ALTER TABLE watchlist ADD COLUMN breakout_alerted INTEGER DEFAULT 0",
            "added_by": "ALTER TABLE watchlist ADD COLUMN added_by TEXT",
        }
        for col, stmt in migrations.items():
            if col not in cols:
                conn.execute(stmt)

def db_execute(query, params=(), fetch=False):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchall() if fetch else None

def get_meta(key, default=None):
    row = db_execute("SELECT value FROM meta WHERE key = ?", (key,), fetch=True)
    return row[0][0] if row else default

def set_meta(key, value):
    db_execute(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value))
    )

def get_watchlist_symbols():
    rows = db_execute("SELECT symbol FROM watchlist", fetch=True)
    return [r[0] for r in rows] if rows else []

def get_subscribers(symbol):
    rows = db_execute("SELECT user_id FROM subscriptions WHERE symbol = ?", (symbol,), fetch=True)
    return [r[0] for r in rows] if rows else []

def get_user_symbols(user_id):
    rows = db_execute("SELECT symbol FROM subscriptions WHERE user_id = ?", (str(user_id),), fetch=True)
    return [r[0] for r in rows] if rows else []

def add_subscription(symbol, user_id):
    db_execute(
        "INSERT OR IGNORE INTO subscriptions (symbol, user_id) VALUES (?, ?)",
        (symbol, str(user_id))
    )

def remove_subscription(symbol, user_id):
    """Remove a user's subscription. If no one is left watching the symbol,
    drop the watchlist row (and its cached history/flags) too, so we stop
    spending API calls on symbols nobody cares about anymore."""
    db_execute(
        "DELETE FROM subscriptions WHERE symbol = ? AND user_id = ?",
        (symbol, str(user_id))
    )
    remaining = get_subscribers(symbol)
    if not remaining:
        db_execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
        _history_cache.pop(f"{symbol}:1y", None)
        _history_cache.pop(f"{symbol}:6mo", None)
    return remaining

def mention_for_symbol(symbol):
    """Build a mention string tagging every subscriber for this symbol.
    Falls back to the configured role/user/everyone ping if, for some
    reason, a symbol has no subscribers."""
    subscribers = get_subscribers(symbol)
    if subscribers:
        return "".join(f"<@{uid}> " for uid in subscribers)
    return alert_prefix()

# -------------------------------------------------
# Async Data Fetch
# -------------------------------------------------

async def fetch_current_price(symbol):
    def _fetch():
        df = yf.Ticker(symbol).history(period="1d")
        return None if df.empty else float(df["Close"].iloc[-1])
    return await asyncio.to_thread(_fetch)

async def fetch_quote(symbol):
    """Return (current_price, prev_close) for a quick quote, or (None, None)."""
    def _fetch():
        df = yf.Ticker(symbol).history(period="5d")
        if df.empty or len(df) < 2:
            return (None, None)
        return (float(df["Close"].iloc[-1]), float(df["Close"].iloc[-2]))
    return await asyncio.to_thread(_fetch)

async def fetch_history(symbol, period="1y", use_cache=True):
    """Fetch history. Uses a short-lived cache to cut redundant calls.

    Cache key includes the period so the 1y fetches used for cross/RSI/52wk
    checks don't collide with any shorter-period fetch elsewhere.
    """
    now = time.monotonic()
    cache_key = f"{symbol}:{period}"

    if use_cache:
        cached = _history_cache.get(cache_key)
        if cached and (now - cached[0]) < HISTORY_CACHE_TTL:
            return cached[1]

    def _fetch():
        df = yf.Ticker(symbol).history(period=period)
        return df if not df.empty else None

    df = await asyncio.to_thread(_fetch)
    if df is not None:
        _history_cache[cache_key] = (now, df)
    return df

# -------------------------------------------------
# Indicators (shared / deduped)
# -------------------------------------------------

def compute_rsi_series(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.rolling(period).mean() / loss.rolling(period).mean()
    return 100 - (100 / (1 + rs))

def get_rsi_value(df):
    """Latest RSI value from a price dataframe, or None if not computable."""
    if df is None or df.empty:
        return None
    rsi_series = compute_rsi_series(df["Close"])
    val = rsi_series.iloc[-1]
    return float(val) if pd.notna(val) else None

def calculate_indicators(df):
    close = df["Close"]

    rsi = compute_rsi_series(close)

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()

    # Trend
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    trend = "Sideways"
    if ma20.iloc[-1] > ma50.iloc[-1]:
        trend = "Uptrend"
    elif ma20.iloc[-1] < ma50.iloc[-1]:
        trend = "Downtrend"

    return {
        "rsi": round(rsi.iloc[-1], 2) if pd.notna(rsi.iloc[-1]) else None,
        "macd": round(macd.iloc[-1], 4),
        "signal": round(signal.iloc[-1], 4),
        "trend": trend
    }

def get_atr(df, period=14):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_series = tr.rolling(period).mean()

    value = atr_series.dropna()
    if value.empty:
        return None
    return float(value.iloc[-1])

# -------------------------------------------------
# Market bias / scoring engine / trade plan
# (ported from friend's bot, RSI calc deduped to use compute_rsi_series)
# -------------------------------------------------

async def get_market_bias():
    """Returns (bias_str, spy_rsi, vix_price). bias_str is RISK_ON / RISK_OFF / NEUTRAL."""
    spy_df = await fetch_history("SPY", period="6mo")
    if spy_df is None or len(spy_df) < 60:
        return "NEUTRAL", None, None

    ema50 = spy_df["Close"].ewm(span=50).mean().iloc[-1]
    ema200 = spy_df["Close"].ewm(span=200).mean().iloc[-1]
    rsi = get_rsi_value(spy_df)

    vix_df = await fetch_history("^VIX", period="1mo")
    vix_price = float(vix_df["Close"].iloc[-1]) if vix_df is not None and not vix_df.empty else None

    if rsi is None or vix_price is None:
        return "NEUTRAL", rsi, vix_price

    if ema50 > ema200 and rsi > 50 and vix_price < 18:
        return "RISK_ON", rsi, vix_price
    elif ema50 < ema200 or vix_price > 22:
        return "RISK_OFF", rsi, vix_price
    else:
        return "NEUTRAL", rsi, vix_price

def get_score(ema50, ema200, rsi, market_bias, signal):
    score = 0

    # --- TREND STRENGTH (0-3 pts) ---
    trend_strength = abs(ema50 - ema200) / ema200

    if trend_strength > 0.05:
        score += 3
    elif trend_strength > 0.02:
        score += 2
    else:
        score += 1

    # --- RSI TIMING (0-2 pts) ---
    if signal == "BULLISH":
        if 45 <= rsi <= 60:
            score += 2
        elif rsi < 70:
            score += 1
    elif signal == "BEARISH":
        if 40 <= rsi <= 55:
            score += 2
        elif rsi > 30:
            score += 1

    # --- MARKET ALIGNMENT (0-2 pts) ---
    if (signal == "BULLISH" and market_bias == "RISK_ON") or \
       (signal == "BEARISH" and market_bias == "RISK_OFF"):
        score += 2
    elif market_bias == "NEUTRAL":
        score += 1

    # --- STRUCTURE BONUS (0-2 pts) ---
    if trend_strength > 0.08:
        score += 2
    elif trend_strength > 0.05:
        score += 1

    return round(score, 1)

async def get_signal(symbol, df=None, persist=True):
    """
    Returns (signal, final_score, price, df, raw_score) or None.

    final_score is the momentum/freshness-adjusted number shown in alerts
    and trade plans. raw_score is the unadjusted 0-10 score from get_score()
    for this scan alone, returned so callers can compare raw-to-raw across
    scans (e.g. "did the underlying setup itself cross the threshold")
    rather than comparing a prior raw score against a momentum-boosted
    current final_score, which conflates two different things.

    persist controls whether this call writes its score back to SQLite as
    the new "last_score" baseline. The automated watchlist_checker loop
    should always persist (persist=True, the default) since it's the one
    walking time forward and building momentum/freshness off itself.
    Manual on-demand lookups (!score, !top) pass persist=False so a quick
    check doesn't reset the freshness clock or momentum baseline that the
    background loop is relying on.
    """
    if df is None:
        df = await fetch_history(symbol, period="6mo")

    if df is None or df.empty:
        return None

    price = float(df["Close"].iloc[-1])

    ema50 = df["Close"].ewm(span=50).mean().iloc[-1]
    ema200 = df["Close"].ewm(span=200).mean().iloc[-1]
    rsi = get_rsi_value(df)

    if rsi is None:
        return None

    if ema50 > ema200:
        signal = "BULLISH"
    elif ema50 < ema200:
        signal = "BEARISH"
    else:
        return None

    bias, _, _ = await get_market_bias()
    score = get_score(ema50, ema200, rsi, bias, signal)

    row = db_execute(
        "SELECT last_score, last_score_time FROM watchlist WHERE symbol = ?",
        (symbol,), fetch=True
    )
    prev_score = row[0][0] if row and row[0][0] is not None else score
    prev_score_time = row[0][1] if row and row[0][1] is not None else time.time()

    score_momentum = score - prev_score
    final_score = score + (score_momentum * 1.5)

    age = time.time() - prev_score_time
    freshness_penalty = min(age / 3600, 2)  # caps at 2 points over time
    final_score = round(final_score - freshness_penalty, 1)

    if persist:
        db_execute(
            "UPDATE watchlist SET last_score = ?, last_score_time = ? WHERE symbol = ?",
            (score, time.time(), symbol)
        )

    return signal, final_score, price, df, score

def build_trade_plan(signal, price, atr, score):
    if atr is None or atr <= 0:
        return None

    price = float(price)
    atr = float(atr)

    if score >= 8:
        stop_mult, target_mult = 1.5, 3.5   # big runners
    elif score >= 6:
        stop_mult, target_mult = 1.5, 2.5   # normal trades
    else:
        stop_mult, target_mult = 1.2, 2.0   # weaker setups

    if signal == "BULLISH":
        entry = price
        stop = price - (atr * stop_mult)
        target = price + (atr * target_mult)
    elif signal == "BEARISH":
        entry = price
        stop = price + (atr * stop_mult)
        target = price - (atr * target_mult)
    else:
        return None

    risk = abs(entry - stop)
    reward = abs(target - entry)

    if risk <= 0:
        return None

    rr = reward / risk
    return entry, stop, target, round(rr, 2)

# -------------------------------------------------
# Heatmap (ported from friend's bot, runs over the dynamic SQLite watchlist)
# -------------------------------------------------

def _generate_heatmap_sync(symbols):
    heatmap_data = []

    for symbol in symbols:
        try:
            df = yf.Ticker(symbol).history(period="2d")
            if len(df) < 2:
                continue

            prev_close = df["Close"].iloc[-2]
            current_close = df["Close"].iloc[-1]
            pct_change = ((current_close - prev_close) / prev_close) * 100

            heatmap_data.append({
                "symbol": symbol,
                "change": pct_change,
                "size": abs(pct_change) + 1
            })
        except Exception as e:
            log(f"Heatmap error for {symbol}: {e}")

    if not heatmap_data:
        log("No heatmap data found")
        return None

    heatmap_data.sort(key=lambda x: x["change"], reverse=True)

    sizes = [x["size"] for x in heatmap_data]
    labels = [f"{x['symbol']}\n{x['change']:+.2f}%" for x in heatmap_data]

    colors = []
    for x in heatmap_data:
        change = x["change"]
        if change >= 3:
            colors.append("#006400")
        elif change >= 1:
            colors.append("#228B22")
        elif change > 0:
            colors.append("#90EE90")
        elif change <= -3:
            colors.append("#8B0000")
        elif change <= -1:
            colors.append("#B22222")
        else:
            colors.append("#F08080")

    fig = plt.figure(figsize=(14, 8))
    squarify.plot(
        sizes=sizes,
        label=labels,
        color=colors,
        alpha=0.9,
        edgecolor="black",
        linewidth=2,
        text_kwargs={"fontsize": 15, "color": "black", "weight": "bold"}
    )
    plt.axis("off")
    plt.title("Market Heat Map", fontsize=18, weight="bold")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf

async def send_heatmap(channel):
    symbols = get_watchlist_symbols()
    if not symbols:
        return

    buf = await asyncio.to_thread(_generate_heatmap_sync, symbols)
    if buf is None:
        return

    await channel.send("\U0001f525 Market Heat Map")
    await channel.send(file=discord.File(buf, filename="heatmap.png"))

# -------------------------------------------------
# Earnings (ported from friend's bot, runs over the dynamic SQLite watchlist)
# -------------------------------------------------

async def get_earnings_this_week(symbol):
    def _check():
        try:
            if symbol in ("SPY", "QQQ", "NDX", "^VIX"):
                return None

            stock = yf.Ticker(symbol)
            cal = stock.calendar
            if not cal:
                return None

            earnings_date = cal.get("Earnings Date")
            if earnings_date is None:
                return None

            if isinstance(earnings_date, (list, tuple)):
                earnings_date = earnings_date[0]

            if hasattr(earnings_date, "date"):
                earnings_date = earnings_date.date()

            today = datetime.now(ZoneInfo("America/New_York")).date()
            end_date = today + timedelta(days=7)

            if today <= earnings_date <= end_date:
                return earnings_date
            return None
        except Exception as e:
            log(f"Earnings check failed for {symbol}: {e}")
            return None

    return await asyncio.to_thread(_check)

# -------------------------------------------------
# Charts
# -------------------------------------------------

def render_analysis_chart(df, symbol):
    """Render a price chart with 20/50 MAs. Returns a BytesIO PNG buffer."""
    plot_df = df.tail(180)
    close = plot_df["Close"]
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(plot_df.index, close, label="Close", linewidth=1.4)
    ax.plot(plot_df.index, ma20, label="MA20", linewidth=1.0)
    ax.plot(plot_df.index, ma50, label="MA50", linewidth=1.0)
    ax.set_title(f"{symbol} - Price & Moving Averages")
    ax.set_ylabel("Price ($)")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

def render_fib_chart(df, symbol, fib):
    """Render a price chart with Fibonacci levels drawn as horizontal lines."""
    close = df["Close"]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df.index, close, label="Close", linewidth=1.4)

    for label, level in fib.items():
        ax.axhline(level, linestyle="--", linewidth=0.8, alpha=0.6)
        ax.text(df.index[0], level, f" {label}", va="center", fontsize=8)

    ax.set_title(f"{symbol} - Fibonacci Retracement")
    ax.set_ylabel("Price ($)")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

async def make_chart(fn, *args):
    """Run a blocking chart render in a thread."""
    return await asyncio.to_thread(fn, *args)

def calculate_fibonacci_levels(df):
    high = df["High"].max()
    low = df["Low"].min()

    if high == low:
        return None

    diff = high - low
    levels = {
        "0.0%": high,
        "23.6%": high - 0.236 * diff,
        "38.2%": high - 0.382 * diff,
        "50.0%": high - 0.5 * diff,
        "61.8%": high - 0.618 * diff,
        "78.6%": high - 0.786 * diff,
        "100.0%": low
    }
    return {k: round(v, 2) for k, v in levels.items()}

# -------------------------------------------------
# Cross Detection (Golden / Death)
# -------------------------------------------------

def detect_cross(df):
    close = df["Close"]
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    if len(df) < 55:
        return None

    prev_20, prev_50 = ma20.iloc[-2], ma50.iloc[-2]
    curr_20, curr_50 = ma20.iloc[-1], ma50.iloc[-1]

    if prev_20 <= prev_50 and curr_20 > curr_50:
        return "golden"
    if prev_20 >= prev_50 and curr_20 < curr_50:
        return "death"
    return None

# -------------------------------------------------
# Events
# -------------------------------------------------

@bot.event
async def on_ready():
    init_db()
    if not watchlist_checker.is_running():
        watchlist_checker.start()
    if not market_status_checker.is_running():
        market_status_checker.start()
    if not monday_earnings_task.is_running():
        monday_earnings_task.start()
    print(f"Logged in as {bot.user}")

# -------------------------------------------------
# Commands
# -------------------------------------------------

@bot.command()
async def help(ctx):
    """Show the command list."""
    msg = (
        "**\U0001f4c8 Stock Bot Commands**\n\n"
        "`!add SYMBOL [percent]` - watch a stock; optional drop-alert percent if it's new (default 15)\n"
        "`!addmany SYMBOL1 SYMBOL2 ...` - watch several stocks at once\n"
        "`!remove SYMBOL` - stop watching a stock (drops off the board if you're the last watcher)\n"
        "`!threshold SYMBOL percent` - change a stock's drop-alert percent (shared with everyone watching it)\n"
        "`!watchlist` - list stocks you're watching\n"
        "`!watchlist all` - list every stock anyone is watching\n"
        "`!setbaseline SYMBOL` - reset a stock's baseline to its current price (shared)\n"
        "`!price SYMBOL` - quick current price and day change\n"
        "`!analyze SYMBOL` - trend, RSI, MACD, plus a chart\n"
        "`!fib SYMBOL` - Fibonacci retracement levels, plus a chart\n"
        "`!score SYMBOL` - setup score (0-10) and trade plan (entry/stop/target/R:R)\n"
        "`!bias` - current market bias (SPY trend, RSI, VIX)\n"
        "`!top` - top 3 scored setups on the watchlist right now\n\n"
        "**Automatic alerts** (every 5 min, during market hours): price drops from peak, "
        "golden/death crosses, RSI overbought/oversold, strong setup scores with trade plans, "
        "and 52-week high approach/breakout, posted in this channel tagging whoever is watching "
        "that symbol. Plus a daily heat map after close and a Monday earnings-this-week summary."
    )
    await ctx.send(msg)

@bot.command()
async def add(ctx, symbol: str, threshold: float = None):
    """Subscribe to a symbol. If it's already tracked, you just join the
    watchers; if it's new, optional threshold sets the shared drop-alert
    percent, e.g. !add AAPL 10"""
    symbol = symbol.upper()
    user_id = ctx.author.id

    existing = db_execute("SELECT threshold FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)

    if existing:
        if user_id in [int(u) for u in get_subscribers(symbol)]:
            return await ctx.send(f"\u26a0\ufe0f You're already watching {symbol}")
        add_subscription(symbol, user_id)
        thr = existing[0][0]
        return await ctx.send(
            f"\u2705 You're now watching {symbol} (shared alert at -{thr*100:.0f}% from peak)"
        )

    price = await fetch_current_price(symbol)
    if price is None:
        return await ctx.send("\u274c Invalid symbol or data unavailable, try again shortly")

    thr = DEFAULT_DROP_THRESHOLD if threshold is None else threshold / 100.0

    db_execute(
        "INSERT INTO watchlist (symbol, baseline, threshold, peak, added_by) VALUES (?, ?, ?, ?, ?)",
        (symbol, price, thr, price, str(user_id))
    )
    add_subscription(symbol, user_id)

    await ctx.send(f"\u2705 Added {symbol} at ${price:.2f} (alert at -{thr*100:.0f}% from peak)")

@bot.command()
async def addmany(ctx, *symbols: str):
    """Subscribe to multiple symbols at once, e.g. !addmany AAPL MSFT NVDA"""
    if not symbols:
        return await ctx.send("Usage: !addmany SYMBOL1 SYMBOL2 ...")

    user_id = ctx.author.id

    seen = set()
    requested = []
    for s in symbols:
        s = s.upper()
        if s not in seen:
            seen.add(s)
            requested.append(s)

    added, already, failed = [], [], []

    for symbol in requested:
        existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)

        if existing:
            if user_id in [int(u) for u in get_subscribers(symbol)]:
                already.append(symbol)
            else:
                add_subscription(symbol, user_id)
                added.append(symbol)
            continue

        price = await fetch_current_price(symbol)
        if price is None:
            failed.append(symbol)
            await asyncio.sleep(0.5)
            continue

        db_execute(
            "INSERT INTO watchlist (symbol, baseline, threshold, peak, added_by) VALUES (?, ?, ?, ?, ?)",
            (symbol, price, DEFAULT_DROP_THRESHOLD, price, str(user_id))
        )
        add_subscription(symbol, user_id)
        added.append(symbol)
        await asyncio.sleep(0.5)  # be gentle with the data source

    lines = []
    if added:
        lines.append(f"\u2705 Now watching: {', '.join(added)}")
    if already:
        lines.append(f"\u26a0\ufe0f Already watching: {', '.join(already)}")
    if failed:
        lines.append(f"\u274c Couldn't fetch: {', '.join(failed)}")

    await ctx.send("\n".join(lines) or "Nothing to add")

@bot.command()
async def threshold(ctx, symbol: str, percent: float):
    """Set the shared drop-alert percent for a symbol you're watching,
    e.g. !threshold AAPL 10"""
    symbol = symbol.upper()
    user_id = ctx.author.id

    existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)
    if not existing:
        return await ctx.send(f"\u26a0\ufe0f {symbol} is not in the watchlist")

    if user_id not in [int(u) for u in get_subscribers(symbol)]:
        return await ctx.send(f"\u26a0\ufe0f You're not watching {symbol}, use !add {symbol} first")

    thr = percent / 100.0
    db_execute("UPDATE watchlist SET threshold = ?, alerted = 0 WHERE symbol = ?", (thr, symbol))
    await ctx.send(f"\u2705 {symbol} drop threshold set to -{percent:.0f}% (shared with everyone watching it)")

@bot.command()
async def remove(ctx, symbol: str):
    """Unsubscribe from a symbol. If you're the last watcher, it drops off
    the watchlist entirely."""
    symbol = symbol.upper()
    user_id = ctx.author.id

    existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)
    if not existing:
        return await ctx.send(f"\u26a0\ufe0f {symbol} is not in the watchlist")

    if user_id not in [int(u) for u in get_subscribers(symbol)]:
        return await ctx.send(f"\u26a0\ufe0f You're not watching {symbol}")

    remaining = remove_subscription(symbol, user_id)

    if remaining:
        await ctx.send(f"\U0001f5d1\ufe0f You've stopped watching {symbol} (others still are)")
    else:
        await ctx.send(f"\U0001f5d1\ufe0f Removed {symbol} (no one left watching it)")

@bot.command()
async def watchlist(ctx, scope: str = None):
    """List symbols you're watching. Use '!watchlist all' to see everyone's."""
    show_all = scope is not None and scope.lower() == "all"

    if show_all:
        rows = db_execute(
            "SELECT symbol, baseline, threshold, peak, last_signal, last_score, added_by FROM watchlist",
            fetch=True
        )
    else:
        my_symbols = get_user_symbols(ctx.author.id)
        if not my_symbols:
            return await ctx.send("You're not watching anything yet. Try !add SYMBOL")
        placeholders = ",".join("?" for _ in my_symbols)
        rows = db_execute(
            f"SELECT symbol, baseline, threshold, peak, last_signal, last_score, added_by "
            f"FROM watchlist WHERE symbol IN ({placeholders})",
            tuple(my_symbols),
            fetch=True
        )

    if not rows:
        return await ctx.send("Empty watchlist")

    lines = []
    for s, b, t, peak, sig, score, added_by in rows:
        peak_str = f", peak ${peak:.2f}" if peak else ""
        score_str = f" | {sig} {score}/10" if sig else ""
        watchers = ""
        if show_all:
            n = len(get_subscribers(s))
            watchers = f" ({n} watching)"
        added_str = f" | added by <@{added_by}>" if added_by else ""
        lines.append(f"{s} - ${b:.2f} (alert -{t*100:.0f}%{peak_str}){score_str}{watchers}{added_str}")
    await ctx.send("\n".join(lines))

@bot.command()
async def setbaseline(ctx, symbol: str):
    """Reset the baseline (and peak) for a symbol to its current price.
    Affects everyone watching it."""
    symbol = symbol.upper()
    user_id = ctx.author.id

    existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)
    if not existing:
        return await ctx.send(f"\u26a0\ufe0f {symbol} is not in the watchlist")

    if user_id not in [int(u) for u in get_subscribers(symbol)]:
        return await ctx.send(f"\u26a0\ufe0f You're not watching {symbol}")

    price = await fetch_current_price(symbol)
    if price is None:
        return await ctx.send("\u274c Could not fetch current price")

    db_execute(
        "UPDATE watchlist SET baseline = ?, peak = ?, alerted = 0 WHERE symbol = ?",
        (price, price, symbol)
    )
    await ctx.send(f"\u2705 Baseline for {symbol} reset to ${price:.2f} (shared with everyone watching it)")

@bot.command()
async def price(ctx, symbol: str):
    """Quick current price and day change."""
    symbol = symbol.upper()
    current, prev = await fetch_quote(symbol)

    if current is None:
        return await ctx.send("\u274c Could not fetch price, check the symbol or try again")

    change = current - prev
    pct = (change / prev) * 100 if prev else 0
    arrow = "\U0001f7e2" if change >= 0 else "\U0001f534"

    await ctx.send(f"{arrow} {symbol}: ${current:.2f} ({change:+.2f}, {pct:+.2f}%)")

@bot.command()
async def analyze(ctx, symbol: str):
    symbol = symbol.upper()
    df = await fetch_history(symbol)

    if df is None or len(df) < 60:
        return await ctx.send("Not enough data, check the symbol or try again shortly")

    ind = calculate_indicators(df)
    chart = await make_chart(render_analysis_chart, df, symbol)
    file = discord.File(chart, filename=f"{symbol}_analysis.png")

    await ctx.send(
        f"\U0001f4ca {symbol}\n"
        f"Trend: {ind['trend']}\n"
        f"RSI: {ind['rsi']}\n"
        f"MACD: {ind['macd']} / {ind['signal']}",
        file=file
    )

@bot.command()
async def fib(ctx, symbol: str):
    symbol = symbol.upper()
    df = await fetch_history(symbol)

    if df is None or len(df) < 20:
        return await ctx.send("\u274c Not enough data, check the symbol or try again shortly")

    df = df.tail(90)
    fib = calculate_fibonacci_levels(df)

    if fib is None:
        return await ctx.send("\u274c Could not calculate Fibonacci levels")

    price_val = await fetch_current_price(symbol)
    price_str = f"${price_val:.2f}" if price_val else "N/A"

    chart = await make_chart(render_fib_chart, df, symbol, fib)
    file = discord.File(chart, filename=f"{symbol}_fib.png")

    msg = (
        f"\U0001f9ee Fibonacci \u2014 {symbol}\n"
        f"Current Price: {price_str}\n\n"
        f"0%   : {fib['0.0%']}\n"
        f"23.6%: {fib['23.6%']}\n"
        f"38.2%: {fib['38.2%']}\n"
        f"50%  : {fib['50.0%']}\n"
        f"61.8%: {fib['61.8%']}\n"
        f"78.6%: {fib['78.6%']}\n"
        f"100% : {fib['100.0%']}"
    )
    await ctx.send(msg, file=file)

@bot.command()
async def bias(ctx):
    """Current market bias (SPY trend, RSI, VIX)."""
    b, rsi, vix = await get_market_bias()
    rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
    vix_str = f"{vix:.2f}" if vix is not None else "N/A"
    await ctx.send(f"\U0001f4c8 Market Bias: {b}\nSPY RSI: {rsi_str}\nVIX: {vix_str}")

@bot.command()
async def score(ctx, symbol: str):
    """Setup score (0-10) and trade plan for a symbol."""
    symbol = symbol.upper()
    result = await get_signal(symbol, persist=False)

    if result is None:
        return await ctx.send(f"\u274c Could not score {symbol}, check the symbol or try again")

    signal, final_score, price, df, raw_score = result
    atr = get_atr(df)
    plan = build_trade_plan(signal, price, atr, final_score)

    msg = f"\U0001f4ca {symbol} {signal} ({final_score}/10)\nPrice: ${price:.2f}"
    if plan:
        entry, stop, target, rr = plan
        msg += f"\nEntry: {entry:.2f} | Stop: {stop:.2f} | Target: {target:.2f}\nR:R 1:{rr}"

    await ctx.send(msg)

@bot.command()
async def top(ctx):
    """Top 3 scored setups on the watchlist right now."""
    symbols = get_watchlist_symbols()
    if not symbols:
        return await ctx.send("Watchlist is empty")

    setups = []
    for symbol in symbols:
        result = await get_signal(symbol, persist=False)
        if result is None:
            continue
        signal, final_score, price, df, raw_score = result
        setups.append((symbol, signal, final_score, price, df))
        await asyncio.sleep(0.3)

    if not setups:
        return await ctx.send("No scoreable setups right now")

    setups.sort(key=lambda x: x[2], reverse=True)
    top3 = setups[:3]

    lines = ["\U0001f525 Top Setups:"]
    for i, (symbol, signal, final_score, price, df) in enumerate(top3, 1):
        atr = get_atr(df)
        plan = build_trade_plan(signal, price, atr, final_score)
        lines.append(f"\n{i}. {symbol} {signal} ({final_score}/10)")
        if plan:
            entry, stop, target, rr = plan
            lines.append(f"Entry: {entry:.2f} | Stop: {stop:.2f} | Target: {target:.2f} | R:R 1:{rr}")

    await ctx.send("\n".join(lines))

# -------------------------------------------------
# Background Task: main watchlist checker (every 5 min)
# -------------------------------------------------

@tasks.loop(minutes=5)
async def watchlist_checker():
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)

    # --- daily heat map, once per day after close. Checked BEFORE the
    # market-hours gate below, since this is specifically meant to fire
    # once the market has closed, not while it's still open. ---
    now_et = datetime.now(ZoneInfo("America/New_York"))
    today_str = now_et.date().isoformat()
    last_heatmap_date = get_meta("last_heatmap_date")

    if (last_heatmap_date != today_str and now_et.hour >= HEATMAP_HOUR_ET
            and now_et.weekday() < 5):  # Mon-Fri only
        await send_heatmap(channel)
        set_meta("last_heatmap_date", today_str)

    if RESPECT_MARKET_HOURS and not market_is_open():
        return

    stocks = db_execute(
        "SELECT symbol, baseline, threshold, peak, alerted, golden_alerted, "
        "death_alerted, rsi_high_alerted, rsi_low_alerted, fail_count, "
        "last_signal, last_score, last_score_alert_time, pre_break_alerted, breakout_alerted "
        "FROM watchlist",
        fetch=True
    )

    top_setups = []

    for (symbol, baseline, thr, peak, alerted, g_flag, d_flag,
         rsi_hi_flag, rsi_lo_flag, fail_count, prev_signal, prev_score_recorded,
         last_score_alert_time, pre_break_flag, breakout_flag) in stocks:
        try:
            # PRICE
            price_now = await fetch_current_price(symbol)
            if price_now is None:
                new_fail = (fail_count or 0) + 1
                db_execute("UPDATE watchlist SET fail_count = ? WHERE symbol = ?", (new_fail, symbol))
                if new_fail == FAIL_LIMIT:
                    await send_alert(
                        channel, symbol,
                        f"\u2753 {symbol} has failed {FAIL_LIMIT} fetches in a row. "
                        f"It may be delisted or mistyped. Consider !remove {symbol}."
                    )
                continue

            if fail_count:
                db_execute("UPDATE watchlist SET fail_count = 0 WHERE symbol = ?", (symbol,))

            # TRAILING PEAK
            current_peak = peak if peak else baseline
            if price_now > current_peak:
                current_peak = price_now
                db_execute("UPDATE watchlist SET peak = ? WHERE symbol = ?", (current_peak, symbol))

            # PRICE DROP from trailing peak
            drop = (current_peak - price_now) / current_peak

            if drop >= thr:
                if not alerted:
                    await send_alert(
                        channel, symbol,
                        f"\U0001f6a8 {symbol} dropped {drop*100:.1f}% from its recent peak "
                        f"(${current_peak:.2f} -> ${price_now:.2f})"
                    )
                    db_execute("UPDATE watchlist SET alerted = 1 WHERE symbol = ?", (symbol,))
            else:
                if alerted:
                    db_execute("UPDATE watchlist SET alerted = 0 WHERE symbol = ?", (symbol,))

            # HISTORY-BASED CHECKS (cached, 1y)
            df = await fetch_history(symbol, period="1y")
            if df is None or len(df) < 60:
                continue

            # CROSS CHECK
            cross = detect_cross(df)
            if cross == "golden" and not g_flag:
                await send_alert(channel, symbol, f"\U0001f680 GOLDEN CROSS: {symbol}")
                db_execute(
                    "UPDATE watchlist SET golden_alerted = 1, death_alerted = 0 WHERE symbol = ?",
                    (symbol,)
                )
            elif cross == "death" and not d_flag:
                await send_alert(channel, symbol, f"\U0001f480 DEATH CROSS: {symbol}")
                db_execute(
                    "UPDATE watchlist SET death_alerted = 1, golden_alerted = 0 WHERE symbol = ?",
                    (symbol,)
                )

            # RSI CHECK
            rsi_val = get_rsi_value(df)
            if rsi_val is not None:
                if rsi_val >= RSI_OVERBOUGHT and not rsi_hi_flag:
                    await send_alert(
                        channel, symbol,
                        f"\U0001f4c8 {symbol} RSI {rsi_val:.0f} (overbought, >= {RSI_OVERBOUGHT})"
                    )
                    db_execute("UPDATE watchlist SET rsi_high_alerted = 1 WHERE symbol = ?", (symbol,))
                elif rsi_val <= RSI_OVERSOLD and not rsi_lo_flag:
                    await send_alert(
                        channel, symbol,
                        f"\U0001f4c9 {symbol} RSI {rsi_val:.0f} (oversold, <= {RSI_OVERSOLD})"
                    )
                    db_execute("UPDATE watchlist SET rsi_low_alerted = 1 WHERE symbol = ?", (symbol,))
                else:
                    if rsi_hi_flag and rsi_val < RSI_OVERBOUGHT:
                        db_execute("UPDATE watchlist SET rsi_high_alerted = 0 WHERE symbol = ?", (symbol,))
                    if rsi_lo_flag and rsi_val > RSI_OVERSOLD:
                        db_execute("UPDATE watchlist SET rsi_low_alerted = 0 WHERE symbol = ?", (symbol,))

            # 52-WEEK HIGH / BREAKOUT CHECK
            if len(df) >= 2:
                prior_52w_high = df["High"].iloc[:-1].max()
                today_high = df["High"].iloc[-1]
                current_close = df["Close"].iloc[-1]

                near_high = current_close >= prior_52w_high * 0.98
                is_breakout = today_high >= prior_52w_high

                if near_high and not pre_break_flag:
                    await send_alert(
                        channel, symbol,
                        f"\u26a0\ufe0f {symbol} approaching 52-week high\n"
                        f"Price: {current_close:.2f} | 52W High: {prior_52w_high:.2f}"
                    )
                    db_execute("UPDATE watchlist SET pre_break_alerted = 1 WHERE symbol = ?", (symbol,))
                elif not near_high and pre_break_flag:
                    db_execute("UPDATE watchlist SET pre_break_alerted = 0 WHERE symbol = ?", (symbol,))

                if is_breakout and not breakout_flag:
                    await send_alert(
                        channel, symbol,
                        f"\U0001f6a8 {symbol} NEW 52-WEEK HIGH\n"
                        f"Today High: {today_high:.2f} | Prior 52W High: {prior_52w_high:.2f}"
                    )
                    db_execute("UPDATE watchlist SET breakout_alerted = 1 WHERE symbol = ?", (symbol,))
                elif not is_breakout and breakout_flag:
                    db_execute("UPDATE watchlist SET breakout_alerted = 0 WHERE symbol = ?", (symbol,))

            # SETUP SCORE / TRADE PLAN CHECK
            # Capture the score on record BEFORE calling get_signal, since
            # get_signal() itself overwrites last_score/last_score_time as
            # part of its momentum/freshness calc. Comparing against this
            # captured value (not a post-hoc re-SELECT) is what makes
            # became_strong/score_jump actually detect real changes run
            # over run, instead of only firing once on a symbol's first scan.
            #
            # became_strong/score_jump compare prev_score_recorded (last
            # cycle's RAW score) against raw_score (this cycle's RAW score),
            # not final_score, since final_score already has a momentum
            # bonus and freshness penalty baked in. Comparing raw-to-final
            # would conflate "the setup genuinely got stronger" with "the
            # momentum math gave it a boost," and could fire on momentum
            # alone without the underlying score having moved much.
            # final_score is still what's shown in the alert and what gates
            # whether a score even counts as a "top setup" in the first
            # place, since that's the number actually being acted on.
            signal_result = await get_signal(symbol, df=df.tail(126))  # ~6mo for EMA stability
            if signal_result:
                signal, final_score, price, sig_df, raw_score = signal_result

                if final_score >= SCORE_ALERT_THRESHOLD:
                    top_setups.append((symbol, signal, final_score, price, sig_df))

                signal_changed = signal != prev_signal

                if prev_score_recorded is None:
                    # never scored before, treat any qualifying score as new
                    became_strong = final_score >= SCORE_ALERT_THRESHOLD
                    score_jump = 0.0
                else:
                    became_strong = prev_score_recorded < SCORE_ALERT_THRESHOLD and raw_score >= SCORE_ALERT_THRESHOLD
                    score_jump = raw_score - prev_score_recorded

                should_alert = (
                    (signal_changed and final_score >= SCORE_ALERT_THRESHOLD) or
                    became_strong or
                    (score_jump >= 1.0 and final_score >= SCORE_ALERT_THRESHOLD)
                )

                cooldown_ok = (
                    last_score_alert_time is None or
                    (time.time() - last_score_alert_time) >= SCORE_ALERT_COOLDOWN
                )

                if should_alert and cooldown_ok:
                    atr = get_atr(sig_df)
                    plan = build_trade_plan(signal, price, atr, final_score)
                    if plan:
                        entry, stop, target, rr = plan
                        await send_alert(
                            channel, symbol,
                            f"\U0001f6a8 {symbol} {signal} ({final_score}/10)\n"
                            f"Entry: {entry:.2f}\nStop: {stop:.2f}\nTarget: {target:.2f}\n"
                            f"R:R: 1:{rr}"
                        )
                        db_execute(
                            "UPDATE watchlist SET last_signal = ?, last_score_alert_time = ? WHERE symbol = ?",
                            (signal, time.time(), symbol)
                        )
                elif signal != prev_signal:
                    db_execute("UPDATE watchlist SET last_signal = ? WHERE symbol = ?", (signal, symbol))

        except Exception as e:
            print(f"[watchlist_checker] Error processing {symbol}: {e}")

        await asyncio.sleep(0.5)

    # --- daily "Top Setups" digest, once per day at/after 8:35 AM Central ---
    # Uses a 10-minute catch window (8:35-8:45 CT) since watchlist_checker
    # ticks every 5 minutes from whenever the bot happened to start, not
    # aligned to the clock, so a single exact-minute check could be missed.
    now_ct = datetime.now(ZoneInfo("America/Chicago"))
    today_ct_str = now_ct.date().isoformat()
    last_top_setups_date = get_meta("last_top_setups_date")

    in_daily_window = (
        now_ct.hour == 8 and 35 <= now_ct.minute < 45
    )

    if last_top_setups_date != today_ct_str and in_daily_window and top_setups:
        top_setups.sort(key=lambda x: x[2], reverse=True)
        top3 = top_setups[:3]

        message = "\U0001f525 Top Setups of the morning:\n"
        for i, (symbol, signal, final_score, price, sig_df) in enumerate(top3, 1):
            atr = get_atr(sig_df)
            plan = build_trade_plan(signal, price, atr, final_score)
            if plan:
                entry, stop, target, rr = plan
                message += (
                    f"\n{i}. {symbol} {signal} ({final_score}/10)\n"
                    f"Entry: {entry:.2f} | Stop: {stop:.2f} | Target: {target:.2f}\n"
                    f"R:R 1:{rr}\n"
                )

        await channel.send(message)
        set_meta("last_top_setups_date", today_ct_str)

@watchlist_checker.before_loop
async def before_loop():
    await bot.wait_until_ready()

# -------------------------------------------------
# Background Task: market open/close announcer (fast loop, no API calls)
# -------------------------------------------------

@tasks.loop(minutes=1)
async def market_status_checker():
    """Announce market open/close transitions. This is just a time check,
    so running every minute adds no data-source load."""
    global _market_open_state

    is_open = market_is_open()

    if _market_open_state is None:
        _market_open_state = is_open
        return

    if is_open == _market_open_state:
        return

    _market_open_state = is_open
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)

    if is_open:
        await send_broadcast(channel, "\U0001f7e2 Market is now OPEN")
    else:
        await send_broadcast(channel, "\U0001f534 Market is now CLOSED")

@market_status_checker.before_loop
async def before_market_loop():
    await bot.wait_until_ready()

# -------------------------------------------------
# Background Task: weekly earnings summary (Mondays 8:30am ET)
# -------------------------------------------------

@tasks.loop(seconds=60)
async def monday_earnings_task():
    """Checks every minute whether it's time to run the weekly earnings
    summary, and runs it once per week at the target time."""
    now = datetime.now(ZoneInfo("America/New_York"))

    last_run_date = get_meta("last_earnings_run_date")
    today_str = now.date().isoformat()

    is_target_time = now.weekday() == 0 and now.hour == 8 and now.minute == 30  # Monday 8:30am

    if not is_target_time or last_run_date == today_str:
        return

    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)
    symbols = get_watchlist_symbols()

    earnings_list = []
    for symbol in symbols:
        earnings_date = await get_earnings_this_week(symbol)
        if earnings_date:
            earnings_list.append((symbol, earnings_date))
        await asyncio.sleep(0.3)

    if earnings_list:
        earnings_list.sort(key=lambda x: x[1])
        message = "\U0001f4c5 Earnings This Week (Watchlist)\n"
        current_day = None

        for symbol, earnings_date in earnings_list:
            day_header = earnings_date.strftime("%A (%-m/%-d)")
            if day_header != current_day:
                current_day = day_header
                message += f"\n\n{day_header}\n"
            message += f"- {symbol}\n"
    else:
        message = "\U0001f4c5 No earnings found this week."

    await channel.send(message)
    set_meta("last_earnings_run_date", today_str)

@monday_earnings_task.before_loop
async def before_earnings_loop():
    await bot.wait_until_ready()

# -------------------------------------------------
# Run
# -------------------------------------------------

bot.run(TOKEN)
