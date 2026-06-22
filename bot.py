"""
Discord Stock Watchlist & Technical Analysis Bot

Features:
- Watchlist (SQLite)
- Price drop alerts with re-alert protection and auto-reset on recovery
- Per-symbol custom drop thresholds
- Golden Cross / Death Cross alerts (20/50 MA) with proper flag resets
- RSI, MACD, Trend analysis
- Fibonacci retracement command (!fib)
- Quick !price command
- Charts attached to !analyze and !fib
- Role/user mention on alerts
- Persistent alert tracking
- Error handling in background task
"""

import os
import io
import sqlite3
import asyncio

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

import pandas as pd
import yfinance as yf

import matplotlib
matplotlib.use("Agg")  # headless backend, no display needed
import matplotlib.pyplot as plt

# -------------------------------------------------
# Config
# -------------------------------------------------

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

# Optional: a role or user ID to ping on alerts. Leave unset to disable.
ALERT_MENTION_ID = os.getenv("ALERT_MENTION_ID")
ALERT_MENTION_TYPE = os.getenv("ALERT_MENTION_TYPE", "role")  # "role" or "user"

DB_FILE = "watchlist.db"
DEFAULT_DROP_THRESHOLD = 0.15  # 15%

# -------------------------------------------------
# Bot Setup
# -------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------------------------------
# Mention Helper
# -------------------------------------------------

def alert_prefix():
    """Return a mention string to prepend to alerts, or empty string if disabled."""
    if not ALERT_MENTION_ID:
        return ""
    if ALERT_MENTION_TYPE == "user":
        return f"<@{ALERT_MENTION_ID}> "
    return f"<@&{ALERT_MENTION_ID}> "

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
                alerted INTEGER DEFAULT 0,
                golden_alerted INTEGER DEFAULT 0,
                death_alerted INTEGER DEFAULT 0
            )
        """)
        # Migrate older databases that predate the threshold column.
        cols = [r[1] for r in conn.execute("PRAGMA table_info(watchlist)").fetchall()]
        if "threshold" not in cols:
            conn.execute(
                "ALTER TABLE watchlist ADD COLUMN threshold REAL DEFAULT 0.15"
            )

def db_execute(query, params=(), fetch=False):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchall() if fetch else None

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

async def fetch_history(symbol):
    def _fetch():
        df = yf.Ticker(symbol).history(period="1y")
        return df if not df.empty else None
    return await asyncio.to_thread(_fetch)

# -------------------------------------------------
# Indicators
# -------------------------------------------------

def calculate_indicators(df):
    close = df["Close"]

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    rs = gain.rolling(14).mean() / loss.rolling(14).mean()
    rsi = 100 - (100 / (1 + rs))

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
        "rsi": round(rsi.iloc[-1], 2),
        "macd": round(macd.iloc[-1], 4),
        "signal": round(signal.iloc[-1], 4),
        "trend": trend
    }

# -------------------------------------------------
# Fibonacci
# -------------------------------------------------

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
    watchlist_checker.start()
    print(f"Logged in as {bot.user}")

# -------------------------------------------------
# Commands
# -------------------------------------------------

@bot.command()
async def add(ctx, symbol: str, threshold: float = None):
    """Add a symbol. Optional threshold is a drop percent, e.g. !add AAPL 10"""
    symbol = symbol.upper()

    if db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True):
        return await ctx.send("\u26a0\ufe0f Already in watchlist")

    price = await fetch_current_price(symbol)
    if price is None:
        return await ctx.send("\u274c Invalid symbol or data unavailable, try again shortly")

    # Threshold comes in as a percent (10 = 10%); store as a fraction.
    thr = DEFAULT_DROP_THRESHOLD if threshold is None else threshold / 100.0

    db_execute(
        "INSERT INTO watchlist (symbol, baseline, threshold) VALUES (?, ?, ?)",
        (symbol, price, thr)
    )

    await ctx.send(f"\u2705 Added {symbol} at ${price:.2f} (alert at -{thr*100:.0f}%)")

@bot.command()
async def threshold(ctx, symbol: str, percent: float):
    """Set a custom drop threshold for a symbol, e.g. !threshold AAPL 10"""
    symbol = symbol.upper()

    existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)
    if not existing:
        return await ctx.send(f"\u26a0\ufe0f {symbol} is not in the watchlist")

    thr = percent / 100.0
    db_execute(
        "UPDATE watchlist SET threshold = ?, alerted = 0 WHERE symbol = ?",
        (thr, symbol)
    )
    await ctx.send(f"\u2705 {symbol} drop threshold set to -{percent:.0f}%")

@bot.command()
async def remove(ctx, symbol: str):
    symbol = symbol.upper()
    existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)

    if not existing:
        return await ctx.send(f"\u26a0\ufe0f {symbol} is not in the watchlist")

    db_execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
    await ctx.send(f"\U0001f5d1\ufe0f Removed {symbol}")

@bot.command()
async def watchlist(ctx):
    rows = db_execute("SELECT symbol, baseline, threshold FROM watchlist", fetch=True)

    if not rows:
        return await ctx.send("Empty watchlist")

    msg = "\n".join([f"{s} - ${b:.2f} (alert -{t*100:.0f}%)" for s, b, t in rows])
    await ctx.send(msg)

@bot.command()
async def setbaseline(ctx, symbol: str):
    """Reset the baseline for a symbol to its current price."""
    symbol = symbol.upper()

    existing = db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True)
    if not existing:
        return await ctx.send(f"\u26a0\ufe0f {symbol} is not in the watchlist")

    price = await fetch_current_price(symbol)
    if price is None:
        return await ctx.send("\u274c Could not fetch current price")

    db_execute(
        "UPDATE watchlist SET baseline = ?, alerted = 0 WHERE symbol = ?",
        (price, symbol)
    )

    await ctx.send(f"\u2705 Baseline for {symbol} reset to ${price:.2f}")

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

    await ctx.send(
        f"{arrow} {symbol}: ${current:.2f} "
        f"({change:+.2f}, {pct:+.2f}%)"
    )

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

# -------------------------------------------------
# Background Task
# -------------------------------------------------

@tasks.loop(minutes=15)
async def watchlist_checker():
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)
    ping = alert_prefix()

    stocks = db_execute(
        "SELECT symbol, baseline, threshold, alerted, golden_alerted, death_alerted FROM watchlist",
        fetch=True
    )

    for symbol, baseline, thr, alerted, g_flag, d_flag in stocks:
        try:
            # PRICE DROP
            price_now = await fetch_current_price(symbol)
            if price_now is None:
                continue

            drop = (baseline - price_now) / baseline

            if drop >= thr:
                if not alerted:
                    await channel.send(
                        f"{ping}\U0001f6a8 {symbol} dropped {drop*100:.1f}% from baseline"
                    )
                    db_execute(
                        "UPDATE watchlist SET alerted = 1 WHERE symbol = ?",
                        (symbol,)
                    )
            else:
                # Price recovered, reset so it can fire again on a future drop.
                if alerted:
                    db_execute(
                        "UPDATE watchlist SET alerted = 0 WHERE symbol = ?",
                        (symbol,)
                    )

            # CROSS CHECK
            df = await fetch_history(symbol)
            if df is None or len(df) < 60:
                continue

            signal = detect_cross(df)

            if signal == "golden" and not g_flag:
                await channel.send(f"{ping}\U0001f680 GOLDEN CROSS: {symbol}")
                db_execute(
                    "UPDATE watchlist SET golden_alerted = 1, death_alerted = 0 WHERE symbol = ?",
                    (symbol,)
                )

            elif signal == "death" and not d_flag:
                await channel.send(f"{ping}\U0001f480 DEATH CROSS: {symbol}")
                db_execute(
                    "UPDATE watchlist SET death_alerted = 1, golden_alerted = 0 WHERE symbol = ?",
                    (symbol,)
                )

        except Exception as e:
            print(f"[watchlist_checker] Error processing {symbol}: {e}")

        await asyncio.sleep(0.5)

@watchlist_checker.before_loop
async def before_loop():
    await bot.wait_until_ready()

# -------------------------------------------------
# Run
# -------------------------------------------------

bot.run(TOKEN)
