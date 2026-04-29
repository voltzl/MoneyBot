"""
Discord Stock Watchlist & Technical Analysis Bot

Features:
- Add/remove stocks to a persistent watchlist (SQLite)
- Automatic price-drop alerts
- Technical analysis on demand:
  - Trend (20/50 MA)
  - RSI (14)
  - MACD (12, 26, 9)
  - Bullish / Bearish / Neutral bias
- Async-safe (non-blocking) data fetching
"""

import os
import sqlite3
import asyncio

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

import pandas as pd
import yfinance as yf

# -------------------------------------------------
# Configuration
# -------------------------------------------------

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

DB_FILE = "watchlist.db"
DROP_THRESHOLD = 0.15  # 15% price drop alert

# -------------------------------------------------
# Discord Bot Setup
# -------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------------------------------
# Database Utilities
# -------------------------------------------------

def init_db():
    """Create watchlist database if it does not exist."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                baseline REAL NOT NULL,
                alerted INTEGER DEFAULT 0
            )
        """)

def db_execute(query, params=(), fetch=False):
    """Execute SQLite queries safely."""
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchall() if fetch else None

# -------------------------------------------------
# Data Fetch Helpers (Async-Safe)
# -------------------------------------------------

async def fetch_current_price(symbol: str):
    """Fetch latest closing price without blocking the event loop."""
    def _fetch():
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d")
        if df.empty:
            return None
        return float(df["Close"].iloc[-1])

    return await asyncio.to_thread(_fetch)

async def fetch_history(symbol: str, period="3mo"):
    """Fetch historical price data."""
    def _fetch():
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period)
        return df if not df.empty else None

    return await asyncio.to_thread(_fetch)

# -------------------------------------------------
# Technical Indicator Calculations
# -------------------------------------------------

def calculate_indicators(df: pd.DataFrame):
    close = df["Close"]

    # ----- RSI (14) -----
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # ----- MACD (12, 26, 9) -----
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()

    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()

    # ----- Trend (20 / 50 MA) -----
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
# Bot Events
# -------------------------------------------------

@bot.event
async def on_ready():
    init_db()
    watchlist_checker.start()
    print(f"✅ Logged in as {bot.user}")

# -------------------------------------------------
# Commands
# -------------------------------------------------

@bot.command()
async def add(ctx, symbol: str):
    """Add a stock to the watchlist."""
    symbol = symbol.upper()

    if db_execute(
        "SELECT 1 FROM watchlist WHERE symbol = ?",
        (symbol,),
        fetch=True
    ):
        await ctx.send(f"⚠️ **{symbol}** is already being tracked.")
        return

    price = await fetch_current_price(symbol)
    if price is None:
        await ctx.send("❌ Invalid stock symbol.")
        return

    db_execute(
        "INSERT INTO watchlist (symbol, baseline, alerted) VALUES (?, ?, 0)",
        (symbol, price)
    )

    await ctx.send(f"✅ Added **{symbol}** at baseline **${price:.2f}**")

@bot.command()
async def remove(ctx, symbol: str):
    """Remove a stock from the watchlist."""
    symbol = symbol.upper()
    db_execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
    await ctx.send(f"🗑️ Removed **{symbol}** from the watchlist.")

@bot.command()
async def watchlist(ctx):
    """Show tracked stocks."""
    rows = db_execute(
        "SELECT symbol, baseline FROM watchlist",
        fetch=True
    )

    if not rows:
        await ctx.send("📭 Watchlist is empty.")
        return

    msg = "**📈 Watchlist:**\n"
    for symbol, baseline in rows:
        msg += f"- **{symbol}** — baseline ${baseline:.2f}\n"

    await ctx.send(msg)

@bot.command()
async def reset(ctx, symbol: str):
    """Reset alert status for a stock."""
    symbol = symbol.upper()
    db_execute(
        "UPDATE watchlist SET alerted = 0 WHERE symbol = ?",
        (symbol,)
    )
    await ctx.send(f"🔁 Alerts reset for **{symbol}**")

@bot.command()
async def analyze(ctx, symbol: str):
    """Analyze trend, RSI, MACD, and sentiment."""
    symbol = symbol.upper()

    df = await fetch_history(symbol)
    if df is None or len(df) < 60:
        await ctx.send("❌ Not enough data to analyze.")
        return

    ind = calculate_indicators(df)

    bullish = 0
    bearish = 0

    # RSI vote
    if ind["rsi"] > 50:
        bullish += 1
    else:
        bearish += 1

    # MACD vote
    if ind["macd"] > ind["signal"]:
        bullish += 1
    else:
        bearish += 1

    # Trend vote
    if ind["trend"] == "Uptrend":
        bullish += 1
    elif ind["trend"] == "Downtrend":
        bearish += 1

    sentiment = "Neutral ⚪"
    if bullish >= 2:
        sentiment = "Bullish 📈"
    elif bearish >= 2:
        sentiment = "Bearish 📉"

    await ctx.send(
        f"📊 **{symbol} Technical Analysis**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔹 Trend: **{ind['trend']}**\n"
        f"🔹 RSI (14): **{ind['rsi']}**\n"
        f"🔹 MACD: **{ind['macd']}**\n"
        f"🔹 Signal: **{ind['signal']}**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🧭 Overall Bias: **{sentiment}**"
    )

# -------------------------------------------------
# Background Alert Task
# -------------------------------------------------

@tasks.loop(minutes=15)
async def watchlist_checker():
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)

    stocks = db_execute(
        "SELECT symbol, baseline FROM watchlist WHERE alerted = 0",
        fetch=True
    )

    for symbol, baseline in stocks:
        current_price = await fetch_current_price(symbol)
        if current_price is None:
            continue

        drop = (baseline - current_price) / baseline

        if drop >= DROP_THRESHOLD:
            await channel.send(
                f"🚨 **PRICE ALERT: {symbol}**\n"
                f"📉 Drop: **{drop * 100:.1f}%**\n"
                f"💲 Baseline: ${baseline:.2f}\n"
                f"💰 Current: ${current_price:.2f}"
            )

            db_execute(
                "UPDATE watchlist SET alerted = 1 WHERE symbol = ?",
                (symbol,)
            )

@watchlist_checker.before_loop
async def before_watchlist_checker():
    await bot.wait_until_ready()

# -------------------------------------------------
# Run Bot
# -------------------------------------------------

bot.run(TOKEN)
