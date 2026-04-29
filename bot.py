"""
Discord Stock Watchlist & Technical Analysis Bot

Features:
- Watchlist (SQLite)
- Price drop alerts
- Golden Cross / Death Cross alerts (20/50 MA)
- RSI, MACD, Trend analysis
- Persistent alert tracking
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
# Config
# -------------------------------------------------

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

DB_FILE = "watchlist.db"
DROP_THRESHOLD = 0.15  # 15%

# -------------------------------------------------
# Bot Setup
# -------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------------------------------
# Database
# -------------------------------------------------

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                baseline REAL NOT NULL,
                alerted INTEGER DEFAULT 0,
                golden_alerted INTEGER DEFAULT 0,
                death_alerted INTEGER DEFAULT 0
            )
        """)

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
async def add(ctx, symbol: str):
    symbol = symbol.upper()

    if db_execute("SELECT 1 FROM watchlist WHERE symbol = ?", (symbol,), fetch=True):
        return await ctx.send("⚠️ Already in watchlist")

    price = await fetch_current_price(symbol)
    if price is None:
        return await ctx.send("❌ Invalid symbol")

    db_execute(
        "INSERT INTO watchlist (symbol, baseline) VALUES (?, ?)",
        (symbol, price)
    )

    await ctx.send(f"✅ Added {symbol} at ${price:.2f}")

@bot.command()
async def remove(ctx, symbol: str):
    db_execute("DELETE FROM watchlist WHERE symbol = ?", (symbol.upper(),))
    await ctx.send("🗑️ Removed")

@bot.command()
async def watchlist(ctx):
    rows = db_execute("SELECT symbol, baseline FROM watchlist", fetch=True)

    if not rows:
        return await ctx.send("Empty watchlist")

    msg = "\n".join([f"{s} - ${b:.2f}" for s, b in rows])
    await ctx.send(msg)

@bot.command()
async def analyze(ctx, symbol: str):
    df = await fetch_history(symbol.upper())

    if df is None or len(df) < 60:
        return await ctx.send("Not enough data")

    ind = calculate_indicators(df)

    await ctx.send(
        f"📊 {symbol.upper()}\n"
        f"Trend: {ind['trend']}\n"
        f"RSI: {ind['rsi']}\n"
        f"MACD: {ind['macd']} / {ind['signal']}"
    )

# -------------------------------------------------
# Background Task
# -------------------------------------------------

@tasks.loop(minutes=15)
async def watchlist_checker():
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)

    stocks = db_execute(
        "SELECT symbol, baseline, golden_alerted, death_alerted FROM watchlist",
        fetch=True
    )

    for symbol, baseline, g_flag, d_flag in stocks:

        # ---------------- PRICE DROP ----------------
        price = await fetch_current_price(symbol)
        if price is None:
            continue

        drop = (baseline - price) / baseline

        if drop >= DROP_THRESHOLD:
            await channel.send(
                f"🚨 {symbol} dropped {drop*100:.1f}%"
            )

        # ---------------- CROSS CHECK ----------------
        df = await fetch_history(symbol)
        if df is None or len(df) < 60:
            continue

        signal = detect_cross(df)

        if signal == "golden" and not g_flag:
            await channel.send(f"🚀 GOLDEN CROSS: {symbol}")

            db_execute(
                "UPDATE watchlist SET golden_alerted = 1 WHERE symbol = ?",
                (symbol,)
            )

        elif signal == "death" and not d_flag:
            await channel.send(f"💀 DEATH CROSS: {symbol}")

            db_execute(
                "UPDATE watchlist SET death_alerted = 1 WHERE symbol = ?",
                (symbol,)
            )

        await asyncio.sleep(0.5)

@watchlist_checker.before_loop
async def before_loop():
    await bot.wait_until_ready()

# -------------------------------------------------
# Run
# -------------------------------------------------

bot.run(TOKEN)
