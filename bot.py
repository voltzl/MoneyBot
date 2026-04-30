"""
Discord Pro Market Scanner Bot

Features:
- S&P 500 universe scanner
- Market cap + volume filtering
- Golden / Death cross detection (20/50 MA)
- Setup scoring system
- Duplicate signal prevention
- Top setups ranking
- Discord alerts
"""

import os
import asyncio
import requests
import pandas as pd
import yfinance as yf

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

if not TOKEN:
    raise ValueError("TOKEN is not set in environment variables")

if not CHANNEL_ID:
    raise ValueError("CHANNEL_ID is not set in environment variables")

CHANNEL_ID = int(CHANNEL_ID)

# -------------------------------------------------
# BOT SETUP
# -------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------------------------------
# STATE
# -------------------------------------------------

last_signals = {}
SP500 = []

# -------------------------------------------------
# S&P 500 LOADER (FIXED 403 ISSUE)
# -------------------------------------------------

def load_sp500():
    """
    Loads S&P 500 tickers from Wikipedia safely using headers
    Prevents HTTP 403 on EC2 / cloud servers
    """
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        tables = pd.read_html(response.text)
        table = tables[0]

        symbols = table["Symbol"].str.replace(".", "-", regex=False).tolist()
        print(f"Loaded {len(symbols)} S&P 500 symbols")

        return symbols

    except Exception as e:
        print("Failed to load S&P 500 list:", e)
        return []

# -------------------------------------------------
# DATA FETCH
# -------------------------------------------------

async def fetch_history(symbol):
    def _fetch():
        try:
            df = yf.Ticker(symbol).history(period="1y")
            return df if not df.empty else None
        except:
            return None

    return await asyncio.to_thread(_fetch)

# -------------------------------------------------
# FILTERS
# -------------------------------------------------

async def quick_liquidity_check(symbol):
    def _fetch():
        try:
            t = yf.Ticker(symbol)
            info = t.info or {}

            return (
                (info.get("marketCap") or 0) >= 10_000_000 and
                (info.get("volume") or 0) >= 3_000_000
            )
        except:
            return False

    return await asyncio.to_thread(_fetch)

# -------------------------------------------------
# SCORING
# -------------------------------------------------

def score_setup(df):
    close = df["Close"]

    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    score = 0

    if ma20.iloc[-1] > ma50.iloc[-1]:
        score += 2

    if "Volume" in df.columns:
        vol = df["Volume"]
        if vol.iloc[-1] > vol.rolling(20).mean().iloc[-1]:
            score += 1

    return score

# -------------------------------------------------
# CROSS DETECTION
# -------------------------------------------------

def detect_cross(df):
    if len(df) < 55:
        return None

    close = df["Close"]

    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    prev_20, prev_50 = ma20.iloc[-2], ma50.iloc[-2]
    curr_20, curr_50 = ma20.iloc[-1], ma50.iloc[-1]

    if prev_20 <= prev_50 and curr_20 > curr_50:
        return "golden"

    if prev_20 >= prev_50 and curr_20 < curr_50:
        return "death"

    return None

# -------------------------------------------------
# SCAN ENGINE
# -------------------------------------------------

async def scan_symbol(symbol):
    df = await fetch_history(symbol)

    if df is None or len(df) < 60:
        return None

    signal = detect_cross(df)
    score = score_setup(df)

    return symbol, signal, score

# -------------------------------------------------
# BOT READY
# -------------------------------------------------

@bot.event
async def on_ready():
    global SP500

    print(f"Logged in as {bot.user}")

    # Load S&P 500 AFTER bot is ready (safe + avoids startup lag)
    if not SP500:
        SP500 = load_sp500()

    if not scanner.is_running():
        scanner.start()

# -------------------------------------------------
# SCANNER LOOP
# -------------------------------------------------

@tasks.loop(minutes=15)
async def scanner():
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)

    if not SP500:
        print("SP500 list is empty, skipping scan")
        return

    candidates = []

    # STEP 1: FILTER UNIVERSE
    for symbol in SP500:
        ok = await quick_liquidity_check(symbol)
        if ok:
            candidates.append(symbol)

        await asyncio.sleep(0.05)

    results = []

    # STEP 2: SCAN SYMBOLS
    for symbol in candidates:
        res = await scan_symbol(symbol)
        if res:
            results.append(res)

        await asyncio.sleep(0.05)

    signals = []

    # STEP 3: PROCESS RESULTS
    for symbol, signal, score in results:

        if not signal:
            continue

        if last_signals.get(symbol) == signal:
            continue

        last_signals[symbol] = signal

        signals.append((symbol, signal, score))

        emoji = "🚀" if signal == "golden" else "💀"

        await channel.send(
            f"{emoji} **{signal.upper()} CROSS**\n"
            f"{symbol}\n"
            f"Score: {score}"
        )

    # STEP 4: TOP SETUPS
    top = sorted(signals, key=lambda x: x[2], reverse=True)[:5]

    if top:
        msg = "**🔥 TOP SETUPS THIS SCAN 🔥**\n\n" + "\n".join(
            [f"{s} ({sig}) Score: {sc}" for s, sig, sc in top]
        )

        await channel.send(msg)

# -------------------------------------------------
# RUN BOT
# -------------------------------------------------

bot.run(TOKEN)
