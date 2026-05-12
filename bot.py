import os
import io
import time
import sqlite3
import asyncio
import logging
from datetime import datetime, timedelta

import discord
from discord.ext import commands, tasks
from discord import app_commands

from dotenv import load_dotenv

import pandas as pd
import yfinance as yf

import mplfinance as mpf

# =========================================================
# CONFIG
# =========================================================

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

DB_FILE = "watchlist.db"

DROP_THRESHOLD = 0.15
CACHE_TTL = 300
ALERT_COOLDOWN_HOURS = 24

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("stockbot")

# =========================================================
# DISCORD SETUP
# =========================================================

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents
)

# =========================================================
# CACHE + RATE LIMITING
# =========================================================

market_cache = {}

semaphore = asyncio.Semaphore(5)

# =========================================================
# DATABASE
# =========================================================

def init_db():
    with sqlite3.connect(DB_FILE) as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                baseline REAL NOT NULL,

                last_drop_alert TEXT,
                last_golden_alert TEXT,
                last_death_alert TEXT
            )
        """)

def db_execute(query, params=(), fetch=False):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(query, params)

        if fetch:
            return cur.fetchall()

        conn.commit()

# =========================================================
# HELPERS
# =========================================================

def should_alert(last_alert):

    if not last_alert:
        return True

    try:
        last = datetime.fromisoformat(last_alert)

        return (
            datetime.utcnow() - last
            > timedelta(hours=ALERT_COOLDOWN_HOURS)
        )

    except:
        return True

# =========================================================
# MARKET DATA
# =========================================================

async def fetch_history(symbol):

    def _fetch():
        return yf.Ticker(symbol).history(period="1y")

    return await asyncio.to_thread(_fetch)

async def fetch_current_price(symbol):

    def _fetch():
        df = yf.Ticker(symbol).history(period="1d")

        if df.empty:
            return None

        return float(df["Close"].iloc[-1])

    return await asyncio.to_thread(_fetch)

async def get_cached_history(symbol):

    now = time.time()

    if symbol in market_cache:

        data, ts = market_cache[symbol]

        if now - ts < CACHE_TTL:
            return data

    async with semaphore:

        try:
            df = await fetch_history(symbol)

            if df is not None and not df.empty:
                market_cache[symbol] = (df, now)

            return df

        except Exception as e:
            logger.error(f"{symbol} history fetch failed: {e}")
            return None

# =========================================================
# INDICATORS
# =========================================================

def calculate_atr(df, period=14):

    high_low = df["High"] - df["Low"]

    high_close = (
        df["High"] - df["Close"].shift()
    ).abs()

    low_close = (
        df["Low"] - df["Close"].shift()
    ).abs()

    tr = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    ).max(axis=1)

    return tr.rolling(period).mean()

def calculate_indicators(df):

    close = df["Close"]

    # RSI
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    rs = (
        gain.rolling(14).mean()
        / loss.rolling(14).mean()
    )

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

    # ATR
    atr = calculate_atr(df)

    # Volume confirmation
    volume_avg = df["Volume"].rolling(20).mean()

    high_volume = (
        df["Volume"].iloc[-1]
        > volume_avg.iloc[-1]
    )

    return {
        "rsi": round(rsi.iloc[-1], 2),
        "macd": round(macd.iloc[-1], 4),
        "signal": round(signal.iloc[-1], 4),
        "trend": trend,
        "atr": round(atr.iloc[-1], 2),
        "high_volume": high_volume
    }

# =========================================================
# FIBONACCI
# =========================================================

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

    return {
        k: round(v, 2)
        for k, v in levels.items()
    }

# =========================================================
# CROSS DETECTION
# =========================================================

def detect_cross(df):

    close = df["Close"]

    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    if len(df) < 55:
        return None

    prev_20 = ma20.iloc[-2]
    prev_50 = ma50.iloc[-2]

    curr_20 = ma20.iloc[-1]
    curr_50 = ma50.iloc[-1]

    if prev_20 <= prev_50 and curr_20 > curr_50:
        return "golden"

    if prev_20 >= prev_50 and curr_20 < curr_50:
        return "death"

    return None

# =========================================================
# CHART GENERATION
# =========================================================

async def generate_chart(symbol, df):

    buf = io.BytesIO()

    mpf.plot(
        df.tail(90),
        type="candle",
        style="yahoo",
        mav=(20, 50),
        volume=True,
        savefig=dict(
            fname=buf,
            dpi=100,
            bbox_inches="tight"
        )
    )

    buf.seek(0)

    return discord.File(
        buf,
        filename=f"{symbol}.png"
    )

# =========================================================
# EVENTS
# =========================================================

@bot.event
async def on_ready():

    init_db()

    await bot.tree.sync()

    watchlist_checker.start()

    logger.info(f"Logged in as {bot.user}")

# =========================================================
# PREFIX COMMANDS
# =========================================================

@bot.command()
async def add(ctx, symbol: str):

    symbol = symbol.upper()

    exists = db_execute(
        "SELECT 1 FROM watchlist WHERE symbol = ?",
        (symbol,),
        fetch=True
    )

    if exists:
        return await ctx.send(
            "⚠️ Already in watchlist"
        )

    price = await fetch_current_price(symbol)

    if price is None:
        return await ctx.send(
            "❌ Invalid symbol"
        )

    db_execute(
        """
        INSERT INTO watchlist
        (symbol, baseline)
        VALUES (?, ?)
        """,
        (symbol, price)
    )

    await ctx.send(
        f"✅ Added {symbol} at ${price:.2f}"
    )

@bot.command()
async def remove(ctx, symbol: str):

    symbol = symbol.upper()

    db_execute(
        "DELETE FROM watchlist WHERE symbol = ?",
        (symbol,)
    )

    await ctx.send("🗑️ Removed")

@bot.command()
async def watchlist(ctx):

    rows = db_execute(
        "SELECT symbol, baseline FROM watchlist",
        fetch=True
    )

    if not rows:
        return await ctx.send(
            "Empty watchlist"
        )

    embed = discord.Embed(
        title="📈 Watchlist",
        color=discord.Color.blurple()
    )

    for symbol, baseline in rows:

        embed.add_field(
            name=symbol,
            value=f"${baseline:.2f}",
            inline=True
        )

    await ctx.send(embed=embed)

@bot.command()
async def analyze(ctx, symbol: str):

    symbol = symbol.upper()

    try:

        df = await get_cached_history(symbol)

        if df is None or len(df) < 60:
            return await ctx.send(
                "❌ Not enough data"
            )

        ind = calculate_indicators(df)

        embed = discord.Embed(
            title=f"📊 {symbol} Analysis",
            color=discord.Color.green()
        )

        embed.add_field(
            name="Trend",
            value=ind["trend"]
        )

        embed.add_field(
            name="RSI",
            value=ind["rsi"]
        )

        embed.add_field(
            name="MACD",
            value=ind["macd"]
        )

        embed.add_field(
            name="Signal",
            value=ind["signal"]
        )

        embed.add_field(
            name="ATR",
            value=ind["atr"]
        )

        embed.add_field(
            name="Volume",
            value=(
                "High"
                if ind["high_volume"]
                else "Normal"
            )
        )

        chart = await generate_chart(symbol, df)

        embed.set_image(
            url=f"attachment://{symbol}.png"
        )

        await ctx.send(
            embed=embed,
            file=chart
        )

    except Exception as e:

        logger.exception(e)

        await ctx.send(
            "❌ Analysis failed"
        )

@bot.command()
async def fib(ctx, symbol: str):

    symbol = symbol.upper()

    try:

        df = await get_cached_history(symbol)

        if df is None or len(df) < 20:
            return await ctx.send(
                "❌ Not enough data"
            )

        df = df.tail(90)

        fib = calculate_fibonacci_levels(df)

        if fib is None:
            return await ctx.send(
                "❌ Could not calculate Fibonacci"
            )

        price = await fetch_current_price(symbol)

        embed = discord.Embed(
            title=f"🧮 Fibonacci — {symbol}",
            color=discord.Color.orange()
        )

        embed.add_field(
            name="Current Price",
            value=f"${price:.2f}"
        )

        for k, v in fib.items():

            embed.add_field(
                name=k,
                value=v,
                inline=True
            )

        chart = await generate_chart(symbol, df)

        embed.set_image(
            url=f"attachment://{symbol}.png"
        )

        await ctx.send(
            embed=embed,
            file=chart
        )

    except Exception as e:

        logger.exception(e)

        await ctx.send(
            "❌ Fibonacci failed"
        )

# =========================================================
# SLASH COMMANDS
# =========================================================

@bot.tree.command(
    name="analyze",
    description="Analyze a stock"
)
async def slash_analyze(
    interaction: discord.Interaction,
    symbol: str
):

    await interaction.response.defer()

    symbol = symbol.upper()

    try:

        df = await get_cached_history(symbol)

        if df is None:
            return await interaction.followup.send(
                "❌ No data"
            )

        ind = calculate_indicators(df)

        embed = discord.Embed(
            title=f"📊 {symbol} Analysis",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name="Trend",
            value=ind["trend"]
        )

        embed.add_field(
            name="RSI",
            value=ind["rsi"]
        )

        embed.add_field(
            name="MACD",
            value=ind["macd"]
        )

        embed.add_field(
            name="Signal",
            value=ind["signal"]
        )

        chart = await generate_chart(symbol, df)

        embed.set_image(
            url=f"attachment://{symbol}.png"
        )

        await interaction.followup.send(
            embed=embed,
            file=chart
        )

    except Exception as e:

        logger.exception(e)

        await interaction.followup.send(
            "❌ Analysis failed"
        )

# =========================================================
# WATCHLIST PROCESSING
# =========================================================

async def process_symbol(channel, stock):

    (
        symbol,
        baseline,
        last_drop_alert,
        last_golden_alert,
        last_death_alert
    ) = stock

    try:

        # -----------------------------------------
        # PRICE CHECK
        # -----------------------------------------

        price = await fetch_current_price(symbol)

        if price is None:
            return

        drop = (
            baseline - price
        ) / baseline

        if (
            drop >= DROP_THRESHOLD
            and should_alert(last_drop_alert)
        ):

            embed = discord.Embed(
                title="🚨 Price Drop Alert",
                color=discord.Color.red()
            )

            embed.add_field(
                name="Symbol",
                value=symbol
            )

            embed.add_field(
                name="Drop",
                value=f"{drop*100:.2f}%"
            )

            embed.add_field(
                name="Current Price",
                value=f"${price:.2f}"
            )

            await channel.send(embed=embed)

            db_execute(
                """
                UPDATE watchlist
                SET last_drop_alert = ?
                WHERE symbol = ?
                """,
                (
                    datetime.utcnow().isoformat(),
                    symbol
                )
            )

        # -----------------------------------------
        # HISTORY
        # -----------------------------------------

        df = await get_cached_history(symbol)

        if df is None or len(df) < 60:
            return

        signal = detect_cross(df)

        ind = calculate_indicators(df)

        # -----------------------------------------
        # FILTERS
        # -----------------------------------------

        if not ind["high_volume"]:
            return

        if ind["atr"] < 2:
            return

        # -----------------------------------------
        # GOLDEN CROSS
        # -----------------------------------------

        if (
            signal == "golden"
            and should_alert(last_golden_alert)
        ):

            embed = discord.Embed(
                title="🚀 GOLDEN CROSS",
                color=discord.Color.green()
            )

            embed.add_field(
                name="Symbol",
                value=symbol
            )

            embed.add_field(
                name="Trend",
                value=ind["trend"]
            )

            embed.add_field(
                name="RSI",
                value=ind["rsi"]
            )

            await channel.send(embed=embed)

            db_execute(
                """
                UPDATE watchlist
                SET last_golden_alert = ?
                WHERE symbol = ?
                """,
                (
                    datetime.utcnow().isoformat(),
                    symbol
                )
            )

        # -----------------------------------------
        # DEATH CROSS
        # -----------------------------------------

        elif (
            signal == "death"
            and should_alert(last_death_alert)
        ):

            embed = discord.Embed(
                title="💀 DEATH CROSS",
                color=discord.Color.dark_red()
            )

            embed.add_field(
                name="Symbol",
                value=symbol
            )

            embed.add_field(
                name="Trend",
                value=ind["trend"]
            )

            embed.add_field(
                name="RSI",
                value=ind["rsi"]
            )

            await channel.send(embed=embed)

            db_execute(
                """
                UPDATE watchlist
                SET last_death_alert = ?
                WHERE symbol = ?
                """,
                (
                    datetime.utcnow().isoformat(),
                    symbol
                )
            )

    except Exception as e:

        logger.exception(
            f"{symbol} processing failed: {e}"
        )

# =========================================================
# BACKGROUND TASK
# =========================================================

@tasks.loop(minutes=15)
async def watchlist_checker():

    try:

        channel = (
            bot.get_channel(CHANNEL_ID)
            or await bot.fetch_channel(CHANNEL_ID)
        )

        stocks = db_execute(
            """
            SELECT
                symbol,
                baseline,
                last_drop_alert,
                last_golden_alert,
                last_death_alert
            FROM watchlist
            """,
            fetch=True
        )

        tasks_list = [
            process_symbol(channel, stock)
            for stock in stocks
        ]

        await asyncio.gather(*tasks_list)

        logger.info(
            f"Processed {len(stocks)} stocks"
        )

    except Exception as e:

        logger.exception(e)

@watchlist_checker.before_loop
async def before_loop():

    await bot.wait_until_ready()

# =========================================================
# RUN
# =========================================================

bot.run(TOKEN)
