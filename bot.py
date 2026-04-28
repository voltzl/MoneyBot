import discord
from discord.ext import commands, tasks
import os
import sqlite3
from dotenv import load_dotenv
import yfinance as yf

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
DB_FILE = "watchlist.db"
DROP_THRESHOLD = 0.15  # 15%

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Database Setup ----------

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            symbol TEXT PRIMARY KEY,
            baseline REAL NOT NULL,
            alerted INTEGER DEFAULT 0
        )
    """)

    conn.commit()
    conn.close()

def db_execute(query, params=(), fetch=False):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(query, params)

    result = cursor.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return result

# ---------- Events ----------

@bot.event
async def on_ready():
    init_db()
    watchlist_checker.start()
    print(f"✅ Logged in as {bot.user}")

# ---------- Commands ----------

@bot.command()
async def add(ctx, symbol: str):
    symbol = symbol.upper()

    existing = db_execute(
        "SELECT symbol FROM watchlist WHERE symbol = ?",
        (symbol,),
        fetch=True
    )

    if existing:
        await ctx.send(f"⚠️ {symbol} is already in the watchlist.")
        return

    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1d")

    if data.empty:
        await ctx.send("❌ Invalid stock symbol.")
        return

    price = float(data["Close"].iloc[-1])

    db_execute(
        "INSERT INTO watchlist (symbol, baseline, alerted) VALUES (?, ?, 0)",
        (symbol, price)
    )

    await ctx.send(f"✅ Added **{symbol}** to watchlist at ${price:.2f}")

@bot.command()
async def remove(ctx, symbol: str):
    symbol = symbol.upper()

    db_execute(
        "DELETE FROM watchlist WHERE symbol = ?",
        (symbol,)
    )

    await ctx.send(f"🗑️ Removed **{symbol}** from watchlist.")

@bot.command()
async def watchlist(ctx):
    rows = db_execute(
        "SELECT symbol, baseline FROM watchlist",
        fetch=True
    )

    if not rows:
        await ctx.send("📭 Watchlist is empty.")
        return

    message = "**📈 Watchlist:**\n"
    for symbol, baseline in rows:
        message += f"- {symbol} (baseline ${baseline:.2f})\n"

    await ctx.send(message)

# ---------- Background Alert Task ----------

@tasks.loop(minutes=15)
async def watchlist_checker():
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return

    rows = db_execute(
        "SELECT symbol, baseline FROM watchlist WHERE alerted = 0",
        fetch=True
    )

    for symbol, baseline in rows:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d")

        if data.empty:
            continue

        current_price = float(data["Close"].iloc[-1])
        drop = (baseline - current_price) / baseline

        if drop >= DROP_THRESHOLD:
            await channel.send(
                f"🚨 **ALERT: {symbol}**\n"
                f"📉 Drop: **{drop*100:.1f}%**\n"
                f"💲 Baseline: ${baseline:.2f}\n"
                f"💰 Current: ${current_price:.2f}"
            )

            db_execute(
                "UPDATE watchlist SET alerted = 1 WHERE symbol = ?",
                (symbol,)
            )

bot.run(TOKEN)
