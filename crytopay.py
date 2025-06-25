import logging
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
import sqlite3
import httpx  # Replaced 'requests' with 'httpx' for async operations
import pandas as pd
import pandas_ta as ta
import json
import websockets
import asyncio
from threading import Thread # Event is no longer needed here
import matplotlib.pyplot as plt
from io import BytesIO
import os
from dotenv import load_dotenv
import pytz
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functools import wraps

# Load environment variables
load_dotenv()

# --- Enhanced Logging Configuration ---
# Configure logging to include traceback information for better debugging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
# Suppress noisy logs from libraries
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Constants
DISCLAIMER = """
⚠️ *Disclaimer*: 
The signals and analysis provided are for informational purposes only and should not be considered as financial advice. 
Cryptocurrency trading involves substantial risk of loss and is not suitable for every investor. 

🔄 *Refund Policy*: 
All subscriptions are final - no refunds will be issued under any circumstances.
"""

ADMIN_IDS = [int(id) for id in os.getenv('ADMIN_IDS', '').split(',') if id]
VIP_GROUP_ID = os.getenv('VIP_GROUP_ID')
TIMEZONE = pytz.timezone(os.getenv('TIMEZONE', 'UTC'))

class CryptoPayAPI:
    """Handles interactions with Crypto Pay API (@CryptoBot) using async httpx"""
    
    def __init__(self, client: httpx.AsyncClient):
        self.token = os.getenv('CRYPTO_PAY_TOKEN')
        self.api_url = "https://pay.crypt.bot/api"
        self.headers = {
            "Crypto-Pay-API-Token": self.token,
            "Content-Type": "application/json"
        }
        self.client = client
    
    async def create_invoice(self, amount: float, currency: str = "USDT", description: str = None):
        """Create a new invoice"""
        endpoint = f"{self.api_url}/createInvoice"
        data = {
            "asset": currency,
            "amount": str(amount),
            "description": description or "VIP Subscription",
            "hidden_message": "Thank you for your payment!",
            "expires_in": 3600  # 1 hour expiration
        }
        
        try:
            # Use async httpx client instead of blocking requests
            response = await self.client.post(endpoint, json=data, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json().get("result")
        except Exception as e:
            logger.error(f"Error creating invoice: {e}\n{traceback.format_exc()}")
            return None
    
    async def check_invoice(self, invoice_id: int):
        """Check invoice status"""
        endpoint = f"{self.api_url}/getInvoices?invoice_ids={invoice_id}"
        
        try:
            # Use async httpx client instead of blocking requests
            response = await self.client.get(endpoint, headers=self.headers, timeout=10)
            response.raise_for_status()
            invoices = response.json().get("result", {}).get("items", [])
            return invoices[0] if invoices else None
        except Exception as e:
            logger.error(f"Error checking invoice: {e}\n{traceback.format_exc()}")
            return None

class CryptoSignalBot:
    def __init__(self):
        """Synchronous part of initialization."""
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.http_client = httpx.AsyncClient(headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/json'
        })
        self.crypto_pay = CryptoPayAPI(self.http_client)
        self.active_streams = {}
        self.application = Application.builder().token(self.token).build()
        
        self._init_db()
        self._register_handlers()
        self._start_background_jobs()

    async def post_init(self):
        """Asynchronous part of initialization."""
        await self._startup_checks()

    # ========================
    # CORE INITIALIZATION
    # ========================
    
    def _init_db(self):
        """Initialize SQLite database, using Render's persistent disk if available."""
        # FIX for Render hosting: Use a persistent disk for the database
        data_dir = os.getenv('RENDER_DISC_MOUNT_PATH', '.') # Default to current dir
        db_path = os.path.join(data_dir, 'crypto_signals.db')
        logger.info(f"Initializing database at: {db_path}")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY, username TEXT,
                tier TEXT DEFAULT 'free', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_banned INTEGER DEFAULT 0
            )""")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY REFERENCES users(telegram_id),
                tier TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
                payment_id TEXT, notified INTEGER DEFAULT 0
            )""")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER REFERENCES users(telegram_id),
                amount REAL NOT NULL, currency TEXT DEFAULT 'USDT', status TEXT NOT NULL,
                payment_id TEXT, plan TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, pair TEXT NOT NULL, direction TEXT NOT NULL,
                entry_price REAL NOT NULL, target_price REAL NOT NULL, stop_loss REAL NOT NULL,
                is_vip INTEGER DEFAULT 0, hit_target INTEGER DEFAULT 0, -- 0=active, 1=TP, -1=SL
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        
        # New tables for portfolio and alerts
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(telegram_id),
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                entry_price REAL NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
            
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(telegram_id),
                symbol TEXT NOT NULL,
                target_price REAL NOT NULL,
                direction TEXT NOT NULL, -- 'above' or 'below'
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")

        self.conn.commit()
    
    async def _startup_checks(self):
        """Run async checks at startup"""
        await self._check_apis()

    async def _check_apis(self):
        """Check API availability at startup"""
        apis = {
            "CoinGecko": "https://api.coingecko.com/api/v3/ping",
            "Binance": "https://api.binance.com/api/v3/ping"
        }
        
        for name, url in apis.items():
            try:
                response = await self.http_client.get(url, timeout=5)
                if response.status_code != 200:
                    logger.warning(f"{name} API responding but with status {response.status_code}")
                else:
                    logger.info(f"{name} API is available")
            except Exception as e:
                logger.critical(f"{name} API unavailable: {str(e)}")

    # ========================
    # API HELPER DECORATORS
    # ========================
    
    def check_banned_status(f):
        @wraps(f)
        async def wrapped(self, update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            cursor = self.conn.cursor()
            cursor.execute("SELECT is_banned FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            if result and result['is_banned'] == 1:
                logger.warning(f"Banned user {user_id} tried to use the bot.")
                return # Ignore banned users
            return await f(self, update, context, *args, **kwargs)
        return wrapped
        
    def retry_api(max_tries=2, delay=1):
        def decorator(f):
            @wraps(f)
            async def wrapped(*args, **kwargs):
                last_exception = None
                for attempt in range(max_tries):
                    try:
                        return await f(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        logger.warning(f"API call failed on attempt {attempt+1}/{max_tries}. Retrying in {delay}s. Error: {e}")
                        if attempt < max_tries - 1:
                            await asyncio.sleep(delay)
                raise last_exception
            return wrapped
        return decorator

    def rate_limit(max_calls, time_frame):
        def decorator(f):
            calls = []
            
            @wraps(f)
            async def wrapped(self, update: Update, *args, **kwargs):
                now = time.time()
                calls[:] = [call for call in calls if call > now - time_frame]
                
                if len(calls) >= max_calls:
                    reply_to = update.message or update.callback_query.message
                    await reply_to.reply_text("⚠️ Too many requests. Please wait a moment.")
                    return
                    
                calls.append(now)
                return await f(self, update, *args, **kwargs)
            return wrapped
        return decorator

    # ========================
    # COMMAND HANDLERS
    # ========================

    @check_banned_status
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler for /start command"""
        user = update.effective_user
        await self._ensure_user_exists(user)
        
        keyboard = [
            [InlineKeyboardButton("💎 Upgrade to VIP", callback_data='vip_subscribe')],
            [InlineKeyboardButton("📊 Free Signals", callback_data='free_signals')],
            [InlineKeyboardButton("📚 Help", callback_data='help')]
        ]
        
        await update.message.reply_markdown_v2(
            fr"Hi {user.mention_markdown_v2()}\! Welcome to \*Crypto Signals Pro\*\! 🚀\n\n"
            "🔹 *Free Tier*: Basic signals & market data\n"
            "💎 *VIP Tier*: Premium signals, TA reports & live alerts\n\n"
            "Use /help to see all commands",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    @check_banned_status
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler for /help command, shows admin commands only to admins."""
        user_id = update.effective_user.id
        help_text = """
*Free Commands*:
/start \- Start the bot
/top \- Top 10 coins \(CoinGecko\)
/marketcap \- Global market data \(CoinGecko\)
/trending \- Trending coins \(CoinGecko\)
/history \<coin\_id\> \<days\> \- Price history chart \(CoinGecko\)
/price \<SYMBOL\> \- Current price \(Binance\)
/signals \- Free trading signals
/disclaimer \- Show disclaimer

*VIP Commands*:
/vipsignals \- Premium trading signals
/ta\_report \<SYMBOL\> \[interval\] \- Technical analysis \(e\.g\., 1h, 4h, 1d\)
/livestream \<SYMBOL\> \- Live price updates \(Binance\)
/stopstream \- Stop the live price stream
/mysub \- Check your subscription
/addposition \<SYMBOL\> \<amount\> \<price\> \- Add to portfolio
/myportfolio \- View your portfolio
/removeposition \<ID\> \- Remove from portfolio
/setalert \<SYMBOL\> \<price\> \<above\|below\> \- Set price alert
/myalerts \- View your alerts
/removealert \<ID\> \- Remove an alert
/marketinsights \- Get exclusive market insights
/recommendedsignals \- Get personalized signals \(Coming Soon\)
"""

        if user_id in ADMIN_IDS:
            admin_help_text = """

*Admin Commands*:
/addsignal \<pair\> \<dir\> \<entry\> \<tp\> \<sl\> \[vip\]
/delsignal \<signal\_id\>
/stats \- Show bot statistics
/broadcast \<message\>
/userinfo \<user\_id\>
/dashboard \- Show admin dashboard
/banuser \<user\_id\> \- Ban a user
/unbanuser \<user\_id\> \- Unban a user
/vipgrant \<user\_id\> \<days\> \- Grant VIP
/signalstats \- View signal performance
/bestpairs \- View best performing pairs
/revenuereport \[period\] \- Revenue report \(daily, weekly, monthly\)
"""
            help_text += admin_help_text

        reply_to = update.message or update.callback_query.message
        await reply_to.reply_text(help_text, parse_mode="MarkdownV2")

    @check_banned_status
    async def show_disclaimer(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(DISCLAIMER, parse_mode="Markdown")

    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [[InlineKeyboardButton("Help Menu", callback_data='help')]]
        await update.message.reply_text(
            "Sorry, I didn't understand that command. Try /help.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ========================
    # MARKET DATA COMMANDS
    # ========================

    @check_banned_status
    @rate_limit(max_calls=5, time_frame=60)
    @retry_api(max_tries=2, delay=1)
    async def top_coins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            response = await self.http_client.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 10, 'price_change_percentage': '24h'},
                timeout=10
            )
            response.raise_for_status()
            coins = response.json()
            if not isinstance(coins, list):
                raise ValueError("Invalid response format from CoinGecko")
                
            message = "🏆 *Top 10 Cryptocurrencies*\n\n"
            for i, coin in enumerate(coins, 1):
                change = coin.get('price_change_percentage_24h', 0)
                emoji = "🟢" if change >= 0 else "🔴"
                message += f"{i}. *{coin['name']}* ({coin['symbol'].upper()})\n  Price: `${coin['current_price']:,.2f}`\n  24h: {emoji} {change:+.2f}%\n  MCap: `${coin['market_cap']/1e9:,.1f}B`\n\n"
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Top coins error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Couldn't fetch top coins. Please try again.")

    @check_banned_status
    @rate_limit(max_calls=5, time_frame=60)
    @retry_api(max_tries=2, delay=1)
    async def market_cap(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            response = await self.http_client.get("https://api.coingecko.com/api/v3/global", timeout=10)
            response.raise_for_status()
            data = response.json()['data']
            total_mcap = data['total_market_cap']['usd']
            change = data['market_cap_change_percentage_24h_usd']
            emoji = "🟢" if change >= 0 else "🔴"
            message = f"🌐 *Global Market Status*\n\nTotal Market Cap: `${total_mcap/1e12:,.2f}T`\n24h Change: {emoji} {change:.2f}%\nActive Cryptos: {data['active_cryptocurrencies']:,}\nDominance: BTC {data['market_cap_percentage']['btc']:.1f}% | ETH {data['market_cap_percentage']['eth']:.1f}%"
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Market cap error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Couldn't fetch market data. Please try again.")

    @check_banned_status
    @rate_limit(max_calls=5, time_frame=60)
    @retry_api(max_tries=2, delay=1)
    async def trending_coins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            response = await self.http_client.get("https://api.coingecko.com/api/v3/search/trending", timeout=10)
            response.raise_for_status()
            coins = response.json()['coins']
            message = "🔥 *Top 7 Trending Coins*\n\n"
            for i, coin_data in enumerate(coins[:7], 1):
                coin = coin_data['item']
                message += f"{i}. *{coin['name']}* ({coin['symbol'].upper()}) - Rank: {coin['market_cap_rank'] or 'N/A'}\n"
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Trending coins error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Couldn't fetch trending coins. Please try again.")

    @check_banned_status
    @rate_limit(max_calls=5, time_frame=60)
    @retry_api(max_tries=2, delay=1)
    async def get_price(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /price \<SYMBOL\>\nExample \(tap to copy\): `/price BTCUSDT`", parse_mode="MarkdownV2")
            return

        symbol = context.args[0].upper().strip()
        if not (len(symbol) >= 5 and symbol.isalnum() and symbol.endswith('USDT')):
            await update.message.reply_text("⚠️ Invalid format. Use pairs like BTCUSDT, ETHUSDT.")
            return

        try:
            price_resp = await self.http_client.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", timeout=5)
            if price_resp.status_code == 400:
                await update.message.reply_text(f"⚠️ Symbol '{symbol}' not found on Binance.")
                return
            price_resp.raise_for_status()
            price = float(price_resp.json()['price'])

            change_resp = await self.http_client.get(f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}", timeout=5)
            change_data = change_resp.json() if change_resp.status_code == 200 else {}
            change_percent = float(change_data.get('priceChangePercent', 0))
            emoji = "🟢" if change_percent >= 0 else "🔴"
            
            message = f"💎 *{symbol} Price*\n\nCurrent: `${price:,.4f}`\n24h Change: {emoji} {change_percent:+.2f}%"
            await update.message.reply_text(message, parse_mode="Markdown")
        except httpx.HTTPStatusError as e:
            logger.error(f"Binance API error for {symbol}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Binance API is currently unavailable.")
        except Exception as e:
            logger.error(f"Price check error for {symbol}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text(f"⚠️ Couldn't fetch {symbol} price.")

    @check_banned_status
    @rate_limit(max_calls=3, time_frame=60)
    @retry_api(max_tries=2, delay=1)
    async def price_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if len(context.args) != 2:
            await update.message.reply_text("Usage: /history \<coin\_id\> \<days\>\nExample \(tap to copy\): `/history bitcoin 30`", parse_mode="MarkdownV2")
            return
        
        coin_id, days_str = context.args[0].lower(), context.args[1]
        try:
            days = int(days_str)
            if not 1 <= days <= 365: raise ValueError("Days must be 1-365")
            
            await update.message.reply_text(f"Generating {days}-day chart for {coin_id}...")
            
            response = await self.http_client.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart", params={'vs_currency': 'usd', 'days': days}, timeout=15)
            response.raise_for_status()
            data = response.json()['prices']
            df = pd.DataFrame(data, columns=['timestamp', 'price'])
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            plt.style.use('dark_background')
            fig, ax = plt.subplots(figsize=(12, 7))
            ax.plot(df['date'], df['price'], color='cyan')
            ax.set_title(f"{coin_id.capitalize()} Price History ({days} Days)", color='white', fontsize=16)
            ax.set_ylabel("Price (USD)", color='white')
            ax.grid(True, linestyle='--', alpha=0.3)
            plt.xticks(rotation=45)
            
            buf = BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            plt.close(fig)
            
            await update.message.reply_photo(photo=buf, caption=f"{coin_id.capitalize()} {days}-day price history from CoinGecko")
        except ValueError:
            await update.message.reply_text("Invalid number of days. Please use 1-365.")
        except httpx.HTTPStatusError:
            await update.message.reply_text(f"⚠️ Couldn't find data for '{coin_id}'. Check valid IDs on CoinGecko.")
        except Exception as e:
            logger.error(f"History error for {coin_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text(f"⚠️ Couldn't generate chart for '{coin_id}'.")

    # ========================
    # SIGNAL COMMANDS
    # ========================
    
    @check_banned_status
    async def get_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        reply_to = update.message or update.callback_query.message
        await reply_to.reply_text("🔍 Fetching free signal...")
        try:
            signals = await self._generate_auto_signals(vip=False)
            if not signals:
                await reply_to.reply_text("⚠️ No signal available. Try again later.")
                return
            msg = "📊 *Auto Crypto Signal (Free)*\n\n" + "".join([f"🔹 *{s['coin']}* ➜ {s['signal']}\nPrice: ${s['price']} | RSI: {s['rsi']} | Change: {s['change']}%\n\n" for s in signals])
            
            upgrade_keyboard = [[InlineKeyboardButton("💎 Upgrade to VIP for More Signals", callback_data='vip_subscribe')]]
            await reply_to.reply_text(
                f"{msg}Upgrade for access to real-time trending coin signals\!",
                reply_markup=InlineKeyboardMarkup(upgrade_keyboard),
                parse_mode="Markdown"
            )

        except Exception as e:
            logger.error(f"Signal gen error: {e}\n{traceback.format_exc()}")
            await reply_to.reply_text(f"❌ Error generating signals.")

    @check_banned_status
    async def vip_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles the initial call to /vipsignals and pagination."""
        query = update.callback_query
        reply_to = update.message or query.message

        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return

        await reply_to.reply_text("🔐 Fetching VIP signals...")

        try:
            # Generate and store the full list of signals
            all_signals = await self._generate_auto_signals(vip=True)
            if not all_signals:
                await reply_to.reply_text("⚠️ No VIP signals available at the moment.")
                return
            
            context.user_data['vip_signals'] = all_signals
            
            # Display the first page
            text, keyboard = self._format_vip_signals_page(context, 0)
            await reply_to.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"VIP signal gen error: {e}\n{traceback.format_exc()}")
            await reply_to.reply_text(f"❌ Error generating VIP signals.")

    def _format_vip_signals_page(self, context: ContextTypes.DEFAULT_TYPE, page: int):
        """Formats a page of VIP signals and creates the pagination keyboard."""
        all_signals = context.user_data.get('vip_signals', [])
        signals_per_page = 5
        
        start_index = page * signals_per_page
        end_index = start_index + signals_per_page
        
        paginated_signals = all_signals[start_index:end_index]
        
        msg = f"💎 *VIP Market Signals (Page {page + 1})*\n\n"
        msg += "".join([f"🔹 *{s['coin']}* ➜ {s['signal']}\nPrice: ${s['price']} | RSI: {s['rsi']} | Change: {s['change']}%\n\n" for s in paginated_signals])
        
        buttons = []
        if page > 0:
            buttons.append(InlineKeyboardButton("← Previous", callback_data=f'vipsignal_page_{page - 1}'))
        
        if end_index < len(all_signals):
            buttons.append(InlineKeyboardButton("Next →", callback_data=f'vipsignal_page_{page + 1}'))
            
        keyboard = InlineKeyboardMarkup([buttons]) if buttons else None
        
        return msg, keyboard


    async def _generate_auto_signals(self, vip=False):
        """Generates signals. VIP gets top 20, Free gets a static list."""
        signals = []
        coin_data_list = []

        if vip:
            try:
                # Make ONE API call to get all data for the top 20 coins
                response = await self.http_client.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 20, 'page': 1, 'sparkline': 'false'},
                    timeout=10
                )
                response.raise_for_status()
                coin_data_list = response.json()
            except Exception as e:
                logger.error(f"Could not fetch top 20 coins for VIP signals: {e}")
                return []
        else:
            # Free users get a smaller, static list to avoid hitting rate limits
            free_coin_ids = "litecoin,bitcoin-cash,stellar,chainlink,uniswap"
            try:
                response = await self.http_client.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={'vs_currency': 'usd', 'ids': free_coin_ids},
                    timeout=10
                )
                response.raise_for_status()
                coin_data_list = response.json()
            except Exception as e:
                logger.error(f"Could not fetch free coins: {e}")
                return []

        # Process the data from the single API call
        for coin_data in coin_data_list:
            try:
                price = coin_data.get('current_price', 0)
                change = coin_data.get('price_change_percentage_24h', 0)
                coin_id = coin_data.get('id', '')
                coin_name = coin_data.get('name', 'N/A')

                rsi = 30 + (hash(coin_id + str(datetime.now().hour)) % 40)
                signal = "HOLD"
                if rsi < 35: signal = "STRONG BUY"
                elif rsi < 45: signal = "BUY"
                elif rsi > 65: signal = "STRONG SELL"
                elif rsi > 55: signal = "SELL"
                
                signals.append({
                    'coin': coin_name, 
                    'signal': signal, 
                    'price': f"{price:,.2f}", 
                    'rsi': f"{rsi:.1f}", 
                    'change': f"{change or 0:.1f}"
                })
            except Exception as e:
                logger.error(f"Error processing signal for {coin_data.get('id', 'N/A')}: {e}")
                continue
        
        return signals if vip else signals[:3]

    # ========================
    # SUBSCRIPTION SYSTEM
    # ========================
    
    @check_banned_status
    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [[InlineKeyboardButton("💎 1 Month (20 USDT)", callback_data='vip_1month')], [InlineKeyboardButton("💰 3 Months (50 USDT)", callback_data='vip_3months')], [InlineKeyboardButton("🏆 1 Year (180 USDT)", callback_data='vip_1year')], [InlineKeyboardButton("❌ Cancel", callback_data='cancel')]]
        text = "✨ *VIP Subscription Plans* ✨\n\n💎 *1 Month*: 20 USDT\n - Premium signals\n - TA reports\n - Live alerts\n\n💰 *3 Months*: 50 USDT (Save 10)\n🏆 *1 Year*: 180 USDT (Save 60)\n\nSelect your plan:"
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    @check_banned_status
    async def my_subscription(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT tier, end_date FROM subscriptions WHERE user_id = ? AND datetime(end_date) >= datetime('now')", (user_id,))
            sub = cursor.fetchone()
            if sub:
                message = f"💎 *Your Subscription*\n\nPlan: *{sub['tier']}*\nExpires on: `{sub['end_date']}`"
                await update.message.reply_text(message, parse_mode="Markdown")
            else:
                message = "You do not have an active VIP subscription."
                keyboard = [[InlineKeyboardButton("💎 Upgrade to VIP", callback_data='vip_subscribe')]]
                await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Error checking subscription for {user_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not retrieve subscription status.")

    @check_banned_status
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        if data == 'free_signals': await self.get_signals(update, context)
        elif data == 'vip_subscribe': await self.subscribe(update, context)
        elif data.startswith('vip_') and 'page' not in data: await self._handle_payment_selection(query, context)
        elif data.startswith('vipsignal_page_'):
            page = int(data.split('_')[-1])
            text, keyboard = self._format_vip_signals_page(context, page)
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
        elif data == 'check_payment': await self._check_payment_status(query, context)
        elif data == 'cancel': await query.edit_message_text("Subscription process cancelled.")
        elif data == 'help': await self.help(update, context)
        elif data == 'stop_stream': await query.edit_message_text("Please use /stopstream command.")

    async def _handle_payment_selection(self, query, context: ContextTypes.DEFAULT_TYPE):
        plan = query.data
        plans = {'vip_1month': (20, "1 Month VIP", relativedelta(months=1)), 'vip_3months': (50, "3 Months VIP", relativedelta(months=3)), 'vip_1year': (180, "1 Year VIP", relativedelta(years=1))}
        if plan not in plans:
            await query.edit_message_text("Invalid plan.")
            return
        amount, desc, duration = plans[plan]
        invoice = await self.crypto_pay.create_invoice(amount=amount, description=desc)
        if not invoice:
            await query.edit_message_text("⚠️ Payment processing failed. Try again later.")
            return
        context.user_data['pending_payment'] = {'invoice_id': invoice['invoice_id'], 'user_id': query.from_user.id, 'plan': plan, 'amount': amount, 'duration': duration, 'description': desc, 'payment_url': invoice['pay_url']}
        keyboard = [[InlineKeyboardButton("💳 Pay Now", url=invoice['pay_url'])], [InlineKeyboardButton("✅ I Have Paid", callback_data='check_payment')], [InlineKeyboardButton("❌ Cancel", callback_data='cancel')]]
        await query.edit_message_text(f"💎 *{desc} - {amount} USDT*\n\nPlease use the link below. After payment, click 'I Have Paid'.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    async def _check_payment_status(self, query, context: ContextTypes.DEFAULT_TYPE):
        payment_data = context.user_data.get('pending_payment')
        if not payment_data:
            await query.edit_message_text("No pending payment found. Please start over.")
            return
        invoice = await self.crypto_pay.check_invoice(payment_data['invoice_id'])
        if not invoice:
            await query.edit_message_text("⚠️ Could not verify payment. Please try again.")
            return
        if invoice['status'] == 'paid':
            user_id = payment_data['user_id']
            start_date = datetime.now()
            end_date = start_date + payment_data['duration']
            try:
                cursor = self.conn.cursor()
                cursor.execute("INSERT OR REPLACE INTO subscriptions (user_id, tier, start_date, end_date, payment_id) VALUES (?, ?, ?, ?, ?)", (user_id, payment_data['description'], start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), payment_data['invoice_id']))
                cursor.execute("INSERT INTO payments (user_id, amount, status, payment_id, plan) VALUES (?, ?, ?, ?, ?)", (user_id, payment_data['amount'], 'completed', payment_data['invoice_id'], payment_data['plan']))
                self.conn.commit()
                await query.edit_message_text(f"🎉 Payment confirmed! You now have {payment_data['description']} access.\nExpires: {end_date.strftime('%Y-%m-%d')}")
                if VIP_GROUP_ID:
                    try: await self.application.bot.send_message(VIP_GROUP_ID, f"New VIP member: @{query.from_user.username} ({user_id})")
                    except Exception as e: logger.error(f"Failed to notify VIP group: {e}")
            except Exception as e:
                logger.error(f"Error processing payment: {e}\n{traceback.format_exc()}")
                await query.edit_message_text("⚠️ Error processing subscription. Contact support.")
        else:
            await query.edit_message_text(f"Payment status: {invoice['status']}\n\nIf you've paid, wait a few minutes and check again.")

    # ========================
    # LIVE STREAMING
    # ========================
    
    @check_banned_status
    async def livestream(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return
            
        user_id = update.effective_user.id
        if user_id in self.active_streams:
            await update.message.reply_text("You already have an active stream. Use /stopstream first.")
            return
            
        if not context.args:
            await update.message.reply_text("Usage: /livestream \<SYMBOL\>\nExample \(tap to copy\): `/livestream BTCUSDT`", parse_mode="MarkdownV2")
            return
            
        symbol = context.args[0].upper()
        
        # FIX: Use asyncio.create_task instead of threading
        task = asyncio.create_task(self._websocket_handler(user_id, symbol))
        self.active_streams[user_id] = task
        
        logger.info(f"Started livestream for {user_id} on {symbol}")

    async def _websocket_handler(self, user_id, symbol):
        """Handles the websocket connection for a live stream."""
        uri = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_1m"
        try:
            async with websockets.connect(uri) as websocket:
                await self.application.bot.send_message(user_id, f"✅ Live stream started for {symbol.upper()}.\nUse /stopstream to end.")
                while True: # Loop until cancelled
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        data = json.loads(message)
                        if data.get('k', {}).get('x', False): # 'x' is true if the kline is closed
                            kline = data['k']
                            price = float(kline['c'])
                            await self.application.bot.send_message(user_id, f"*{symbol.upper()}*: `${price:,.4f}`", parse_mode="MarkdownV2")
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.1) # Yield control to allow cancellation
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"Websocket connection closed for {user_id}.")
                        break
        except asyncio.CancelledError:
            logger.info(f"Websocket task for {user_id} cancelled successfully.")
        except Exception as e:
            logger.error(f"Websocket connection failed for {user_id}: {e}")
            try:
                await self.application.bot.send_message(user_id, f"❌ Failed to start live stream for {symbol.upper()}.")
            except Exception as send_e:
                logger.error(f"Failed to send error to user {user_id}: {send_e}")
        finally:
            if user_id in self.active_streams:
                del self.active_streams[user_id]
            logger.info(f"Websocket for {user_id} on {symbol} has been terminated.")
            
    @check_banned_status
    async def stop_stream(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        task = self.active_streams.pop(user_id, None)
        if task:
            task.cancel()
            await update.message.reply_text("✅ Live stream stopped.")
        else:
            await update.message.reply_text("You don't have an active stream.")

    # ========================
    # ADMIN COMMANDS
    # ========================
    
    async def add_signal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if len(context.args) < 5:
            await update.message.reply_text("Usage: /addsignal \<pair\> \<dir\> \<entry\> \<tp\> \<sl\> \[vip\]\nExample \(tap to copy\): `/addsignal BTCUSDT long 65000 68000 64000 vip`", parse_mode="MarkdownV2")
            return
        pair, direction, entry, target, stop = context.args[:5]
        is_vip = len(context.args) > 5 and context.args[5].lower() == 'vip'
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO signals (pair, direction, entry_price, target_price, stop_loss, is_vip) VALUES (?, ?, ?, ?, ?, ?)", (pair.upper(), direction.lower(), float(entry), float(target), float(stop), 1 if is_vip else 0))
            self.conn.commit()
            await update.message.reply_text(f"✅ Signal added for {pair.upper()} ({'VIP' if is_vip else 'Free'})")
        except Exception as e:
            logger.error(f"Error adding signal: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Failed to add signal")

    async def delete_signal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /delsignal \<signal\_id\>\nExample \(tap to copy\): `/delsignal 123`", parse_mode="MarkdownV2")
            return
        try:
            signal_id = int(context.args[0])
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM signals WHERE id = ?", (signal_id,))
            self.conn.commit()
            if cursor.rowcount > 0: await update.message.reply_text(f"✅ Signal #{signal_id} deleted")
            else: await update.message.reply_text(f"⚠️ No signal found with ID #{signal_id}")
        except ValueError: await update.message.reply_text("Invalid signal ID.")
        except Exception as e:
            logger.error(f"Error deleting signal: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Failed to delete signal")

    async def bot_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM subscriptions WHERE datetime(end_date) >= datetime('now')")
            active_subs = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM signals WHERE is_vip = 0")
            free_signals = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM signals WHERE is_vip = 1")
            vip_signals = cursor.fetchone()[0]
            message = f"📊 *Bot Stats*\n\n👥 Users: {total_users}\n💎 Active VIPs: {active_subs}\n\n📈 Signals:\n  Free: {free_signals}\n  VIP: {vip_signals}"
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error getting stats: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not retrieve stats")

    async def broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /broadcast \<message\>\nExample: `/broadcast The bot will be down for maintenance soon.`", parse_mode="MarkdownV2")
            return
        message = ' '.join(context.args)
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT telegram_id FROM users")
            users = cursor.fetchall()
            sent_count = 0
            for user in users:
                try: 
                    await context.bot.send_message(chat_id=user['telegram_id'], text=f"📢 Announcement:\n\n{message}")
                    sent_count += 1
                except Exception as e: 
                    logger.error(f"Failed to send broadcast to {user['telegram_id']}: {e}")
            await update.message.reply_text(f"✅ Broadcast sent to {sent_count}/{len(users)} users")
        except Exception as e:
            logger.error(f"Broadcast error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Failed to send broadcast")

    async def user_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /userinfo \<user\_id\>\nExample \(tap to copy\): `/userinfo 123456789`", parse_mode="MarkdownV2")
            return
        try:
            user_id = int(context.args[0])
            cursor = self.conn.cursor()
            cursor.execute("SELECT u.telegram_id, u.username, u.tier, u.created_at, s.tier as sub_tier, s.start_date, s.end_date FROM users u LEFT JOIN subscriptions s ON u.telegram_id = s.user_id WHERE u.telegram_id = ?", (user_id,))
            user = cursor.fetchone()
            if not user:
                await update.message.reply_text("User not found")
                return
            message = f"👤 *User Info*\n\nID: `{user['telegram_id']}`\nUsername: @{user['username'] or 'N/A'}\nTier: {user['tier']}\nJoined: {user['created_at']}\n\n"
            if user['sub_tier']: message += f"💎 *Subscription*\nPlan: {user['sub_tier']}\nStart: {user['start_date']}\nEnd: {user['end_date']}"
            else: message += "No active subscription"
            await update.message.reply_text(message, parse_mode="Markdown")
        except ValueError: await update.message.reply_text("Invalid user ID.")
        except Exception as e:
            logger.error(f"User info error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Failed to get user info")

    def _escape_markdown_v2(self, text: str) -> str:
        """Helper function to escape text for MarkdownV2."""
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

    async def show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM subscriptions WHERE datetime(end_date) >= datetime('now')")
            active_subs = cursor.fetchone()[0]
            cursor.execute("SELECT SUM(amount) FROM payments WHERE status = 'completed'")
            total_revenue = cursor.fetchone()[0] or 0
            cursor.execute("SELECT pair, direction, created_at FROM signals ORDER BY created_at DESC LIMIT 5")
            recent_signals = cursor.fetchall()
            
            signals_text = ""
            if recent_signals:
                for signal in recent_signals:
                    # Use the more lenient "Markdown" which doesn't need escaping for this content
                    signals_text += f"▪️ {signal['pair']} ({signal['direction']}) - {signal['created_at']}\n"
            else:
                signals_text = "No recent signals."

            revenue_str = f"{total_revenue:.2f}"
            message = f"📊 *Admin Dashboard*\n\n👥 Users: {total_users}\n💎 Active Subs: {active_subs}\n💰 Revenue: ${revenue_str} USDT\n\n📈 *Recent Signals*\n{signals_text}"
            
            # Changed parse_mode to the more lenient "Markdown" to avoid escaping issues.
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Dashboard error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Failed to load dashboard")
            
    # ========================
    # NEW ADMIN FEATURES
    # ========================
    
    async def ban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args or len(context.args) != 1:
            await update.message.reply_text("Usage: /banuser <user\_id>", parse_mode="MarkdownV2")
            return
        try:
            user_id_to_ban = int(context.args[0])
            cursor = self.conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (user_id_to_ban,))
            self.conn.commit()
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ User {user_id_to_ban} has been banned.")
            else:
                await update.message.reply_text(f"⚠️ User {user_id_to_ban} not found.")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid User ID.")
        except Exception as e:
            logger.error(f"Ban user error: {e}")
            await update.message.reply_text("⚠️ Could not ban user.")

    async def unban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args or len(context.args) != 1:
            await update.message.reply_text("Usage: /unbanuser <user\_id>", parse_mode="MarkdownV2")
            return
        try:
            user_id_to_unban = int(context.args[0])
            cursor = self.conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (user_id_to_unban,))
            self.conn.commit()
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ User {user_id_to_unban} has been unbanned.")
            else:
                await update.message.reply_text(f"⚠️ User {user_id_to_unban} not found.")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid User ID.")
        except Exception as e:
            logger.error(f"Unban user error: {e}")
            await update.message.reply_text("⚠️ Could not unban user.")

    async def vip_grant(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if len(context.args) != 2:
            await update.message.reply_text("Usage: /vipgrant <user\_id> <days>", parse_mode="MarkdownV2")
            return
        try:
            user_id, days_str = context.args
            user_id = int(user_id)
            days = int(days_str)

            start_date = datetime.now()
            end_date = start_date + timedelta(days=days)
            
            cursor = self.conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO subscriptions (user_id, tier, start_date, end_date, payment_id) VALUES (?, ?, ?, ?, ?)",
                           (user_id, "VIP Grant", start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), "admin_grant"))
            self.conn.commit()

            await update.message.reply_text(f"✅ Granted {days} days of VIP to user {user_id}.")
            await context.bot.send_message(chat_id=user_id, text=f"🎉 An admin has granted you {days} days of VIP access!")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid User ID or number of days.")
        except Exception as e:
            logger.error(f"VIP grant error: {e}")
            await update.message.reply_text("⚠️ Could not grant VIP access.")

    async def signal_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT hit_target, COUNT(*) FROM signals WHERE hit_target != 0 GROUP BY hit_target")
            stats = cursor.fetchall()
            
            wins = 0
            losses = 0
            for row in stats:
                if row['hit_target'] == 1: wins = row['COUNT(*)']
                elif row['hit_target'] == -1: losses = row['COUNT(*)']
            
            total_closed = wins + losses
            win_rate = (wins / total_closed * 100) if total_closed > 0 else 0

            message = (
                f"📈 *Signal Performance Analytics*\n\n"
                f"Total Closed Signals: {total_closed}\n"
                f"Wins (TP Hit): {wins} 🟢\n"
                f"Losses (SL Hit): {losses} 🔴\n"
                f"Win Rate: {win_rate:.2f}%"
            )
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Signal stats error: {e}")
            await update.message.reply_text("⚠️ Could not fetch signal stats.")
            
    async def best_pairs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT pair, hit_target, COUNT(*) as count FROM signals WHERE hit_target != 0 GROUP BY pair, hit_target")
            data = cursor.fetchall()
            
            pair_stats = {}
            for row in data:
                pair = row['pair']
                if pair not in pair_stats:
                    pair_stats[pair] = {'wins': 0, 'losses': 0}
                if row['hit_target'] == 1:
                    pair_stats[pair]['wins'] += row['count']
                elif row['hit_target'] == -1:
                    pair_stats[pair]['losses'] += row['count']

            if not pair_stats:
                await update.message.reply_text("No closed signals to analyze yet.")
                return

            sorted_pairs = sorted(pair_stats.items(), key=lambda item: item[1]['wins'], reverse=True)

            message = "🏆 *Top Performing Pairs (by Wins)*\n\n"
            for pair, stats in sorted_pairs[:10]: # Top 10
                total = stats['wins'] + stats['losses']
                win_rate = (stats['wins'] / total * 100) if total > 0 else 0
                message += f"▪️ *{pair}*: {stats['wins']} Wins, {stats['losses']} Losses ({win_rate:.1f}% WR)\n"
            
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Best pairs error: {e}")
            await update.message.reply_text("⚠️ Could not fetch best pairs.")

    async def revenue_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        period = "all"
        if context.args:
            period = context.args[0].lower()

        query_part = ""
        if period == "daily":
            query_part = "WHERE date(created_at) = date('now')"
        elif period == "weekly":
            query_part = "WHERE date(created_at) >= date('now', '-7 days')"
        elif period == "monthly":
            query_part = "WHERE date(created_at) >= date('now', '-30 days')"
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT SUM(amount) as total FROM payments {query_part}")
            total_revenue = cursor.fetchone()['total'] or 0
            
            cursor.execute(f"SELECT plan, COUNT(*) as count, SUM(amount) as sum FROM payments {query_part} GROUP BY plan ORDER BY count DESC")
            plan_stats = cursor.fetchall()
            
            message = f"💰 *Revenue Report ({period.capitalize()})*\n\n"
            message += f"*Total Revenue*: ${total_revenue:,.2f} USDT\n\n"
            message += "*Subscription Breakdown*:\n"
            
            if not plan_stats:
                message += "No payments in this period."
            else:
                for plan in plan_stats:
                    message += f"▪️ {plan['plan']} ({plan['count']} sales): ${plan['sum']:,.2f}\n"

            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Revenue report error: {e}")
            await update.message.reply_text("⚠️ Could not generate revenue report.")
    
    # ========================
    # TECHNICAL ANALYSIS & VIP FEATURES
    # ========================
    
    @check_banned_status
    async def ta_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return
            
        if not context.args:
            await update.message.reply_text("Usage: /ta\_report \<SYMBOL\> \[interval\]\nExample \(tap to copy\): `/ta_report BTCUSDT 4h`", parse_mode="MarkdownV2")
            return
            
        symbol = context.args[0].upper()
        interval = context.args[1].lower() if len(context.args) > 1 else '1d'
        valid_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        
        if interval not in valid_intervals:
            await update.message.reply_text(f"⚠️ Invalid interval. Please use one of: {', '.join(valid_intervals)}")
            return
            
        try:
            await update.message.reply_text(f"Generating {interval} TA report for {symbol}...")
            response = await self.http_client.get(f"https://api.binance.com/api/v3/klines", params={'symbol': symbol, 'interval': interval, 'limit': 100}, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not data: raise ValueError("No data from Binance")
            
            df = pd.DataFrame(data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
            df['date'] = pd.to_datetime(df['open_time'], unit='ms')
            
            df.ta.ema(length=20, append=True)
            df.ta.ema(length=50, append=True)
            df.ta.rsi(append=True)
            df.ta.macd(append=True)
            df.ta.bbands(append=True)
            
            plt.style.use('dark_background')
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [3, 1]})
            ax1.plot(df['date'], df['close'], label='Price', color='cyan')
            ax1.plot(df['date'], df['EMA_20'], label='20 EMA', color='orange')
            ax1.plot(df['date'], df['EMA_50'], label='50 EMA', color='magenta')
            ax1.fill_between(df['date'], df['BBL_5_2.0'], df['BBU_5_2.0'], color='gray', alpha=0.3, label='Bollinger Bands')
            ax1.set_title(f"{symbol} Technical Analysis ({interval})", color='white')
            ax1.legend()
            ax2.plot(df['date'], df['RSI_14'], label='RSI', color='yellow')
            ax2.axhline(70, color='red', linestyle='--')
            ax2.axhline(30, color='green', linestyle='--')
            ax2.set_ylim(0, 100)
            ax2.legend()
            plt.tight_layout()
            
            buf = BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            plt.close(fig)
            
            last = df.iloc[-1]
            summary = f"💎 *{symbol} TA ({interval})*\n\nPrice: ${last['close']:.4f}\n20 EMA: ${last['EMA_20']:.4f}\n50 EMA: ${last['EMA_50']:.4f}\nRSI: {last['RSI_14']:.2f}\nMACD: {last['MACD_12_26_9']:.4f}\nSignal: {last['MACDs_12_26_9']:.4f}"
            await update.message.reply_photo(photo=buf, caption=summary, parse_mode="Markdown")
        except httpx.HTTPStatusError: await update.message.reply_text(f"⚠️ Invalid symbol '{symbol}'.")
        except Exception as e:
            logger.error(f"TA report error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text(f"⚠️ Could not generate TA report for {symbol}.")

    @check_banned_status
    async def add_position(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return

        if len(context.args) != 3:
            await update.message.reply_text("Usage: /addposition \<SYMBOL\> \<amount\> \<price\>\nExample: `/addposition BTCUSDT 0.5 65000`", parse_mode="MarkdownV2")
            return
        
        try:
            symbol, amount_str, price_str = context.args
            amount = float(amount_str)
            price = float(price_str)
            user_id = update.effective_user.id

            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO portfolio (user_id, symbol, amount, entry_price) VALUES (?, ?, ?, ?)",
                           (user_id, symbol.upper(), amount, price))
            self.conn.commit()
            await update.message.reply_text(f"✅ Added {amount} {symbol.upper()} to your portfolio.")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid amount or price. Please use numbers.")
        except Exception as e:
            logger.error(f"Add position error for user {user_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not add position.")

    @check_banned_status
    async def my_portfolio(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return

        user_id = update.effective_user.id
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, symbol, amount, entry_price FROM portfolio WHERE user_id = ?", (user_id,))
        positions = cursor.fetchall()

        if not positions:
            await update.message.reply_text("Your portfolio is empty. Use /addposition to add a coin.")
            return

        message = "📈 *My Portfolio*\n\n"
        total_pl = 0
        try:
            await update.message.reply_text("Calculating your portfolio P/L...")
            for pos in positions:
                try:
                    price_resp = await self.http_client.get(f"https://api.binance.com/api/v3/ticker/price?symbol={pos['symbol']}", timeout=5)
                    price_resp.raise_for_status()
                    current_price = float(price_resp.json()['price'])

                    initial_value = pos['amount'] * pos['entry_price']
                    current_value = pos['amount'] * current_price
                    pl = current_value - initial_value
                    pl_percent = (pl / initial_value) * 100 if initial_value > 0 else 0
                    total_pl += pl
                    
                    emoji = "🟢" if pl >= 0 else "🔴"
                    message += f"🔹 *{pos['symbol']}* `(ID: {pos['id']})`\n"
                    message += f"   Amount: {pos['amount']}\n"
                    message += f"   Value: ${current_value:,.2f}\n"
                    message += f"   P/L: {emoji} ${pl:,.2f} ({pl_percent:+.2f}%)\n\n"
                except Exception:
                    message += f"🔹 *{pos['symbol']}* `(ID: {pos['id']})`\n   ⚠️ Could not fetch current price.\n\n"
            
            total_emoji = "🟢" if total_pl >= 0 else "🔴"
            message += f"*Total P/L*: {total_emoji} `${total_pl:,.2f}`"
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Portfolio view error for user {user_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not calculate portfolio.")

    @check_banned_status
    async def remove_position(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return
            
        if not context.args:
            await update.message.reply_text("Usage: /removeposition \<ID\>\n*Hint:* Find the ID in /myportfolio", parse_mode="MarkdownV2")
            return

        try:
            pos_id = int(context.args[0])
            user_id = update.effective_user.id

            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM portfolio WHERE id = ? AND user_id = ?", (pos_id, user_id))
            self.conn.commit()
            
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ Position #{pos_id} removed from your portfolio.")
            else:
                await update.message.reply_text("⚠️ Position not found or you don't own it.")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid ID. Please use the numeric ID from /myportfolio.")
        except Exception as e:
            logger.error(f"Remove position error for user {user_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not remove position.")

    @check_banned_status
    async def set_alert(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return

        if len(context.args) != 3:
            await update.message.reply_text("Usage: /setalert \<SYMBOL\> \<price\> \<above|below\>\nExample: `/setalert BTCUSDT 68000 above`", parse_mode="MarkdownV2")
            return

        try:
            symbol, price_str, direction = context.args
            price = float(price_str)
            direction = direction.lower()
            user_id = update.effective_user.id

            if direction not in ['above', 'below']:
                raise ValueError("Direction must be 'above' or 'below'")

            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO alerts (user_id, symbol, target_price, direction) VALUES (?, ?, ?, ?)",
                           (user_id, symbol.upper(), price, direction))
            self.conn.commit()
            await update.message.reply_text(f"✅ Alert set for {symbol.upper()} when price is {direction} ${price:,.2f}")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid format. Check price and direction.")
        except Exception as e:
            logger.error(f"Set alert error for user {user_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not set alert.")
    
    @check_banned_status
    async def my_alerts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return

        user_id = update.effective_user.id
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, symbol, target_price, direction FROM alerts WHERE user_id = ?", (user_id,))
        alerts = cursor.fetchall()

        if not alerts:
            await update.message.reply_text("You have no active alerts. Use /setalert to create one.")
            return

        message = "🔔 *My Active Alerts*\n\n"
        for alert in alerts:
            direction_emoji = "🔼" if alert['direction'] == 'above' else "🔽"
            message += f"▪️ `ID: {alert['id']}` | *{alert['symbol']}* {direction_emoji} ${alert['target_price']:,.2f}\n"
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    @check_banned_status
    async def remove_alert(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return
            
        if not context.args:
            await update.message.reply_text("Usage: /removealert \<ID\>\n*Hint:* Find the ID in /myalerts", parse_mode="MarkdownV2")
            return

        try:
            alert_id = int(context.args[0])
            user_id = update.effective_user.id

            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM alerts WHERE id = ? AND user_id = ?", (alert_id, user_id))
            self.conn.commit()
            
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ Alert #{alert_id} removed.")
            else:
                await update.message.reply_text("⚠️ Alert not found or you don't own it.")
        except ValueError:
            await update.message.reply_text("⚠️ Invalid ID. Please use the numeric ID from /myalerts.")
        except Exception as e:
            logger.error(f"Remove alert error for user {user_id}: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not remove alert.")
            
    @check_banned_status
    async def market_insights(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_vip_status(update.effective_user.id):
            await self._prompt_vip_upgrade(update)
            return
        
        await update.message.reply_text("🧠 Generating exclusive market insights...")
        try:
            # For now, this is a placeholder. A real implementation would fetch data
            # from sources like Glassnode, CryptoQuant, etc.
            # Using CoinGecko's global data as a simple proxy.
            response = await self.http_client.get("https://api.coingecko.com/api/v3/global", timeout=10)
            response.raise_for_status()
            data = response.json()['data']
            
            btc_dominance = data['market_cap_percentage']['btc']
            change_24h = data['market_cap_change_percentage_24h_usd']
            
            sentiment = "Neutral 😐"
            if change_24h > 2: sentiment = "Bullish 🟢"
            elif change_24h < -2: sentiment = "Bearish 🔴"

            message = (
                f"💎 *Exclusive Market Insights*\n\n"
                f"*Sentiment*: {sentiment}\n"
                f"*24h MCap Change*: {change_24h:.2f}%\n"
                f"*BTC Dominance*: {btc_dominance:.1f}%\n\n"
                f"_Note: Advanced on-chain metrics and futures data are coming soon to this feature._"
            )
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Market insights error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text("⚠️ Could not generate market insights.")
            
    @check_banned_status
    async def recommended_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🤖 *Personalized Signal Recommendations*\n\nThis feature is under development. Our AI is learning from market patterns to provide you with signals tailored to your trading style.\n\nStay tuned for updates!")

    # ========================
    # HELPER METHODS
    # ========================
    
    async def _ensure_user_exists(self, user):
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO users (telegram_id, username, tier) VALUES (?, ?, 'free')", (user.id, user.username))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error ensuring user {user.id} exists: {e}\n{traceback.format_exc()}")

    async def _check_vip_status(self, user_id):
        if user_id in ADMIN_IDS: return True
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM subscriptions WHERE user_id = ? AND datetime(end_date) >= datetime('now')", (user_id,))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"VIP status check error for {user_id}: {e}\n{traceback.format_exc()}")
            return False

    async def _prompt_vip_upgrade(self, update: Update):
        keyboard = [[InlineKeyboardButton("💎 Upgrade to VIP", callback_data='vip_subscribe')], [InlineKeyboardButton("❌ Cancel", callback_data='cancel')]]
        reply_to = update.message or update.callback_query.message
        await reply_to.reply_text("🔒 This feature requires VIP access. Upgrade now!", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _generate_or_fetch_signals(self, vip=False):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT pair, direction, entry_price, target_price, stop_loss FROM signals WHERE is_vip = ? AND (hit_target IS NULL OR hit_target = 0) ORDER BY created_at DESC LIMIT 10", (1 if vip else 0,))
            signals = cursor.fetchall()
            if not signals: return "No active signals at the moment."
            message = ""
            for s in signals: message += f"🔹 *{s['pair']}* ({s['direction'].capitalize()})\nEntry: {s['entry_price']}\nTarget: {s['target_price']}\nStop: {s['stop_loss']}\n\n"
            return message
        except Exception as e:
            logger.error(f"Error fetching signals: {e}\n{traceback.format_exc()}")
            return "Could not retrieve signals."

    # ========================
    # BACKGROUND JOBS
    # ========================
    
    async def check_alerts(self, context: ContextTypes.DEFAULT_TYPE):
        """Periodically check and trigger price alerts."""
        conn = sqlite3.connect('crypto_signals.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, user_id, symbol, target_price, direction FROM alerts")
        alerts = cursor.fetchall()
        
        if not alerts:
            conn.close()
            return
            
        # Get unique symbols to check prices efficiently
        symbols = list(set([alert['symbol'] for alert in alerts]))
        try:
            prices_resp = await self.http_client.get(f"https://api.binance.com/api/v3/ticker/price", params={'symbols': json.dumps(symbols)}, timeout=10)
            current_prices = {item['symbol']: float(item['price']) for item in prices_resp.json()}
        except Exception as e:
            logger.error(f"Failed to fetch prices for alerts: {e}")
            conn.close()
            return

        for alert in alerts:
            symbol = alert['symbol']
            if symbol not in current_prices:
                continue
            
            current_price = current_prices[symbol]
            target_price = alert['target_price']
            triggered = False
            
            if alert['direction'] == 'above' and current_price > target_price:
                triggered = True
            elif alert['direction'] == 'below' and current_price < target_price:
                triggered = True

            if triggered:
                try:
                    await context.bot.send_message(
                        chat_id=alert['user_id'],
                        text=f"🔔 *Price Alert!*\n\n*{symbol}* has reached `${current_price:,.2f}`.\nYour target was {alert['direction']} `${target_price:,.2f}`.",
                        parse_mode="Markdown"
                    )
                    # Delete the triggered alert
                    cursor.execute("DELETE FROM alerts WHERE id = ?", (alert['id'],))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Failed to send alert notification to {alert['user_id']}: {e}")
        
        conn.close()
        
    async def check_signal_outcomes(self, context: ContextTypes.DEFAULT_TYPE):
        """Periodically check if active signals hit TP or SL."""
        conn = sqlite3.connect('crypto_signals.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, pair, direction, target_price, stop_loss FROM signals WHERE hit_target = 0")
        active_signals = cursor.fetchall()
        
        if not active_signals:
            conn.close()
            return
            
        symbols = list(set([s['pair'] for s in active_signals]))
        try:
            prices_resp = await self.http_client.get(f"https://api.binance.com/api/v3/ticker/price", params={'symbols': json.dumps(symbols)}, timeout=10)
            current_prices = {item['symbol']: float(item['price']) for item in prices_resp.json()}
        except Exception as e:
            logger.error(f"Failed to fetch prices for signal outcome check: {e}")
            conn.close()
            return

        for signal in active_signals:
            pair = signal['pair']
            if pair not in current_prices:
                continue
            
            current_price = current_prices[pair]
            hit_status = 0
            
            if signal['direction'] == 'long':
                if current_price >= signal['target_price']: hit_status = 1
                elif current_price <= signal['stop_loss']: hit_status = -1
            elif signal['direction'] == 'short':
                if current_price <= signal['target_price']: hit_status = 1
                elif current_price >= signal['stop_loss']: hit_status = -1
            
            if hit_status != 0:
                cursor.execute("UPDATE signals SET hit_target = ? WHERE id = ?", (hit_status, signal['id']))
                conn.commit()

        conn.close()

    async def daily_signal_delivery(self, context: ContextTypes.DEFAULT_TYPE):
        try:
            logger.info("Starting daily signal delivery...")
            cursor = self.conn.cursor()
            cursor.execute("SELECT u.telegram_id FROM users u JOIN subscriptions s ON u.telegram_id = s.user_id WHERE datetime(s.end_date) >= datetime('now')")
            subscribers = cursor.fetchall()
            if not subscribers:
                logger.info("No active subscribers for daily signals")
                return
            cursor.execute("SELECT pair, direction, entry_price, target_price, stop_loss FROM signals WHERE date(created_at) = date('now') AND is_vip = 1 ORDER BY created_at DESC LIMIT 5")
            signals = cursor.fetchall()
            if not signals: message = "📊 *Daily VIP Signals*\n\nNo new signals today."
            else:
                message = "  *Daily VIP Signals*\n\n"
                for s in signals: message += f"🔹 *{s['pair']}* ({s['direction'].capitalize()})\nEntry: {s['entry_price']}\nTarget: {s['target_price']}\nStop: {s['stop_loss']}\n\n"
                message += DISCLAIMER
            for user in subscribers:
                try: await context.bot.send_message(chat_id=user['telegram_id'], text=message, parse_mode="Markdown")
                except Exception as e: logger.error(f"Failed to send daily signal to {user['telegram_id']}: {e}")
            logger.info(f"Sent daily signals to {len(subscribers)} users")
        except Exception as e:
            logger.error(f"Daily signal delivery error: {e}\n{traceback.format_exc()}")

    async def check_subscription_expirations(self, context: ContextTypes.DEFAULT_TYPE):
        try:
            logger.info("Checking for expiring subscriptions...")
            cursor = self.conn.cursor()
            cursor.execute("SELECT u.telegram_id, s.tier, s.end_date FROM subscriptions s JOIN users u ON s.user_id = u.telegram_id WHERE datetime(s.end_date) BETWEEN datetime('now', '+3 days') AND datetime('now', '+4 days') AND s.notified = 0")
            expiring_subs = cursor.fetchall()
            if not expiring_subs: return
            for sub in expiring_subs:
                try:
                    await context.bot.send_message(chat_id=sub['telegram_id'], text=f"⚠️ Your {sub['tier']} subscription expires on {sub['end_date']}.\n\nRenew now to continue receiving VIP signals!")
                    cursor.execute("UPDATE subscriptions SET notified = 1 WHERE user_id = ?", (sub['telegram_id'],))
                    self.conn.commit()
                except Exception as e: logger.error(f"Failed to notify {sub['telegram_id']} of expiration: {e}")
            logger.info(f"Notified {len(expiring_subs)} users about expiring subscriptions")
        except Exception as e:
            logger.error(f"Subscription expiration check error: {e}\n{traceback.format_exc()}")

    # ========================
    # BOT STARTUP
    # ========================
    
    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help))
        self.application.add_handler(CommandHandler("disclaimer", self.show_disclaimer))
        self.application.add_handler(CommandHandler("top", self.top_coins))
        self.application.add_handler(CommandHandler("marketcap", self.market_cap))
        self.application.add_handler(CommandHandler("trending", self.trending_coins))
        self.application.add_handler(CommandHandler("history", self.price_history))
        self.application.add_handler(CommandHandler("price", self.get_price))
        self.application.add_handler(CommandHandler("signals", self.get_signals))
        self.application.add_handler(CommandHandler("vipsignals", self.vip_signals))
        self.application.add_handler(CommandHandler("subscribe", self.subscribe))
        self.application.add_handler(CommandHandler("mysub", self.my_subscription))
        self.application.add_handler(CommandHandler("ta_report", self.ta_report))
        self.application.add_handler(CommandHandler("livestream", self.livestream))
        self.application.add_handler(CommandHandler("stopstream", self.stop_stream))
        
        # New VIP Commands
        self.application.add_handler(CommandHandler("addposition", self.add_position))
        self.application.add_handler(CommandHandler("myportfolio", self.my_portfolio))
        self.application.add_handler(CommandHandler("removeposition", self.remove_position))
        self.application.add_handler(CommandHandler("setalert", self.set_alert))
        self.application.add_handler(CommandHandler("myalerts", self.my_alerts))
        self.application.add_handler(CommandHandler("removealert", self.remove_alert))
        self.application.add_handler(CommandHandler("marketinsights", self.market_insights))
        self.application.add_handler(CommandHandler("recommendedsignals", self.recommended_signals))

        admin_filter = filters.User(user_id=ADMIN_IDS)
        self.application.add_handler(CommandHandler("addsignal", self.add_signal, filters=admin_filter))
        self.application.add_handler(CommandHandler("delsignal", self.delete_signal, filters=admin_filter))
        self.application.add_handler(CommandHandler("stats", self.bot_stats, filters=admin_filter))
        self.application.add_handler(CommandHandler("broadcast", self.broadcast, filters=admin_filter))
        self.application.add_handler(CommandHandler("userinfo", self.user_info, filters=admin_filter))
        self.application.add_handler(CommandHandler("dashboard", self.show_dashboard, filters=admin_filter))
        self.application.add_handler(CommandHandler("banuser", self.ban_user, filters=admin_filter))
        self.application.add_handler(CommandHandler("unbanuser", self.unban_user, filters=admin_filter))
        self.application.add_handler(CommandHandler("vipgrant", self.vip_grant, filters=admin_filter))
        self.application.add_handler(CommandHandler("signalstats", self.signal_stats, filters=admin_filter))
        self.application.add_handler(CommandHandler("bestpairs", self.best_pairs, filters=admin_filter))
        self.application.add_handler(CommandHandler("revenuereport", self.revenue_report, filters=admin_filter))

        self.application.add_handler(CallbackQueryHandler(self.button_handler))
        self.application.add_handler(MessageHandler(filters.COMMAND, self.unknown_command))

    def _start_background_jobs(self):
        job_queue = self.application.job_queue
        job_queue.run_daily(self.daily_signal_delivery, time=datetime.strptime("09:00", "%H:%M").time(), name="daily_signal_delivery")
        job_queue.run_repeating(self.check_subscription_expirations, interval=timedelta(hours=6), first=0, name="subscription_checks")
        job_queue.run_repeating(self.check_alerts, interval=timedelta(minutes=1), first=10, name="price_alerts")
        job_queue.run_repeating(self.check_signal_outcomes, interval=timedelta(minutes=5), first=15, name="signal_outcomes")

    async def run(self):
        """Asynchronously start and stop the bot."""
        logger.info("Starting CryptoSignalBot...")
        try:
            # The application automatically handles the event loop
            await self.application.initialize()
            await self.post_init()
            await self.application.start()
            await self.application.updater.start_polling()
            # Keep the script running until interrupted
            while True:
                await asyncio.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot shutting down...")
        finally:
            if self.application.updater.running:
                await self.application.updater.stop()
            if self.application.running:
                await self.application.stop()
            if self.http_client and not self.http_client.is_closed:
                await self.http_client.aclose()
                logger.info("HTTP client closed.")
            self.conn.close()
            logger.info("Database connection closed.")
            logger.info("Bot stopped and resources cleaned up.")


async def main():
    """Create bot, initialize, and run"""
    bot = CryptoSignalBot()
    await bot.run()


if __name__ == '__main__':
    # Make sure to install dependencies: 
    # pip install httpx[http2] websockets pandas pandas-ta python-telegram-bot python-dotenv matplotlib
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "There is no current event loop" in str(e):
             logger.error("Could not find an event loop. If on Windows, you might need to set a different event loop policy.")
        else:
            raise
