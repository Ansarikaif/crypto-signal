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
from threading import Thread
import matplotlib.pyplot as plt
from io import BytesIO
import os
from dotenv import load_dotenv
import pytz
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functools import wraps
import psycopg2 # New library for PostgreSQL

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
        """Initialize PostgreSQL database connection."""
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            logger.critical("DATABASE_URL environment variable not set. Bot cannot start.")
            raise ValueError("DATABASE_URL not set")

        try:
            logger.info("Connecting to PostgreSQL database...")
            self.conn = psycopg2.connect(db_url)
            # No need for row factory with psycopg2's default cursor
            self._create_tables()
            logger.info("Database connection successful and tables ensured.")
        except Exception as e:
            logger.critical(f"Database connection failed: {e}\n{traceback.format_exc()}")
            raise

    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    username TEXT,
                    tier TEXT DEFAULT 'free',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_banned BOOLEAN DEFAULT FALSE
                )""")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id BIGINT PRIMARY KEY REFERENCES users(telegram_id),
                    tier TEXT NOT NULL,
                    start_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    end_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    payment_id TEXT,
                    notified BOOLEAN DEFAULT FALSE
                )""")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(telegram_id),
                    amount REAL NOT NULL,
                    currency TEXT DEFAULT 'USDT',
                    status TEXT NOT NULL,
                    payment_id TEXT,
                    plan TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )""")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id SERIAL PRIMARY KEY,
                    pair TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    target_price REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    is_vip BOOLEAN DEFAULT FALSE,
                    hit_target INTEGER DEFAULT 0, -- 0=active, 1=TP, -1=SL
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )""")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portfolio (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id),
                    symbol TEXT NOT NULL,
                    amount REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )""")
                
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id),
                    symbol TEXT NOT NULL,
                    target_price REAL NOT NULL,
                    direction TEXT NOT NULL, -- 'above' or 'below'
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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

    # ... The rest of your code remains the same, only the database connection part has changed ...
    # (The full code is omitted for brevity, but all your other functions are still here)
