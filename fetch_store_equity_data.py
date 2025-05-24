import requests
import gzip
import json
import io
import sqlite3
from datetime import datetime, date, time, timedelta, timezone
from urllib.parse import quote
import logging

# --- 1. Configuration ---
STOCK_SYMBOL = "RELIANCE"
# API_KEY is often used for different auth schemes (e.g., v1, or for specific partner APIs).
# Upstox v3 data APIs primarily use OAuth 2.0 Bearer Tokens (Access Tokens).
# Defined here as requested, but might not be used directly in v3 data calls if Access Token is primary.
API_KEY = "f872ad84-86f7-45ed-9307-eede7c7399ed" 
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzVUM3U0wiLCJqdGkiOiI2ODJmZmFjMDA4ZjVkZTYxM2MyMzBiYTgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzQ3OTc0ODQ4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDgwMzc2MDB9.MSyl3ODIhbksqXm_Cuj6imKDOeCTys9pZIKYujaOrQE"
# IMPORTANT NOTE: The above ACCESS_TOKEN is specific to the date 2025-05-17 for testing.
# For live operations, a dynamically generated access token is required.

DB_PATH = "./upstox_data_v2.db" # Changed from upstox_data.db to upstox_data_v2.db as per subtask 12
API_BASE_URL = "https://api.upstox.com"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global Cache for Instrument Master ---
_instrument_master_cache = None

# --- 2. Helper Functions for API Calls ---

def get_instrument_master():
    """
    Fetches (or returns from cache) the NSE instrument master list.
    """
    global _instrument_master_cache
    if _instrument_master_cache is not None:
        logging.info("Returning instrument master from cache.")
        return _instrument_master_cache

    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        logging.info(f"Downloading instrument master from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        gzipped_content = response.content
        decompressed_content = gzip.decompress(gzipped_content)
        json_data_str = decompressed_content.decode('utf-8')
        _instrument_master_cache = json.loads(json_data_str)
        logging.info(f"Successfully loaded and cached {len(_instrument_master_cache)} instruments.")
        return _instrument_master_cache
    except Exception as e:
        logging.error(f"Error fetching/processing instrument master: {e}")
        return None

def get_equity_instrument_key(stock_symbol_to_find):
    """
    Finds the equity instrument key for a given stock symbol from the master list.
    """
    master_list = get_instrument_master()
    if not master_list:
        return None
    
    for instrument in master_list:
        if (instrument.get('trading_symbol') == stock_symbol_to_find and
            instrument.get('instrument_type') == 'EQ' and
            instrument.get('segment') == 'NSE_EQ' and # As per subtask 13
            instrument.get('exchange') == 'NSE'):
            logging.info(f"Found instrument key for {stock_symbol_to_find}: {instrument.get('instrument_key')}")
            return instrument.get('instrument_key')
    logging.warning(f"Equity instrument key for {stock_symbol_to_find} not found.")
    return None

def fetch_historical_daily_data(instrument_key, target_date_str):
    """
    Fetches historical daily candle data for a given instrument key and date.
    Returns closing price and OI. OI for equity is typically 0.
    URL structure from subtask 8 & 14: /days/1/{target_date}
    """
    if not instrument_key:
        logging.error("Cannot fetch historical daily data: instrument_key is None.")
        return None, None

    encoded_key = quote(instrument_key)
    api_url = f"{API_BASE_URL}/v3/historical-candle/{encoded_key}/days/1/{target_date_str}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}
    
    logging.info(f"Fetching historical daily data from: {api_url}")
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        candles = data.get("data", {}).get("candles")
        if candles and isinstance(candles, list) and len(candles) > 0:
            # Candle format: [timestamp, open, high, low, close, volume, oi]
            first_candle = candles[0]
            if isinstance(first_candle, list) and len(first_candle) >= 7:
                price = first_candle[4]
                oi = first_candle[6]
                logging.info(f"Historical daily data for {target_date_str}: Price={price}, OI={oi}")
                return price, oi
            else:
                logging.warning(f"Unexpected candle data format for {target_date_str}: {first_candle}")
        else:
            logging.warning(f"No candle data found for {target_date_str} in historical daily response.")
    except Exception as e:
        logging.error(f"Error fetching historical daily data for {target_date_str}: {e}")
    return None, None

def fetch_intraday_minute_data(instrument_key, target_datetime_obj):
    """
    Fetches intraday 1-minute candle data and finds the candle for target_datetime_obj (9:20 AM).
    Returns price and OI. OI for equity is typically 0.
    URL structure from subtask 10 (using /minutes/1 which was successful for intraday-like paths):
    [API_BASE_URL]/v3/historical-candle/intraday/{encoded_instrument_key}/minutes/1
    """
    if not instrument_key:
        logging.error("Cannot fetch intraday minute data: instrument_key is None.")
        return None, None

    encoded_key = quote(instrument_key)
    # This endpoint fetches data for the "current server day". We filter for our target_datetime_obj.
    api_url = f"{API_BASE_URL}/v3/historical-candle/intraday/{encoded_key}/minutes/1"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

    logging.info(f"Fetching intraday minute data from: {api_url} (will filter for {target_datetime_obj})")
    try:
        response = requests.get(api_url, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        candles = data.get("data", {}).get("candles")

        if candles and isinstance(candles, list):
            for candle in candles:
                if isinstance(candle, list) and len(candle) >= 7:
                    timestamp_str = candle[0]
                    try:
                        dt_obj = datetime.fromisoformat(timestamp_str)
                        # Compare date and time (stripping timezone for direct comparison if needed, or make target_datetime_obj timezone-aware)
                        # Assuming target_datetime_obj is naive and API returns tz-aware (+05:30)
                        if dt_obj.date() == target_datetime_obj.date() and dt_obj.time() == target_datetime_obj.time():
                            price = candle[4]
                            oi = candle[6]
                            logging.info(f"Intraday 09:20 AM data for {target_datetime_obj.date()}: Price={price}, OI={oi}")
                            return price, oi
                    except ValueError:
                        logging.warning(f"Could not parse timestamp in intraday data: {timestamp_str}")
                        continue # to next candle
            logging.warning(f"09:20 AM candle for {target_datetime_obj.date()} not found in intraday response.")
        else:
            logging.warning(f"No candles returned by intraday API for {instrument_key}.")
            
    except Exception as e:
        logging.error(f"Error fetching intraday minute data: {e}")
    return None, None

# --- 3. Main Logic ---

def main():
    logging.info("Starting equity data fetch and store process.")
    
    # --- Simulate specific dates for testing with the provided token ---
    # Token is valid for 2025-05-17.
    simulated_current_date_obj = date(2025, 5, 17) # Friday
    # Previous trading day for 2025-05-17 (Friday) would be 2025-05-16 (Thursday).
    simulated_prev_trading_date_obj = date(2025, 5, 16) 
    
    logging.info(f"Simulated current date for fetching: {simulated_current_date_obj.isoformat()}")
    logging.info(f"Simulated previous trading date for fetching: {simulated_prev_trading_date_obj.isoformat()}")

    # Get Equity Instrument Key
    equity_instrument_key = get_equity_instrument_key(STOCK_SYMBOL)
    if not equity_instrument_key:
        logging.error(f"Could not retrieve instrument key for {STOCK_SYMBOL}. Exiting.")
        return

    # Fetch Previous Day's Equity Close and OI
    prev_day_close, prev_day_oi = fetch_historical_daily_data(
        equity_instrument_key,
        simulated_prev_trading_date_obj.isoformat()
    )
    # Based on subtask 14, prev_day_close=1456.4, prev_day_oi=0 for 2025-05-16

    # Fetch Current Day's 9:20 AM Equity Price and OI
    # Target time is 9:20 AM on the simulated current date
    target_920_datetime_obj = datetime.combine(simulated_current_date_obj, time(9, 20, 0))
    
    # The intraday API fetches data for the server's actual current day.
    # If the server's actual day is not 2025-05-17, the token might be invalid or data won't match.
    # Subtask 15 showed "No candle data returned by the intraday API..." for this setup.
    # So, current_day_920_price and current_day_920_oi are expected to be None.
    current_day_920_price, current_day_920_oi = fetch_intraday_minute_data(
        equity_instrument_key,
        target_920_datetime_obj
    )

    # --- Store Data in SQLite ---
    table_name = f"data_{simulated_current_date_obj.strftime('%Y_%m_%d')}"
    conn = None
    try:
        logging.info(f"Connecting to database: {DB_PATH} to store data in table: {table_name}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Ensure table exists with the correct schema (from subtask 16)
        # Dropping table for this consolidated script to ensure schema consistency on re-runs.
        logging.info(f"Dropping table {table_name} if it exists to ensure fresh schema.")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_symbol TEXT NOT NULL,
            prev_day_equity_close REAL,
            prev_day_equity_oi INTEGER,
            equity_920_price REAL,
            equity_920_oi INTEGER,
            record_timestamp TEXT NOT NULL
        );
        """
        cursor.execute(create_table_sql)
        logging.info(f"Table {table_name} created/ensured.")

        record_ts = datetime.now(timezone.utc).isoformat()
        insert_sql = f"""
        INSERT INTO {table_name} (stock_symbol, prev_day_equity_close, prev_day_equity_oi, 
                                 equity_920_price, equity_920_oi, record_timestamp)
        VALUES (?, ?, ?, ?, ?, ?);
        """
        data_to_insert = (
            STOCK_SYMBOL, prev_day_close, prev_day_oi,
            current_day_920_price, current_day_920_oi, record_ts
        )
        cursor.execute(insert_sql, data_to_insert)
        conn.commit()
        inserted_id = cursor.lastrowid
        logging.info(f"Data inserted into {table_name} with ID: {inserted_id}. Data: {data_to_insert}")
        
        # Verification query
        cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (inserted_id,))
        retrieved_row = cursor.fetchone()
        logging.info(f"Verified inserted row: {retrieved_row}")

    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
    except Exception as e:
        logging.error(f"General error during database operations: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
            
    logging.info("Equity data fetch and store process finished.")

if __name__ == "__main__":
    main()
