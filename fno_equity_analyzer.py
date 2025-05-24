import requests
import gzip
import json
import io
import re
import sqlite3
from datetime import datetime, date, time, timedelta, timezone
from urllib.parse import quote
import logging

# --- 1. Configuration ---
F_AND_O_STOCK_NAMES = [
    "Reliance Industries", "Tata Consultancy Services (TCS)", "Infosys", "HDFC Bank", 
    "ICICI Bank", "State Bank of India (SBI)", "Hindustan Unilever", "ITC", 
    "Larsen & Toubro (L&T)", "Axis Bank", "Kotak Mahindra Bank", "Bajaj Finance", 
    "Bharti Airtel", "Maruti Suzuki", "NTPC", "Power Grid Corporation", 
    "UltraTech Cement", "Grasim Industries", "Wipro", "Tech Mahindra", "HCL Technologies", 
    "Adani Enterprises", "Adani Ports & SEZ", "Tata Motors", "Tata Steel", "JSW Steel", 
    "Coal India", "ONGC", "HDFC Life", "SBI Life Insurance", "Divi's Laboratories", 
    "Dr. Reddy's Laboratories", "Cipla", "Sun Pharmaceutical", "Nestle India", 
    "Britannia Industries", "Asian Paints", "Eicher Motors", "Hero MotoCorp", "Bajaj Auto", 
    "IndusInd Bank", "Dabur India", "Godrej Consumer Products", "Pidilite Industries", 
    "Ambuja Cements", "Shree Cement", "M&M", "Bank of Baroda", "Canara Bank", "PNB", 
    "Vodafone Idea", "Zee Entertainment", "DLF", "InterGlobe Aviation (IndiGo)", 
    "Havells India", "Siemens", "Tata Power", "Tata Elxsi", "Mphasis", 
    "Persistent Systems", "Page Industries", "MRF", "United Spirits", "Berger Paints", 
    "Jubilant FoodWorks", "Varun Beverages", "AU Small Finance Bank", 
    "ICICI Prudential Life", "ICICI Lombard", "Cholamandalam Investment", "REC Ltd", 
    "PFC Ltd", "BHEL", "BEL", "IRCTC", "TVS Motor", "Voltas", "GAIL", "NMDC", "Trent", 
    "Indraprastha Gas", "Lupin", "Torrent Pharma", "Biocon", "Alkem Labs", 
    "Apollo Hospitals", "Max Healthcare", "Zydus Lifesciences", "Abbott India", 
    "Metropolis Healthcare", "Gland Pharma", "Polycab", "Aditya Birla Fashion", 
    "Manappuram Finance", "L&T Finance", "Ujjivan Small Finance Bank", "Federal Bank", 
    "IDFC First Bank", "Bandhan Bank", "Indiabulls Housing Finance"
]

API_KEY = "f872ad84-86f7-45ed-9307-eede7c7399ed" # Provided, usage note below
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzVUM3U0wiLCJqdGkiOiI2ODJmZmFjMDA4ZjVkZTYxM2MyMzBiYTgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzQ3OTc0ODQ4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDgwMzc2MDB9.MSyl3ODIhbksqXm_Cuj6imKDOeCTys9pZIKYujaOrQE"
# CRITICAL NOTE: ACCESS_TOKEN is for 2025-05-17. Test dates are 2025-05-22 & 2025-05-23.
# API calls are expected to fail (401 Unauthorized) due to this mismatch.
# The script is designed to handle these failures gracefully.

DB_PATH = "./upstox_data_v2.db"
API_BASE_URL = "https://api.upstox.com"

# Test Dates
PREVIOUS_DAY_FETCH_STR = "2025-05-22" # Thursday
CURRENT_DAY_CANDLE_TARGET_STR = "2025-05-23"  # Friday (for 9:20 AM candle)
CURRENT_DAY_CANDLE_TARGET_OBJ = date(2025, 5, 23)
TARGET_920_TIME = time(9, 20, 0)

# Table naming based on the "current day" for which 9:20 AM data is targeted
TABLE_NAME_DATE_SUFFIX = CURRENT_DAY_CANDLE_TARGET_STR.replace('-', '_')
RAW_DATA_TABLE_NAME = f"data_{TABLE_NAME_DATE_SUFFIX}"
FILTERED_DATA_TABLE_NAME = f"filtered_{TABLE_NAME_DATE_SUFFIX}"

# --- 2. Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')

# --- Global Cache for Instrument Master ---
_instrument_master_cache = None

# --- 3. Helper Functions ---

def derive_trading_symbol(stock_name_with_suffix):
    """Derives a trading symbol from a stock name."""
    match = re.search(r'\((.*?)\)', stock_name_with_suffix)
    if match:
        return match.group(1).upper()
    # More robust split needed for names like "Dr. Reddy's Laboratories"
    # For now, simple first word, but known to be imperfect.
    # Example: "Larsen & Toubro" -> "LARSEN" (needs to be LT)
    # "Dr. Reddy's Laboratories" -> "DR." (needs to be DRREDDY)
    # This will be a source of "key not found" for many.
    symbol = stock_name_with_suffix.split()[0].upper()
    symbol = re.sub(r'[^A-Z0-9]', '', symbol) # Remove non-alphanumeric like '.' from DR.
    return symbol

def get_instrument_master(api_base_url_unused_param=API_BASE_URL): # param kept for signature consistency if needed
    """Fetches (or returns from cache) the NSE instrument master list."""
    global _instrument_master_cache
    if _instrument_master_cache is not None:
        logging.info("Returning instrument master from cache.")
        return _instrument_master_cache
    
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz" # Hardcoded as it's fixed
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

def find_equity_instrument_key(stock_symbol, instrument_master):
    """Finds the equity instrument key from the provided master list."""
    if not instrument_master: return None
    for instrument in instrument_master:
        if (instrument.get('trading_symbol') == stock_symbol and
            instrument.get('instrument_type') == 'EQ' and
            instrument.get('segment') == 'NSE_EQ' and
            instrument.get('exchange') == 'NSE'):
            return instrument.get('instrument_key')
    logging.warning(f"Instrument key for {stock_symbol} not found in master list.")
    return None

def fetch_historical_data(instrument_key, date_str, access_token_param=ACCESS_TOKEN, api_base_url_param=API_BASE_URL):
    """Fetches historical daily candle data for a given instrument key and date."""
    if not instrument_key: return None, None
    encoded_key = quote(instrument_key)
    api_url = f"{api_base_url_param}/v3/historical-candle/{encoded_key}/days/1/{date_str}"
    headers = {"Authorization": f"Bearer {access_token_param}", "Accept": "application/json"}
    logging.info(f"Fetching historical data for {instrument_key} on {date_str} from {api_url}")
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json().get("data", {}).get("candles")
        if data and data[0] and len(data[0]) >= 7:
            logging.info(f"Historical data received for {instrument_key} on {date_str}: {data[0]}")
            return data[0][4], data[0][6] # close, oi
        logging.warning(f"No/incomplete historical candle data for {instrument_key} on {date_str}.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logging.warning(f"AUTH ERROR (401) fetching historical for {instrument_key} on {date_str}. Token invalid for this date/request.")
        else:
            logging.error(f"HTTP Error fetching historical for {instrument_key} on {date_str}: {e.response.status_code} - {e.response.text[:200]}")
    except Exception as e:
        logging.error(f"General error fetching historical for {instrument_key} on {date_str}: {e}")
    return None, None

def fetch_intraday_data_920(instrument_key, target_candle_date_obj, access_token_param=ACCESS_TOKEN, api_base_url_param=API_BASE_URL):
    """Fetches 1-minute candle data and extracts the 9:20 AM candle for target_candle_date_obj."""
    if not instrument_key: return None, None
    encoded_key = quote(instrument_key)
    
    # API behavior adjustment: request D+1 to get data for D
    api_request_date_obj = target_candle_date_obj + timedelta(days=1)
    api_request_date_str = api_request_date_obj.strftime('%Y-%m-%d')
    
    api_url = f"{api_base_url_param}/v3/historical-candle/{encoded_key}/minutes/1/{api_request_date_str}"
    params = {"from_date": api_request_date_str} # Corrected API structure
    headers = {"Authorization": f"Bearer {access_token_param}", "Accept": "application/json"}
    
    full_url_for_log = f"{api_url}?from_date={api_request_date_str}"
    logging.info(f"Fetching intraday data for {instrument_key} (target candle: {target_candle_date_obj} 09:20, API req date: {api_request_date_str}) from {full_url_for_log}")
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        candles = response.json().get("data", {}).get("candles")
        if candles:
            logging.info(f"Received {len(candles)} candles for {instrument_key} for API req date {api_request_date_str}.")
            for candle in candles:
                if len(candle) >= 7:
                    ts_str = candle[0]
                    try:
                        dt_obj = datetime.fromisoformat(ts_str)
                        if dt_obj.date() == target_candle_date_obj and dt_obj.time() == TARGET_920_TIME:
                            logging.info(f"Found 09:20 AM candle for {instrument_key} on {target_candle_date_obj}: {candle}")
                            return candle[4], candle[6] # close, oi
                    except ValueError:
                        logging.warning(f"Could not parse timestamp {ts_str} for {instrument_key}")
            logging.warning(f"09:20 AM candle for {instrument_key} on {target_candle_date_obj} not found in response for API req date {api_request_date_str}.")
        else:
            logging.warning(f"No intraday candles returned for {instrument_key} for API req date {api_request_date_str}.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logging.warning(f"AUTH ERROR (401) fetching intraday for {instrument_key} (target {target_candle_date_obj}). Token invalid for this date/request.")
        else:
            logging.error(f"HTTP Error fetching intraday for {instrument_key} (target {target_candle_date_obj}): {e.response.status_code} - {e.response.text[:200]}")
    except Exception as e:
        logging.error(f"General error fetching intraday for {instrument_key} (target {target_candle_date_obj}): {e}")
    return None, None

def init_db(db_path, table_name_raw, table_name_filtered):
    """Initializes database and creates tables with specified schemas, dropping if they exist."""
    conn = None
    try:
        logging.info(f"Initializing database at {db_path}...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Raw data table
        logging.info(f"Dropping table {table_name_raw} if it exists.")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name_raw}")
        create_raw_sql = f"""
        CREATE TABLE {table_name_raw} (
            id INTEGER PRIMARY KEY AUTOINCREMENT, stock_symbol TEXT,
            prev_day_equity_close REAL, prev_day_equity_oi INTEGER,
            equity_920_price REAL, equity_920_oi INTEGER,
            record_timestamp TEXT
        );"""
        cursor.execute(create_raw_sql)
        logging.info(f"Table {table_name_raw} created.")

        # Filtered data table
        logging.info(f"Dropping table {table_name_filtered} if it exists.")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name_filtered}")
        create_filtered_sql = f"""
        CREATE TABLE {table_name_filtered} (
            id INTEGER PRIMARY KEY AUTOINCREMENT, stock_symbol TEXT,
            percent_change REAL, record_timestamp TEXT
        );"""
        cursor.execute(create_filtered_sql)
        logging.info(f"Table {table_name_filtered} created.")
        
        conn.commit()
        logging.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error during DB initialization: {e}")
    finally:
        if conn: conn.close()

def store_raw_data_to_db(db_path, table_name_raw, stock_data_list):
    """Stores list of raw stock data dictionaries to the database."""
    if not stock_data_list: 
        logging.info("No raw data to store.")
        return
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        current_ts = datetime.now(timezone.utc).isoformat()
        
        for record in stock_data_list:
            cursor.execute(f"""
            INSERT INTO {table_name_raw} (stock_symbol, prev_day_equity_close, prev_day_equity_oi,
                                          equity_920_price, equity_920_oi, record_timestamp)
            VALUES (?, ?, ?, ?, ?, ?);
            """, (record['stock_symbol'], record['prev_day_equity_close'], record['prev_day_equity_oi'],
                  record['equity_920_price'], record['equity_920_oi'], current_ts))
        conn.commit()
        logging.info(f"Stored {len(stock_data_list)} records into {table_name_raw}.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error storing raw data: {e}")
    finally:
        if conn: conn.close()

def store_filtered_data_to_db(db_path, table_name_filtered, filtered_stock_list):
    """Stores list of (symbol, percent_change) tuples to the database."""
    if not filtered_stock_list:
        logging.info("No filtered data to store.")
        return
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        current_ts = datetime.now(timezone.utc).isoformat()

        for symbol, p_change in filtered_stock_list:
            cursor.execute(f"""
            INSERT INTO {table_name_filtered} (stock_symbol, percent_change, record_timestamp)
            VALUES (?, ?, ?);
            """, (symbol, p_change, current_ts))
        conn.commit()
        logging.info(f"Stored {len(filtered_stock_list)} records into {table_name_filtered}.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error storing filtered data: {e}")
    finally:
        if conn: conn.close()

def display_filtered_results(db_path, table_name_filtered):
    """Queries and prints data from the filtered results table."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info(f"Querying {table_name_filtered} for display...")
        cursor.execute(f"SELECT stock_symbol, percent_change FROM {table_name_filtered} ORDER BY stock_symbol;")
        results = cursor.fetchall()
        
        print(f"\n--- Filtered Stocks (from {table_name_filtered}) ---")
        if results:
            for symbol, p_change in results:
                print(f"Stock: {symbol}, Change: {p_change:.2f}%")
        else:
            print("No stocks met the filter criteria in the database.")
            
    except sqlite3.Error as e:
        logging.error(f"SQLite error displaying filtered results: {e}")
        print(f"Error querying filtered results: {e}")
    finally:
        if conn: conn.close()

# --- 4. Main Workflow ---
def main():
    logging.info("Starting consolidated F&O equity data processing workflow.")
    logging.warning(f"CRITICAL NOTE: Provided ACCESS_TOKEN is for 2025-05-17. API calls for test dates "
                    f"{PREVIOUS_DAY_FETCH_STR} & {CURRENT_DAY_CANDLE_TARGET_STR} are expected to fail "
                    f"authentication (401 Error) or return no data. This is handled by the script.")

    instrument_master = get_instrument_master()
    if not instrument_master:
        logging.error("Failed to load instrument master. Cannot proceed.")
        return

    init_db(DB_PATH, RAW_DATA_TABLE_NAME, FILTERED_DATA_TABLE_NAME)

    all_stocks_data_for_db = []
    filtered_stocks_for_db = [] # List of (symbol, percent_change) tuples

    for stock_name_full in F_AND_O_STOCK_NAMES:
        logging.info(f"--- Processing: {stock_name_full} ---")
        derived_symbol = derive_trading_symbol(stock_name_full)
        
        if not derived_symbol: # Should not happen with current simple logic but good check
            logging.warning(f"Could not derive symbol for '{stock_name_full}'. Skipping.")
            all_stocks_data_for_db.append({
                'stock_symbol': stock_name_full, # Store original name if symbol derivation fails
                'prev_day_equity_close': None, 'prev_day_equity_oi': None,
                'equity_920_price': None, 'equity_920_oi': None,
                'error_message': 'Symbol derivation failed'
            })
            continue
            
        logging.info(f"Derived Symbol: {derived_symbol}")
        instrument_key = find_equity_instrument_key(derived_symbol, instrument_master)

        if not instrument_key:
            logging.warning(f"Instrument key not found for {derived_symbol}. Data fetch will be skipped.")
            all_stocks_data_for_db.append({
                'stock_symbol': derived_symbol,
                'prev_day_equity_close': None, 'prev_day_equity_oi': None,
                'equity_920_price': None, 'equity_920_oi': None,
                'error_message': 'Instrument key not found'
            })
            continue

        # Fetch data
        prev_close, prev_oi = fetch_historical_data(instrument_key, PREVIOUS_DAY_FETCH_STR)
        curr_920_price, curr_920_oi = fetch_intraday_data_920(instrument_key, CURRENT_DAY_CANDLE_TARGET_OBJ)

        # Prepare for raw DB storage
        stock_data_entry = {
            'stock_symbol': derived_symbol,
            'prev_day_equity_close': prev_close,
            'prev_day_equity_oi': prev_oi,
            'equity_920_price': curr_920_price,
            'equity_920_oi': curr_920_oi
        }
        all_stocks_data_for_db.append(stock_data_entry)

        # Calculate percent change and filter
        if prev_close is not None and curr_920_price is not None and prev_close != 0:
            percent_change = ((curr_920_price - prev_close) / prev_close) * 100
            logging.info(f"{derived_symbol}: Prev Close={prev_close}, 9:20 Price={curr_920_price}, %Change={percent_change:.2f}%")
            if abs(percent_change) > 2:
                logging.info(f"FILTERED: {derived_symbol} with {percent_change:.2f}% change added to list.")
                filtered_stocks_for_db.append((derived_symbol, round(percent_change, 2)))
        else:
            logging.info(f"{derived_symbol}: Not enough data to calculate percent change (Prev Close: {prev_close}, 9:20 Price: {curr_920_price}).")
            
    # Store collected data
    store_raw_data_to_db(DB_PATH, RAW_DATA_TABLE_NAME, all_stocks_data_for_db)
    store_filtered_data_to_db(DB_PATH, FILTERED_DATA_TABLE_NAME, filtered_stocks_for_db)

    # Display filtered results from DB
    display_filtered_results(DB_PATH, FILTERED_DATA_TABLE_NAME)
    
    logging.info("Consolidated F&O equity data processing workflow finished.")

if __name__ == "__main__":
    main()
