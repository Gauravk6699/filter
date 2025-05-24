import flask
from flask import Flask, request, jsonify
from flask_cors import CORS # For enabling CORS
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
# (Mostly copied from fno_equity_analyzer.py, adapted for Flask context)
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

API_KEY = "f872ad84-86f7-45ed-9307-eede7c7399ed" # Provided for completeness
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzVUM3U0wiLCJqdGkiOiI2ODJmZmFjMDA4ZjVkZTYxM2MyMzBiYTgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzQ3OTc0ODQ4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDgwMzc2MDB9.MSyl3ODIhbksqXm_Cuj6imKDOeCTys9pZIKYujaOrQE"
# CRITICAL NOTE: ACCESS_TOKEN is for 2025-05-17. Using it for other dates will likely cause API auth failures.

DB_PATH = "./upstox_data_v2.db" # Database path
API_BASE_URL = "https://api.upstox.com"
TARGET_920_TIME_OBJ = time(9, 20, 0) # For 9:20 AM data

# --- 2. Logging Setup ---
# Basic logging; for Flask, you might want more sophisticated setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__) # Flask specific logger can also be used: app.logger

# --- Global Cache for Instrument Master ---
_instrument_master_cache = None

# --- 3. Helper Functions (from fno_equity_analyzer.py, slightly adapted) ---

def derive_trading_symbol(stock_name_with_suffix):
    match = re.search(r'\((.*?)\)', stock_name_with_suffix)
    if match:
        return match.group(1).upper()
    symbol = stock_name_with_suffix.split()[0].upper()
    symbol = re.sub(r'[^A-Z0-9]', '', symbol)
    return symbol

def get_instrument_master():
    global _instrument_master_cache
    if _instrument_master_cache is not None:
        logger.info("Returning instrument master from cache.")
        return _instrument_master_cache
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        logger.info(f"Downloading instrument master from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        gzipped_content = response.content
        decompressed_content = gzip.decompress(gzipped_content)
        json_data_str = decompressed_content.decode('utf-8')
        _instrument_master_cache = json.loads(json_data_str)
        logger.info(f"Successfully loaded and cached {len(_instrument_master_cache)} instruments.")
        return _instrument_master_cache
    except Exception as e:
        logger.error(f"Error fetching/processing instrument master: {e}")
        return None

def find_equity_instrument_key(stock_symbol, instrument_master):
    if not instrument_master: return None
    for instrument in instrument_master:
        if (instrument.get('trading_symbol') == stock_symbol and
            instrument.get('instrument_type') == 'EQ' and
            instrument.get('segment') == 'NSE_EQ' and # As per subtask 13
            instrument.get('exchange') == 'NSE'):
            return instrument.get('instrument_key')
    logger.warning(f"Instrument key for {stock_symbol} not found in master list.")
    return None

def fetch_historical_data_for_analyzer(instrument_key, date_str): # Renamed to avoid conflict if other versions exist
    if not instrument_key: return None, None, "Instrument key was None"
    encoded_key = quote(instrument_key)
    api_url = f"{API_BASE_URL}/v3/historical-candle/{encoded_key}/days/1/{date_str}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}
    logger.info(f"Fetching historical data for {instrument_key} on {date_str}")
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json().get("data", {}).get("candles")
        if data and data[0] and len(data[0]) >= 7:
            return data[0][4], data[0][6], None # close, oi, error
        return None, None, "No/incomplete candle data"
    except requests.exceptions.HTTPError as e:
        err_msg = f"HTTP Error for {instrument_key} on {date_str} (hist): {e.response.status_code} - {e.response.text[:100]}"
        logger.error(err_msg)
        return None, None, err_msg
    except Exception as e:
        err_msg = f"General error for {instrument_key} on {date_str} (hist): {e}"
        logger.error(err_msg)
        return None, None, err_msg

def fetch_intraday_data_920_for_analyzer(instrument_key, target_candle_date_obj): # Renamed
    if not instrument_key: return None, None, "Instrument key was None"
    encoded_key = quote(instrument_key)
    api_request_date_obj = target_candle_date_obj + timedelta(days=1) # API D+1 quirk
    api_request_date_str = api_request_date_obj.strftime('%Y-%m-%d')
    
    api_url = f"{API_BASE_URL}/v3/historical-candle/{encoded_key}/minutes/1/{api_request_date_str}"
    params = {"from_date": api_request_date_str}
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}
    logger.info(f"Fetching intraday for {instrument_key} (target {target_candle_date_obj}, API req {api_request_date_str})")
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        candles = response.json().get("data", {}).get("candles")
        if candles:
            for candle in candles:
                if len(candle) >= 7:
                    ts_str = candle[0]
                    try:
                        dt_obj = datetime.fromisoformat(ts_str)
                        if dt_obj.date() == target_candle_date_obj and dt_obj.time() == TARGET_920_TIME_OBJ:
                            return candle[4], candle[6], None # close, oi, error
                    except ValueError: 
                        logger.warning(f"Timestamp parse error for {ts_str} in {instrument_key}")
                        continue # Skip malformed timestamp
            return None, None, f"9:20 candle for {target_candle_date_obj} not in response"
        return None, None, "No intraday candles in response"
    except requests.exceptions.HTTPError as e:
        err_msg = f"HTTP Error for {instrument_key} (intraday {target_candle_date_obj}): {e.response.status_code} - {e.response.text[:100]}"
        logger.error(err_msg)
        return None, None, err_msg
    except Exception as e:
        err_msg = f"General error for {instrument_key} (intraday {target_candle_date_obj}): {e}"
        logger.error(err_msg)
        return None, None, err_msg

def init_db_for_analyzer(db_path, table_name_raw, table_name_filtered):
    conn = None
    try:
        logger.info(f"Initializing DB at {db_path} for tables {table_name_raw}, {table_name_filtered}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name_raw}")
        cursor.execute(f"""CREATE TABLE {table_name_raw} (
            id INTEGER PRIMARY KEY AUTOINCREMENT, stock_symbol TEXT,
            prev_day_equity_close REAL, prev_day_equity_oi INTEGER,
            equity_920_price REAL, equity_920_oi INTEGER, record_timestamp TEXT, error_message TEXT)""")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name_filtered}")
        cursor.execute(f"""CREATE TABLE {table_name_filtered} (
            id INTEGER PRIMARY KEY AUTOINCREMENT, stock_symbol TEXT,
            percent_change REAL, record_timestamp TEXT)""")
        conn.commit()
        logger.info("DB initialized.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error during DB init: {e}")
        raise # Re-raise to signal failure in analysis function
    finally:
        if conn: conn.close()

def store_data_to_db_for_analyzer(db_path, table_name_raw, table_name_filtered, all_data, filtered_data):
    conn = None
    try:
        logger.info(f"Storing data to DB. Raw: {len(all_data)}, Filtered: {len(filtered_data)}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        current_ts = datetime.now(timezone.utc).isoformat()
        
        for record in all_data: # Assumes record is a dict
            cursor.execute(f"""INSERT INTO {table_name_raw} 
                (stock_symbol, prev_day_equity_close, prev_day_equity_oi,
                 equity_920_price, equity_920_oi, record_timestamp, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?);""", 
                (record['stock_symbol'], record.get('prev_day_equity_close'), record.get('prev_day_equity_oi'),
                 record.get('equity_920_price'), record.get('equity_920_oi'), current_ts, record.get('error_message')))
        
        for symbol, p_change in filtered_data: # Assumes (symbol, percent_change) tuples
            cursor.execute(f"""INSERT INTO {table_name_filtered}
                (stock_symbol, percent_change, record_timestamp) VALUES (?, ?, ?);""",
                (symbol, p_change, current_ts))
        conn.commit()
        logger.info("Data stored in DB.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error storing data: {e}")
        # Potentially add to an errors_list to be returned
    finally:
        if conn: conn.close()

# --- 4. Core Analysis Function ---
def analyze_stocks_for_dates(current_processing_date_str, previous_processing_date_str):
    logger.info(f"Starting analysis for current_date: {current_processing_date_str}, previous_date: {previous_processing_date_str}")
    
    # Date conversions
    try:
        current_date_obj = datetime.strptime(current_processing_date_str, '%Y-%m-%d').date()
        # previous_date_obj = datetime.strptime(previous_processing_date_str, '%Y-%m-%d').date() # Not directly used by fetchers
    except ValueError:
        return {"error": "Invalid date format. Please use YYYY-MM-DD."}

    # Dynamic table names
    table_suffix = current_processing_date_str.replace('-', '_')
    raw_data_table = f"data_{table_suffix}"
    filtered_data_table = f"filtered_{table_suffix}"

    instrument_master = get_instrument_master()
    if not instrument_master:
        return {"error": "Failed to load instrument master."}

    try:
        init_db_for_analyzer(DB_PATH, raw_data_table, filtered_data_table)
    except Exception as e: # Catch DB init errors
        return {"error": f"Failed to initialize database: {e}"}


    all_stocks_data_for_db = []
    filtered_stocks_output = [] # For the JSON response: list of dicts
    errors_list = []
    processed_count = 0

    for stock_name_full in F_AND_O_STOCK_NAMES:
        processed_count += 1
        logger.info(f"Processing ({processed_count}/{len(F_AND_O_STOCK_NAMES)}): {stock_name_full}")
        derived_symbol = derive_trading_symbol(stock_name_full)
        
        api_error_message = None # To store specific API errors for a stock

        if not derived_symbol:
            err_msg = f"Symbol derivation failed for '{stock_name_full}'"
            logger.warning(err_msg)
            errors_list.append(err_msg)
            all_stocks_data_for_db.append({'stock_symbol': stock_name_full, 'error_message': err_msg})
            continue
            
        instrument_key = find_equity_instrument_key(derived_symbol, instrument_master)
        if not instrument_key:
            err_msg = f"Instrument key not found for {derived_symbol}."
            logger.warning(err_msg)
            errors_list.append(err_msg)
            all_stocks_data_for_db.append({'stock_symbol': derived_symbol, 'error_message': err_msg})
            continue

        prev_close, prev_oi, prev_err = fetch_historical_data_for_analyzer(instrument_key, previous_processing_date_str)
        if prev_err: api_error_message = prev_err
        
        curr_920_price, curr_920_oi, curr_err = fetch_intraday_data_920_for_analyzer(instrument_key, current_date_obj)
        if curr_err and not api_error_message : api_error_message = curr_err # Prioritize prev_err if both exist
        elif curr_err: api_error_message += f"; {curr_err}"


        stock_data_entry = {
            'stock_symbol': derived_symbol,
            'prev_day_equity_close': prev_close, 'prev_day_equity_oi': prev_oi,
            'equity_920_price': curr_920_price, 'equity_920_oi': curr_920_oi,
            'error_message': api_error_message
        }
        all_stocks_data_for_db.append(stock_data_entry)

        if prev_close is not None and curr_920_price is not None and prev_close != 0:
            percent_change = ((curr_920_price - prev_close) / prev_close) * 100
            logger.info(f"{derived_symbol}: Prev Close={prev_close}, 9:20 Price={curr_920_price}, %Change={percent_change:.2f}%")
            if abs(percent_change) > 2: # Filter condition
                logger.info(f"FILTERED: {derived_symbol} with {percent_change:.2f}% change.")
                filtered_stocks_output.append({"symbol": derived_symbol, "percent_change": round(percent_change, 2)})
        elif not api_error_message: # Only add calculation error if no API error already logged
            calc_err = f"{derived_symbol}: Not enough data to calculate % change (Prev: {prev_close}, Curr: {curr_920_price})"
            logger.info(calc_err)
            # Add to main errors_list if this specific error is important to report
            # errors_list.append(calc_err) # Decided not to add this to main errors_list for now

    store_data_to_db_for_analyzer(DB_PATH, raw_data_table, filtered_data_table, all_stocks_data_for_db, 
                                  [(item['symbol'], item['percent_change']) for item in filtered_stocks_output])
    
    # Add API/Key errors to the main errors_list for the response
    for stock_entry in all_stocks_data_for_db:
        if stock_entry.get('error_message') and 'Instrument key not found' not in stock_entry['error_message'] and 'Symbol derivation failed' not in stock_entry['error_message']: # Filter out non-API errors
            errors_list.append(f"{stock_entry['stock_symbol']}: {stock_entry['error_message']}")


    return {
        "filtered_stocks": filtered_stocks_output,
        "processed_stocks_count": processed_count,
        "errors_list": errors_list[:10] # Return only first 10 API related errors to keep response size manageable
    }

# --- 5. Flask Application Setup ---
app = Flask(__name__)
CORS(app) # Enable CORS for all routes

@app.route('/api/analyze_stocks', methods=['GET'])
def api_analyze_stocks():
    logger.info("Received request for /api/analyze_stocks")
    previous_date_str = request.args.get('previous_date')
    current_date_str = request.args.get('current_date')

    if not previous_date_str or not current_date_str:
        logger.error("Missing date parameters.")
        return jsonify({"error": "Missing 'previous_date' or 'current_date' query parameters."}), 400

    try:
        # Basic date format validation (YYYY-MM-DD)
        datetime.strptime(previous_date_str, '%Y-%m-%d')
        datetime.strptime(current_date_str, '%Y-%m-%d')
    except ValueError:
        logger.error(f"Invalid date format: prev={previous_date_str}, curr={current_date_str}")
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400
    
    logger.info(f"Processing analysis for prev_date={previous_date_str}, curr_date={current_date_str}")
    # NOTE: The hardcoded ACCESS_TOKEN is for 2025-05-17. 
    # Providing other dates (e.g., 2025-05-22, 2025-05-23) will likely result in API authentication (401) errors
    # for each stock, and thus 'None' for all price/OI data.
    # The 'filtered_stocks' list will likely be empty. This is expected behavior given the token constraint.
    try:
        analysis_result = analyze_stocks_for_dates(current_date_str, previous_date_str)
        if "error" in analysis_result and ("instrument master" in analysis_result["error"] or "database" in analysis_result["error"]):
             # If there's a critical setup error, return 500
            return jsonify(analysis_result), 500
        return jsonify(analysis_result), 200
    except Exception as e:
        logger.critical(f"Unexpected error during stock analysis: {e}", exc_info=True)
        return jsonify({"error": "An unexpected internal server error occurred."}), 500

if __name__ == '__main__':
    # Note: For production, use a proper WSGI server like Gunicorn or Waitress.
    # Flask's development server is not suitable for production.
    # Host '0.0.0.0' makes it accessible externally if run in a container/VM.
    app.run(debug=False, host='0.0.0.0', port=5001) # debug=False for cleaner logs in this context
    # To test: After running, open browser to e.g., 
    # http://localhost:5001/api/analyze_stocks?previous_date=2025-05-22&current_date=2025-05-23
    # (Expect API errors in logs and empty filtered_stocks due to token/date mismatch for these example dates)
    # Or use dates for which token is valid if you want to see data processing:
    # http://localhost:5001/api/analyze_stocks?previous_date=2025-05-16&current_date=2025-05-17
    # (This would work if the API calls within analyze_stocks_for_dates used these for data fetching)
    # The current analyze_stocks_for_dates uses the DATES GIVEN IN PARAMETERS for fetching. Good.
