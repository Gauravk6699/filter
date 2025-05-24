import requests
import gzip
import json
import io
import re # For symbol derivation
from datetime import datetime, date, time, timedelta, timezone
from urllib.parse import quote
import logging

# --- 1. Configuration & Initialization ---
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

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzVUM3U0wiLCJqdGkiOiI2ODJmZmFjMDA4ZjVkZTYxM2MyMzBiYTgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzQ3OTc0ODQ4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDgwMzc2MDB9.MSyl3ODIhbksqXm_Cuj6imKDOeCTys9pZIKYujaOrQE"
API_BASE_URL = "https://api.upstox.com"

# Test Dates
PREVIOUS_DAY_STR = "2025-05-22" # Thursday
CURRENT_DAY_STR = "2025-05-23"  # Friday
CURRENT_DAY_OBJ = date(2025, 5, 23)
TARGET_920_TIME = time(9, 20, 0)

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

# Global Cache for Instrument Master
_instrument_master_cache = None

def get_instrument_master():
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

# --- 2a. Derive Trading Symbol ---
def derive_trading_symbol(stock_name):
    match = re.search(r'\((.*?)\)', stock_name)
    if match:
        return match.group(1).upper() # Content within parentheses
    return stock_name.split()[0].upper() # First word

# --- 2b. Find Equity Instrument Key ---
def find_equity_instrument_key(trading_symbol):
    master_list = get_instrument_master()
    if not master_list: return None
    for instrument in master_list:
        if (instrument.get('trading_symbol') == trading_symbol and
            instrument.get('instrument_type') == 'EQ' and
            instrument.get('segment') == 'NSE_EQ' and
            instrument.get('exchange') == 'NSE'):
            return instrument.get('instrument_key')
    return None

# --- 2c. Fetch Previous Day's Data ---
def fetch_prev_day_data(instrument_key, date_str):
    if not instrument_key: return None, None
    encoded_key = quote(instrument_key)
    api_url = f"{API_BASE_URL}/v3/historical-candle/{encoded_key}/days/1/{date_str}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}
    logging.info(f"Fetching prev day data for {instrument_key} on {date_str} from {api_url}")
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json().get("data", {}).get("candles")
        if data and data[0]:
            return data[0][4], data[0][6] # close, oi
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logging.warning(f"Auth error (401) for prev_day data {instrument_key} on {date_str}. Token likely invalid for this date.")
        else:
            logging.error(f"HTTP Error fetching prev_day data for {instrument_key} on {date_str}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logging.error(f"Error fetching prev_day data for {instrument_key} on {date_str}: {e}")
    return None, None

# --- 2d. Fetch Current Day's 9:20 AM Data ---
def fetch_current_day_920_data(instrument_key, target_date_obj):
    if not instrument_key: return None, None
    encoded_key = quote(instrument_key)
    
    # IMPORTANT API BEHAVIOR: This endpoint tends to return data for day D-1 when D is specified for both to_date and from_date.
    # So, to get data for target_date_obj (e.g., 2025-05-23), we must request for target_date_obj + 1 day.
    api_request_date_obj = target_date_obj + timedelta(days=1)
    api_request_date_str = api_request_date_obj.strftime('%Y-%m-%d')

    api_url = f"{API_BASE_URL}/v3/historical-candle/{encoded_key}/minutes/1/{api_request_date_str}"
    params = {"from_date": api_request_date_str}
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}
    
    logging.info(f"Fetching 9:20 AM data for {instrument_key} (target: {target_date_obj}, request: {api_request_date_str}) from {api_url}")
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        candles = response.json().get("data", {}).get("candles")
        if candles:
            for candle in candles:
                if len(candle) >= 7:
                    ts_str = candle[0]
                    dt_obj = datetime.fromisoformat(ts_str)
                    if dt_obj.date() == target_date_obj and dt_obj.time() == TARGET_920_TIME:
                        return candle[4], candle[6] # close, oi
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logging.warning(f"Auth error (401) for 9:20 AM data {instrument_key} on {target_date_obj}. Token likely invalid for this date.")
        else:
            logging.error(f"HTTP Error fetching 9:20 AM data for {instrument_key} on {target_date_obj}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logging.error(f"Error fetching 9:20 AM data for {instrument_key} on {target_date_obj}: {e}")
    return None, None

# --- Main Processing Logic ---
def process_fno_stocks():
    logging.info("Starting F&O stock processing.")
    get_instrument_master() # Ensure master list is loaded and cached

    all_stocks_raw_data = []
    filtered_stocks_percent_change = []

    for stock_name in F_AND_O_STOCK_NAMES:
        logging.info(f"Processing stock: {stock_name}")
        derived_symbol = derive_trading_symbol(stock_name)
        if not derived_symbol:
            logging.warning(f"Could not derive symbol for {stock_name}. Skipping.")
            continue
        
        logging.info(f"Derived symbol: {derived_symbol} for {stock_name}")
        instrument_key = find_equity_instrument_key(derived_symbol)
        if not instrument_key:
            logging.warning(f"Instrument key not found for {derived_symbol}. Skipping.")
            all_stocks_raw_data.append({
                "stock_symbol": derived_symbol, "prev_day_equity_close": None, "prev_day_equity_oi": None,
                "equity_920_price": None, "equity_920_oi": None, "error": "Instrument key not found"
            })
            continue

        prev_close, prev_oi = fetch_prev_day_data(instrument_key, PREVIOUS_DAY_STR)
        curr_920_price, curr_920_oi = fetch_current_day_920_data(instrument_key, CURRENT_DAY_OBJ)
        
        # Store raw data
        raw_data_entry = {
            "stock_symbol": derived_symbol,
            "prev_day_equity_close": prev_close,
            "prev_day_equity_oi": prev_oi,
            "equity_920_price": curr_920_price,
            "equity_920_oi": curr_920_oi
        }
        all_stocks_raw_data.append(raw_data_entry)

        # Calculate Percent Change
        percent_change = None
        if prev_close is not None and curr_920_price is not None and prev_close != 0:
            percent_change = ((curr_920_price - prev_close) / prev_close) * 100
            logging.info(f"{derived_symbol}: Prev Close={prev_close}, 9:20 Price={curr_920_price}, %Change={percent_change:.2f}%")
        
        # Apply Filter
        if percent_change is not None and (percent_change > 2 or percent_change < -2):
            filtered_stocks_percent_change.append((derived_symbol, round(percent_change, 2)))
            logging.info(f"{derived_symbol} added to filtered list with {percent_change:.2f}% change.")
            
    logging.info("Finished processing all F&O stocks.")
    return all_stocks_raw_data, filtered_stocks_percent_change

if __name__ == "__main__":
    raw_data, filtered_data = process_fno_stocks()
    
    logging.info("\n--- All Processed Stocks Raw Data (Sample) ---")
    for i, item in enumerate(raw_data):
        if i < 5: # Print sample of 5
            print(json.dumps(item))
        elif i == 5:
            print("...") # Indicate more data
            break
            
    logging.info("\n--- Filtered Stocks (Percent Change > 2% or < -2%) ---")
    if filtered_data:
        for item in filtered_data:
            print(json.dumps(item))
    else:
        print("No stocks met the filter criteria.")
    
    # The actual return values are `raw_data` and `filtered_data`.
    # These would be used by subsequent steps for DB storage or final output.
    # This script primarily focuses on the core processing logic.
    # For this subtask, the "returned" values are implicitly available in these variables
    # upon script completion, and their structure is demonstrated by the print statements.
