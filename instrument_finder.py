import requests
import gzip
import json
import io
from datetime import datetime, date, timedelta

# --- Data Loading (adapted from nse_downloader.py) ---
def load_nse_instruments():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        gzipped_content = response.raw.read()
        decompressed_content = gzip.decompress(gzipped_content)
        json_data_str = decompressed_content.decode('utf-8')
        parsed_data = json.loads(json_data_str)
        return parsed_data
    except Exception as e:
        print(f"Error loading or parsing instrument data: {e}")
        return None

# --- Instrument Finding Logic ---
def find_reliance_keys(instruments):
    if not instruments:
        print("Instrument data is empty or not loaded.")
        return {"equity_key": None, "future_key": None, "error": "Instrument data not loaded"}

    reliance_equity_key = None
    reliance_future_key = None
    
    # Target symbols
    equity_trading_symbol = 'RELIANCE'
    underlying_symbol_future = 'RELIANCE' # For futures, we usually check underlying

    # For futures: find the contract with the nearest expiry in the future
    current_dt = datetime.now()
    nearest_future_expiry = None
    
    # Debug: print sample instrument details to understand segments and types
    # print("Sample instruments (first 5):")
    # for i in range(min(5, len(instruments))):
    # print(instruments[i])
    # print("-" * 20)

    for instrument in instruments:
        try:
            # 1. Find Equity Instrument Key
            if (instrument.get('trading_symbol') == equity_trading_symbol and
                instrument.get('instrument_type') == 'EQ' and
                instrument.get('exchange') == 'NSE' and # ensure it's NSE exchange
                'EQ' in instrument.get('segment', '')): # e.g. NSE_EQ or similar
                reliance_equity_key = instrument.get('instrument_key')
                # print(f"Found equity: {instrument}") # Debug

            # 2. Find Stock Future Instrument Key (Current Month)
            if (instrument.get('underlying_symbol') == underlying_symbol_future and
                instrument.get('instrument_type') == 'FUT' and # Common type for stock futures
                instrument.get('exchange') == 'NSE' and
                'FO' in instrument.get('segment', '')): # e.g. NSE_FO, NFO_FO
                
                expiry_ms = instrument.get('expiry')
                if expiry_ms:
                    # Convert milliseconds to datetime object
                    # expiry_date = datetime.fromtimestamp(expiry_ms / 1000).date() # .date() to compare dates only
                    expiry_datetime = datetime.fromtimestamp(expiry_ms / 1000)

                    # We need the contract that expires this month or the nearest future month
                    # It must expire on or after today
                    if expiry_datetime >= current_dt:
                        if nearest_future_expiry is None or expiry_datetime < nearest_future_expiry:
                            nearest_future_expiry = expiry_datetime
                            reliance_future_key = instrument.get('instrument_key')
                            # print(f"Found potential future: {instrument}, Expiry: {expiry_datetime}") # Debug
                        # If two futures have the same expiry datetime (should not happen for different keys),
                        # this logic takes the first one it encounters.
        
        except Exception as e:
            # print(f"Error processing instrument: {instrument}. Error: {e}") # Debug
            continue # Skip to next instrument if there's an error processing one

    if not reliance_equity_key:
        print(f"Equity instrument for {equity_trading_symbol} not found.")
    if not reliance_future_key:
        print(f"Current month future for {underlying_symbol_future} not found.")
        
    return {"equity_key": reliance_equity_key, "future_key": reliance_future_key}

if __name__ == "__main__":
    print("Loading instrument master data...")
    all_instruments = load_nse_instruments()
    
    if all_instruments:
        print("Instrument data loaded. Searching for Reliance keys...")
        keys = find_reliance_keys(all_instruments)
        print("Search complete.")
        print(json.dumps(keys)) # Output in JSON format as requested
    else:
        print("Failed to load instrument data. Cannot perform search.")
        print(json.dumps({"equity_key": None, "future_key": None, "error": "Failed to load instrument data"}))
