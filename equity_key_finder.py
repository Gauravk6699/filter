import requests
import gzip
import json
import io

# --- Data Loading (adapted from nse_downloader.py) ---
def load_nse_instruments():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        print("Downloading instrument master data...")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        print("Download complete. Decompressing...")
        gzipped_content = response.content 
        decompressed_content = gzip.decompress(gzipped_content)
        print("Decompression complete. Parsing JSON...")
        json_data_str = decompressed_content.decode('utf-8')
        parsed_data = json.loads(json_data_str)
        print(f"Parsing complete. Loaded {len(parsed_data)} instruments.")
        return parsed_data
    except Exception as e:
        print(f"An error occurred during data loading: {e}")
        return None

# --- Equity Key Finding Logic ---
def find_reliance_equity_key(instruments):
    if not instruments:
        print("Instrument data is empty or not loaded.")
        return {"equity_key": None, "error": "Instrument data not loaded"}

    reliance_equity_key = None
    target_trading_symbol = 'RELIANCE'
    target_instrument_type = 'EQ'
    target_segment = 'NSE_EQ' # As specified in the prompt

    print(f"Searching for equity key for {target_trading_symbol} with type '{target_instrument_type}' and segment '{target_segment}'...")
    
    found_instrument_details = None # For logging

    for instrument in instruments:
        try:
            # Check all conditions
            if (instrument.get('trading_symbol') == target_trading_symbol and
                instrument.get('instrument_type') == target_instrument_type and
                instrument.get('segment') == target_segment and # Strict check for segment
                instrument.get('exchange') == 'NSE'): # Good to ensure it's NSE exchange
                
                reliance_equity_key = instrument.get('instrument_key')
                found_instrument_details = instrument # Store for logging
                break # Found, no need to search further
        
        except Exception as e:
            # This might happen if an instrument record is malformed, though unlikely for this dataset.
            # print(f"Error processing an instrument record: {instrument}. Error: {e}") 
            continue 

    if reliance_equity_key:
        print(f"Found equity instrument: {found_instrument_details}")
        return {"equity_key": reliance_equity_key}
    else:
        print(f"Equity instrument for {target_trading_symbol} (type: {target_instrument_type}, segment: {target_segment}) not found.")
        return {"equity_key": None, "error": f"Equity instrument for {target_trading_symbol} not found with specified criteria."}

if __name__ == "__main__":
    all_instruments = load_nse_instruments()
    
    if all_instruments:
        result = find_reliance_equity_key(all_instruments)
        print("\n--- Search Result (JSON) ---")
        print(json.dumps(result))
    else:
        print("\n--- Search Result (JSON) ---")
        print(json.dumps({"equity_key": None, "error": "Failed to load instrument data"}))
