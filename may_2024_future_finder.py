import requests
import gzip
import json
import io
from datetime import datetime, date
import calendar

# --- Data Loading ---
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

def get_last_thursday(year, month):
    month_calendar = calendar.monthcalendar(year, month)
    for week in reversed(month_calendar):
        thursday = week[calendar.THURSDAY]
        if thursday != 0:
            return date(year, month, thursday)
    return None

# --- Instrument Finding Logic ---
def find_may_2024_reliance_future(instruments):
    if not instruments:
        return {"may_2024_future_key": None, "error": "Instrument data not loaded"}

    target_underlying = 'RELIANCE'
    target_year = 2024
    target_month = 5 # May
    
    target_expiry_date = get_last_thursday(target_year, target_month)
    if not target_expiry_date:
        return {"may_2024_future_key": None, "error": "Could not determine target expiry date (last Thursday)."}
    
    print(f"Targeting {target_underlying} future for month {target_month}/{target_year}, specifically expiring on: {target_expiry_date}")

    may_2024_future_key = None
    potential_may_contracts = [] # Store all futures expiring in May 2024

    print(f"\nScanning all {len(instruments)} instruments for Reliance futures...")
    reliance_futures_found_count = 0
    sample_reliance_futures_printed = 0

    for instrument in instruments:
        try:
            # Basic filters for any Reliance future
            if (instrument.get('underlying_symbol') == target_underlying and
                instrument.get('instrument_type') == 'FUT' and # Standard for stock futures
                instrument.get('exchange') == 'NSE' and
                'FO' in instrument.get('segment', '')): # NSE_FO or NFO_FO etc.
                
                reliance_futures_found_count += 1
                expiry_ms = instrument.get('expiry')
                
                if expiry_ms:
                    instrument_expiry_date = datetime.utcfromtimestamp(expiry_ms / 1000).date()

                    # Print a few samples of Reliance futures found for broader context
                    if sample_reliance_futures_printed < 5:
                        print(f"  Sample RELIANCE FUT: {instrument.get('trading_symbol')}, "
                              f"Type: {instrument.get('instrument_type')}, Seg: {instrument.get('segment')}, "
                              f"Exp: {instrument_expiry_date}, Key: {instrument.get('instrument_key')}")
                        sample_reliance_futures_printed += 1

                    # Check if this contract expires in May 2024
                    if instrument_expiry_date.year == target_year and instrument_expiry_date.month == target_month:
                        potential_may_contracts.append({
                            "key": instrument.get('instrument_key'),
                            "trading_symbol": instrument.get('trading_symbol'),
                            "expiry_date_obj": instrument_expiry_date,
                            "instrument_details": instrument # Store full details for review if needed
                        })
                        
                        # Check if this specific contract is the one expiring on the target last Thursday
                        if instrument_expiry_date == target_expiry_date:
                            may_2024_future_key = instrument.get('instrument_key')
                            print(f"SUCCESS: Found exact match for {target_expiry_date}: {instrument.get('trading_symbol')}, Key: {may_2024_future_key}")
                            # It's possible there could be other instruments mapping to same expiry (e.g. mini contracts)
                            # but for standard monthly, this should be unique enough. We take the first.
                            break # Exit loop once the primary target is found
                
        except Exception as e:
            # print(f"Error processing instrument: {instrument.get('instrument_key', 'N/A')}. Error: {e}")
            continue 
    
    print(f"\nTotal Reliance futures contracts processed (basic filter match): {reliance_futures_found_count}")

    if may_2024_future_key:
        return {"may_2024_future_key": may_2024_future_key}
    
    # If exact match wasn't found, analyze potential May contracts
    if potential_may_contracts:
        print(f"\nExact last Thursday match ({target_expiry_date}) not found directly.")
        print(f"Found {len(potential_may_contracts)} other Reliance futures contracts expiring in May {target_year}:")
        # Sort them by expiry date to make it easier to analyze
        potential_may_contracts.sort(key=lambda x: x['expiry_date_obj'])
        for contract in potential_may_contracts:
            print(f"  Symbol: {contract['trading_symbol']}, Key: {contract['key']}, Expiry: {contract['expiry_date_obj']}")
            # One could add logic here: if only one, or if one clearly matches "monthly" pattern
            # For now, if exact last Thursday not found, we report not found to stick to primary logic.
        return {"may_2024_future_key": None, "error": f"Exact future for {target_underlying} expiring on {target_expiry_date} not found. Other May {target_year} contracts listed above."}
    else:
        print(f"\nNo Reliance futures contracts found expiring in May {target_year} at all.")
        return {"may_2024_future_key": None, "error": f"No future contracts for {target_underlying} found expiring in May {target_year}."}

if __name__ == "__main__":
    all_instruments = load_nse_instruments()
    
    if all_instruments:
        result = find_may_2024_reliance_future(all_instruments)
        print("\n--- Search Result ---")
        print(json.dumps(result))
    else:
        print("\n--- Search Result ---")
        print(json.dumps({"may_2024_future_key": None, "error": "Failed to load instrument data"}))
