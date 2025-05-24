import requests
import json
from datetime import datetime, date, time 
from urllib.parse import quote

# Configuration from prompt
API_BASE_URL = "https://api.upstox.com"
FUTURES_INSTRUMENT_KEY_RAW = "NSE_FO|57507" # Reliance Futures (May 2025 expiry)
FUTURES_INSTRUMENT_KEY_ENCODED = quote(FUTURES_INSTRUMENT_KEY_RAW)
UPSTOX_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzVUM3U0wiLCJqdGkiOiI2ODJmZmFjMDA4ZjVkZTYxM2MyMzBiYTgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzQ3OTc0ODQ4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDgwMzc2MDB9.MSyl3ODIhbksqXm_Cuj6imKDOeCTys9pZIKYujaOrQE"

# Target time and ASSUMED current execution date for test
TARGET_CANDLE_TIME = time(9, 20, 0)
ASSUMED_EXECUTION_DATE_STR = "2025-05-17" # Friday, May 17, 2025
ASSUMED_EXECUTION_DATE_OBJ = date(2025, 5, 17) 
# Note: The token is valid only for 2025-05-17. The intraday API fetches for the server's current day.
# If this script is run on a day other than 2025-05-17, the token will be invalid, 
# OR the API will return data for the actual current day, which won't match ASSUMED_EXECUTION_DATE_OBJ.

def fetch_intraday_futures_920_data():
    access_token = UPSTOX_ACCESS_TOKEN
    if not access_token:
        return {"error": "UPSTOX_ACCESS_TOKEN is missing internally."}

    # Using the INTRADAY API URL (does not take a date in the path/query)
    # Unit "minutes", interval "1" as per previous successful structure for intraday-like paths.
    # If this specific endpoint expects "minute" (singular), it might fail.
    # Let's try "minutes" first, as it was accepted by the historical endpoint with /minutes/1/{date}
    # The prompt specifies ".../minutes/1" for the intraday endpoint.
    api_url = f"{API_BASE_URL}/v3/historical-candle/intraday/{FUTURES_INSTRUMENT_KEY_ENCODED}/minutes/1"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    print(f"Fetching URL for intraday data: {api_url}")
    print(f"Assuming current execution date is: {ASSUMED_EXECUTION_DATE_STR}. Will search for 09:20 candle for this date.")

    try:
        response = requests.get(api_url, headers=headers, timeout=20)
        response.raise_for_status() 
        
        response_json = response.json()
        candles = response_json.get("data", {}).get("candles")

        if not candles:
            # This could happen if run outside market hours, or if 2025-05-17 was a non-trading day.
            # Or if the token is invalid for the *actual* current server date.
            return {"error": f"No candle data returned by the intraday API. This could be due to market hours, the assumed date {ASSUMED_EXECUTION_DATE_STR} being a non-trading day, or token issues with the actual current server date.", "response_details": response_json, "url_attempted": api_url}

        found_candle_data = None
        actual_dates_in_response = set()

        for candle_item in candles: 
            if not isinstance(candle_item, list) or len(candle_item) < 7:
                print(f"Skipping malformed candle: {candle_item}")
                continue

            timestamp_str = candle_item[0]
            try:
                dt_object = datetime.fromisoformat(timestamp_str)
                actual_dates_in_response.add(dt_object.date())
                
                # Filter by the ASSUMED_EXECUTION_DATE_OBJ and TARGET_CANDLE_TIME
                if dt_object.date() == ASSUMED_EXECUTION_DATE_OBJ and dt_object.time() == TARGET_CANDLE_TIME:
                    found_candle_data = candle_item
                    break
            except ValueError:
                print(f"Could not parse timestamp: {timestamp_str}")
                continue
        
        unique_dates_found_str = ", ".join(sorted([d.strftime('%Y-%m-%d') for d in actual_dates_in_response]))

        if found_candle_data:
            closing_price = found_candle_data[4] 
            open_interest = found_candle_data[6] 
            return {"futures_920_price": closing_price, "futures_920_oi": open_interest}
        else:
            return {
                "error": f"09:20:00 candle for assumed date {ASSUMED_EXECUTION_DATE_STR} not found in intraday response.",
                "total_candles_checked": len(candles),
                "unique_dates_in_response": unique_dates_found_str if unique_dates_found_str else "None (or no valid timestamps)",
                "first_candle_time_if_any": candles[0][0] if candles else "N/A",
                "url_attempted": api_url
            }

    except requests.exceptions.HTTPError as e:
        error_details_text = e.response.text
        try:
            error_details_json = e.response.json()
            error_details = error_details_json
        except json.JSONDecodeError:
            error_details = error_details_text
        
        # The token is for 2025-05-17. If this script runs on a different actual day, a 401 is expected.
        if e.response.status_code == 401:
             if "expired" in error_details_text.lower() or "unauthorized" in error_details_text.lower() or "token" in error_details_text.lower():
                 return {"error": f"API request failed: Token (valid for 2025-05-17) is likely expired or invalid for the server's actual current date (401).", "details": error_details, "url_attempted": api_url}
        
        if isinstance(error_details, dict) and "errors" in error_details:
            for err_item in error_details["errors"]:
                if err_item.get("errorCode") == "UDAPI1146": # Invalid Unit
                     return {"error": f"API request failed: 'Invalid unit' error for unit '{err_item.get('invalidValue', 'UNKNOWN')}' using intraday URL structure.", "details": error_details, "url_attempted": api_url}
        
        return {"error": f"API request failed with HTTPError status {e.response.status_code}.", "details": error_details, "url_attempted": api_url}
    # ... (rest of specific exception handling remains same) ...
    except requests.exceptions.Timeout:
        return {"error": "API request timed out.", "url_attempted": api_url}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request exception: {e}", "url_attempted": api_url}
    except json.JSONDecodeError:
        return {"error": "Failed to decode JSON response from API.", "response_text": response.text if 'response' in locals() else "No response object", "url_attempted": api_url}
    except IndexError: 
        return {"error": "Candle data format error (IndexError).", "url_attempted": api_url}
    except Exception as e: 
        return {"error": f"An unexpected error occurred: {e}", "url_attempted": api_url}

if __name__ == "__main__":
    result = fetch_intraday_futures_920_data() 
    print(json.dumps(result))
