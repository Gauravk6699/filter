import requests
import json
from datetime import datetime, date # Not strictly needed for fixed date but good practice
from urllib.parse import quote

# Configuration from prompt
API_BASE_URL = "https://api.upstox.com"
EQUITY_INSTRUMENT_KEY_RAW = "NSE_EQ|INE002A01018" # Reliance Equity
EQUITY_INSTRUMENT_KEY_ENCODED = quote(EQUITY_INSTRUMENT_KEY_RAW)

# Directly use the token provided in the subtask description
UPSTOX_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzVUM3U0wiLCJqdGkiOiI2ODJmZmFjMDA4ZjVkZTYxM2MyMzBiYTgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzQ3OTc0ODQ4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDgwMzc2MDB9.MSyl3ODIhbksqXm_Cuj6imKDOeCTys9pZIKYujaOrQE"

# Target date for "Previous Day"
TARGET_DATE_STR = "2025-05-16" # Friday, before token's active date of 2025-05-17

def fetch_equity_data_for_target_date():
    access_token = UPSTOX_ACCESS_TOKEN
    if not access_token:
        return {"error": "UPSTOX_ACCESS_TOKEN is missing internally."}

    # Constructing the API URL as specified:
    # [API_BASE_URL]/v3/historical-candle/{encoded_instrument_key}/days/1/{target_date}
    # Unit "days", interval "1", target_date TARGET_DATE_STR
    api_url = f"{API_BASE_URL}/v3/historical-candle/{EQUITY_INSTRUMENT_KEY_ENCODED}/days/1/{TARGET_DATE_STR}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    print(f"Fetching URL: {api_url}")
    print(f"Requesting data for fixed target date: {TARGET_DATE_STR}")

    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        
        response_json = response.json()
        
        candles = response_json.get("data", {}).get("candles")

        if candles and isinstance(candles, list) and len(candles) > 0:
            # Assuming the first candle in the list is the one for the requested day
            first_candle = candles[0]
            if isinstance(first_candle, list) and len(first_candle) >= 7: # Ensure candle has enough elements
                # Indices: 0:timestamp, 1:open, 2:high, 3:low, 4:close, 5:volume, 6:oi
                closing_price = first_candle[4]
                open_interest = first_candle[6] # OI for equity is usually 0 or not applicable
                
                # Verify timestamp of the candle if possible (optional but good)
                candle_timestamp_str = first_candle[0]
                print(f"Data found. Candle timestamp from response: {candle_timestamp_str}, Close: {closing_price}, OI: {open_interest}")

                return {"prev_day_equity_close": closing_price, "prev_day_equity_oi": open_interest}
            else:
                return {"error": "Candle data format is unexpected (inner list too short or not a list).", "response_details": response_json, "url_attempted": api_url}
        else:
            # This implies no data for that date or unexpected format
            return {"error": f"No candle data found in response for {TARGET_DATE_STR}.", "response_details": response_json, "url_attempted": api_url}

    except requests.exceptions.HTTPError as e:
        error_details_text = e.response.text
        try:
            error_details_json = e.response.json()
            error_details = error_details_json
        except json.JSONDecodeError:
            error_details = error_details_text
        
        # Specific check for token expiry (401)
        # Token iat: 2025-05-17. Requesting for 2025-05-16. This might be an issue if token is strictly for "today" based on iat.
        if e.response.status_code == 401:
             if "expired" in error_details_text.lower() or "unauthorized" in error_details_text.lower() or "token" in error_details_text.lower():
                 return {"error": f"API request failed: Token (issued {UPSTOX_ACCESS_TOKEN[85:95]}...) may be invalid for target date {TARGET_DATE_STR} (401).", "details": error_details, "url_attempted": api_url}
        
        # Check for "Invalid unit" error specifically, though "days/1" worked before for a different date
        if isinstance(error_details, dict) and "errors" in error_details:
            for err_item in error_details["errors"]:
                if err_item.get("errorCode") == "UDAPI1146": # Invalid Unit
                     return {"error": f"API request failed: 'Invalid unit' error for unit '{err_item.get('invalidValue', 'UNKNOWN')}' using URL {api_url}.", "details": error_details}

        return {"error": f"API request failed with HTTPError status {e.response.status_code}.", "details": error_details, "url_attempted": api_url}
    # ... (rest of standard exception handling: Timeout, RequestException, JSONDecodeError, IndexError, generic Exception) ...
    except requests.exceptions.Timeout:
        return {"error": "API request timed out.", "url_attempted": api_url}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request exception: {e}", "url_attempted": api_url}
    except json.JSONDecodeError:
        return {"error": "Failed to decode JSON response from API.", "response_text": response.text if 'response' in locals() else "No response object", "url_attempted": api_url}
    except IndexError: 
        return {"error": "Candle data format error (IndexError). Could not extract data.", "response_data": response_json if 'response_json' in locals() else "No JSON response", "url_attempted": api_url}
    except Exception as e: 
        return {"error": f"An unexpected error occurred: {e}", "url_attempted": api_url}

if __name__ == "__main__":
    result = fetch_equity_data_for_target_date()
    print(json.dumps(result))
