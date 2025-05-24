import requests
import gzip
import json
import io

def download_and_parse_nse_data():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    
    try:
        # Step 1: Download the gzipped JSON file
        print(f"Downloading data from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        print("Download successful.")
        
        # Step 2: Decompress the gzipped content
        print("Decompressing data...")
        # Ensure we are reading the raw content for gzip decompression
        gzipped_content = response.raw.read()
        
        # Decompress directly from bytes
        decompressed_content = gzip.decompress(gzipped_content)
        print("Decompression successful.")
        
        # Step 3: Parse the JSON data
        print("Parsing JSON data...")
        # The decompressed content is bytes, decode to string for json.loads
        json_data_str = decompressed_content.decode('utf-8')
        parsed_data = json.loads(json_data_str)
        print(f"JSON parsing successful. Number of records: {len(parsed_data)}")
        
        # Step 4: Return the parsed JSON data
        # For now, let's print a small part of it to verify
        if isinstance(parsed_data, list) and len(parsed_data) > 0:
            print("Sample of parsed data (first 2 records):")
            for i in range(min(2, len(parsed_data))):
                print(parsed_data[i])
        elif isinstance(parsed_data, dict):
            print("Parsed data is a dictionary. Sample of keys:", list(parsed_data.keys())[:5])

        return parsed_data
        
    except requests.exceptions.RequestException as e:
        error_message = f"Error during download: {e}"
        print(error_message)
        return {"error": error_message}
    except gzip.BadGzipFile as e:
        error_message = f"Error during decompression (BadGzipFile): {e}. Downloaded content might not be a valid gzip file."
        print(error_message)
        # print("First 500 bytes of downloaded content:", gzipped_content[:500]) # For debugging
        return {"error": error_message}
    except EOFError as e:
        error_message = f"Error during decompression (EOFError): {e}. This can happen if the gzip stream is incomplete."
        print(error_message)
        return {"error": error_message}
    except json.JSONDecodeError as e:
        error_message = f"Error during JSON parsing: {e}"
        print(error_message)
        return {"error": error_message}
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return {"error": error_message}

if __name__ == "__main__":
    data = download_and_parse_nse_data()
    # The subtask implies returning the data, not just printing it.
    # However, for the script execution via run_in_bash_session,
    # we'll rely on the print statements for verification for now.
    # The actual data will be handled by the agent based on this script's success.
    if "error" not in data:
        print("Script finished successfully.")
    else:
        print(f"Script finished with error: {data['error']}")
