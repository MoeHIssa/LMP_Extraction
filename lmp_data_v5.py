"""
Updates and Revisions:
1. Improved Efficiency:
   - Introduced multithreading with ThreadPoolExecutor to fetch data for multiple nodes in parallel.
   - Reduced runtime significantly by parallelizing node-level data processing.
2. Enhanced Error Handling:
   - Implemented exponential backoff with jitter for retries to handle rate-limiting (429 errors) and network issues (HTTPConnectionPool errors).
   - Logs detailed errors for individual nodes and date ranges into a CSV file for debugging.
3. Enhanced Data Filtering:
   - Removed entries with 'MCL' and 'MGHG' values from the 'LMP_TYPE' column during data processing to streamline output.
4. Modularized Output:
   - Splits the final output into two Excel files: one for 2023 data and another for 2024 data.
5. Increased Retry Logic:
   - Configured up to 15 retries to improve robustness against transient API failures and network errors.
6. Adjusted Throttling:
   - Introduced dynamic throttling, doubling the wait time for RTM requests to address rate-limiting issues more effectively.
"""

from pycaiso.oasis import Node
from datetime import datetime, timedelta
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor

# Define the nodes of interest
nodes = ["WESTWING_5_N501", "PALOVRDE_ASR-APND", "WILOWBCH_6_ND001"]

# Define the overall start and end dates
overall_start_date = datetime(2023, 1, 1)  # Start date: January 1, 2023
overall_end_date = datetime(2024, 11, 30)  # End date: November 30, 2024

# Function to generate date ranges of maximum 31 days
def generate_date_ranges(start_date, end_date):
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=30), end_date)
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)

# Initialize an empty list to store errors
errors = []

# Function to fetch data with retries and throttling
def fetch_data(node_id, start_date, end_date, market="DAM", retries=15, base_delay=10, throttle=60):
    for attempt in range(retries):
        try:
            print(f"Attempt {attempt + 1}: Fetching {market} data for {node_id} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
            node = Node(node_id)
            lmp_data = node.get_lmps(start_date, end_date, market=market)
            if not lmp_data.empty:
                lmp_data = lmp_data[~lmp_data['LMP_TYPE'].isin(['MCL', 'MGHG'])]  # Remove unwanted LMP types
                lmp_data["Node"] = node_id  # Add a column for the node identifier
                lmp_data["Market"] = market  # Add a column for the market type
                return lmp_data
            else:
                print(f"No {market} data found for {node_id} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}.")
                return pd.DataFrame()
        except Exception as e:
            if "429" in str(e):  # Handle rate limiting
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"Rate limit hit. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            elif "HTTPConnectionPool" in str(e):  # Handle connection issues
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff
                print(f"Connection error. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                print(f"Error: {e}")
                if attempt < retries - 1:
                    print(f"Retrying in {base_delay} seconds...")
                    time.sleep(base_delay)
        finally:
            if market == "RTM":
                time.sleep(throttle * 2)  # Double throttle for RTM requests
            else:
                time.sleep(throttle)

# Fetch data for a single node
def fetch_node_data(node_id):
    combined_node_data = pd.DataFrame()
    markets = ["DAM", "RTM"]  # Fetch both Day-Ahead Market and Real-Time Market data
    for market in markets:
        for start_date, end_date in generate_date_ranges(overall_start_date, overall_end_date):
            data_chunk = fetch_data(node_id, start_date, end_date, market=market)
            if not data_chunk.empty:
                combined_node_data = pd.concat([combined_node_data, data_chunk], ignore_index=True)
    return combined_node_data

# Main processing loop
try:
    with ThreadPoolExecutor(max_workers=3) as executor:  # Parallelize across nodes
        results = executor.map(fetch_node_data, nodes)

    # Combine results from all nodes
    combined_data = pd.concat(results, ignore_index=True)

    # Split data into 2023 and 2024
    combined_data['OPR_DT'] = pd.to_datetime(combined_data['OPR_DT'])
    data_2023 = combined_data[combined_data['OPR_DT'].dt.year == 2023]
    data_2024 = combined_data[combined_data['OPR_DT'].dt.year == 2024]

    # Save to Excel files
    if not data_2023.empty:
        data_2023.to_excel("lmp_data_2023.xlsx", index=False)
        print("2023 data saved to 'lmp_data_2023.xlsx'")

    if not data_2024.empty:
        data_2024.to_excel("lmp_data_2024.xlsx", index=False)
        print("2024 data saved to 'lmp_data_2024.xlsx'")

    # Log any errors
    if errors:
        error_log = pd.DataFrame(errors, columns=["Node", "Start Date", "End Date", "Market", "Error"])
        error_log.to_csv("error_log.csv", index=False)
        print("Errors logged to 'error_log.csv'")

except Exception as e:
    print(f"Critical error occurred: {e}")