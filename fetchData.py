import os  # Required for environment variables
import json  # Required for JSON parsing
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import pandas as pd
import numpy as np


# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from GitHub Secrets
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# 🔹 Function to authenticate with Google Sheets using JSON from environment variables
def authenticate_with_json(json_str):
    creds_dict = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authenticate_with_json(CREDS_JSON_1)
active_api = 1  # Track which API key is being used

# Open the main spreadsheet and access both Large Cap & Mid Cap sheets
sheet = client.open("Stock Investment Analysis")
sheets_to_update = {
    "Large Cap": sheet.worksheet("Large Cap"),
    "Mid Cap": sheet.worksheet("Mid Cap")
}

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_JSON_2 if active_api == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to API Key {active_api}")

# 🔹 Function to fetch tickers from a Google Sheet
def fetch_tickers(worksheet):
    try:
        tickers = worksheet.col_values(1)[1:]  # Reads tickers from Column A, skipping header
        print(f"✅ {worksheet.title}: {len(tickers)} tickers fetched")
        return tickers
    except Exception as e:
        print(f"❌ Error fetching tickers from {worksheet.title}: {e}")
        return []

# 🔹 Function to safely convert values
def safe_convert(value):
    """Convert values to JSON-compliant types and handle invalid floats."""
    if isinstance(value, (pd.Series, pd.DataFrame)):
        return value.iloc[0] if not value.empty else "N/A"
    if isinstance(value, (np.int64, np.float64)):
        value = value.item()
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):  # Handle NaN, Infinity, and -Infinity
            return "N/A"
    return value

# 🔹 Function to format percentage values
def format_percentage(value):
    return f"{round(value, 2)}%" if value != "N/A" else "N/A"

# 🔹 Function to fetch stock data
def get_stock_data(ticker):
    max_retries = 3  # Retry up to 3 times if rate limited
    retries = 0

    while retries < max_retries:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="3mo")  # ✅ Fetch 3 months of data

            if hist.empty:
                print(f"⚠️ No historical data for {ticker}")
                return None

            # Extract Close prices and Volumes
            prices = hist["Close"]
            volumes = hist["Volume"]

            # Market Cap and P/E Ratio
            market_cap = safe_convert(stock.info.get("marketCap", "N/A"))
            pe_ratio = safe_convert(stock.info.get("trailingPE", "N/A"))

            # Current Price (latest available close price)
            current_price = safe_convert(prices.iloc[-1])

            # Yesterday's Close Price
            yesterday_close_price = safe_convert(prices.iloc[-2]) if len(prices) > 1 else "N/A"

            # 1-Day Price Change
            percent_change_1d = round(((current_price - yesterday_close_price) / yesterday_close_price) * 100, 2) if yesterday_close_price != "N/A" else "N/A"

            # 1-Week and 1-Month Price Changes
            one_week_ago_price = safe_convert(prices.iloc[-6]) if len(prices) > 6 else "N/A"
            one_month_ago_price = safe_convert(prices.iloc[0])
            percent_change_1wk = round(((current_price - one_week_ago_price) / one_week_ago_price) * 100, 2) if one_week_ago_price != "N/A" else "N/A"
            percent_change_1mo = round(((current_price - one_month_ago_price) / one_month_ago_price) * 100, 2)

            # Volume
            volume = safe_convert(volumes.iloc[-1])

            return [
                market_cap, pe_ratio, current_price, yesterday_close_price,
                format_percentage(percent_change_1d), format_percentage(percent_change_1wk), format_percentage(percent_change_1mo),
                volume
            ]

        except Exception as e:
            if "Too Many Requests" in str(e) or "rate limited" in str(e):
                print(f"⏳ Rate limit hit! Waiting 60 seconds before retrying {ticker}...")
                time.sleep(60)  # ✅ Wait before retrying
                retries += 1  # Increment retry counter
            else:
                print(f"❌ Error fetching data for {ticker}: {e}")
                return None  # Skip stock if it's a different error

    print(f"⚠️ Skipping {ticker} after {max_retries} failed attempts due to rate limit.")
    switch_api_key()  # Switch API key if retries fail
    return None

# 🔹 Process tickers **in batches of 10** to reduce API calls
for sheet_name, worksheet in sheets_to_update.items():
    tickers = fetch_tickers(worksheet)
    batch_size = 10  # ✅ Process 10 stocks at a time
    batch_data = []
    row_numbers = []

    for idx, ticker in enumerate(tickers, start=2):  # Start from row 2
        stock_data = get_stock_data(ticker)
        if stock_data:
            batch_data.append(stock_data)
            row_numbers.append(idx)

        # 🔹 **Batch update every 10 rows or at the end**
        if len(batch_data) == batch_size or idx == len(tickers):
            while True:  # ✅ Retry on rate limit error
                try:
                    worksheet.update(f"C{row_numbers[0]}:J{row_numbers[-1]}", batch_data)
                    print(f"✅ Batch updated {sheet_name} - Rows {row_numbers[0]} to {row_numbers[-1]}")
                    batch_data.clear()
                    row_numbers.clear()
                    time.sleep(1)  # ✅ Add small delay to prevent quota errors
                    break  # Exit retry loop
                except gspread.exceptions.APIError as e:
                    if "429" in str(e):  # Detect API Rate Limit error
                        print(f"⚠️ Rate limit hit! Waiting 60 seconds before retrying...")
                        time.sleep(60)  # Wait 60 seconds
                        switch_api_key()
                    else:
                        print(f"❌ Error updating {sheet_name}: {e}")
                        break  # Move to next batch

print("✅ Google Sheets 'Large Cap' & 'Mid Cap' updated with technical analysis!")
