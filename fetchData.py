import os  # Required for environment variables
import json  # Required for JSON parsing
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import pandas as pd
import numpy as np
from datetime import datetime  
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
    "SP Tracker":sheet.worksheet("SP Tracker"),
    "Large Cap": sheet.worksheet("Large Cap"),
    "Mid Cap": sheet.worksheet("Mid Cap"),
    "Technology":sheet.worksheet("Technology")
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

# 🔹 Function to calculate RSI
def calculate_rsi(prices, period=14):
    try:
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return safe_convert(rsi.iloc[-1]) if not rsi.isna().iloc[-1] else "N/A"
    except Exception as e:
        print(f"❌ Error calculating RSI: {e}")
        return "N/A"

# 🔹 Function to calculate VWMA
def calculate_vwma(prices, volumes, period=20):
    try:
        if len(prices) < period:
            return "N/A"
        vwma = (prices * volumes).rolling(window=period).sum() / volumes.rolling(window=period).sum()
        return safe_convert(vwma.iloc[-1])
    except Exception as e:
        print(f"❌ Error calculating VWMA: {e}")
        return "N/A"

# 🔹 Function to fetch stock data (Handles YFinance Rate Limits)
def get_stock_data(ticker, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            print(ticker)
            stock = yf.Ticker(ticker)
            hist = stock.history(period="3mo")  # ✅ Fetch 3 months of data
            #print(hist)

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
            # Check if current_price is zero or N/A and assign yesterday_close_price
            if current_price == 0 or current_price == "N/A":
                current_price = yesterday_close_price
                print(f"🔄 Current price for {ticker} set to yesterday's close: {current_price}")

            # 1-Day Price Change
            percent_change_1d = round(((current_price - yesterday_close_price) / yesterday_close_price) * 100, 2) if yesterday_close_price != "N/A" else "N/A"

            # 1-Week and 1-Month Price Changes
            one_week_ago_price = safe_convert(prices.iloc[-6]) if len(prices) > 6 else "N/A"
            one_month_ago_price = safe_convert(prices.iloc[0])
            percent_change_1wk = round(((current_price - one_week_ago_price) / one_week_ago_price) * 100, 2) if one_week_ago_price != "N/A" else "N/A"
            percent_change_1mo = round(((current_price - one_month_ago_price) / one_month_ago_price) * 100, 2)

            # Volume
            volume = safe_convert(volumes.iloc[-1])

            # RSI (14-day)
            rsi = calculate_rsi(prices, period=14)

            # VWMA (20-day)
            vwma = calculate_vwma(prices, volumes, period=20)

            # EMA (10-day)
            ema = safe_convert(prices.ewm(span=10, adjust=False).mean().iloc[-1])

            # ATR (14-day)
            atr = safe_convert((hist["High"] - hist["Low"]).rolling(14).mean().iloc[-1])
# NEW ► Relative ATR  (risk normalised)
            rel_atr = "N/A"
            if current_price not in ("N/A", 0):
                rel_atr = safe_convert(round(atr / current_price, 4))            
            # --- NEW momentum‑centric metrics ---
            # Relative Volume (today vs 20‑day avg excluding today)
            rvol = "N/A"
            if len(volumes) > 21:
                avg20 = volumes.iloc[-21:-1].mean()
                rvol = safe_convert(round(volume/avg20, 2)) if avg20 else "N/A"

            dollar_vol = safe_convert(round(current_price * volume, 0)) if current_price!="N/A" else "N/A"

            float_shares        = safe_convert(stock.info.get("floatShares", "N/A"))
            short_percent_float = safe_convert(stock.info.get("shortPercentOfFloat", stock.info.get("shortPercentFloat", "N/A")))
            days_to_cover       = safe_convert(stock.info.get("shortRatio", "N/A"))

            # Gap % (today open vs yesterday close)
            gap_pct = "N/A"
            if len(hist)>1:
                open_today = hist["Open"].iloc[-1]
                gap_pct = format_percentage(((open_today - yesterday_close_price)/yesterday_close_price)*100) if yesterday_close_price!="N/A" else "N/A"

            dist_to_vwap = safe_convert(round(current_price - vwma, 2)) if current_price!="N/A" and vwma!="N/A" else "N/A"
            
            print(market_cap, pe_ratio, current_price, yesterday_close_price,
                format_percentage(percent_change_1d), format_percentage(percent_change_1wk), format_percentage(percent_change_1mo),
                volume, rsi, vwma, ema, atr,rvol, dollar_vol, float_shares, short_percent_float, days_to_cover,
                gap_pct, dist_to_vwap,rel_atr)

            return [
                market_cap, pe_ratio, current_price, yesterday_close_price,
                format_percentage(percent_change_1d), format_percentage(percent_change_1wk), format_percentage(percent_change_1mo),
                volume, rsi, vwma, ema, atr,rvol, dollar_vol, float_shares, short_percent_float, days_to_cover,
                gap_pct, dist_to_vwap,rel_atr
            ]

        except Exception as e:
            error_msg = str(e)
            if "Too Many Requests" in error_msg:
                print(f"⚠️ YFinance Rate Limit hit for {ticker}. Pausing for 60 seconds...")
                time.sleep(20)  # ✅ Pause for 60 seconds before retrying
                retries += 1
            else:
                print(f"❌ Error fetching data for {ticker}: {e}")
                return None

    print(f"❌ Skipping {ticker} after {max_retries} failed attempts due to YFinance rate limits.")
    return None  # Skip stock if all retries fail
# 🔹 Process each ticker row-by-row
api_call_count = 0  # Track number of API calls

for sheet_name, worksheet in sheets_to_update.items():
    tickers = fetch_tickers(worksheet)

    for idx, ticker in enumerate(tickers, start=2):  # Start from row 2
        while True:
            try:
                stock_data = get_stock_data(ticker)
                if stock_data is None:
                    print(f"⚠️ Skipping update for {ticker}: No data available.")
                    break  
                fetch_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # Convert to valid types for Google Sheets
                stock_data = [safe_convert(val) for val in stock_data]

                # ✅ Prepare batch update payload
                updates = [
                    {"range": f"I{idx}:AB{idx}", "values": [stock_data]},  # Stock data (C:N)
                    {"range": f"AT{idx}", "values": [[fetch_datetime]]}    # Fetch timestamp in AF
                ]

                # ✅ Perform batch update
                worksheet.batch_update(updates)
                print(f"✅ Updated {sheet_name} - {ticker} in row {idx}")

                # ✅ Increment API call count
                api_call_count += 1

                # ✅ Switch API keys every 20 calls
                if api_call_count % 15 == 0:
                    print(f"🔄 Switching API key after 20 calls...  {api_call_count}")
                    switch_api_key()

                break  

            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    print(f"⚠️ Rate limit hit! Pausing for 60 seconds...")
                    time.sleep(10)  
                    switch_api_key()
                    worksheet = client.open("Stock Investment Analysis").worksheet(sheet_name)
                else:
                    print(f"❌ Error updating {sheet_name} - {ticker}: {e}")
                    break  

print("✅ Google Sheets 'Large Cap' & 'Mid Cap' updated!")
