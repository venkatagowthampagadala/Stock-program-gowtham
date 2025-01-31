import os  # Required for environment variables
import json  # Required for JSON parsing
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import openai
import time
import yfinance as yf
import numpy as np

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from GitHub Secrets
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# 🔹 OpenAI API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY  # ✅ Set OpenAI Key

# Ensure json module is imported
def authenticate_with_json(json_str):
    creds_dict = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authenticate_with_json(CREDS_JSON_1)
active_api = 1  # Track which API key is being used

# Open the spreadsheet and access the 'Top Picks' sheet
sheet = client.open("Stock Investment Analysis")
top_picks_ws = sheet.worksheet("Top Picks")

print("✅ Successfully authenticated with Google Sheets and OpenAI!")

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_JSON_2 if active_api == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to API Key {active_api}")

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

# 🔹 Fetch stock tickers from Google Sheets
tickers = top_picks_ws.col_values(2)[1:]  # Read tickers from Column B (Symbol), skipping header
print(f"✅ Found {len(tickers)} tickers to analyze.")

# 🔹 Function to fetch stock data & OpenAI Analysis
def analyze_stock(ticker):
    print(f"🔍 Analyzing {ticker}...")

    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="6mo")  # ✅ Fetch 6 months of data

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

        # RSI (14-day)
        rsi = calculate_rsi(prices, period=14)

        # VWMA (20-day)
        vwma = safe_convert((prices * volumes).rolling(window=20).sum() / volumes.rolling(window=20).sum().iloc[-1])

        # ATR (14-day)
        atr = safe_convert((hist["High"] - hist["Low"]).rolling(14).mean().iloc[-1])

        # OpenAI Analysis Prompt
        prompt = f"""
        Analyze the stock {ticker} based on:
        - Market Cap: {market_cap}
        - P/E Ratio: {pe_ratio}
        - Current Price: {current_price}
        - RSI (14-day): {rsi}
        - VWMA (20-day): {vwma}
        - ATR (14-day): {atr}
        
        Provide a risk assessment and recommendation (Buy, Hold, Sell) based on:
        - Market Trends
        - Volatility
        - Technical Indicators
        - Recent Performance
        """

        # ✅ FIX: Updated for OpenAI API v1+
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a professional stock analyst."},
                {"role": "user", "content": prompt}
            ]
        )

        ai_analysis = response.choices[0].message["content"]  # ✅ Extract correct response
        print(f"✅ AI Analysis for {ticker}: {ai_analysis[:100]}...")  # Print first 100 chars for preview

        return [
            ticker, market_cap, current_price, pe_ratio, rsi, vwma, atr, ai_analysis
        ]

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        return None

# 🔹 Process stocks one by one and update Google Sheet
updates = []
for idx, ticker in enumerate(tickers, start=2):  # Start from row 2
    while True:
        try:
            stock_data = analyze_stock(ticker)
            if stock_data is None:
                print(f"⚠️ Skipping {ticker}: No data available.")
                break  # Skip this row

            updates.append(stock_data)

            time.sleep(5)  # ✅ Prevent hitting API limits
            break  # Successfully analyzed, break retry loop

        except gspread.exceptions.APIError as e:
            if "429" in str(e):  # Detect API Rate Limit error
                print(f"⚠️ Rate limit hit! Switching API keys...")
                switch_api_key()
                sheet = client.open("Stock Investment Analysis")
                top_picks_ws = sheet.worksheet("Top Picks")  # Re-authenticate the sheet
            else:
                print(f"❌ Error updating {ticker}: {e}")
                break  # Move to next stock

# 🔹 Update Google Sheets with AI Analysis
if updates:
    top_picks_ws.update("A2", updates)
    print(f"✅ Updated 'Top Picks' sheet with AI analysis for {len(updates)} stocks.")

print("🎯 Analysis Completed!")
