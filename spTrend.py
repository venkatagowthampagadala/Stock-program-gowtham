import gspread
import yfinance as yf
from oauth2client.service_account import ServiceAccountCredentials
import time
import pandas as pd
import numpy as np
from datetime import datetime

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from GitHub Secrets
CREDS_JSON = "your-google-credentials.json"

# ✅ Function to authenticate with Google Sheets
def authenticate_with_json(json_file):
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, SCOPE)
    return gspread.authorize(creds)

# ✅ Authenticate client
client = authenticate_with_json(CREDS_JSON)

# ✅ Open Google Sheet and Select "SP Trend" worksheet
sheet = client.open("Stock Investment Analysis")
sp_trend_ws = sheet.worksheet("SP Trend")

# ✅ Define Headers
HEADERS = ["Ticker", "Current Price", "1M Change (%)", "3M Change (%)", "RSI (14)", "ATR (14)", "VIX"]

# ✅ Update headers if missing
def update_headers():
    existing_headers = sp_trend_ws.row_values(1)
    if existing_headers != HEADERS:
        sp_trend_ws.update("A1:G1", [HEADERS])

update_headers()

# ✅ Fetch S&P 500 Market Data
def fetch_sp_trend():
    spy = yf.Ticker("SPY")
    vix = yf.Ticker("^VIX")
    hist = spy.history(period="6mo")
    vix_data = vix.history(period="1d")

    if hist.empty or vix_data.empty:
        print("⚠️ No market data available!")
        return None
    
    current_price = hist["Close"].iloc[-1]
    one_month_ago_price = hist["Close"].iloc[-22] if len(hist) > 22 else np.nan
    three_months_ago_price = hist["Close"].iloc[-66] if len(hist) > 66 else np.nan
    
    one_month_change = round(((current_price - one_month_ago_price) / one_month_ago_price) * 100, 2) if one_month_ago_price else "N/A"
    three_month_change = round(((current_price - three_months_ago_price) / three_months_ago_price) * 100, 2) if three_months_ago_price else "N/A"
    
    rsi = calculate_rsi(hist["Close"], period=14)
    atr = calculate_atr(hist)
    vix_value = round(vix_data["Close"].iloc[-1], 2) if not vix_data.empty else "N/A"
    
    return ["SPY", round(current_price, 2), one_month_change, three_month_change, rsi, atr, vix_value]

# ✅ RSI Calculation
def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2) if not rsi.isna().iloc[-1] else "N/A"

# ✅ ATR Calculation
def calculate_atr(hist, period=14):
    high_low = hist["High"] - hist["Low"]
    high_close = np.abs(hist["High"] - hist["Close"].shift())
    low_close = np.abs(hist["Low"] - hist["Close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return round(atr.iloc[-1], 2) if not atr.isna().iloc[-1] else "N/A"

# ✅ Update Google Sheet with S&P 500 Trend Data
def update_sp_trend():
    market_data = fetch_sp_trend()
    if market_data:
        sp_trend_ws.update("A2:G2", [market_data])
        print(f"✅ Updated SP Trend: {market_data}")
    else:
        print("⚠️ Failed to fetch S&P 500 market trend data.")

# ✅ Run Update
update_sp_trend()
print("✅ S&P 500 Market Trend Data Updated Successfully!")
