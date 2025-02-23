import os
import json
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from gspread_formatting import format_cell_range, CellFormat, Color

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials for API key rotation
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# ✅ Function to authenticate with Google Sheets
def authenticate_with_json(json_str):
    creds_dict = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

# ✅ Start with API Key 1
client = authenticate_with_json(CREDS_JSON_1)
active_api = 1  # Track which API key is being used

# ✅ Function to switch API keys
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_JSON_2 if active_api == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to Google Sheets API Key {active_api}")

# ✅ Open the spreadsheet and access "SP Trend" sheet
sheet = client.open("Stock Investment Analysis")
sp_trend_ws = sheet.worksheet("SP Trend")

# ✅ Function to fetch S&P 500 Data
def fetch_sp500_data():
    print("🔍 Fetching S&P 500 data...")
    sp500 = yf.Ticker("^GSPC")
    hist = sp500.history(period="1y")  # Fetch 1 year of data

    # ✅ Calculate Trend Metrics
    sp_1m_change = round(((hist['Close'].iloc[-1] - hist['Close'].iloc[-22]) / hist['Close'].iloc[-22]) * 100, 2)
    sp_3m_change = round(((hist['Close'].iloc[-1] - hist['Close'].iloc[-66]) / hist['Close'].iloc[-66]) * 100, 2)
    sp_52w_high = round(hist['Close'].max(), 2)
    sp_52w_low = round(hist['Close'].min(), 2)
    sp_rsi = calculate_rsi(hist['Close'])
    sp_atr = calculate_atr(hist)

    # ✅ Fetch VIX for Market Volatility
    vix = yf.Ticker("^VIX")
    vix_data = vix.history(period="1mo")
    sp_vix = round(vix_data['Close'].iloc[-1], 2) if not vix_data.empty else "N/A"

    return sp_1m_change, sp_3m_change, sp_52w_high, sp_52w_low, sp_rsi, sp_atr, sp_vix

# ✅ Function to calculate RSI
def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2) if not rsi.isna().iloc[-1] else "N/A"

# ✅ Function to calculate ATR
def calculate_atr(hist, period=14):
    high_low = hist['High'] - hist['Low']
    high_close = abs(hist['High'] - hist['Close'].shift())
    low_close = abs(hist['Low'] - hist['Close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return round(atr.iloc[-1], 2) if not atr.isna().iloc[-1] else "N/A"

# ✅ Process S&P 500 Data
sp_1m_change, sp_3m_change, sp_52w_high, sp_52w_low, sp_rsi, sp_atr, sp_vix = fetch_sp500_data()

# ✅ Update Google Sheets with S&P 500 Trend Data
updates = [
    {"range": "A1", "values": [["Metric", "Value"]]},
    {"range": "A2", "values": [["1-Month Change (%)", sp_1m_change]]},
    {"range": "A3", "values": [["3-Month Change (%)", sp_3m_change]]},
    {"range": "A4", "values": [["52-Week High", sp_52w_high]]},
    {"range": "A5", "values": [["52-Week Low", sp_52w_low]]},
    {"range": "A6", "values": [["RSI", sp_rsi]]},
    {"range": "A7", "values": [["ATR", sp_atr]]},
    {"range": "A8", "values": [["VIX", sp_vix]]}
]

sp_trend_ws.batch_update(updates)
print("✅ S&P 500 Trend Data Successfully Updated in 'SP Trend' Sheet!")
