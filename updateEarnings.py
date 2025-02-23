import os  # Required for environment variables
import json  # Required for JSON parsing
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import pandas as pd
import numpy as np
from datetime import datetime  
import re  # ✅ Ensure `re` is imported for regex parsing
from gspread_formatting import format_cell_range, CellFormat, Color

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials for API key rotation from environment variables
CREDS_JSON_1 = json.loads(os.getenv("GOOGLE_CREDENTIALS_1"))
CREDS_JSON_2 = json.loads(os.getenv("GOOGLE_CREDENTIALS_2"))

# ✅ Function to authenticate with Google Sheets
def authenticate_with_json(creds_json):
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPE)
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

# ✅ Open the spreadsheet and access the "Top Picks" sheet
sheet = client.open("Stock Investment Analysis")
top_picks_ws = sheet.worksheet("Top Picks")

# ✅ Fetch all data from "Top Picks"
data = top_picks_ws.get_all_values()
headers = data[0]  # Extract column headers

# ✅ Define function to fetch existing data
def fetch_existing_data():
    return top_picks_ws.get_all_values()

existing_data = fetch_existing_data()

# ✅ Define new headers including earnings data
new_headers = ["Rank", "Symbol", "Earnings Date", "EPS", "Revenue Growth", "Debt-to-Equity", "Earnings Surprise"] + existing_data[0][2:]

# ✅ Reorganize data by shifting columns to the right
updated_data = [new_headers] + [[row[0], row[1], "", "", "", "", ""] + row[2:] for row in existing_data[1:]]

# ✅ Update Google Sheets with the existing data structure
top_picks_ws.clear()
top_picks_ws.update("A1", updated_data)
def get_earnings_data(ticker, max_retries=3):
    """Fetch earnings data from Yahoo Finance, ensuring accurate values."""
    retries = 0
    while retries < max_retries:
        try:
            stock = yf.Ticker(ticker)
            stock_info = stock.info  # Fetch full stock info

            # ✅ Fetch Earnings Date safely
            earnings_date = "N/A"
            if stock_info.get("earningsTimestampStart"):
                earnings_date = datetime.utcfromtimestamp(stock_info["earningsTimestampStart"]).strftime("%Y-%m-%d")

            # ✅ Fetch Key Financial Data
            eps_actual = stock_info.get("trailingEps", "N/A")  # Reported EPS
            eps_estimate = stock_info.get("epsForward", "N/A")  # Expected EPS
            revenue_growth = stock_info.get("revenueGrowth", "N/A")  # Revenue Growth %
            debt_to_equity = stock_info.get("debtToEquity", "N/A")  # Debt-to-Equity Ratio
            print(f"eps_actual : {eps_actual},eps_estimate : {eps_estimate} , revenue_growth : {revenue_growth},debt_to_equity : {debt_to_equity} ")
            # ✅ Ensure numerical values are valid
            eps_actual = float(eps_actual) if isinstance(eps_actual, (int, float)) else "N/A"
            eps_estimate = float(eps_estimate) if isinstance(eps_estimate, (int, float)) else "N/A"
            revenue_growth = float(revenue_growth) if isinstance(revenue_growth, (int, float)) else "N/A"
            debt_to_equity = float(debt_to_equity) if isinstance(debt_to_equity, (int, float)) else "N/A"
            print(f"eps_actual : {eps_actual},eps_estimate : {eps_estimate} , revenue_growth : {revenue_growth},debt_to_equity : {debt_to_equity} ")
            # ✅ Calculate Earnings Surprise
            earnings_surprise = "N/A"
            if eps_actual != "N/A" and eps_estimate != "N/A" and eps_estimate > 0:
                earnings_surprise = round(((eps_actual - eps_estimate) / eps_estimate) * 100, 2)
                print(f"earnings_surprise : {earnings_surprise}")
            else:
                # ⚠️ If `epsForward` is missing, use `earningsQuarterlyGrowth` instead.
                earnings_surprise = stock_info.get("earningsQuarterlyGrowth", "N/A")
                earnings_surprise = round(float(earnings_surprise) * 100, 2) if isinstance(earnings_surprise, (int, float)) else "N/A"

            return earnings_date, eps_actual, revenue_growth, debt_to_equity, earnings_surprise

        except Exception as e:
            error_msg = str(e)
            if "Too Many Requests" in error_msg:
                print(f"⚠️ YFinance Rate Limit hit for {ticker}. Pausing for 60 seconds...")
                time.sleep(60)  # ✅ Pause for 60 seconds before retrying
                retries += 1
            else:
                print(f"❌ Error fetching earnings data for {ticker}: {e}")
                return "N/A", "N/A", "N/A", "N/A", "N/A"

    print(f"❌ Skipping {ticker} after {max_retries} failed attempts due to YFinance rate limits.")
    return "N/A", "N/A", "N/A", "N/A", "N/A"



# ✅ Process each row and update Google Sheets with earnings data
for i, row in enumerate(data[1:], start=2):  # Skip headers
    row_dict = {headers[j]: row[j] if j < len(row) else "N/A" for j in range(len(headers))}
    ticker = row_dict.get('Symbol', 'N/A')

    # ✅ Fetch Earnings Data
    earnings_date, eps, revenue_growth, debt_to_equity, earnings_surprise = get_earnings_data(ticker)

    # ✅ Prepare updates batch
    updates = [
        {"range": f"C{i}", "values": [[earnings_date]]},  # Earnings Date
        {"range": f"D{i}", "values": [[eps]]},  # EPS
        {"range": f"E{i}", "values": [[revenue_growth]]},  # Revenue Growth
        {"range": f"F{i}", "values": [[debt_to_equity]]},  # Debt-to-Equity Ratio
        {"range": f"G{i}", "values": [[earnings_surprise]]}  # Earnings Surprise
    ]

    retry_attempts = 0
    while retry_attempts < 5:
        try:
            top_picks_ws.batch_update(updates)
            print(f"✅ Updated Earnings Data for {ticker} in row {i}")
            time.sleep(1)  # Prevent hitting rate limits
            break  # ✅ Exit retry loop if successful
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                retry_attempts += 1
                print(f"⚠️ Rate limit hit! Retrying in 10 seconds (Attempt {retry_attempts})...")
                time.sleep(10)  # ✅ Wait for 10 seconds before retrying
                switch_api_key()  # ✅ Switch API key if needed
                sheet = client.open("Stock Investment Analysis")  # Reconnect
                top_picks_ws = sheet.worksheet("Top Picks")  # Rebind worksheet
            else:
                print(f"❌ Error updating Google Sheets for {ticker}: {e}")
                break  # Exit loop for non-429 errors

print("✅ Earnings Data Successfully Updated in 'Top Picks' Sheet!")
