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
sheets_to_update = {
    "Large Cap" : sheet.worksheet("Large Cap"),
    "Mid Cap"   : sheet.worksheet("Mid Cap"),
    "Technology": sheet.worksheet("Technology"),
    "SP Tracker": sheet.worksheet("SP Tracker"),
    "Top Picks" : sheet.worksheet("Top Picks"),
}

for sheet_name, ws in sheets_to_update.items():
    print(f"\n🔁 Processing Sheet: {sheet_name}")
    data = ws.get_all_values()
    if not data or len(data) < 2:
        print(f"⚠️ Sheet {sheet_name} has no data.")
        continue

    headers = data[0]
    existing_data = data


# ✅ Define function to fetch existing data
def fetch_existing_data(ws):
    return ws.get_all_values()
    
existing_data = fetch_existing_data()

# ✅ Define new headers including earnings data
new_headers = ["Rank", "Symbol", "Earnings Date", "EPS", "Revenue Growth", "Debt-to-Equity", "Earnings Surprise"] + existing_data[0][2:]

# ✅ Reorganize data by shifting columns to the right
updated_data = [new_headers] + [[row[0], row[1], "", "", "", "", ""] + row[2:] for row in existing_data[1:]]

# ✅ Update Google Sheets with the existing data structure
top_picks_ws.clear()
top_picks_ws.update("A1", updated_data)
def get_earnings_data(ticker, max_retries=3):
    """Fetch accurate earnings data from Yahoo Finance, ensuring valid values."""
    retries = 0
    while retries < max_retries:
        try:
            stock = yf.Ticker(ticker)
            stock_info = stock.info  # Fetch full stock info
            print(json.dumps(stock_info, indent=4))  # Pretty print the full response
            # ✅ Fix Earnings Date Selection
            if "earningsTimestamp" in stock_info:
                earnings_date = datetime.utcfromtimestamp(stock_info["earningsTimestamp"]).strftime("%Y-%m-%d")
            elif "earningsTimestampStart" in stock_info:
                earnings_date = datetime.utcfromtimestamp(stock_info["earningsTimestampStart"]).strftime("%Y-%m-%d")
            else:
                earnings_date = "N/A"

            # ✅ Fix EPS Selection (Ensure accuracy)
            eps_actual = stock_info.get("trailingEps", "N/A")  # Reported EPS
            eps_estimate = stock_info.get("epsCurrentYear", stock_info.get("epsForward", "N/A"))  # Expected EPS (use best available)
            
            # ✅ Fix Revenue Growth (Ensure accuracy)
            revenue_growth = stock_info.get("revenueGrowth", "N/A")
            if revenue_growth != "N/A":
                revenue_growth = round(revenue_growth, 3)  # Keep 3 decimal places

            # ✅ Fix Debt-to-Equity (Add Fallback)
            debt_to_equity = stock_info.get("debtToEquity", "N/A")
            if debt_to_equity == "N/A":
                debt_to_equity = stock_info.get("totalDebt", "N/A")  # Use total debt if ratio is missing

            # ✅ Fix Earnings Surprise Calculation
            earnings_surprise = "N/A"
            if eps_actual != "N/A" and eps_estimate != "N/A" and eps_estimate > 0:
                earnings_surprise = round(((eps_actual - eps_estimate) / eps_estimate) * 100, 2)
            else:
                earnings_surprise = stock_info.get("earningsQuarterlyGrowth", "N/A")  # Fallback

            print(f"📊 {ticker} Earnings Data: EPS={eps_actual}, Growth={revenue_growth}, Debt={debt_to_equity}, Surprise={earnings_surprise}")
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
Reprocess after header update
    for i, row in enumerate(data[1:], start=2):
        row_dict = {headers[j]: row[j] if j < len(row) else "N/A" for j in range(len(headers))}
        ticker = row_dict.get('Symbol', 'N/A')

        if not ticker or ticker == "N/A":
            continue

        earnings_date, eps, revenue_growth, debt_to_equity, earnings_surprise = get_earnings_data(ticker)

        # ✅ Prepare updates batch
        updates = [
            {"range": f"C{i}", "values": [[earnings_date]]},        # Earnings Date
            {"range": f"D{i}", "values": [[eps]]},                 # EPS
            {"range": f"E{i}", "values": [[revenue_growth]]},     # Revenue Growth
            {"range": f"F{i}", "values": [[debt_to_equity]]},     # Debt-to-Equity
            {"range": f"G{i}", "values": [[earnings_surprise]]}   # Earnings Surprise
        ]

        retry_attempts = 0
        while retry_attempts < 5:
            try:
                ws.batch_update(updates)
                print(f"✅ Updated Earnings Data for {ticker} in {sheet_name} row {i}")
                time.sleep(1)
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    retry_attempts += 1
                    print(f"⚠️ Rate limit! Retrying in 10 sec (Attempt {retry_attempts})...")
                    time.sleep(10)
                    switch_api_key()
                    sheet = client.open("Stock Investment Analysis")
                    ws = sheet.worksheet(sheet_name)
                else:
                    print(f"❌ Error updating Google Sheets for {ticker} in {sheet_name}: {e}")
                    break

print("\n✅ Earnings Data Successfully Updated in All Sheets!")

