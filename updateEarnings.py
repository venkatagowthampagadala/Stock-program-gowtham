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
    "Large Cap": sheet.worksheet("Large Cap"),
    "Mid Cap": sheet.worksheet("Mid Cap"),
    "Technology": sheet.worksheet("Technology"),
    "SP Tracker": sheet.worksheet("SP Tracker"),
    "Top Picks": sheet.worksheet("Top Picks"),
}

def get_earnings_data(ticker, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            stock = yf.Ticker(ticker)
            stock_info = stock.info
           # print('Checking Stock')
            #print(json.dumps(stock_info, indent=4))

            if "earningsTimestamp" in stock_info:
                earnings_date = datetime.utcfromtimestamp(stock_info["earningsTimestamp"]).strftime("%Y-%m-%d")
            elif "earningsTimestampStart" in stock_info:
                earnings_date = datetime.utcfromtimestamp(stock_info["earningsTimestampStart"]).strftime("%Y-%m-%d")
            else:
                earnings_date = "N/A"

            eps_actual = stock_info.get("trailingEps", "N/A")
            eps_estimate = stock_info.get("epsCurrentYear", stock_info.get("epsForward", "N/A"))
            revenue_growth = stock_info.get("revenueGrowth", "N/A")
            if revenue_growth != "N/A":
                revenue_growth = round(revenue_growth, 3)

            debt_to_equity = stock_info.get("debtToEquity", stock_info.get("totalDebt", "N/A"))

            earnings_surprise = "N/A"
            if eps_actual != "N/A" and eps_estimate != "N/A" and eps_estimate > 0:
                earnings_surprise = round(((eps_actual - eps_estimate) / eps_estimate) * 100, 2)
            else:
                earnings_surprise = stock_info.get("earningsQuarterlyGrowth", "N/A")
            try:
                dte_val = (
                    datetime.strptime(earnings_date, "%Y-%m-%d").date()
                    - datetime.today().date()
                ).days
            except (TypeError, ValueError):
                dte_val = 999        # default when earnings date is “N/A”
     
            
            print(f"📊 {ticker} Earnings Data: EPS={eps_actual}, Growth={revenue_growth}, Debt={debt_to_equity}, Surprise={earnings_surprise},DTE={dte_val}")
            return earnings_date, eps_actual, revenue_growth, debt_to_equity, earnings_surprise,dte_val

        except Exception as e:
            if "Too Many Requests" in str(e):
                print(f"⚠️ YFinance Rate Limit hit for {ticker}. Pausing...")
                time.sleep(60)
                retries += 1
            else:
                print(f"❌ Error fetching earnings data for {ticker}: {e}")
                return "N/A", "N/A", "N/A", "N/A", "N/A",999

    print(f"❌ Skipping {ticker} after retries.")
    return "N/A", "N/A", "N/A", "N/A", "N/A",999

for sheet_name, ws in sheets_to_update.items():
    print(f"\n🔁 Processing Sheet: {sheet_name}")
    data = ws.get_all_values()
    if len(data) < 2:
        print(f"⚠️ Sheet {sheet_name} has no rows.")
        continue

    headers = data[0]

## ✅ Redefine new headers with earnings columns
    #new_headers = ["Rank", "Symbol", "Earnings Date", "EPS", "Revenue Growth", "Debt-to-Equity", "Earnings Surprise"] + headers[2:]
   # updated_data = [new_headers] + [[row[0], row[1], "", "", "", "", ""] + row[2:] for row in data[1:]]

    # ✅ Clear and update sheet
   # ws.clear()
    #ws.update("A1", updated_data)

    # ✅ Re-fetch after sheet is wiped
   # data = ws.get_all_values()
   # headers = data[0]

    for i, row in enumerate(data[1:], start=2):
        row_dict = {headers[j]: row[j] if j < len(row) else "N/A" for j in range(len(headers))}
        ticker = row_dict.get("Symbol", "N/A")

        if not ticker or ticker == "N/A":
            continue

        earnings_date, eps, revenue_growth, debt_to_equity, earnings_surprise,dte_val = get_earnings_data(ticker)

        updates = [
            {"range": f"C{i}", "values": [[earnings_date]]},
            {"range": f"D{i}", "values": [[eps]]},
            {"range": f"E{i}", "values": [[revenue_growth]]},
            {"range": f"F{i}", "values": [[debt_to_equity]]},
            {"range": f"G{i}", "values": [[earnings_surprise]]},
            {"range": f"H{i}", "values": [[dte_val]]},  
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
                    print(f"⚠️ Rate limit! Retrying (Attempt {retry_attempts})...")
                    time.sleep(10)
                    switch_api_key()
                    sheet = client.open("Stock Investment Analysis")
                    ws = sheet.worksheet(sheet_name)
                else:
                    print(f"❌ Error updating Google Sheets for {ticker} in {sheet_name}: {e}")
                    break

print("\n✅ Earnings Data Successfully Updated in All Sheets!")
