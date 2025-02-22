import gspread
import yfinance as yf
import json
import time
import openai
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import re  # ✅ Ensure `re` is imported for regex parsing
from gspread_formatting import format_cell_range, CellFormat, Color
# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials for API key rotation
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# ✅ Function to authenticate with Google Sheets
def authenticate_with_json(json_file):
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, SCOPE)
    return gspread.authorize(creds)

# ✅ Start with API Key 1
client = authenticate_with_json(CREDS_FILE_1)
active_api = 1  # Track which API key is being used

# ✅ Function to switch API keys
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_FILE_2 if active_api == 2 else CREDS_FILE_1)
    print(f"🔄 Switched to Google Sheets API Key {active_api}")

# ✅ Open the spreadsheet and access the "Top Picks" sheet
sheet = client.open("Stock Investment Analysis")
top_picks_ws = sheet.worksheet("Top Picks")
ai_cache_ws = sheet.worksheet("AI_Cache")  # ✅ Access AI_Cache sheet

# ✅ Fetch all data from "Top Picks"
data = top_picks_ws.get_all_values()
headers = data[0]  # Extract column headers
# 🔹 Fetch existing data
def fetch_existing_data():
    return top_picks_ws.get_all_values()

existing_data = fetch_existing_data()

# 🔹 Define new headers
new_headers = ["Rank", "Symbol","earnings_date", "eps", "revenue_growth", "debt_to_equity", "earnings_surprise"] + existing_data[0][2:]

# 🔹 Reorganize data by shifting columns to the right
updated_data = [new_headers] + [[row[0], row[1], "", "", "", "", "",] + row[2:] for row in existing_data[1:]]

# 🔹 Update Google Sheets with existing data structure first
top_picks_ws.clear()
top_picks_ws.update("A1", updated_data)
for i, row in enumerate(data[1:], start=2):  # Skip headers
    row_dict = {headers[j]: row[j] if j < len(row) else "N/A" for j in range(len(headers))}
    ticker = row_dict.get('Symbol', 'N/A')

    # ✅ Fetch Earnings Data
    earnings_date, eps, revenue_growth, debt_to_equity, earnings_surprise = get_earnings_data(ticker)

    # ✅ Update Google Sheets with Earnings Data
    updates = [
        {"range": f"H{i}", "values": [[earnings_date]]},  # Next Earnings Date
        {"range": f"I{i}", "values": [[eps]]},  # EPS
        {"range": f"J{i}", "values": [[revenue_growth]]},  # Revenue Growth
        {"range": f"K{i}", "values": [[debt_to_equity]]},  # Debt-to-Equity Ratio
        {"range": f"L{i}", "values": [[earnings_surprise]]}  # Earnings Surprise
    ]
    
    top_picks_ws.batch_update(updates)
    print(f"✅ Updated Earnings Data for {ticker} in row {i}")
    time.sleep(1)  # Prevent hitting rate limits

print("✅ Earnings Data Successfully Updated in 'Top Picks' Sheet!")

