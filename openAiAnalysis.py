import os
import json
import gspread
import time
import yfinance as yf
import pandas as pd
import numpy as np
from openai import OpenAI
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from local JSON files
CREDS_FILE_1 = r"C:\Users\venka\Downloads\stock-analysis-447717-f449ebc79388.json"
CREDS_FILE_2 = r"C:\Users\venka\Downloads\stock-analysis-447717-6d99fc514040.json"

# 🔹 OpenAI API Key (Set in Environment Variables)
OPENAI_API_KEY = ""

# ✅ Initialize OpenAI Client
client_ai = OpenAI(api_key=OPENAI_API_KEY)

# 🔹 Function to authenticate with Google Sheets
def authenticate_with_json(json_file):
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authenticate_with_json(CREDS_FILE_1)
active_api = 1  # Track which API key is being used

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_FILE_2 if active_api == 2 else CREDS_FILE_1)
    print(f"🔄 Switched to API Key {active_api}")

# Open the spreadsheet and access the 'Top Picks' sheet
sheet = client.open("Stock Investment Analysis")
top_picks_ws = sheet.worksheet("Top Picks")

print("✅ Successfully authenticated with Google Sheets and OpenAI!")

# 🔹 Fetch existing data
def fetch_existing_data():
    return top_picks_ws.get_all_values()

existing_data = fetch_existing_data()

# 🔹 Define new headers
new_headers = ["Rank", "Symbol", "AI Decision(Buy/Hold/Sell)", "AI Recommended Buy Price", "Recommended Sell Price", "Technical Indicators Summary", "Rest of AI Analysis"] + existing_data[0][2:]

# 🔹 Reorganize data by shifting columns to the right
updated_data = [new_headers] + [[row[0], row[1], "", "", "", "", ""] + row[2:] for row in existing_data[1:]]

# 🔹 Update Google Sheets with existing data structure first
top_picks_ws.clear()
top_picks_ws.update("A1", updated_data)
print("✅ Google Sheet updated successfully with previous logic!")

# 🔹 Function to analyze stock with deep market and technical analysis
def analyze_stock(ticker):
    print(f"🔍 Analyzing {ticker}...")
    stock = yf.Ticker(ticker)
    hist = stock.history(period="6mo")
    if hist.empty:
        print(f"⚠️ No historical data for {ticker}")
        return None

    market_cap = stock.info.get("marketCap", "N/A")
    current_price = stock.info.get("regularMarketPrice", "N/A")
    pe_ratio = stock.info.get("trailingPE", "N/A")
    
    prompt = f"""
    As a professional stock analyst, perform a deep market and technical analysis for {ticker}. Provide:
    1️⃣ Buy, Hold, or Sell Recommendation
    2️⃣ Recommended Buy Price
    3️⃣ Recommended Sell Price
    4️⃣ Technical Indicators Summary
    """
    response = client_ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a professional stock analyst specializing in high-growth, low-risk investments."},
            {"role": "user", "content": prompt}
        ]
    )
    
    ai_analysis = response.choices[0].message.content
    print(f"✅ AI Analysis for {ticker}: {ai_analysis[:100]}...")
    
    # Parsing AI response into structured data
    decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis = "N/A", "N/A", "N/A", "", ""
    
    if "Recommendation:" in ai_analysis:
        decision = ai_analysis.split("Recommendation:")[1].split("\n")[0].replace("**", "").strip()
    
    if "Recommended Buy Price:" in ai_analysis:
        buy_price = ai_analysis.split("Recommended Buy Price:")[1].split("\n")[0].replace("**", "").strip()
    
    if "Recommended Sell Price:" in ai_analysis:
        sell_price = ai_analysis.split("Recommended Sell Price:")[1].split("\n")[0].replace("**", "").strip()
    
    if "Technical Indicators Summary" in ai_analysis:
        technical_summary = ai_analysis.split("Technical Indicators Summary")[1].strip().split("\n## Conclusion")[0].strip()
    
    rest_of_ai_analysis = ai_analysis
    
    return [decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis]

# 🔹 Process stocks and update Google Sheets
updates = []
tickers = [row[1] for row in existing_data[1:]]
for i, ticker in enumerate(tickers, start=2):  # Start from row 2 since headers are in row 1
    stock_data = analyze_stock(ticker)
    if stock_data:
        top_picks_ws.update(f"C{i}:G{i}", [stock_data])
    time.sleep(5)

# 🔹 Apply conditional formatting for Buy/Sell decisions
def apply_formatting():
    cell_list = top_picks_ws.range(f"C2:C{len(existing_data)}")
    for cell in cell_list:
        if cell.value == "Buy":
            cell.format = {"backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}}
        elif cell.value == "Sell":
            cell.format = {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}
    top_picks_ws.batch_update([{"range": f"C2:C{len(existing_data)}", "values": [[cell.value] for cell in cell_list]}])

apply_formatting()
print("✅ Google Sheet updated successfully with AI analysis on specified columns and conditional formatting!")
