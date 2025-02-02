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
import requests

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

# 🔹 Function to fetch missing data from the web
def fetch_web_data(ticker):
    try:
        search_url = f"https://www.google.com/search?q={ticker}+stock+price"
        response = requests.get(search_url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code == 200:
            return "Web data retrieved"
        return "Data not found"
    except Exception as e:
        return f"Error fetching data: {e}"

# 🔹 Function to parse AI response
def parse_ai_analysis(ai_analysis):
    decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis = "N/A", "N/A", "N/A", "", ai_analysis
    
    if "Recommendation:" in ai_analysis:
        decision = ai_analysis.split("Recommendation:")[1].split("\n")[0].replace("**", "").strip()
    
    if "Recommended Buy Price:" in ai_analysis or "Buy Price Range:" in ai_analysis:
        buy_price_section = ai_analysis.split("Recommended Buy Price:")[-1] if "Recommended Buy Price:" in ai_analysis else ai_analysis.split("Buy Price Range:")[-1]
        buy_price = buy_price_section.split("\n")[0].replace("**", "").strip()
    
    if "Recommended Sell Price:" in ai_analysis or "Sell Price Range:" in ai_analysis:
        sell_price_section = ai_analysis.split("Recommended Sell Price:")[-1] if "Recommended Sell Price:" in ai_analysis else ai_analysis.split("Sell Price Range:")[-1]
        sell_price = sell_price_section.split("\n")[0].replace("**", "").strip()
    
    if "Technical Indicators Summary" in ai_analysis:
        technical_summary = ai_analysis.split("Technical Indicators Summary")[1].strip().split("\n## Conclusion")[0].strip()
    
    return decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis

# 🔹 Function to analyze stock with historical data
def analyze_stock(ticker):
    print(f"🔍 Analyzing {ticker}...")
    stock = yf.Ticker(ticker)
    hist = stock.history(period="6mo")
    if hist.empty:
        print(f"⚠️ No historical data for {ticker}, fetching from web...")
        web_data = fetch_web_data(ticker)
        return [ticker, "No data from Yahoo", web_data, "", ""]
    
    market_cap = stock.info.get("marketCap", "N/A")
    current_price = stock.info.get("regularMarketPrice", "N/A")
    pe_ratio = stock.info.get("trailingPE", "N/A")
    
    # Fetch historical data
    hist_high = hist["High"].max() if not hist.empty else "N/A"
    hist_low = hist["Low"].min() if not hist.empty else "N/A"
    hist_avg = hist["Close"].mean() if not hist.empty else "N/A"
    
    prompt = f"""
    As a professional stock analyst, use real-time and historical data for {ticker} to make precise recommendations:
    - Market Cap: {market_cap}
    - Current Price: {current_price}
    - P/E Ratio: {pe_ratio}
    - Historical High: {hist_high}
    - Historical Low: {hist_low}
    - Historical Average: {hist_avg}
    If any of these values are missing, use external web sources to find the data. Provide:
    1️⃣ Buy, Hold, or Sell Recommendation
    2️⃣ Recommended Buy Price (numeric value or range)
    3️⃣ Recommended Sell Price (numeric value or range)
    4️⃣ Technical Indicators Summary
    """
    response = client_ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a professional stock analyst specializing in high-growth, low-risk investments. Ensure all price recommendations include numeric values and leverage both real-time and historical data. If any data is missing, search the web to obtain better insights."},
            {"role": "user", "content": prompt}
        ]
    )
    
    ai_analysis = response.choices[0].message.content
    print(f"✅ AI Analysis for {ticker}: {ai_analysis[:100]}...")
    
    return parse_ai_analysis(ai_analysis)

# 🔹 Process stocks and update Google Sheets
tickers = [row[1] for row in existing_data[1:] if len(row) > 1]
for i, ticker in enumerate(tickers, start=2):
    stock_data = analyze_stock(ticker)
    if stock_data:
        top_picks_ws.update(f"C{i}:G{i}", [stock_data])
    time.sleep(5)

print("✅ Google Sheet updated successfully with AI analysis!")
