import os
import openai
import requests
import yfinance as yf
import gspread
import pandas as pd
import time
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials

# 📌 Set OpenAI API Key (Replace this with your API key)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 📌 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from GitHub Secrets or local file
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# 🔹 Function to authenticate with Google Sheets
def authenticate_with_json(json_str):
    creds_dict = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authenticate_with_json(CREDS_JSON_1)
active_api = 1  # Track which API key is being used

# Open the spreadsheet and access Top Picks sheet
sheet = client.open("Stock Investment Analysis")
top_picks_ws = sheet.worksheet("Top Picks")

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_JSON_2 if active_api == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to API Key {active_api}")

# 📌 Function to get live market data using Yahoo Finance
def get_stock_data(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="3mo")

    if hist.empty:
        return None  # No data available

    data = {
        "Current Price": stock.info.get("lastPrice", "N/A"),
        "Market Cap": stock.info.get("marketCap", "N/A"),
        "Volume": stock.info.get("volume", "N/A"),
        "RSI": calculate_rsi(hist["Close"]),
        "VWMA": calculate_vwma(hist["Close"], hist["Volume"]),
        "EMA": hist["Close"].ewm(span=10, adjust=False).mean().iloc[-1],
        "ATR": (hist["High"] - hist["Low"]).rolling(14).mean().iloc[-1]
    }
    return data

# 📌 Function to fetch news & sentiment from the web
def fetch_sentiment(ticker):
    search_url = f"https://www.google.com/search?q={ticker}+stock+news"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(search_url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        headlines = [h.text for h in soup.find_all("h3")][:5]  # Get top 5 headlines
        return headlines if headlines else "No news found"
    except Exception as e:
        print(f"⚠️ Error fetching sentiment for {ticker}: {e}")
        return "N/A"

# 📌 Function to ask OpenAI API for AI-driven investment recommendation
def get_ai_analysis(ticker, stock_data, headlines):
    prompt = f"""
    Analyze the following stock data and provide a trading decision for {ticker}:
    
    - Current Price: {stock_data['Current Price']}
    - Market Cap: {stock_data['Market Cap']}
    - Volume: {stock_data['Volume']}
    - RSI: {stock_data['RSI']}
    - VWMA: {stock_data['VWMA']}
    - EMA: {stock_data['EMA']}
    - ATR: {stock_data['ATR']}
    
    Recent News Headlines:
    {headlines}
    
    Is this stock a **BUY, HOLD, or SELL**? Explain why in 2-3 sentences.
    """
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        api_key=OPENAI_API_KEY
    )
    
    return response["choices"][0]["message"]["content"]

# 📌 Process each ticker in Top Picks
tickers = top_picks_ws.col_values(2)[1:]  # Read tickers from Column B (Skipping header)

for idx, ticker in enumerate(tickers, start=2):  
    print(f"\n🔍 Analyzing {ticker}...")
    
    try:
        stock_data = get_stock_data(ticker)
        if not stock_data:
            print(f"⚠️ Skipping {ticker}: No market data available")
            continue

        headlines = fetch_sentiment(ticker)
        ai_analysis = get_ai_analysis(ticker, stock_data, headlines)

        # Update Google Sheet with AI decision
        top_picks_ws.update(f"Q{idx}", [[ai_analysis]])
        print(f"✅ Updated AI analysis for {ticker} in row {idx}")

        time.sleep(1)  # Avoid rate limits
    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        continue  # Move to next stock

print("✅ All stocks in 'Top Picks' updated with AI-driven insights!")
