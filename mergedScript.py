import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = r"C:\Users\venka\Downloads\Stock Python"

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)

# Open the spreadsheet and access Large Cap & Mid Cap sheets
sheet = client.open("Stock Investment Analysis")
sheets_to_update = {
    "Large Cap": sheet.worksheet("Large Cap"),
    "Mid Cap": sheet.worksheet("Mid Cap")
}

# 🔹 Define numeric columns
numeric_cols = [
    "Market Cap", "P/E", "Current Price", "Yesterday Close Price",
    "1 Day Price Change", "1 Week Price Change", "1 Month Price Change",
    "Volume", "RSI", "VWMA", "EMA", "ATR", "Sentiment Ratio"
]

# Weight assignments for scoring (balanced)
WEIGHTS = {
    "1 Month Price Change": 0.30,
    "1 Week Price Change": 0.20,
    "1 Day Price Change": 0.15,
    "Volume": 0.05,  # Lowered impact
    "RSI": 0.10,
    "Sentiment Ratio": 0.10,
    "ATR": 0.05,
    "VWMA vs Current Price": 0.05,
}

# Normalize high-value fields to prevent huge scores
def normalize(value, max_value):
    """Normalize a value between 0 and 1 using a given max_value."""
    return min(value / max_value, 1) if value != "N/A" else 0

# 🔹 News Recency Score Adjustments
def news_score_adjustment(news_age, sentiment_ratio):
    if news_age <= 3:
        return 1.0 if sentiment_ratio >= 0.75 else 0.75 if sentiment_ratio >= 0.5 else 0.5
    elif 4 <= news_age <= 7:
        return 0.5 if sentiment_ratio >= 0.75 else 0.25 if sentiment_ratio >= 0.5 else 0
    elif 8 <= news_age <= 14:
        return 0.1
    elif news_age > 14:
        return -0.5 if sentiment_ratio >= 0.75 else -0.75 if sentiment_ratio >= 0.5 else -1.0
    return 0  # Default no change

# 🔹 Categorization function
def categorize_score(score):
    if score >= 6.8:
        return "🚀 Strong Buy", (0, 128, 0)  # Super Green
    elif 5.5 <= score < 6.8:
        return "✅ Buy", (144, 238, 144)  # Light Green
    elif 4.0 <= score < 5.5:
        return "🤔 Neutral", None  # No color
    elif 3.0 <= score < 4.0:
        return "⚠️ Caution / Weak", (255, 255, 0)  # Yellow
    else:
        return "❌ Avoid / Sell", (255, 102, 102)  # Light Red

# 🔹 Safe conversion function
def safe_convert(value):
    if isinstance(value, (pd.Series, pd.DataFrame)):
        return value.iloc[0] if not value.empty else "N/A"
    if isinstance(value, (np.int64, np.float64)):
        value = value.item()
    if isinstance(value, float):
        return "N/A" if np.isnan(value) or np.isinf(value) else round(value, 2)
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

# 🔹 Function to calculate VWMA
def calculate_vwma(prices, volumes, period=20):
    try:
        if len(prices) < period:
            return "N/A"
        vwma = (prices * volumes).rolling(window=period).sum() / volumes.rolling(window=period).sum()
        return safe_convert(vwma.iloc[-1])
    except Exception as e:
        print(f"❌ Error calculating VWMA: {e}")
        return "N/A"

# 🔹 Function to fetch stock data
def get_stock_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="3mo")

        if hist.empty:
            print(f"⚠️ No historical data for {ticker}")
            return None

        prices = hist["Close"]
        volumes = hist["Volume"]

        market_cap = safe_convert(stock.info.get("marketCap", "N/A"))
        pe_ratio = safe_convert(stock.info.get("trailingPE", "N/A"))

        current_price = safe_convert(prices.iloc[-1])
        yesterday_close_price = safe_convert(prices.iloc[-2]) if len(prices) > 1 else "N/A"

        percent_change_1d = ((current_price - yesterday_close_price) / yesterday_close_price) * 100 if yesterday_close_price != "N/A" else "N/A"
        percent_change_1wk = ((current_price - safe_convert(prices.iloc[-6])) / safe_convert(prices.iloc[-6])) * 100 if len(prices) > 6 else "N/A"
        percent_change_1mo = ((current_price - safe_convert(prices.iloc[0])) / safe_convert(prices.iloc[0])) * 100

        volume = safe_convert(volumes.iloc[-1])
        rsi = calculate_rsi(prices, period=14)
        vwma = calculate_vwma(prices, volumes, period=20)
        ema = safe_convert(prices.ewm(span=10, adjust=False).mean().iloc[-1])
        atr = safe_convert((hist["High"] - hist["Low"]).rolling(14).mean().iloc[-1])

        return [
            market_cap, pe_ratio, current_price, yesterday_close_price,
            round(percent_change_1d, 2), round(percent_change_1wk, 2), round(percent_change_1mo, 2),
            volume, rsi, vwma, ema, atr
        ]

    except Exception as e:
        print(f"❌ Error fetching data for {ticker}: {e}")
        return None

# 🔹 Process each ticker
for sheet_name, worksheet in sheets_to_update.items():
    tickers = worksheet.col_values(1)[1:]  # Reads tickers from Column A, skipping header

    for idx, ticker in enumerate(tickers, start=2):
        while True:
            try:
                stock_data = get_stock_data(ticker)
                if stock_data is None:
                    print(f"⚠️ Skipping update for {ticker}: No data available.")
                    break

                stock_data_dict = dict(zip(numeric_cols, stock_data))
                stock_data_dict["VWMA vs Current Price"] = stock_data_dict["Current Price"] - stock_data_dict["VWMA"]

                stock_score = sum(stock_data_dict.get(key, 0) * WEIGHTS[key] for key in WEIGHTS)

                category, color = categorize_score(stock_score)
                worksheet.update(range_name=f"AE{idx}", values=[[stock_score]])

                if color:
                    worksheet.format(f"A{idx}", {"backgroundColor": {"red": color[0] / 255, "green": color[1] / 255, "blue": color[2] / 255}})
                else:
                    worksheet.format(f"A{idx}", {"backgroundColor": None})

                print(f"✅ {sheet_name} - {ticker} | Score: {stock_score} | Category: {category}")
                time.sleep(0.5)
                break

            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    print(f"⚠️ Rate limit hit! Pausing for 60 seconds before retrying...")
                    time.sleep(60)

print("✅ Google Sheets Updated with Scores & Colors!")
