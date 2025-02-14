import os  # Required for environment variables
import json  # Required for JSON parsing
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import time
from datetime import datetime, timedelta
import numpy as np


# 🔹 Google Sheets API Setup with Two Keys
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from GitHub Secrets
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# Function to authorize with a given key
def authorize_client(creds_json):
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authorize_client(CREDS_JSON_1)
active_api = 1  # Track which API key is being used

# Open the spreadsheet and access both Large Cap & Mid Cap sheets
sheet = client.open("Stock Investment Analysis")
sheets_to_update = {
    "Large Cap": sheet.worksheet("Large Cap"),
    "Mid Cap": sheet.worksheet("Mid Cap"),
    "Technology":sheet.worksheet("Technology")
}

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authorize_client(CREDS_JSON_2 if active_api == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to API Key {active_api}")

# Weight assignments for scoring
WEIGHTS = {
    "1 Month Price Change": 0.30,
    "1 Week Price Change": 0.20,
    "1 Day Price Change": 0.15,
    "Volume": 0.10,
    "RSI": 0.10,
    "Sentiment Ratio": 0.10,
    "ATR": 0.05,
    "VWMA vs Current Price": 0.05,
}

# News Recency Score Adjustments
def news_score_adjustment(news_age, sentiment_ratio):
    if news_age <= 3:
        return 1.0 if sentiment_ratio >= 0.75 else 0.75 if sentiment_ratio >= 0.5 else 0.5
    elif 4 <= news_age <= 7:
        return 0.5 if sentiment_ratio >= 0.75 else 0.25 if sentiment_ratio >= 0.5 else 0
    elif 8 <= news_age <= 14:
        return 0.1
    elif news_age > 14:
        return -0.5 if sentiment_ratio >= 0.75 else -0.75 if sentiment_ratio >= 0.5 else -1.0
    return 0

# Scoring function
def calculate_score(row):
    # Ensure all values are numeric
    try:
        score = (
            float(row["1 Month Price Change"]) * WEIGHTS["1 Month Price Change"] +
            float(row["1 Week Price Change"]) * WEIGHTS["1 Week Price Change"] +
            float(row["1 Day Price Change"]) * WEIGHTS["1 Day Price Change"] +
            float(row["Volume"]) * WEIGHTS["Volume"] +
            (float(row["RSI"]) * WEIGHTS["RSI"] if 30 <= float(row["RSI"]) <= 70 else 0) +
            float(row["Sentiment Ratio"]) * WEIGHTS["Sentiment Ratio"] +
            float(row["ATR"]) * WEIGHTS["ATR"] +
            (WEIGHTS["VWMA vs Current Price"] if float(row["VWMA vs Current Price"]) > 0 else 0) +
            news_score_adjustment(float(row["News Age"]), float(row["Sentiment Ratio"]))
        )
        return round(score, 2)
    except (ValueError, TypeError) as e:
        print(f"❌ Error calculating score for row: {e}")
        return 0  # Return 0 if there's an error


# Categorization function
def categorize_score(score):
    if score >= 6.8:
        return "🚀 Strong Buy", (0, 128, 0)
    elif 5.5 <= score < 6.8:
        return "✅ Buy", (144, 238, 144)
    elif 4.0 <= score < 5.5:
        return "🤔 Neutral", None
    elif 3.0 <= score < 4.0:
        return "⚠️ Caution / Weak", (255, 255, 0)
    else:
        return "❌ Avoid / Sell", (255, 102, 102)

# 🔹 Process in batches of 10 rows for both Large Cap & Mid Cap sheets
for sheet_name, worksheet in sheets_to_update.items():
    print(f"\n🔄 Processing {sheet_name}...")

    # Fetch data into a DataFrame
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    # Convert columns to numeric (handling errors)
    numeric_cols = [
        "1 Day Price Change", "1 Week Price Change", "1 Month Price Change",
        "Volume", "RSI", "VWMA", "Current Price", "EMA", "ATR", "Sentiment Ratio"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('%', ''), errors='coerce').fillna(0)


    # Convert "Latest News Date" to datetime format
    df["Latest News Date"] = pd.to_datetime(df["Latest News Date"], errors='coerce')

    # Compute News Age (Days)
    today = datetime.today()
    df["News Age"] = (today - df["Latest News Date"]).dt.days.fillna(999)

    
    # Add VWMA vs Current Price column
    df["VWMA vs Current Price"] = df["Current Price"] - df["VWMA"]

    # Normalize values before scoring
    df["1 Day Price Change"] *= 100
    df["1 Week Price Change"] *= 100
    df["1 Month Price Change"] *= 100
    df["Volume"] /= 1e6
    df["ATR"] = 1 / (df["ATR"] + 1)
    # Replace NaN, inf, and -inf with "N/A"
    df.replace([np.nan, np.inf, -np.inf], "N/A", inplace=True) 

    # Process in **batches of 10 rows at a time**
    batch_size = 10
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]

        batch_updates = []
        batch_formatting = []
        row_numbers = []

        for idx, row in batch.iterrows():
            stock_score = calculate_score(row)
            category, color = categorize_score(stock_score)
            row_number = idx + 2  # Adjust for Google Sheets

            print(f"Batch updating {sheet_name} row {row_number} - {row['Symbol']} | Score: {stock_score} | Category: {category} | News Age: {row['News Age']} days")

            batch_updates.append([[stock_score]])
            row_numbers.append(row_number)

            if color:
                batch_formatting.append((row_number, color))

        retry = True
        while retry:
            try:
                if batch_updates:
                    cell_ranges = [f"AE{row}" for row in row_numbers]
                    worksheet.batch_update([{"range": r, "values": v} for r, v in zip(cell_ranges, batch_updates)])

                for row_number, color in batch_formatting:
                    worksheet.format(f"A{row_number}", {"backgroundColor": {"red": color[0] / 255, "green": color[1] / 255, "blue": color[2] / 255}})

                print(f"✅ Successfully batch updated {sheet_name} rows {row_numbers}")
                retry = False  # Stop retry loop

            except gspread.exceptions.APIError as e:
                error_message = str(e)
                if "429" in error_message:
                    print(f"⚠️ Rate limit hit! Pausing for 60 seconds before switching API keys...")
                    time.sleep(10)  # Wait for 60 seconds
                    switch_api_key()  # Switch API Key
                    worksheet = client.open("Stock Investment Analysis").worksheet(sheet_name)
                else:
                    print(f"❌ Error batch updating {sheet_name} rows {row_numbers}: {e}")
                    retry = False

        time.sleep(1)  # Small delay to prevent hitting API limits

print("✅ Scores updated in batches of 10 & Colors applied to Column A for both Large Cap & Mid Cap!")
