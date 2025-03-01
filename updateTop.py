import os  # Required for environment variables
import json  # Required for JSON parsing
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import time
from datetime import datetime, timedelta
import numpy as np 

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 Load credentials from GitHub Secrets
CREDS_JSON_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_JSON_2 = os.getenv("GOOGLE_CREDENTIALS_2")

# 🔹 Function to authenticate with Google Sheets using JSON from environment variables
def authenticate_with_json(json_str):
    creds_dict = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authenticate_with_json(CREDS_JSON_1)
active_api = 1  # Track which API key is being used

# Open the main spreadsheet and access sheets
sheet = client.open("Stock Investment Analysis")
super_green_ws = sheet.worksheet("Super Green")
top_picks_ws = sheet.worksheet("Top Picks")
hybrid_ws = sheet.worksheet("Hybrid") # ✅ Updated to "Top Picks"

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_JSON_2 if active_api == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to API Key {active_api}")
# Fetch data from the Hybrid sheet
hybrid_data = hybrid_ws.get_all_values()

# Convert to DataFrame using the first row as column headers
df_hybrid = pd.DataFrame(hybrid_data[1:], columns=hybrid_data[0])



# Fetch data from Super Green sheet
super_green_data = super_green_ws.get_all_values()

# Convert to DataFrame, using first row as column headers
df_super_green = pd.DataFrame(super_green_data[1:], columns=super_green_data[0])

# Convert necessary columns to numeric
numeric_cols = [
    "Market Cap", "Current Price", "VWMA", "EMA", "ATR",
    "1 Month Price Change", "1 Week Price Change", "1 Day Price Change",
    "Volume", "RSI", "Sentiment Ratio", "Score","P/E"
]

def clean_float(value):
    """Safely convert values to float, ensuring they are valid numbers."""
    try:
        value = str(value).replace('%', '').strip()  # Remove % if present
        float_value = float(value) if value else float("nan")  # Convert empty strings to NaN
        if not np.isfinite(float_value):  # Check for NaN, inf, or extremely large values
            return "N/A"
        return float_value
    except ValueError:
        return "N/A"  # Return "N/A" for invalid numeric values


for col in numeric_cols:
    df_super_green[col] = df_super_green[col].apply(clean_float)
# Convert necessary columns to numeric using the clean_float function
for col in numeric_cols:
    if col in df_hybrid.columns:
        df_hybrid[col] = df_hybrid[col].apply(clean_float)

# Merge Super Green and Hybrid data
df_combined = pd.concat([df_super_green, df_hybrid], ignore_index=True)

# Convert "Latest News Date" to datetime format and specify day-first format
df_combined["Latest News Date"] = pd.to_datetime(df_combined["Latest News Date"], format="%d-%m-%Y %H:%M:%S", errors='coerce')

# Calculate News Age (Days)
today = datetime.today()
df_combined["News Age"] = (today - df_combined["Latest News Date"]).dt.days.fillna(999)

# **Modify Score Based on News Age:**
# - Stocks with recent news (≤90 days) maintain their score.
# - Stocks with news older than 90 days get their score reduced by 20%.
df_combined["Adjusted Score"] = df_combined.apply(
    lambda row: row["Score"] * 0.8 if row["News Age"] > 90 else row["Score"], axis=1
)

# 🔹 Rank stocks based on adjusted score
df_combined = df_combined.sort_values(by="Adjusted Score", ascending=False)
df_combined["Rank"] = range(1, len(df_combined) + 1)

# 🔹 Calculate Stop Price, Buy Price, Sell Price
def calculate_prices(row):
    current_price = row["Current Price"]
    atr = row["ATR"]
    
    # Stop Price = Current Price - (ATR * 1.5)
    stop_price = round(current_price - (atr * 1.5), 2)

    # Buy Price = Current Price
    buy_price = round(current_price, 2)

    # Sell Price = Buy Price * 1.20 (20% profit target)
    sell_price = round(buy_price * 1.20, 2)

    return pd.Series([stop_price, buy_price, sell_price])

df_combined[["Stop Price", "Buy Price", "Sell Price"]] = df_combined.apply(calculate_prices, axis=1)

# **All high-potential stocks included (not limited to 30)**
df_top_picks = df_combined.copy()

# Reorder columns to match the required format
column_order = [
    "Rank", "Symbol", "Market Cap", "Current Price", "Stop Price", "Buy Price", "Sell Price", "Name",
    "P/E", "Yesterday Close Price", "1 Day Price Change", "1 Week Price Change", "1 Month Price Change",
    "Volume", "RSI", "VWMA", "EMA", "ATR", "Industry", "Positive Rating", "Negative Rating",
    "Sentiment Ratio", "Latest News Date", "News 1", "News 2", "News 3", "News 4", "News 5",
    "News Link 1", "News Link 2", "News Link 3", "News Link 4", "News Link 5", "News Update Date", "Adjusted Score", "VWMA vs Current Price"
]

# Remove duplicates based on the "Symbol" column, keeping the first occurrence
df_top_picks = df_top_picks.drop_duplicates(subset="Symbol", keep="first")

# Ensure only existing columns are included
df_top_picks = df_top_picks[[col for col in column_order if col in df_top_picks.columns]]

# Convert Pandas Timestamps to String before updating Google Sheets
df_top_picks["Latest News Date"] = df_top_picks["Latest News Date"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("N/A")
df_top_picks["News Update Date"] = df_top_picks["News Update Date"].astype(str)

df_top_picks.replace([np.inf, -np.inf, np.nan], "N/A", inplace=True)
# ✅ Convert DataFrame to list of lists (for Google Sheets update)
top_picks_data = [df_top_picks.columns.tolist()] + df_top_picks.astype(str).values.tolist()  # Convert all to string

# ✅ Clear and update the "Top Picks" sheet safely
retry = True
while retry:
    try:
        top_picks_ws.clear()
        top_picks_ws.update(values=top_picks_data, range_name="A1")  # ✅ Fixed argument order
        print(f"✅ Top Picks Identified & Updated in 'Top Picks' Sheet - {len(df_top_picks)} stocks")
        retry = False  # Successfully updated, exit retry loop
    except gspread.exceptions.APIError as e:
        if "429" in str(e):  # Detect API Rate Limit error
            print(f"⚠️ Rate limit hit! Pausing for 60 seconds before switching API keys...")
            time.sleep(60)  # ✅ Wait before retrying
            switch_api_key()
            sheet = client.open("Stock Investment Analysis")
            top_picks_ws = sheet.worksheet("Top Picks")  # Re-authenticate the sheet
        else:
            print(f"❌ Error updating Top Picks Sheet: {e}")
            retry = False  # Stop retrying if it's a different error
