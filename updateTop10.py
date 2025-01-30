import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import time

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE_1 = r"C:\Users\venka\Downloads\stock-analysis-447717-f449ebc79388.json"
CREDS_FILE_2 = r"C:\Users\venka\Downloads\stock-analysis-447717-6d99fc514040.json"

# 🔹 Function to authenticate with Google Sheets using a JSON key
def authenticate_with_json(json_key):
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_key, SCOPE)
    return gspread.authorize(creds)

# Start with API Key 1
client = authenticate_with_json(CREDS_FILE_1)
active_api = 1  # Track which API key is being used

# Open the main spreadsheet and access sheets
sheet = client.open("Stock Investment Analysis")
super_green_ws = sheet.worksheet("Super Green")
top30_ws = sheet.worksheet("Top 30")

# 🔹 Function to switch API keys when hitting rate limits
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_FILE_2 if active_api == 2 else CREDS_FILE_1)
    print(f"🔄 Switched to API Key {active_api}")

# Fetch data from Super Green sheet
super_green_data = super_green_ws.get_all_values()

# Convert to DataFrame, using first row as column headers
df_super_green = pd.DataFrame(super_green_data[1:], columns=super_green_data[0])

# Convert necessary columns to numeric
numeric_cols = [
    "Market Cap", "Current Price", "VWMA", "EMA", "ATR",
    "1 Month Price Change", "1 Week Price Change", "1 Day Price Change",
    "Volume", "RSI", "Sentiment Ratio", "Score"
]

def clean_float(value):
    """Safely convert values to float, returning 0.0 for invalid numbers."""
    try:
        return float(str(value).replace('%', '').strip())  # Remove % if present
    except ValueError:
        return 0.0

for col in numeric_cols:
    df_super_green[col] = df_super_green[col].apply(clean_float)

# 🔹 Rank stocks based on key performance indicators
df_super_green = df_super_green.sort_values(by="Score", ascending=False)
df_super_green["Rank"] = range(1, len(df_super_green) + 1)

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

df_super_green[["Stop Price", "Buy Price", "Sell Price"]] = df_super_green.apply(calculate_prices, axis=1)

# 🔹 Select Top 30 stocks for Top 30 sheet with all columns
df_top30 = df_super_green.head(30)

# Reorder columns to match the required format
column_order = [
    "Rank", "Symbol", "Market Cap", "Current Price", "Stop Price", "Buy Price", "Sell Price", "Name",
    "P/E", "Yesterday Close Price", "1 Day Price Change", "1 Week Price Change", "1 Month Price Change",
    "Volume", "RSI", "VWMA", "EMA", "ATR", "Industry", "Positive Rating", "Negative Rating",
    "Sentiment Ratio", "Latest News Date", "News 1", "News 2", "News 3", "News 4", "News 5",
    "News Link 1", "News Link 2", "News Link 3", "News Link 4", "News Link 5", "News Update Date", "Score", "VWMA vs Current Price"
]

# Ensure only existing columns are included
df_top30 = df_top30[[col for col in column_order if col in df_top30.columns]]

# Convert DataFrame to list of lists (for Google Sheets update)
top30_data = [df_top30.columns.tolist()] + df_top30.values.tolist()

# Clear and update the Top 30 sheet
retry = True
while retry:
    try:
        top30_ws.clear()
        top30_ws.update("A1", top30_data)
        print(f"✅ Top 30 Stocks Identified & Updated in 'Top 30' Sheet - {len(df_top30)} stocks")
        retry = False  # Successfully updated, exit retry loop
    except gspread.exceptions.APIError as e:
        if "429" in str(e):  # Detect API Rate Limit error
            print(f"⚠️ Rate limit hit! Switching API keys...")
            switch_api_key()
            sheet = client.open("Stock Investment Analysis")
            top30_ws = sheet.worksheet("Top 30")  # Re-authenticate the Top 30 Sheet
        else:
            print(f"❌ Error updating Top 30 Sheet: {e}")
            retry = False  # Stop retrying if it's a different error
