import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import time

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 🔹 JSON keys for API rotation
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
large_cap_ws = sheet.worksheet("Large Cap")
mid_cap_ws = sheet.worksheet("Mid Cap")
hybrid_ws = sheet.worksheet("Hybrid")
super_green_ws = sheet.worksheet("Super Green")  # ✅ New Super Green Sheet

# 🔹 Function to switch API keys
def switch_api_key():
    global active_api, client
    active_api = 2 if active_api == 1 else 1  # Toggle API key
    client = authenticate_with_json(CREDS_FILE_2 if active_api == 2 else CREDS_FILE_1)
    print(f"🔄 Switched to API Key {active_api}")

# Fetch data from Large Cap & Mid Cap sheets
large_cap_data = large_cap_ws.get_all_values()
mid_cap_data = mid_cap_ws.get_all_values()

# Convert to DataFrame, using first row as column headers
df_large = pd.DataFrame(large_cap_data[1:], columns=large_cap_data[0])
df_mid = pd.DataFrame(mid_cap_data[1:], columns=mid_cap_data[0])

# Convert necessary columns to numeric
numeric_cols = [
    "Market Cap", "Current Price", "VWMA", "1 Month Price Change",
    "1 Week Price Change", "1 Day Price Change", "Volume", "RSI", "Sentiment Ratio", "Score"
]

def clean_float(value):
    """Safely convert values to float, returning 0.0 for invalid numbers."""
    try:
        return float(str(value).replace('%', '').strip())  # Remove % if present
    except ValueError:
        return 0.0

for df in [df_large, df_mid]:
    for col in numeric_cols:
        df[col] = df[col].apply(clean_float)

# 🔹 Process Large Cap & Mid Cap stocks **separately first** before comparison
eligible_large_cap = []
eligible_mid_cap = []
super_green_stocks = []  # ✅ Super Green stocks list

# Process Large Cap Stocks
for idx, row in df_large.iterrows():
    stock_data = row.to_dict()
    stock_data["VWMA vs Current Price"] = stock_data["Current Price"] - stock_data["VWMA"]

    print(f"🧐 Checking Large Cap: {stock_data['Symbol']}")

    # **Weak Large Cap Stock Criteria**
    if (
        stock_data["1 Month Price Change"] < -3
        and stock_data["1 Week Price Change"] < -2
        and stock_data["RSI"] < 45
        and stock_data["Current Price"] < stock_data["VWMA"]
        and stock_data["Sentiment Ratio"] < 0.5
    ):
        eligible_large_cap.append(stock_data)
        print(f"⚠️ Weak Large Cap Identified: {stock_data['Symbol']}")

    # **Super Green Criteria**
    if stock_data["Score"] >= 6.8:
        super_green_stocks.append(stock_data)
        print(f"🚀 Super Green Stock Identified: {stock_data['Symbol']}")

    time.sleep(0.2)  # ✅ Wait per row

# Process Mid Cap Stocks
for idx, row in df_mid.iterrows():
    stock_data = row.to_dict()
    stock_data["VWMA vs Current Price"] = stock_data["Current Price"] - stock_data["VWMA"]

    print(f"🔍 Checking Mid Cap: {stock_data['Symbol']}")

    # **Momentum Mid Cap Stock Criteria**
    if (
        stock_data["1 Month Price Change"] > 5
        and stock_data["1 Week Price Change"] > 3
        and 50 <= stock_data["RSI"] <= 75
        and stock_data["Current Price"] > stock_data["VWMA"]
        and stock_data["Volume"] > df_mid["Volume"].mean() * 1.2
        and stock_data["Sentiment Ratio"] > 0.7
    ):
        eligible_mid_cap.append(stock_data)
        print(f"✅ Momentum Mid Cap Identified: {stock_data['Symbol']}")

    # **Super Green Criteria**
    if stock_data["Score"] >= 6.8:
        super_green_stocks.append(stock_data)
        print(f"🚀 Super Green Stock Identified: {stock_data['Symbol']}")

    time.sleep(0.2)  # ✅ Wait per row

# 🔹 Merge the two lists for Hybrid stocks
hybrid_stocks = eligible_large_cap + eligible_mid_cap

# Convert to DataFrame
df_hybrid = pd.DataFrame(hybrid_stocks)
df_super_green = pd.DataFrame(super_green_stocks)

# Sort by Market Cap (optional)
df_hybrid = df_hybrid.sort_values(by="Market Cap", ascending=False)
df_super_green = df_super_green.sort_values(by="Market Cap", ascending=False)

# Convert DataFrame to list of lists (for Google Sheets update)
if not df_hybrid.empty:
    hybrid_data = [df_hybrid.columns.tolist()] + df_hybrid.values.tolist()

    retry = True
    while retry:
        try:
            hybrid_ws.clear()
            hybrid_ws.update("A1", hybrid_data)
            print(f"✅ Hybrid Stocks Identified & Updated in 'Hybrid' Sheet - {len(df_hybrid)} stocks")
            retry = False  
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"⚠️ Rate limit hit! Switching API keys...")
                switch_api_key()
                sheet = client.open("Stock Investment Analysis")
                hybrid_ws = sheet.worksheet("Hybrid")
            else:
                print(f"❌ Error updating Hybrid Sheet: {e}")
                retry = False  

else:
    print(f"⚠️ No stocks met the criteria for Hybrid Sheet.")

# 🔹 Update the "Super Green" Sheet
if not df_super_green.empty:
    super_green_data = [df_super_green.columns.tolist()] + df_super_green.values.tolist()

    retry = True
    while retry:
        try:
            super_green_ws.clear()
            super_green_ws.update("A1", super_green_data)
            print(f"✅ Super Green Stocks Identified & Updated in 'Super Green' Sheet - {len(df_super_green)} stocks")
            retry = False  
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"⚠️ Rate limit hit! Switching API keys...")
                switch_api_key()
                sheet = client.open("Stock Investment Analysis")
                super_green_ws = sheet.worksheet("Super Green")
            else:
                print(f"❌ Error updating Super Green Sheet: {e}")
                retry = False  
