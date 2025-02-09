import gspread
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
CREDS_FILE_1 = os.getenv("GOOGLE_CREDENTIALS_1")
CREDS_FILE_2 = os.getenv("GOOGLE_CREDENTIALS_2")


# 🔹 OpenAI API Key
OPENAI_API_KEY =os.getenv("OPENAI_API_KEY")

# ✅ Initialize OpenAI Client
client_ai = openai.OpenAI(api_key=OPENAI_API_KEY)

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
new_headers = ["Rank", "Symbol", "AI Decision(Buy/Hold/Sell)", "AI Recommended Buy Price", "Recommended Sell Price", "Technical Indicators Summary", "Rest of AI Analysis"] + existing_data[0][2:]

# 🔹 Reorganize data by shifting columns to the right
updated_data = [new_headers] + [[row[0], row[1], "", "", "", "", ""] + row[2:] for row in existing_data[1:]]

# 🔹 Update Google Sheets with existing data structure first
top_picks_ws.clear()
top_picks_ws.update("A1", updated_data)



def parse_ai_analysis(ai_analysis):
    try:
        decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis = "N/A", "N/A", "N/A", "", ai_analysis

        # ✅ Extract Recommendation (Buy/Hold/Sell)
        match = re.search(r"Recommendation:?\s\*\*(Buy|Hold|Sell)", ai_analysis, re.IGNORECASE)
        if match:
            decision = match.group(1).strip()

        # ✅ Extract Buy Price Range (Find 'Recommended Buy Price' and next '$X - $Y' pattern)
        buy_match = re.search(r"Recommended Buy Price.*?(\$[\d,.]+\s*-\s*\$[\d,.]+)", ai_analysis, re.DOTALL)
        if buy_match:
            buy_price = buy_match.group(1).strip()
        else:
            print(f"⚠️ Buy Price Not Found: {ai_analysis}")
            buy_price = "N/A"

        # ✅ Extract Sell Price Range (Find 'Recommended Sell Price' and next '$X - $Y' pattern)
        sell_match = re.search(r"Recommended Sell Price.*?(\$[\d,.]+\s*-\s*\$[\d,.]+)", ai_analysis, re.DOTALL)
        if sell_match:
            sell_price = sell_match.group(1).strip()
        else:
            print(f"⚠️ Sell Price Not Found: {ai_analysis}")
            sell_price = "N/A"

        # ✅ Extract Technical Indicators Summary
        match = re.search(r"Technical Analysis Summary(.*?)(?=\n###|$)", ai_analysis, re.DOTALL)
        if match:
            technical_summary = match.group(1).strip()

        # ✅ Ensure numeric conversion where needed
        if buy_price != "N/A":
            buy_price = buy_price.replace("$", "").replace(",", "")
        if sell_price != "N/A":
            sell_price = sell_price.replace("$", "").replace(",", "")

        return decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis

    except Exception as e:
        print(f"❌ Error parsing AI analysis: {e}")
        return "N/A", "N/A", "N/A", "", ai_analysis  # Return default values if parsing fails

# ✅ Load AI Cache from Google Sheets
def load_ai_cache():
    """Load AI cache from Google Sheets into a dictionary."""
    cache_data = ai_cache_ws.get_all_values()
    cache_dict = {}

    if len(cache_data) > 1:  # Ensure data exists beyond headers
        headers = cache_data[0]  # First row as column names
        for row in cache_data[1:]:
            if len(row) >= 7:  # Ensure row has enough values
                ticker, cached_price, cached_rsi, cached_vwma, cached_sentiment, ai_analysis, timestamp = row

                # Convert numeric values
                cached_price = float(cached_price) if cached_price.replace(".", "", 1).isdigit() else "N/A"
                cached_rsi = float(cached_rsi) if cached_rsi.replace(".", "", 1).isdigit() else "N/A"
                cached_vwma = float(cached_vwma) if cached_vwma.replace(".", "", 1).isdigit() else "N/A"

                # Convert timestamp and check age
                cache_age_days = 0
                try:
                    last_updated = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    cache_age_days = (datetime.now() - last_updated).days
                except ValueError:
                    last_updated = "N/A"  # Handle parsing errors

                cache_dict[ticker] = {
                    "cached_price": cached_price,
                    "cached_rsi": cached_rsi,
                    "cached_vwma": cached_vwma,
                    "cached_sentiment": cached_sentiment,
                    "ai_analysis": ai_analysis,
                    "timestamp": timestamp,
                    "cache_age_days": cache_age_days
                }

    return cache_dict
ai_cache = load_ai_cache()  # ✅ Load cache once at the start
# ✅ Declare 'get_ai_analysis' function (Used but missing)
def get_ai_analysis(row_dict):
    """Fetch AI Analysis using GPT-4o"""
    prompt = f"""
    📈 **Stock Analysis & Investment Decision** 📈
    
    **Role:** You are a highly successful **stock analyst and portfolio manager**, specializing in identifying **high-growth, low-risk investments** with strong profit potential. Your vision in analyzing stocks ensures strategic and profitable investment decisions. 

    🔍 **Task:** Analyze the stock **{row_dict.get('Symbol', 'N/A')}** using **real-time, historical, and sentiment data**. If data is missing or labeled **N/A**, **search the web** for the most recent and relevant information.

    ---
    
    ## 🔹 **Stock Information & Market Data**
    - **Symbol**: {row_dict.get('Symbol', 'N/A')}
    - **Name**: {row_dict.get('Name', 'N/A')}
    - **Current Price**: {row_dict.get('Current Price', 'N/A')}  
    - **Market Cap**: {row_dict.get('Market Cap', 'N/A')}  
    - **P/E Ratio**: {row_dict.get('P/E', 'N/A')}  
    - **Yesterday’s Close**: {row_dict.get('Yesterday Close Price', 'N/A')}  
    - **1-Day Price Change (%)**: {row_dict.get('1 Day Price Change', 'N/A')}  
    - **1-Week Price Change (%)**: {row_dict.get('1 Week Price Change', 'N/A')}  
    - **1-Month Price Change (%)**: {row_dict.get('1 Month Price Change', 'N/A')}  
    - **Volume**: {row_dict.get('Volume', 'N/A')}  
    - **Industry**: {row_dict.get('Industry', 'N/A')}  

    ---
    
    ## 🔹 **Technical Indicators & Trends**
    - **RSI (Relative Strength Index)**: {row_dict.get('RSI', 'N/A')}  
    - **VWMA (Volume Weighted Moving Average)**: {row_dict.get('VWMA', 'N/A')}  
    - **EMA (Exponential Moving Average)**: {row_dict.get('EMA', 'N/A')}  
    - **ATR (Average True Range - Volatility Indicator)**: {row_dict.get('ATR', 'N/A')}  
    - **VWMA vs Current Price**: {row_dict.get('VWMA vs Current Price', 'N/A')}  

    ---
    
    ## 🔹 **Market Sentiment & News Analysis**
    - **Positive Rating (%)**: {row_dict.get('Positive Rating', 'N/A')}  
    - **Negative Rating (%)**: {row_dict.get('Negative Rating', 'N/A')}  
    - **Latest News Headlines:**  
      - {row_dict.get('News 1', 'N/A')}  
      - {row_dict.get('News 2', 'N/A')}  
      - {row_dict.get('News 3', 'N/A')}  
      - {row_dict.get('News 4', 'N/A')}  
      - {row_dict.get('News 5', 'N/A')}  
    ---

    ### 1️⃣ Recommendation: **[Buy/Hold/Sell]**
     - **Decision must be clear:** **BUY**, **HOLD**, or **SELL**.
    - Justify your decision based on **technical indicators, sentiment, valuation, and market conditions**.
    - Use **data-driven insights** to explain if this stock has **high growth potential** or **risks**.
    ---

    ### 2️⃣ Recommended Buy Price
    - **Buy Range:** **$[Predicted Min Buy Price] - $[Predicted Max Buy Price]**
    - Ensure the buy price is **below the current price**.
    - If the **current price is already below the buy range**, **adjust the calculations accordingly**.
    ---

    ### 3️⃣ Recommended Sell Price
    - **Sell Range:** **$[Predicted Min Sell Price] - $[Predicted Max Sell Price]**
    - **Use historical highs, resistance levels, and market trends to determine exit targets**.

    ### **4️⃣ Technical Analysis Summary**
    - Identify the most **relevant indicators impacting stock performance**.
    - Highlight any major **breakouts, trend reversals, or risk factors**.
    - Provide insights on whether this is a **strong technical setup** for a trade.

    ✅ **If data is missing, conduct a web search to retrieve accurate and updated values.**  
    ✅ **Your goal is to maximize profitability by providing an accurate, data-driven stock analysis.**  
    ✅ **Follow this exact response structure.**
    """

    print(f"🔹 Sending AI Request for {row_dict.get('Symbol', 'N/A')}...")

    response = client_ai.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": "You are a stock analyst providing precise buy/sell recommendations."},
                  {"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content  # ✅ Return AI analysis response
# ✅ Save AI Cache to Google Sheets
def save_ai_cache(ticker, current_price, rsi, vwma, sentiment, ai_analysis):
    """Update AI Cache row by row in Google Sheets."""

    # ✅ Fetch existing AI Cache data
    cache_data = ai_cache_ws.get_all_values()
    existing_rows = {row[0]: idx for idx, row in enumerate(cache_data) if len(row) > 0}  # {Ticker: RowIndex}

    # ✅ Convert values to string
    row_data = [
        ticker,
        str(current_price),
        str(rsi),
        str(vwma),
        sentiment,
        ai_analysis,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ]

    if ticker in existing_rows:
        row_index = existing_rows[ticker] + 1  # Google Sheets index is 1-based
        print(f"🔄 Updating AI Cache for {ticker} at row {row_index}")
        ai_cache_ws.update(f"A{row_index}:G{row_index}", [row_data])  # ✅ Update row
    else:
        print(f"➕ Adding new AI Cache entry for {ticker}")
        ai_cache_ws.append_row(row_data)  # ✅ Append new row if ticker not found




# ✅ 2.5% Variance Check Logic
def is_within_variance(cached_value, new_value, threshold=0.05):
    """Check if new_value is within ±2.5% of cached_value."""
    if cached_value == "N/A" or new_value == "N/A":
        return False  # If any value is missing, treat it as changed
    return (cached_value * (1 - threshold)) <= new_value <= (cached_value * (1 + threshold))



# 🔹 Process stocks and update Google Sheets

for i, row in enumerate(data[1:], start=2):
    row_dict = {headers[j]: row[j] if j < len(row) else "N/A" for j in range(len(headers))}
    ticker = row_dict.get('Symbol', 'N/A')

    current_price = float(row_dict.get("Current Price", "N/A")) if row_dict.get("Current Price", "N/A").replace(".", "", 1).isdigit() else "N/A"
    rsi = float(row_dict.get("RSI", "N/A")) if row_dict.get("RSI", "N/A").replace(".", "", 1).isdigit() else "N/A"
    vwma = float(row_dict.get("VWMA", "N/A")) if row_dict.get("VWMA", "N/A").replace(".", "", 1).isdigit() else "N/A"
    sentiment = row_dict.get("Sentiment Ratio", "N/A")

    # ✅ Retry logic to handle 429 rate limit errors
    retry = True
    while retry:
        try:
            # ✅ AI Call or Use Cache
            if ticker in ai_cache:
                cached_data = ai_cache[ticker]
                cache_age_days = cached_data["cache_age_days"]

                # ✅ Check if cache is still valid based on 2.5% variance and not older than 7 days
                if (
                    cache_age_days <= 7 and
                    is_within_variance(cached_data["cached_price"], current_price) and
                    is_within_variance(cached_data["cached_rsi"], rsi) and
                    is_within_variance(cached_data["cached_vwma"], vwma)
                ):
                    print(f"⚡ Using Cached AI Analysis for {ticker} (within 2.5% threshold & cache age {cache_age_days} days)")
                    ai_analysis = cached_data["ai_analysis"]
                else:
                    print(f"⚠️ Cache expired OR values changed beyond 2.5%, fetching new AI analysis for {ticker}...")
                    ai_analysis = get_ai_analysis(row_dict)  # ✅ Call AI
                    save_ai_cache(ticker, current_price, rsi, vwma, sentiment, ai_analysis)  # ✅ Save updated cache
            else:
                print(f"⚠️ No cached data found for {ticker}, fetching new AI analysis...")
                ai_analysis = get_ai_analysis(row_dict)
                save_ai_cache(ticker, current_price, rsi, vwma, sentiment, ai_analysis)

            # ✅ Parse AI Response into structured data
            decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis = parse_ai_analysis(ai_analysis)

            # ✅ Update Google Sheets with structured AI response
            top_picks_ws.update(f"C{i}:G{i}", [[decision, buy_price, sell_price, technical_summary, rest_of_ai_analysis]])
            time.sleep(1)  # ✅ Prevent hitting rate limits
            
            retry = False  # Exit retry loop if no exception occurs

        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"⚠️ Rate limit hit! Pausing for 60 seconds before switching API keys...")
                time.sleep(60)  # ✅ Wait for 60 seconds
                switch_api_key()  # ✅ Switch to another API key
                sheet = client.open("Stock Investment Analysis")  # Reconnect to the spreadsheet with the new client
                top_picks_ws = sheet.worksheet("Top Picks")
                ai_cache_ws = sheet.worksheet("AI_Cache")
            else:
                print(f"❌ Error updating Google Sheets: {e}")
                retry = False  # Exit loop for non-rate limit errors


def apply_decision_formatting():
    """Apply conditional formatting based on AI decision and reset row height with 429 error handling."""
    global top_picks_ws  # ✅ Declare as global to ensure it's accessible after API reconnection

    existing_data = fetch_existing_data()  # Ensure the latest data is fetched
    row_count = len(existing_data)

    # Define colors
    buy_format = CellFormat(backgroundColor=Color(0.8, 1.0, 0.8))  # Light Green (Buy)
    sell_format = CellFormat(backgroundColor=Color(1.0, 0.8, 0.8))  # Light Red (Sell)
    hold_format = CellFormat(backgroundColor=Color(1.0, 1.0, 0.8))  # Light Yellow (Hold)

    for i in range(2, row_count + 1):  # Start from row 2 (skip headers)
        retry = True
        while retry:
            try:
                cell_value = top_picks_ws.acell(f"C{i}").value  # Get value of Column C (Decision)
                
                if cell_value:
                    if "BUY" in cell_value.upper():
                        format_cell_range(top_picks_ws, f"C{i}", buy_format)
                    elif "SELL" in cell_value.upper():
                        format_cell_range(top_picks_ws, f"C{i}", sell_format)
                    elif "HOLD" in cell_value.upper():
                        format_cell_range(top_picks_ws, f"C{i}", hold_format)

                time.sleep(1)  # Prevent hitting rate limits
                retry = False  # Exit retry loop if no exception occurs

            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    print(f"⚠️ Rate limit hit while applying formatting! Pausing for 60 seconds...")
                    time.sleep(60)  # Wait for 60 seconds
                    switch_api_key()  # Switch to another API key
                    sheet = client.open("Stock Investment Analysis")  # Reconnect to the spreadsheet
                    top_picks_ws = sheet.worksheet("Top Picks")  # Rebind `top_picks_ws` after reconnection
                else:
                    print(f"❌ Error applying formatting: {e}")
                    retry = False  # Exit loop for non-rate limit errors

    print("✅ Conditional formatting successfully applied!")

apply_decision_formatting()  # ✅ Apply color formatting
