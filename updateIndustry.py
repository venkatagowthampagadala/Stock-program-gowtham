import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# 🔹 Google Sheets API Setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = r"C:\Users\venka\Downloads\Stock Python"  # Replace with your JSON key file path

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)

# Open the main spreadsheet and access the "Mid Cap" sheet
sheet = client.open("Stock Investment Analysis")  # Replace with your spreadsheet name
worksheet = sheet.worksheet("Large Cap")  # Replace with your sheet name

# Fetch tickers from Column A (skip the header row)
tickers = worksheet.col_values(1)[1:]  # Column A contains tickers
print(f"✅ Tickers fetched: {tickers}")

# Function to fetch industry for a given ticker
def fetch_industry(ticker):
    try:
        stock = yf.Ticker(ticker)
        industry = stock.info.get("industry", "N/A")
        return industry
    except Exception as e:
        print(f"❌ Error fetching industry for {ticker}: {e}")
        return "N/A"

# Loop through tickers, fetch industry, and update Column O
for idx, ticker in enumerate(tickers, start=2):  # Start from row 2
    print(f"Processing ticker: {ticker}")
    
    retry = True
    while retry:
        try:
            industry = fetch_industry(ticker)  # Fetch industry information
            
            # Update industry in Column O
            worksheet.update(f"O{idx}", [[industry]])  # Provide value as a list of lists
            print(f"✅ Updated {ticker} with industry: {industry}")
            
            retry = False  # Successfully updated, exit retry loop

        except gspread.exceptions.APIError as e:
            error_message = str(e)
            if "429" in error_message:  # Detect API Rate Limit error
                print(f"⚠️ Rate limit hit! Waiting 60 seconds before retrying...")
                time.sleep(60)  # Wait for 60 seconds
            else:
                print(f"❌ Error updating Google Sheet for {ticker}: {e}")
                retry = False  # Exit loop on other errors

    time.sleep(0.3)  # Avoid hitting API limits

print("✅ Industry information updated successfully in Column O!")
