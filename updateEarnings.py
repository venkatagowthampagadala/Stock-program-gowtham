# update_earnings.py – enrich ALL universe sheets with earnings data **without removing any existing logic**
# ----------------------------------------------------------------------------------
# Why this rewrite?
# 1. Extend the script from *Top Picks only* → *Large Cap, Mid Cap, Technology, SP Tracker, Top Picks*.
# 2. Keep the original header‑creation / column‑shift logic so first‑time execution still aligns
#    every sheet exactly as before.  (No user data lost.)
# 3. Factor the header check into a helper so it works for every sheet.
# 4. Preserve the original Yahoo‑Finance extraction logic – unchanged – only wrapped so it
#    can be reused for hundreds of symbols.

import os, json, time, re
from datetime import datetime
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# ── Google‑Sheets auth (unchanged logic, just turned into helper) ───────
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
CREDS_JSON_1 = json.loads(os.getenv("GOOGLE_CREDENTIALS_1"))
CREDS_JSON_2 = json.loads(os.getenv("GOOGLE_CREDENTIALS_2"))

def authenticate(creds_dict):
    """Preserves original credential routine; now callable for key rotation."""
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

client     = authenticate(CREDS_JSON_1)
active_key = 1

def switch_api_key():
    """Keeps original key‑switch idea; now reusable for any sheet."""
    global client, active_key
    active_key = 2 if active_key == 1 else 1
    client     = authenticate(CREDS_JSON_2 if active_key == 2 else CREDS_JSON_1)
    print(f"🔄 Switched to Google Sheets key {active_key}")

# ── target worksheets – ADDED four raw sheets but kept Top Picks last ──
sheet = client.open("Stock Investment Analysis")
sheets_to_update = {
    "Large Cap" : sheet.worksheet("Large Cap"),
    "Mid Cap"   : sheet.worksheet("Mid Cap"),
    "Technology": sheet.worksheet("Technology"),
    "SP Tracker": sheet.worksheet("SP Tracker"),
    "Top Picks" : sheet.worksheet("Top Picks"),
}

EARN_COLS = [
    "Earnings Date", "EPS", "Revenue Growth", "Debt-to-Equity", "Earnings Surprise",
]

# ── unchanged YF extraction logic (just pulled into function) ───────────

def fetch_earnings(symbol: str, retries: int = 3):
    while retries:
        try:
            info = yf.Ticker(symbol).info
            if not info:
                return ("N/A",) * 5
            # *Same* extraction as original script – nothing removed.
            if ts := info.get("earningsTimestamp"):
                earn_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            elif ts := info.get("earningsTimestampStart"):
                earn_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            else:
                earn_date = "N/A"
            eps_actual  = info.get("trailingEps", "N/A")
            rev_growth  = info.get("revenueGrowth", "N/A")
            if rev_growth != "N/A":
                rev_growth = round(rev_growth, 3)
            debt_equity = info.get("debtToEquity", info.get("totalDebt", "N/A"))
            eps_est     = info.get("epsCurrentYear", info.get("epsForward", 0))
            surprise_pct = round(((eps_actual - eps_est) / eps_est) * 100, 2) if eps_est else "N/A"
            return earn_date, eps_actual, rev_growth, debt_equity, surprise_pct
        except Exception as e:
            # original 429 logic preserved
            if "Too Many Requests" in str(e):
                print(f"⚠️ YF 429 {symbol}. Cooling 30s …")
                time.sleep(30); retries -= 1; continue
            print(f"❌ Yahoo error {symbol}: {e}")
            return ("N/A",) * 5
    return ("N/A",) * 5

# ── helper: ensure earnings columns exist (keeps original shift logic) ──

def ensure_earn_cols(ws):
    """If the five earnings columns are missing, insert them at C…G **exactly**
    like the original Top Picks logic did – nothing removed."""
    hdrs = ws.row_values(1)
    if hdrs[1] != "Symbol":
        print(f"⚠️ Unexpected header order in {ws.title}; skipping header insert")
        return False
    need_shift = any(col not in hdrs for col in EARN_COLS)
    if need_shift:
        # REASON: maintain identical structure across all sheets for downstream code.
        ws.insert_cols([EARN_COLS], 3)  # inserts as a block at column C
    return True

# ── MAIN LOOP (logic preserved, just wrapped for multiple sheets) ──────

for sheet_name, ws in sheets_to_update.items():
    print(f"\n🟢 Processing {sheet_name} …")
    if not ensure_earn_cols(ws):
        continue  # skip if header issue
    rows    = ws.get_all_values()
    headers = rows[0]
    sym_idx = headers.index("Symbol")

    for r_idx, row in enumerate(rows[1:], start=2):
        symbol = row[sym_idx].strip()
        if not symbol:
            continue
        earn_date, eps, growth, d2e, surprise = fetch_earnings(symbol)
        updates = [
            {"range": f"C{r_idx}", "values": [[earn_date]]},
            {"range": f"D{r_idx}", "values": [[eps]]},
            {"range": f"E{r_idx}", "values": [[growth]]},
            {"range": f"F{r_idx}", "values": [[d2e]]},
            {"range": f"G{r_idx}", "values": [[surprise]]},
        ]
        # original retry+rotation logic retained
        attempt = 0
        while attempt < 4:
            try:
                ws.batch_update(updates)
                break
            except APIError as e:
                if "429" in str(e):
                    attempt += 1
                    print(f"⚠️ Sheets 429 {symbol}. Wait 15 s then rotate key …")
                    time.sleep(15)
                    switch_api_key()
                    ws = client.open("Stock Investment Analysis").worksheet(sheet_name)
                else:
                    print(f"❌ Sheets error {symbol}: {e}")
                    break
        time.sleep(0.5)  # original small delay retained

print("✅ Earnings data refreshed across Large/Mid/Tech/SP Tracker/Top Picks")
