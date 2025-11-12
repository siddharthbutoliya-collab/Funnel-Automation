import os
import time
import json
import requests
import pandas as pd
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# -------------------- ENV & AUTH --------------------
sec = os.getenv("PRABHAT_SECRET_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# Parse service account credentials
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

# -------------------- CONFIG --------------------
METABASE_HEADERS = {
    'Content-Type': 'application/json'
}

res = requests.post(
    'https://metabase-lierhfgoeiwhr.newtonschool.co/api/session',
    headers={"Content-Type": "application/json"},
    json={"username": "prabhat.kumar@newtonschool.co", "password": sec}
)
res.raise_for_status()
token = res.json()['id']
METABASE_HEADERS['X-Metabase-Session'] = token
print(f"✅ Metabase session created: {token}")

SHEET_KEY = '1QCyzrW-Jd5Ny43F7ck7Pk2kJa4nVf7a1KovPj3d8S4c'
SHEET1_NAME = "Helper StageChange Dump"
SHEET2_NAME = "Helper Call Dump"
SHEET3_NAME = 'Created on Leads'
SHEET_PIVOT = "New_DS_Summary"

# -------------------- UTILITIES --------------------
def fetch_with_retry(url, headers, retries=3, delay=15):
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, headers=headers, timeout=120)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise

def update_with_retry(worksheet, df, retries=3, delay=20):
    for attempt in range(1, retries + 1):
        try:
            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
            print(f"✅ Updated: {worksheet.title}")
            return
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise

# -------------------- MAIN LOGIC --------------------
print("Fetching Metabase data...")
Funnel = fetch_with_retry('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8484/query/json', METABASE_HEADERS)
Input = fetch_with_retry('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8495/query/json', METABASE_HEADERS)
Createdon = fetch_with_retry('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8606/query/json', METABASE_HEADERS)

df_Funnel = pd.DataFrame(Funnel.json())
df_Input = pd.DataFrame(Input.json())
df_Createon = pd.DataFrame(Createdon.json())

# Reorder and trim columns
common_cols = [
    'lead_created_on', 'modified_on', 'prospect_email', 'prospect_stage',
    'mx_prospect_status', 'crm_user_role', 'sales_user_email', 'mx_utm_medium',
    'mx_utm_source', 'mx_lead_quality_grade', 'mx_lead_inherent_intent',
    'mx_priority_status', 'mx_organic_inbound', 'lead_last_call_status',
    'mx_city', 'event', 'current_stage', 'previous_stage',
    'mx_identifer', 'mx_phoenix_identifer'
]
df_Funnel = df_Funnel[common_cols]
df_Input = df_Input[common_cols + ['call_type', 'duration']]
df_Createon = df_Createon[common_cols]

print("Connecting to Google Sheets...")
sheet = gc.open_by_key(SHEET_KEY)
ws1, ws2, ws3, ws_pivot = [
    sheet.worksheet(name) for name in [SHEET1_NAME, SHEET2_NAME, SHEET3_NAME, SHEET_PIVOT]
]

print("Clearing old data...")
for ws, rng in [(ws1, 'A:T'), (ws2, 'A:X'), (ws3, 'A:T')]:
    ws.batch_clear([rng])
    time.sleep(5)

print("Writing new data...")
update_with_retry(ws1, df_Funnel)
time.sleep(5)
update_with_retry(ws2, df_Input)
time.sleep(5)
update_with_retry(ws3, df_Createon)

current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
ws_pivot.update("B1", [[current_time]])
print(f"✅ Updated timestamp: {current_time}")
print("🎯 All tasks completed successfully!")
