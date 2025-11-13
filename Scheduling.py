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
from concurrent.futures import ThreadPoolExecutor

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH --------------------
sec = os.getenv("PRABHAT_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URl = os.getenv("METABASE_URL")

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# Parse service account credentials
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

# -------------------- CONFIG --------------------
METABASE_HEADERS = {'Content-Type': 'application/json'}
res = requests.post(
    MB_URl,
    headers={"Content-Type": "application/json"},
    json={"username": User_name, "password": sec}
)
res.raise_for_status()
token = res.json()['id']
METABASE_HEADERS['X-Metabase-Session'] = token
# print(f"✅ Metabase session created: {token}")

SHEET_KEY = '1QCyzrW-Jd5Ny43F7ck7Pk2kJa4nVf7a1KovPj3d8S4c'
SHEET1_NAME = "Helper StageChange Dump"
SHEET2_NAME = "Helper Call Dump"
SHEET3_NAME = "Created on Leads"
SHEET_PIVOT = "New_DS_Summary"

# -------------------- UTILITIES --------------------
def fetch_with_retry(url, headers, retries=5, delay=15):
    """Fetch data from Metabase with retries."""
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

def safe_update_range(worksheet, df, data_range, retries=5, delay=20):
    """
    Updates Google Sheet safely:
    - Backs up current range data.
    - Tries to write new data.
    - Restores backup if the update fails.
    """
    print(f"🔄 Preparing to update {worksheet.title} ({data_range})")

    backup_data = worksheet.get(data_range)
    success = False

    for attempt in range(1, retries + 1):
        try:
            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True, resize=False)
            print(f"✅ Successfully updated {worksheet.title}")
            success = True
            break
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {worksheet.title}: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                print(f"❌ All attempts failed for {worksheet.title}. Restoring backup...")
                worksheet.update(data_range, backup_data)
                print(f"✅ Backup restored for {worksheet.title}")
                raise

    return success

# -------------------- MAIN LOGIC --------------------
print("Fetching Metabase data in parallel...")

urls = {
    "Funnel": "https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8484/query/json",
    "Input": "https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8495/query/json",
    "Createdon": "https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8606/query/json"
}

# Run all Metabase queries in parallel
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {name: executor.submit(fetch_with_retry, url, METABASE_HEADERS) for name, url in urls.items()}
    results = {name: f.result() for name, f in futures.items()}

df_Funnel = pd.DataFrame(results["Funnel"].json())
df_Input = pd.DataFrame(results["Input"].json())
df_Createon = pd.DataFrame(results["Createdon"].json())

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

# Skip clearing — directly update safely
print("Updating data safely...")
safe_update_range(ws1, df_Funnel, "A:T")
time.sleep(3)
safe_update_range(ws2, df_Input, "A:X")
time.sleep(3)
safe_update_range(ws3, df_Createon, "A:T")

# Update pivot timestamp
current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
ws_pivot.update("B1", [[current_time]])
print(f"✅ Updated timestamp: {current_time}")

# -------------------- TIMER SUMMARY --------------------
end_time = time.time()
elapsed_time = end_time - start_time
mins, secs = divmod(elapsed_time, 60)
print(f"⏱ Total time taken: {int(mins)}m {int(secs)}s")

print("🎯 All tasks completed successfully!")
