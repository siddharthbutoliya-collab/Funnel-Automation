# !pip install requests
# !pip install --upgrade gspread
# !pip install pandas
import gspread
from gspread_dataframe import set_with_dataframe
import requests
import pandas as pd
from google.colab import auth
import gspread
from google.auth import default
from oauth2client.client import GoogleCredentials
import numpy as np
from datetime import datetime
import os

sec = userdata.get('Prabhat_Secret_Key')
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)
res = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/session',
                    headers = {"Content-Type": "application/json"},
                    json =  {"username": 'prabhat.kumar@newtonschool.co',
                             "password": sec}
                   )
assert res.ok == True
token = res.json()['id']
print(token)

import time
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from zoneinfo import ZoneInfo

# -------------------- CONFIG --------------------
METABASE_HEADERS = {
    'Content-Type': 'application/json',
    'X-Metabase-Session': token  # make sure your token variable is defined
}

SHEET_KEY = '1QCyzrW-Jd5Ny43F7ck7Pk2kJa4nVf7a1KovPj3d8S4c'
SHEET1_NAME = "Helper StageChange Dump"
SHEET2_NAME = "Helper Call Dump"
SHEET3_NAME = 'Created on Leads'
SHEET_PIVOT = "New_DS_Summary"

# ------------------------------------------------

# ----------- Utility: Metabase fetch with retry ----------
def fetch_with_retry(url, headers, retries=3, delay=15):
    """Fetch data from Metabase with retry on failure."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, timeout=120)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...\n")
                time.sleep(delay)
            else:
                raise

# ----------- Utility: Google Sheets write with retry ----------
def update_with_retry(worksheet, df, retries=3, delay=20):
    """Write DataFrame to Google Sheet with retry on failure."""
    for attempt in range(1, retries + 1):
        try:
            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
            print(f"✅ Data written to sheet '{worksheet.title}' successfully.")
            return
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...\n")
                time.sleep(delay)
            else:
                raise

# ------------------------------------------------

# 1️⃣ Fetch data from Metabase
print("Fetching data from Metabase...")
Funnel = fetch_with_retry('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8484/query/json', METABASE_HEADERS)
Input = fetch_with_retry('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8495/query/json', METABASE_HEADERS)
Createdon = fetch_with_retry('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8606/query/json', METABASE_HEADERS)

# 2️⃣ Convert responses to DataFrames
df_Funnel = pd.DataFrame(Funnel.json())
df_Input = pd.DataFrame(Input.json())
df_Createon = pd.DataFrame(Createdon.json())

# 3️⃣ Select and reorder columns
df_Funnel = df_Funnel[
    [
        'lead_created_on', 'modified_on', 'prospect_email', 'prospect_stage',
        'mx_prospect_status', 'crm_user_role', 'sales_user_email', 'mx_utm_medium',
        'mx_utm_source', 'mx_lead_quality_grade', 'mx_lead_inherent_intent',
        'mx_priority_status', 'mx_organic_inbound', 'lead_last_call_status',
        'mx_city', 'event', 'current_stage', 'previous_stage', 'mx_identifer', 'mx_phoenix_identifer'
    ]
]

df_Input = df_Input[
    [
        'lead_created_on', 'modified_on', 'prospect_email', 'prospect_stage',
        'mx_prospect_status', 'crm_user_role', 'sales_user_email', 'mx_utm_medium',
        'mx_utm_source', 'mx_lead_quality_grade', 'mx_lead_inherent_intent',
        'mx_priority_status', 'mx_organic_inbound', 'lead_last_call_status',
        'mx_city', 'event', 'current_stage', 'previous_stage', 'mx_identifer', 'mx_phoenix_identifer',
        'call_type', 'duration'
    ]
]

df_Createon = df_Createon[
    [
        'lead_created_on', 'modified_on', 'prospect_email', 'prospect_stage',
        'mx_prospect_status', 'crm_user_role', 'sales_user_email', 'mx_utm_medium',
        'mx_utm_source', 'mx_lead_quality_grade', 'mx_lead_inherent_intent',
        'mx_priority_status', 'mx_organic_inbound', 'lead_last_call_status',
        'mx_city', 'event', 'current_stage', 'previous_stage', 'mx_identifer', 'mx_phoenix_identifer'
    ]
]

# 4️⃣ Connect to Google Sheets
print("Connecting to Google Sheets...")
gc =  os.getenv("GITHUB_RUN_ID")
sheet = gc.open_by_key(SHEET_KEY)

worksheet1 = sheet.worksheet(SHEET1_NAME)
worksheet2 = sheet.worksheet(SHEET2_NAME)
worksheet3 = sheet.worksheet(SHEET3_NAME)
worksheet_pivot = sheet.worksheet(SHEET_PIVOT)

# 5️⃣ Clear old data (with delay between operations)
print("Clearing old data...")
worksheet1.batch_clear(['A:T'])
time.sleep(5)
worksheet2.batch_clear(['A:X'])
time.sleep(5)
worksheet3.batch_clear(['A:T'])
time.sleep(5)

# 6️⃣ Write data with retry
print("Writing new data to sheets...")
update_with_retry(worksheet1, df_Funnel)
time.sleep(5)
update_with_retry(worksheet2, df_Input)
time.sleep(5)
update_with_retry(worksheet3, df_Createon)


# 7️⃣ Update timestamp in pivot sheet
current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
worksheet_pivot.update("B1", [[current_time]])
print(f"✅ Timestamp updated: {current_time}")

print("🎯 All tasks completed successfully!")

