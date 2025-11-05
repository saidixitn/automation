import os
import sys
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, UpdateOne
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from concurrent.futures import ThreadPoolExecutor, as_completed
from pytz import timezone as pytz_timezone
from gspread.utils import rowcol_to_a1

# === ENV SETUP ===
REMOTE_MONGO_URI = os.getenv("REMOTE_MONGO_URI")
if not REMOTE_MONGO_URI:
    raise Exception("REMOTE_MONGO_URI not found in environment variables")

# === CONNECT TO REMOTE MONGO ===
client = MongoClient(REMOTE_MONGO_URI)

# --- Get credentials.json from google_creds.creds ---
google_creds_db = client["google_creds"]
google_creds_col = google_creds_db["creds"]
cred_doc = google_creds_col.find_one({"name": "google_credentials"})

if not cred_doc or "content" not in cred_doc:
    raise Exception("Google credentials not found in google_creds.creds")

# Write credentials.json content to a temporary file
CREDENTIALS_FILE = "/tmp/credentials.json"
with open(CREDENTIALS_FILE, "w") as f:
    f.write(cred_doc["content"])

# --- Authorize Google Sheets ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# --- Connect to mongo_creds for domain mappings ---
mongo_creds_db = client["mongo_creds"]
domains_col = mongo_creds_db["creds"]

# --- Stats collection lives here ---
banana_db = client["banana_analytics"]
stats_col = banana_db["stats"]

# === BANANA EMPLOYER MAPPINGS ===
BANANA_IDS = {
    "emp-gxc-banana":       ("banana", banana_db["banana"]),
    "emp-gxc-banana1":      ("banana1", banana_db["banana1"]),
    "emp-gxc-banana2":      ("banana2", banana_db["banana2"]),
    "emp-gxc-banana3":      ("banana3", banana_db["banana3"]),
    "emp-gxc-banana4":      ("banana4", banana_db["banana4"]),
    "emp-gxc-staffattract": ("staffattract", banana_db["staffattract"]),
}

# === URL patterns for fallback ===
BANANA_URL_PATTERNS = {
    "emp-gxc-banana": "utm_source=jobiakxmlus",
    "emp-gxc-banana1": "utm_source=jobiak1xmlus",
    "emp-gxc-banana2": "utm_source=jobiak2xmlus",
    "emp-gxc-banana3": "utm_source=jobiakdirectus",
    "emp-gxc-banana4": "utm_source=jobiakcompaniesus"
}

# === Helpers ===
def get_or_create_worksheet(sheet, name, rows=200, cols=30):
    try:
        return sheet.worksheet(name)
    except WorksheetNotFound:
        return sheet.add_worksheet(title=name, rows=rows, cols=cols)

def make_unique_columns(columns):
    seen, fixed = {}, []
    for col in columns:
        if col not in seen:
            seen[col] = 1
            fixed.append(col)
        else:
            seen[col] += 1
            fixed.append(f"{col}_{seen[col]}")
    return fixed

def _df_to_values(df: pd.DataFrame):
    safe = df.copy()
    return safe.where(pd.notnull(safe), "").values.tolist()

def smart_update(ws, new_df, key_col="Date"):
    """Safely replace rows for the same key (Date), append others."""
    try:
        old = pd.DataFrame(ws.get_all_records())
    except Exception:
        old = pd.DataFrame()
    new_df.columns = make_unique_columns(new_df.columns)
    if old.empty:
        ws.clear()
        ws.update([new_df.columns.tolist()] + _df_to_values(new_df))
        return
    all_cols = list(dict.fromkeys(list(old.columns) + list(new_df.columns)))
    old = old.reindex(columns=all_cols)
    new_df = new_df.reindex(columns=all_cols)
    keys = set(str(k) for k in new_df[key_col])
    old = old[~old[key_col].astype(str).isin(keys)]
    merged = pd.concat([old, new_df], ignore_index=True).sort_values(by=[key_col])
    ws.clear()
    ws.update([merged.columns.tolist()] + _df_to_values(merged))

def format_numeric_columns(ws, df):
    headers = list(df.columns)
    numeric_cols = []
    for i, col in enumerate(headers, start=1):
        name = col.lower().strip()
        if pd.api.types.is_numeric_dtype(df[col]) or name.endswith(("views", "clicks")):
            numeric_cols.append(i)
    for col_idx in numeric_cols:
        try:
            a1_range = f"{rowcol_to_a1(2, col_idx)}:{rowcol_to_a1(len(df)+1, col_idx)}"
            ws.format(a1_range, {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
        except Exception:
            pass

# === Core Logic ===
def fetch_views(domain_name, min_dt, max_dt):
    """Fetch total + unique views per domain using both sourceEmployerId and URL logic."""
    record = domains_col.find_one({"domain": domain_name})
    if not record or "mongo_uri" not in record:
        print(f"Missing mongo_uri for {domain_name}")
        return [], False
    try:
        client = MongoClient(record["mongo_uri"], serverSelectionTimeoutMS=5000)
        db = client[domain_name]
        col = db["userAnalytics"]

        base_filter = {
            "isBot": False,
            "browserType": {"$ne": "Unknown"},
            "deviceType": {"$ne": "Unknown"},
            "isFromGoogle": True,
            "createdDt": {"$gte": min_dt, "$lt": max_dt}
        }

        query1 = {**base_filter, "extraInfo.sourceEmployerId": {"$in": list(BANANA_IDS.keys())}}
        url_regex = "|".join(BANANA_URL_PATTERNS.values())
        query2 = {
            **base_filter,
            "extraInfo.url": {"$regex": url_regex, "$options": "i"},
            "extraInfo.sourceEmployerId": {"$not": {"$regex": "banana", "$options": "i"}}
        }

        def run_pipeline(match_query, collected_by, derive=False):
            pipeline = [
                {"$match": match_query},
                {"$addFields": {
                    "derivedEmployerId": {
                        "$cond": {
                            "if": {"$eq": [derive, True]},
                            "then": {
                                "$switch": {
                                    "branches": [
                                        {"case": {"$regexMatch": {"input": "$extraInfo.url", "regex": "utm_source=jobiakxmlus", "options": "i"}}, "then": "emp-gxc-banana"},
                                        {"case": {"$regexMatch": {"input": "$extraInfo.url", "regex": "utm_source=jobiak1xmlus", "options": "i"}}, "then": "emp-gxc-banana1"},
                                        {"case": {"$regexMatch": {"input": "$extraInfo.url", "regex": "utm_source=jobiak2xmlus", "options": "i"}}, "then": "emp-gxc-banana2"},
                                        {"case": {"$regexMatch": {"input": "$extraInfo.url", "regex": "utm_source=jobiakdirectus", "options": "i"}}, "then": "emp-gxc-banana3"},
                                        {"case": {"$regexMatch": {"input": "$extraInfo.url", "regex": "utm_source=jobiakcompaniesus", "options": "i"}}, "then": "emp-gxc-banana4"}
                                    ],
                                    "default": "unknown"
                                }
                            },
                            "else": "$extraInfo.sourceEmployerId"
                        }
                    }
                }},
                {"$group": {
                    "_id": {
                        "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdDt"}},
                        "company": "$company",
                        "domain": domain_name,
                        "sourceEmployerId": "$derivedEmployerId",
                        "employerId": "$employerId",
                        "cpc": "$extraInfo.cpc"
                    },
                    "uniqueIps": {"$addToSet": "$ipAddress"},
                    "total_clicks": {"$sum": 1}
                }},
                {"$project": {
                    "_id": 0,
                    "Date": "$_id.date",
                    "Company": "$_id.company",
                    "Domain": "$_id.domain",
                    "Source Employer": "$_id.sourceEmployerId",
                    "Employer": "$_id.employerId",
                    "cpc": "$_id.cpc",
                    "Unique Views": {"$size": "$uniqueIps"},
                    "Total Clicks": "$total_clicks",
                    "Collected By": collected_by
                }}
            ]
            return list(col.aggregate(pipeline, allowDiskUse=True))

        docs1 = run_pipeline(query1, "sourceEmployerId", derive=False)
        docs2 = run_pipeline(query2, "url", derive=True)
        combined = docs1 + docs2
        if not combined:
            print(f"{domain_name}: No records found")
            return [], False

        df = pd.DataFrame(combined)
        agg_df = (
            df.groupby(["Date", "Domain", "Company", "Source Employer", "Employer", "cpc"], as_index=False)
            .agg({
                "Unique Views": "sum",
                "Total Clicks": "sum",
                "Collected By": lambda x: ",".join(sorted(set(x)))
            })
        )
        print(f"{domain_name}: {len(agg_df)} rows aggregated")
        return agg_df.to_dict("records"), True
    except Exception as e:
        print(f"Error fetching {domain_name}: {e}")
        return [], False

# === Utility ===
def upsert_many(collection, records, key_fields):
    ops = []
    for rec in records:
        filt = {k: rec[k] for k in key_fields if k in rec}
        ops.append(UpdateOne(filt, {"$set": rec}, upsert=True))
    if ops:
        collection.bulk_write(ops, ordered=False)

# === Main processing ===
def process_date(date_str, sheet):
    min_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    max_dt = min_dt + timedelta(days=1)
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ist_now = datetime.now(pytz_timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Processing {date_str} ...")

    domains_df = pd.DataFrame(sheet.worksheet("Domains").get_all_records())
    domains_df.rename(columns=lambda x: x.strip().lower(), inplace=True)
    shared_df = pd.DataFrame(sheet.worksheet("Shared-Domains").get_all_records())
    shared_df.rename(columns=lambda x: x.strip().lower(), inplace=True)

    shared_map = {(r["domain"], r["company"]): r["analyst name"] for _, r in shared_df.iterrows()}
    domain_map = (domains_df[domains_df["shared"].astype(str).str.upper() == "FALSE"]
                  .set_index("domain")["analyst name"].to_dict())
    shared_domains = set(domains_df[domains_df["shared"].astype(str).str.upper() == "TRUE"]["domain"].tolist())

    all_results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_views, row["domain"], min_dt, max_dt): row["domain"] for _, row in domains_df.iterrows()}
        for fut in as_completed(futures):
            domain = futures[fut]
            results, _ = fut.result()
            if results:
                all_results.extend(results)
    if not all_results:
        print("No clicks found")
        return

    stats_df = pd.DataFrame(all_results)
    stats_df["Analyst"] = [
        domain_map.get(r["Domain"]) if r["Domain"] in domain_map
        else shared_map.get((r["Domain"], r["Company"])) if r["Domain"] in shared_domains
        else "Unknown"
        for _, r in stats_df.iterrows()
    ]
    stats_df["Report Run (UTC)"] = utc_now
    stats_df["Report Run (IST)"] = ist_now

    upsert_many(stats_col, stats_df.to_dict("records"), ["Date", "Domain", "Company", "cpc"])

    stats_ws = get_or_create_worksheet(sheet, "stats")
    ordered_cols = ["Date", "Analyst", "Company", "Domain", "Source Employer", "Employer",
                    "Unique Views", "Total Clicks", "cpc", "Collected By",
                    "Report Run (UTC)", "Report Run (IST)"]
    stats_df = stats_df.reindex(columns=ordered_cols)
    smart_update(stats_ws, stats_df)
    format_numeric_columns(stats_ws, stats_df)

    for emp_id, (sheet_name, col) in BANANA_IDS.items():
        subset = stats_df[stats_df["Source Employer"] == emp_id]
        if subset.empty:
            continue
        pivot = subset.pivot_table(index="Date", columns="Analyst", values="Unique Views", aggfunc="sum").fillna(0).astype(int)
        pivot.columns = make_unique_columns(pivot.columns)
        pivot.insert(0, "Metric", "Unique Views")
        pivot = pivot.reset_index()
        upsert_many(col, pivot.to_dict("records"), ["Date", "Metric"])
        ws = get_or_create_worksheet(sheet, sheet_name)
        smart_update(ws, pivot)
        format_numeric_columns(ws, pivot)
        print(f"Updated {sheet_name} with {len(pivot)} rows")

    print("Building CPC Stats ...")
    cpc_df = stats_df.copy()
    cpc_df = cpc_df[cpc_df["Source Employer"].isin(BANANA_IDS.keys())]
    cpc_df["Source Employer"] = cpc_df["Source Employer"].map(lambda x: BANANA_IDS[x][0])

    cpc_pivot = (
        cpc_df.pivot_table(index=["Date", "Analyst", "cpc"], columns="Source Employer",
                           values="Unique Views", aggfunc="sum")
        .fillna(0).astype(int).reset_index()
    )
    ordered_cols = ["Date", "Analyst", "cpc", "banana", "banana1", "banana2", "banana3", "banana4", "staffattract"]
    for col in ordered_cols:
        if col not in cpc_pivot.columns:
            cpc_pivot[col] = 0
    cpc_pivot = cpc_pivot[ordered_cols]

    cpc_ws = get_or_create_worksheet(sheet, "CPC Stats")
    smart_update(cpc_ws, cpc_pivot)
    format_numeric_columns(cpc_ws, cpc_pivot)
    print("CPC Stats sheet updated")
    print(f"Done for {date_str}")

# === Entry point ===
def main():
    if len(sys.argv) < 2:
        print("Usage: python banana_report.py YYYY-MM-DD [YYYY-MM-DD ...]")
        sys.exit(1)
    sheet = gc.open("Banana & Staffattract Clicks Report")
    for date_str in sys.argv[1:]:
        process_date(date_str, sheet)
    print("All reports updated successfully.")

if __name__ == "__main__":
    main()
