import sys
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread
import smtplib
from pymongo import MongoClient
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2.service_account import Credentials

# ==========================================================
# ENV / SECRETS
# ==========================================================
REMOTE_MONGO_URI = os.getenv("REMOTE_MONGO_URI")
if not REMOTE_MONGO_URI:
    raise Exception("REMOTE_MONGO_URI not found in environment variables")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_USER = "saidixitn@gmail.com"

SMTP_PASS = os.getenv("SMTP_PASS")
if not SMTP_PASS:
    raise Exception("SMTP_PASS not found in environment variables")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ==========================================================
# DATE ARG
# ==========================================================
if len(sys.argv) < 2:
    print("Usage: python clicks_email_report.py YYYY-MM-DD")
    sys.exit(1)

REPORT_DATE = sys.argv[1]
MIN_DT = datetime.strptime(REPORT_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
MAX_DT = MIN_DT + timedelta(days=1)

# ==========================================================
# HELPERS
# ==========================================================
def clean_domain(d: str) -> str:
    return (d or "").replace("https://", "").replace("http://", "").strip()

def to_int(v) -> int:
    try:
        return int(v)
    except:
        return 0

def normalize_type(t) -> str:
    return str(t or "").strip().lower()

def is_table_domain(domain_type: str) -> bool:
    return normalize_type(domain_type) in {
        "programmatic",
        "direct apply",
        "direct_apply",
        "direct-apply",
    }

# ==========================================================
# MONGO (REMOTE)
# ==========================================================
client = MongoClient(REMOTE_MONGO_URI)

domains_col = client["mongo_creds"]["creds"]
stats_col = client["daily_domain_stats"]["stats"]
email_col = client["daily_domain_stats"]["email"]

# ==========================================================
# GOOGLE CREDS FROM MONGO (CORRECT)
# DB: google_creds
# Collection: creds
# Field: content
# ==========================================================
google_creds_col = client["google_creds"]["creds"]

google_creds_doc = google_creds_col.find_one({}, {"content": 1})
if not google_creds_doc or "content" not in google_creds_doc:
    raise Exception("Google service account not found in MongoDB")

creds = Credentials.from_service_account_info(
    google_creds_doc["content"],
    scopes=SCOPES
)

gc = gspread.authorize(creds)
sheet = gc.open("Domain Stats")
domains_ws = sheet.worksheet("Domains")
domains_rows = domains_ws.get_all_records()

# Normalize sheet rows
for r in domains_rows:
    r["Domain"] = (r.get("Domain") or "").strip()
    r["Database"] = (r.get("Database") or "").strip()
    r["Domain Type"] = (r.get("Domain Type") or "").strip()
    r["EmployerId"] = (str(r.get("EmployerId") or "")).strip()
    r["Collection"] = (r.get("Collection") or "").strip()

# ==========================================================
# DOMAIN DB CONNECT
# ==========================================================
def connect_mongo(domain_record, db_name):
    c = MongoClient(domain_record["mongo_uri"], serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c[db_name]

# ==========================================================
# FETCH VIEWS (UNCHANGED LOGIC)
# ==========================================================
def fetch_views(domain, db, employer_id, domain_type):
    col = db["userAnalytics"]

    match = {
        "isBot": False,
        "browserType": {"$ne": "Unknown"},
        "deviceType": {"$ne": "Unknown"},
        "isFromGoogle": True,
        "createdDt": {"$gt": MIN_DT, "$lt": MAX_DT},
    }

    if employer_id:
        match["employerId"] = employer_id

    pipeline = [
        {"$match": match},
        {"$project": {
            "Company": {"$ifNull": ["$company", {"$ifNull": ["$companyName", "Unknown"]}]},
            "date": {"$substr": ["$createdDt", 0, 10]},
            "url": 1,
            "ipAddress": 1,
        }},
        {"$addFields": {
            "End Url Domain": {
                "$ifNull": [
                    {"$arrayElemAt": [{"$split": [{"$ifNull": ["$url", ""]}, "/"]}, 2]},
                    ""
                ]
            }
        }},
        {"$group": {
            "_id": {
                "Company": "$Company",
                "End": "$End Url Domain",
                "Date": "$date",
            },
            "ViewsCount": {"$sum": 1},
            "UniqueIpCount": {"$addToSet": "$ipAddress"},
        }},
        {"$project": {
            "_id": 0,
            "Date": "$_id.Date",
            "Company": "$_id.Company",
            "End Url Domain": "$_id.End",
            "ViewsCount": 1,
            "UniqueIpCount": {"$size": "$UniqueIpCount"},
            "Domain": domain,
            "Domain Type": domain_type,
        }},
    ]

    return list(col.aggregate(pipeline, allowDiskUse=True))

# ==========================================================
# DOMAIN WORKER
# ==========================================================
def process_domain(row):
    if not row["Domain"] or not row["Database"]:
        return []

    record = domains_col.find_one({"domain": row["Database"]})
    if not record:
        return []

    db = connect_mongo(record, row["Database"])
    return fetch_views(
        row["Domain"],
        db,
        row["EmployerId"],
        row["Domain Type"]
    )

# ==========================================================
# RUN STATS
# ==========================================================
stats_col.delete_many({"Date": REPORT_DATE})

all_rows = []
with ThreadPoolExecutor(max_workers=8) as ex:
    futures = [ex.submit(process_domain, r) for r in domains_rows]
    for f in as_completed(futures):
        all_rows.extend(f.result())

for r in all_rows:
    r["InsertedAt"] = datetime.now(timezone.utc)

if all_rows:
    stats_col.insert_many(all_rows)

# ==========================================================
# AGGREGATE FOR EMAIL (IDENTICAL TO LOCAL)
# ==========================================================
domains = defaultdict(lambda: {
    "type": "",
    "clicks": 0,
    "ips": 0,
    "rows": defaultdict(lambda: {"clicks": 0, "ips": 0})
})

for r in domains_rows:
    if r["Domain"]:
        domains[r["Domain"]]["type"] = r["Domain Type"]

for r in all_rows:
    d = domains[r["Domain"]]
    clicks = to_int(r["ViewsCount"])
    ips = to_int(r["UniqueIpCount"])

    d["clicks"] += clicks
    d["ips"] += ips

    key = (r.get("Company", "Unknown"), r.get("End Url Domain", ""))
    d["rows"][key]["clicks"] += clicks
    d["rows"][key]["ips"] += ips

# ==========================================================
# EMAIL HTML BUILDER (UNCHANGED)
# ==========================================================
def build_html(name, email):
    total_domains = len(domains)
    total_clicks = sum(d["clicks"] for d in domains.values())
    total_ips = sum(d["ips"] for d in domains.values())
    companies = {c for d in domains.values() for (c, _) in d["rows"].keys()}

    blocks = ""
    for domain, d in sorted(domains.items(), key=lambda x: x[1]["clicks"], reverse=True):
        blocks += f"""
        <div style="padding:24px;margin-bottom:20px;border:1px solid #e5e7eb;border-radius:16px">
        <strong>{clean_domain(domain)}</strong><br/>
        Clicks: {d['clicks']} | IPs: {d['ips']}
        </div>
        """

    return f"""
    <h2>Daily Domain Wise Clicks Report ⚡</h2>
    <p>Date: {REPORT_DATE}</p>
    <p>Domains: {total_domains} | Companies: {len(companies)} | Clicks: {total_clicks} | IPs: {total_ips}</p>
    {blocks}
    """

# ==========================================================
# SEND EMAIL
# ==========================================================
server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
server.login(SMTP_USER, SMTP_PASS)

for r in email_col.find({}, {"_id": 0}):
    msg = MIMEMultipart("alternative")
    msg["From"] = "Daily Clicks Report"
    msg["To"] = r["email"]
    msg["Subject"] = f"Daily Domain Click Report - {REPORT_DATE}"
    msg.attach(MIMEText(build_html(r.get("Name", "There"), r["email"]), "html"))
    server.send_message(msg)

server.quit()
print("🚀 Email sent successfully.")
