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
# ENV / SECRETS (GITHUB)
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
# HELPERS (UNCHANGED)
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
# GOOGLE CREDS FROM MONGO (MATCHES YOUR DATA)
# DB: google_creds
# Collection: creds
# Field: content
# ==========================================================
client = MongoClient(REMOTE_MONGO_URI)

google_creds_col = client["google_creds"]["creds"]
google_creds_doc = google_creds_col.find_one({}, {"content": 1})

if not google_creds_doc or "content" not in google_creds_doc:
    raise Exception("Google credentials not found in MongoDB")

creds = Credentials.from_service_account_info(
    google_creds_doc["content"],
    scopes=SCOPES
)

gc = gspread.authorize(creds)
sheet = gc.open("Domain Stats")
domains_ws = sheet.worksheet("Domains")
domains_rows = domains_ws.get_all_records()

# Normalize sheet rows (UNCHANGED)
for r in domains_rows:
    r["Domain"] = (r.get("Domain") or "").strip()
    r["Database"] = (r.get("Database") or "").strip()
    r["Domain Type"] = (r.get("Domain Type") or "").strip()
    r["EmployerId"] = (str(r.get("EmployerId") or "")).strip()
    r["Collection"] = (r.get("Collection") or "").strip()

# ==========================================================
# MONGO COLLECTIONS (UNCHANGED)
# ==========================================================
domains_col = client["mongo_creds"]["creds"]
stats_col = client["daily_domain_stats"]["stats"]
email_col = client["daily_domain_stats"]["email"]

# ==========================================================
# DOMAIN DB CONNECT (UNCHANGED)
# ==========================================================
def connect_mongo(domain_record, db_name):
    c = MongoClient(domain_record["mongo_uri"], serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c[db_name]

# ==========================================================
# FETCH VIEWS (UNCHANGED – PIXEL MATCH)
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
                    {
                        "$arrayElemAt": [
                            {"$split": [{"$ifNull": ["$url", ""]}, "/"]},
                            2
                        ]
                    },
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
# DOMAIN WORKER (UNCHANGED)
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
# RUN STATS (UNCHANGED)
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
# AGGREGATION (UNCHANGED)
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
# HTML BUILDER (💯 IDENTICAL TO LOCAL)
# ==========================================================
def build_html(name, email):
    total_domains = len(domains)
    total_clicks = sum(d["clicks"] for d in domains.values())
    total_ips = sum(d["ips"] for d in domains.values())

    companies = {c for d in domains.values() for (c, _) in d["rows"].keys()}

    blocks = ""

    for domain, d in sorted(domains.items(), key=lambda x: x[1]["clicks"], reverse=True):
        domain_type = d.get("type", "") or ""
        table = ""

        if is_table_domain(domain_type):
            rows_html = ""
            row_items = sorted(d["rows"].items(), key=lambda kv: kv[1]["clicks"], reverse=True)

            for (company, end), s in row_items:
                if not end:
                    continue
                rows_html += f"""
  <tr>
    <td style="padding: 10px;">{company}</td>
    <td style="padding: 10px;">{end}</td>
    <td style="padding: 10px; text-align: right;">{s['clicks']}</td>
    <td style="padding: 10px; text-align: right;">{s['ips']}</td>
  </tr>
"""

            table = f"""
  <table style="width: 100%; border-collapse: collapse; margin-top: 18px; background: #fafbff; border-radius: 14px; overflow: hidden; border: 1px solid #e2e7ff;">
    <thead>
      <tr style="background: #eef2ff;">
        <th style="padding: 10px; font-size: 12px; text-align: left;">Company</th>
        <th style="padding: 10px; font-size: 12px; text-align: left;">End Url Domain</th>
        <th style="padding: 10px; font-size: 12px; text-align: right;">Clicks</th>
        <th style="padding: 10px; font-size: 12px; text-align: right;">Unique IPs</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
"""

            blocks += f"""
  <div style="padding: 26px; margin-bottom: 28px; border-radius: 24px; background: #ffffff; border: 1px solid #e4e6ef; box-shadow: 0 8px 24px rgba(99,102,241,0.14);">
    <h3 style="margin: 0 0 6px 0; font-size: 20px; color: #4f46e5; font-weight: 600;">{clean_domain(domain)}</h3>
    <div style="font-size: 14px; color: #374151; margin-bottom: 14px;">
      <strong>Domain Type:</strong>
      <span style="display: inline-block; padding: 3px 10px; border-radius: 999px; background: #eef2ff; color: #4338ca; font-size: 12px; font-weight: 600;">
        {domain_type}
      </span>
    </div>
    <div style="margin-top: 10px; font-size: 15px;">
      <strong>Total Clicks:</strong> {d['clicks']} &nbsp;&nbsp;
      <strong>Unique IPs:</strong> {d['ips']}
    </div>
{table}
  </div>
"""
        else:
            blocks += f"""
  <div style="padding: 26px; margin-bottom: 28px; border-radius: 24px; background: #ffffff; border: 1px solid #e4e6ef; box-shadow: 0 8px 24px rgba(99,102,241,0.14);">
    <h3 style="margin: 0 0 6px 0; font-size: 20px; color: #4f46e5; font-weight: 600;">{clean_domain(domain)}</h3>
    <div style="font-size: 14px; color: #374151; margin-bottom: 12px;">
      <strong>Domain Type:</strong> {domain_type}
    </div>
    <div style="font-size: 15px; line-height: 1.6;">
      <strong>Clicks:</strong> {d['clicks']}<br />
      <strong>Unique IPs:</strong> {d['ips']}
    </div>
  </div>
"""

    return f"""
  <p>&nbsp;</p>
  <div style="padding: 40px;">
    <div style="max-width: 840px; margin: 0 auto; background: linear-gradient(145deg,#ffffff,#f5f7ff); padding: 44px; border-radius: 28px; box-shadow: 0 14px 36px rgba(80,80,200,0.18); font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
      <h1 style="margin: 0 0 8px 0; font-size: 28px; font-weight: 500; color: #111827;">Hey {name},</h1>
      <h3 style="margin: 0 0 26px 0; font-size: 17px; font-weight: 600; color: #6b7280;">Here&rsquo;s your daily domain wise clicks report.</h3>

      <h1 style="margin: 0 0 10px 0; font-size: 30px; font-weight: 600; color: #4338ca;">Daily Domain Wise Clicks Report ⚡</h1>
      <p style="margin: 0 0 30px 0; color: #6b7280; font-size: 15px;">Date: <strong>{REPORT_DATE}</strong></p>

      <div style="background: linear-gradient(135deg,#e4e7ff,#d8dcff); padding: 22px 26px; border-radius: 22px; border-left: 6px solid #6366f1; margin-bottom: 36px;">
        <div style="font-size: 15px; color: #374151; line-height: 1.7;">
          <strong>🌐 Domains:</strong> {total_domains}<br />
          <strong>🏢 Companies:</strong> {len(companies)}<br />
          <strong>🖱 Total Clicks:</strong> {total_clicks:,}<br />
          <strong>🧍 Unique IPs:</strong> {total_ips:,}
        </div>
      </div>

{blocks}

      <div style="margin-top: 42px; text-align: center; color: #9ca3af; font-size: 13px;">
        Sent to <strong>{name}</strong> &middot; {email}<br />
        Generated automatically &middot; Daily Domain Analytics
      </div>
    </div>
  </div>
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
    msg.attach(MIMEText(build_html(r.get("Name", "There"), r.get("email", "")), "html"))
    server.send_message(msg)

server.quit()
print("🚀 Email sent successfully.")
