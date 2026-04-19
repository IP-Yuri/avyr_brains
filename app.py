"""
AVYR DIGITAL — Lead Engine (Pure Backend)
=========================================
Headless automation script for scheduled GitHub Actions.
"""

import os
from dotenv import load_dotenv
load_dotenv()
import re
import libsql_client
import time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SERPAPI_KEY = os.environ.get("SERPAPI_KEY")
SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

PAGESPEED_API_KEY = os.environ.get("PAGESPEED_API_KEY")
PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

MIN_RATING = 4.5
MIN_REVIEW_COUNT = 30
MAX_LEADS = 7
REQUEST_DELAY = 2
LCP_THRESHOLD = 4.0

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def init_db():
    url = os.environ.get("TURSO_DATABASE_URL")
    auth_token = os.environ.get("TURSO_AUTH_TOKEN")
    client = libsql_client.create_client_sync(url=url, auth_token=auth_token)

    _schema = """
        Business_Name TEXT,
        Address       TEXT,
        Rating        REAL,
        Reviews       INTEGER,
        Website       TEXT,
        LCP_Score     TEXT,
        Email         TEXT,
        Instagram_URL TEXT,
        Digital_Status TEXT,
        Date_Added    TEXT
    """

    client.execute(f"CREATE TABLE IF NOT EXISTS target_leads ({_schema})")
    client.execute(f"CREATE TABLE IF NOT EXISTS benchmark_leads ({_schema})")
    
    for table in ["target_leads", "benchmark_leads"]:
        for col in ["Email", "Instagram_URL", "Digital_Status"]:
            try:
                client.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
            except Exception:
                pass

    return client

def _parse_lcp_float(val: str) -> float | None:
    if not isinstance(val, str) or val.strip() in ["Failed", "N/A"]:
        return None
    try:
        return float(val.replace(" s", ""))
    except (ValueError, AttributeError):
        return None

def route_and_save(df: pd.DataFrame, client) -> None:
    df = df.copy()
    df["Date_Added"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_lcp_f"] = df["LCP_Score"].apply(_parse_lcp_float)

    targets = df[(df["_lcp_f"].isna()) | (df["_lcp_f"] > LCP_THRESHOLD)].drop(columns="_lcp_f")
    benchmarks = df[(df["_lcp_f"].notna()) & (df["_lcp_f"] <= LCP_THRESHOLD)].drop(columns="_lcp_f")

    def insert_df(table_name, target_df):
        if target_df.empty:
            return
        columns = ", ".join(target_df.columns)
        placeholders = ", ".join(["?"] * len(target_df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        statements = []
        for row in target_df.itertuples(index=False, name=None):
            args = []
            for val in row:
                if pd.isna(val):
                    args.append(None)
                elif type(val) in (int, float, str, bytes):
                    args.append(val)
                else:
                    try:
                        args.append(val.item())
                    except AttributeError:
                        args.append(str(val))
            statements.append(libsql_client.Statement(query, args))
        
        if statements:
            client.batch(statements)

    insert_df("target_leads", targets)
    insert_df("benchmark_leads", benchmarks)
    print(f"💾 Saved {len(targets)} targets and {len(benchmarks)} benchmarks to Turso.")

# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def collect_leads(query: str) -> list[dict]:
    params = {"engine": "google_maps", "q": query, "api_key": SERPAPI_KEY}
    try:
        resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"❌ SerpApi Error: {exc}")
        return []

    records = []
    for entry in data.get("local_results", []):
        try:
            records.append({
                "Business_Name": entry.get("title", ""),
                "Address": entry.get("address", ""),
                "Rating": entry.get("rating", None),
                "Reviews": entry.get("reviews", 0),
                "Website": entry.get("website", None),
            })
        except Exception:
            continue
    return records

def clean_and_filter(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["Website"] = df["Website"].fillna("").astype(str).str.strip()
    df["Website"] = df["Website"].replace(["None", "nan", "NaN"], "")
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    df["Reviews"] = pd.to_numeric(df["Reviews"], errors="coerce").fillna(0).astype(int)
    
    df = df[(df["Rating"] >= MIN_RATING) & (df["Reviews"] >= MIN_REVIEW_COUNT)]
    return df.head(MAX_LEADS).reset_index(drop=True)

def fetch_lcp(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    params = {"url": url, "strategy": "desktop", "key": PAGESPEED_API_KEY}
    try:
        resp = requests.get(PAGESPEED_ENDPOINT, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        lcp_ms = data.get("lighthouseResult", {}).get("audits", {}).get("largest-contentful-paint", {}).get("numericValue", None)
        return f"{lcp_ms / 1000:.2f} s" if lcp_ms else "Failed"
    except Exception:
        return "Failed"

def inspect_digital_flaws(url: str) -> dict:
    result = {"Digital_Status": "CUSTOM_MODERN", "Instagram_URL": None}
    if not url.startswith(("http://", "https://")): url = "https://" + url
    try:
        import warnings
        warnings.filterwarnings('ignore')
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, verify=False)
        resp.raise_for_status()
        html = resp.text.lower()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        if any(x in html for x in ["wix.com", "wixpress", "squarespace", "shopify", "cdn.shopify", "wp-content", "wordpress"]):
            result["Digital_Status"] = "TEMPLATE"
            
        if re.search(r"©\s*20(1[0-9]|20|21|22)", soup.get_text()) and result["Digital_Status"] != "TEMPLATE":
            result["Digital_Status"] = "OUTDATED"
                
        for a_tag in soup.find_all("a", href=True):
            if "instagram.com" in a_tag["href"] and not result["Instagram_URL"]:
                result["Instagram_URL"] = a_tag["href"]
    except Exception:
        result["Digital_Status"] = "Failed"
    return result

def extract_email(business_name: str, url: str) -> str | None:
    def _is_valid(e): return not e.lower().endswith(('.png', '.jpg', '.css', '.js')) and "example@" not in e.lower()
    def _find_in_html(html):
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                e = a["href"].replace("mailto:", "").split("?")[0].strip()
                if _is_valid(e): return e
        for e in re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', soup.get_text()):
            if _is_valid(e): return e
        return None

    if url:
        if not url.startswith(("http://", "https://")): url = "https://" + url
        try:
            import warnings
            warnings.filterwarnings('ignore')
            h = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=h, timeout=5, verify=False)
            if email := _find_in_html(resp.text): return email
            
            for path in ["/contact", "/contact-us"]:
                try:
                    c_resp = requests.get(url.rstrip("/") + path, headers=h, timeout=5, verify=False)
                    if email := _find_in_html(c_resp.text): return email
                except: pass
        except: pass
    return "Not Found"

def find_instagram_only(business_name: str) -> str | None:
    try:
        resp = requests.get(SERPAPI_ENDPOINT, params={"engine": "google", "q": f'site:instagram.com "{business_name}"', "api_key": SERPAPI_KEY}, timeout=10)
        if org := resp.json().get("organic_results", []): return org[0].get("link")
    except: pass
    return None

def run_audit(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    scores, digital_statuses, emails, ig_urls = [], [], [], []

    for idx, (url, b_name) in enumerate(zip(df["Website"], df["Business_Name"])):
        print(f"\n🔄 Auditing [{idx + 1}/{total}]: {b_name}")
        
        if url:
            score = fetch_lcp(url)
            print(f"   -> PageSpeed Score: {score}")
            flaw_data = inspect_digital_flaws(url)
            d_status, ig = flaw_data["Digital_Status"], flaw_data["Instagram_URL"]
        else:
            score, d_status = "N/A", "IG_ONLY"
            print(f"   -> Ghost Business (No Website)")
            ig = find_instagram_only(b_name)
            
        print(f"   -> Extracting contact info...")
        email = extract_email(b_name, url)
            
        scores.append(score)
        digital_statuses.append(d_status)
        emails.append(email)
        ig_urls.append(ig)
        
        if idx < total - 1: time.sleep(REQUEST_DELAY)

    df = df.copy()
    df["LCP_Score"] = scores
    df["Digital_Status"] = digital_statuses
    df["Email"] = emails
    df["Instagram_URL"] = ig_urls
    return df

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=======================================")
    print("⚡ AVYR DIGITAL LEAD ENGINE (HEADLESS) ⚡")
    print("=======================================\n")
    
    conn = init_db()
    print("✅ Database connected successfully.")

    daily_niches = [
        "Agence financière",       # Mon
        "Promoteur immobilier",    # Tue
        "Cabinet d'avocats",       # Wed
        "Clinique esthétique",     # Thu
        "Architecte d'intérieur"   # Fri
    ]
    
    today = datetime.today().weekday()
    niche = daily_niches[today] if today <= 4 else daily_niches[0]
    query = f"{niche} Casablanca, Morocco"
    
    print(f"🎯 Target Niche for Today: {query}\n")
    
    print("1️⃣ Collecting leads via SerpApi...")
    records = collect_leads(query)
    if not records:
        print("❌ No records found. Exiting.")
        return
        
    df = clean_and_filter(records)
    print(f"✅ Filtered down to {len(df)} highly qualified leads.\n")
    
    if df.empty:
        print("❌ No leads met the criteria. Exiting.")
        return

    print("2️⃣ Beginning deep digital audit...")
    df = run_audit(df)
    
    print("\n3️⃣ Routing and saving to Turso...")
    route_and_save(df, conn)
    
    print("\n🚀 SCRAPING COMPLETE. HANDING OFF TO AI BRAIN.")
    conn.close()

if __name__ == "__main__":
    main()
