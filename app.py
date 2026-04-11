"""
AVYR DIGITAL — Lead Engine
===========================
A dark-themed Streamlit app that automates lead generation and digital
auditing for a premium web agency targeting local businesses in Casablanca.

Run:  streamlit run app.py
"""

import os
from dotenv import load_dotenv
load_dotenv()
import re
import sqlite3
import time
from datetime import datetime
from urllib.parse import urlparse

import io

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

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

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avyr_leads.db")


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    """
    Open (or create) the local SQLite database and ensure the
    target_leads and benchmark_leads tables exist.

    Returns
    -------
    sqlite3.Connection
    """
    conn = sqlite3.connect(DB_PATH)

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

    conn.execute(f"CREATE TABLE IF NOT EXISTS target_leads ({_schema})")
    conn.execute(f"CREATE TABLE IF NOT EXISTS benchmark_leads ({_schema})")
    
    # Gracefully add new columns to existing tables
    for table in ["target_leads", "benchmark_leads"]:
        for col in ["Email", "Instagram_URL", "Digital_Status"]:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass # Column already exists

    conn.commit()
    return conn


def _parse_lcp_float(val: str) -> float | None:
    """Safely convert an LCP string like '5.34 s' to a float, or None."""
    if not isinstance(val, str) or val.strip() in ["Failed", "N/A"]:
        return None
    try:
        return float(val.replace(" s", ""))
    except (ValueError, AttributeError):
        return None


def route_and_save(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """
    Add a timestamp, then split the DataFrame into two tables:

    * **target_leads** — LCP > 4.0 s *or* "Failed"
    * **benchmark_leads** — LCP ≤ 4.0 s (valid numbers only)
    """
    df = df.copy()
    df["Date_Added"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Temporary float column for safe comparison
    df["_lcp_f"] = df["LCP_Score"].apply(_parse_lcp_float)

    targets = df[(df["_lcp_f"].isna()) | (df["_lcp_f"] > LCP_THRESHOLD)].drop(columns="_lcp_f")
    benchmarks = df[(df["_lcp_f"].notna()) & (df["_lcp_f"] <= LCP_THRESHOLD)].drop(columns="_lcp_f")

    if not targets.empty:
        targets.to_sql("target_leads", conn, if_exists="append", index=False)
    if not benchmarks.empty:
        benchmarks.to_sql("benchmark_leads", conn, if_exists="append", index=False)
    conn.commit()


def load_leads(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    """Return every row from the given table as a DataFrame."""
    return pd.read_sql(f"SELECT * FROM {table} ORDER BY Date_Added DESC", conn)


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def collect_leads(query: str) -> list[dict]:
    """
    Query SerpApi Google Maps and return a list of business records.

    Parameters
    ----------
    query : str
        Full search query including location suffix.

    Returns
    -------
    list[dict]
        Business records with keys:
        Business_Name, Address, Rating, Reviews, Website
    """
    params = {
        "engine": "google_maps",
        "q": query,
        "api_key": SERPAPI_KEY,
    }

    try:
        resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        st.error(f"SerpApi request failed: {exc}")
        return []

    local_results = data.get("local_results", [])
    if not local_results:
        st.warning("No results returned from SerpApi for this query.")
        return []

    records = []
    for entry in local_results:
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
    """
    Load records into a DataFrame, format websites,
    apply rating/review thresholds, and cap at MAX_LEADS rows.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with at most MAX_LEADS rows.
    """
    df = pd.DataFrame(records)

    # Format empty websites to keep them for Ghost Hunter
    df["Website"] = df["Website"].fillna("").astype(str).str.strip()
    df["Website"] = df["Website"].replace(["None", "nan", "NaN"], "")

    # Ensure numeric types
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    df["Reviews"] = pd.to_numeric(df["Reviews"], errors="coerce").fillna(0).astype(int)

    # Apply thresholds
    df = df[(df["Rating"] >= MIN_RATING) & (df["Reviews"] >= MIN_REVIEW_COUNT)]

    # Cap at 5 leads
    df = df.head(MAX_LEADS)
    df.reset_index(drop=True, inplace=True)
    return df


def fetch_lcp(url: str) -> str:
    """
    Query Google PageSpeed Insights (desktop) for a single URL.

    Returns
    -------
    str
        LCP in seconds (e.g. "5.34 s") or "Failed".
    """
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    params = {
        "url": url,
        "strategy": "desktop",
        "key": PAGESPEED_API_KEY,
    }

    try:
        resp = requests.get(PAGESPEED_ENDPOINT, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        lcp_ms = (
            data
            .get("lighthouseResult", {})
            .get("audits", {})
            .get("largest-contentful-paint", {})
            .get("numericValue", None)
        )

        if lcp_ms is not None:
            return f"{lcp_ms / 1000:.2f} s"
        return "Failed"

    except Exception:
        return "Failed"


def inspect_digital_flaws(url: str) -> dict:
    """Parse website for template footprints, old copyright, and IG."""
    result = {"Digital_Status": "CUSTOM_MODERN", "Instagram_URL": None}
    
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
        
    try:
        # We must disable warnings for unverified HTTPS if needed, 
        # but better to just use standard requests and catch exceptions.
        import warnings
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=10, verify=False)
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        
        # 1. Inspect Footprints
        html_lower = html.lower()
        if "wix.com" in html_lower or "wixpress" in html_lower:
            result["Digital_Status"] = "TEMPLATE"
        elif "squarespace" in html_lower:
            result["Digital_Status"] = "TEMPLATE"
        elif "shopify" in html_lower or "cdn.shopify" in html_lower:
            result["Digital_Status"] = "TEMPLATE"
        elif "wp-content" in html_lower or "wordpress" in html_lower:
            result["Digital_Status"] = "TEMPLATE"
            
        # 2. Check outdated copyright
        text = soup.get_text()
        if re.search(r"©\s*20(1[0-9]|20|21|22)", text) or re.search(r"copyright\s*20(1[0-9]|20|21|22)", text, re.IGNORECASE):
            if result["Digital_Status"] != "TEMPLATE":
                result["Digital_Status"] = "OUTDATED"
                
        # 3. Extract IG URL
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "instagram.com" in href:
                if not result["Instagram_URL"]:
                    result["Instagram_URL"] = href
                    
    except Exception:
        result["Digital_Status"] = "Failed"
        
    return result

def extract_email(business_name: str, url: str) -> str | None:
    """Two-layer email extraction (BeautifulSoup + DDGS)."""
    def _is_valid(email_str):
        email_str = email_str.lower()
        if email_str.endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp', '.css', '.js')):
            return False
        if "example@" in email_str or "email@" in email_str or "domain" in email_str:
            return False
        return True

    def _find_in_html(html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if _is_valid(email):
                    return email
        
        text = soup.get_text()
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
        for e in emails:
            if _is_valid(e):
                return e
        return None

    # Layer 1: Local Website Scraping
    if url:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
            
        import warnings
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            resp = requests.get(url, headers=headers, timeout=5, verify=False)
            email = _find_in_html(resp.text)
            if email:
                return email
                
            # Fallback to standard contact pages
            for path in ["/contact", "/contact-us"]:
                c_url = url.rstrip("/") + path
                try:
                    c_resp = requests.get(c_url, headers=headers, timeout=5, verify=False)
                    email = _find_in_html(c_resp.text)
                    if email:
                        return email
                except Exception:
                    pass
        except Exception:
            pass
            
    # Layer 2: OSINT DuckDuckGo Fallback
    query = f'"{business_name}" "Casablanca" email OR contact "@"'
    try:
        from duckduckgo_search import DDGS
        results = DDGS().text(query, max_results=5)
        for r in results:
            text = r.get("body", "") + " " + r.get("title", "")
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            for e in emails:
                if _is_valid(e):
                    return e
    except Exception:
        pass
        
    return "Not Found"

def find_instagram_only(business_name: str) -> str | None:
    """Use SerpApi to find the Instagram page for ghost businesses."""
    query = f'site:instagram.com "{business_name}"'
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
    }
    
    try:
        resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        org_results = data.get("organic_results", [])
        if org_results:
            return org_results[0].get("link")
    except Exception:
        pass
    return None


def run_audit(df: pd.DataFrame, progress_bar, status_text) -> pd.DataFrame:
    """
    Iterate through leads, perform PageSpeed audit, inspect digital flaws, 
    and identify ghost businesses. Updates the Streamlit progress bar.
    """
    total = len(df)
    scores: list[str] = []
    digital_statuses: list[str] = []
    emails: list[str | None] = []
    ig_urls: list[str | None] = []

    for idx, (url, b_name) in enumerate(zip(df["Website"], df["Business_Name"])):
        display_url = url if url else "No Website"
        status_text.text(f"Auditing [{idx + 1}/{total}]: {b_name} ({display_url})")
        
        if url:
            # Full website audit
            score = fetch_lcp(url)
            flaw_data = inspect_digital_flaws(url)
            d_status = flaw_data["Digital_Status"]
            ig = flaw_data["Instagram_URL"]
        else:
            # Ghost Hunter: No website scenario
            score = "N/A"
            d_status = "IG_ONLY"
            ig = find_instagram_only(b_name)
            
        status_text.text(f"Extracting contact info for {b_name}...")
        email = extract_email(b_name, url)
            
        scores.append(score)
        digital_statuses.append(d_status)
        emails.append(email)
        ig_urls.append(ig)
        
        progress_bar.progress((idx + 1) / total)

        # Rate-limit guard
        if idx < total - 1:
            time.sleep(REQUEST_DELAY)

    df = df.copy()
    df["LCP_Score"] = scores
    df["Digital_Status"] = digital_statuses
    df["Email"] = emails
    df["Instagram_URL"] = ig_urls
    return df


def generate_audit_chart(
    target_name: str,
    target_lcp: float,
    comp_names: list[str],
    comp_scores: list[float],
) -> plt.Figure:
    """
    Generate a dark-themed horizontal bar chart comparing a target
    business's LCP against up to three competitors.

    Parameters
    ----------
    target_name : str
        Name of the target (slowest) business.
    target_lcp : float
        LCP score of the target (in seconds).
    comp_names : list[str]
        Names of the competitor businesses (1-3 items).
    comp_scores : list[float]
        LCP scores of the competitor businesses (same length as comp_names).

    Returns
    -------
    matplotlib.figure.Figure
    """
    # Build bars bottom-to-top: competitors (reversed) then target on top
    grey_palette = ["#F5F5F5", "#CCCCCC", "#A3A3A3"]

    labels = list(reversed(comp_names)) + [target_name]
    values = list(reversed(comp_scores)) + [target_lcp]
    colors = list(reversed(grey_palette[: len(comp_names)])) + ["#D93838"]

    bar_count = len(labels)
    fig_height = max(2.5, bar_count * 1.0)

    fig, ax = plt.subplots(figsize=(10, fig_height), facecolor="#0A0A0A")
    ax.set_facecolor("#0A0A0A")

    bars = ax.barh(labels, values, color=colors, height=0.55, edgecolor="none")

    # Remove all spines except left
    for spine in ["top", "right", "bottom"]:
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#2A2A2A")

    # Hide x-axis
    ax.xaxis.set_visible(False)

    # Y-axis styling
    ax.tick_params(axis="y", colors="#FFFFFF", length=0)
    for label in ax.get_yticklabels():
        label.set_fontfamily("sans-serif")
        label.set_fontsize(11)
        label.set_fontweight("500")

    # Data labels at the end of each bar
    max_val = max(values)
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + max_val * 0.02,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}s",
            va="center",
            ha="left",
            color="#FFFFFF",
            fontsize=11,
            fontweight="bold",
            fontfamily="sans-serif",
        )

    # Add breathing room on the right for labels
    ax.set_xlim(0, max_val * 1.2)

    fig.tight_layout()
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════════════════

def inject_custom_css():
    """Inject dark-themed custom CSS for a premium, minimalist look."""
    st.markdown(
        """
        <style>
        /* ── Global dark overrides ────────────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        html, body, [data-testid="stAppViewContainer"],
        [data-testid="stHeader"], [data-testid="stToolbar"] {
            background-color: #0a0a0f !important;
            color: #e0e0e8 !important;
            font-family: 'Inter', sans-serif !important;
        }

        [data-testid="stSidebar"] {
            background-color: #0e0e15 !important;
        }

        /* ── Brand header ─────────────────────────────────────── */
        .brand-header {
            text-align: center;
            padding: 2.5rem 0 1rem;
        }
        .brand-header h1 {
            font-family: 'Inter', sans-serif;
            font-weight: 700;
            font-size: 2rem;
            letter-spacing: 0.25em;
            color: #ffffff;
            margin: 0;
        }
        .brand-header .accent {
            color: #6c63ff;
        }
        .brand-header p {
            font-weight: 300;
            font-size: 0.85rem;
            letter-spacing: 0.15em;
            color: #6b6b80;
            margin-top: 0.4rem;
        }

        /* ── Divider ──────────────────────────────────────────── */
        .divider {
            height: 1px;
            background: linear-gradient(90deg, transparent, #6c63ff40, transparent);
            margin: 1.5rem 0 2rem;
        }

        /* ── Input field ──────────────────────────────────────── */
        [data-testid="stTextInput"] input {
            background-color: #12121c !important;
            border: 1px solid #1e1e30 !important;
            border-radius: 8px !important;
            color: #e0e0e8 !important;
            font-family: 'Inter', sans-serif !important;
            padding: 0.75rem 1rem !important;
            font-size: 0.95rem !important;
            transition: border-color 0.2s ease;
        }
        [data-testid="stTextInput"] input:focus {
            border-color: #6c63ff !important;
            box-shadow: 0 0 0 2px #6c63ff20 !important;
        }
        [data-testid="stTextInput"] label {
            color: #8888a0 !important;
            font-weight: 400 !important;
            font-size: 0.82rem !important;
            letter-spacing: 0.06em !important;
        }

        /* ── Buttons ──────────────────────────────────────────── */
        [data-testid="stButton"] > button {
            background: linear-gradient(135deg, #6c63ff, #4e47c9) !important;
            color: #ffffff !important;
            border: none !important;
            border-radius: 8px !important;
            padding: 0.65rem 2rem !important;
            font-family: 'Inter', sans-serif !important;
            font-weight: 600 !important;
            font-size: 0.88rem !important;
            letter-spacing: 0.1em !important;
            cursor: pointer !important;
            transition: all 0.25s ease !important;
            width: 100% !important;
        }
        [data-testid="stButton"] > button:hover {
            transform: translateY(-1px) !important;
            box-shadow: 0 6px 24px #6c63ff30 !important;
        }
        [data-testid="stButton"] > button:active {
            transform: translateY(0) !important;
        }

        /* ── Download button ──────────────────────────────────── */
        [data-testid="stDownloadButton"] > button {
            background: transparent !important;
            color: #6c63ff !important;
            border: 1px solid #6c63ff50 !important;
            border-radius: 8px !important;
            font-family: 'Inter', sans-serif !important;
            font-weight: 500 !important;
            letter-spacing: 0.08em !important;
            transition: all 0.25s ease !important;
            width: 100% !important;
        }
        [data-testid="stDownloadButton"] > button:hover {
            background: #6c63ff15 !important;
            border-color: #6c63ff !important;
        }

        /* ── DataFrame ────────────────────────────────────────── */
        [data-testid="stDataFrame"] {
            border: 1px solid #1e1e30 !important;
            border-radius: 10px !important;
            overflow: hidden !important;
        }

        /* ── Results card ─────────────────────────────────────── */
        .result-card {
            background: #12121c;
            border: 1px solid #1e1e30;
            border-left: 3px solid #6c63ff;
            border-radius: 10px;
            padding: 1.25rem 1.5rem;
            margin-top: 1.5rem;
        }
        .result-card h3 {
            font-size: 0.78rem;
            letter-spacing: 0.18em;
            color: #6c63ff;
            font-weight: 600;
            margin: 0 0 0.6rem;
        }
        .result-card p {
            font-size: 0.82rem;
            color: #6b6b80;
            margin: 0;
        }

        /* ── Metric pills ─────────────────────────────────────── */
        .metric-row {
            display: flex;
            gap: 1rem;
            margin-top: 1.5rem;
            margin-bottom: 0.5rem;
        }
        .metric-pill {
            flex: 1;
            background: #12121c;
            border: 1px solid #1e1e30;
            border-radius: 10px;
            padding: 1rem 1.25rem;
            text-align: center;
        }
        .metric-pill .label {
            font-size: 0.68rem;
            letter-spacing: 0.15em;
            color: #6b6b80;
            margin-bottom: 0.3rem;
        }
        .metric-pill .value {
            font-size: 1.4rem;
            font-weight: 700;
            color: #ffffff;
        }
        .metric-pill .value.accent {
            color: #6c63ff;
        }

        /* ── Spinner text ─────────────────────────────────────── */
        [data-testid="stSpinner"] > div {
            color: #8888a0 !important;
        }

        /* ── Hide Streamlit menu & footer ─────────────────────── */
        #MainMenu, footer, [data-testid="stToolbar"] {
            display: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header():
    """Render the branded header."""
    st.markdown(
        """
        <div class="brand-header">
            <h1>AVYR <span class="accent">DIGITAL</span></h1>
            <p>LEAD ENGINE</p>
        </div>
        <div class="divider"></div>
        """,
        unsafe_allow_html=True,
    )


def render_metrics(total_collected: int, qualified: int, audited: int):
    """Show metric pills summarising the pipeline run."""
    st.markdown(
        f"""
        <div class="metric-row">
            <div class="metric-pill">
                <div class="label">COLLECTED</div>
                <div class="value">{total_collected}</div>
            </div>
            <div class="metric-pill">
                <div class="label">QUALIFIED</div>
                <div class="value accent">{qualified}</div>
            </div>
            <div class="metric-pill">
                <div class="label">AUDITED</div>
                <div class="value">{audited}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main():
    """Streamlit entry point."""
    st.set_page_config(
        page_title="AVYR DIGITAL — Lead Engine",
        page_icon="⚡",
        layout="centered",
    )

    inject_custom_css()
    render_header()

    # ── Database connection ───────────────────────────────────────────────────
    conn = init_db()

    # ── Sidebar: Lead Database ────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(
            '<h2 style="letter-spacing:0.15em;font-size:1rem;color:#6c63ff;">'
            'LEAD DATABASE</h2>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<p style="font-size:0.78rem;color:#6b6b80;margin-bottom:1rem;">'
            'Consultez vos leads classés par performance.</p>',
            unsafe_allow_html=True,
        )

        table_choice = st.selectbox(
            "Vue",
            options=[
                "🎯 Cibles à Contacter (LCP > 4s / Failed)",
                "⚡ Références Rapides (LCP ≤ 4s)",
            ],
            key="db_table_selector",
        )

        _table_map = {
            "🎯 Cibles à Contacter (LCP > 4s / Failed)": "target_leads",
            "⚡ Références Rapides (LCP ≤ 4s)": "benchmark_leads",
        }

        if st.button("📂  Charger les Leads"):
            selected_table = _table_map[table_choice]
            history_df = load_leads(conn, selected_table)
            if history_df.empty:
                st.info("Aucun lead dans cette table. Lancez une recherche.")
            else:
                st.success(f"{len(history_df)} leads dans {selected_table}")
                st.dataframe(
                    history_df,
                    use_container_width=True,
                    hide_index=True,
                )

    # ── Input ─────────────────────────────────────────────────────────────────
    niche = st.text_input(
        "Enter target niche (e.g., Promoteur immobilier)",
        placeholder="Architecte d'intérieur",
    )

    generate = st.button("⚡  Generate Leads")

    # ── Pipeline ──────────────────────────────────────────────────────────────
    if generate:
        if not niche.strip():
            st.warning("Please enter a target niche to begin.")
            return

        # Lock to Casablanca
        query = f"{niche.strip()} Casablanca, Morocco"

        with st.spinner("Auditing digital architecture..."):
            # Step 1 — Collect
            records = collect_leads(query)
            if not records:
                return
            total_collected = len(records)

            # Step 2 — Clean & filter (capped at 5)
            df = clean_and_filter(records)
            if df.empty:
                st.warning(
                    "No businesses matched the quality thresholds "
                    f"(Rating ≥ {MIN_RATING}, Reviews ≥ {MIN_REVIEW_COUNT})."
                )
                return
            qualified_count = len(df)

        # Step 3 — PageSpeed audit (with visible progress)
        st.markdown(
            '<div class="result-card"><h3>PAGESPEED AUDIT</h3>'
            "<p>Scanning website performance…</p></div>",
            unsafe_allow_html=True,
        )
        progress_bar = st.progress(0)
        status_text = st.empty()

        df = run_audit(df, progress_bar, status_text)

        # Clean up progress widgets
        progress_bar.empty()
        status_text.empty()

        # ── Route leads into target_leads / benchmark_leads ────────────
        route_and_save(df, conn)

        # ── Output: show complete DataFrame ───────────────────────────────
        render_metrics(total_collected, qualified_count, len(df))

        st.markdown(
            '<div class="result-card"><h3>ALL AUDITED LEADS</h3>'
            f"<p>Showing {len(df)} leads for: <strong>{query}</strong></p></div>",
            unsafe_allow_html=True,
        )

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

        # CSV download
        csv = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="⬇  Download avyr_leads.csv",
            data=csv,
            file_name="avyr_leads.csv",
            mime="text/csv",
        )

        # ══════════════════════════════════════════════════════════════════
        # TROJAN HORSE — AUTO-SORTED CHART PIPELINE
        # ══════════════════════════════════════════════════════════════════
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="result-card"><h3>📊 AUDIT VISUEL (TROJAN HORSE)</h3>'
            '<p>Comparaison automatique : le site le plus lent vs. les concurrents les plus rapides.</p></div>',
            unsafe_allow_html=True,
        )

        # Build a clean copy for the chart: drop Failed / NaN, convert to float
        chart_df = df.copy()
        chart_df = chart_df[chart_df["LCP_Score"] != "Failed"]
        chart_df = chart_df[chart_df["LCP_Score"].notna()]

        # Convert "5.34 s" → 5.34
        chart_df["LCP_Float"] = (
            chart_df["LCP_Score"]
            .str.replace(" s", "", regex=False)
            .apply(pd.to_numeric, errors="coerce")
        )
        chart_df = chart_df.dropna(subset=["LCP_Float"])

        # Sort ascending (fastest first → slowest last)
        chart_df = chart_df.sort_values("LCP_Float", ascending=True).reset_index(drop=True)

        if len(chart_df) < 2:
            st.warning(
                "Not enough valid LCP data in this batch to generate a "
                "comparison chart. Check database for failed sites."
            )
        else:
            # Target = slowest (last row)
            target_row = chart_df.iloc[-1]
            target_name = target_row["Business_Name"]
            target_lcp = target_row["LCP_Float"]

            # Competitors = up to 3 fastest (first rows)
            comp_df = chart_df.iloc[:-1].head(3)
            comp_names = comp_df["Business_Name"].tolist()
            comp_scores = comp_df["LCP_Float"].tolist()

            fig = generate_audit_chart(
                target_name=target_name,
                target_lcp=target_lcp,
                comp_names=comp_names,
                comp_scores=comp_scores,
            )

            st.pyplot(fig)

            # Export to in-memory PNG
            buf = io.BytesIO()
            fig.savefig(
                buf,
                format="png",
                dpi=300,
                facecolor="#0A0A0A",
                bbox_inches="tight",
                pad_inches=0.3,
            )
            buf.seek(0)
            plt.close(fig)

            safe_name = target_name.replace(" ", "_")
            st.download_button(
                label="⬇  Télécharger le graphique (PNG)",
                data=buf,
                file_name=f"audit_{safe_name}.png",
                mime="image/png",
            )


if __name__ == "__main__":
    main()
