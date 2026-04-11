"""
AVYR DIGITAL — AI Brain Worker
================================
Standalone automated dispatch script that queries the sqlite database for unprocessed
leads, scrapes website context, uses Gemini 2.5 Flash to write highly customized
bespoke copy, and pushes the Lead to a Notion database.
"""

import os
import sqlite3
import json
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from google import genai
from google.genai import types

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & SETUP
# ═══════════════════════════════════════════════════════════════════════════════

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avyr_leads.db")

console = Console()

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Ensure Processed_By_Brain exists
    try:
        conn.execute("ALTER TABLE target_leads ADD COLUMN Processed_By_Brain INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass # Column likely exists
    try:
        conn.execute("ALTER TABLE target_leads ADD COLUMN Drafted_IG_DM TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass # Column likely exists
    return conn

def fetch_unprocessed_leads(conn: sqlite3.Connection, limit: int = 7) -> list:
    cur = conn.cursor()
    cur.execute(
        "SELECT rowid, * FROM target_leads WHERE Processed_By_Brain = 0 LIMIT ?", 
        (limit,)
    )
    return cur.fetchall()

def mark_lead_processed(conn: sqlite3.Connection, rowid: int, ig_dm: str = None):
    cur = conn.cursor()
    cur.execute("UPDATE target_leads SET Processed_By_Brain = 1, Drafted_IG_DM = ? WHERE rowid = ?", (ig_dm, rowid))
    conn.commit()

# ═══════════════════════════════════════════════════════════════════════════════
# WEB SCRAPING
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_website_context(url: str) -> str:
    if not url:
        return "No website provided."
        
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=8, verify=False)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Extract visible paragraph text
        paragraphs = soup.find_all("p")
        text_chunks = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20]
        
        # Extract footer text
        footer = soup.find("footer")
        if footer:
            text_chunks.append("FOOTER: " + footer.get_text(strip=True))
            
        context = " ".join(text_chunks)
        # Truncate to avoid massive payloads
        return context[:3000]
    except Exception as e:
        return f"Could not scrape website. Error: {str(e)}"

# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI COPYWRITER
# ═══════════════════════════════════════════════════════════════════════════════

def draft_pitch(business_name: str, digital_status: str, website_text: str) -> dict:
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not found. Export it or put in .env")
        
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    system_instruction = (
        "You are the Technical Director of AVYR DIGITAL, a high-end, luxury digital architecture agency. "
        "You are writing a cold email to a potential high-ticket client.\n\n"
        "Tone & Voice: Authoritative, sophisticated, concise, and direct. This is an 'A+' standard pitch. "
        "Do not use overly enthusiastic marketing jargon (no exclamation points, no 'we would love to', no 'super excited'). "
        "Speak like a high-level consultant pointing out a critical structural flaw.\n\n"
        "The Narrative Arc:\n"
        "The Hook (The Flaw): Dynamically reference the specific digital_status flaw provided to you. Highlight the disconnect "
        "between the premium nature of their physical business/brand and their current digital footprint (e.g., pointing out "
        "that relying on an outdated template, an Instagram-only presence, or generic email providers damages their high-end positioning).\n"
        "The Solution: Position AVYR as the architects who build bespoke digital infrastructure.\n"
        "The Offer (Frictionless CTA): Do not ask for a sales call. Offer to build and send them a custom digital architecture "
        "mockup or wireframe for their brand, entirely upfront.\n\n"
        "In addition to the email, you must draft an Instagram DM variation (ig_dm). The DM must be hyper-concise (1 to 2 sentences maximum). "
        "It should drop formal email greetings and feel native to the platform—punchy and direct, while maintaining the 'apex' luxury authority of AVYR DIGITAL. "
        "Do not use generic marketing emojis.\n\n"
        "Format:\n"
        "Keep the email body strictly under 80 words. Short, punchy, asymmetric paragraphs.\n"
        "Output strictly in JSON format: {\"subject\": \"...\", \"body\": \"...\", \"ig_dm\": \"...\"}."
    )
    
    prompt = f"Business Name: {business_name}\nDigital Flaw Detected: {digital_status}\nWebsite Context: {website_text}\n"

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                temperature=0.7,
            ),
        )
        
        result = json.loads(response.text)
        return result
    except Exception as e:
         console.print(f"[bold red]❌ Gemini API Error:[/bold red] {e}")
         return {"subject": "Digital Architecture Review", "body": "Could not generate draft due to an AI error.", "ig_dm": "Could not generate DM."}

# ═══════════════════════════════════════════════════════════════════════════════
# NOTION DISPATCHER
# ═══════════════════════════════════════════════════════════════════════════════

def push_to_notion(lead_data: dict, pitch_data: dict) -> bool:
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        console.print("[yellow]⚠️  Notion tokens missing. Skipping Notion push.[/yellow]")
        return False
        
    url = "https://api.notion.com/v1/pages"
    
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "Business Name": {
                "title": [{"text": {"content": lead_data.get("Business_Name", "")}}]
            },
            "Digital Flaw": {
                "select": {"name": lead_data.get("Digital_Status", "UNKNOWN")}
            },
            "Status": {
                "status": {"name": "📥 Drafted"}
            }
        }
    }
    
    # Optional fields
    email = lead_data.get("Email")
    if email and email != "Not Found":
        payload["properties"]["Contact Email"] = {"email": email}
        
    subject = pitch_data.get("subject")
    if subject:
        payload["properties"]["Drafted Subject Line"] = {
            "rich_text": [{"text": {"content": subject}}]
        }
        
    body = pitch_data.get("body")
    if body:
        payload["properties"]["Drafted Pitch / Body"] = {
            "rich_text": [{"text": {"content": body}}]
        }
        
    ig_dm = pitch_data.get("ig_dm")
    if ig_dm:
        payload["properties"]["Drafted IG DM"] = {
            "rich_text": [{"text": {"content": ig_dm}}]
        }
        
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        console.print(f"[bold red]❌ Notion API Error:[/bold red] {e}")
        if hasattr(e, 'response') and e.response is not None:
             console.print(f"Details: {e.response.text}")
        return False

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    console.print(Panel.fit(
        "[bold white]AVYR DIGITAL — AI BRAIN WORKER[/bold white]\n"
        "[dim]Automated Scraping, Copywriting, and Dispatch Engine[/dim]",
        border_style="purple"
    ))
    
    if not os.path.exists(DB_PATH):
        console.print(f"[bold red]❌ Database not found at {DB_PATH}.[/bold red]")
        return
        
    conn = get_db_connection()
    leads = fetch_unprocessed_leads(conn, limit=7)
    
    if not leads:
        console.print("[bold green]✅ No new leads to process.[/bold green]")
        return
        
    console.print(f"[bold cyan]🔍 Found {len(leads)} unprocessed leads. Commencing processing...[/bold cyan]\n")
    
    for lead in leads:
        rowid = lead["rowid"]
        lead_dict = dict(lead)
        b_name = lead_dict.get("Business_Name", "Unknown Business")
        url = lead_dict.get("Website", "")
        status = lead_dict.get("Digital_Status", "UNKNOWN")
        
        console.print(f"[bold white]▶ Processing:[/bold white] [magenta]{b_name}[/magenta]")
        
        # 1. Scrape
        console.print("  [dim]└─[/dim] [blue]FETCHING[/blue] website context...")
        website_text = scrape_website_context(url)
        
        # 2. Draft
        console.print("  [dim]└─[/dim] [yellow]DRAFTING[/yellow] personalized pitch...")
        time.sleep(1) # Small sleep for rate limits
        pitch_json = draft_pitch(b_name, status, website_text)
        
        # Display drafts
        console.print(f"      [dim]Email:[/dim] {pitch_json.get('subject')}")
        console.print(f"      [dim]IG DM:[/dim] {pitch_json.get('ig_dm')}")
        
        # 3. Dispatch
        console.print("  [dim]└─[/dim] [green]DISPATCHING[/green] to Notion...")
        success = push_to_notion(lead_dict, pitch_json)
        
        # 4. Mark Processed
        if success:
            mark_lead_processed(conn, rowid, pitch_json.get('ig_dm'))
            console.print("  [dim]└─[/dim] [bold green]✔ Done and marked processed.[/bold green]\n")
        else:
            console.print("  [dim]└─[/dim] [bold red]✖ Failed to dispatch. Keeping unprocessed.[/bold red]\n")

if __name__ == "__main__":
    main()
