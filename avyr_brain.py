"""
AVYR DIGITAL — AI Brain Worker (Cloud Native)
=============================================
Standalone automated dispatch script that queries the remote Turso database for unprocessed
leads, scrapes website context, uses Gemini to write highly customized
bespoke copy, and pushes the Lead to a Notion database.
"""

import os
import sys
import libsql_client
import json
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from google import genai
from google.genai import types

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

console = Console(force_terminal=True)

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_db_connection():
    url = os.environ.get("TURSO_DATABASE_URL")
    auth_token = os.environ.get("TURSO_AUTH_TOKEN")
    client = libsql_client.create_client_sync(url=url, auth_token=auth_token)
    
    try:
        client.execute("ALTER TABLE target_leads ADD COLUMN Processed_By_Brain INTEGER DEFAULT 0")
    except Exception:
        pass 
    try:
        client.execute("ALTER TABLE target_leads ADD COLUMN Drafted_IG_DM TEXT")
    except Exception:
        pass 
    return client

def fetch_unprocessed_leads(client, limit: int = 7) -> list:
    result = client.execute("SELECT rowid, * FROM target_leads WHERE Processed_By_Brain = 0 LIMIT ?", [limit])
    return [dict(zip(result.columns, row)) for row in result.rows]

def mark_lead_processed(client, rowid: int, ig_dm: str = None):
    client.execute("UPDATE target_leads SET Processed_By_Brain = 1, Drafted_IG_DM = ? WHERE rowid = ?", [ig_dm, rowid])

# ═══════════════════════════════════════════════════════════════════════════════
# WEB SCRAPING
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_website_context(url: str) -> str:
    if not url: return "No website provided."
    if not url.startswith(("http://", "https://")): url = "https://" + url

    import warnings
    warnings.filterwarnings('ignore')
    
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8, verify=False)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        text_chunks = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 20]
        if footer := soup.find("footer"): text_chunks.append("FOOTER: " + footer.get_text(strip=True))
            
        return " ".join(text_chunks)[:3000]
    except Exception as e:
        return f"Could not scrape website. Error: {str(e)}"

# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI COPYWRITER
# ═══════════════════════════════════════════════════════════════════════════════

def draft_pitch(business_name: str, digital_status: str, website_text: str) -> dict:
    if not GEMINI_API_KEY:
        console.print("[bold red]❌ GEMINI_API_KEY not found.[/bold red]")
        return {"subject": "Error", "body": "Missing API Key", "ig_dm": "Missing API Key"}
        
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # -------------------------------------------------------------------------
    # 💎 THE AVYR MASTERCLASS PROMPT (Dynamic Framework)
    # -------------------------------------------------------------------------
    system_instruction = (
        "You are Salaheddine, Founder and Lead Architect of AVYR DIGITAL, an elite digital architecture agency based in Casablanca. "
        "Your task is to write a highly bespoke cold email to a premium prospect.\n\n"
        
        "TONE & PERSONA: A+ Standard. You are not a marketer; you are a digital structural engineer. "
        "Your tone is observant, prestigious, brutally concise, and authoritative. "
        "Use architectural and physical metaphors (e.g., 'écrin digital', 'monolithe', 'fondation', 'rupture', 'friction'). "
        "Write in highly refined, professional French (unless the context strictly demands English).\n\n"
        
        "THE AVYR NARRATIVE ARC:\n"
        "1. Compliment the Artistry: Analyze the 'Context/Vibe Indicators' to deduce their Brand Vibe (e.g., minimalist, heritage, raw materials, clinical precision). Start by validating their physical or professional standard of excellence.\n"
        "2. The Rupture (The Hook): Frame their specific 'Digital Flaw' not as a technical error, but as a 'prestige leak' or 'rupture of the customer journey' that contradicts their high-end physical positioning.\n"
        "3. The Solution: Position AVYR as the architects who build bespoke, secure, and immersive digital infrastructures that match their physical standing.\n"
        "4. The CTA (Zero-Friction): Offer a 'Structural Mockup' or 'Blueprint' that you have already started drafting, or ask for a brief exchange to secure the breach.\n\n"
        
        "CONSTRAINTS:\n"
        "- NEVER use exclamation points (!).\n"
        "- NEVER use generic marketing terms like 'booster', 'synergie', 'optimiser', 'leader', or 'supercharge'.\n"
        "- Draft an Instagram DM variation that is 1-2 sentences maximum. Hyper-concise, authoritative, and native to IG.\n\n"
        
        "FORMAT: Strictly JSON: {\"subject\": \"...\", \"body\": \"...\", \"ig_dm\": \"...\"}. "
        "Use asymmetric, punchy paragraphs."
    )
    
    prompt = (
        f"Target Name: {business_name}\n"
        f"The Flaw: {digital_status}\n"
        f"Context/Vibe Indicators: {website_text}\n"
    )

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
        return json.loads(response.text)
    except Exception as e:
         console.print(f"[bold red]❌ Gemini API Error:[/bold red] {e}")
         return {"subject": "Digital Architecture Review", "body": "AI error.", "ig_dm": "AI error."}

# ═══════════════════════════════════════════════════════════════════════════════
# NOTION DISPATCHER
# ═══════════════════════════════════════════════════════════════════════════════

def push_to_notion(lead_data: dict, pitch_data: dict) -> bool:
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        console.print("[bold yellow]⚠️ Notion tokens missing. Skipping push.[/bold yellow]")
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
            "Business Name": {"title": [{"text": {"content": lead_data.get("Business_Name", "")}}]},
            "Digital Flaw": {"select": {"name": lead_data.get("Digital_Status", "UNKNOWN")}},
            "Status": {"status": {"name": "📥 Drafted"}}
        }
    }
    
    if email := lead_data.get("Email"):
        if email != "Not Found": payload["properties"]["Contact Email"] = {"email": email}
    if subject := pitch_data.get("subject"):
        payload["properties"]["Drafted Subject Line"] = {"rich_text": [{"text": {"content": subject}}]}
    if body := pitch_data.get("body"):
        payload["properties"]["Drafted Pitch / Body"] = {"rich_text": [{"text": {"content": body}}]}
    if ig_dm := pitch_data.get("ig_dm"):
        payload["properties"]["Drafted IG DM"] = {"rich_text": [{"text": {"content": ig_dm}}]}
        
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        console.print(f"[bold red]❌ Notion API Error:[/bold red] {e}")
        if hasattr(e, 'response') and e.response is not None:
             console.print(f"[bold red]Details:[/bold red] {e.response.text}")
        return False

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    console.print(Panel.fit("[bold cyan]AVYR DIGITAL — AI BRAIN WORKER[/bold cyan]\n[dim]Cloud-Native Dispatch Engine[/dim]", border_style="cyan"))
        
    conn = get_db_connection()
    leads = fetch_unprocessed_leads(conn, limit=7)
    
    if not leads:
        console.print("[bold green]✅ No new leads to process in Turso.[/bold green]")
        return
        
    console.print(f"[bold cyan]🔍 Found {len(leads)} unprocessed leads in Turso. Executing AI Analysis...[/bold cyan]\n")
    
    for lead in leads:
        rowid = lead["rowid"]
        b_name = lead.get("Business_Name", "Unknown")
        console.print(f"[bold white]▶ Target:[/bold white] [cyan]{b_name}[/cyan]")
        
        console.print("  [dim]└─[/dim] Fetching architecture context...")
        website_text = scrape_website_context(lead.get("Website", ""))
        
        console.print("  [dim]└─[/dim] Engineering bespoke pitch via Gemini...")
        time.sleep(30)
        pitch_json = draft_pitch(b_name, lead.get("Digital_Status", "UNKNOWN"), website_text)
        
        console.print("  [dim]└─[/dim] Dispatching payload to Notion ecosystem...")
        success = push_to_notion(lead, pitch_json)
        
        if success:
            mark_lead_processed(conn, rowid, pitch_json.get('ig_dm'))
            console.print("  [dim]└─[/dim] [bold green]✔ Payload successfully integrated into Notion.[/bold green]\n")
        else:
            console.print("  [dim]└─[/dim] [bold red]✖ Dispatch failed.[/bold red]\n")

if __name__ == "__main__":
    main()
    sys.exit(0)
