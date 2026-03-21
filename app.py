import os, time, re, random
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

LOCATIONS = [
    "Mumbai","Delhi","Bangalore","Hyderabad","Chennai","Pune","Kolkata",
    "Ahmedabad","Jaipur","Surat","Noida","Gurgaon","Indore","Bhopal",
    "USA","UK","Canada","Australia","Dubai","Singapore","London","New York"
]

def generate_queries(target_client, city="", count=8):
    tc = target_client.strip()
    base = [city, city, city] + random.sample(LOCATIONS, 5) if city else random.sample(LOCATIONS, count)
    base = list(dict.fromkeys(base))[:count]
    patterns = [
        '"{tc}" "{loc}" "@gmail.com"', 'intitle:"{tc}" "{loc}" "contact" "@gmail.com"',
        '"{tc}" "{loc}" "email" "gmail"', 'intitle:"{tc}" "{loc}" "gmail.com"',
        '"{tc} owner" "{loc}" "@gmail.com"', '"{tc}" "{loc}" "reach me" "gmail"',
    ]
    return [p.format(tc=tc, loc=loc) for p, loc in zip(patterns, base)]

def run_hunter():
    print("🚀 GitHub Hunter Bot Awake!", flush=True)
    
    # 1. Auto-generate tasks from pending campaigns
    camps = supabase.table("campaigns").select("*").eq("status", "pending").limit(3).execute()
    if camps.data:
        for camp in camps.data:
            tc = camp.get("target_client") or camp.get("occupation", "business owner")
            for q in generate_queries(tc, camp.get("city", ""), 6):
                ex = supabase.table("task_queue").select("id").eq("campaign_id", camp["id"]).eq("query", q).execute()
                if not ex.data:
                    supabase.table("task_queue").insert({"campaign_id": camp["id"], "query": q, "status": "pending"}).execute()
        print("📋 New tasks generated from campaigns!", flush=True)

    # 2. Claim a pending task
    claimed = supabase.rpc("claim_task", {"worker_name": "GitHub_Hunter"}).execute()
    if not claimed.data:
        print("ZZZ: No tasks. Node sleeping.", flush=True)
        return

    task = claimed.data[0]
    camp_id = task["campaign_id"]
    print(f"🎯 HUNTING: {task['query']}", flush=True)
    
    supabase.table("campaigns").update({"status": "processing"}).eq("id", camp_id).execute()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page()
        try:
            page.goto(f"https://www.google.com/search?q={task['query'].replace(' ', '+')}&num=100", timeout=30000)
            time.sleep(8)
            page.mouse.wheel(0, 3000)
            time.sleep(3)
            
            # Scrape ALL emails (Raw)
            raw_emails = list(set(re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", page.content())))
            print(f"📧 Found {len(raw_emails)} raw emails. Sending to Validator...", flush=True)

            for email in raw_emails:
                try:
                    # Insert as 'raw' for HF to validate
                    supabase.table("leads").insert({
                        "campaign_id": camp_id, "user_id": claimed.data[0].get("user_id"), 
                        "email": email.lower(), "status": "raw"
                    }).execute()
                except: pass
                
            supabase.table("task_queue").update({"status": "completed"}).eq("id", task["id"]).execute()
            print("✅ Hunter Job Done!", flush=True)
        except Exception as e:
            print(f"❌ Error: {e}", flush=True)
            supabase.table("task_queue").update({"status": "failed"}).eq("id", task["id"]).execute()
        finally:
            browser.close()

if __name__ == "__main__":
    run_hunter()
