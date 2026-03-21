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
    print("🚀 GitHub Hunter Bot Awake! Starting Continuous Hunt...", flush=True)
    
    # 1. Auto-generate tasks from pending campaigns
    camps = supabase.table("campaigns").select("*").eq("status", "pending").execute()
    if camps.data:
        for camp in camps.data:
            tc = camp.get("target_client") or camp.get("occupation", "business owner")
            for q in generate_queries(tc, camp.get("city", ""), 6):
                ex = supabase.table("task_queue").select("id").eq("campaign_id", camp["id"]).eq("query", q).execute()
                if not ex.data:
                    supabase.table("task_queue").insert({"campaign_id": camp["id"], "query": q, "status": "pending"}).execute()
        print("📋 New tasks generated from campaigns!", flush=True)

    EXT_PATH = os.path.join(os.getcwd(), "my_extension")
    ext_ok = os.path.exists(EXT_PATH)

    with sync_playwright() as p:
        args = ["--no-sandbox", "--disable-dev-shm-usage"]
        if ext_ok:
            args += [f"--disable-extensions-except={EXT_PATH}", f"--load-extension={EXT_PATH}"]
            
        browser = p.chromium.launch_persistent_context(
            user_data_dir="./profile_hunter",
            headless=True,
            args=args
        )
        page = browser.pages[0] if browser.pages else browser.new_page()
        
        # 🔄 THE SMART LOOP
        while True:
            claimed = supabase.rpc("claim_task", {"worker_name": "GitHub_Hunter"}).execute()
            if not claimed.data:
                print("ZZZ: No tasks left. Target met or queue empty. Hunter sleeping.", flush=True)
                break

            task = claimed.data[0]
            task_id = task["id"]
            camp_id = task["campaign_id"]
            
            c = supabase.table("campaigns").select("*").eq("id", camp_id).single().execute()
            if not c.data:
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task_id).execute()
                continue
            camp = c.data

            prof = supabase.table("profiles").select("daily_limit").eq("id", camp["user_id"]).single().execute()
            target_limit = prof.data.get("daily_limit", 5) if prof.data else 5
            
            leads_count_res = supabase.table("leads").select("id", count="exact").eq("campaign_id", camp_id).execute()
            current_leads = leads_count_res.count if leads_count_res else 0

            if current_leads >= target_limit:
                print(f"🏆 TARGET COMPLETE for Campaign {camp_id[:8]}! ({current_leads}/{target_limit}). Stopping campaign.", flush=True)
                supabase.table("campaigns").update({"status": "completed"}).eq("id", camp_id).execute()
                supabase.table("task_queue").update({"status": "completed"}).eq("campaign_id", camp_id).eq("status", "pending").execute()
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task_id).execute()
                continue

            print(f"🎯 HUNTING: {task['query']} | Target Progress: {current_leads}/{target_limit}", flush=True)
            supabase.table("campaigns").update({"status": "processing"}).eq("id", camp_id).execute()

            try:
                page.goto(f"https://www.google.com/search?q={task['query'].replace(' ', '+')}&num=100", timeout=30000)
                time.sleep(8)
                page.mouse.wheel(0, 3000)
                time.sleep(3)
                
                # 🛠️ THE MASTER FIX: Reading visible text instead of raw HTML
                visible_text = page.inner_text("body")
                
                # Strict Regex without % symbol to avoid grabbing URLs
                raw_emails = list(set(re.findall(r"[a-zA-Z0-9._-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", visible_text)))
                
                # Pre-Filtering on GitHub to save Database load
                clean_emails = []
                for e in raw_emails:
                    e = e.lower().strip().rstrip(".")
                    if "%" not in e and "/" not in e and "+" not in e and "=" not in e and e.endswith("@gmail.com"):
                        clean_emails.append(e)

                print(f"📧 Found {len(clean_emails)} clean emails. Processing...", flush=True)

                inserted_this_round = 0
                for email in clean_emails:
                    if current_leads + inserted_this_round >= target_limit:
                        print(f"🛑 Target of {target_limit} met exactly! Stopping insertion.", flush=True)
                        break
                        
                    try:
                        res = supabase.table("leads").insert({
                            "campaign_id": camp_id, "user_id": camp["user_id"], 
                            "email": email, "status": "raw"
                        }).execute()
                        if res.data:
                            inserted_this_round += 1
                    except: pass 
                    
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task_id).execute()
                
                if current_leads + inserted_this_round >= target_limit:
                     print(f"🏆 TARGET COMPLETE after this query! Marking campaign as COMPLETED.", flush=True)
                     supabase.table("campaigns").update({"status": "completed"}).eq("id", camp_id).execute()
                     supabase.table("task_queue").update({"status": "completed"}).eq("campaign_id", camp_id).eq("status", "pending").execute()
                     
            except Exception as e:
                print(f"❌ Error: {e}", flush=True)
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task_id).execute()
        
        print("✅ All queues processed. Hunter shutting down.", flush=True)
        browser.close()

if __name__ == "__main__":
    run_hunter()
