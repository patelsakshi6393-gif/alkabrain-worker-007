import os, time, re, random
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

# ══ CONFIG ══
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
WORKER_ID = f"Global_Hunter_{random.randint(100, 999)}"

# Top Global Business Hubs for rotation
GLOBAL_HUBS = ["New York", "London", "Dubai", "Singapore", "San Francisco", "Toronto", "Sydney", "Delhi", "Mumbai", "Austin"]

def get_global_queries(prof, tc, city):
    # Agar city EMPTY hai toh global hubs use karo
    locations = [city] * 4 if city and city != "EMPTY" else random.sample(GLOBAL_HUBS, 5)
    queries = []
    for loc in locations:
        queries.append(f'site:instagram.com "{tc}" "{loc}" "@gmail.com"')
        queries.append(f'"{tc}" "{loc}" hiring "{prof}" "@gmail.com"')
        queries.append(f'"{tc}" "{loc}" "contact us" "@gmail.com"')
    return queries

def validate_strict(email):
    e = email.lower().strip().rstrip(".")
    if any(x in e for x in ["%", "/", "=", "+", "image", "png", "jpg"]): return None
    return e if re.match(r"^[a-z0-9._-]+@gmail\.com$", e) else None

def run_hunter():
    print(f"🚀 {WORKER_ID} Awake! Target: Global Clients", flush=True)

    # 1. Claim ONE Campaign (1 Worker = 1 User logic)
    res = supabase.table("campaigns").select("*").eq("status", "pending").limit(1).execute()
    if not res.data:
        res = supabase.table("campaigns").select("*").eq("status", "processing").limit(1).execute()
    
    if not res.data:
        print("😴 No campaigns. Sleeping.")
        return

    camp = res.data[0]
    camp_id, user_id = camp["id"], camp["user_id"]
    prof, tc = camp.get("occupation", "Expert"), camp.get("target_client") or "Business"
    
    supabase.table("campaigns").update({"status": "processing"}).eq("id", camp_id).execute()

    # Generate Tasks
    queries = get_global_queries(prof, tc, camp.get("city"))
    for q in queries:
        try:
            supabase.table("task_queue").upsert({"campaign_id": camp_id, "query": q, "status": "pending"}, on_conflict="campaign_id,query").execute()
        except: pass

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()

        while True:
            # Atomic Claim Task
            claimed = supabase.rpc("claim_task", {"worker_name": WORKER_ID}).execute()
            if not claimed.data: break

            task = claimed.data[0]
            print(f"🎯 Scouting: {task['query']}", flush=True)

            try:
                page.goto(f"https://www.google.com/search?q={task['query'].replace(' ', '+')}&num=100", timeout=30000)
                time.sleep(10)
                
                # Visible Text Scraping to avoid junk URL codes
                visible_text = page.inner_text("body")
                emails = set(re.findall(r"[a-z0-9._-]+@[a-z0-9.\-]+\.[a-z]{2,}", visible_text.lower()))
                
                valid_count = 0
                for e in emails:
                    clean_e = validate_strict(e)
                    if clean_e and "@gmail.com" in clean_e:
                        try:
                            supabase.table("leads").insert({"campaign_id": camp_id, "user_id": user_id, "email": clean_e, "status": "raw"}).execute()
                            valid_count += 1
                        except: pass
                
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task["id"]).execute()
                print(f"✅ Found {valid_count} potential clients.", flush=True)
            except:
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task["id"]).execute()

        browser.close()
    
    # Auto-Complete Campaign
    pending = supabase.table("task_queue").select("id").eq("campaign_id", camp_id).eq("status", "pending").execute()
    if not pending.data:
        supabase.table("campaigns").update({"status": "completed"}).eq("id", camp_id).execute()

if __name__ == "__main__":
    run_hunter()
