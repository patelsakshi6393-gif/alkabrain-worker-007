import os, time, re, random
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

# ══ CONFIGURATION ══
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
WORKER_ID = f"Hunter_{random.randint(100, 999)}"

# Genuine Email Search Queries (Google Dorks)
def get_advanced_queries(tc, loc):
    return [
        f'site:instagram.com "{tc}" "{loc}" "@gmail.com"',
        f'site:facebook.com "{tc}" "{loc}" "@gmail.com"',
        f'"{tc}" "{loc}" "contact us" "@gmail.com"',
        f'"{tc}" "{loc}" "owner" "@gmail.com"',
        f'"{tc}" "{loc}" "email me at" "@gmail.com"',
        f'"{tc}" "{loc}" "get a quote" "@gmail.com"'
    ]

# Strict Validation: Kachra email filter karna
def validate_strict(email):
    e = email.lower().strip().rstrip(".")
    # Junk characters filter
    if any(x in e for x in ["%", "/", "=", "+", "image", "png", "jpg", "jpeg", "webp"]): 
        return None
    # Sirf genuine format allow karna
    return e if re.match(r"^[a-z0-9._-]+@gmail\.com$", e) else None

def run_hunter():
    print(f"🚀 {WORKER_ID} Awake! Searching for new missions...", flush=True)

    # 1. 🛑 PREVENT COLLISION: Ek worker sirf EK campaign pakdega
    # Pehle 'pending' campaigns dekho
    camp_res = supabase.table("campaigns").select("*").eq("status", "pending").limit(1).execute()
    
    # Agar pending nahi hai, toh check karo koi 'processing' wala campaign jo adhura ho
    if not camp_res.data:
        camp_res = supabase.table("campaigns").select("*").eq("status", "processing").limit(1).execute()
    
    if not camp_res.data:
        print("😴 Sab kaam khatam! No pending campaigns found. Node sleeping.", flush=True)
        return

    camp = camp_res.data[0]
    camp_id, user_id = camp["id"], camp["user_id"]
    tc = camp.get("target_client") or camp.get("occupation", "business")
    city = camp.get("city", "Global")
    
    # 2. LOCK CAMPAIGN: Is campaign ko block kardo taaki doosra worker na chhuye
    supabase.table("campaigns").update({"status": "processing"}).eq("id", camp_id).execute()

    # 3. TASK GENERATION: Is user ke liye search tasks banao (Agar pehle se nahi bane)
    queries = get_advanced_queries(tc, city)
    for q in queries:
        try:
            supabase.table("task_queue").upsert({
                "campaign_id": camp_id, 
                "query": q, 
                "status": "pending"
            }, on_conflict="campaign_id,query").execute()
        except: pass

    EXT_PATH = os.path.join(os.getcwd(), "my_extension")
    
    with sync_playwright() as p:
        # Browser Launch with optional extension
        browser_args = ["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
        if os.path.exists(EXT_PATH):
            browser_args += [f"--disable-extensions-except={EXT_PATH}", f"--load-extension={EXT_PATH}"]
            
        browser = p.chromium.launch_persistent_context(
            user_data_dir=f"./profile_{WORKER_ID}",
            headless=True,
            args=browser_args
        )
        page = browser.new_page()

        # 🔄 CONTINUOUS HUNTING LOOP
        while True:
            # Atomic Claim: 'pending' task ko 'processing' mein badlo
            claimed = supabase.rpc("claim_task", {"worker_name": WORKER_ID}).execute()
            if not claimed.data:
                print(f"🏁 Is campaign ke saare tasks khatam. Hunter exiting...", flush=True)
                break

            task = claimed.data[0]
            print(f"🎯 TARGET LOCKED: {task['query']}", flush=True)

            try:
                # Search on Google
                page.goto(f"https://www.google.com/search?q={task['query'].replace(' ', '+')}&num=100", timeout=40000)
                time.sleep(10)
                
                # Step A: Pehle Google ke text se emails nikaalo
                raw_text = page.inner_text("body")
                direct_emails = re.findall(r"[a-zA-Z0-9._-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", raw_text)
                
                # Step B: Pehle 10-15 Websites ke andar ghuso (Deep Scan)
                links = page.locator("a").evaluate_all("els => els.map(el => el.href)")
                target_urls = [l for l in links if "google.com" not in l and l.startswith("http")][:12]
                
                found_emails = set()
                # Direct Google text emails add karo
                for e in direct_emails:
                    valid = validate_strict(e)
                    if valid: found_emails.add(valid)

                # Deep Website Scraping
                for url in target_urls:
                    try:
                        print(f"  🔍 Deep Scanning Website: {url[:50]}...", flush=True)
                        new_tab = browser.new_page()
                        new_tab.goto(url, timeout=15000)
                        time.sleep(3)
                        # Page ka asali text uthao
                        site_text = new_tab.inner_text("body")
                        site_emails = re.findall(r"[a-zA-Z0-9._-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", site_text)
                        for e in site_emails:
                            valid = validate_strict(e)
                            if valid: found_emails.add(valid)
                        new_tab.close()
                    except:
                        continue # Website block kare toh next par jao

                # 4. PUSH TO DB: Found emails ko 'raw' status mein save karo
                pushed_count = 0
                for email in found_emails:
                    try:
                        # campaign_id aur email ka UNIQUE constraint collision bachayega
                        res = supabase.table("leads").insert({
                            "campaign_id": camp_id, 
                            "user_id": user_id, 
                            "email": email, 
                            "status": "raw"
                        }).execute()
                        if res.data: pushed_count += 1
                    except: pass # Duplicate skip
                
                # Task Completed
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task["id"]).execute()
                print(f"✅ Mission Success: {pushed_count} genuine emails added to vault.", flush=True)

            except Exception as e:
                print(f"⚠️ Hunt Failed for this query: {e}", flush=True)
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task["id"]).execute()

        browser.close()
    
    # Final Check: Agar campaign ke saare tasks khatam, toh mark as COMPLETED
    rem_tasks = supabase.table("task_queue").select("id").eq("campaign_id", camp_id).eq("status", "pending").execute()
    if not rem_tasks.data:
        supabase.table("campaigns").update({"status": "completed"}).eq("id", camp_id).execute()
        print(f"🎊 Campaign {camp_id[:8]} fully completed!", flush=True)

if __name__ == "__main__":
    run_hunter()
