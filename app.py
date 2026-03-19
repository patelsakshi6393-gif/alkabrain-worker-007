# Eta Node Silent Hunter
# Node: 007 | Identity: Eta
import os, time, re, random, smtplib, traceback
from playwright.sync_api import sync_playwright
from supabase import create_client, Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
EXT_PATH     = os.path.join(os.getcwd(), "my_extension")

print("=" * 50, flush=True)
print("ALKABRAIN Node 007 (Eta) Starting...", flush=True)
print("=" * 50, flush=True)
print(f"SUPABASE_URL set: {bool(SUPABASE_URL)}", flush=True)
print(f"SUPABASE_KEY set: {bool(SUPABASE_KEY)}", flush=True)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("OK: Supabase connected!", flush=True)

session_text = os.getenv("AUTH_SESSION")
if session_text:
    with open("auth.json", "w") as f:
        f.write(session_text)
    print("OK: auth.json saved!", flush=True)

# ── QUERY GENERATOR ─────────────────────────────────
LOCATIONS = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat",
    "USA", "UK", "Canada", "Australia", "Dubai",
    "India", "London", "New York", "Singapore", "Germany"
]

def generate_queries(occupation, count=8):
    occ = occupation.lower().strip()
    locs = random.sample(LOCATIONS, min(count, len(LOCATIONS)))
    queries = []
    patterns = [
        f'intitle:"{occ}" "gmail.com" "{{}}"',
        f'intitle:"hire {occ}" "{{}}" "@gmail.com"',
        f'intitle:"{occ} services" "{{}}" "contact"',
        f'"{occ}" "{{}}" "@gmail.com" "email"',
        f'intitle:"{occ} portfolio" "{{}}" "gmail"',
        f'"{occ} freelancer" "{{}}" "@gmail.com"',
        f'intitle:"about" "{occ}" "{{}}" "gmail.com"',
        f'"{occ}" "{{}}" "reach me" "@gmail.com"',
        f'intitle:"{occ} studio" "{{}}" "contact"',
        f'"{occ} agency" "{{}}" "@gmail.com"',
    ]
    for i, loc in enumerate(locs):
        pattern = patterns[i % len(patterns)]
        queries.append(pattern.format(loc))
    return queries

# ── EMAIL TEMPLATES (20) ─────────────────────────────
def get_email_template(occ):
    templates = [
        {
            "subject": f"Business Inquiry for {occ}",
            "body": f"Hi,\n\nI came across your {occ} services online and was really impressed. Are you currently taking on new clients or open to business collaborations? Let me know!\n\nBest regards,"
        },
        {
            "subject": f"Quick question about your {occ} work",
            "body": f"Hello,\n\nI was looking for a skilled {occ} and found your profile. I have a project that might be a great fit for you. Would you be open to a quick chat?\n\nThanks,"
        },
        {
            "subject": f"Collaboration opportunity - {occ}",
            "body": f"Hi there,\n\nI am looking to connect with experienced {occ} professionals for potential referrals and collaborations. Would you be open to discussing this?\n\nLooking forward,"
        },
        {
            "subject": f"Exciting project for a {occ}",
            "body": f"Hey,\n\nI have an exciting project that requires a talented {occ}. I believe your skills would be a perfect match. Are you available for new work?\n\nBest,"
        },
        {
            "subject": f"Looking for a {occ} — are you available?",
            "body": f"Hi,\n\nWe are actively looking for a reliable {occ} for an upcoming project. Your profile caught our attention. Are you currently accepting new clients?\n\nRegards,"
        },
        {
            "subject": f"Potential work opportunity — {occ}",
            "body": f"Hello,\n\nI am reaching out because we have a project that needs an experienced {occ}. The timeline is flexible and the budget is competitive. Interested?\n\nCheers,"
        },
        {
            "subject": f"Can we work together? ({occ})",
            "body": f"Hi,\n\nI found your {occ} work online and I am genuinely impressed. I have a client who is looking for exactly your type of expertise. Can we connect?\n\nBest regards,"
        },
        {
            "subject": f"New project — need a {occ}",
            "body": f"Hey there,\n\nWe just kicked off a new project and are looking for a skilled {occ} to join our team. Your background looks like a great fit. Let us talk!\n\nThanks,"
        },
        {
            "subject": f"Referral opportunity for {occ}",
            "body": f"Hi,\n\nI run a small agency and we frequently need {occ} professionals for client projects. Would you be interested in being on our referral list?\n\nKind regards,"
        },
        {
            "subject": f"Quick opportunity for you — {occ}",
            "body": f"Hello,\n\nI came across your profile while searching for a qualified {occ}. We have a short-term project that could turn into a long-term collaboration. Interested?\n\nBest,"
        },
        {
            "subject": f"High-paying {occ} project available",
            "body": f"Hi,\n\nWe have a well-paying project available for the right {occ}. The work is interesting and the client is easy to work with. Are you open to hearing more?\n\nRegards,"
        },
        {
            "subject": f"Is your {occ} calendar open?",
            "body": f"Hey,\n\nI am building a team for a new venture and I am looking for a talented {occ}. Your work stood out to me. Do you have bandwidth for a new project?\n\nCheers,"
        },
        {
            "subject": f"Let us collaborate — {occ} project",
            "body": f"Hi,\n\nI have been looking for a great {occ} to collaborate with on some exciting projects. Your portfolio looks impressive. Would love to connect!\n\nBest wishes,"
        },
        {
            "subject": f"Freelance {occ} opportunity",
            "body": f"Hello,\n\nAre you open to freelance work? We have ongoing {occ} projects and are always looking for talented professionals to partner with.\n\nLooking forward to hearing from you,"
        },
        {
            "subject": f"Your {occ} skills are needed",
            "body": f"Hi,\n\nWe are urgently looking for a {occ} for an important client project. Deadline is soon and the pay is good. Can you help?\n\nThank you,"
        },
        {
            "subject": f"Business proposal for a {occ}",
            "body": f"Hello,\n\nI would like to propose a business collaboration with you as our go-to {occ}. We have multiple clients who regularly need these services.\n\nHope to hear from you,"
        },
        {
            "subject": f"Are you the right {occ} for us?",
            "body": f"Hi,\n\nWe are a growing startup and we need a skilled {occ} to help us scale. We offer competitive rates and flexible working hours. Interested?\n\nKind regards,"
        },
        {
            "subject": f"Urgent: {occ} needed for project",
            "body": f"Hey,\n\nWe have an urgent requirement for an experienced {occ}. The project is well-defined and the client is ready to start immediately. Available?\n\nThanks,"
        },
        {
            "subject": f"Long-term work for a {occ}",
            "body": f"Hello,\n\nWe are looking for a {occ} for a long-term engagement. If you are interested in stable, ongoing work with good pay, let us talk!\n\nBest regards,"
        },
        {
            "subject": f"Special project — {occ} required",
            "body": f"Hi,\n\nWe have a special project that is a perfect match for a talented {occ} like yourself. The scope is exciting and the team is great. Want to know more?\n\nCheers,"
        },
    ]
    return random.choice(templates)

# ── UTILS ────────────────────────────────────────────
def validate_email(raw):
    email = raw.lower().strip().rstrip('.')
    if re.match(r'^[a-z0-9._%+-]+@gmail\.com$', email):
        return email
    return None

def send_outreach(sender, pwd, target, occ):
    template = get_email_template(occ)
    msg = MIMEMultipart()
    msg['From']    = sender
    msg['To']      = target
    msg['Subject'] = template['subject']
    msg.attach(MIMEText(template['body'], 'plain'))
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=10) as s:
            s.login(sender, pwd)
            s.send_message(msg)
        print(f"   Sent template: '{template['subject']}'", flush=True)
        return True
    except Exception as e:
        print(f"WARN: Mail failed to {target}: {e}", flush=True)
        return False

# ── MAIN ─────────────────────────────────────────────
def run_ghost_hunter():
    res = supabase.table("task_queue").select("*").eq("status", "pending").limit(5).execute()
    if not res.data:
        print("ZZZ: No tasks. Node sleeping.", flush=True)
        return

    print(f"INFO: {len(res.data)} tasks found!", flush=True)

    for task in res.data:
        task_id = task['id']
        camp_id = task['campaign_id']
        query   = task['query']
        print(f"\nHUNT: {query}", flush=True)

        c_res = supabase.table("campaigns").select("*").eq("id", camp_id).single().execute()
        if not c_res.data:
            print(f"ERROR: Campaign {camp_id} not found", flush=True)
            supabase.table("task_queue").update({"status": "failed"}).eq("id", task_id).execute()
            continue

        camp = c_res.data
        occ  = camp.get('occupation', 'professional')

        # Auto-generate more queries aur queue mein add karo
        new_queries = generate_queries(occ, 5)
        for q in new_queries:
            try:
                existing = supabase.table("task_queue").select("id").eq("query", q).execute()
                if not existing.data:
                    supabase.table("task_queue").insert({
                        "campaign_id": camp_id,
                        "query": q,
                        "status": "pending"
                    }).execute()
            except:
                pass

        supabase.table("task_queue").update({"status": "processing"}).eq("id", task_id).execute()

        with sync_playwright() as p:
            auth_file = "auth.json" if os.path.exists("auth.json") else None
            try:
                browser = p.chromium.launch_persistent_context(
                    user_data_dir="./gh_profile",
                    headless=False,
                    storage_state=auth_file,
                    args=[
                        f"--disable-extensions-except={EXT_PATH}",
                        f"--load-extension={EXT_PATH}",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                    ]
                )
                print("OK: Browser launched!", flush=True)
            except Exception as e:
                print(f"ERROR: Browser failed: {e}\n{traceback.format_exc()}", flush=True)
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task_id).execute()
                continue

            page = browser.pages[0] if browser.pages else browser.new_page()
            try:
                page.goto(
                    f"https://www.google.com/search?q={query.replace(' ', '+')}&num=100",
                    timeout=30000
                )
                print("OK: Google loaded!", flush=True)
                print("WAIT: 15 sec for Hunter Extension...", flush=True)
                time.sleep(15)
                page.mouse.wheel(0, 2000)
                time.sleep(3)

                emails = re.findall(
                    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
                    page.content()
                )
                print(f"SCAN: {len(set(emails))} unique emails found", flush=True)

                count = 0
                for raw in set(emails):
                    valid = validate_email(raw)
                    if valid:
                        if send_outreach(camp['sender_email'], camp['app_password'], valid, occ):
                            supabase.table("leads").insert({
                                "campaign_id": camp_id,
                                
                                "email": valid,
                                "status": "emailed"
                            }).execute()
                            count += 1
                            print(f"MAIL: {valid}", flush=True)
                            delay = random.uniform(30, 60)
                            print(f"DELAY: {delay:.0f}s anti-spam...", flush=True)
                            time.sleep(delay)

                print(f"\nDONE: Total leads: {count}", flush=True)
                try:
                    supabase.rpc('update_campaign_stats', {
                        'camp_id': camp_id,
                        'inc_leads': count
                    }).execute()
                except Exception as e:
                    print(f"WARN: Stats update: {e}", flush=True)

            except Exception as e:
                print(f"ERROR: {e}\n{traceback.format_exc()}", flush=True)
            finally:
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task_id).execute()
                browser.close()

    print(f"\nFIN: Node 007 (Eta) complete!", flush=True)

if __name__ == "__main__":
    run_ghost_hunter()

# End Node 007 - Eta


