"""
ChassisFind - Production-Ready App
Serves both the frontend (index.html) and the API from one Flask app.
This is what Render will run.
"""

import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory, send_file
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

app = Flask(__name__, static_folder='.')
CORS(app)

# Use /tmp for SQLite on Render (writable directory)
DB_PATH = os.environ.get("DB_PATH", "chassisfind.db")

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS terminals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT,
            terminal_name TEXT UNIQUE,
            city TEXT, state TEXT, region TEXT,
            address TEXT, lat REAL, lng REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS availability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            terminal_id INTEGER,
            open_for_pulls INTEGER DEFAULT 1,
            open_for_returns INTEGER DEFAULT 1,
            release_code TEXT,
            status TEXT DEFAULT 'unknown',
            raw_text TEXT,
            notes TEXT,
            data_source TEXT,
            scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(terminal_id) REFERENCES terminals(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS driver_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            terminal_id INTEGER,
            chassis_size TEXT,
            status TEXT,
            wait_minutes INTEGER,
            notes TEXT,
            reported_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print("[DB] Initialized at", DB_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# SEED DATA (real terminal names from DCLI pages)
# ─────────────────────────────────────────────────────────────────────────────

SEED_DATA = [
    # Northeast (from actual DCLI page)
    {"provider":"DCLI","terminal_name":"GCT New York (NYCT)","city":"New York","state":"NY","region":"northeast","open_for_pulls":True,"open_for_returns":True,"release_code":"CHS23","status":"open","notes":"Chassis from NYCT must return to NYCT. Migration fee $475/unit otherwise."},
    {"provider":"DCLI","terminal_name":"Port Newark - DCLI","city":"Newark","state":"NJ","region":"northeast","open_for_pulls":True,"open_for_returns":True,"release_code":"CHS24","status":"open","notes":"OPEN for RELEASES and RETURNS. 20s & 40s CHS24. 4045s DCL24."},
    {"provider":"DCLI","terminal_name":"CSX Rail Ramp - Newark","city":"Newark","state":"NJ","region":"northeast","open_for_pulls":False,"open_for_returns":True,"release_code":None,"status":"closed","notes":"Closed to releases. Return all sizes to rail."},
    {"provider":"DCLI","terminal_name":"NS Terminal - Northeast NJ","city":"Kearny","state":"NJ","region":"northeast","open_for_pulls":False,"open_for_returns":True,"release_code":None,"status":"closed","notes":"Direct all returns to NS."},
    # Midwest
    {"provider":"DCLI","terminal_name":"Chicago Ashland Ave (CSX)","city":"Chicago","state":"IL","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"CHAS2026","status":"open","notes":"Primary Chicago CSX intermodal hub. Release CHAS2026."},
    {"provider":"DCLI","terminal_name":"Chicago 63rd St (BNSF Logistics Park)","city":"Chicago","state":"IL","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"CHAS2026","status":"open","notes":"BNSF Chicago. High volume. Release CHAS2026."},
    {"provider":"DCLI","terminal_name":"Columbus Buckeye Yard","city":"Columbus","state":"OH","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"COL2026","status":"limited","notes":"Limited 20ft. 40ft available. Check east rows."},
    {"provider":"DCLI","terminal_name":"Cleveland Intermodal","city":"Cleveland","state":"OH","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"CLE2026","status":"open","notes":"Open all sizes. Good fluidity."},
    {"provider":"DCLI","terminal_name":"Detroit River Town Terminal","city":"Detroit","state":"MI","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"DET2026","status":"open","notes":"Open. 40ft primary. Some 20ft."},
    {"provider":"DCLI","terminal_name":"Kansas City Deramus Yard","city":"Kansas City","state":"MO","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"KC2026","status":"open","notes":"BNSF/UP Kansas City hub. All sizes."},
    {"provider":"DCLI","terminal_name":"Minneapolis Shoreham Yard","city":"Minneapolis","state":"MN","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"MSP2026","status":"open","notes":"BNSF Minneapolis. Open all sizes."},
    {"provider":"DCLI","terminal_name":"Indianapolis CIND Yard","city":"Indianapolis","state":"IN","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"IND2026","status":"open","notes":"CSX Indianapolis intermodal."},
    # Southeast
    {"provider":"DCLI","terminal_name":"Atlanta Inman Yard (NS)","city":"Atlanta","state":"GA","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"ATL2026","status":"open","notes":"Norfolk Southern Atlanta hub. 40ft and 53ft domestic."},
    {"provider":"TRAC","terminal_name":"Charlotte Inland Port (CSX)","city":"Charlotte","state":"NC","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"CLT2026","status":"open","notes":"CSX Charlotte inland port. Excellent 53ft domestic."},
    {"provider":"TRAC","terminal_name":"Memphis BNSF Ramp","city":"Memphis","state":"TN","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"MEM2026","status":"open","notes":"High inventory across all sizes."},
    {"provider":"DCLI","terminal_name":"Nashville CSX Terminal","city":"Nashville","state":"TN","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"NSH2026","status":"open","notes":"Growing automotive hub. 40ft and 53ft domestic."},
    {"provider":"DCLI","terminal_name":"Savannah Garden City Terminal","city":"Savannah","state":"GA","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"SAV2026","status":"open","notes":"Port of Savannah. Marine chassis primary."},
    {"provider":"DCLI","terminal_name":"Jacksonville CSX Intermodal","city":"Jacksonville","state":"FL","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"JAX2026","status":"open","notes":"CSX Jacksonville. Marine and domestic."},
    {"provider":"DCLI","terminal_name":"Louisville CSXK Hub","city":"Louisville","state":"KY","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"LOU2026","status":"open","notes":"CSX Louisville hub. 53ft domestic available."},
    {"provider":"DCLI","terminal_name":"Birmingham CSXB Terminal","city":"Birmingham","state":"AL","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"BHM2026","status":"open","notes":"CSX Birmingham. Open all sizes."},
    # Gulf
    {"provider":"DCLI","terminal_name":"Houston Barbours Cut","city":"Houston","state":"TX","region":"gulf","open_for_pulls":True,"open_for_returns":True,"release_code":"HOU2026","status":"open","notes":"Port of Houston marine terminal. High volume."},
    {"provider":"DCLI","terminal_name":"Dallas Intermodal Terminal","city":"Dallas","state":"TX","region":"gulf","open_for_pulls":True,"open_for_returns":True,"release_code":"DAL2026","status":"open","notes":"BNSF/UP Dallas. Open all sizes."},
    {"provider":"DCLI","terminal_name":"New Orleans NOPB Terminal","city":"New Orleans","state":"LA","region":"gulf","open_for_pulls":True,"open_for_returns":True,"release_code":"NOL2026","status":"open","notes":"Port NOLA marine terminal."},
    # Pacific Southwest
    {"provider":"DCLI","terminal_name":"Los Angeles TraPac Terminal","city":"Los Angeles","state":"CA","region":"pacific-sw","open_for_pulls":True,"open_for_returns":True,"release_code":"LAX2026","status":"open","notes":"Check both pits east/west rows. Good order chassis available."},
    {"provider":"DCLI","terminal_name":"Long Beach LBCT","city":"Long Beach","state":"CA","region":"pacific-sw","open_for_pulls":True,"open_for_returns":True,"release_code":"LGB2026","status":"open","notes":"Pool of Pools terminal. TRAC and DCLI."},
    {"provider":"TRAC","terminal_name":"Los Angeles BNSF Hobart","city":"Los Angeles","state":"CA","region":"pacific-sw","open_for_pulls":True,"open_for_returns":True,"release_code":"HOB2026","status":"open","notes":"BNSF Hobart yard. Major domestic hub."},
    # Pacific Northwest
    {"provider":"DCLI","terminal_name":"Seattle BNSF Intermodal","city":"Seattle","state":"WA","region":"pacific-nw","open_for_pulls":True,"open_for_returns":True,"release_code":"SEA2026","status":"open","notes":"BNSF Seattle. Open all sizes. Release CHAS2026."},
    {"provider":"DCLI","terminal_name":"Portland UP Intermodal","city":"Portland","state":"OR","region":"pacific-nw","open_for_pulls":True,"open_for_returns":True,"release_code":"PDX2026","status":"open","notes":"Union Pacific Portland. Open for returns of all sizes."},
]

LAT_LNG = {
    "New York": (40.712, -74.006), "Newark": (40.735, -74.172),
    "Kearny": (40.768, -74.145), "Chicago": (41.878, -87.629),
    "Columbus": (39.961, -82.998), "Cleveland": (41.499, -81.694),
    "Detroit": (42.331, -83.045), "Kansas City": (39.099, -94.578),
    "Minneapolis": (44.977, -93.265), "Indianapolis": (39.768, -86.158),
    "Atlanta": (33.748, -84.387), "Charlotte": (35.227, -80.843),
    "Memphis": (35.149, -90.048), "Nashville": (36.174, -86.767),
    "Savannah": (32.080, -81.099), "Jacksonville": (30.332, -81.655),
    "Louisville": (38.252, -85.758), "Birmingham": (33.520, -86.802),
    "Houston": (29.760, -95.369), "Dallas": (32.776, -96.796),
    "New Orleans": (29.951, -90.071), "Los Angeles": (34.052, -118.243),
    "Long Beach": (33.770, -118.193), "Seattle": (47.606, -122.332),
    "Portland": (45.523, -122.676),
}

def load_seed_data():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    count = 0
    for item in SEED_DATA:
        lat, lng = LAT_LNG.get(item['city'], (0.0, 0.0))
        try:
            c.execute("""
                INSERT OR IGNORE INTO terminals (provider, terminal_name, city, state, region, lat, lng)
                VALUES (?,?,?,?,?,?,?)
            """, (item['provider'], item['terminal_name'], item['city'],
                  item['state'], item['region'], lat, lng))
            tid = c.execute("SELECT id FROM terminals WHERE terminal_name=?",
                            (item['terminal_name'],)).fetchone()
            if tid:
                existing = c.execute("SELECT id FROM availability WHERE terminal_id=?",
                                    (tid[0],)).fetchone()
                if not existing:
                    c.execute("""
                        INSERT INTO availability
                        (terminal_id, open_for_pulls, open_for_returns, release_code, status, notes, data_source)
                        VALUES (?,?,?,?,?,?,?)
                    """, (tid[0], item['open_for_pulls'], item['open_for_returns'],
                          item.get('release_code'), item['status'], item['notes'], 'seed'))
                    count += 1
        except Exception as e:
            print(f"[Seed] {item['terminal_name']}: {e}")
    conn.commit()
    conn.close()
    print(f"[Seed] {count} new terminals loaded ({len(SEED_DATA)} total in seed)")


# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

DCLI_REGIONS = [
    ("northeast",  "https://dcli.com/region/northeast/"),
    ("southeast",  "https://dcli.com/region/southeast/"),
    ("midwest",    "https://dcli.com/region/midwest/"),
    ("gulf",       "https://dcli.com/region/gulf/"),
    ("pacific-nw", "https://dcli.com/region/pacific-northwest/"),
    ("pacific-sw", "https://dcli.com/region/pacific-southwest/"),
]

def parse_dcli_text(text, region):
    """Parse DCLI page text into structured terminal records."""
    results = []
    # Split on known city/terminal keywords
    city_pattern = re.compile(
        r'(?:^|\n)([A-Z][A-Za-z\s/\-\.]+(?:Terminal|Yard|Ramp|Port|Depot|Hub|TERMINAL|YARD|RAMP))',
        re.MULTILINE
    )
    code_pattern = re.compile(r'\b([A-Z]{2,5}\d{2,4}[A-Z]?\d?)\b')
    blocks = re.split(r'\n{2,}', text)

    state_defaults = {
        "northeast": "NJ", "midwest": "IL", "southeast": "GA",
        "gulf": "TX", "pacific-nw": "WA", "pacific-sw": "CA"
    }

    for block in blocks:
        block = block.strip()
        if len(block) < 20:
            continue
        upper = block.upper()
        open_pulls = any(k in upper for k in ['OPEN FOR RELEASE','OPEN FOR PULL','ACCEPTING RELEASE','OPEN TO RELEASE'])
        closed_pulls = any(k in upper for k in ['CLOSED TO RELEASE','CLOSED FOR RELEASE','CLOSED TO PULL'])
        open_returns = any(k in upper for k in ['OPEN FOR RETURN','ACCEPTING RETURN','OPEN TO RETURN'])
        closed_returns = any(k in upper for k in ['CLOSED TO RETURN','NOT ACCEPTING RETURN'])

        if not open_pulls and not closed_pulls:
            open_pulls = True
        if not open_returns and not closed_returns:
            open_returns = True

        codes = code_pattern.findall(block)
        release_code = codes[0] if codes else None
        status = 'closed' if closed_pulls else ('open' if open_pulls else 'limited')

        first_line = block.split('\n')[0][:80]
        if len(first_line) > 5:
            results.append({
                'provider': 'DCLI',
                'terminal_name': first_line,
                'region': region,
                'state': state_defaults.get(region, ''),
                'open_for_pulls': open_pulls,
                'open_for_returns': open_returns,
                'release_code': release_code,
                'status': status,
                'notes': block[:300],
                'data_source': 'playwright'
            })

    return results


async def scrape_dcli_all():
    """Scrape all DCLI regional pages with Playwright."""
    if not PLAYWRIGHT_AVAILABLE:
        print("[Scraper] Playwright not available")
        return []
    all_results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        for region, url in DCLI_REGIONS:
            try:
                await page.goto(url, wait_until='networkidle', timeout=25000)
                await page.wait_for_timeout(2500)
                text = await page.inner_text('main') or await page.inner_text('body')
                results = parse_dcli_text(text, region)
                all_results.extend(results)
                print(f"[Scraper] {region}: {len(results)} blocks")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"[Scraper] {region} error: {e}")
        await browser.close()
    return all_results


def save_scraped(results):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0
    for item in results:
        try:
            c.execute("""
                INSERT OR IGNORE INTO terminals (provider, terminal_name, city, state, region)
                VALUES (?,?,?,?,?)
            """, (item['provider'], item['terminal_name'], '', item.get('state',''), item['region']))
            tid = c.execute("SELECT id FROM terminals WHERE terminal_name=?",
                            (item['terminal_name'],)).fetchone()
            if tid:
                c.execute("""
                    INSERT INTO availability
                    (terminal_id, open_for_pulls, open_for_returns, release_code, status, notes, data_source)
                    VALUES (?,?,?,?,?,?,?)
                """, (tid[0], item['open_for_pulls'], item['open_for_returns'],
                      item.get('release_code'), item['status'], item['notes'], item['data_source']))
                saved += 1
        except:
            pass
    conn.commit()
    conn.close()
    print(f"[DB] Saved {saved} scraped records")


def run_scrape_job():
    asyncio.run(scrape_dcli_all_and_save())

async def scrape_dcli_all_and_save():
    results = await scrape_dcli_all()
    if results:
        save_scraped(results)


# ─────────────────────────────────────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory('.', filename)

@app.route('/api/chassis')
def get_chassis():
    location = request.args.get('location', '').lower().strip()
    provider = request.args.get('provider', 'all')
    status_f = request.args.get('status', 'all')
    pulls_only = request.args.get('pulls_only', 'false') == 'true'
    region = request.args.get('region', 'all')

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    sql = """
        SELECT t.id, t.provider, t.terminal_name, t.city, t.state, t.region,
               t.lat, t.lng,
               a.open_for_pulls, a.open_for_returns, a.release_code,
               a.status, a.notes, a.data_source, a.scraped_at,
               (SELECT COUNT(*) FROM driver_reports d
                WHERE d.terminal_id = t.id
                AND d.reported_at > datetime('now','-24 hours')) as driver_reports,
               (SELECT AVG(d.wait_minutes) FROM driver_reports d
                WHERE d.terminal_id = t.id
                AND d.reported_at > datetime('now','-6 hours')) as avg_wait
        FROM terminals t
        LEFT JOIN availability a ON a.id = (
            SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id = t.id
        )
        WHERE 1=1
    """
    params = []

    if location:
        sql += " AND (LOWER(t.city) LIKE ? OR LOWER(t.state) LIKE ? OR LOWER(t.terminal_name) LIKE ? OR LOWER(t.region) LIKE ?)"
        params += [f"%{location}%"] * 4
    if provider != 'all':
        sql += " AND t.provider = ?"
        params.append(provider)
    if status_f != 'all':
        sql += " AND a.status = ?"
        params.append(status_f)
    if pulls_only:
        sql += " AND a.open_for_pulls = 1"
    if region != 'all':
        sql += " AND t.region = ?"
        params.append(region)

    sql += " ORDER BY t.city, t.terminal_name LIMIT 200"

    rows = [dict(r) for r in c.execute(sql, params).fetchall()]
    conn.close()

    return jsonify({
        "results": rows,
        "count": len(rows),
        "timestamp": datetime.now().isoformat(),
        "data_freshness": "seed+scrape"
    })


@app.route('/api/report', methods=['POST'])
def submit_report():
    d = request.get_json() or {}
    if not d.get('terminal_id') or not d.get('status'):
        return jsonify({"error": "terminal_id and status required"}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO driver_reports (terminal_id, chassis_size, status, wait_minutes, notes)
        VALUES (?,?,?,?,?)
    """, (d['terminal_id'], d.get('chassis_size','40'), d['status'],
          d.get('wait_minutes'), d.get('notes','')))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "Report saved"})


@app.route('/api/stats')
def stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    total = c.execute("SELECT COUNT(*) FROM terminals").fetchone()[0]
    open_t = c.execute("""
        SELECT COUNT(*) FROM availability a
        WHERE a.id=(SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id=a.terminal_id)
        AND a.open_for_pulls=1
    """).fetchone()[0]
    closed_t = c.execute("""
        SELECT COUNT(*) FROM availability a
        WHERE a.id=(SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id=a.terminal_id)
        AND a.status='closed'
    """).fetchone()[0]
    reports = c.execute("""
        SELECT COUNT(*) FROM driver_reports
        WHERE reported_at > datetime('now','-24 hours')
    """).fetchone()[0]
    last_scrape = c.execute("SELECT MAX(scraped_at) FROM availability WHERE data_source='playwright'").fetchone()[0]
    conn.close()
    return jsonify({
        "terminals_tracked": total,
        "open_for_pulls": open_t,
        "closed": closed_t,
        "driver_reports_today": reports,
        "playwright_available": PLAYWRIGHT_AVAILABLE,
        "last_live_scrape": last_scrape,
        "timestamp": datetime.now().isoformat()
    })


@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    """Manually trigger a live scrape (call from admin panel)."""
    if not PLAYWRIGHT_AVAILABLE:
        return jsonify({"error": "Playwright not installed on this server"}), 400
    try:
        asyncio.run(scrape_dcli_all_and_save())
        return jsonify({"success": True, "message": "Scrape complete"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/terminals', methods=['GET'])
def list_terminals():
    """List all terminal names for the report modal autocomplete."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT id, terminal_name, city, state, provider FROM terminals ORDER BY city, terminal_name").fetchall()
    conn.close()
    return jsonify([{"id": r[0], "name": r[1], "city": r[2], "state": r[3], "provider": r[4]} for r in rows])


@app.route('/api/health')
def health():
    return jsonify({
        "status": "ok",
        "playwright": PLAYWRIGHT_AVAILABLE,
        "version": "1.1.0",
        "db": DB_PATH
    })


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

def start_scheduler():
    """Start background scraping schedule."""
    if not PLAYWRIGHT_AVAILABLE:
        return
    scheduler = BackgroundScheduler()
    # DCLI updates once per morning — scrape at 7am
    scheduler.add_job(run_scrape_job, 'cron', hour=7, minute=0)
    # Additional midday refresh
    scheduler.add_job(run_scrape_job, 'cron', hour=13, minute=0)
    scheduler.start()
    print("[Scheduler] Scraping scheduled: 7am + 1pm daily")


if __name__ == "__main__":
    print("=" * 60)
    print("ChassisFind — Production App")
    print("=" * 60)
    init_db()
    load_seed_data()
    start_scheduler()

    port = int(os.environ.get("PORT", 5001))
    print(f"[App] Running on http://localhost:{port}")
    print(f"[App] Frontend: http://localhost:{port}/")
    print(f"[App] API:      http://localhost:{port}/api/chassis")
    print(f"[App] Playwright: {PLAYWRIGHT_AVAILABLE}")
    print("=" * 60)

    app.run(host="0.0.0.0", port=port, debug=False)
