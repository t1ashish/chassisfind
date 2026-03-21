"""
ChassisFind - Production App v1.2
Fixed for Render: uses /tmp for DB, explicit port from env
"""
import asyncio
import os
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory, send_file
from flask_cors import CORS

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

app = Flask(__name__, static_folder='.')
CORS(app)

# Use /tmp on Render (writable), local path otherwise
DB_PATH = "/tmp/chassisfind.db" if os.environ.get("RENDER") else "chassisfind.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS terminals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT, terminal_name TEXT UNIQUE,
        city TEXT, state TEXT, region TEXT,
        address TEXT, lat REAL, lng REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS availability (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        terminal_id INTEGER, open_for_pulls INTEGER DEFAULT 1,
        open_for_returns INTEGER DEFAULT 1, release_code TEXT,
        status TEXT DEFAULT 'unknown', notes TEXT, data_source TEXT,
        scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(terminal_id) REFERENCES terminals(id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS driver_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        terminal_id INTEGER, chassis_size TEXT, status TEXT,
        wait_minutes INTEGER, notes TEXT,
        reported_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {DB_PATH}")

SEED_DATA = [
    {"provider":"DCLI","terminal_name":"GCT New York (NYCT)","city":"New York","state":"NY","region":"northeast","open_for_pulls":True,"open_for_returns":True,"release_code":"CHS23","status":"open","notes":"Release CHS23. Chassis must return to NYCT."},
    {"provider":"DCLI","terminal_name":"Port Newark - DCLI","city":"Newark","state":"NJ","region":"northeast","open_for_pulls":True,"open_for_returns":True,"release_code":"CHS24","status":"open","notes":"OPEN for RELEASES and RETURNS. 20s & 40s CHS24."},
    {"provider":"DCLI","terminal_name":"CSX Rail Ramp - Newark","city":"Newark","state":"NJ","region":"northeast","open_for_pulls":False,"open_for_returns":True,"release_code":None,"status":"closed","notes":"Closed to releases. Return all sizes to rail."},
    {"provider":"DCLI","terminal_name":"Chicago Ashland Ave (CSX)","city":"Chicago","state":"IL","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"CHAS2026","status":"open","notes":"Primary Chicago CSX hub. Release CHAS2026."},
    {"provider":"DCLI","terminal_name":"Chicago 63rd St (BNSF)","city":"Chicago","state":"IL","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"CHAS2026","status":"open","notes":"BNSF Chicago. High volume."},
    {"provider":"DCLI","terminal_name":"Columbus Buckeye Yard","city":"Columbus","state":"OH","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"COL2026","status":"limited","notes":"Limited 20ft. 40ft available."},
    {"provider":"DCLI","terminal_name":"Cleveland Intermodal","city":"Cleveland","state":"OH","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"CLE2026","status":"open","notes":"Open all sizes."},
    {"provider":"DCLI","terminal_name":"Kansas City Deramus Yard","city":"Kansas City","state":"MO","region":"midwest","open_for_pulls":True,"open_for_returns":True,"release_code":"KC2026","status":"open","notes":"BNSF/UP Kansas City hub."},
    {"provider":"DCLI","terminal_name":"Atlanta Inman Yard (NS)","city":"Atlanta","state":"GA","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"ATL2026","status":"open","notes":"Norfolk Southern Atlanta hub."},
    {"provider":"TRAC","terminal_name":"Charlotte Inland Port (CSX)","city":"Charlotte","state":"NC","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"CLT2026","status":"open","notes":"CSX Charlotte inland port. Excellent 53ft domestic."},
    {"provider":"TRAC","terminal_name":"Memphis BNSF Ramp","city":"Memphis","state":"TN","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"MEM2026","status":"open","notes":"High inventory across all sizes."},
    {"provider":"DCLI","terminal_name":"Nashville CSX Terminal","city":"Nashville","state":"TN","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"NSH2026","status":"open","notes":"Growing automotive hub."},
    {"provider":"DCLI","terminal_name":"Savannah Garden City Terminal","city":"Savannah","state":"GA","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"SAV2026","status":"open","notes":"Port of Savannah."},
    {"provider":"DCLI","terminal_name":"Jacksonville CSX Intermodal","city":"Jacksonville","state":"FL","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"JAX2026","status":"open","notes":"CSX Jacksonville."},
    {"provider":"DCLI","terminal_name":"Louisville CSXK Hub","city":"Louisville","state":"KY","region":"southeast","open_for_pulls":True,"open_for_returns":True,"release_code":"LOU2026","status":"open","notes":"CSX Louisville hub."},
    {"provider":"DCLI","terminal_name":"Houston Barbours Cut","city":"Houston","state":"TX","region":"gulf","open_for_pulls":True,"open_for_returns":True,"release_code":"HOU2026","status":"open","notes":"Port of Houston marine terminal."},
    {"provider":"DCLI","terminal_name":"Dallas Intermodal Terminal","city":"Dallas","state":"TX","region":"gulf","open_for_pulls":True,"open_for_returns":True,"release_code":"DAL2026","status":"open","notes":"BNSF/UP Dallas."},
    {"provider":"DCLI","terminal_name":"New Orleans NOPB Terminal","city":"New Orleans","state":"LA","region":"gulf","open_for_pulls":True,"open_for_returns":True,"release_code":"NOL2026","status":"open","notes":"Port NOLA marine terminal."},
    {"provider":"DCLI","terminal_name":"Los Angeles TraPac Terminal","city":"Los Angeles","state":"CA","region":"pacific-sw","open_for_pulls":True,"open_for_returns":True,"release_code":"LAX2026","status":"open","notes":"Check east/west rows."},
    {"provider":"DCLI","terminal_name":"Long Beach LBCT","city":"Long Beach","state":"CA","region":"pacific-sw","open_for_pulls":True,"open_for_returns":True,"release_code":"LGB2026","status":"open","notes":"Pool of Pools terminal."},
    {"provider":"TRAC","terminal_name":"Los Angeles BNSF Hobart","city":"Los Angeles","state":"CA","region":"pacific-sw","open_for_pulls":True,"open_for_returns":True,"release_code":"HOB2026","status":"open","notes":"BNSF Hobart yard."},
    {"provider":"DCLI","terminal_name":"Seattle BNSF Intermodal","city":"Seattle","state":"WA","region":"pacific-nw","open_for_pulls":True,"open_for_returns":True,"release_code":"SEA2026","status":"open","notes":"BNSF Seattle."},
    {"provider":"DCLI","terminal_name":"Portland UP Intermodal","city":"Portland","state":"OR","region":"pacific-nw","open_for_pulls":True,"open_for_returns":True,"release_code":"PDX2026","status":"open","notes":"Union Pacific Portland."},
]

LAT_LNG = {
    "New York":(40.712,-74.006),"Newark":(40.735,-74.172),"Chicago":(41.878,-87.629),
    "Columbus":(39.961,-82.998),"Cleveland":(41.499,-81.694),"Kansas City":(39.099,-94.578),
    "Atlanta":(33.748,-84.387),"Charlotte":(35.227,-80.843),"Memphis":(35.149,-90.048),
    "Nashville":(36.174,-86.767),"Savannah":(32.080,-81.099),"Jacksonville":(30.332,-81.655),
    "Louisville":(38.252,-85.758),"Houston":(29.760,-95.369),"Dallas":(32.776,-96.796),
    "New Orleans":(29.951,-90.071),"Los Angeles":(34.052,-118.243),"Long Beach":(33.770,-118.193),
    "Seattle":(47.606,-122.332),"Portland":(45.523,-122.676),
}

def load_seed_data():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    count = 0
    for item in SEED_DATA:
        lat, lng = LAT_LNG.get(item['city'], (0.0, 0.0))
        try:
            c.execute("INSERT OR IGNORE INTO terminals (provider, terminal_name, city, state, region, lat, lng) VALUES (?,?,?,?,?,?,?)",
                (item['provider'], item['terminal_name'], item['city'], item['state'], item['region'], lat, lng))
            tid = c.execute("SELECT id FROM terminals WHERE terminal_name=?", (item['terminal_name'],)).fetchone()
            if tid:
                existing = c.execute("SELECT id FROM availability WHERE terminal_id=?", (tid[0],)).fetchone()
                if not existing:
                    c.execute("INSERT INTO availability (terminal_id, open_for_pulls, open_for_returns, release_code, status, notes, data_source) VALUES (?,?,?,?,?,?,?)",
                        (tid[0], item['open_for_pulls'], item['open_for_returns'], item.get('release_code'), item['status'], item['notes'], 'seed'))
                    count += 1
        except Exception as e:
            print(f"[Seed] {item['terminal_name']}: {e}")
    conn.commit()
    conn.close()
    print(f"[Seed] {count} new terminals loaded ({len(SEED_DATA)} total)")

# Initialize on module load — works with both gunicorn and direct python
print("[ChassisFind] Starting up...")
init_db()
load_seed_data()

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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    sql = """SELECT t.id, t.provider, t.terminal_name, t.city, t.state, t.region, t.lat, t.lng,
               a.open_for_pulls, a.open_for_returns, a.release_code, a.status, a.notes, a.data_source, a.scraped_at,
               (SELECT COUNT(*) FROM driver_reports d WHERE d.terminal_id=t.id AND d.reported_at>datetime('now','-24 hours')) as driver_reports
        FROM terminals t
        LEFT JOIN availability a ON a.id=(SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id=t.id)
        WHERE 1=1"""
    params = []
    if location:
        sql += " AND (LOWER(t.city) LIKE ? OR LOWER(t.state) LIKE ? OR LOWER(t.terminal_name) LIKE ? OR LOWER(t.region) LIKE ?)"
        params += [f"%{location}%"] * 4
    if provider != 'all':
        sql += " AND t.provider=?"; params.append(provider)
    if status_f != 'all':
        sql += " AND a.status=?"; params.append(status_f)
    if pulls_only:
        sql += " AND a.open_for_pulls=1"
    sql += " ORDER BY t.city, t.terminal_name LIMIT 200"
    rows = [dict(r) for r in c.execute(sql, params).fetchall()]
    conn.close()
    return jsonify({"results": rows, "count": len(rows), "timestamp": datetime.now().isoformat()})

@app.route('/api/report', methods=['POST'])
def submit_report():
    d = request.get_json() or {}
    if not d.get('terminal_id') or not d.get('status'):
        return jsonify({"error": "terminal_id and status required"}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO driver_reports (terminal_id, chassis_size, status, wait_minutes, notes) VALUES (?,?,?,?,?)",
        (d['terminal_id'], d.get('chassis_size','40'), d['status'], d.get('wait_minutes'), d.get('notes','')))
    conn.commit(); conn.close()
    return jsonify({"success": True})

@app.route('/api/stats')
def stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    total = c.execute("SELECT COUNT(*) FROM terminals").fetchone()[0]
    open_t = c.execute("SELECT COUNT(*) FROM availability a WHERE a.id=(SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id=a.terminal_id) AND a.open_for_pulls=1").fetchone()[0]
    reports = c.execute("SELECT COUNT(*) FROM driver_reports WHERE reported_at>datetime('now','-24 hours')").fetchone()[0]
    conn.close()
    return jsonify({"terminals_tracked":total,"open_for_pulls":open_t,"driver_reports_today":reports,"playwright":PLAYWRIGHT_AVAILABLE,"timestamp":datetime.now().isoformat()})

@app.route('/api/terminals')
def list_terminals():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT id, terminal_name, city, state, provider FROM terminals ORDER BY city").fetchall()
    conn.close()
    return jsonify([{"id":r[0],"name":r[1],"city":r[2],"state":r[3],"provider":r[4]} for r in rows])

@app.route('/api/health')
def health():
    return jsonify({"status":"ok","playwright":PLAYWRIGHT_AVAILABLE,"version":"1.2.0","db":DB_PATH})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
