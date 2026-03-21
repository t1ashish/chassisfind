"""
ChassisFind MVP - Data Aggregation Backend
==========================================
Scrapes chassis availability from:
1. DCLI regional pages (publicly available)
2. TRAC terminal locator (publicly available)
3. Flexi-Van locations (publicly available)
4. Driver crowdsource reports (your own database)

Run: python scraper.py
Requires: pip install requests beautifulsoup4 flask flask-cors apscheduler
"""

import requests
from bs4 import BeautifulSoup
import json
import sqlite3
import time
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)

DB_PATH = "chassisfind.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ChassisFind/1.0; +chassis availability aggregator)"
}

# ─────────────────────────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS terminals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            terminal_name TEXT NOT NULL,
            terminal_code TEXT,
            city TEXT,
            state TEXT,
            address TEXT,
            region TEXT,
            lat REAL,
            lng REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS availability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            terminal_id INTEGER,
            size_20 INTEGER DEFAULT 0,
            size_40 INTEGER DEFAULT 0,
            size_45 INTEGER DEFAULT 0,
            size_53 INTEGER DEFAULT 0,
            open_for_pulls BOOLEAN DEFAULT 1,
            open_for_returns BOOLEAN DEFAULT 1,
            release_code TEXT,
            status TEXT DEFAULT 'unknown',
            notes TEXT,
            data_source TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (terminal_id) REFERENCES terminals(id)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS driver_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            terminal_id INTEGER,
            reporter_id TEXT,
            chassis_size TEXT,
            status TEXT,
            wait_minutes INTEGER,
            notes TEXT,
            reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (terminal_id) REFERENCES terminals(id)
        )
    """)
    
    conn.commit()
    conn.close()
    print("[DB] Database initialized")


# ─────────────────────────────────────────────────────────────────
# SCRAPER: DCLI
# Source: https://dcli.com/regions-listing/
# DCLI publishes per-region pages with depot availability
# ─────────────────────────────────────────────────────────────────

DCLI_REGIONS = {
    "midwest":       "https://dcli.com/region/midwest/",
    "northeast":     "https://dcli.com/region/northeast/",
    "southeast":     "https://dcli.com/region/southeast/",
    "gulf":          "https://dcli.com/region/gulf/",
    "pacific-nw":    "https://dcli.com/region/pacific-northwest/",
    "pacific-sw":    "https://dcli.com/region/pacific-southwest/",
}

def scrape_dcli():
    """
    Scrape DCLI regional availability pages.
    Note: DCLI updates inventory once per morning.
    Their pages show open/closed status per depot.
    """
    results = []
    
    for region, url in DCLI_REGIONS.items():
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                print(f"[DCLI] {region}: HTTP {resp.status_code}")
                continue
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # DCLI pages contain depot info in structured divs/tables
            # Parse depot blocks (structure may vary — inspect live page)
            # This is a template — adapt selectors to match actual DCLI HTML
            
            depot_blocks = soup.find_all('div', class_='depot-block')  # adjust selector
            if not depot_blocks:
                # Try alternate structure
                depot_blocks = soup.find_all('section', class_='terminal')
            
            for block in depot_blocks:
                name = block.find('h3') or block.find('h2')
                status_el = block.find('span', class_='status')
                notes_el = block.find('p', class_='notes')
                
                if not name:
                    continue
                
                # Extract text and normalize
                terminal_name = name.get_text(strip=True)
                status_text = status_el.get_text(strip=True).lower() if status_el else ''
                notes = notes_el.get_text(strip=True) if notes_el else ''
                
                # Parse open/closed from status text
                open_for_pulls = 'open' in status_text or 'available' in status_text
                open_for_returns = 'return' in status_text or 'open' in status_text
                
                # Look for release code (format: XXXDDDD)
                import re
                code_match = re.search(r'\b[A-Z]{3,4}\d{4}[A-Z]?\b', notes)
                release_code = code_match.group(0) if code_match else None
                
                results.append({
                    "provider": "DCLI",
                    "terminal_name": terminal_name,
                    "region": region,
                    "open_for_pulls": open_for_pulls,
                    "open_for_returns": open_for_returns,
                    "release_code": release_code,
                    "notes": notes,
                    "data_source": "scrape",
                    "status": "open" if open_for_pulls else "closed"
                })
            
            print(f"[DCLI] {region}: {len(depot_blocks)} depots scraped")
            time.sleep(1)  # be polite — don't hammer the server
            
        except Exception as e:
            print(f"[DCLI] {region} error: {e}")
    
    return results


# ─────────────────────────────────────────────────────────────────
# SCRAPER: TRAC INTERMODAL  
# Source: https://www.tracintermodal.com/locations
# ─────────────────────────────────────────────────────────────────

def scrape_trac():
    """
    Scrape TRAC Intermodal location data.
    TRAC's site has a location finder — we extract terminal details.
    """
    results = []
    url = "https://www.tracintermodal.com/locations"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # TRAC uses a location directory — adapt selectors to actual HTML
        location_items = soup.find_all('div', class_='location-item')
        
        for item in location_items:
            name = item.find('h4') or item.find('h3')
            address = item.find('address') or item.find('p', class_='address')
            phone = item.find('a', href=lambda h: h and 'tel:' in h)
            
            if not name:
                continue
            
            results.append({
                "provider": "TRAC",
                "terminal_name": name.get_text(strip=True),
                "address": address.get_text(strip=True) if address else "",
                "phone": phone.get_text(strip=True) if phone else "",
                "data_source": "scrape",
                "status": "open",  # TRAC doesn't publish availability publicly
                "notes": "Contact TRAC directly for current availability"
            })
        
        print(f"[TRAC] {len(results)} locations scraped")
        
    except Exception as e:
        print(f"[TRAC] scrape error: {e}")
    
    return results


# ─────────────────────────────────────────────────────────────────
# OPTIONAL: OPENTRACK API INTEGRATION
# OpenTrack has direct Class 1 railroad integrations
# Provides chassis numbers from rail events
# Sign up at: https://www.opentrack.co
# ─────────────────────────────────────────────────────────────────

def fetch_opentrack(api_key, terminal_splc=None):
    """
    Fetch chassis data from OpenTrack API.
    Returns chassis numbers associated with rail events at a terminal.
    
    Requires: OpenTrack API subscription
    Docs: https://docs.opentrack.co/api
    """
    if not api_key:
        return []
    
    url = "https://api.opentrack.co/v1/equipment"
    params = {
        "terminal": terminal_splc,
        "equipment_type": "chassis",
        "status": "available",
        "limit": 100
    }
    headers = {
        **HEADERS,
        "Authorization": f"Bearer {api_key}"
    }
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("equipment", [])
    except Exception as e:
        print(f"[OpenTrack] API error: {e}")
    
    return []


# ─────────────────────────────────────────────────────────────────
# DATABASE OPERATIONS
# ─────────────────────────────────────────────────────────────────

def upsert_availability(data):
    """Save scraped availability data to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for item in data:
        # Upsert terminal
        c.execute("""
            INSERT INTO terminals (provider, terminal_name, city, state, region)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(terminal_name) DO UPDATE SET
                region = excluded.region
        """, (
            item.get("provider"),
            item.get("terminal_name"),
            item.get("city", ""),
            item.get("state", ""),
            item.get("region", "")
        ))
        
        terminal_id = c.lastrowid or c.execute(
            "SELECT id FROM terminals WHERE terminal_name = ?",
            (item.get("terminal_name"),)
        ).fetchone()[0]
        
        # Insert availability snapshot
        c.execute("""
            INSERT INTO availability (
                terminal_id, size_20, size_40, size_45, size_53,
                open_for_pulls, open_for_returns, release_code,
                status, notes, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            terminal_id,
            item.get("size_20", 0),
            item.get("size_40", 0),
            item.get("size_45", 0),
            item.get("size_53", 0),
            item.get("open_for_pulls", True),
            item.get("open_for_returns", True),
            item.get("release_code"),
            item.get("status", "unknown"),
            item.get("notes", ""),
            item.get("data_source", "scrape")
        ))
    
    conn.commit()
    conn.close()
    print(f"[DB] Saved {len(data)} availability records")


# ─────────────────────────────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────────────────────────────

@app.route("/api/chassis", methods=["GET"])
def get_chassis():
    """
    Search chassis availability.
    
    Query params:
    - location: city, state, zip, or terminal name
    - size: 20, 40, 45, 53
    - provider: DCLI, TRAC, FLEXI, all
    - radius: miles (25, 50, 100, 200)
    - status: open, limited, closed, all
    """
    location = request.args.get("location", "").lower()
    size = request.args.get("size", "all")
    provider = request.args.get("provider", "all")
    status_filter = request.args.get("status", "all")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    query = """
        SELECT 
            t.id, t.provider, t.terminal_name, t.city, t.state,
            t.address, t.region, t.lat, t.lng,
            a.size_20, a.size_40, a.size_45, a.size_53,
            a.open_for_pulls, a.open_for_returns, a.release_code,
            a.status, a.notes, a.data_source, a.scraped_at,
            (SELECT COUNT(*) FROM driver_reports dr WHERE dr.terminal_id = t.id 
             AND dr.reported_at > datetime('now', '-24 hours')) as driver_reports,
            (SELECT AVG(dr.wait_minutes) FROM driver_reports dr 
             WHERE dr.terminal_id = t.id 
             AND dr.reported_at > datetime('now', '-4 hours')) as avg_wait
        FROM terminals t
        LEFT JOIN availability a ON a.terminal_id = t.id
        WHERE a.id = (
            SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id = t.id
        )
    """
    
    params = []
    conditions = []
    
    if location:
        conditions.append("(LOWER(t.city) LIKE ? OR LOWER(t.state) LIKE ? OR LOWER(t.terminal_name) LIKE ?)")
        params.extend([f"%{location}%", f"%{location}%", f"%{location}%"])
    
    if provider != "all":
        conditions.append("t.provider = ?")
        params.append(provider)
    
    if status_filter != "all":
        conditions.append("a.status = ?")
        params.append(status_filter)
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    if size != "all":
        size_col = f"a.size_{size}"
        query += f" AND {size_col} > 0"
    
    query += " ORDER BY a.scraped_at DESC LIMIT 100"
    
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    
    results = []
    for row in rows:
        r = dict(row)
        r["inventory"] = {
            "20": r.pop("size_20", 0),
            "40": r.pop("size_40", 0),
            "45": r.pop("size_45", 0),
            "53": r.pop("size_53", 0),
        }
        r["total_available"] = sum(r["inventory"].values())
        results.append(r)
    
    return jsonify({
        "results": results,
        "count": len(results),
        "timestamp": datetime.now().isoformat()
    })


@app.route("/api/report", methods=["POST"])
def submit_report():
    """Submit a driver crowdsource report."""
    data = request.get_json()
    
    required = ["terminal_id", "status", "chassis_size"]
    if not all(k in data for k in required):
        return jsonify({"error": "Missing required fields"}), 400
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        INSERT INTO driver_reports (terminal_id, reporter_id, chassis_size, status, wait_minutes, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        data["terminal_id"],
        data.get("reporter_id", "anonymous"),
        data["chassis_size"],
        data["status"],
        data.get("wait_minutes"),
        data.get("notes", "")
    ))
    
    # Update terminal availability based on driver report
    if data["status"] == "none":
        c.execute(f"""
            UPDATE availability SET status = 'closed', size_{data['chassis_size']} = 0
            WHERE terminal_id = ? ORDER BY id DESC LIMIT 1
        """, (data["terminal_id"],))
    
    conn.commit()
    conn.close()
    
    return jsonify({"success": True, "message": "Report submitted — data updated"})


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Get aggregate platform statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    total_terminals = c.execute("SELECT COUNT(*) FROM terminals").fetchone()[0]
    total_available = c.execute("""
        SELECT COALESCE(SUM(size_20+size_40+size_45+size_53), 0) 
        FROM availability a 
        WHERE a.id = (SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id = a.terminal_id)
    """).fetchone()[0]
    avg_wait = c.execute("""
        SELECT COALESCE(AVG(wait_minutes), 0) FROM driver_reports 
        WHERE reported_at > datetime('now', '-4 hours')
    """).fetchone()[0]
    reports_today = c.execute("""
        SELECT COUNT(*) FROM driver_reports 
        WHERE reported_at > datetime('now', '-24 hours')
    """).fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "terminals_tracked": total_terminals,
        "total_available": int(total_available),
        "avg_wait_minutes": round(avg_wait),
        "driver_reports_today": reports_today,
        "last_sync": datetime.now().isoformat()
    })


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": "1.0.0-mvp"})


# ─────────────────────────────────────────────────────────────────
# SCHEDULED SCRAPING
# ─────────────────────────────────────────────────────────────────

def run_all_scrapers():
    """Run all scrapers and save results. Called on schedule."""
    print(f"\n[Scraper] Starting sync at {datetime.now().isoformat()}")
    
    all_results = []
    
    # Scrape DCLI (updates once per morning — run at 6:30am)
    dcli_data = scrape_dcli()
    all_results.extend(dcli_data)
    print(f"[DCLI] Collected {len(dcli_data)} records")
    
    # Scrape TRAC
    trac_data = scrape_trac()
    all_results.extend(trac_data)
    print(f"[TRAC] Collected {len(trac_data)} records")
    
    if all_results:
        upsert_availability(all_results)
    
    print(f"[Scraper] Sync complete — {len(all_results)} total records saved")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("ChassisFind MVP Backend")
    print("=" * 60)
    
    # Init database
    init_db()
    
    # Schedule scrapers
    scheduler = BackgroundScheduler()
    
    # DCLI updates once per morning — scrape at 6:30am
    scheduler.add_job(run_all_scrapers, 'cron', hour=6, minute=30)
    
    # Also run every 2 hours for TRAC/other sources
    scheduler.add_job(run_all_scrapers, 'interval', hours=2)
    
    scheduler.start()
    print("[Scheduler] Scraping jobs scheduled")
    
    # Run initial scrape on startup
    # run_all_scrapers()  # uncomment for production
    
    print("[API] Starting Flask server on http://localhost:5000")
    print("[API] Endpoints:")
    print("  GET  /api/chassis?location=chicago&size=40&provider=DCLI")
    print("  POST /api/report   (driver crowdsource report)")
    print("  GET  /api/stats    (platform statistics)")
    print("  GET  /api/health   (health check)")
    print("=" * 60)
    
    app.run(debug=True, port=5000)
