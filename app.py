"""
ChassisFind v2.0 - Multi-Source Data Aggregator
================================================
Free data sources:
  1. DCLI regional pages (6 regions) - scrape at 6:30am daily
  2. TRAC chassis-availability page  - scrape at 6:30am + 12:30pm + 5:30pm
  3. Pool of Pools LA/LB KPIs        - scrape daily (public metrics)
  4. BTS/NTAD facility directory     - loaded once on startup (gov open data)
  5. Driver crowdsource reports      - your proprietary real-time layer
"""
import asyncio
import os
import re
import sqlite3
import json
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__)
CORS(app)

DB_PATH = "/tmp/chassisfind.db" if os.environ.get("RENDER") else "chassisfind.db"

# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS terminals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT, terminal_name TEXT UNIQUE,
        city TEXT, state TEXT, region TEXT,
        lat REAL DEFAULT 0.0, lng REAL DEFAULT 0.0,
        splc TEXT, pool TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS availability (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        terminal_id INTEGER, open_for_pulls INTEGER DEFAULT 1,
        open_for_returns INTEGER DEFAULT 1, release_code TEXT,
        status TEXT DEFAULT 'unknown', notes TEXT,
        chassis_20ft INTEGER, chassis_40ft INTEGER, chassis_45ft INTEGER, chassis_53ft INTEGER,
        oos_ratio REAL, street_dwell REAL, terminal_dwell REAL,
        data_source TEXT,
        scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(terminal_id) REFERENCES terminals(id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS driver_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        terminal_id INTEGER, chassis_size TEXT, status TEXT,
        wait_minutes INTEGER, notes TEXT, driver_name TEXT,
        reported_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS scrape_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT, records_added INTEGER, status TEXT,
        ran_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {DB_PATH}")


# ─────────────────────────────────────────────────────────────────
# SEED DATA — 40 terminals across all providers
# ─────────────────────────────────────────────────────────────────

SEED_DATA = [
    # DCLI Northeast
    {"provider":"DCLI","terminal_name":"GCT New York (NYCT)","city":"New York","state":"NY","region":"northeast","lat":40.672,"lng":-74.073,"open_for_pulls":True,"open_for_returns":True,"release_code":"CHS23","status":"open","notes":"Release CHS23. Chassis must return to NYCT.","pool":"DCLI Northeast"},
    {"provider":"DCLI","terminal_name":"Port Newark - DCLI","city":"Newark","state":"NJ","region":"northeast","lat":40.697,"lng":-74.152,"open_for_pulls":True,"open_for_returns":True,"release_code":"CHS24","status":"open","notes":"OPEN for RELEASES and RETURNS. 20s & 40s CHS24.","pool":"DCLI Northeast"},
    {"provider":"DCLI","terminal_name":"CSX Rail Ramp - Newark","city":"Newark","state":"NJ","region":"northeast","lat":40.735,"lng":-74.172,"open_for_pulls":False,"open_for_returns":True,"release_code":None,"status":"closed","notes":"Closed to releases. Return all sizes to rail.","pool":"DCLI Northeast"},
    {"provider":"DCLI","terminal_name":"GCT Bayonne","city":"Bayonne","state":"NJ","region":"northeast","lat":40.669,"lng":-74.115,"open_for_pulls":True,"open_for_returns":True,"release_code":"BAY24","status":"open","notes":"Marine terminal. 40ft and 45ft.","pool":"DCLI Northeast"},
    {"provider":"TRAC","terminal_name":"PNCT - Port Newark Container Terminal","city":"Newark","state":"NJ","region":"northeast","lat":40.700,"lng":-74.148,"open_for_pulls":True,"open_for_returns":True,"release_code":"PNCT24","status":"open","notes":"TRAC Metro Pool. Major NJ terminal.","pool":"TRAC Metro"},
    {"provider":"TRAC","terminal_name":"Maher Terminals Newark","city":"Newark","state":"NJ","region":"northeast","lat":40.695,"lng":-74.155,"open_for_pulls":True,"open_for_returns":True,"release_code":"MAHER24","status":"open","notes":"TRAC Metro Pool.","pool":"TRAC Metro"},
    # DCLI Midwest
    {"provider":"DCLI","terminal_name":"Chicago Ashland Ave (CSX)","city":"Chicago","state":"IL","region":"midwest","lat":41.852,"lng":-87.667,"open_for_pulls":True,"open_for_returns":True,"release_code":"CHAS2026","status":"open","notes":"Primary Chicago CSX hub. Release CHAS2026.","pool":"DCLI Midwest"},
    {"provider":"DCLI","terminal_name":"Chicago 63rd St (BNSF)","city":"Chicago","state":"IL","region":"midwest","lat":41.780,"lng":-87.640,"open_for_pulls":True,"open_for_returns":True,"release_code":"CHAS2026","status":"open","notes":"BNSF Chicago. High volume.","pool":"DCLI Midwest"},
    {"provider":"TRAC","terminal_name":"Chicago Global III Intermodal (UP)","city":"Chicago","state":"IL","region":"midwest","lat":41.846,"lng":-87.802,"open_for_pulls":True,"open_for_returns":True,"release_code":"CHUP26","status":"open","notes":"TRAC Eastern Pool. Union Pacific Chicago.","pool":"TRAC Eastern"},
    {"provider":"DCLI","terminal_name":"Columbus Buckeye Yard","city":"Columbus","state":"OH","region":"midwest","lat":39.983,"lng":-82.910,"open_for_pulls":True,"open_for_returns":True,"release_code":"COL2026","status":"limited","notes":"Limited 20ft. 40ft available.","pool":"DCLI Midwest"},
    {"provider":"DCLI","terminal_name":"Cleveland Intermodal","city":"Cleveland","state":"OH","region":"midwest","lat":41.499,"lng":-81.694,"open_for_pulls":True,"open_for_returns":True,"release_code":"CLE2026","status":"open","notes":"Open all sizes.","pool":"DCLI Midwest"},
    {"provider":"DCLI","terminal_name":"Kansas City Deramus Yard","city":"Kansas City","state":"MO","region":"midwest","lat":39.100,"lng":-94.596,"open_for_pulls":True,"open_for_returns":True,"release_code":"KC2026","status":"open","notes":"BNSF/UP Kansas City hub.","pool":"DCLI Midwest"},
    {"provider":"DCLI","terminal_name":"Indianapolis Avon Yard","city":"Indianapolis","state":"IN","region":"midwest","lat":39.791,"lng":-86.292,"open_for_pulls":True,"open_for_returns":True,"release_code":"IND26","status":"open","notes":"CSX Avon Yard. Growing e-commerce hub.","pool":"DCLI Midwest"},
    {"provider":"TRAC","terminal_name":"Detroit Intermodal Freight Terminal","city":"Detroit","state":"MI","region":"midwest","lat":42.371,"lng":-83.150,"open_for_pulls":True,"open_for_returns":True,"release_code":"DET26","status":"open","notes":"TRAC Eastern Pool. Auto-adjacent market.","pool":"TRAC Eastern"},
    # Southeast
    {"provider":"DCLI","terminal_name":"Atlanta Inman Yard (NS)","city":"Atlanta","state":"GA","region":"southeast","lat":33.760,"lng":-84.410,"open_for_pulls":True,"open_for_returns":True,"release_code":"ATL2026","status":"open","notes":"Norfolk Southern Atlanta hub.","pool":"DCLI Southeast"},
    {"provider":"TRAC","terminal_name":"Charlotte Inland Port (CSX)","city":"Charlotte","state":"NC","region":"southeast","lat":35.267,"lng":-80.899,"open_for_pulls":True,"open_for_returns":True,"release_code":"CLT2026","status":"open","notes":"CSX Charlotte inland port.","pool":"TRAC Eastern"},
    {"provider":"TRAC","terminal_name":"Memphis BNSF Ramp","city":"Memphis","state":"TN","region":"southeast","lat":35.140,"lng":-90.040,"open_for_pulls":True,"open_for_returns":True,"release_code":"MEM2026","status":"open","notes":"High inventory across all sizes.","pool":"TRAC Eastern"},
    {"provider":"DCLI","terminal_name":"Nashville CSX Terminal","city":"Nashville","state":"TN","region":"southeast","lat":36.186,"lng":-86.815,"open_for_pulls":True,"open_for_returns":True,"release_code":"NSH2026","status":"open","notes":"Growing automotive hub.","pool":"DCLI Southeast"},
    {"provider":"DCLI","terminal_name":"Savannah Garden City Terminal","city":"Savannah","state":"GA","region":"southeast","lat":32.096,"lng":-81.193,"open_for_pulls":True,"open_for_returns":True,"release_code":"SAV2026","status":"open","notes":"Port of Savannah. 3rd busiest US port.","pool":"DCLI Southeast"},
    {"provider":"DCLI","terminal_name":"Jacksonville CSX Intermodal","city":"Jacksonville","state":"FL","region":"southeast","lat":30.398,"lng":-81.654,"open_for_pulls":True,"open_for_returns":True,"release_code":"JAX2026","status":"open","notes":"CSX Jacksonville.","pool":"DCLI Southeast"},
    {"provider":"DCLI","terminal_name":"Louisville CSXK Hub","city":"Louisville","state":"KY","region":"southeast","lat":38.273,"lng":-85.741,"open_for_pulls":True,"open_for_returns":True,"release_code":"LOU2026","status":"open","notes":"CSX Louisville hub.","pool":"DCLI Southeast"},
    {"provider":"DCLI","terminal_name":"Birmingham NS Intermodal","city":"Birmingham","state":"AL","region":"southeast","lat":33.518,"lng":-86.810,"open_for_pulls":True,"open_for_returns":True,"release_code":"BHM26","status":"open","notes":"Norfolk Southern Birmingham.","pool":"DCLI Southeast"},
    {"provider":"TRAC","terminal_name":"Raleigh-Durham Intermodal","city":"Raleigh","state":"NC","region":"southeast","lat":35.867,"lng":-78.638,"open_for_pulls":True,"open_for_returns":True,"release_code":"RDU26","status":"limited","notes":"TRAC Eastern. Limited 20ft.","pool":"TRAC Eastern"},
    # Gulf
    {"provider":"DCLI","terminal_name":"Houston Barbours Cut","city":"Houston","state":"TX","region":"gulf","lat":29.726,"lng":-95.015,"open_for_pulls":True,"open_for_returns":True,"release_code":"HOU2026","status":"open","notes":"Port of Houston marine terminal.","pool":"DCLI Gulf"},
    {"provider":"DCLI","terminal_name":"Houston Bayport Terminal","city":"Houston","state":"TX","region":"gulf","lat":29.614,"lng":-95.020,"open_for_pulls":True,"open_for_returns":True,"release_code":"BAY26","status":"open","notes":"Port of Houston Bayport.","pool":"DCLI Gulf"},
    {"provider":"DCLI","terminal_name":"Dallas Intermodal Terminal","city":"Dallas","state":"TX","region":"gulf","lat":32.797,"lng":-96.878,"open_for_pulls":True,"open_for_returns":True,"release_code":"DAL2026","status":"open","notes":"BNSF/UP Dallas.","pool":"DCLI Gulf"},
    {"provider":"DCLI","terminal_name":"New Orleans NOPB Terminal","city":"New Orleans","state":"LA","region":"gulf","lat":29.986,"lng":-90.057,"open_for_pulls":True,"open_for_returns":True,"release_code":"NOL2026","status":"open","notes":"Port NOLA marine terminal.","pool":"DCLI Gulf"},
    {"provider":"TRAC","terminal_name":"Houston BNSF - Pearland","city":"Houston","state":"TX","region":"gulf","lat":29.703,"lng":-95.369,"open_for_pulls":True,"open_for_returns":True,"release_code":"HOUP26","status":"open","notes":"TRAC Gulf Pool. BNSF Houston yard.","pool":"TRAC Gulf"},
    # Pacific SW — Pool of Pools territory
    {"provider":"DCLI","terminal_name":"Los Angeles TraPac Terminal","city":"Los Angeles","state":"CA","region":"pacific-sw","lat":33.737,"lng":-118.266,"open_for_pulls":True,"open_for_returns":True,"release_code":"LAX2026","status":"open","notes":"Pool of Pools. Check east/west rows.","pool":"DCLP (Pool of Pools)"},
    {"provider":"DCLI","terminal_name":"Long Beach LBCT","city":"Long Beach","state":"CA","region":"pacific-sw","lat":33.755,"lng":-118.223,"open_for_pulls":True,"open_for_returns":True,"release_code":"LGB2026","status":"open","notes":"Pool of Pools terminal.","pool":"DCLP (Pool of Pools)"},
    {"provider":"DCLI","terminal_name":"APM Terminals Los Angeles","city":"Los Angeles","state":"CA","region":"pacific-sw","lat":33.741,"lng":-118.262,"open_for_pulls":True,"open_for_returns":True,"release_code":"APM26","status":"open","notes":"Pool of Pools. One of largest US terminals.","pool":"DCLP (Pool of Pools)"},
    {"provider":"TRAC","terminal_name":"Los Angeles BNSF Hobart","city":"Los Angeles","state":"CA","region":"pacific-sw","lat":33.988,"lng":-118.232,"open_for_pulls":True,"open_for_returns":True,"release_code":"HOB2026","status":"open","notes":"TRAC Pacific SW Pool. BNSF Hobart inland yard.","pool":"TPSP (Pool of Pools)"},
    {"provider":"TRAC","terminal_name":"Long Beach Pacific Container Terminal","city":"Long Beach","state":"CA","region":"pacific-sw","lat":33.751,"lng":-118.205,"open_for_pulls":True,"open_for_returns":True,"release_code":"PCT26","status":"open","notes":"TRAC Pacific SW Pool.","pool":"TPSP (Pool of Pools)"},
    {"provider":"TRAC","terminal_name":"Everport Terminal Los Angeles","city":"Los Angeles","state":"CA","region":"pacific-sw","lat":33.735,"lng":-118.272,"open_for_pulls":True,"open_for_returns":True,"release_code":"EVR26","status":"open","notes":"TRAC Pacific SW Pool.","pool":"TPSP (Pool of Pools)"},
    # Pacific NW
    {"provider":"DCLI","terminal_name":"Seattle BNSF Intermodal (SIG)","city":"Seattle","state":"WA","region":"pacific-nw","lat":47.531,"lng":-122.349,"open_for_pulls":True,"open_for_returns":True,"release_code":"SEA2026","status":"open","notes":"BNSF Seattle International Gateway.","pool":"DCLI Pacific NW"},
    {"provider":"DCLI","terminal_name":"Seattle South Intermodal (BNSF)","city":"Seattle","state":"WA","region":"pacific-nw","lat":47.505,"lng":-122.347,"open_for_pulls":True,"open_for_returns":True,"release_code":"SEA2026","status":"open","notes":"BNSF South Seattle facility.","pool":"DCLI Pacific NW"},
    {"provider":"DCLI","terminal_name":"Portland BNSF Intermodal","city":"Portland","state":"OR","region":"pacific-nw","lat":45.561,"lng":-122.670,"open_for_pulls":True,"open_for_returns":True,"release_code":"PDX2026","status":"open","notes":"BNSF Portland facility.","pool":"DCLI Pacific NW"},
    {"provider":"DCLI","terminal_name":"Portland UP Intermodal","city":"Portland","state":"OR","region":"pacific-nw","lat":45.523,"lng":-122.676,"open_for_pulls":True,"open_for_returns":True,"release_code":"PDX2026","status":"open","notes":"Union Pacific Portland.","pool":"DCLI Pacific NW"},
    {"provider":"DCLI","terminal_name":"Tacoma Intermodal","city":"Tacoma","state":"WA","region":"pacific-nw","lat":47.259,"lng":-122.414,"open_for_pulls":True,"open_for_returns":True,"release_code":"TAC26","status":"open","notes":"Port of Tacoma.","pool":"DCLI Pacific NW"},
    {"provider":"TRAC","terminal_name":"Portland ConGlobal (CGI)","city":"Portland","state":"OR","region":"pacific-nw","lat":45.541,"lng":-122.614,"open_for_pulls":True,"open_for_returns":True,"release_code":"PDXT26","status":"open","notes":"TRAC Pacific NW. ConGlobal operated.","pool":"TRAC Pacific NW"},
]


def load_seed_data():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    count = 0
    for item in SEED_DATA:
        try:
            c.execute("""INSERT OR IGNORE INTO terminals
                (provider, terminal_name, city, state, region, lat, lng, pool)
                VALUES (?,?,?,?,?,?,?,?)""",
                (item['provider'], item['terminal_name'], item['city'],
                 item['state'], item['region'], item.get('lat',0.0),
                 item.get('lng',0.0), item.get('pool','')))
            tid = c.execute("SELECT id FROM terminals WHERE terminal_name=?",
                            (item['terminal_name'],)).fetchone()
            if tid:
                existing = c.execute("SELECT id FROM availability WHERE terminal_id=?",
                                     (tid[0],)).fetchone()
                if not existing:
                    c.execute("""INSERT INTO availability
                        (terminal_id, open_for_pulls, open_for_returns,
                         release_code, status, notes, data_source)
                        VALUES (?,?,?,?,?,?,?)""",
                        (tid[0], item['open_for_pulls'], item['open_for_returns'],
                         item.get('release_code'), item['status'],
                         item['notes'], 'seed'))
                    count += 1
        except Exception as e:
            print(f"[Seed] {item['terminal_name']}: {e}")
    conn.commit()
    conn.close()
    print(f"[Seed] {count} new terminals loaded ({len(SEED_DATA)} total)")


# ─────────────────────────────────────────────────────────────────
# SOURCE 1: DCLI REGIONAL SCRAPER
# ─────────────────────────────────────────────────────────────────

DCLI_REGIONS = [
    ("northeast",  "https://dcli.com/region/northeast/"),
    ("southeast",  "https://dcli.com/region/southeast/"),
    ("midwest",    "https://dcli.com/region/midwest/"),
    ("gulf",       "https://dcli.com/region/gulf/"),
    ("pacific-nw", "https://dcli.com/region/pacific-northwest/"),
    ("pacific-sw", "https://dcli.com/region/pacific-southwest/"),
]

async def scrape_dcli():
    """Scrape DCLI's 6 regional pages for live availability."""
    if not PLAYWRIGHT_AVAILABLE:
        return 0
    added = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for region, url in DCLI_REGIONS:
            try:
                page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                await page.goto(url, wait_until="networkidle", timeout=25000)
                await page.wait_for_timeout(2000)
                text = await page.inner_text("body")
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                # Parse terminal blocks from DCLI page text
                release_pat = re.compile(r'\b([A-Z]{2,5}\d{2,4}[A-Z]?\d?)\b')
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                for line in lines:
                    lu = line.upper()
                    if any(kw in lu for kw in ["TERMINAL","YARD","RAMP","PORT","DEPOT"]) and len(line) < 120:
                        open_pulls = any(kw in lu for kw in ["OPEN","AVAILABLE","RELEASING"])
                        closed = any(kw in lu for kw in ["CLOSED","NO RELEASE","NOT ACCEPTING"])
                        codes = release_pat.findall(line)
                        release_code = codes[0] if codes else None
                        status = "closed" if closed else ("open" if open_pulls else "unknown")
                        # Upsert
                        c.execute("INSERT OR IGNORE INTO terminals (provider, terminal_name, region) VALUES (?,?,?)",
                                  ("DCLI", line[:120], region))
                        tid = c.execute("SELECT id FROM terminals WHERE terminal_name=?", (line[:120],)).fetchone()
                        if tid:
                            c.execute("""INSERT INTO availability
                                (terminal_id, open_for_pulls, open_for_returns, release_code, status, notes, data_source)
                                VALUES (?,?,?,?,?,?,?)""",
                                (tid[0], 1 if open_pulls else 0, 1, release_code, status,
                                 f"DCLI {region} live scrape", "dcli_scrape"))
                            added += 1
                conn.commit()
                conn.close()
                await page.close()
            except Exception as e:
                print(f"[DCLI] {region}: {e}")
        await browser.close()
    print(f"[DCLI] Scraped {added} records")
    log_scrape("dcli_scrape", added, "ok")
    return added


# ─────────────────────────────────────────────────────────────────
# SOURCE 2: TRAC CHASSIS AVAILABILITY PAGE
# Updates 3x daily. Scrape right after each update window.
# ─────────────────────────────────────────────────────────────────

TRAC_REGIONS = {
    "METRO": ["Newark", "New York", "Philadelphia", "Baltimore"],
    "EASTERN": ["Chicago", "Cleveland", "Columbus", "Detroit", "Memphis", "Nashville"],
    "MIDWEST": ["Kansas City", "St. Louis", "Minneapolis", "Omaha"],
    "WESTERN": ["Minneapolis", "Chippewa"],
    "PACIFIC SW": ["Los Angeles", "Long Beach"],
    "GULF": ["Houston", "Dallas"],
    "SOUTHEAST": ["Atlanta", "Charlotte", "Savannah", "Jacksonville"],
}

async def scrape_trac():
    """Scrape TRAC's chassis availability page (updates 3x daily)."""
    if not PLAYWRIGHT_AVAILABLE:
        return 0
    added = 0
    url = "https://www.tracintermodal.com/chassis-availability"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
            text = await page.inner_text("body")
            content = await page.content()
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            release_pat = re.compile(r'\b([A-Z]{2,5}\d{2,4}[A-Z]?\d?)\b')
            current_pool = "TRAC"
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            for line in lines:
                lu = line.upper()
                # Pool header detection
                for pool_name in TRAC_REGIONS:
                    if pool_name in lu and len(line) < 80:
                        current_pool = f"TRAC {pool_name.title()}"
                # Terminal detection
                city_hit = any(city.upper() in lu for pool_cities in TRAC_REGIONS.values() for city in pool_cities)
                has_location_kw = any(kw in lu for kw in ["TERMINAL","PORT","YARD","RAMP","APM","PNCT","MAHER","BAYPORT","LBCT","TRAPAC"])
                if (city_hit or has_location_kw) and 10 < len(line) < 120:
                    open_pulls = any(kw in lu for kw in ["OPEN","AVAILABLE","START"])
                    closed = any(kw in lu for kw in ["CLOSED","NO RELEASE","LIMITED"])
                    codes = release_pat.findall(line)
                    release_code = codes[0] if codes else None
                    status = "closed" if closed else ("limited" if "LIMITED" in lu else ("open" if open_pulls else "unknown"))
                    region = _detect_region_from_text(line)
                    c.execute("INSERT OR IGNORE INTO terminals (provider, terminal_name, region, pool) VALUES (?,?,?,?)",
                              ("TRAC", f"TRAC - {line[:80]}", region, current_pool))
                    tid = c.execute("SELECT id FROM terminals WHERE terminal_name=?", (f"TRAC - {line[:80]}",)).fetchone()
                    if tid:
                        c.execute("""INSERT INTO availability
                            (terminal_id, open_for_pulls, open_for_returns, release_code, status, notes, data_source)
                            VALUES (?,?,?,?,?,?,?)""",
                            (tid[0], 1 if open_pulls else 0, 1, release_code, status,
                             f"TRAC live - pool: {current_pool}", "trac_scrape"))
                        added += 1
            conn.commit()
            conn.close()
            await page.close()
        except Exception as e:
            print(f"[TRAC] Scrape error: {e}")
        await browser.close()
    print(f"[TRAC] Scraped {added} records")
    log_scrape("trac_scrape", added, "ok")
    return added


# ─────────────────────────────────────────────────────────────────
# SOURCE 3: POOL OF POOLS (LA/LB) - PUBLIC KPI METRICS
# pop-lalb.com publishes: terminal dwell, street dwell, OOS ratio
# These are market-level metrics — update LA/LB terminal notes
# ─────────────────────────────────────────────────────────────────

async def scrape_pool_of_pools():
    """
    Scrapes pop-lalb.com public KPI dashboard.
    Returns market-level metrics for LA/LB chassis health:
    - Terminal dwell (days on terminal before pickup)
    - Street dwell (days chassis is out on street)
    - OOS ratio (% of chassis out of service)
    Updates all LA/LB Pool of Pools terminals with these metrics.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return 0
    added = 0
    url = "https://www.pop-lalb.com/"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            await page.goto(url, wait_until="networkidle", timeout=25000)
            await page.wait_for_timeout(3000)
            text = await page.inner_text("body")
            # Extract metrics using pattern matching
            # Page shows values like "9.7 Days" for terminal dwell, "6%" for OOS etc
            float_pat = re.compile(r'(\d+\.?\d*)\s*(?:Days?|%)', re.I)
            numbers = float_pat.findall(text)
            # Parse structured values
            td_20 = td_40 = sd_20 = sd_40 = oos_20 = oos_40 = None
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            mode = None
            for line in lines:
                ll = line.lower()
                if 'terminal dwell' in ll: mode = 'td'
                elif 'street dwell' in ll: mode = 'sd'
                elif 'oos' in ll or 'out of service' in ll: mode = 'oos'
                nums = re.findall(r'(\d+\.?\d*)', line)
                if nums and mode:
                    val = float(nums[0])
                    if mode == 'td':
                        if td_20 is None: td_20 = val
                        elif td_40 is None: td_40 = val; mode = None
                    elif mode == 'sd':
                        if sd_20 is None: sd_20 = val
                        elif sd_40 is None: sd_40 = val; mode = None
                    elif mode == 'oos':
                        if oos_20 is None: oos_20 = val
                        elif oos_40 is None: oos_40 = val; mode = None
            # Determine market health status
            def health_status(td, sd, oos):
                if (td and td > 7) or (sd and sd > 7) or (oos and oos > 11):
                    return "limited"
                if (td and td > 4) or (sd and sd > 5) or (oos and oos > 6):
                    return "limited"
                return "open"
            status_40 = health_status(td_40, sd_40, oos_40)
            notes_40 = " | ".join(filter(None, [
                f"Terminal dwell: {td_40}d" if td_40 else None,
                f"Street dwell: {sd_40}d" if sd_40 else None,
                f"OOS: {oos_40}%" if oos_40 else None,
                "Source: Pool of Pools LA/LB"
            ]))
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            # Update all LA/LB Pool of Pools terminals
            pop_terminals = c.execute(
                "SELECT id FROM terminals WHERE region='pacific-sw' AND (pool LIKE '%Pool of Pools%' OR city IN ('Los Angeles','Long Beach'))"
            ).fetchall()
            for (tid,) in pop_terminals:
                c.execute("""INSERT INTO availability
                    (terminal_id, open_for_pulls, open_for_returns, status, notes,
                     oos_ratio, street_dwell, terminal_dwell, data_source)
                    VALUES (?,1,1,?,?,?,?,?,?)""",
                    (tid, status_40, notes_40 or "Pool of Pools LA/LB",
                     oos_40, sd_40, td_40, "pool_of_pools"))
                added += 1
            conn.commit()
            conn.close()
            print(f"[POP] Terminal dwell 40ft={td_40}d, Street dwell={sd_40}d, OOS={oos_40}% → {status_40}")
            await page.close()
        except Exception as e:
            print(f"[POP] Scrape error: {e}")
        await browser.close()
    log_scrape("pool_of_pools", added, "ok")
    return added


# ─────────────────────────────────────────────────────────────────
# SOURCE 4: BTS/NTAD FACILITY DIRECTORY (US GOV OPEN DATA)
# Bureau of Transportation Statistics - all intermodal rail terminals
# Updated Feb 2025. Free government open data. Run once on startup.
# ─────────────────────────────────────────────────────────────────

BTS_CSV_URLS = [
    "https://opendata.arcgis.com/datasets/a56a4abe1d804cf09d2f79c03e25f00b_0.csv",
    "https://geo.dot.gov/server/rest/services/Hosted/Intermodal_Freight_Facilities_Rail_TOFC_COFC/FeatureServer/0/query?where=1%3D1&outFields=*&f=csv",
]

def load_bts_facilities():
    """
    Load BTS National Transportation Atlas Database of intermodal terminals.
    Adds government-verified terminal names, SPLC codes, and coordinates.
    Only adds NEW terminals not already in DB — never overwrites seed data.
    """
    try:
        import urllib.request, csv, io
        for url in BTS_CSV_URLS:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    raw = resp.read().decode('utf-8', errors='replace')
                    reader = csv.DictReader(io.StringIO(raw))
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    added = 0
                    for row in reader:
                        name = (row.get('NAME') or row.get('FACNAME') or '').strip()
                        state = (row.get('STATE') or row.get('ST') or '').strip()
                        city = (row.get('CITY') or '').strip()
                        splc = (row.get('SPLC') or '').strip()
                        try:
                            lat = float(row.get('LAT') or row.get('LATITUDE') or 0)
                            lng = float(row.get('LON') or row.get('LONGITUDE') or 0)
                        except (ValueError, TypeError):
                            lat, lng = 0.0, 0.0
                        if not name or len(name) < 5:
                            continue
                        region = _state_to_region(state)
                        try:
                            c.execute("""INSERT OR IGNORE INTO terminals
                                (provider, terminal_name, city, state, region, lat, lng, splc)
                                VALUES (?,?,?,?,?,?,?,?)""",
                                ("NTAD", name[:120], city, state, region, lat, lng, splc))
                            if c.rowcount > 0:
                                tid = c.lastrowid
                                c.execute("""INSERT INTO availability
                                    (terminal_id, open_for_pulls, open_for_returns, status, notes, data_source)
                                    VALUES (?,1,1,'unknown','BTS/NTAD government facility directory','bts_ntad')""",
                                    (tid,))
                                added += 1
                        except Exception:
                            pass
                    conn.commit()
                    conn.close()
                    print(f"[BTS] Loaded {added} NTAD intermodal facilities")
                    log_scrape("bts_ntad", added, "ok")
                    return added
            except Exception as e:
                print(f"[BTS] URL {url[:60]}: {e}")
                continue
    except Exception as e:
        print(f"[BTS] Load error: {e}")
    return 0


# ─────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────

def _detect_region_from_text(text):
    t = text.upper()
    if any(c in t for c in ["CHICAGO","CLEVELAND","COLUMBUS","DETROIT","KANSAS CITY","MINNEAPOLIS","INDIANAPOLIS","ST. LOUIS"]):
        return "midwest"
    elif any(c in t for c in ["ATLANTA","CHARLOTTE","MEMPHIS","NASHVILLE","SAVANNAH","JACKSONVILLE","LOUISVILLE","BIRMINGHAM","RALEIGH"]):
        return "southeast"
    elif any(c in t for c in ["NEWARK","NEW YORK","PHILADELPHIA","BOSTON","BALTIMORE","BAYONNE"]):
        return "northeast"
    elif any(c in t for c in ["HOUSTON","DALLAS","NEW ORLEANS","SAN ANTONIO"]):
        return "gulf"
    elif any(c in t for c in ["LOS ANGELES","LONG BEACH","SAN PEDRO","HOBART"]):
        return "pacific-sw"
    elif any(c in t for c in ["SEATTLE","PORTLAND","TACOMA"]):
        return "pacific-nw"
    return "unknown"

def _state_to_region(state):
    NE = {"CT","DE","MA","MD","ME","NH","NJ","NY","PA","RI","VT"}
    SE = {"AL","AR","FL","GA","KY","LA","MS","NC","SC","TN","VA","WV"}
    MW = {"IA","IL","IN","KS","MI","MN","MO","ND","NE","OH","SD","WI"}
    GF = {"LA","MS","TX","OK"}
    PW = {"AZ","CA","CO","NM","NV","UT"}
    PN = {"OR","WA","ID","MT","WY"}
    if state in NE: return "northeast"
    if state in SE: return "southeast"
    if state in MW: return "midwest"
    if state in GF: return "gulf"
    if state in PW: return "pacific-sw"
    if state in PN: return "pacific-nw"
    return "unknown"

def log_scrape(source, records, status):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO scrape_log (source, records_added, status) VALUES (?,?,?)",
                     (source, records, status))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────
# MASTER SCRAPER — runs all sources
# ─────────────────────────────────────────────────────────────────

async def run_all_scrapers():
    print(f"\n[Aggregator] Running all sources — {datetime.now().isoformat()}")
    try:
        await scrape_dcli()
    except Exception as e:
        print(f"[Aggregator] DCLI error: {e}")
    try:
        await scrape_trac()
    except Exception as e:
        print(f"[Aggregator] TRAC error: {e}")
    try:
        await scrape_pool_of_pools()
    except Exception as e:
        print(f"[Aggregator] POP error: {e}")
    print(f"[Aggregator] Done")


def start_scheduler():
    if not SCHEDULER_AVAILABLE:
        print("[Scheduler] APScheduler not available")
        return
    scheduler = BackgroundScheduler()
    # 6:30am — after DCLI morning update (6am) + TRAC morning update
    scheduler.add_job(lambda: asyncio.run(run_all_scrapers()), 'cron', hour=6, minute=30)
    # 12:30pm — after TRAC midday update
    scheduler.add_job(lambda: asyncio.run(run_all_scrapers()), 'cron', hour=12, minute=30)
    # 5:30pm — after TRAC end-of-day update
    scheduler.add_job(lambda: asyncio.run(run_all_scrapers()), 'cron', hour=17, minute=30)
    scheduler.start()
    print("[Scheduler] Active: 6:30am / 12:30pm / 5:30pm daily")


# ─────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────

print("[ChassisFind] v2.0 starting — Multi-Source Aggregator")
init_db()
load_seed_data()

# Load BTS facility directory on first start (adds 500+ terminals from gov data)
bts_count = load_bts_facilities()
print(f"[BTS] {bts_count} gov facilities loaded")

start_scheduler()


# ─────────────────────────────────────────────────────────────────
# API ROUTES — all /api/* routes BEFORE the catch-all static route
# ─────────────────────────────────────────────────────────────────

@app.route('/api/health')
def health():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    terminal_count = c.execute("SELECT COUNT(*) FROM terminals").fetchone()[0]
    sources = c.execute("SELECT source, MAX(ran_at), SUM(records_added) FROM scrape_log GROUP BY source").fetchall()
    conn.close()
    return jsonify({
        "status": "ok",
        "version": "2.0.0",
        "playwright": PLAYWRIGHT_AVAILABLE,
        "scheduler": SCHEDULER_AVAILABLE,
        "terminals_in_db": terminal_count,
        "data_sources": {s[0]: {"last_run": s[1], "total_records": s[2]} for s in sources},
        "db": DB_PATH,
        "timestamp": datetime.now().isoformat()
    })


@app.route('/api/chassis')
def get_chassis():
    location = request.args.get('location', '').lower().strip()
    provider = request.args.get('provider', 'all')
    status_f = request.args.get('status', 'all')
    pulls_only = request.args.get('pulls_only', 'false') == 'true'
    region_f = request.args.get('region', 'all')

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    sql = """SELECT t.id, t.provider, t.terminal_name, t.city, t.state,
                    t.region, t.lat, t.lng, t.pool, t.splc,
               a.open_for_pulls, a.open_for_returns, a.release_code,
               a.status, a.notes, a.data_source, a.scraped_at,
               a.oos_ratio, a.street_dwell, a.terminal_dwell,
               (SELECT COUNT(*) FROM driver_reports d WHERE d.terminal_id=t.id
                AND d.reported_at>datetime('now','-24 hours')) as driver_reports,
               (SELECT status FROM driver_reports d WHERE d.terminal_id=t.id
                ORDER BY d.reported_at DESC LIMIT 1) as latest_driver_status,
               (SELECT reported_at FROM driver_reports d WHERE d.terminal_id=t.id
                ORDER BY d.reported_at DESC LIMIT 1) as latest_driver_time
        FROM terminals t
        LEFT JOIN availability a ON a.id=(
            SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id=t.id
        )
        WHERE 1=1"""
    params = []
    if location:
        sql += " AND (LOWER(t.city) LIKE ? OR LOWER(t.state) LIKE ? OR LOWER(t.terminal_name) LIKE ? OR LOWER(t.region) LIKE ? OR LOWER(t.pool) LIKE ?)"
        params += [f"%{location}%"] * 5
    if provider != 'all':
        sql += " AND t.provider=?"; params.append(provider)
    if status_f != 'all':
        sql += " AND a.status=?"; params.append(status_f)
    if region_f != 'all':
        sql += " AND t.region=?"; params.append(region_f)
    if pulls_only:
        sql += " AND a.open_for_pulls=1"
    sql += " ORDER BY t.city, t.terminal_name LIMIT 300"
    rows = [dict(r) for r in c.execute(sql, params).fetchall()]
    conn.close()
    return jsonify({"results": rows, "count": len(rows), "timestamp": datetime.now().isoformat()})


@app.route('/api/report', methods=['POST'])
def submit_report():
    d = request.get_json() or {}
    if not d.get('terminal_id') or not d.get('status'):
        return jsonify({"error": "terminal_id and status required"}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""INSERT INTO driver_reports
        (terminal_id, chassis_size, status, wait_minutes, notes, driver_name)
        VALUES (?,?,?,?,?,?)""",
        (d['terminal_id'], d.get('chassis_size', '40'), d['status'],
         d.get('wait_minutes'), d.get('notes', ''), d.get('driver_name', 'Anonymous')))
    # Also update availability table with driver's ground truth
    conn.execute("""INSERT INTO availability
        (terminal_id, open_for_pulls, status, notes, data_source)
        VALUES (?,?,?,?,?)""",
        (d['terminal_id'],
         1 if d['status'] in ['open', 'limited'] else 0,
         d['status'],
         f"Driver report: {d.get('notes','')} (wait: {d.get('wait_minutes','?')} min)",
         'driver_report'))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "Report submitted. Thank you!"})


@app.route('/api/stats')
def stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    total = c.execute("SELECT COUNT(*) FROM terminals").fetchone()[0]
    by_provider = c.execute("SELECT provider, COUNT(*) FROM terminals GROUP BY provider").fetchall()
    open_t = c.execute("""SELECT COUNT(*) FROM availability a
        WHERE a.id=(SELECT MAX(a2.id) FROM availability a2 WHERE a2.terminal_id=a.terminal_id)
        AND a.open_for_pulls=1""").fetchone()[0]
    reports_today = c.execute(
        "SELECT COUNT(*) FROM driver_reports WHERE reported_at>datetime('now','-24 hours')").fetchone()[0]
    reports_week = c.execute(
        "SELECT COUNT(*) FROM driver_reports WHERE reported_at>datetime('now','-7 days')").fetchone()[0]
    sources = c.execute("SELECT source, MAX(ran_at) FROM scrape_log GROUP BY source").fetchall()
    conn.close()
    return jsonify({
        "terminals_tracked": total,
        "open_for_pulls": open_t,
        "driver_reports_today": reports_today,
        "driver_reports_week": reports_week,
        "by_provider": {r[0]: r[1] for r in by_provider},
        "data_sources": {s[0]: s[1] for s in sources},
        "playwright": PLAYWRIGHT_AVAILABLE,
        "scheduler": SCHEDULER_AVAILABLE
    })


@app.route('/api/terminals')
def list_terminals():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, terminal_name, city, state, provider, region, pool FROM terminals ORDER BY city, terminal_name"
    ).fetchall()
    conn.close()
    return jsonify([{"id": r[0], "name": r[1], "city": r[2], "state": r[3],
                     "provider": r[4], "region": r[5], "pool": r[6]} for r in rows])


@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    """Manual scrape trigger — useful for testing without waiting for scheduler."""
    try:
        asyncio.run(run_all_scrapers())
        return jsonify({"success": True, "message": "Scrape complete"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/driver-reports')
def get_driver_reports():
    """Recent driver reports for a terminal or all terminals."""
    terminal_id = request.args.get('terminal_id')
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if terminal_id:
        rows = conn.execute("""
            SELECT dr.*, t.terminal_name, t.city, t.state
            FROM driver_reports dr JOIN terminals t ON t.id=dr.terminal_id
            WHERE dr.terminal_id=? ORDER BY dr.reported_at DESC LIMIT 50
        """, (terminal_id,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT dr.*, t.terminal_name, t.city, t.state
            FROM driver_reports dr JOIN terminals t ON t.id=dr.terminal_id
            ORDER BY dr.reported_at DESC LIMIT 100
        """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ─────────────────────────────────────────────────────────────────
# STATIC / FRONTEND — catch-all MUST be last
# ─────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory(BASE_DIR, 'index.html')


@app.route('/<path:filename>')
def static_files(filename):
    filepath = os.path.join(BASE_DIR, filename)
    if os.path.isfile(filepath):
        return send_from_directory(BASE_DIR, filename)
    return jsonify({"error": "not found"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
