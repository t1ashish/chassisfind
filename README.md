# ChassisFind MVP
## Cross-Provider Intermodal Chassis Availability Platform

---

## What This Is

A full-stack MVP that aggregates chassis availability from DCLI, TRAC, 
Flexi-Van, and private fleets into one searchable interface. 

This solves the #1 pain point for drayage drivers: hunting across 3 
separate provider websites to find available chassis before a pull.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    index.html                       │
│        (Search UI + Results + Driver Reports)       │
└─────────────────────────┬───────────────────────────┘
                          │ REST API calls
                          ▼
┌─────────────────────────────────────────────────────┐
│                  scraper.py (Flask)                 │
│   /api/chassis  /api/report  /api/stats             │
└─────────────────────────┬───────────────────────────┘
                          │
          ┌───────────────┼──────────────────┐
          ▼               ▼                  ▼
    DCLI scraper    TRAC scraper     SQLite database
    (6 regions)     (locations)      (chassisfind.db)
    daily 6:30am    every 2 hrs      + driver reports
```

---

## Quickstart

### 1. Install dependencies
```bash
pip install requests beautifulsoup4 flask flask-cors apscheduler
```

### 2. Run the backend API
```bash
python scraper.py
# API runs on http://localhost:5000
```

### 3. Open the frontend
```bash
# Simply open index.html in a browser
# OR serve it:
python -m http.server 8080
# Then visit: http://localhost:8080
```

---

## Using with Claude Code

Claude Code can help you extend this MVP rapidly. Here are exact 
prompts to give Claude Code:

### Add real DCLI scraping
```
Look at the scrape_dcli() function in scraper.py. 
Fetch https://dcli.com/region/midwest/ and inspect the HTML 
structure. Then update the scraper to correctly parse all depot 
names, open/closed status, and release codes from the actual page.
```

### Add geocoding so distance search works
```
Add geocoding to scraper.py using the free Nominatim API 
(no key required). When a terminal is inserted, call 
https://nominatim.openstreetmap.org/search?q={address}&format=json 
to get lat/lng and store it. Then update the /api/chassis endpoint 
to calculate and sort by haversine distance from the user's location.
```

### Connect frontend to real API
```
Update index.html so that when runSearch() is called, it fetches 
from http://localhost:5000/api/chassis with the search params 
instead of using MOCK_TERMINALS. Keep the same card rendering 
logic — just replace the data source.
```

### Add map view
```
Add a map view to index.html using Leaflet.js 
(https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.js).
When the user clicks "Map" toggle, show a fullscreen map with 
colored markers: green for open, amber for limited, red for closed.
Clicking a marker shows the terminal details in a popup.
```

### Add SMS/email alerts
```
Add an /api/alert POST endpoint to scraper.py that lets users 
subscribe to availability alerts for a specific terminal and 
chassis size. Use Twilio for SMS (pip install twilio) and store 
subscriptions in a new alerts table. When availability changes, 
trigger the alert.
```

### Deploy to production
```
Add a Dockerfile and docker-compose.yml for this project. 
The frontend should be served by nginx, the API by gunicorn, 
and data should persist in a mounted volume. Include environment 
variable handling for API keys (OpenTrack, Twilio).
```

---

## Data Sources

| Source | URL | Update Frequency | Access |
|--------|-----|-----------------|--------|
| DCLI regions | dcli.com/regions-listing/ | Once per morning | Public web scrape |
| DCLI Pacific NW | dcli.com/region/pacific-northwest/ | Once per morning | Public web scrape |
| DCLI Pacific SW | dcli.com/region/pacific-southwest/ | Once per morning | Public web scrape |
| DCLI Midwest | dcli.com/region/midwest/ | Once per morning | Public web scrape |
| DCLI Northeast | dcli.com/region/northeast/ | Once per morning | Public web scrape |
| DCLI Southeast | dcli.com/region/southeast/ | Once per morning | Public web scrape |
| DCLI Gulf | dcli.com/region/gulf/ | Once per morning | Public web scrape |
| TRAC locations | tracintermodal.com | Periodic | Public web scrape |
| OpenTrack | opentrack.co/api | Real-time | Commercial API ($) |
| Railinc | public.railinc.com/developers | Real-time | Commercial API ($) |
| Driver reports | Your platform | Real-time | Your crowdsource layer |

---

## MVP Validation Metrics to Track

After launch, these numbers prove product-market fit:
- Daily active drivers (target: 100 in 30 days)
- Searches per day (target: 500 in 60 days)  
- Driver report submissions (target: 50/day in 30 days)
- "Dry run prevented" events (self-reported)
- Conversion to $19.99/mo paid (target: 5% of MAU)

---

## Revenue Model

| Tier | Price | Features |
|------|-------|----------|
| Free | $0 | 10 searches/day, basic availability |
| Driver Pro | $19.99/mo | Unlimited search, alerts, release codes |
| Dispatcher | $49/mo | Fleet view, 5 drivers, bulk alerts |
| Enterprise API | $299/mo | API access, webhooks, white-label |

---

## Next Steps After MVP Validation

1. **Week 1-2**: Launch free tier, recruit drivers via Facebook groups
2. **Week 3-4**: First 100 users, collect feedback
3. **Month 2**: Add Leaflet map view, SMS alerts, paid tiers
4. **Month 3**: Partner with one IMC for data sharing agreement
5. **Month 4**: Integrate OpenTrack API for real-time rail events
6. **Month 6**: Add AI-powered shortage prediction model
