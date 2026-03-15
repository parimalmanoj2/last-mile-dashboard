import asyncio
import json
from collections import deque
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from pydantic import BaseModel

import config
import data_fetcher as df
import analyzer

app = FastAPI(title="Last Mile Delivery Intelligence")

BASE_DIR   = Path(__file__).parent
templates  = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ── in-memory state ─────────────────────────────────────────────────────────────
_state: dict = {}
_history: deque = deque(maxlen=288)   # 24 h @ 5-min intervals

# ── background refresh ──────────────────────────────────────────────────────────

async def refresh_all():
    global _state
    weather, traffic, events = await asyncio.gather(
        df.fetch_weather(),
        df.fetch_traffic(),
        df.fetch_events(),
    )
    deliveries = df.get_deliveries()
    deliveries = analyzer.apply_risk_to_deliveries(deliveries, weather, traffic, events)

    impact = analyzer.build_impact_analysis(deliveries, weather, traffic, events)
    recs   = analyzer.build_recommendations(weather, traffic, events, impact["stats"])

    loc = df.get_active_location()
    _state = {
        "city":            loc["name"],
        "state":           loc.get("state", ""),
        "pincode":         loc.get("pincode", ""),
        "lat":             loc["lat"],
        "lon":             loc["lon"],
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "weather":         weather,
        "traffic":         traffic,
        "events":          events,
        "deliveries":      deliveries,
        "impact":          impact,
        "recommendations": recs,
    }

    _history.append({
        "time":            datetime.now().strftime("%H:%M"),
        "overall":         impact["overall_risk"],
        "weather":         impact["weather_score"],
        "traffic":         impact["traffic_score"],
        "events":          impact["events_score"],
    })

# ── startup / scheduler ─────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup():
    df.init_deliveries()
    await refresh_all()
    scheduler.add_job(refresh_all, "interval", seconds=config.TRAFFIC_REFRESH_SECS)
    scheduler.start()

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()

# ── routes ──────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/dashboard")
async def api_dashboard():
    return JSONResponse(_state)

@app.get("/api/history")
async def api_history():
    return JSONResponse(list(_history))

@app.post("/api/refresh")
async def api_refresh():
    await refresh_all()
    return {"status": "ok", "updated": _state.get("last_updated")}

@app.get("/api/search-location")
async def api_search_location(q: str = Query(..., min_length=2)):
    """Geocode pincode / city / state using OSM Nominatim (India only)."""
    results = await df.search_locations(q)
    return JSONResponse(results)

class LocationPayload(BaseModel):
    name: str
    lat: float
    lon: float
    state: str = ""
    pincode: str = ""

@app.post("/api/set-location")
async def api_set_location(payload: LocationPayload):
    """Switch active city — regenerates deliveries and refreshes all data."""
    df.set_active_location(payload.name, payload.lat, payload.lon, payload.state, payload.pincode)
    _history.clear()
    await refresh_all()
    return {"status": "ok", "location": df.get_active_location()}

# ── entry point ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn, os
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
