import asyncio
import json
from collections import deque
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi import Request
from pydantic import BaseModel
import io

import config
import data_fetcher as df
import analyzer
import csv_loader

app = FastAPI(title="Last Mile Delivery Intelligence")

BASE_DIR   = Path(__file__).parent
templates  = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ── in-memory state ─────────────────────────────────────────────────────────────
_state: dict = {}
_history: deque = deque(maxlen=288)   # 24 h @ 5-min intervals

# ── SSE subscriber queues ────────────────────────────────────────────────────────
_sse_queues: set[asyncio.Queue] = set()

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
        "google_maps_key": config.GOOGLE_MAPS_API_KEY,
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

    # ── notify SSE subscribers ──────────────────────────────────────────────────
    payload = {**_state, "_history": list(_history)}
    for q in list(_sse_queues):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass

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

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

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

@app.get("/api/stream")
async def api_stream(request: Request):
    """SSE endpoint — pushes dashboard + history updates to the browser in real time."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=10)
    _sse_queues.add(queue)

    # Send current state immediately so the client doesn't wait for the next refresh
    if _state:
        await queue.put({**_state, "_history": list(_history)})

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    # Keepalive comment to prevent proxies from closing the connection
                    yield ": keepalive\n\n"
        finally:
            _sse_queues.discard(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


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

@app.post("/api/upload-deliveries")
async def api_upload_deliveries(file: UploadFile = File(...)):
    """Upload a CSV of delivery orders — replaces simulated data with real orders."""
    if not file.filename.endswith(".csv"):
        return JSONResponse({"error": "Only CSV files are supported"}, status_code=400)
    deliveries, warnings, total_rows = await csv_loader.parse_csv(file.file, geocode=True)
    if not deliveries:
        return JSONResponse({"error": "No valid rows found in CSV", "warnings": warnings}, status_code=400)
    df.set_uploaded_deliveries(deliveries)
    await refresh_all()
    skipped = total_rows - len(deliveries)
    return {
        "status":     "ok",
        "count":      len(deliveries),
        "total_rows": total_rows,
        "skipped":    skipped,
        "warnings":   warnings,
        "updated":    _state.get("last_updated"),
    }

@app.get("/api/download-template")
async def api_download_template():
    """Download a sample CSV template with correct column names."""
    csv_str = csv_loader.generate_template_csv()
    return StreamingResponse(
        io.StringIO(csv_str),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=delivery_template.csv"}
    )

# ── entry point ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn, os
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
