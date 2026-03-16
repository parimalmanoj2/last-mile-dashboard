import httpx
import random
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import config

# ── helpers ────────────────────────────────────────────────────────────────────

def _rnd(lo, hi, decimals=1):
    return round(random.uniform(lo, hi), decimals)

# ── WEATHER ────────────────────────────────────────────────────────────────────

WEATHER_CONDITIONS = [
    {"main": "Clear",        "description": "clear sky",        "icon": "01d", "base_risk": 0},
    {"main": "Clouds",       "description": "partly cloudy",    "icon": "02d", "base_risk": 5},
    {"main": "Clouds",       "description": "overcast clouds",  "icon": "04d", "base_risk": 10},
    {"main": "Drizzle",      "description": "light drizzle",    "icon": "09d", "base_risk": 20},
    {"main": "Rain",         "description": "moderate rain",    "icon": "10d", "base_risk": 40},
    {"main": "Rain",         "description": "heavy rain",       "icon": "10d", "base_risk": 65},
    {"main": "Thunderstorm", "description": "thunderstorm",     "icon": "11d", "base_risk": 80},
    {"main": "Snow",         "description": "light snow",       "icon": "13d", "base_risk": 55},
    {"main": "Snow",         "description": "heavy snow",       "icon": "13d", "base_risk": 85},
    {"main": "Mist",         "description": "foggy",            "icon": "50d", "base_risk": 35},
]

def _mock_weather() -> dict:
    weights = [15, 12, 10, 10, 12, 8, 5, 5, 3, 10]  # India: more rain/drizzle/mist
    cond = random.choices(WEATHER_CONDITIONS, weights=weights, k=1)[0]
    wind = _rnd(5, 40)
    visibility = _rnd(1.0, 10.0)
    return {
        "temperature": _rnd(22, 42),   # Celsius — India range
        "feels_like":  _rnd(20, 45),
        "humidity":    random.randint(50, 95),  # India is more humid
        "wind_speed":  wind,
        "wind_deg":    random.randint(0, 359),
        "visibility":  visibility,
        "condition":   cond["main"],
        "description": cond["description"],
        "icon":        cond["icon"],
        "base_risk":   cond["base_risk"],
        "pressure":    random.randint(988, 1025),
        "timestamp":   datetime.now().isoformat(),
        "source": "mock",
    }

async def fetch_weather() -> dict:
    if not config.OPENWEATHER_API_KEY:
        return _mock_weather()
    try:
        lat, lon = _active_location["lat"], _active_location["lon"]
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={lat}&lon={lon}"
            f"&appid={config.OPENWEATHER_API_KEY}&units=metric"
        )
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            d = r.json()
        cond_main = d["weather"][0]["main"]
        base_risk = next(
            (c["base_risk"] for c in WEATHER_CONDITIONS if c["main"] == cond_main),
            10
        )
        return {
            "temperature": d["main"]["temp"],
            "feels_like":  d["main"]["feels_like"],
            "humidity":    d["main"]["humidity"],
            "wind_speed":  d["wind"]["speed"],
            "wind_deg":    d["wind"].get("deg", 0),
            "visibility":  round(d.get("visibility", 10000) / 1000, 1),
            "condition":   cond_main,
            "description": d["weather"][0]["description"],
            "icon":        d["weather"][0]["icon"],
            "base_risk":   base_risk,
            "pressure":    d["main"]["pressure"],
            "timestamp":   datetime.now().isoformat(),
            "source": "openweathermap",
        }
    except Exception:
        return _mock_weather()

# ── TRAFFIC ────────────────────────────────────────────────────────────────────

INCIDENT_TYPES = [
    "Road accident", "Pothole / Road damage", "Construction work",
    "Traffic signal failure", "Waterlogging", "Police nakabandi",
    "Flyover maintenance", "Water pipeline burst", "Tree fallen",
    "VIP movement / convoy", "Cattle on road", "Auto-rickshaw breakdown",
]

INDIA_ROADS = [
    "NH-48", "NH-44", "NH-19", "Outer Ring Road", "Inner Ring Road",
    "Eastern Express Hwy", "Western Express Hwy", "Sardar Patel Marg",
    "MG Road", "Brigade Road", "Anna Salai", "Rajiv Gandhi Salai",
    "Bandra-Worli Sea Link", "Peripheral Ring Road", "Dwarka Expressway",
    "Mumbai-Pune Expressway", "NICE Road", "ORR Tollgate",
]

def _mock_traffic() -> dict:
    lat, lon = _active_location["lat"], _active_location["lon"]
    num_incidents = random.randint(2, 6)
    incidents = []
    for _ in range(num_incidents):
        dlat = _rnd(-0.05, 0.05)
        dlon = _rnd(-0.07, 0.07)
        incidents.append({
            "type":     random.choice(INCIDENT_TYPES),
            "severity": random.choice(["Minor", "Moderate", "Major", "Critical"]),
            "lat":      round(lat + dlat, 5),
            "lon":      round(lon + dlon, 5),
            "road":     random.choice(INDIA_ROADS),
            "delay_min": random.randint(10, 60),
            "reported": (datetime.now() - timedelta(minutes=random.randint(5, 90))).strftime("%H:%M"),
        })

    zones = []
    zone_names = ["City Centre", "Industrial Area", "Airport Zone", "Old City", "IT Corridor", "Residential Hub"]
    for i, name in enumerate(zone_names):
        flow_ratio = _rnd(0.3, 1.0)
        zones.append({
            "name":       name,
            "lat":        round(lat + _rnd(-0.06, 0.06), 5),
            "lon":        round(lon + _rnd(-0.08, 0.08), 5),
            "flow_ratio": round(flow_ratio, 2),
            "congestion": _congestion_label(flow_ratio),
        })


    overall = sum(1 - z["flow_ratio"] for z in zones) / len(zones)
    return {
        "overall_congestion": round(overall * 100, 1),
        "congestion_label":   _congestion_label(1 - overall),
        "incidents":          incidents,
        "zones":              zones,
        "timestamp":          datetime.now().isoformat(),
        "source": "mock",
    }

def _congestion_label(flow_ratio: float) -> str:
    if flow_ratio >= 0.85:   return "Free Flow"
    if flow_ratio >= 0.65:   return "Light"
    if flow_ratio >= 0.45:   return "Moderate"
    if flow_ratio >= 0.25:   return "Heavy"
    return "Gridlock"

async def fetch_traffic() -> dict:
    if config.GOOGLE_MAPS_API_KEY:
        return await _fetch_traffic_google()
    if config.TOMTOM_API_KEY:
        return await _fetch_traffic_tomtom()
    return _mock_traffic()


async def _fetch_traffic_google() -> dict:
    """Use Google Distance Matrix API to get real congestion per zone."""
    lat, lon = _active_location["lat"], _active_location["lon"]
    # 8 probe destinations spread around city — compare free-flow vs traffic time
    zone_defs = [
        ("City Centre",      0.00,  0.00),
        ("North Zone",       0.07,  0.00),
        ("South Zone",      -0.07,  0.00),
        ("East Zone",        0.00,  0.09),
        ("West Zone",        0.00, -0.09),
        ("Airport Corridor", 0.05,  0.06),
        ("Industrial Area",  -0.04, 0.07),
        ("IT Corridor",      0.06, -0.05),
    ]
    destinations = "|".join(f"{lat+dlat},{lon+dlon}" for _, dlat, dlon in zone_defs)
    url = (
        f"https://maps.googleapis.com/maps/api/distancematrix/json"
        f"?origins={lat},{lon}"
        f"&destinations={destinations}"
        f"&departure_time=now"
        f"&traffic_model=best_guess"
        f"&key={config.GOOGLE_MAPS_API_KEY}"
    )
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(url)
            r.raise_for_status()
            d = r.json()

        rows    = d.get("rows", [{}])[0].get("elements", [])
        zones   = []
        delays  = []
        for i, (name, dlat, dlon) in enumerate(zone_defs):
            el = rows[i] if i < len(rows) else {}
            status = el.get("status", "ZERO_RESULTS")
            if status != "OK":
                flow_ratio = _rnd(0.5, 0.9)
            else:
                free_flow  = el.get("duration", {}).get("value", 600)
                in_traffic = el.get("duration_in_traffic", {}).get("value", free_flow)
                # flow_ratio = free_flow / in_traffic  (1.0 = no congestion, <0.5 = gridlock)
                flow_ratio = round(min(free_flow / max(in_traffic, 1), 1.0), 2)
                delay_min  = max(0, int((in_traffic - free_flow) / 60))
                delays.append(delay_min)
            zones.append({
                "name":       name,
                "lat":        round(lat + dlat, 5),
                "lon":        round(lon + dlon, 5),
                "flow_ratio": flow_ratio,
                "congestion": _congestion_label(flow_ratio),
            })

        overall       = sum(1 - z["flow_ratio"] for z in zones) / len(zones)
        avg_delay     = int(sum(delays) / len(delays)) if delays else 0
        # Build incident-style delay summary from worst zones
        incidents     = []
        for z in sorted(zones, key=lambda x: x["flow_ratio"])[:3]:
            if z["flow_ratio"] < 0.75:
                incidents.append({
                    "type":      "Traffic Congestion",
                    "severity":  _congestion_severity(z["flow_ratio"]),
                    "lat":       z["lat"],
                    "lon":       z["lon"],
                    "road":      z["name"],
                    "delay_min": max(5, int((1 - z["flow_ratio"]) * 40)),
                    "reported":  datetime.now().strftime("%H:%M"),
                })
        return {
            "overall_congestion": round(overall * 100, 1),
            "congestion_label":   _congestion_label(1 - overall),
            "avg_delay_min":      avg_delay,
            "incidents":          incidents,
            "zones":              zones,
            "timestamp":          datetime.now().isoformat(),
            "source":             "google",
        }
    except Exception:
        return _mock_traffic()


async def _fetch_traffic_tomtom() -> dict:
    """
    Uses two TomTom APIs in parallel:
    - Traffic Flow Segment: real speed / congestion per zone
    - Incident Details: real accidents, roadworks, closures
    Free tier: 2,500 req/day — 8 flow + 1 incident = 9 calls per refresh.
    """
    lat, lon = _active_location["lat"], _active_location["lon"]

    zone_defs = [
        ("City Centre",      0.00,  0.00),
        ("North Zone",       0.07,  0.00),
        ("South Zone",      -0.07,  0.00),
        ("East Zone",        0.00,  0.09),
        ("West Zone",        0.00, -0.09),
        ("Airport Corridor", 0.05,  0.06),
        ("Industrial Area", -0.04,  0.07),
        ("IT Corridor",      0.06, -0.05),
    ]

    async def _flow(client: httpx.AsyncClient, zlat: float, zlon: float) -> float:
        """Return flow_ratio (0–1) for a point. Falls back to random on error."""
        try:
            url = (
                f"https://api.tomtom.com/traffic/services/4/flowSegmentData"
                f"/absolute/10/json"
                f"?point={zlat},{zlon}&key={config.TOMTOM_API_KEY}"
            )
            r = await client.get(url, timeout=8)
            r.raise_for_status()
            d = r.json().get("flowSegmentData", {})
            current   = d.get("currentSpeed", 0)
            free_flow = d.get("freeFlowSpeed", 1)
            return round(min(current / max(free_flow, 1), 1.0), 2)
        except Exception:
            return _rnd(0.4, 0.9)

    async def _incidents(client: httpx.AsyncClient) -> list[dict]:
        """Return list of real incidents from TomTom Incidents API v5."""
        try:
            bbox = f"{lon-0.15},{lat-0.10},{lon+0.15},{lat+0.10}"
            url  = (
                f"https://api.tomtom.com/traffic/services/5/incidentDetails"
                f"?key={config.TOMTOM_API_KEY}&bbox={bbox}"
                f"&fields={{incidents{{type,geometry,properties}}}}"
                f"&language=en-GB&timeValidityFilter=present"
            )
            r = await client.get(url, timeout=10)
            r.raise_for_status()
            raw = r.json().get("incidents", [])[:12]
            incidents = []
            for inc in raw:
                props  = inc.get("properties", {})
                geom   = inc.get("geometry", {})
                coords = geom.get("coordinates", [[[lon, lat]]])
                pt     = coords[0][0] if coords and coords[0] else [lon, lat]
                mag    = props.get("magnitudeOfDelay", 0)
                sev    = {0: "Minor", 1: "Minor", 2: "Moderate", 3: "Major", 4: "Critical"}.get(mag, "Minor")
                incidents.append({
                    "type":      props.get("events", [{}])[0].get("description", "Traffic Incident") if props.get("events") else props.get("incidentCategory", "Incident"),
                    "severity":  sev,
                    "lat":       pt[1],
                    "lon":       pt[0],
                    "road":      props.get("roadNumbers", ["Unknown road"])[0] if props.get("roadNumbers") else "Unknown road",
                    "delay_min": max(0, int(props.get("delay", 0) / 60)),
                    "reported":  datetime.now().strftime("%H:%M"),
                })
            return incidents
        except Exception:
            return []

    try:
        async with httpx.AsyncClient() as client:
            flow_tasks = [
                _flow(client, round(lat + dlat, 5), round(lon + dlon, 5))
                for _, dlat, dlon in zone_defs
            ]
            flow_results, incidents = await asyncio.gather(
                asyncio.gather(*flow_tasks),
                _incidents(client),
            )

        zones = []
        for i, (name, dlat, dlon) in enumerate(zone_defs):
            fr = flow_results[i]
            zones.append({
                "name":       name,
                "lat":        round(lat + dlat, 5),
                "lon":        round(lon + dlon, 5),
                "flow_ratio": fr,
                "congestion": _congestion_label(fr),
            })

        if not incidents:
            # Build congestion-based incidents from worst zones when incidents API returns empty
            incidents = []
            for z in sorted(zones, key=lambda x: x["flow_ratio"])[:3]:
                if z["flow_ratio"] < 0.75:
                    incidents.append({
                        "type":      "Traffic Congestion",
                        "severity":  _congestion_severity(z["flow_ratio"]),
                        "lat":       z["lat"],
                        "lon":       z["lon"],
                        "road":      z["name"],
                        "delay_min": max(5, int((1 - z["flow_ratio"]) * 45)),
                        "reported":  datetime.now().strftime("%H:%M"),
                    })

        overall   = sum(1 - z["flow_ratio"] for z in zones) / len(zones)
        avg_delay = int(sum(i["delay_min"] for i in incidents) / len(incidents)) if incidents else 0

        return {
            "overall_congestion": round(overall * 100, 1),
            "congestion_label":   _congestion_label(1 - overall),
            "avg_delay_min":      avg_delay,
            "incidents":          incidents,
            "zones":              zones,
            "timestamp":          datetime.now().isoformat(),
            "source":             "tomtom",
        }
    except Exception:
        return _mock_traffic()


def _congestion_severity(flow_ratio: float) -> str:
    if flow_ratio >= 0.75: return "Minor"
    if flow_ratio >= 0.55: return "Moderate"
    if flow_ratio >= 0.35: return "Major"
    return "Critical"

# ── EVENTS ─────────────────────────────────────────────────────────────────────

MOCK_EVENTS = [
    {"name": "IPL Match — MI vs RCB",       "venue": "Wankhede Stadium",       "category": "Sports",     "attendance": 33000, "start": "19:30", "end": "23:00"},
    {"name": "IPL Match — CSK vs KKR",      "venue": "Chepauk Stadium",        "category": "Sports",     "attendance": 38000, "start": "19:30", "end": "23:00"},
    {"name": "Diwali Mela",                 "venue": "City Grounds",           "category": "Festival",   "attendance": 80000, "start": "17:00", "end": "23:59"},
    {"name": "Ganesh Chaturthi Procession", "venue": "City-Wide Route",        "category": "Parade",     "attendance": 200000,"start": "10:00", "end": "22:00"},
    {"name": "Arijit Singh Concert",        "venue": "MMRDA Grounds",          "category": "Concert",    "attendance": 25000, "start": "19:00", "end": "22:30"},
    {"name": "India Trade Expo",            "venue": "Pragati Maidan",         "category": "Conference", "attendance": 50000, "start": "09:00", "end": "18:00"},
    {"name": "Kumbh Mela Shahi Snan",       "venue": "Sangam Ghat",            "category": "Religious",  "attendance": 500000,"start": "05:00", "end": "12:00"},
    {"name": "IT Corridor Marathon",        "venue": "HITEC City",             "category": "Sports",     "attendance": 15000, "start": "06:00", "end": "11:00"},
    {"name": "Navratri Garba Night",        "venue": "Exhibition Grounds",     "category": "Festival",   "attendance": 40000, "start": "20:00", "end": "02:00"},
    {"name": "Republic Day Parade Rehearsal","venue":"Rajpath / Kartavya Path","category": "Parade",     "attendance": 30000, "start": "08:00", "end": "12:00"},
    {"name": "Weekly Farmers Market",       "venue": "Local Market Area",      "category": "Market",     "attendance": 5000,  "start": "07:00", "end": "13:00"},
    {"name": "Political Rally",             "venue": "City Stadium",           "category": "Political",  "attendance": 60000, "start": "15:00", "end": "19:00"},
]

def _mock_events() -> list[dict]:
    lat, lon = _active_location["lat"], _active_location["lon"]
    num = random.randint(1, 3)
    chosen = random.sample(MOCK_EVENTS, num)
    events = []
    for ev in chosen:
        att = ev["attendance"]
        if att > 50000:   impact = "Critical"
        elif att > 20000: impact = "High"
        elif att > 5000:  impact = "Medium"
        else:             impact = "Low"
        events.append({
            **ev,
            "lat":    round(lat + _rnd(-0.05, 0.05), 5),
            "lon":    round(lon + _rnd(-0.07, 0.07), 5),
            "impact": impact,
            "road_closures": att > 10000,
            "parking_impact": att > 5000,
            "source": "mock",
        })
    return events

async def fetch_events() -> list[dict]:
    if not config.TICKETMASTER_API_KEY:
        return _mock_events()
    try:
        url = (
            f"https://app.ticketmaster.com/discovery/v2/events.json"
            f"?apikey={config.TICKETMASTER_API_KEY}"
            f"&latlong={config.CITY_LAT},{config.CITY_LON}&radius=10&unit=miles"
            f"&size=10&sort=date,asc"
        )
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            d = r.json()
        events = []
        for ev in d.get("_embedded", {}).get("events", []):
            venue = ev.get("_embedded", {}).get("venues", [{}])[0]
            loc   = venue.get("location", {})
            att   = int(venue.get("capacity", random.randint(1000, 30000)))
            if att > 50000:   impact = "Critical"
            elif att > 20000: impact = "High"
            elif att > 5000:  impact = "Medium"
            else:             impact = "Low"
            events.append({
                "name":         ev.get("name", "Unknown Event"),
                "venue":        venue.get("name", "Unknown Venue"),
                "category":     ev.get("classifications", [{}])[0].get("segment", {}).get("name", "Other"),
                "attendance":   att,
                "start":        ev.get("dates", {}).get("start", {}).get("localTime", "TBD"),
                "end":          "TBD",
                "lat":          float(loc.get("latitude", config.CITY_LAT)),
                "lon":          float(loc.get("longitude", config.CITY_LON)),
                "impact":       impact,
                "road_closures": att > 10000,
                "parking_impact": att > 5000,
                "source": "ticketmaster",
            })
        return events or _mock_events()
    except Exception:
        return _mock_events()

# ── DELIVERIES (simulated fleet) ───────────────────────────────────────────────

INDIA_LOCALITIES = [
    "Koramangala", "Indiranagar", "Whitefield", "HSR Layout", "Jayanagar",
    "Bandra West", "Andheri East", "Powai", "Thane", "Dadar",
    "Connaught Place", "Lajpat Nagar", "Dwarka", "Rohini", "Noida Sector 18",
    "Banjara Hills", "Jubilee Hills", "Gachibowli", "Madhapur", "Kukatpally",
    "T. Nagar", "Adyar", "Velachery", "Anna Nagar", "Perambur",
    "Salt Lake", "Park Street", "Dum Dum", "Howrah", "New Town",
    "Viman Nagar", "Kothrud", "Hinjewadi", "Wakad", "Aundh",
    "Gomti Nagar", "Hazratganj", "Alambagh", "Vikas Nagar", "Indira Nagar",
]

INDIA_STREET_TYPES = ["Main Road", "Cross", "Layout", "Colony", "Nagar", "Marg", "Path", "Extension", "Phase", "Sector"]

PACKAGE_TYPES = ["Electronics", "Apparel", "Groceries", "Furniture", "Pharmacy", "Documents", "Fragile", "FMCG", "Jewellery"]

DRIVERS = [
    "Ramesh K.", "Suresh P.", "Priya S.", "Ankit M.", "Kavitha R.",
    "Mohammed A.", "Deepak V.", "Sunita B.", "Raju T.", "Lakshmi N.",
    "Arjun S.", "Pooja D.", "Vikram C.", "Meena R.", "Ashok G.",
]

# ── active location (mutable, updated via set_active_location) ─────────────────
_active_location: dict = {
    "name": config.CITY_NAME,
    "lat":  config.CITY_LAT,
    "lon":  config.CITY_LON,
    "state": "",
    "pincode": "",
}

def set_active_location(name: str, lat: float, lon: float, state: str = "", pincode: str = "") -> None:
    global _active_location, _delivery_store
    _active_location = {"name": name, "lat": lat, "lon": lon, "state": state, "pincode": pincode}
    _delivery_store = []  # force regeneration for new location

def get_active_location() -> dict:
    return _active_location

# ── geocode search (Nominatim — free, no key) ──────────────────────────────────

async def search_locations(query: str) -> list[dict]:
    """Search Indian pincodes, cities, states via OSM Nominatim."""
    try:
        url = (
            f"https://nominatim.openstreetmap.org/search"
            f"?q={query}&countrycodes=in&format=json&limit=8&addressdetails=1"
        )
        headers = {"User-Agent": "LastMileIntelDashboard/1.0"}
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            results = r.json()
        locations = []
        seen = set()
        for item in results:
            addr  = item.get("address", {})
            city  = (addr.get("city") or addr.get("town") or addr.get("village")
                     or addr.get("county") or addr.get("state_district") or "")
            state = addr.get("state", "")
            pin   = addr.get("postcode", "")
            name  = city or item.get("display_name", "").split(",")[0]
            key   = f"{name}|{state}"
            if key in seen or not name:
                continue
            seen.add(key)
            label_parts = [p for p in [name, state] if p]
            if pin:
                label_parts.append(pin)
            locations.append({
                "name":    name,
                "state":   state,
                "pincode": pin,
                "lat":     float(item["lat"]),
                "lon":     float(item["lon"]),
                "label":   ", ".join(label_parts),
                "type":    item.get("type", ""),
            })
        return locations
    except Exception:
        return []

# ── deliveries ─────────────────────────────────────────────────────────────────

_delivery_store: list[dict] = []

def init_deliveries() -> list[dict]:
    global _delivery_store
    if _delivery_store:
        return _delivery_store
    lat, lon  = _active_location["lat"], _active_location["lon"]
    city_name = _active_location["name"]
    deliveries = []
    statuses = ["In Transit"] * 14 + ["At Risk"] * 5 + ["Delayed"] * 4 + ["Delivered"] * 2
    random.shuffle(statuses)
    for i, status in enumerate(statuses):
        dlat = _rnd(-0.07, 0.07)
        dlon = _rnd(-0.09, 0.09)
        locality = random.choice(INDIA_LOCALITIES)
        street_num = random.randint(1, 250)
        street_type = random.choice(INDIA_STREET_TYPES)
        eta_offset = random.randint(-10, 90)
        eta = (datetime.now() + timedelta(minutes=eta_offset)).strftime("%H:%M")
        deliveries.append({
            "id":           f"DLV-{1000 + i}",
            "address":      f"{street_num}, {locality} {street_type}, {city_name}",
            "lat":          round(lat + dlat, 5),
            "lon":          round(lon + dlon, 5),
            "status":       status,
            "driver":       random.choice(DRIVERS),
            "package_type": random.choice(PACKAGE_TYPES),
            "eta":          eta,
            "eta_offset":   eta_offset,
            "risk_score":   0,
            "risk_factors": [],
            "priority":     random.choice(["Standard", "Express", "Same-Day"]),
        })
    _delivery_store = deliveries
    return deliveries

def get_deliveries() -> list[dict]:
    if not _delivery_store:
        init_deliveries()
    return _delivery_store

def set_uploaded_deliveries(deliveries: list[dict]) -> None:
    """Replace simulated deliveries with real uploaded data."""
    global _delivery_store
    _delivery_store = deliveries
