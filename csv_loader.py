"""
csv_loader.py — Parse uploaded delivery CSV and geocode addresses.

Supported column names (case-insensitive, spaces/underscores interchangeable):
  id / order_id / delivery_id
  address / delivery_address
  city
  pincode / postal_code / pin / zip
  status / delivery_status
  driver / driver_name / delivery_agent / agent
  package_type / category / item_type / product_type
  eta / expected_delivery / delivery_time
  priority
  lat / latitude
  lon / lng / longitude
"""

import csv
import io
import asyncio
import httpx
from datetime import datetime
from typing import Optional

# ── column aliases ─────────────────────────────────────────────────────────────

_ALIASES = {
    "id":           ["id", "order_id", "delivery_id", "order id", "delivery id", "orderid"],
    "address":      ["address", "delivery_address", "ship_address", "shipping address", "full_address", "customer_address"],
    "city":         ["city", "town", "delivery_city"],
    "pincode":      ["pincode", "pin_code", "postal_code", "pin", "zip", "zipcode", "postcode"],
    "status":       ["status", "delivery_status", "order_status", "shipment_status"],
    "driver":       ["driver", "driver_name", "delivery_agent", "agent", "delivery_boy", "rider"],
    "package_type": ["package_type", "category", "item_type", "product_type", "item_category", "product"],
    "eta":          ["eta", "expected_delivery", "delivery_time", "delivery_date", "expected_time", "estimated_delivery"],
    "priority":     ["priority", "delivery_priority", "order_priority", "urgency"],
    "lat":          ["lat", "latitude"],
    "lon":          ["lon", "lng", "longitude"],
    "customer":     ["customer", "customer_name", "name", "recipient", "consignee"],
    "phone":        ["phone", "mobile", "contact", "phone_number"],
    "weight":       ["weight", "weight_kg", "kg"],
    "value":        ["value", "order_value", "amount", "price"],
}

def _map_columns(headers: list[str]) -> dict[str, str]:
    """Return {canonical_name: actual_header} for matched columns."""
    h_lower = {h.lower().replace(" ", "_"): h for h in headers}
    mapping = {}
    for canonical, aliases in _ALIASES.items():
        for alias in aliases:
            if alias in h_lower:
                mapping[canonical] = h_lower[alias]
                break
    return mapping

def _get(row: dict, mapping: dict, key: str, default="") -> str:
    col = mapping.get(key)
    return str(row.get(col, default)).strip() if col else default

def _normalize_status(raw: str) -> str:
    r = raw.lower().strip()
    if any(x in r for x in ["transit", "shipped", "dispatch", "out for", "ofd"]):
        return "In Transit"
    if any(x in r for x in ["delay", "late", "hold", "stuck"]):
        return "Delayed"
    if any(x in r for x in ["risk", "warn", "alert"]):
        return "At Risk"
    if any(x in r for x in ["deliver", "done", "complet", "success"]):
        return "Delivered"
    if any(x in r for x in ["cancel", "rto", "return"]):
        return "Cancelled"
    if any(x in r for x in ["pending", "assign", "ready", "pickup"]):
        return "In Transit"
    return "In Transit"

def _normalize_priority(raw: str) -> str:
    r = raw.lower()
    if any(x in r for x in ["same", "urgent", "critical", "express same"]):
        return "Same-Day"
    if any(x in r for x in ["express", "fast", "next day", "priority"]):
        return "Express"
    return "Standard"

# ── geocoding (Nominatim — free, India) ───────────────────────────────────────

_geo_cache: dict[str, tuple[float, float]] = {}

async def _geocode(address: str, city: str, pincode: str) -> tuple[float, float]:
    """Return (lat, lon) for an Indian address. Falls back to (0, 0)."""
    query = pincode or f"{address} {city} India".strip()
    if query in _geo_cache:
        return _geo_cache[query]
    try:
        url = (
            f"https://nominatim.openstreetmap.org/search"
            f"?q={query}&countrycodes=in&format=json&limit=1"
        )
        headers = {"User-Agent": "LastMileIntelDashboard/1.0"}
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            _geo_cache[query] = (lat, lon)
            return lat, lon
    except Exception:
        pass
    return 0.0, 0.0

# ── main parser ───────────────────────────────────────────────────────────────

async def parse_csv(content: bytes, geocode: bool = True) -> tuple[list[dict], list[str]]:
    """
    Parse CSV bytes into a list of delivery dicts.
    Returns (deliveries, warnings).
    """
    text = content.decode("utf-8-sig", errors="replace")
    # Auto-detect delimiter
    sample = text[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = reader.fieldnames or []
    mapping = _map_columns(list(headers))
    warnings = []

    if "address" not in mapping and "pincode" not in mapping:
        warnings.append("No address or pincode column found — deliveries will be placed at city centre")

    deliveries = []
    tasks = []
    rows = list(reader)

    for i, row in enumerate(rows):
        delivery_id = _get(row, mapping, "id") or f"DLV-{1000 + i}"
        address     = _get(row, mapping, "address")
        city        = _get(row, mapping, "city")
        pincode     = _get(row, mapping, "pincode")
        status_raw  = _get(row, mapping, "status", "In Transit")
        driver      = _get(row, mapping, "driver", "Unassigned")
        pkg_type    = _get(row, mapping, "package_type", "General")
        eta         = _get(row, mapping, "eta", datetime.now().strftime("%H:%M"))
        priority    = _get(row, mapping, "priority", "Standard")
        lat_raw     = _get(row, mapping, "lat")
        lon_raw     = _get(row, mapping, "lon")
        customer    = _get(row, mapping, "customer")
        phone       = _get(row, mapping, "phone")
        weight      = _get(row, mapping, "weight")
        value       = _get(row, mapping, "value")

        # Parse lat/lon if present
        try:
            lat = float(lat_raw) if lat_raw else None
            lon = float(lon_raw) if lon_raw else None
        except ValueError:
            lat = lon = None

        d = {
            "id":           delivery_id,
            "address":      address or f"{city} {pincode}".strip() or "—",
            "city":         city,
            "pincode":      pincode,
            "lat":          lat or 0.0,
            "lon":          lon or 0.0,
            "status":       _normalize_status(status_raw),
            "driver":       driver,
            "package_type": pkg_type,
            "eta":          eta[:5] if len(eta) >= 5 else eta,
            "priority":     _normalize_priority(priority),
            "customer":     customer,
            "phone":        phone,
            "weight":       weight,
            "order_value":  value,
            "risk_score":   0,
            "risk_factors": [],
            "needs_geocode": lat is None and geocode,
            "_geo_query":   pincode or f"{address} {city} India".strip(),
        }
        deliveries.append(d)

    # Geocode in batches (rate-limit: 1 req/sec for Nominatim)
    if geocode:
        needs_geo = [d for d in deliveries if d.pop("needs_geocode", False)]
        for d in deliveries:
            d.pop("needs_geocode", None)

        for d in needs_geo:
            query = d.pop("_geo_query", "")
            if query:
                lat, lon = await _geocode("", "", query)
                d["lat"] = lat
                d["lon"] = lon
                await asyncio.sleep(1.1)  # Nominatim rate limit
            else:
                d.pop("_geo_query", None)

    for d in deliveries:
        d.pop("_geo_query", None)
        d.pop("needs_geocode", None)

    if not deliveries:
        warnings.append("No rows found in CSV")

    return deliveries, warnings


def generate_template_csv() -> str:
    """Return a sample CSV string users can download as a template."""
    headers = [
        "order_id", "address", "city", "pincode", "status",
        "driver", "package_type", "eta", "priority",
        "customer", "phone", "weight", "value"
    ]
    rows = [
        ["DLV-1001", "42 Koramangala 5th Block", "Bangalore", "560095", "In Transit",
         "Ramesh K.", "Electronics", "14:30", "Express", "Amit Shah", "9876543210", "1.2", "4999"],
        ["DLV-1002", "15 Banjara Hills Road No 12", "Hyderabad", "500034", "Delayed",
         "Priya S.", "Apparel", "16:00", "Standard", "Sneha Reddy", "9123456789", "0.8", "1299"],
        ["DLV-1003", "8 Andheri East MIDC", "Mumbai", "400093", "At Risk",
         "Suresh P.", "Pharmacy", "13:45", "Same-Day", "Raj Mehta", "9988776655", "0.3", "599"],
        ["DLV-1004", "27 Sector 18 Noida", "Noida", "201301", "In Transit",
         "Ankit M.", "Groceries", "15:15", "Standard", "Pooja Singh", "9871234560", "3.5", "850"],
        ["DLV-1005", "3 T Nagar Usman Road", "Chennai", "600017", "Delivered",
         "Kavitha R.", "Documents", "11:00", "Express", "Kiran Kumar", "9445566778", "0.2", "299"],
    ]
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(headers)
    w.writerows(rows)
    return out.getvalue()
