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
from typing import IO

# Maximum rows to load into the dashboard (map + table can't handle millions)
MAX_DISPLAY = 10_000

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

async def _geocode_query(query: str) -> tuple[float, float]:
    """Return (lat, lon) for a pincode or city string. Falls back to (0, 0)."""
    if not query:
        return 0.0, 0.0
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
    _geo_cache[query] = (0.0, 0.0)
    return 0.0, 0.0

# ── main parser ───────────────────────────────────────────────────────────────

async def parse_csv(
    source: "bytes | IO",
    geocode: bool = True,
    max_display: int = MAX_DISPLAY,
) -> tuple[list[dict], list[str], int]:
    """
    Parse a CSV file (bytes or file-like object) into delivery dicts.
    Streams rows to avoid loading the whole file into RAM.

    Returns (deliveries, warnings, total_rows).
    - deliveries: up to max_display rows
    - total_rows: actual row count in the file (may exceed max_display)
    """
    # Wrap bytes in a text stream; file objects are wrapped via TextIOWrapper
    if isinstance(source, (bytes, bytearray)):
        text_stream = io.StringIO(source.decode("utf-8-sig", errors="replace"))
    else:
        # source is a SpooledTemporaryFile or similar binary IO from FastAPI
        text_stream = io.TextIOWrapper(source, encoding="utf-8-sig", errors="replace", newline="")

    # Auto-detect delimiter from first 4 KB
    try:
        sample = text_stream.read(4096)
        text_stream.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except Exception:
        dialect = csv.excel

    reader = csv.DictReader(text_stream, dialect=dialect)
    headers = reader.fieldnames or []
    mapping = _map_columns(list(headers))
    warnings: list[str] = []

    if "address" not in mapping and "pincode" not in mapping:
        warnings.append("No address or pincode column found — deliveries placed at city centre")

    deliveries: list[dict] = []
    # geo_key -> list of delivery indices that need this location
    geo_needed: dict[str, list[int]] = {}
    total_rows = 0

    for i, row in enumerate(reader):
        total_rows += 1

        # Only keep up to max_display rows; still count remaining for the summary
        if total_rows > max_display:
            continue

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
        }

        if lat is None and geocode:
            geo_key = pincode or f"{address} {city} India".strip()
            if geo_key:
                geo_needed.setdefault(geo_key, []).append(len(deliveries))

        deliveries.append(d)

    # Close the text wrapper without closing the underlying file
    if not isinstance(source, (bytes, bytearray)):
        text_stream.detach()

    if total_rows > max_display:
        warnings.append(
            f"File contains {total_rows:,} rows — displaying first {max_display:,}. "
            "Upload a filtered/sampled file to see a different subset."
        )

    # Geocode unique locations only (deduplicated), respecting Nominatim rate limit
    if geocode and geo_needed:
        for geo_key, indices in geo_needed.items():
            lat, lon = await _geocode_query(geo_key)
            for idx in indices:
                deliveries[idx]["lat"] = lat
                deliveries[idx]["lon"] = lon
            await asyncio.sleep(1.1)  # Nominatim: max 1 req/sec

    if not deliveries:
        warnings.append("No rows found in CSV")

    return deliveries, warnings, total_rows


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
