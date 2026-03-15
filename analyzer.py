import random
from datetime import datetime
from typing import Any
import config

# ── risk scoring ───────────────────────────────────────────────────────────────

def score_weather(weather: dict) -> tuple[int, list[str]]:
    """Return (score 0-100, list of impact reasons)."""
    score   = weather.get("base_risk", 0)
    factors = []
    wind    = weather.get("wind_speed", 0)
    vis     = weather.get("visibility", 10)
    cond    = weather.get("condition", "Clear")

    if wind > 40:
        score += 30; factors.append(f"Dangerous winds {wind:.0f} mph")
    elif wind > 25:
        score += 15; factors.append(f"Strong winds {wind:.0f} mph")
    elif wind > 15:
        score += 5;  factors.append(f"Moderate winds {wind:.0f} mph")

    if vis < 1.0:
        score += 25; factors.append(f"Near-zero visibility {vis:.1f} km")
    elif vis < 3.0:
        score += 15; factors.append(f"Poor visibility {vis:.1f} km")
    elif vis < 6.0:
        score += 5

    if cond:
        factors.append(f"{cond}: {weather.get('description','')}")

    return min(score, 100), factors


def score_traffic(traffic: dict) -> tuple[int, list[str]]:
    cong    = traffic.get("overall_congestion", 0)
    incs    = traffic.get("incidents", [])
    score   = int(cong)
    factors = []

    if cong > 70:
        factors.append(f"Severe congestion city-wide ({cong:.0f}%)")
    elif cong > 50:
        factors.append(f"Heavy traffic ({cong:.0f}%)")
    elif cong > 30:
        factors.append(f"Moderate congestion ({cong:.0f}%)")

    critical = [i for i in incs if i.get("severity") in ("Critical", "Major")]
    if critical:
        score += len(critical) * 10
        factors.append(f"{len(critical)} major incident(s) reported")
    if incs:
        factors.append(f"{len(incs)} total traffic incident(s)")

    return min(score, 100), factors


def score_events(events: list[dict]) -> tuple[int, list[str]]:
    if not events:
        return 0, []
    score   = 0
    factors = []
    for ev in events:
        att = ev.get("attendance", 0)
        if att > 50000:   s = 70
        elif att > 20000: s = 50
        elif att > 5000:  s = 30
        else:             s = 10
        if ev.get("road_closures"):
            s += 15
            factors.append(f"Road closures: {ev['name']}")
        score = max(score, s)
        factors.append(f"{ev['name']} @ {ev['venue']} ({att:,} attendees)")
    return min(score, 100), factors


def overall_risk(w_score: int, t_score: int, e_score: int) -> int:
    weighted = w_score * 0.35 + t_score * 0.45 + e_score * 0.20
    peak     = max(w_score, t_score, e_score)
    # Blend weighted average with peak — severe single factor matters
    return min(int(weighted * 0.7 + peak * 0.3), 100)


def risk_level(score: int) -> str:
    if score < config.RISK_LOW:    return "Low"
    if score < config.RISK_MEDIUM: return "Medium"
    if score < config.RISK_HIGH:   return "High"
    return "Critical"


def risk_color(score: int) -> str:
    if score < config.RISK_LOW:    return "#22c55e"
    if score < config.RISK_MEDIUM: return "#f59e0b"
    if score < config.RISK_HIGH:   return "#f97316"
    return "#ef4444"

# ── delivery impact ────────────────────────────────────────────────────────────

def apply_risk_to_deliveries(
    deliveries: list[dict],
    weather:    dict,
    traffic:    dict,
    events:     list[dict],
) -> list[dict]:
    w_score, w_factors = score_weather(weather)
    t_score, t_factors = score_traffic(traffic)
    e_score, e_factors = score_events(events)

    incidents = traffic.get("incidents", [])
    event_lats = [(ev["lat"], ev["lon"]) for ev in events if "lat" in ev]

    result = []
    for d in deliveries:
        d = dict(d)
        local_risk = 0
        factors    = []

        # Weather always affects all deliveries
        local_risk += int(w_score * 0.4)
        if w_factors:
            factors += w_factors[:1]

        # Traffic: proximity to incidents
        dlat, dlon = d.get("lat", 0), d.get("lon", 0)
        for inc in incidents:
            dist = abs(inc["lat"] - dlat) + abs(inc["lon"] - dlon)
            if dist < 0.04:
                local_risk += 20
                factors.append(f"Near incident: {inc['type']} on {inc['road']}")
                break

        # Events: proximity
        for elat, elon in event_lats:
            dist = abs(elat - dlat) + abs(elon - dlon)
            if dist < 0.05:
                local_risk += 15
                factors.append("Near major event — congestion expected")
                break

        # Base traffic
        local_risk += int(t_score * 0.25)

        local_risk = min(local_risk, 100)
        d["risk_score"]   = local_risk
        d["risk_factors"] = factors[:3]

        # Update status based on computed risk
        if d["status"] not in ("Delivered",):
            if local_risk >= config.RISK_HIGH:
                d["status"] = "Delayed"
            elif local_risk >= config.RISK_MEDIUM:
                d["status"] = "At Risk"

        result.append(d)
    return result

# ── impact analysis ────────────────────────────────────────────────────────────

def build_impact_analysis(
    deliveries: list[dict],
    weather:    dict,
    traffic:    dict,
    events:     list[dict],
) -> dict:
    w_score, _ = score_weather(weather)
    t_score, _ = score_traffic(traffic)
    e_score, _ = score_events(events)
    o_score    = overall_risk(w_score, t_score, e_score)

    total     = len(deliveries)
    delayed   = sum(1 for d in deliveries if d["status"] == "Delayed")
    at_risk   = sum(1 for d in deliveries if d["status"] == "At Risk")
    on_time   = sum(1 for d in deliveries if d["status"] == "In Transit")
    delivered = sum(1 for d in deliveries if d["status"] == "Delivered")

    avg_delay = max(0, int((o_score / 100) * 35))  # estimated extra minutes
    success_rate = round((on_time + delivered) / total * 100, 1) if total else 0

    roadblocks = []
    if w_score >= 20:
        roadblocks.append({
            "type":         "Weather",
            "severity":     risk_level(w_score),
            "score":        w_score,
            "description":  f"{weather.get('condition')} — {weather.get('description')}",
            "affected_est": int(total * w_score / 200),
            "delay_est":    int(w_score * 0.4),
            "icon":         "🌦️",
        })
    if t_score >= 20:
        inc_count = len(traffic.get("incidents", []))
        roadblocks.append({
            "type":         "Traffic",
            "severity":     risk_level(t_score),
            "score":        t_score,
            "description":  f"{inc_count} incidents, {traffic.get('congestion_label','—')} conditions",
            "affected_est": int(total * t_score / 150),
            "delay_est":    int(t_score * 0.5),
            "icon":         "🚦",
        })
    for ev in events:
        e = score_events([ev])[0]
        roadblocks.append({
            "type":         "Event",
            "severity":     risk_level(e),
            "score":        e,
            "description":  f"{ev['name']} @ {ev['venue']}",
            "affected_est": int(total * e / 300),
            "delay_est":    int(e * 0.4),
            "icon":         "🎪",
        })

    return {
        "overall_risk":  o_score,
        "risk_level":    risk_level(o_score),
        "risk_color":    risk_color(o_score),
        "weather_score": w_score,
        "traffic_score": t_score,
        "events_score":  e_score,
        "stats": {
            "total":        total,
            "on_time":      on_time,
            "at_risk":      at_risk,
            "delayed":      delayed,
            "delivered":    delivered,
            "success_rate": success_rate,
            "avg_delay_min": avg_delay,
        },
        "roadblocks": sorted(roadblocks, key=lambda x: -x["score"]),
    }

# ── proactive recommendations ──────────────────────────────────────────────────

def build_recommendations(
    weather: dict,
    traffic: dict,
    events:  list[dict],
    stats:   dict,
) -> list[dict]:
    w_score, _ = score_weather(weather)
    t_score, _ = score_traffic(traffic)
    e_score, _ = score_events(events)
    recs = []

    # Weather-based
    cond = weather.get("condition", "Clear")
    if cond in ("Thunderstorm", "Snow") or w_score >= 70:
        recs.append({
            "priority": "Critical",
            "category": "Weather",
            "icon":     "⛈️",
            "action":   "Suspend fragile & electronics deliveries temporarily",
            "reason":   f"Extreme {cond.lower()} conditions may damage goods or endanger drivers",
            "impact":   f"Protect ~{stats.get('total',0)//4} high-value shipments",
        })
    elif cond in ("Rain", "Mist") or w_score >= 40:
        recs.append({
            "priority": "High",
            "category": "Weather",
            "icon":     "🌧️",
            "action":   "Extend delivery windows by 20–30 min, notify customers proactively",
            "reason":   f"{cond} reduces drive speed and increases accident probability",
            "impact":   "Set accurate expectations, reduce failed delivery complaints",
        })
    if weather.get("wind_speed", 0) > 30:
        recs.append({
            "priority": "High",
            "category": "Weather",
            "icon":     "💨",
            "action":   "Avoid elevated routes (bridges, highway overpasses)",
            "reason":   f"Wind speed {weather['wind_speed']:.0f} mph — unsafe for vans/motorbikes",
            "impact":   "Reduce vehicle safety incidents",
        })

    # Traffic-based
    incidents = traffic.get("incidents", [])
    critical_roads = [i["road"] for i in incidents if i.get("severity") in ("Critical","Major")]
    if critical_roads:
        roads_str = ", ".join(set(critical_roads[:3]))
        recs.append({
            "priority": "High",
            "category": "Traffic",
            "icon":     "🚧",
            "action":   f"Reroute away from {roads_str}",
            "reason":   "Major incidents causing significant delays",
            "impact":   f"Save ~{len(critical_roads) * 15} min per affected delivery",
        })
    if t_score > 60:
        recs.append({
            "priority": "Medium",
            "category": "Traffic",
            "icon":     "🗺️",
            "action":   "Dispatch early morning wave — shift 3 AM – 6 AM for downtown deliveries",
            "reason":   f"City congestion at {traffic.get('overall_congestion',0):.0f}% — off-peak drastically faster",
            "impact":   "Cut average delivery time by 25–40%",
        })

    # Events-based
    for ev in events:
        if ev.get("road_closures"):
            recs.append({
                "priority": "High",
                "category": "Events",
                "icon":     "🎪",
                "action":   f"Avoid {ev['venue']} vicinity — use alternate corridors",
                "reason":   f"{ev['name']} road closures ({ev['attendance']:,} attendees)",
                "impact":   "Prevent delivery failure in affected zone",
            })
        if ev.get("impact") in ("High","Critical"):
            recs.append({
                "priority": "Medium",
                "category": "Events",
                "icon":     "📅",
                "action":   f"Reschedule {ev['venue']}-area deliveries to pre/post event window",
                "reason":   f"{ev['name']} creates heavy congestion {ev['start']}–{ev['end']}",
                "impact":   "Avoid {}-{} blackout period".format(ev['start'], ev['end']),
            })

    # General proactive
    delayed_pct = (stats.get("delayed", 0) / max(stats.get("total", 1), 1)) * 100
    if delayed_pct > 20:
        recs.append({
            "priority": "High",
            "category": "Operations",
            "icon":     "📦",
            "action":   "Activate surge capacity — call backup drivers",
            "reason":   f"{delayed_pct:.0f}% of deliveries delayed — normal capacity insufficient",
            "impact":   "Recover SLA compliance for same-day orders",
        })

    recs.append({
        "priority": "Low",
        "category": "Customer Experience",
        "icon":     "📱",
        "action":   "Send proactive SMS/app notifications to all at-risk recipients",
        "reason":   "Transparency reduces support tickets and improves NPS",
        "impact":   "Cut inbound 'where is my order' calls by ~35%",
    })

    priority_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    return sorted(recs, key=lambda x: priority_order.get(x["priority"], 9))
