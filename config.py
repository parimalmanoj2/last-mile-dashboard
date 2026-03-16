import os
from dotenv import load_dotenv

load_dotenv()

CITY_NAME = os.getenv("CITY_NAME", "Mumbai")
CITY_LAT = float(os.getenv("CITY_LAT", "19.0760"))
CITY_LON = float(os.getenv("CITY_LON", "72.8777"))

OPENWEATHER_API_KEY  = os.getenv("OPENWEATHER_API_KEY", "")
TOMTOM_API_KEY       = os.getenv("TOMTOM_API_KEY", "")
TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY", "")
GOOGLE_MAPS_API_KEY  = os.getenv("GOOGLE_MAPS_API_KEY", "")
PREDICTHQ_TOKEN      = os.getenv("PREDICTHQ_TOKEN", "")

WEATHER_REFRESH_SECS = 600
TRAFFIC_REFRESH_SECS = 300
EVENTS_REFRESH_SECS = 3600

RISK_LOW = 30
RISK_MEDIUM = 60
RISK_HIGH = 80
