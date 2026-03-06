import os
from dotenv import load_dotenv

load_dotenv()

# API Keys (set these in .env file)
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")  # Get free key from newsapi.org
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY", "")  # Get free key from gnews.io

# Search Keywords
YSRCP_KEYWORDS = [
    "YSRCP",
    "YS Jagan",
    "Jagan Mohan Reddy",
    "YSR Congress",
    "Navaratnalu",
    "Amma Vodi",
    "Rythu Bharosa",
    "Jagananna"
]

TDP_KEYWORDS = [
    "TDP",
    "Chandrababu Naidu",
    "Telugu Desam",
    "Nara Lokesh",
    "Amaravati capital",
    "Chandrababu"
]

# Hashtags to track
YSRCP_HASHTAGS = ["#YSRCP", "#YSJagan", "#Navaratnalu", "#AmmaVodi", "#Jagananna"]
TDP_HASHTAGS = ["#TDP", "#Chandrababu", "#TeluguDesam", "#NaraLokesh", "#Amaravati"]

# Andhra Pradesh Districts
AP_DISTRICTS = [
    "Visakhapatnam", "Vijayawada", "Guntur", "Tirupati", "Kurnool",
    "Nellore", "Rajahmundry", "Kadapa", "Anantapur", "Kakinada",
    "Eluru", "Ongole", "Srikakulam", "Vizianagaram", "Chittoor",
    "Prakasam", "Krishna", "West Godavari", "East Godavari"
]

# News Sources for AP Politics
NEWS_SOURCES = [
    "the-times-of-india",
    "the-hindu",
    "google-news-in"
]

# Google News RSS Feeds
GOOGLE_NEWS_RSS = {
    "ysrcp": "https://news.google.com/rss/search?q=YSRCP+OR+%22YS+Jagan%22+when:7d&hl=en-IN&gl=IN&ceid=IN:en",
    "tdp": "https://news.google.com/rss/search?q=TDP+OR+%22Chandrababu+Naidu%22+when:7d&hl=en-IN&gl=IN&ceid=IN:en",
    "ap_politics": "https://news.google.com/rss/search?q=Andhra+Pradesh+politics+when:7d&hl=en-IN&gl=IN&ceid=IN:en"
}

# Cache settings (in seconds)
CACHE_TTL = {
    "trends": 3600,      # 1 hour
    "news": 1800,        # 30 minutes
    "sentiment": 3600,   # 1 hour
}
