from urllib.parse import urlparse
from dateutil import parser as dtparser
from typing import Optional  

def pretty_date_from_entry(published_str: Optional[str]) -> Optional[str]:
    #Return 'Month day, Year' if we can parse the feed's date string.
    if not published_str:
        return None
    try:
        dt = dtparser.parse(published_str)
        return dt.strftime("%B %d, %Y")
    except Exception:
        return None

def compute_source_other(feed_title: Optional[str], url: str) -> str:
    """
    Prefer the RSS feed title (e.g., “Luiza's Newsletter”, “Marcus on AI”).
    If missing, fall back to hostname (without www.).
    """
    if feed_title:
        return feed_title.strip()
    host = urlparse(url).netloc
    return host.replace("www.", "")
