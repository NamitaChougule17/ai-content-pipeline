import feedparser
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime, timezone
from fetch.content_extractor import fetch_full_article_text


def fetch_rss_feed(url):
    feed = feedparser.parse(url)
    articles = []

    for entry in feed.entries:
        title = entry.get("title")
        link = entry.get("link")
        published = parser.parse(entry.get("published", "")) if entry.get("published") else None

        author = entry.get("author", "")
        if not author and "authors" in entry and len(entry.authors) > 0:
            author = entry.authors[0].get("name", "")

        # Fetch full HTML and convert to plain text
        full_html = fetch_full_article_text(link, return_html=True)
        content = BeautifulSoup(full_html, "html.parser").get_text(separator="\n").strip()

        articles.append({
            "title": title,
            "url": link,
            "content": content,
            "author": author,
            "published": published,
            "summary": None,
            "category": None,
            "fetched_at": datetime.now(timezone.utc)
        })

    return articles
