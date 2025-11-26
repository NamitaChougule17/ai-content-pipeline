import feedparser
from fetch.content_extractor import fetch_full_article_text
from fetch.fields_fetcher import compute_source_other, pretty_date_from_entry


def fetch_rss_feed(url: str):
    feed = feedparser.parse(url)
    articles = []

    feed_title = None
    try:
        feed_title = feed.feed.get("title")
    except Exception:
        pass

    for entry in feed.entries:
        short_title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()

        # Collect all authors if present
        author = ""
        try:
            if hasattr(entry, "authors") and entry.authors:
                names = []
                for a in entry.authors:
                    name = a.get("name") if isinstance(a, dict) else getattr(a, "name", None)
                    if name:
                        names.append(name.strip())
                # fallback to single author field if authors list empty after filtering
                author = ", ".join([n for n in names if n]) or entry.get("author", "")
            else:
                author = entry.get("author", "")
        except Exception:
            author = entry.get("author", "")

        # Friendly display date, if the feed provides one
        date_display = pretty_date_from_entry(
            entry.get("published") or entry.get("updated")
        )

        # Fetch full, cleaned article content
        content = fetch_full_article_text(link)

        articles.append({
            "short_title": short_title,
            "url": link,
            "source": "Other",                                 # per your requirement
            "source_other": compute_source_other(feed_title, link),
            "author": author,
            "content": content,
            "summary": None,                                   # filled later by summarizer
            "category": None,                                  # filled later by categorizer
            "more_than_1": 0,                                  # per your requirement
            "date": date_display, 
            "hub" : None                                                          # string like "August 03, 2025"
        })

    return articles