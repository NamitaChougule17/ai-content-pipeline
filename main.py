from db.feed_repository import get_all_active_feeds
from fetch.rss_fetcher import fetch_rss_feed
from db.article_repository import insert_article

def fetch_and_store_all():
    feed_urls = get_all_active_feeds()
    for url in feed_urls:
        try:
            articles = fetch_rss_feed(url)
            for article in articles:
                insert_article(article)
        except Exception as e:
            print(f" Error fetching from {url}: {e}")

if __name__ == "__main__":
    fetch_and_store_all()

