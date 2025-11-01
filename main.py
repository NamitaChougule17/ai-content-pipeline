from db.feed_repository import get_all_active_feeds
from fetch.rss_fetcher import fetch_rss_feed
from db.article_repository import insert_article
from summarize.summarize_articles import summarize_and_store_all_articles

def fetch_and_store_all():
    feeds = get_all_active_feeds()  # [(url, source_name, source_category)]
    for url, source_name, source_category, hub in feeds:
        try:
            articles = fetch_rss_feed(url)
            for article in articles:
                # override the per-article category with the feed's source_category
                article["category"] = source_category 
                article["hub"] = hub
                insert_article(article)
        except Exception as e:
            print(f" Error fetching from {url}: {e}")

def main():
    # 1) Fetch & store fresh articles
    fetch_and_store_all()

    # 2) Summarize any articles that still need summaries
    print("\n--- Summarizing newly fetched articles ---")
    summarize_and_store_all_articles()

if __name__ == "__main__":
    main()

