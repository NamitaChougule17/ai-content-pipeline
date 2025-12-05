from db.feed_repository import get_all_active_feeds
from fetch.rss_fetcher import fetch_rss_feed
from db.article_repository import insert_article
from summarize.summarize_articles import summarize_and_store_all_articles
from db.populate_wp_post_ids import populate_wp_post_ids


def fetch_and_store_all():
    feeds = get_all_active_feeds()

    for feed_id, url, feed_name, feed_category, hub_name in feeds:
        try:
            articles = fetch_rss_feed(url)
            for article in articles:
                article["feed_id"] = feed_id
                article["category"] = feed_category
                article["hub_name"] = hub_name
                insert_article(article)
        except Exception as e:
            print(f"❌ Error fetching from {url}: {e}")


def main():
    print(" STEP 1: Fetch RSS Feeds ")
    fetch_and_store_all()


    print(" STEP 2: Summarize Articles ")
    summarize_and_store_all_articles()

 
    print(" STEP 3: Populate WP Post IDs for all posts ")
    populate_wp_post_ids()



if __name__ == "__main__":
    main()
