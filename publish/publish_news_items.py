# publish/publish_news_items_acf.py
import re
import requests
import mysql.connector
from requests.auth import HTTPBasicAuth
from config.db_config import DB_CONFIG
from config.wp_config import (
    WP_DEFAULT_USER,
    WP_DEFAULT_PASS,
    REST_BASE,
    POST_STATUS,
    BATCH_LIMIT,
)

def clean(text):
    return re.sub(r"\s+", " ", str(text)).strip() if text else ""

def build_payload(row):
    """Build the JSON payload for each News Item."""
    title = clean(row.get("short_title") or "Untitled")   # use DB short_title
    summary = clean(row.get("summary"))
    url = clean(row.get("url"))
    source = clean(row.get("source") or "Other")
    source_other = clean(row.get("source_other")) or "Unknown"
    author = clean(row.get("author"))
    date_val = clean(row.get("date"))
    more_than_1 = bool(row.get("more_than_1"))

    payload = {
        "title": title,  # WordPress main title will be your short title
        "status": POST_STATUS,
        "acf": {
            "news_type": source,        # corresponds to "Item Type" dropdown
            "short_title": title,       # ACF field for short title
            "url": url,
            "source": source,
            "source_other": source_other,
            "author": author,
            "more_than_1": more_than_1,
            "date": date_val,
            "summary": summary,
        },
    }
    return payload

def fetch_pending_articles(conn):
    query = """
        SELECT id, short_title, url, source, source_other, author,
               summary, category, more_than_1, date, hub
        FROM articles
        WHERE (summary IS NOT NULL AND summary <> '')
          AND (wp_news_item_id IS NULL OR wp_news_item_id = 0)
          AND hub IS NOT NULL
        ORDER BY id ASC
        LIMIT %s
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(query, (BATCH_LIMIT,))
    rows = cur.fetchall()
    cur.close()
    return rows

def update_article(conn, article_id, wp_id, wp_url, wp_status):
    """Update the DB after successful publish."""
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE articles
        SET wp_news_item_id=%s,
            news_item_url=%s,
            news_item_status=%s
        WHERE Id=%s
        """,
        (wp_id, wp_url, wp_status, article_id),
    )
    conn.commit()
    cur.close()

def publish_to_hub(row):
    """Publish the article to its hub site."""
    hub = row.get("hub")
    base_url = f"https://{hub}"

    payload = build_payload(row)
    print(f" Posting article {row['Id']} to {base_url}...")

    resp = requests.post(
        f"{base_url}/wp-json/wp/v2/{REST_BASE}",
        json=payload,
        auth=HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS),
    )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{hub}: HTTP {resp.status_code} → {resp.text[:400]}")

    return resp.json()

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        rows = fetch_pending_articles(conn)
        if not rows:
            print(" No new articles to publish.")
            return

        print(f"🚀 Found {len(rows)} article(s) to publish...\n")
        for row in rows:
            try:
                result = publish_to_hub(row)
                wp_id = result.get("id")
                wp_url = result.get("link", "")
                wp_status = result.get("status", POST_STATUS)
                update_article(conn, row["Id"], wp_id, wp_url, wp_status)
                print(f"Article {row['Id']} → {row['hub']} → WP #{wp_id} ({wp_status})")
            except Exception as e:
                print(f"Failed article {row['Id']} ({row['hub']}): {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
