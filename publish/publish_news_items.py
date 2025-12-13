# publish_news_items.py

import re
import requests
import mysql.connector
import datetime
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


def get_pending_articles(conn, limit=BATCH_LIMIT):
    """
    Fetch articles NOT yet published to their hub.
    We detect this by checking article_push for missing wp_news_item_id.
    """
    query = """
        SELECT
            a.id            AS article_id,
            a.short_title   AS short_title,
            a.url           AS url,
            a.source        AS source,
            a.source_other  AS source_other,
            a.author        AS author,
            a.content       AS content,
            a.summary       AS summary,
            a.category      AS category,
            a.more_than_1   AS more_than_1,
            a.date          AS date,
            a.hub_name      AS hub_name,
            h.id            AS hub_id
        FROM articles a
        JOIN hubs h
          ON h.hub_name = a.hub_name
        LEFT JOIN article_push ap
          ON ap.article_id = a.id
         AND ap.hub_id = h.id
        WHERE ap.id IS NULL
        ORDER BY a.fetched_at ASC
        LIMIT %s
    """

    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def build_payload(row):
    """
    Build the JSON payload for each News Item (ACF fields included).
    """

    # Main fields
    #item_type = "Article"
    title = clean(row.get("short_title") or "Untitled")
    summary = clean(row.get("summary"))
    url = clean(row.get("url"))
    #source = clean(row.get("source") or "Other")
    source_other = clean(row.get("source_other"))
    author = clean(row.get("author"))
    date_val = clean(row.get("date"))
    more_than_1 = bool(row.get("more_than_1"))
   

    payload = {
        "title": title,
        "status": POST_STATUS,

        "acf": {
            #"news_type": item_type,      # ACF item type
            "short_title": title,
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


def publish_to_hub(row):
    hub_name = row["hub_name"]

    endpoint = f"https://{hub_name}/wp-json/wp/v2/{REST_BASE}"
    auth = HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS)
    payload = build_payload(row)

    resp = requests.post(endpoint, auth=auth, json=payload, timeout=20)

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Publish error {resp.status_code}: {resp.text[:500]}")

    return resp.json()


def insert_article_push(conn, row, wp_response):
    """
    Insert the publish result into article_push.
    """
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        INSERT INTO article_push (
            article_id,
            hub_id,
            wp_news_item_id,
            wp_news_item_url,
            wp_news_item_status,
            pushed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = (
        row["article_id"],
        row["hub_id"],
        wp_response.get("id"),
        wp_response.get("link", ""),
        wp_response.get("status", POST_STATUS),
        now,
    )

    c = conn.cursor()
    c.execute(query, values)
    conn.commit()
    c.close()


def main():
    conn = mysql.connector.connect(**DB_CONFIG)

    try:
        rows = get_pending_articles(conn)

        if not rows:
            print("No pending articles to publish.")
            return

        print(f"Found {len(rows)} article(s) to publish...")

        for row in rows:
            aid = row["article_id"]
            hub = row["hub_name"]

            try:
                print(f"→ Publishing article {aid} to hub {hub}...")
                wp_result = publish_to_hub(row)
                insert_article_push(conn, row, wp_result)

                print(
                    f"Article {aid} → '{hub}' → WP #{wp_result.get('id')} "
                    f"({wp_result.get('status')})"
                )

            except Exception as e:
                print(f"Failed to publish article {aid} to {hub}: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
