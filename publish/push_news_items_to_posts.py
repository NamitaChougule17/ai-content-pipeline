# push_news_items_to_posts.py

import requests
import mysql.connector
from datetime import datetime
from requests.auth import HTTPBasicAuth

from config.db_config import DB_CONFIG
from config.wp_config import WP_DEFAULT_USER, WP_DEFAULT_PASS


def get_unpushed_news_items(conn):
    """
    Get all article_push rows that:
      - Have wp_news_item_id (published)
      - Have NOT been pushed to post (pushed_post_id is NULL)
    Join with:
      - articles (feed_id)
      - feed_post_map (post mapping)
      - posts (post details)
      - hubs (hub_name)
    """
    query = """
        SELECT
            ap.id AS push_id,
            ap.article_id,
            ap.hub_id,
            ap.wp_news_item_id,
            h.hub_name,
            a.feed_id,
            p.id AS post_id,
            p.post_name,
            p.post_url
        FROM article_push ap
        JOIN hubs h ON h.id = ap.hub_id
        JOIN articles a ON a.id = ap.article_id
        LEFT JOIN feed_post_map fpm ON fpm.feed_id = a.feed_id
        LEFT JOIN posts p ON p.id = fpm.post_id
        WHERE ap.wp_news_item_id IS NOT NULL
          AND ap.pushed_post_id IS NULL
        ORDER BY ap.id ASC
    """

    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def push_news_to_post(hub_name, wp_news_item_id, post_id):
    """
    Try /news_to_post first.
    If that fails, try /news_to_news_post.
    """
    base = f"https://{hub_name}/wp-json"
    auth = HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS)

    endpoints = [
        f"{base}/news_to_post?news_item_id={wp_news_item_id}&post_id={post_id}",
        f"{base}/news_to_news_post?news_item_id={wp_news_item_id}&post_id={post_id}",
    ]

    last_error = None

    for url in endpoints:
        resp = requests.post(url, auth=auth, timeout=20)
        if resp.status_code in (200, 201):
            return "ok", url
        last_error = resp.text[:400]

    return "failed", last_error or "Unknown error"


def update_push_record(conn, push_id, post_id, status):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        UPDATE article_push
        SET
            pushed_post_id = %s,
            pushed_post_status = %s,
            pushed_to_post_at = %s
        WHERE id = %s
    """

    c = conn.cursor()
    c.execute(query, (post_id, status, now, push_id))
    conn.commit()
    c.close()


def main():
    conn = mysql.connector.connect(**DB_CONFIG)

    try:
        rows = get_unpushed_news_items(conn)
        if not rows:
            print("No news_items left to attach to posts.")
            return

        print(f"Found {len(rows)} item(s) to push to posts...")

        for row in rows:
            push_id = row["push_id"]
            hub_name = row["hub_name"]
            news_id = row["wp_news_item_id"]
            post_id = row["post_id"]

            if post_id is None:
                print(f"→ push_id {push_id}: feed has no post mapping — skipping.")
                continue

            print(
                f"→ Attaching news_item {news_id} → post {post_id} "
                f"on {hub_name}..."
            )

            try:
                status, detail = push_news_to_post(hub_name, news_id, post_id)
                update_push_record(conn, push_id, post_id, status)

                print(
                    f" push_id {push_id} → {status} ({detail})"
                    if status == "ok"
                    else f" push_id {push_id} → FAILED: {detail}"
                )

            except Exception as e:
                print(f" push_id {push_id} error: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
