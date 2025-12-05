# push_news_items_to_posts.py
# FINAL VERSION — USE EXISTING wp_post_id ONLY
# No lookup, no slug extraction, no updates to posts table.

import requests
import mysql.connector
from datetime import datetime
from requests.auth import HTTPBasicAuth

from config.db_config import DB_CONFIG
from config.wp_config import WP_DEFAULT_USER, WP_DEFAULT_PASS


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_unpushed_news_items(conn):
    """
    Fetch news_items that must be pushed to related posts.
    NOTE: wp_post_id MUST already be populated by populate_wp_post_ids.py
    """
    query = """
        SELECT
            ap.id AS push_id,
            ap.article_id,
            ap.hub_id,
            ap.wp_news_item_id,
            h.hub_name,
            p.id AS post_local_id,
            p.wp_post_id AS wp_post_id
        FROM article_push ap
        JOIN hubs h ON h.id = ap.hub_id
        LEFT JOIN feed_post_map fpm ON fpm.feed_id = (
            SELECT feed_id FROM articles WHERE id = ap.article_id LIMIT 1
        )
        LEFT JOIN posts p ON p.id = fpm.post_id
        WHERE ap.wp_news_item_id IS NOT NULL
          AND ap.pushed_post_id IS NULL
        ORDER BY ap.id ASC
    """

    cur = conn.cursor(dictionary=True)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


def push_news_to_post(hub_name, wp_news_item_id, wp_post_id):
    """
    Use Todd’s 2 endpoints to attach news_item to a post.
    """
    base = f"https://{hub_name}/wp-json"
    auth = HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS)

    endpoints = [
        f"{base}/news_to_post?news_item_id={wp_news_item_id}&post_id={wp_post_id}",
        f"{base}/news_to_news_post?news_item_id={wp_news_item_id}&post_id={wp_post_id}",
    ]

    last_error = None

    for url in endpoints:
        resp = requests.post(url, auth=auth, timeout=20)
        if resp.status_code in (200, 201):
            return "ok", url

        last_error = resp.text[:300]

    return "failed", last_error or "Unknown error"


def update_push_record(conn, push_id, wp_post_id, status):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        UPDATE article_push
        SET
            pushed_post_id = %s,
            pushed_post_status = %s,
            pushed_to_post_at = %s
        WHERE id = %s
    """

    cur = conn.cursor()
    cur.execute(query, (wp_post_id, status, now, push_id))
    conn.commit()
    cur.close()


def main():
    conn = get_db_connection()

    try:
        rows = get_unpushed_news_items(conn)
        if not rows:
            print("No pending push-to-post items.")
            return

        print(f"Found {len(rows)} item(s) to push into posts...")

        for row in rows:
            push_id = row["push_id"]
            hub_name = row["hub_name"]
            news_item_id = row["wp_news_item_id"]
            wp_post_id = row["wp_post_id"]

            # ❗ Option A: If wp_post_id is missing → skip quietly
            if not wp_post_id:
                print(f"[INFO] push_id={push_id}: wp_post_id missing → skipping.")
                continue

            print(
                f"[INFO] Attaching news_item={news_item_id} "
                f"→ wp_post_id={wp_post_id} on hub={hub_name}"
            )

            status, detail = push_news_to_post(hub_name, news_item_id, wp_post_id)

            update_push_record(conn, push_id, wp_post_id, status)

            if status == "ok":
                print(f"[OK] push_id={push_id} successfully attached → {detail}")
            else:
                print(f"[FAIL] push_id={push_id} failed: {detail}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
