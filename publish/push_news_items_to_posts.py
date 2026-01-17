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
    Get all article_push rows that:
      - have a WP news item already created (wp_news_item_id IS NOT NULL)
      - have NOT yet been pushed to a post (pushed_post_id IS NULL)

    We join through feed_post_map -> posts to get wp_post_id for the target post.
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
        JOIN hubs h
          ON h.id = ap.hub_id
        LEFT JOIN feed_post_map fpm
          ON fpm.feed_id = (
                SELECT feed_id FROM articles
                WHERE id = ap.article_id
                LIMIT 1
             )
        LEFT JOIN posts p
          ON p.id = fpm.post_id
        WHERE ap.wp_news_item_id IS NOT NULL
          AND ap.pushed_post_id IS NULL
        ORDER BY ap.id ASC
    """

    cur = conn.cursor(dictionary=True)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


def push_news_to_post(hub_name: str, wp_news_item_id: int, wp_post_id: int):
    """
    Call Todd's endpoint:

      /wp-json/onair/v2/news_to_post?news_id=<news id>&post_id=<post id>

    This attaches the given news item to the specific WordPress post.
    """

    base = f"https://{hub_name}/wp-json/onair/v2"
    auth = HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS)

    url = f"{base}/news_to_post?news_id={wp_news_item_id}&post_id={wp_post_id}"

    print(f"[DEBUG] Calling URL: {url}")

    try:
        resp = requests.put(url, auth=auth, timeout=20)
    except Exception as e:
        return "failed", f"Request error: {e}"

    if resp.status_code in (200, 201):
        return "ok", url

    # Return some detail from WP for debugging
    return "failed", resp.text[:300]


def update_push_record_success(conn, push_id: int, wp_post_id: int):
    """
    Only update DB when the push succeeds.
    We store:
      - pushed_post_id     = wp_post_id (WordPress post ID)
      - pushed_post_status = 1
      - pushed_to_post_at  = timestamp
    """

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        UPDATE article_push
        SET
            pushed_post_id = %s,
            pushed_post_status = 1,
            pushed_to_post_at = %s
        WHERE id = %s
    """

    cur = conn.cursor()
    cur.execute(query, (wp_post_id, now, push_id))
    conn.commit()
    cur.close()


def main():
    conn = get_db_connection()

    try:
        rows = get_unpushed_news_items(conn)
        if not rows:
            print("No pending items to push.")
            return

        print(f"Found {len(rows)} item(s) to push into posts...")

        for row in rows:
            push_id = row["push_id"]
            hub_name = row["hub_name"]
            news_item_id = row["wp_news_item_id"]
            wp_post_id = row["wp_post_id"]
            post_local_id = row["post_local_id"]

            # If there is no mapping to a post or wp_post_id is missing, skip
            if post_local_id is None:
                print(f"[INFO] push_id={push_id}: no post mapping (feed_post_map/posts) → skipping.")
                continue

            if not wp_post_id:
                print(
                    f"[INFO] push_id={push_id}: wp_post_id is NULL for posts.id={post_local_id}. "
                    f"Run populate_posts_wp_id.py first → skipping."
                )
                continue

            print(
                f"[INFO] push_id={push_id}: attaching news_item={news_item_id} "
                f"to wp_post_id={wp_post_id} on hub={hub_name}"
            )

            status, detail = push_news_to_post(hub_name, news_item_id, wp_post_id)

            if status == "ok":
                update_push_record_success(conn, push_id, wp_post_id)
                print(f"[OK] push_id={push_id} successfully attached → {detail}")
            else:
                # ❗ IMPORTANT: do NOT update DB on failure
                # This allows you to fix issues and rerun; the row stays with pushed_post_id = NULL.
                print(f"[FAIL] push_id={push_id} failed: {detail}")
                print("[INFO] DB not updated — item will be retried on next run.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
