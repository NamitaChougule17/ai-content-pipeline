import requests
import mysql.connector
from datetime import datetime
from requests.auth import HTTPBasicAuth

from config.db_config import DB_CONFIG
from config.wp_config import WP_DEFAULT_USER, WP_DEFAULT_PASS


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_unpushed_featured_news_items(conn):
    """
    Fetch article_push rows where:
      - wp_news_item_id exists
      - NOT yet pushed to featured post
    """

    query = """
        SELECT
            ap.id AS push_id,
            ap.wp_news_item_id,
            h.hub_name
        FROM article_push ap
        JOIN hubs h ON h.id = ap.hub_id
        WHERE ap.wp_news_item_id IS NOT NULL
          AND ap.posted_to_featured_post = 0
        ORDER BY ap.id ASC
    """

    cur = conn.cursor(dictionary=True)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


def push_news_to_featured_post(hub_name, wp_news_item_id):
    """
    Todd's endpoint:
      /wp-json/onair/v2/news_to_news_post?news_id=<news_id>

    WordPress auto-selects the first post
    in the 0-featured-posts category.
    """

    base = f"https://{hub_name}/wp-json/onair/v2"
    auth = HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS)

    url = f"{base}/news_to_news_post?news_id={wp_news_item_id}"

    print(f"[DEBUG] PUT {url}")

    try:
        resp = requests.put(url, auth=auth, timeout=20)
    except Exception as e:
        return "failed", f"Request error: {e}"

    if resp.status_code in (200, 201):
        return "ok", url

    return "failed", resp.text[:300]


def mark_featured_push_success(conn, push_id):
    """
    Option 1:
    Update DB ONLY on success.
    """

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        UPDATE article_push
        SET
            posted_to_featured_post = 1,
            posted_to_featured_post_at = %s
        WHERE id = %s
    """

    cur = conn.cursor()
    cur.execute(query, (now, push_id))
    conn.commit()
    cur.close()


def push_pending_news_items_to_featured_posts():
    """
    Callable function.
    Safe to run multiple times.
    """

    conn = get_db_connection()

    try:
        rows = get_unpushed_featured_news_items(conn)
        if not rows:
            print("[INFO] No pending featured-post pushes.")
            return

        print(f"[INFO] Found {len(rows)} news item(s) to push to featured posts")

        for row in rows:
            push_id = row["push_id"]
            hub_name = row["hub_name"]
            news_item_id = row["wp_news_item_id"]

            print(
                f"[INFO] push_id={push_id}: "
                f"pushing news_item={news_item_id} to featured post on {hub_name}"
            )

            status, detail = push_news_to_featured_post(
                hub_name, news_item_id
            )

            if status == "ok":
                mark_featured_push_success(conn, push_id)
                print(f"[OK] push_id={push_id} featured-post success → {detail}")
            else:
                print(f"[FAIL] push_id={push_id} featured-post failed → {detail}")
                print("[INFO] DB not updated; will retry next run.")

    finally:
        conn.close()


if __name__ == "__main__":
    push_pending_news_items_to_featured_posts()
