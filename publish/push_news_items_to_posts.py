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
    Fetch all article_push rows where:
    - wp_news_item_id exists (news published)
    - pushed_post_id is NULL (not yet pushed)
    """
    query = """
        SELECT
            ap.id AS push_id,
            ap.article_id,
            ap.hub_id,
            ap.wp_news_item_id,
            h.hub_name
        FROM article_push ap
        JOIN hubs h ON h.id = ap.hub_id
        WHERE ap.wp_news_item_id IS NOT NULL
          AND ap.pushed_post_id IS NULL
        ORDER BY ap.id ASC
    """

    cur = conn.cursor(dictionary=True)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


def push_news_to_featured_post(hub_name, wp_news_item_id):
    """
    Push a news item into the FIRST featured post.
    Uses Todd's endpoint: /news_to_news_post?news_id=<id>
    """

    base = f"https://{hub_name}/wp-json/onair/v2"
    auth = HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS)

    # The ONLY endpoint we are using now
    url = f"{base}/news_to_news_post?news_id={wp_news_item_id}"

    print(f"[DEBUG] Calling URL: {url}")

    try:
        resp = requests.put(url, auth=auth, timeout=20)
    except Exception as e:
        return "failed", str(e)

    if resp.status_code in (200, 201):
        return "ok", url

    return "failed", resp.text[:300]


def update_push_record(conn, push_id, status):
    """
    Update article_push row to indicate push result.
    We do NOT store post_id now because we aren't targeting specific posts.
    """
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        UPDATE article_push
        SET
            pushed_post_status = %s,
            pushed_to_post_at = %s
        WHERE id = %s
    """

    cur = conn.cursor()
    cur.execute(query, (status, now, push_id))
    conn.commit()
    cur.close()


def main():
    conn = get_db_connection()

    try:
        rows = get_unpushed_news_items(conn)
        if not rows:
            print("No news items to push.")
            return

        print(f"Found {len(rows)} item(s) to push into featured post...")

        for row in rows:
            push_id = row["push_id"]
            hub_name = row["hub_name"]
            news_item_id = row["wp_news_item_id"]

            print(
                f"[INFO] Attaching news_item={news_item_id} "
                f"to FIRST featured post on hub={hub_name}"
            )

            status, detail = push_news_to_featured_post(
                hub_name, news_item_id
            )

            update_push_record(conn, push_id, status)

            if status == "ok":
                print(f"[OK] push_id={push_id} successfully attached → {detail}")
            else:
                print(f"[FAIL] push_id={push_id} failed: {detail}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
