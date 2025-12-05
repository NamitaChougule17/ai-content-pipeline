import mysql.connector
from mysql.connector import Error
from urllib.parse import urlparse
import requests
from requests.auth import HTTPBasicAuth
from config.db_config import DB_CONFIG
from config.wp_config import WP_DEFAULT_USER, WP_DEFAULT_PASS


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def extract_slug_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return None
    return path.split("/")[-1]


def fetch_wp_post_id(post_url, slug):
    """
    Build WP REST URL dynamically using ONLY post_url.
    """
    parsed = urlparse(post_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    api_url = f"{base_url}/wp-json/wp/v2/posts"

    try:
        resp = requests.get(
            api_url,
            params={"slug": slug},
            auth=HTTPBasicAuth(WP_DEFAULT_USER, WP_DEFAULT_PASS),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0].get("id")
    except Exception as e:
        print(f"[ERROR] Failed fetching post ID from {api_url}: {e}")

    return None


def populate_wp_post_ids():
    """
    Updates only posts where wp_post_id IS NULL.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT id, post_url FROM posts WHERE wp_post_id IS NULL")
        rows = cursor.fetchall()

        if not rows:
            print("[INFO] No posts with NULL wp_post_id.")
            return

        for row in rows:
            post_id = row["id"]
            post_url = row["post_url"]

            slug = extract_slug_from_url(post_url)
            if not slug:
                print(f"[WARN] Could not extract slug from {post_url}")
                continue

            print(f"[INFO] Looking up WP post ID for slug '{slug}'")

            wp_id = fetch_wp_post_id(post_url, slug)
            if not wp_id:
                print(f"[WARN] No WP post found for slug '{slug}'")
                continue

            cursor.execute(
                "UPDATE posts SET wp_post_id=%s WHERE id=%s",
                (wp_id, post_id),
            )
            conn.commit()

            print(f"[OK] posts.id={post_id} updated with wp_post_id={wp_id}")

    except Error as e:
        print(f"[DB ERROR] {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    populate_wp_post_ids()
