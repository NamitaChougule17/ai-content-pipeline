import mysql.connector
from mysql.connector import Error
from urllib.parse import urlparse
import requests
from requests.auth import HTTPBasicAuth
from config.db_config import DB_CONFIG
from config.wp_config import WP_BASE_URL, WP_USERNAME, WP_APP_PASSWORD


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def extract_slug_from_url(url: str) -> str | None:
    """
    Extract the slug from a WordPress URL.
    """
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return None

    segments = [seg for seg in path.split("/") if seg]
    if not segments:
        return None

    return segments[-1]


def fetch_wp_post_id_by_slug(slug: str) -> int | None:
    """
    Use the WordPress REST API to get post ID from slug.
    """
    url = f"{WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/posts"
    try:
        resp = requests.get(
            url,
            params={"slug": slug},
            auth=HTTPBasicAuth(WP_USERNAME, WP_APP_PASSWORD),
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] WP API call failed for slug '{slug}': {e}")
        return None

    try:
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0].get("id")
    except Exception:
        pass

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
                print(f"[WARN] Could not extract slug from {post_url} (id={post_id})")
                continue

            print(f"[INFO] Looking up WP post ID for slug '{slug}'...")

            wp_post_id = fetch_wp_post_id_by_slug(slug)
            if not wp_post_id:
                print(f"[WARN] No WP post found for slug '{slug}' (id={post_id})")
                continue

            cursor.execute(
                "UPDATE posts SET wp_post_id=%s WHERE id=%s",
                (wp_post_id, post_id),
            )
            conn.commit()

            print(f"[OK] posts.id={post_id} updated with wp_post_id={wp_post_id}")

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
