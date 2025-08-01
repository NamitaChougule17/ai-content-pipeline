import mysql.connector
from config.db_config import DB_CONFIG


def get_all_active_feeds():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT feed_url FROM rss_feeds WHERE active = TRUE")
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Failed to load feed URLs from DB: {e}")
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
