import mysql.connector
from config.db_config import DB_CONFIG


def get_all_active_feeds():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT feed_url, source_name, source_category, hub
            FROM rss_feeds
            WHERE active = TRUE
        """)
        return cursor.fetchall()  # [(url, source_name, source_category), ...]
    except Exception as e:
        print(f" Failed to load feed URLs from DB: {e}")
        return []
    finally:
        try: cursor.close()
        except: pass
        try: conn.close()
        except: pass
