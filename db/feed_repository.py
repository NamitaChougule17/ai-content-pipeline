import mysql.connector
from config.db_config import DB_CONFIG


def get_all_active_feeds():
    try:
        """
        Returns a list of tuples:
        (feed_id, feed_url, feed_name, feed_category, hub_name)
        One row per (feed, hub) pair based on feed_hub_map.
        """
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
            SELECT 
            rf.id AS feed_id,
            rf.feed_url AS feed_url,
            rf.feed_name AS feed_name,
            rf.feed_category AS feed_category,
            h.hub_name AS hub_name
            FROM rss_feeds rf
            JOIN feed_hub_map fhm ON fhm.feed_id = rf.id
            JOIN hubs h ON h.id = fhm.hub_id
            WHERE rf.active = 1
            ORDER BY h.hub_name, rf.feed_name;
        """
        cursor.execute(query)
        return cursor.fetchall() 
    except Exception as e:
        print(f" Failed to load feed URLs from DB: {e}")
        return []
    finally:
        try: cursor.close()
        except: pass
        try: conn.close()
        except: pass
