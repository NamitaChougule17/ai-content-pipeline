import mysql.connector
from config.db_config import DB_CONFIG

def insert_article(article):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
        INSERT INTO articles (title, url, content, author, published, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            article['title'],
            article['url'],
            article['content'],
            article['author'],
            article['published'],
            article['fetched_at']
        ))

        conn.commit()
        print(f"Inserted: {article['title']}")
    except mysql.connector.errors.IntegrityError:
        print(f"Duplicate skipped: {article['url']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
