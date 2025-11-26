import mysql.connector
from config.db_config import DB_CONFIG

def insert_article(article):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        INSERT INTO articles (
            feed_id,
            hub_name,
            short_title,
            url,
            source,
            source_other,
            author,
            content,
            summary,
            category,
            more_than_1,
            date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        article.get("feed_id"),
        article.get("hub_name"),
        article.get("short_title"),
        article.get("url"),
        article.get("source", "Other"),
        article.get("source_other"),
        article.get("author"),
        article.get("content"),
        article.get("summary"),
        article.get("category"),
        int(article.get("more_than_1") or 0),
        article.get("date"),
    )

    try:
        cursor.execute(query, values)
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error inserting article ({article.get('url')}): {e}")
    finally:
        cursor.close()
        conn.close()
