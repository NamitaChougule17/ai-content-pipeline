# db/article_repository.py
import mysql.connector
from config.db_config import DB_CONFIG

def insert_article(article: dict):
    """
    Expects `article` to have keys:
      short_title, url, source, source_other, author, content,
      summary, category, more_than_1, date
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
        INSERT INTO articles
          (short_title, url, source, source_other, author, content,
           summary, category, more_than_1, date)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            query,
            (
                article.get("short_title", "").strip(),
                article["url"],                                  # UNIQUE
                article.get("source", "Other"),                  # default per schema
                article.get("source_other"),
                article.get("author"),
                article.get("content"),
                article.get("summary"),
                article.get("category"),
                int(article.get("more_than_1", 0)),              # TINYINT
                article.get("date"),                              # pretty string e.g. "August 03, 2025"
            ),
        )

        conn.commit()
        print(f"Inserted: {article.get('short_title', '')}")
    except mysql.connector.errors.IntegrityError:
        # URL already exists (UNIQUE)
        print(f"Duplicate skipped: {article.get('url')}")
    except Exception as e:
        print(f"Error inserting article: {e}")
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
