import mysql.connector
from config.db_config import DB_CONFIG

def show_all_articles_content():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT content FROM articles")
        rows = cursor.fetchall()

        for i, (content,) in enumerate(rows, 1):
            print(content)  # Print raw content only

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    show_all_articles_content()
