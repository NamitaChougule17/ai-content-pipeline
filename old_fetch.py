import feedparser
from bs4 import BeautifulSoup
from dateutil import parser
import mysql.connector
from config.db_config import DB_CONFIG

def extract_image_from_html(html_content):
    """Extract the first image URL from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    img_tag = soup.find("img")
    return img_tag["src"] if img_tag else ""

def fetch_rss_feed(url):
    """Fetch and parse articles from a single RSS feed URL."""
    feed = feedparser.parse(url)
    articles = []

    for entry in feed.entries:
        title = entry.get("title")
        link = entry.get("link")
        summary = entry.get("summary", "")
        content = entry.get("content", [{"value": ""}])[0]["value"]
        image_url = extract_image_from_html(content) or extract_image_from_html(summary)
        author = entry.get("author", "")

        if not author and "authors" in entry and len(entry.authors) > 0:
            author = entry.authors[0].get("name", "")

        published = parser.parse(entry.get("published", "")) if entry.get("published") else None

        articles.append({
            "title": title,
            "url": link,
            "content": content,
            "summary": summary,
            "image_url": image_url,
            "published": published,
            "category": None  # Will be filled in later
        })

    return articles

def insert_article(article):
    """Insert an article into MySQL database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
        INSERT INTO articles (Title, URL, Content, Image_URL, Summary, Category, Published)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(query, (
            article['title'],
            article['url'],
            article['content'],
            article['image_url'],
            article['summary'],
            article['category'],
            article['published']
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

def fetch_and_store_all():
    """Fetch all articles and store them in MySQL."""
    feed_urls = [
        "https://www.luizasnewsletter.com/feed",
        "https://garymarcus.substack.com/feed",
        "https://www.hyperdimensional.co/feed",
        "https://digitalspirits.substack.com/feed",
        "https://onepercentrule.substack.com/feed",
        "https://futuresdigest.substack.com/feed",
        "https://www.humanityredefined.com/feed",
        "https://bengoertzel.substack.com/feed",
        "https://davekarpf.substack.com/feed",
        "https://diamantai.substack.com/feed",
        "https://www.ai-supremacy.com/feed",
        "https://www.understandingai.org/feed"
    ]

    for url in feed_urls:
        try:
            articles = fetch_rss_feed(url)
            for article in articles:
                insert_article(article)
        except Exception as e:
            print(f"Error fetching from {url}: {e}")

if __name__ == "__main__":
    fetch_and_store_all()
 