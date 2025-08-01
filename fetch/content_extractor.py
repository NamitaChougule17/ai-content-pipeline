import requests
from bs4 import BeautifulSoup

def fetch_full_article_text(url, return_html=False):
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }

        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        if return_html:
            return str(soup)

        article_tag = soup.find("article")
        if article_tag:
            paragraphs = article_tag.find_all("p")
        else:
            paragraphs = soup.find_all("p")

        clean_paragraphs = []
        for p in paragraphs:
            text = p.get_text(strip=True)

            # Stop collecting if footer/junk starts appearing
            if any(footer_phrase in text for footer_phrase in [
                "JavaScript", "Substack", "turn on JavaScript", "©", "Get the app"
            ]):
                break

            # Skip empty or extremely short paragraphs
            if len(text) > 20:
                clean_paragraphs.append(text)

        full_text = "\n".join(clean_paragraphs)
        return full_text.strip()

    except Exception as e:
        print(f"⚠️ Error fetching full article from {url}: {e}")
        return ""
