import requests
from bs4 import BeautifulSoup
import re

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

        # Prefer <article>, fallback to all <p>
        article_tag = soup.find("article")
        elements = article_tag.find_all(["h1", "h2", "h3", "p"]) if article_tag else soup.find_all(["h1", "h2", "h3", "p"])

        clean_parts = []
        for el in elements:
            text = el.get_text(strip=True)

            # Stop if footer/junk text appears
            if any(footer_phrase in text for footer_phrase in [
                "JavaScript", "Substack", "turn on JavaScript", "Get the app"
            ]):
                break

            if text:
                if el.name in ["h1", "h2", "h3"]:
                    clean_parts.append(f"\n{text.upper()}\n")  # Emphasize headings
                else:
                    clean_parts.append(text)

        # Join and remove excessive spaces/newlines
        full_text = "\n".join(clean_parts)
        full_text = re.sub(r'\n\s*\n+', '\n\n', full_text)  # collapse multiple blank lines
        full_text = re.sub(r'[ \t]+', ' ', full_text)       # collapse extra spaces

        return full_text.strip()

    except Exception as e:
        print(f"⚠️ Error fetching full article from {url}: {e}")
        return ""
