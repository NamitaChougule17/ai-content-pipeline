# categorize_zero_shot.py
# Reads uncategorized articles from MySQL, does zero-shot labeling, writes back.

import mysql.connector
from config.db_config import DB_CONFIG
from transformers import pipeline

# ---- settings you can tweak ----
LABELS = ["AI Policy", "AI", "Data Engineering", "US Network"]
THRESHOLD = 0.55          # keep labels with score >= this
BATCH_SIZE = 150          # rows per DB batch
ONLY_EMPTY_CATEGORY = True  # set to False to re-label everything
MAX_CHARS = 2000          # truncate long text for speed
MODEL_NAME = "facebook/bart-large-mnli"
# --------------------------------

_classifier = None
def clf():
    global _classifier
    if _classifier is None:
        _classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
    return _classifier

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def fetch_batch(conn):
    where = "WHERE (category IS NULL OR category='')" if ONLY_EMPTY_CATEGORY else ""
    sql = f"""
      SELECT id, short_title, summary, content
      FROM articles
      {where}
      ORDER BY id ASC
      LIMIT %s
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (BATCH_SIZE,))
    rows = cur.fetchall()
    cur.close()
    return rows

def update_category(conn, article_id, categories_str):
    sql = "UPDATE articles SET category=%s WHERE id=%s"
    cur = conn.cursor()
    cur.execute(sql, (categories_str, article_id))
    conn.commit()
    cur.close()

def classify(text):
    if not text:
        return []
    text = text[:MAX_CHARS]
    res = clf()(text, candidate_labels=LABELS, multi_label=True)
    keep = [lbl for lbl, score in zip(res["labels"], res["scores"]) if float(score) >= THRESHOLD]
    return keep

def main():
    conn = get_conn()
    total = 0
    try:
        while True:
            rows = fetch_batch(conn)
            if not rows:
                break
            for r in rows:
                text = f"{r.get('short_title','')} {r.get('summary','')} {r.get('content','')}"
                cats = classify(text)
                cats_str = ", ".join(cats)
                update_category(conn, r["id"], cats_str)
                total += 1
                print(f"[{r['id']}] -> {cats_str or '(none)'}")
        print(f"\nDone. Updated {total} article(s).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
