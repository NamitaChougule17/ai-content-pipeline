# categorize_hybrid.py
# Hybrid categorization: keywords first, then calibrated zero-shot fallback.
# - Uses DB creds from config/db_config.py (expects DB_CONFIG = {...})
# - Title gets extra weight; title+summary first, then content if needed
# - Whole-word keyword matching with per-label MIN_HITS
# - Zero-shot uses per-label thresholds + margin vs. best
# - "US Network" requires US + gov/politics co-occurrence
# - Final tie-breaker avoids "Uncategorized" when a label has minimal evidence

import re
import mysql.connector
from config.db_config import DB_CONFIG
from transformers import pipeline

# ---------------- SETTINGS ----------------
LABELS = ["AI Policy", "AI", "Data Engineering", "US Network"]

# Zero-shot candidate descriptions (perform better than bare labels)
LABEL_DESCRIPTIONS = {
    "AI Policy": "AI Policy (laws, regulation, governance, compliance, standards, oversight)",
    "AI": "AI (LLMs, ML models, agents, reasoning, computer vision, NLP, generative AI)",
    "Data Engineering": "Data Engineering (ETL/ELT, data pipelines, storage, orchestration, Spark, Kafka, Snowflake, Airflow, Databricks)",
    "US Network": "US Network (US government, Congress, elections, agencies, committees, President, governors)",
}

# --- Keyword map (AI Policy tightened to avoid tech bleed) ---
CATEGORY_KEYWORDS = {
    "AI Policy": [
        "ai regulation", "ai law", "governance", "policy", "compliance",
        "responsible ai", "ai act", "ai ethics", "oversight", "standard",
        "un governs", "robots govern", "countries govern", "oligarchs govern",
        "scenarios from futurists", "beneficial asi", "international regulation",
        "united nations", "united states", "europe", "africa", "middle east", "asia"
        # intentionally removed broad tech terms to reduce false AI Policy tags
    ],
    "AI": [
        "artificial intelligence", "machine learning", "deep learning",
        "neural network", "llm", "large language model", "gpt",
        "transformer model", "computer vision", "nlp", "genai",
        "generative ai", "reasoning model", "reasoning models",
        "ai agents", "data centers", "quantum computing", "cybersecurity",
        "agi", "scientific research", "asi", "asi scenarios", "robots",
        "robotics", "industrial robot", "home robot", "other robots",
        "humanoid robot", "autonomous robot", "artificial general intelligence"
    ],
    "Data Engineering": [
        "etl", "elt", "data pipeline", "apache kafka", "airflow", "snowflake",
        "redshift", "spark", "databricks", "data warehouse", "dbt", "ingestion",
        "collection", "storage", "cleaning", "transformation", "ai & data engineering",
        "delivery", "governance", "infrastructure", "privacy", "security",
        "data migration", "business intelligence", "ai and machine learning",
        "data science", "e-commerce analytics", "financial services",
        "fraud detection", "manufacturing", "public health", "real-time analytics",
        "processing", "analytics", "orchestration", "programming languages",
        "loading", "visualization", "warehousing", "top data engineering jobs","datastrategy","Iceberg","Databricks"
    ],
    "US Network": [
        "united states", "u.s.", "us network", "america", "washington", "dc",
        "federal", "statewide", "office of the president", "all us senators",
        "democratic senate leaders", "democratic senators", "republican senate leaders",
        "republican senators", "democratic house leaders", "democratic house members",
        "republican house leaders", "republican house members", "us executive branch",
        "trump cabinet", "key trump appointments", "us government departments",
        "key us agencies", "us senate committees", "us house committees",
        "joint committees", "us judicial branch", "2020 presidential race",
        "2020 us senate races", "2020 governor races", "2022 all governor races",
        "2022 competitive governor races", "2022 all senate races",
        "2022 competitive us senate races", "2022 all house races",
        "2022 competitive us house races", "2024 us presidential race",
        "2024 competitive us senate races", "2024 competitive us house races"
    ],
}

# Minimum keyword hits per label
MIN_HITS = {"AI Policy": 2, "AI": 2, "Data Engineering": 2, "US Network": 2}

# Zero-shot thresholds (AI Policy stricter, DE easier)
THRESHOLD = 0.78
LABEL_THRESH = {
    "AI Policy": 0.86,     # stricter
    "AI": 0.78,
    "Data Engineering": 0.68,  # easier so DE stories aren’t missed
    "US Network": 0.80,
}
# Keep labels within this margin of the best zero-shot score
ZS_MARGIN = 0.10  # 10%

# Final fallback — if nothing survives, pick best label if >= this score
FINAL_MIN_SCORE = 0.15

# Use content or not (title/summary first is cleaner)
USE_CONTENT_IN_TEXT = False

# Input shaping
TITLE_WEIGHT = 2
SUMMARY_WEIGHT = 1
CONTENT_SLICE = 800

# DB + runtime
BATCH_SIZE = 150
ONLY_EMPTY_CATEGORY = True
MAX_CHARS = 1200
MODEL_NAME = "facebook/bart-large-mnli"
UNCATEGORIZED_LABEL = "Uncategorized"
DEBUG_SCORES = False  # set True to print scores
# ----------------------------------------

# --- Technical vs Policy regex (for biasing) ---
TECH_INDICATORS = re.compile(
    r"\b(etl|elt|pipeline|pipelines|orchestration|airflow|dag|spark|kafka|snowflake|redshift|bigquery|databricks|dbt|parquet|lakehouse|data\s?warehouse|data\s?lake|schema|ingestion|batch|stream(ing)?)\b",
    re.I,
)
POLICY_TERMS = re.compile(
    r"\b(policy|regulation|regulatory|law|legislation|act|governance|standard|compliance|oversight)\b",
    re.I,
)

# US Network co-occurrence gate
US_REQUIRED = re.compile(r"\b(us|u\.s\.|united states|america)\b", re.I)
US_GOV = re.compile(r"\b(president|senate|house|govern(or|ors)|committee|agency|cabinet|election|congress)\b", re.I)
def passes_us_network_gate(text: str) -> bool:
    return bool(US_REQUIRED.search(text) and US_GOV.search(text))

# Weighted text builder
def build_text(title, summary, content):
    t = (title or "").strip()
    s = (summary or "").strip()
    c = (content or "")[:CONTENT_SLICE] if USE_CONTENT_IN_TEXT else ""
    return " ".join([t] * TITLE_WEIGHT + [s] * SUMMARY_WEIGHT + [c]).strip()

# Model (lazy)
_classifier = None
def clf():
    global _classifier
    if _classifier is None:
        _classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
    return _classifier

# DB helpers
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

# Compile whole-word keyword patterns
def _compile_patterns():
    compiled = {}
    for label, words in CATEGORY_KEYWORDS.items():
        pats = []
        for w in words:
            w_esc = re.escape(w.strip().lower())
            pats.append(re.compile(rf"\b{w_esc}\b", re.IGNORECASE))
        compiled[label] = pats
    return compiled

_PATTERNS = _compile_patterns()

def keyword_labels(text: str):
    if not text:
        return []
    hits = []
    for label, pats in _PATTERNS.items():
        count = sum(1 for p in pats if p.search(text))
        if count >= MIN_HITS.get(label, 1):
            hits.append((label, count))
    # US gate if hit via keywords
    filtered = []
    for lbl, cnt in hits:
        if lbl == "US Network" and not passes_us_network_gate(text):
            continue
        filtered.append((lbl, cnt))
    filtered.sort(key=lambda x: (-x[1], LABELS.index(x[0])))
    return [lbl for lbl, _ in filtered]

# Weak keyword counts (>=1 hit)
def weak_keyword_hit_counts(text: str):
    counts = {}
    for label, pats in _PATTERNS.items():
        cnt = sum(1 for p in pats if p.search(text))
        if cnt >= 1:
            counts[label] = cnt
    return counts

# Zero-shot with thresholds + margin + weak-hit support + tech-over-policy bias + final tie-breaker
def zero_shot_labels(text: str):
    if not text:
        return []
    text = text[:MAX_CHARS]
    candidate_texts = [LABEL_DESCRIPTIONS[lbl] for lbl in LABELS]
    res = clf()(text, candidate_labels=candidate_texts, multi_label=True)

    labels = LABELS[:]
    scores = [float(s) for s in res["scores"]]
    best = max(scores) if scores else 0.0

    if DEBUG_SCORES:
        print({lbl: round(s, 3) for lbl, s in zip(labels, scores)})

    # --- Tech-over-policy bias: if technical and not policy, prefer DE when close to AI Policy ---
    is_tech = bool(TECH_INDICATORS.search(text))
    is_policy = bool(POLICY_TERMS.search(text))
    if is_tech and not is_policy:
        try:
            ai_pol_idx = labels.index("AI Policy")
            de_idx = labels.index("Data Engineering")
            if scores[de_idx] + 0.05 >= scores[ai_pol_idx]:
                scores[de_idx] += 0.04
                scores[ai_pol_idx] -= 0.04
                best = max(scores)
        except ValueError:
            pass

    weak_hits = weak_keyword_hit_counts(text)

    kept = []
    for lbl, s in zip(labels, scores):
        thr = LABEL_THRESH.get(lbl, THRESHOLD)
        # relax if weak keyword present; widen margin slightly
        local_thr = thr - 0.10 if lbl in weak_hits else thr
        local_margin = ZS_MARGIN + (0.05 if lbl in weak_hits else 0.0)
        if s >= local_thr and (best - s) <= local_margin:
            kept.append(lbl)

    # Enforce US gate
    if "US Network" in kept and not passes_us_network_gate(text):
        kept.remove("US Network")

    # Final tie-breaker: if nothing survived, pick best label if it has minimal evidence
    if not kept and scores:
        best_idx = scores.index(best)
        best_lbl = labels[best_idx]
        if best >= FINAL_MIN_SCORE:
            if best_lbl != "US Network" or passes_us_network_gate(text):
                kept = [best_lbl]

    return kept

def categorize_text(title, summary, content):
    # Pass 1: title + summary
    text_ts = build_text(title, summary, "")
    labels = keyword_labels(text_ts)
    if labels:
        return labels
    labels = zero_shot_labels(text_ts)
    if labels:
        return labels

    # Pass 2: include content slice only if still empty
    text_all = build_text(title, summary, content)
    labels = keyword_labels(text_all)
    if labels:
        return labels
    return zero_shot_labels(text_all)

def main():
    conn = get_conn()
    total = 0
    try:
        while True:
            rows = fetch_batch(conn)
            if not rows:
                break
            for r in rows:
                cats = categorize_text(r.get("short_title"), r.get("summary"), r.get("content"))
                cats_str = ", ".join(cats) if cats else UNCATEGORIZED_LABEL
                update_category(conn, r["id"], cats_str)
                total += 1
                print(f"[{r['id']}] -> {cats_str}")
        print(f"\nDone. Updated {total} article(s).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
