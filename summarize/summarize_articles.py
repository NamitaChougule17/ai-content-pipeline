from transformers import PegasusTokenizer, PegasusForConditionalGeneration
import mysql.connector
import re
from config.db_config import DB_CONFIG

# ------------------ Load Model ------------------
def load_model(model_name="google/pegasus-cnn_dailymail"):
    tokenizer = PegasusTokenizer.from_pretrained(model_name)
    model = PegasusForConditionalGeneration.from_pretrained(model_name)
    return tokenizer, model

# ------------------ Get Articles Without Summary ------------------
def get_articles_to_summarize(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, content FROM articles 
        WHERE content IS NOT NULL AND (summary IS NULL OR summary = '')
    """)
    return cursor.fetchall()

# ------------------ Summarize Text with Chunking & Cleanup ------------------
def summarize_text(text, tokenizer, model):
    max_len = tokenizer.model_max_length  # Usually 1024 for Pegasus

    # Tokenize without truncation
    inputs = tokenizer(text, return_tensors="pt", truncation=False, padding="longest")
    input_ids = inputs["input_ids"][0]

    # Split into chunks to avoid truncation
    chunks = [input_ids[i:i + max_len] for i in range(0, len(input_ids), max_len)]
    summaries = []

    for chunk in chunks:
        summary_ids = model.generate(
            chunk.unsqueeze(0),
            max_length=225,
            min_length=150,
            length_penalty=2.0,
            num_beams=4,
            early_stopping=True
        )
        chunk_summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)

        # ✅ Clean Pegasus special tokens like <n>
        chunk_summary = chunk_summary.replace("<n>", " ").strip()

        # ✅ Fix missing spaces between lowercase-uppercase 
        chunk_summary = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', chunk_summary)

        # ✅ Fix missing spaces between letters & numbers
        chunk_summary = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', chunk_summary)
        chunk_summary = re.sub(r'([0-9])([a-zA-Z])', r'\1 \2', chunk_summary)

        summaries.append(chunk_summary)

    # Combine summaries and remove extra spaces
    final_summary = " ".join(summaries)
    final_summary = re.sub(r'\s+', ' ', final_summary).strip()

    return final_summary

# ------------------ Update DB with Summary ------------------
def update_summary(conn, article_id, summary):
    cursor = conn.cursor()
    cursor.execute("UPDATE articles SET summary = %s WHERE id = %s", (summary, article_id))
    conn.commit()

# ------------------ Main Function ------------------
def summarize_and_store_all_articles():
    tokenizer, model = load_model()
    conn = mysql.connector.connect(**DB_CONFIG)

    try:
        articles = get_articles_to_summarize(conn)
        for article_id, content in articles:
            try:
                print(f"📝 Summarizing article ID: {article_id}...")
                summary = summarize_text(content, tokenizer, model)
                update_summary(conn, article_id, summary)
                print(f"✅ Summary saved for article ID {article_id}")
            except Exception as e:
                print(f"❌ Failed to summarize ID {article_id}: {e}")
    finally:
        conn.close()

# ------------------ Run Script ------------------
if __name__ == "__main__":
    summarize_and_store_all_articles()
