from transformers import PegasusTokenizer, PegasusForConditionalGeneration
import mysql.connector
from config.db_config import DB_CONFIG

def load_model(model_name="google/pegasus-cnn_dailymail"):
    tokenizer = PegasusTokenizer.from_pretrained(model_name)
    model = PegasusForConditionalGeneration.from_pretrained(model_name)
    return tokenizer, model

def get_articles_to_summarize(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, content FROM articles 
        WHERE content IS NOT NULL AND (summary IS NULL OR summary = '')
    """)
    return cursor.fetchall()

def summarize_text(text, tokenizer, model):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding="longest")
    summary_ids = model.generate(
        inputs["input_ids"],
        max_length=200,
        min_length=100,
        length_penalty=2.0,
        num_beams=4,
        early_stopping=True
    )
    return tokenizer.decode(summary_ids[0], skip_special_tokens=True)

def update_summary(conn, article_id, summary):
    cursor = conn.cursor()
    cursor.execute("UPDATE articles SET summary = %s WHERE id = %s", (summary, article_id))
    conn.commit()

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
                print(f"Summary saved for article ID {article_id}")
            except Exception as e:
                print(f"Failed to summarize ID {article_id}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    summarize_and_store_all_articles()
