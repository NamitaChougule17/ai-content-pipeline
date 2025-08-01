from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

# Build MySQL connection config
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT', 3306))  # default to 3306 if not set
}
