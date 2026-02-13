import os

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'stocks'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
}

# AlphaVantage API key for ETF holdings fallback
AV_API_KEY = os.getenv('AV_API_KEY', '')

# Path to stock_data_pipeline CSVs for ETF holdings
CSV_DIR = os.getenv(
    'CSV_DIR',
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 'stock_data_pipeline')
)

# RS computation defaults
DEFAULT_WINDOW = 252
FULL_HISTORY_START = "2020-01-01"

# Twelve Data fallback API
TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY', '')
TWELVE_DATA_BATCH_SIZE = 120  # max tickers per API call

# Retry settings for rate limits
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_BACKOFF_BASE = 30  # seconds: 30, 60, 120

# Parallelism
DEFAULT_MAX_WORKERS = min(os.cpu_count() or 4, 8)
DEFAULT_BATCH_SIZE = 100
YFINANCE_BATCH_SIZE = 500
