"""
Configuration file for the Supermarket Scraping System
"""
import os
from typing import Dict, Any


def get_default_config() -> Dict[str, Any]:
    """Get default configuration settings"""
    return {
        'database': {
            'host': 'localhost',
            'user': 'root',
            'password': '',  # Set your MySQL password
            'database': 'supermarket_products',
            'port': 3306,
            'charset': 'utf8mb4',
            'autocommit': True
        },
        'scraping': {
            'request_timeout': 30,
            'retry_attempts': 3,
            'delay_between_requests': 1.0,
            'concurrent_scrapers': False,  # Set to True for parallel scraping
            'max_products_per_category': None,  # None for unlimited
            'log_level': 'INFO'
        },
        'scrapers': {
            # Dirk configuration
            'dirk': {
                'enabled': True,
                'sitemap_url': 'https://www.dirk.nl/sitemap_products_categories.xml',
                'max_pages_per_category': None,
                'use_json_ld': True,  # Prefer JSON-LD over HTML parsing
                'fallback_to_html': True
            },
            # AH configuration
            'ah': {
                'enabled': True,
                'sitemap_url': 'https://www.ah.nl/sitemaps/entities/products/categories.xml',
                'api_base_url': 'https://www.ah.nl/zoeken/api/products/search',
                'page_size': 360,  # Maximum allowed by AH API
                'max_pages_per_category': None
            }
        }
    }


def load_config_from_env() -> Dict[str, Any]:
    """Load configuration from environment variables"""
    config = get_default_config()
    
    # Database configuration from environment
    if os.getenv('DB_HOST'):
        config['database']['host'] = os.getenv('DB_HOST')
    if os.getenv('DB_USER'):
        config['database']['user'] = os.getenv('DB_USER')
    if os.getenv('DB_PASSWORD'):
        config['database']['password'] = os.getenv('DB_PASSWORD')
    if os.getenv('DB_NAME'):
        config['database']['database'] = os.getenv('DB_NAME')
    if os.getenv('DB_PORT'):
        config['database']['port'] = int(os.getenv('DB_PORT'))
    
    # Scraping configuration from environment
    if os.getenv('REQUEST_TIMEOUT'):
        config['scraping']['request_timeout'] = int(os.getenv('REQUEST_TIMEOUT'))
    if os.getenv('RETRY_ATTEMPTS'):
        config['scraping']['retry_attempts'] = int(os.getenv('RETRY_ATTEMPTS'))
    if os.getenv('DELAY_BETWEEN_REQUESTS'):
        config['scraping']['delay_between_requests'] = float(os.getenv('DELAY_BETWEEN_REQUESTS'))
    if os.getenv('LOG_LEVEL'):
        config['scraping']['log_level'] = os.getenv('LOG_LEVEL')
    
    return config


# Example .env file content:
ENV_EXAMPLE = """
# Database Configuration
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=supermarket_products
DB_PORT=3306

# Scraping Configuration
REQUEST_TIMEOUT=30
RETRY_ATTEMPTS=3
DELAY_BETWEEN_REQUESTS=1.0
LOG_LEVEL=INFO
"""


def create_env_file():
    """Create a sample .env file"""
    env_path = os.path.join(os.path.dirname(__file__), '.env.example')
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write(ENV_EXAMPLE.strip())
    print(f"Created example environment file: {env_path}")
    print("Copy this to '.env' and update with your actual values")


if __name__ == "__main__":
    create_env_file()
