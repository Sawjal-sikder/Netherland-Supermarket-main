import logging
import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

from database import DatabaseManager, get_db_config
from Supermarkets.dirk import DirkScraper
from Supermarkets.ah import AHScraper
from Supermarkets.jumbo import JumboScraper
from Supermarkets.hoogvliet import HoogvlietScraper
from Supermarkets.aldi import AldiScraper
from Supermarkets.lidl import LidlScraper
from Supermarkets.plus import PlusScraper
from Supermarkets.dekamarkt import DekamarktScraper

def setup_logging(scraper_name: str):
    """Set up logging configuration"""
    log_filename = f"{scraper_name}_scraper.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

def run_scraper(scraper_name: str, force_full_scrape: bool = False, product_limit: int | None = None):
    """
    Run a specific scraper
    
    Args:
        scraper_name: Name of the scraper ('dirk', 'ah', 'jumbo', 'hoogvliet', 'aldi', 'lidl', 'plus', 'dekamarkt')
        force_full_scrape: Unused (kept for CLI compatibility)
        product_limit: If provided, limit number of products saved (useful for tests)
    """
    setup_logging(scraper_name)
    logger = logging.getLogger(__name__)
    logger.info(f"--- Starting new {scraper_name.upper()} scraping job ---")

    db_config = get_db_config()
    db_manager = DatabaseManager(db_config)

    if not db_manager.connect():
        logger.error("Could not connect to the database. Exiting.")
        return False

    try:
        # Map CLI names to actual supermarket codes
        scraper_code_mapping = {
            'dirk': 'DIRK',
            'ah': 'AH', 
            'jumbo': 'JUMBO',
            'hoogvliet': 'HOOGVLIET',
            'aldi': 'ALDI',
            'lidl': 'LIDL',
            'plus': 'PLUS',
            'dekamarkt': 'DEKA'
        }
        
        supermarket_code = scraper_code_mapping.get(scraper_name.lower())
        if not supermarket_code:
            logger.error(f"Unknown scraper: {scraper_name}")
            return False
            
        # Ensure supermarket exists (auto-create if missing)
        db_manager.ensure_supermarket(supermarket_code)

        # Initialize scraper
        name = scraper_name.lower()
        if name == 'dirk':
            scraper = DirkScraper(db_manager)
            
        elif name == 'ah':
            scraper = AHScraper(db_manager)
        elif name == 'jumbo':
            scraper = JumboScraper(db_manager)
        elif name == 'hoogvliet':
            scraper = HoogvlietScraper(db_manager)
        elif name == 'dekamarkt':
            scraper = DekamarktScraper(db_manager)
        elif name == 'aldi':
            scraper = AldiScraper(db_manager)
        elif name == 'lidl':
            scraper = LidlScraper(db_manager)
        elif name == 'plus':
            scraper = PlusScraper(db_manager)
        else:
            logger.error(f"Unknown scraper: {scraper_name}")
            return False
        
        # Run via BaseScraper to manage session and saving
        scraper.run(product_limit=product_limit)
        
        logger.info(f"--- {scraper_name.upper()} scraping job finished ---")
        return True

    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return False
    finally:
        db_manager.disconnect()
        logger.info("--- Database connection closed ---")

def main():
    """Main function with command line argument parsing"""
    load_dotenv()
    
    # Add startup logging for DigitalOcean visibility
    print("=" * 60)
    print("🛒 SUPERMARKET SCRAPERS STARTING")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🐍 Python version: {os.sys.version}")
    print(f"📁 Working directory: {os.getcwd()}")
    print("=" * 60)
    
    parser = argparse.ArgumentParser(description='Run supermarket scrapers')
    parser.add_argument('scraper', choices=['dirk', 'ah', 'jumbo', 'hoogvliet', 'aldi', 'lidl', 'plus', 'dekamarkt', 'all'], 
                       help='Which scraper to run')
    parser.add_argument('--full', action='store_true', 
                       help='(Deprecated) Force full scrape, ignoring last scrape date')
    parser.add_argument('--limit', type=int, default=None, 
                       help='Limit number of products to save (useful for tests)')
    
    args = parser.parse_args()
    
    success = True
    
    if args.scraper == 'all':
        print("Running all scrapers (Dirk, AH, Jumbo, Hoogvliet, ALDI, LIDL, Plus, and Dekamarkt)...")
        print(f"🎯 Target: All 8 supermarket scrapers")
        if args.limit:
            print(f"⚠️  Product limit: {args.limit} per scraper")
        for name in ['dirk', 'ah', 'jumbo', 'hoogvliet', 'aldi', 'lidl', 'plus', 'dekamarkt']:
            print(f"\n🏪 Starting {name.upper()} scraper...")
            success &= run_scraper(name, args.full, args.limit)
    else:
        print(f"🎯 Target: {args.scraper.upper()} scraper only")
        if args.limit:
            print(f"⚠️  Product limit: {args.limit}")
        success = run_scraper(args.scraper, args.full, args.limit)
    
    print("\n" + "=" * 60)
    if success:
        print("✅ All scraping jobs completed successfully!")
    else:
        print("❌ One or more scraping jobs failed. Check the logs for details.")
        exit(1)
    print("=" * 60)

if __name__ == "__main__":
    main()
