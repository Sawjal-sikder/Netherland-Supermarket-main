"""
Daily Offers Scraping Script
Orchestrates offer-specific scrapers for daily deal hunting
"""
import logging
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from database import DatabaseManager, get_db_config
from Supermarkets.offers.dirk_offers import DirkOfferScraper
from Supermarkets.offers.ah_offers import AHOfferScraper
from Supermarkets.offers.aldi_offers import AldiOfferScraper
from Supermarkets.offers.jumbo_offers import JumboOfferScraper
from Supermarkets.offers.other_offers import (
    LidlOfferScraper,
    PlusOfferScraper,
    DekamarktOfferScraper,
    HoogvlietOfferScraper
)


class OfferScrapingOrchestrator:
    """Main orchestrator for daily offer scraping"""
    
    def __init__(self):
        self.config = get_db_config()
        self.setup_logging()
        self.scrapers = self._initialize_scrapers()
        self.results = {}

    def setup_logging(self):
        """Setup logging for offer scraping"""
        # Create logs directory
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        
        # Configure logging
        log_filename = logs_dir / f"offers_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=" * 60)
        self.logger.info("DAILY OFFERS SCRAPING SESSION STARTED")
        self.logger.info("=" * 60)

    def _initialize_scrapers(self) -> Dict[str, Any]:
        """Initialize all offer scrapers"""
        return {
            'dirk': DirkOfferScraper,
            'ah': AHOfferScraper,
            'aldi': AldiOfferScraper,
            'jumbo': JumboOfferScraper,
            'lidl': LidlOfferScraper,
            'plus': PlusOfferScraper,
            'dekamarkt': DekamarktOfferScraper,
            'hoogvliet': HoogvlietOfferScraper
        }

    def run_all_offers(self, product_limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        """Run all offer scrapers"""
        self.logger.info("Starting daily offers scraping for all supermarkets")
        
        with DatabaseManager(self.config) as db_manager:
            for scraper_name, scraper_class in self.scrapers.items():
                try:
                    self.logger.info(f"Starting {scraper_name.upper()} offer scraping")
                    
                    scraper = scraper_class(db_manager)
                    products_scraped = scraper.run(product_limit=product_limit)
                    
                    self.results[scraper_name] = {
                        'status': 'success',
                        'products_count': products_scraped,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    self.logger.info(f"{scraper_name.upper()} offers: {products_scraped} products scraped")
                    
                except Exception as e:
                    self.logger.error(f"Failed to scrape {scraper_name} offers: {e}")
                    self.results[scraper_name] = {
                        'status': 'failed',
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    }

        return self.results

    def run_specific_offers(self, supermarket_names: list, product_limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        """Run offer scraping for specific supermarkets"""
        self.logger.info(f"Starting offers scraping for: {', '.join(supermarket_names)}")
        
        with DatabaseManager(self.config) as db_manager:
            for scraper_name in supermarket_names:
                if scraper_name not in self.scrapers:
                    self.logger.error(f"Unknown supermarket: {scraper_name}")
                    self.results[scraper_name] = {
                        'status': 'failed',
                        'error': f'Unknown supermarket: {scraper_name}',
                        'timestamp': datetime.now().isoformat()
                    }
                    continue
                
                try:
                    self.logger.info(f"Starting {scraper_name.upper()} offer scraping")
                    
                    scraper_class = self.scrapers[scraper_name]
                    scraper = scraper_class(db_manager)
                    products_scraped = scraper.run(product_limit=product_limit)
                    
                    self.results[scraper_name] = {
                        'status': 'success',
                        'products_count': products_scraped,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    self.logger.info(f"{scraper_name.upper()} offers: {products_scraped} products scraped")
                    
                except Exception as e:
                    self.logger.error(f"Failed to scrape {scraper_name} offers: {e}")
                    self.results[scraper_name] = {
                        'status': 'failed',
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    }

        return self.results

    def print_summary(self):
        """Print summary of scraping results"""
        self.logger.info("=" * 60)
        self.logger.info("DAILY OFFERS SCRAPING SUMMARY")
        self.logger.info("=" * 60)
        
        total_products = 0
        successful_scrapers = 0
        failed_scrapers = 0
        
        for scraper_name, result in self.results.items():
            status = result['status']
            if status == 'success':
                products_count = result['products_count']
                total_products += products_count
                successful_scrapers += 1
                self.logger.info(f"✓ {scraper_name.upper()}: {products_count} offers")
            else:
                failed_scrapers += 1
                error = result.get('error', 'Unknown error')
                self.logger.error(f"✗ {scraper_name.upper()}: {error}")
        
        self.logger.info("-" * 60)
        self.logger.info(f"Total offers scraped: {total_products}")
        self.logger.info(f"Successful scrapers: {successful_scrapers}")
        self.logger.info(f"Failed scrapers: {failed_scrapers}")
        self.logger.info("=" * 60)


def run_offer_scraper(supermarket_name: str, product_limit: Optional[int] = None):
    """Run a specific offer scraper"""
    orchestrator = OfferScrapingOrchestrator()
    results = orchestrator.run_specific_offers([supermarket_name], product_limit)
    orchestrator.print_summary()
    return results


def main():
    """Main entry point for offer scraping"""
    parser = argparse.ArgumentParser(description='Daily Offers Scraper for Netherlands Supermarkets')
    parser.add_argument('--supermarket', '-s', type=str, 
                       choices=['dirk', 'ah', 'aldi', 'jumbo', 'lidl', 'plus', 'dekamarkt', 'hoogvliet', 'all'],
                       default='all',
                       help='Supermarket to scrape offers from (default: all)')
    parser.add_argument('--limit', '-l', type=int, default=None,
                       help='Limit number of products to scrape (useful for testing)')
    parser.add_argument('--multiple', '-m', nargs='+', 
                       choices=['dirk', 'ah', 'aldi', 'jumbo', 'lidl', 'plus', 'dekamarkt', 'hoogvliet'],
                       help='Scrape multiple specific supermarkets')
    
    args = parser.parse_args()
    
    orchestrator = OfferScrapingOrchestrator()
    
    try:
        if args.multiple:
            # Scrape multiple specific supermarkets
            results = orchestrator.run_specific_offers(args.multiple, args.limit)
        elif args.supermarket == 'all':
            # Scrape all supermarkets
            results = orchestrator.run_all_offers(args.limit)
        else:
            # Scrape single supermarket
            results = orchestrator.run_specific_offers([args.supermarket], args.limit)
        
        orchestrator.print_summary()
        
        # Return appropriate exit code
        failed_scrapers = sum(1 for result in results.values() if result['status'] == 'failed')
        if failed_scrapers == len(results):
            sys.exit(1)  # All failed
        elif failed_scrapers > 0:
            sys.exit(2)  # Some failed
        else:
            sys.exit(0)  # All successful
            
    except KeyboardInterrupt:
        orchestrator.logger.info("Offer scraping interrupted by user")
        sys.exit(130)
    except Exception as e:
        orchestrator.logger.error(f"Unexpected error during offer scraping: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()