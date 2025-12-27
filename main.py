"""
Main Scraping Script - Simplified Architecture
Orchestrates all supermarket scrapers with clean error handling
"""
import logging
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from database import DatabaseManager, get_db_config, Product
from Supermarkets.dirk import DirkScraper
from Supermarkets.ah import AHScraper


class ScrapingOrchestrator:
    """Main orchestrator for all supermarket scrapers"""
    
    def __init__(self):
        self.config = get_db_config()
        self.scrapers = {}
        self.results = {}
        self.setup_logging()
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        # Create logs directory
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        
        # Configure logging
        log_filename = logs_dir / f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=" * 50)
        self.logger.info("SUPERMARKET SCRAPING SESSION STARTED")
        self.logger.info("=" * 50)
    
    def initialize_scrapers(self, db_manager: DatabaseManager) -> bool:
        """Initialize all available scrapers"""
        try:
            # Initialize scrapers
            scrapers_config = [
                ('dirk', DirkScraper, "Dirk van den Broek"),
                ('ah', AHScraper, "Albert Heijn")
            ]
            
            for scraper_key, scraper_class, description in scrapers_config:
                try:
                    scraper = scraper_class(db_manager)
                    self.scrapers[scraper_key] = scraper
                    self.logger.info(f"✓ Initialized scraper for {description}")
                except Exception as e:
                    self.logger.error(f"✗ Failed to initialize scraper for {description}: {e}")
            
            self.logger.info(f"Successfully initialized {len(self.scrapers)} scrapers")
            return len(self.scrapers) > 0
            
        except Exception as e:
            self.logger.error(f"Failed to initialize scrapers: {e}")
            return False
    
    def run_scraper(self, scraper_name: str, scraper) -> Dict[str, Any]:
        """Run a single scraper and return results"""
        start_time = time.time()
        result = {
            'scraper': scraper_name,
            'start_time': datetime.now(),
            'success': False,
            'products_scraped': 0,
            'products_saved': 0,
            'duration': 0,
            'error': None
        }
        
        try:
            self.logger.info(f"Starting scraper: {scraper_name}")
            
            # Run the scraper
            products = scraper.scrape_products()
            result['products_scraped'] = len(products)
            
            # Save products to database
            saved_count = 0
            for product in products:
                try:
                    scraper.db_manager.save_product(product)
                    saved_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to save product {product.name}: {e}")
            
            result['products_saved'] = saved_count
            result['success'] = True
            
            self.logger.info(f"✓ {scraper_name}: Scraped {len(products)} products, saved {saved_count}")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"✗ {scraper_name} failed: {e}")
        
        finally:
            result['duration'] = time.time() - start_time
            result['end_time'] = datetime.now()
        
        return result
    
    def run_all_scrapers(self, selected_scrapers: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run all or selected scrapers"""
        session_start = time.time()
        
        # Determine which scrapers to run
        scrapers_to_run = selected_scrapers or list(self.scrapers.keys())
        available_scrapers = [s for s in scrapers_to_run if s in self.scrapers]
        
        if not available_scrapers:
            self.logger.error("No valid scrapers selected")
            return {'success': False, 'error': 'No valid scrapers selected'}
        
        self.logger.info(f"Running scrapers: {', '.join(available_scrapers)}")
        
        # Initialize database connection
        try:
            with DatabaseManager(self.config) as db_manager:
                # Initialize scrapers with database connection
                if not self.initialize_scrapers(db_manager):
                    return {'success': False, 'error': 'Failed to initialize scrapers'}
                
                # Create scraping session record
                session_id = db_manager.create_scraping_session()
                self.logger.info(f"Created scraping session: {session_id}")
                
                # Run each scraper
                for scraper_name in available_scrapers:
                    scraper = self.scrapers[scraper_name]
                    result = self.run_scraper(scraper_name, scraper)
                    self.results[scraper_name] = result
                    
                    # Update session with results
                    db_manager.update_scraping_session(
                        session_id, 
                        scraper_name, 
                        result['products_scraped'],
                        result['success']
                    )
                
                # Summary
                total_scraped = sum(r['products_scraped'] for r in self.results.values())
                total_saved = sum(r['products_saved'] for r in self.results.values())
                successful_scrapers = sum(1 for r in self.results.values() if r['success'])
                
                session_summary = {
                    'success': True,
                    'session_id': session_id,
                    'scrapers_run': len(available_scrapers),
                    'scrapers_successful': successful_scrapers,
                    'total_products_scraped': total_scraped,
                    'total_products_saved': total_saved,
                    'total_duration': time.time() - session_start,
                    'results_by_scraper': self.results
                }
                
                self.log_session_summary(session_summary)
                return session_summary
                
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return {'success': False, 'error': f'Database connection failed: {e}'}
    
    def log_session_summary(self, summary: Dict[str, Any]):
        """Log detailed session summary"""
        self.logger.info("=" * 50)
        self.logger.info("SCRAPING SESSION SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"Session ID: {summary['session_id']}")
        self.logger.info(f"Scrapers run: {summary['scrapers_run']}")
        self.logger.info(f"Scrapers successful: {summary['scrapers_successful']}")
        self.logger.info(f"Total products scraped: {summary['total_products_scraped']}")
        self.logger.info(f"Total products saved: {summary['total_products_saved']}")
        self.logger.info(f"Total duration: {summary['total_duration']:.2f} seconds")
        
        self.logger.info("\nPer-scraper results:")
        for scraper_name, result in summary['results_by_scraper'].items():
            status = "✓" if result['success'] else "✗"
            self.logger.info(f"  {status} {scraper_name}: {result['products_scraped']} scraped, "
                           f"{result['products_saved']} saved ({result['duration']:.2f}s)")
            if result['error']:
                self.logger.info(f"    Error: {result['error']}")
        
        self.logger.info("=" * 50)


def main():
    """Main entry point"""
    orchestrator = ScrapingOrchestrator()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        selected_scrapers = sys.argv[1].split(',')
        print(f"Running selected scrapers: {selected_scrapers}")
    else:
        selected_scrapers = None
        print("Running all available scrapers...")
    
    # Run scrapers
    try:
        results = orchestrator.run_all_scrapers(selected_scrapers)
        
        if results['success']:
            print(f"\n✓ Scraping completed successfully!")
            print(f"  Total products: {results['total_products_scraped']}")
            print(f"  Saved to database: {results['total_products_saved']}")
            print(f"  Duration: {results['total_duration']:.2f} seconds")
        else:
            print(f"\n✗ Scraping failed: {results.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠ Scraping interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
