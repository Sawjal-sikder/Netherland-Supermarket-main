"""
Lidl Offers Scraper
Uses the existing LidlScraper from lidl.py to extract offer products
No difference from regular Lidl scraping - just reuses the same implementation
"""

import sys
import os
from typing import List

# Add parent directories to path for proper imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, grandparent_dir])

from Supermarkets.lidl import LidlScraper
from database import Product


class LidlOfferScraper(LidlScraper):
    """Lidl offers scraper that uses the existing LidlScraper implementation"""
    
    def __init__(self, db_manager):
        # Initialize using the parent LidlScraper class
        super().__init__(db_manager)
        # Update logger name to reflect this is for offers
        self.logger.name = "LidlOfferScraper"
        
    def scrape_products(self) -> List[Product]:
        """Scrape offer products from Lidl - same as regular scraping"""
        self.logger.info("Starting Lidl offers scraping (using regular Lidl scraper)")
        
        # Call the parent class method - no difference for offers
        products = super().scrape_products()
        
        self.logger.info(f"Lidl offers scraping completed: {len(products)} products found")
        return products


# Test function for development
def test_lidl_offers_scraper():
    """Test the Lidl offers scraper"""
    import logging
    from database import DatabaseManager
    import config
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Get database config
        db_config = config.get_default_config()['database']
        db_manager = DatabaseManager(db_config)
        
        # Create and test the scraper
        scraper = LidlOfferScraper(db_manager)
        scraper.product_limit = 10  # Limit for testing
        
        products = scraper.scrape_products()
        
        print(f"\n🎯 Lidl Offers Test Results:")
        print(f"📦 Total products found: {len(products)}")
        
        if products:
            print(f"\n🛍️ Sample products:")
            for i, product in enumerate(products[:5], 1):
                print(f"  {i}. {product.name}")
                print(f"     Price: €{product.price}")
                print(f"     Category: {product.category_name}")
                if product.discount_type:
                    print(f"     Discount: {product.discount_type}")
                print()
        
        print("✅ Lidl offers scraper test completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_lidl_offers_scraper()
