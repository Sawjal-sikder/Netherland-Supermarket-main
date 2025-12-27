"""
Base scraper class following clean architecture principles
"""
from abc import ABC, abstractmethod
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime

from database import DatabaseManager, Product, PriceCalculator, UnitType


class BaseScraper(ABC):
    """Abstract base class for all supermarket scrapers"""
    
    def __init__(self, db_manager: DatabaseManager, supermarket_code: str):
        self.db_manager = db_manager
        self.supermarket_code = supermarket_code
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.session = self._create_session()
        # Limit of products to scrape/save (optional, can be set by runner)
        self.product_limit: Optional[int] = None
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with common headers"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        return session
    
    @abstractmethod
    def scrape_products(self) -> List[Product]:
        """
        Main scraping method to be implemented by each supermarket scraper
        
        Returns:
            List of Product objects
        """
        pass
    
    def run(self, product_limit: Optional[int] = None) -> int:
        """
        Run the complete scraping process
        
        Args:
            product_limit: If provided, only this many products will be saved.
        
        Returns:
            Number of products scraped (saved)
        """
        session_id = self.db_manager.start_scraping_session(self.supermarket_code)
        products_saved = 0
        
        # Set optional product limit on the scraper instance for downstream use
        self.product_limit = product_limit
        
        try:
            self.logger.info(f"Starting {self.supermarket_code} scraper")
            if self.product_limit:
                self.logger.info(f"Product limit set to {self.product_limit}")
            
            # Scrape products
            products = self.scrape_products()
            self.logger.info(f"Scraped {len(products)} products")
            
            # Apply limit before saving if provided
            if self.product_limit is not None:
                products = products[: self.product_limit]
                self.logger.info(f"Limiting to first {len(products)} products before saving")
            
            # Save products to database
            if products:
                products_saved = self.db_manager.save_products_batch(products)
            
            # End session successfully
            self.db_manager.end_scraping_session(session_id, products_saved, 'completed')
            self.logger.info(f"Scraping completed: {products_saved} products saved")
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}", exc_info=True)
            self.db_manager.end_scraping_session(session_id, products_saved, 'failed', str(e))
            raise
            
        finally:
            # Clear limit after run
            self.product_limit = None
            
        return products_saved
    
    def _extract_price_info(self, soup: BeautifulSoup, price_selectors: dict) -> tuple[float, Optional[float], Optional[str]]:
        """
        Extract price information from soup using provided selectors
        
        Args:
            soup: BeautifulSoup object
            price_selectors: Dictionary with price selectors
            
        Returns:
            Tuple of (current_price, original_price, discount_type)
        """
        current_price = None
        original_price = None
        discount_type = None
        
        # Try to find current price
        for selector in price_selectors.get('current', []):
            element = soup.select_one(selector)
            if element:
                price_text = element.get_text(strip=True)
                price_match = re.search(r'(\d+[.,]\d{2})', price_text.replace(',', '.'))
                if price_match:
                    current_price = float(price_match.group(1))
                    break
        
        # Try to find original price (for discounts)
        for selector in price_selectors.get('original', []):
            element = soup.select_one(selector)
            if element:
                price_text = element.get_text(strip=True)
                price_match = re.search(r'(\d+[.,]\d{2})', price_text.replace(',', '.'))
                if price_match:
                    original_price = float(price_match.group(1))
                    break
        
        # Try to find discount information
        for selector in price_selectors.get('discount', []):
            element = soup.select_one(selector)
            if element:
                discount_type = element.get_text(strip=True)
                break
        
        return current_price, original_price, discount_type
    
    def _extract_unit_amount(self, soup: BeautifulSoup, unit_selectors: List[str]) -> str:
        """Extract unit amount information"""
        for selector in unit_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                # Clean up common unit patterns
                unit_match = re.search(r'(\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))', text.lower())
                if unit_match:
                    return unit_match.group(1).replace(',', '.')
        
        return "1 piece"  # Default fallback
    
    def _extract_category(self, soup: BeautifulSoup, category_selectors: List[str]) -> str:
        """Extract category information"""
        for selector in category_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        
        return "Unknown"  # Default fallback
    
    def _create_product(self, product_id: str, name: str, category: str, 
                       price: float, unit_amount: str, original_price: Optional[float] = None,
                       discount_type: Optional[str] = None, brand: str = None,
                       discount_start_date: Optional[datetime] = None,
                       discount_end_date: Optional[datetime] = None,
                       image_url: Optional[str] = None) -> Product:
        """
        Create a Product object with calculated fields
        
        Args:
            product_id: Unique product identifier
            name: Product name
            category: Product category
            price: Current price
            unit_amount: Unit amount string (e.g., "500g")
            original_price: Original price if on discount
            discount_type: Type of discount
            brand: Product brand
            discount_start_date: Start date of discount (datetime object)
            discount_end_date: End date of discount (datetime object)
            image_url: URL to product image
            
        Returns:
            Product object
        """
        # Calculate price per unit
        price_per_unit, unit_type = PriceCalculator.calculate_price_per_unit(price, unit_amount)
        
        # Generate search tags
        search_tags = PriceCalculator.generate_search_tags(name, category, brand)
        
        # Use provided discount dates, or try to parse from discount text if not provided
        if discount_start_date is None and discount_end_date is None and discount_type and original_price:
            # Try to extract dates from discount text (fallback for legacy scrapers)
            date_match = re.search(r'(\d{1,2})\s+(\w+)\s+t/m\s+(\d{1,2})\s+(\w+)', discount_type)
            if date_match:
                # This is a simple example - you might need more sophisticated date parsing
                try:
                    # Parse Dutch date format if found
                    current_year = datetime.now().year
                    discount_start_date = datetime.now().date()  # Simplified
                    discount_end_date = datetime.now().date()    # Simplified
                except:
                    pass
        
        return Product(
            product_id=product_id,
            name=name.strip(),
            category_name=category.strip(),
            price=price,
            unit_amount=unit_amount,
            price_per_unit=price_per_unit,
            unit_type=unit_type,
            supermarket_code=self.supermarket_code,
            search_tags=search_tags,
            original_price=original_price,
            discount_type=discount_type,
            discount_start_date=discount_start_date,
            discount_end_date=discount_end_date,
            image_url=image_url
        )
