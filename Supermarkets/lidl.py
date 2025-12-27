"""
Lidl Scraper
Uses Lidl's search API to fetch product data from specific category
Focuses on category ID '10068374' as requested
"""
import requests
import logging
import json
import re
import math
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse
from datetime import datetime

from base_scraper import BaseScraper
from database import Product, PriceCalculator


class LidlScraper(BaseScraper):
    """Scraper for Lidl.nl supermarket using their search API"""

    BASE_URL = "https://www.lidl.nl"
    API_URL = "https://www.lidl.nl/q/api/search"
    TARGET_CATEGORY_ID = "10068374"  # Specific category to scrape
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "LIDL")
        
        # Set up cookies and headers as per the working example
        self.cookies = {
            'CookieConsent': '{necessary:true%2Cpreferences:false%2Cstatistics:false%2Cmarketing:false}',
            'LidlID': 'abe7da47-53a5-4487-8855-3ed437715f5d',
            'i18n_redirected': 'nl_NL',
        }
        
        self.api_headers = {
            'accept': 'application/mindshift.search+json;version=2',
            'accept-language': 'en-US,en;q=0.7',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        }

    def scrape_products(self) -> List[Product]:
        """Scrape all products from the specific Lidl category"""
        products = []
        
        try:
            # Fetch all products using pagination
            api_products = self._fetch_all_products()
            self.logger.info(f"Found {len(api_products)} products from Lidl category {self.TARGET_CATEGORY_ID}")
            
            # Process each product
            for i, api_product in enumerate(api_products, 1):
                try:
                    if i % 50 == 0:
                        self.logger.info(f"Processed {i}/{len(api_products)} products")
                    
                    product = self._process_api_product(api_product)
                    if product:
                        products.append(product)
                        
                except Exception as e:
                    product_name = api_product.get('gridbox', {}).get('data', {}).get('fullTitle', 'Unknown')
                    self.logger.error(f"Failed to process product '{product_name}': {e}")
                    continue
            
            self.logger.info(f"Successfully processed {len(products)} products from Lidl")
            return products
            
        except Exception as e:
            self.logger.error(f"Failed to scrape Lidl products: {e}")
            return products

    def _fetch_all_products(self) -> List[Dict[str, Any]]:
        """
        Fetch all products from the target category using pagination
        """
        all_products = []
        offset = 0
        page_size = 100
        page_count = 0
        
        while True:
            self.logger.info(f"Fetching page {page_count + 1} (offset: {offset})...")
            
            params = {
                'offset': str(offset),
                'fetchsize': str(page_size),
                'locale': 'nl_NL',
                'assortment': 'NL',
                'version': '2.1.0',
                'category.id': self.TARGET_CATEGORY_ID,
            }
            
            try:
                response = requests.get(
                    self.API_URL, 
                    params=params, 
                    headers=self.api_headers, 
                    cookies=self.cookies,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', [])
                    total_found = data.get('numFound', 0)
                    
                    self.logger.info(f"  Page {page_count + 1}: Found {len(items)} products (Total available: {total_found})")
                    
                    if items:
                        all_products.extend(items)
                        page_count += 1
                        
                        # Check if we've got all products
                        if len(all_products) >= total_found or len(items) < page_size:
                            self.logger.info(f"Reached end of results. Total fetched: {len(all_products)}")
                            break
                        
                        offset += len(items)
                    else:
                        self.logger.info("No more products found")
                        break
                        
                else:
                    self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching page {page_count + 1}: {e}")
                break
        
        return all_products

    def _process_api_product(self, api_item: Dict[str, Any]) -> Optional[Product]:
        """Process a single product from Lidl API response"""
        try:
            # Extract data from the nested structure
            gridbox_data = api_item.get('gridbox', {}).get('data', {})
            
            # Basic product information
            product_id = str(gridbox_data.get('productId', ''))
            name = gridbox_data.get('fullTitle', '').strip()
            category = gridbox_data.get('category', 'Unknown').strip()
            
            if not product_id or not name:
                self.logger.debug(f"Skipping product with missing ID or name")
                return None
            
            # Price extraction - handle both regular and lidlPlus pricing
            current_price, original_price, discount_type, unit_amount = self._extract_price_info(gridbox_data)
            
            if current_price is None or current_price <= 0:
                self.logger.debug(f"Skipping product {name} - no valid price found")
                return None
            
            # Validate price calculation before creating product
            try:
                test_price_per_unit, test_unit_type = PriceCalculator.calculate_price_per_unit(current_price, unit_amount)
                if test_price_per_unit <= 0:
                    self.logger.warning(f"Invalid price calculation for {name}: €{current_price}/{unit_amount} = €{test_price_per_unit}")
                    return None
            except Exception as calc_error:
                self.logger.error(f"Price calculation failed for product {name} (ID: {product_id}): {calc_error}")
                self.logger.debug(f"  Price: {current_price}, Unit: {unit_amount}")
                return None
            
            # Log product info for debugging
            self.logger.debug(f"Processing: {name} - €{current_price} - {unit_amount}")
            if discount_type:
                self.logger.debug(f"  Discount: {discount_type}")
            
            # Extract image URL from gridbox data
            image_url = ''
            image_data = gridbox_data.get('image') or gridbox_data.get('imageUrl')
            if image_data:
                if isinstance(image_data, str):
                    image_url = image_data
                elif isinstance(image_data, dict):
                    image_url = image_data.get('url', '') or image_data.get('src', '')
            
            return self._create_product(
                product_id=product_id,
                name=name,
                category=category,
                price=current_price,
                unit_amount=unit_amount,
                original_price=original_price,
                discount_type=discount_type,
                brand=None,  # Lidl doesn't seem to provide brand info in this API
                discount_start_date=None,  # Lidl doesn't provide date fields in API
                discount_end_date=None,
                image_url=image_url
            )
            
        except Exception as e:
            product_name = api_item.get('gridbox', {}).get('data', {}).get('fullTitle', 'unknown')
            self.logger.error(f"Failed to process API product '{product_name}': {e}")
            return None

    def _extract_price_info(self, gridbox_data: Dict[str, Any]) -> tuple:
        """
        Extract price information from gridbox data.
        Handles both regular pricing and lidlPlus pricing.
        Returns: (current_price, original_price, discount_type, unit_amount)
        """
        current_price = None
        original_price = None
        discount_type = None
        unit_amount = "1 piece"
        
        # First, try regular price structure
        regular_price_dict = gridbox_data.get('price', {})
        regular_price = regular_price_dict.get('price')
        old_price = regular_price_dict.get('oldPrice')
        
        # Check for lidlPlus pricing
        lidl_plus_array = gridbox_data.get('lidlPlus', [])
        lidl_plus_price = None
        lidl_plus_old_price = None
        
        if lidl_plus_array and len(lidl_plus_array) > 0:
            lidl_plus_price_dict = lidl_plus_array[0].get('price', {})
            lidl_plus_price = lidl_plus_price_dict.get('price')
            lidl_plus_old_price = lidl_plus_price_dict.get('oldPrice')
        
        # Determine which price to use (prefer regular, fallback to lidlPlus)
        if regular_price is not None and regular_price > 0:
            try:
                current_price = float(regular_price)
            except (ValueError, TypeError):
                print(f"Error converting regular_price to float: {regular_price}")
                return None, None, None, None
            
            original_price = None
            if old_price and old_price > current_price:
                try:
                    original_price = float(old_price)
                except (ValueError, TypeError):
                    print(f"Error converting old_price to float: {old_price}")
                    original_price = None
            
            # Extract unit from regular price packaging
            packaging = regular_price_dict.get('packaging', {})
            unit_text = packaging.get('text', '')
            if unit_text:
                unit_amount = self._clean_unit_text(unit_text)
            
            # Check for discount in regular price
            discount_dict = regular_price_dict.get('discount', {})
            if discount_dict:
                discount_type = discount_dict.get('discountText', 'Discount')
        
        elif lidl_plus_price is not None and lidl_plus_price > 0:
            try:
                current_price = float(lidl_plus_price)
            except (ValueError, TypeError):
                print(f"Error converting lidl_plus_price to float: {lidl_plus_price}")
                return None, None, None, None
            
            original_price = None
            if lidl_plus_old_price and lidl_plus_old_price > current_price:
                try:
                    original_price = float(lidl_plus_old_price)
                except (ValueError, TypeError):
                    print(f"Error converting lidl_plus_old_price to float: {lidl_plus_old_price}")
                    original_price = None
            discount_type = "Lidl Plus Price"
            
            # Try to get unit from lidlPlus packaging if available
            if lidl_plus_array and len(lidl_plus_array) > 0:
                lidl_plus_packaging = lidl_plus_array[0].get('price', {}).get('packaging', {})
                unit_text = lidl_plus_packaging.get('text', '')
                if unit_text:
                    unit_amount = self._clean_unit_text(unit_text)
        
        return current_price, original_price, discount_type, unit_amount

    def _clean_unit_text(self, unit_text: str) -> str:
        """Clean and standardize unit text from Lidl API"""
        if not unit_text:
            return "1 piece"
        
        unit_text = unit_text.strip()
        
        # Handle common Dutch unit formats
        unit_text = unit_text.replace('st.', 'pieces')
        unit_text = unit_text.replace('stuks', 'pieces')
        unit_text = unit_text.replace('stuk', 'piece')
        
        # Normalize whitespace
        unit_text = re.sub(r'\s+', ' ', unit_text)
        
        # If it doesn't look like a proper unit (no numbers), default to piece
        if not re.search(r'\d+\s*[a-zA-Z]', unit_text) and not re.search(r'x\s*\d+', unit_text):
            return '1 piece'
        
        return unit_text


# Test function
def test_lidl_scraper():
    """Test function for development"""
    from database import DatabaseManager, get_db_config
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    config = get_db_config()
    
    with DatabaseManager(config) as db:
        scraper = LidlScraper(db)
        
        # Test fetching products
        products = scraper.scrape_products()
        print(f"Successfully found {len(products)} products from Lidl category {scraper.TARGET_CATEGORY_ID}")
        
        if products:
            # Show first few products
            print("\nSample products:")
            for i, product in enumerate(products[:5]):
                print(f"\n{i+1}. {product.name}")
                print(f"   Price: €{product.price}")
                print(f"   Unit: {product.unit_amount}")
                print(f"   Price per unit: €{product.price_per_unit}/{product.unit_type.value}")
                print(f"   Category: {product.category_name}")
                if product.original_price:
                    print(f"   Original price: €{product.original_price}")
                    print(f"   Discount: {product.discount_type}")
                print(f"   Search Tags: {product.search_tags}")
            
            # Summary by pricing type
            regular_prices = sum(1 for p in products if not p.discount_type or p.discount_type != "Lidl Plus Price")
            lidl_plus_prices = sum(1 for p in products if p.discount_type == "Lidl Plus Price")
            discounted_products = sum(1 for p in products if p.original_price)
            
            print(f"\n=== Pricing Summary ===")
            print(f"Regular prices: {regular_prices}")
            print(f"Lidl Plus only: {lidl_plus_prices}")
            print(f"Products with discounts/original prices: {discounted_products}")


if __name__ == "__main__":
    # Add parent directory to path for imports
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    test_lidl_scraper()
