"""
Aldi Scraper
Uses Algolia search API to fetch product data from Aldi.nl
"""
import requests
import logging
import json
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

from base_scraper import BaseScraper
from database import Product, PriceCalculator


class AldiScraper(BaseScraper):
    """Scraper for Aldi.nl supermarket using Algolia API"""

    BASE_URL = "https://www.aldi.nl"
    API_URL = "https://2hu29pf6bh-2.algolianet.com/1/indexes/an_prd_nl_nl_products/browse"
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "ALDI")
        
        # Set up headers for Algolia API
        self.api_headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive',
            'Origin': 'https://www.aldi.nl',
            'Referer': 'https://www.aldi.nl/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'X-Algolia-API-Key': '686cf0c8ddcf740223d420d1115c94c1',
            'X-Algolia-Application-Id': '2HU29PF6BH'
        }

    def scrape_products(self) -> List[Product]:
        """Scrape all products using Algolia browse API with cursor pagination"""
        products = []
        
        try:
            # Fetch all products using browse method
            api_products = self._fetch_all_products()
            self.logger.info(f"Found {len(api_products)} products from Aldi API")
            
            # Process each product
            for i, api_product in enumerate(api_products, 1):
                try:
                    if i % 100 == 0:
                        self.logger.info(f"Processed {i}/{len(api_products)} products")
                    
                    product = self._process_api_product(api_product)
                    if product:
                        products.append(product)
                        
                except Exception as e:
                    self.logger.error(f"Failed to process product {i}: {e}")
                    continue
            
            self.logger.info(f"Successfully processed {len(products)} products")
            return products
            
        except Exception as e:
            self.logger.error(f"Failed to scrape Aldi products: {e}")
            return products

    def _fetch_all_products(self) -> List[Dict[str, Any]]:
        """
        Fetch all available Aldi products using Algolia's browse method with cursor pagination
        This gets all products by following cursor-based pagination
        """
        all_products = []
        cursor = None
        page_count = 0
        
        while True:
            self.logger.info(f"Fetching browse page {page_count} (products so far: {len(all_products)})...")
            
            params = {
                'hitsPerPage': 1000,  # Max hits per page for browse
                'filters': 'isAvailable:true'
            }
            
            # Add cursor if we have one (for pagination)
            if cursor:
                params['cursor'] = cursor
                
            try:
                response = requests.get(self.API_URL, headers=self.api_headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    hits = data.get('hits', [])
                    cursor = data.get('cursor')  # Get cursor for next page
                    
                    self.logger.info(f"  Page {page_count}: Found {len(hits)} products")
                    
                    if hits:
                        all_products.extend(hits)
                        page_count += 1
                        
                        # If no cursor, we've reached the end
                        if not cursor:
                            self.logger.info("  No more cursor - reached end of results")
                            break
                    else:
                        self.logger.info("  No hits found - reached end")
                        break
                        
                else:
                    self.logger.error(f"Error: HTTP {response.status_code}")
                    self.logger.error(f"Response: {response.text}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error in browse method: {e}")
                break
        
        return all_products

    def _process_api_product(self, api_product: Dict[str, Any]) -> Optional[Product]:
        """Process a single product from Aldi API response"""
        try:
            # Extract basic info using the actual Aldi API structure
            product_id = str(api_product.get('objectID', ''))
            variant_name = api_product.get('variantName') or ''
            brand_name = api_product.get('brandName') or ''
            
            # Clean up names
            if variant_name:
                name = variant_name.strip()
            else:
                name = ''
            
            if brand_name:
                brand_name = brand_name.strip()
            
            if not name:
                self.logger.debug(f"Skipping product with missing name: {api_product.get('objectID', 'unknown')}")
                return None
            
            if not product_id:
                self.logger.debug(f"Skipping product with missing ID: {name}")
                return None
            
            # Price information from currentPrice structure
            current_price_info = api_product.get('currentPrice', {})
            price_value = current_price_info.get('priceValue')
            
            if price_value is None:
                self.logger.debug(f"Skipping product {name} - no price information")
                return None
            
            try:
                current_price = float(price_value)
            except (ValueError, TypeError):
                self.logger.debug(f"Skipping product {name} - invalid price: {price_value}")
                return None
            
            # Check for original price (strike price indicates discount)
            original_price = None
            discount_type = None
            strike_price_value = current_price_info.get('strikePriceValue')
            reduction = current_price_info.get('reduction')
            
            if strike_price_value:
                try:
                    strike_price_float = float(strike_price_value)
                    if strike_price_float > current_price:
                        original_price = strike_price_float
                        if reduction:
                            discount_type = f"{reduction}% korting"
                        else:
                            discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
                            discount_type = f"{discount_percentage}% korting"
                except (ValueError, TypeError):
                    # Invalid strike price, ignore discount
                    pass
            
            # Check for permanent low price
            permanent_low_price = api_product.get('permanentLowPrice', False)
            if permanent_low_price and not discount_type:
                discount_type = "permanent low price"
            
            # Unit size from salesUnit and shortDescription
            unit_amount = api_product.get('salesUnit')
            if not unit_amount:
                unit_amount = api_product.get('shortDescription')
            
            # Handle case where unit_amount might be a list or None
            if isinstance(unit_amount, list):
                unit_amount = ' '.join(str(item) for item in unit_amount) if unit_amount else ''
            elif unit_amount is None:
                unit_amount = ''
            else:
                unit_amount = str(unit_amount)
            
            unit_amount = unit_amount.strip()
            
            # Clean up unit amount for better parsing
            if unit_amount:
                # Handle common Dutch unit formats
                unit_amount = unit_amount.replace('st.', 'pieces')
                unit_amount = unit_amount.replace('stuks', 'pieces')
                unit_amount = unit_amount.replace('stuk', 'piece')
                unit_amount = re.sub(r'\s+', ' ', unit_amount)  # Normalize whitespace
                
                # If unit looks like a description rather than a unit, default to piece
                if not re.search(r'\d+\s*[a-zA-Z]', unit_amount) and not re.search(r'x\s*\d+', unit_amount):
                    unit_amount = '1 piece'
            else:
                unit_amount = '1 piece'
            
            # Category information from hierarchicalCategories
            category = "Unknown"
            hierarchical_categories = api_product.get('hierarchicalCategories', {})
            if hierarchical_categories:
                # Use the most specific category (lvl1 if available, otherwise lvl0)
                lvl1_categories = hierarchical_categories.get('lvl1')
                lvl0_categories = hierarchical_categories.get('lvl0')
                
                if lvl1_categories and isinstance(lvl1_categories, list) and len(lvl1_categories) > 0:
                    # Take the first lvl1 category and extract the part after '>'
                    category_path = lvl1_categories[0]
                    if ' > ' in category_path:
                        category = category_path.split(' > ')[-1]  # Get the last part
                    else:
                        category = category_path
                elif lvl0_categories and isinstance(lvl0_categories, list) and len(lvl0_categories) > 0:
                    category = lvl0_categories[0]
            
            # Alternative category from categories array (including 'offer')
            if category == "Unknown":
                categories = api_product.get('categories', [])
                if categories and isinstance(categories, list) and len(categories) > 0:
                    # Use first category, even if it's 'offer'
                    category = categories[0]
            
            # Clean up and validate category
            if category and isinstance(category, str):
                category = category.strip()
                # Capitalize 'offer' to 'Offers' for consistency
                if category.lower() == 'offer':
                    category = 'Offers'
                # Default to Unknown if category is empty
                if not category:
                    category = 'Unknown'
            
            # Brand (clean up brand name)
            brand = brand_name if brand_name else None
            
            # Additional product information for debugging
            is_available = api_product.get('isAvailable', True)
            permanent_low_price_flag = api_product.get('permanentLowPrice', False)
            
            # Log some key information for debugging
            self.logger.debug(f"Processing product: {name} - €{current_price} - {unit_amount}")
            if brand:
                self.logger.debug(f"  Brand: {brand}")
            if discount_type:
                self.logger.debug(f"  Discount type: {discount_type}")
            if permanent_low_price_flag:
                self.logger.debug(f"  Permanent low price: True")
            
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
            
            # Extract image URL from API response
            image_url = ''
            images = api_product.get('images', [])
            if images and isinstance(images, list):
                # Find primary image or use first image
                primary_image = next((img for img in images if img.get('type') == 'primary'), None)
                if primary_image and isinstance(primary_image, dict):
                    image_url = primary_image.get('url', '')
                elif images and isinstance(images[0], dict):
                    image_url = images[0].get('url', '')
            
            return self._create_product(
                product_id=product_id,
                name=name,
                category=category,
                price=current_price,
                unit_amount=unit_amount,
                original_price=original_price,
                discount_type=discount_type,
                brand=brand,
                discount_start_date=None,  # Aldi doesn't provide date fields in API
                discount_end_date=None,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process API product {api_product.get('objectID', 'unknown')}: {e}")
            self.logger.debug(f"Problem product data: {api_product}")
            return None


# Test function
def test_aldi_scraper():
    """Test function for development"""
    from database import DatabaseManager, get_db_config
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    config = get_db_config()
    
    with DatabaseManager(config) as db:
        scraper = AldiScraper(db)
        
        # Test fetching a small number of products
        products = scraper.scrape_products()
        print(f"✓ Found {len(products)} products")
        
        if products:
            # Show first few products
            for i, product in enumerate(products[:5]):
                print(f"\n{i+1}. {product.name}")
                print(f"   Price: €{product.price}")
                print(f"   Unit: {product.unit_amount}")
                print(f"   Price per unit: €{product.price_per_unit}/{product.unit_type.value}")
                print(f"   Category: {product.category_name}")
                if product.brand:
                    print(f"   Brand: {product.brand}")
                if product.original_price:
                    print(f"   Original price: €{product.original_price}")
                    print(f"   Discount: {product.discount_type}")
                print(f"   Search Tags: {product.search_tags}")


if __name__ == "__main__":
    # Add parent directory to path for imports
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    test_aldi_scraper()
