"""
ALDI Offer Scraper
Scrapes offers from https://www.aldi.nl/aanbiedingen.html using Next.js data
"""
import requests
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from database import Product


class AldiOfferScraper(BaseScraper):
    """Scraper for Aldi.nl offers page using Next.js embedded data"""

    BASE_URL = "https://www.aldi.nl"
    OFFERS_URL = "https://www.aldi.nl/aanbiedingen.html"

    def __init__(self, db_manager):
        super().__init__(db_manager, "ALDI")

    def scrape_products(self) -> List[Product]:
        """Main method to scrape offer products from ALDI"""
        try:
            offers_page_data = self._fetch_offers_page_data()
            if not offers_page_data:
                self.logger.warning("Failed to fetch offers page data")
                return []

            algolia_products = self._extract_algolia_products(offers_page_data)
            if not algolia_products:
                self.logger.warning("No products found in offers data")
                return []

            offer_products = self._filter_offer_products(algolia_products)
            products = []

            for product_data in offer_products:
                product = self._create_product_from_algolia_data(product_data)
                if product:
                    products.append(product)

                    # Respect product limit
                    if self.product_limit and len(products) >= self.product_limit:
                        break

            self.logger.info(f"Successfully scraped {len(products)} offer products from ALDI")
            return products

        except Exception as e:
            self.logger.error(f"Failed to scrape ALDI offers: {e}")
            return []

    def _fetch_offers_page_data(self) -> Optional[List[Any]]:
        """Fetch and parse the offers page to extract Next.js data"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(self.OFFERS_URL, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            next_data_script = soup.find('script', id='__NEXT_DATA__', type='application/json')

            if not next_data_script:
                self.logger.error("Could not find __NEXT_DATA__ script")
                return None

            # Get the script content
            script_content = next_data_script.get_text()
            if not script_content:
                self.logger.error("Could not get script content")
                return None

            # Parse the Next.js data
            next_data = json.loads(script_content)

            # Extract apiData
            api_data_str = next_data.get('props', {}).get('pageProps', {}).get('apiData')
            if not api_data_str:
                self.logger.error("Could not find apiData in Next.js data")
                return None

            # Parse the apiData JSON string
            api_data = json.loads(api_data_str)

            return api_data

        except Exception as e:
            self.logger.error(f"Failed to fetch offers page data: {e}")
            return None

    def _extract_algolia_products(self, api_data: List[Any]) -> Dict[str, Any]:
        """Extract products from the algoliaDataMap"""
        try:
            # Navigate to the algoliaDataMap
            # api_data is a list with format: [['OFFER_GET', {...}]]
            if isinstance(api_data, list) and len(api_data) > 0:
                first_item = api_data[0]
                if isinstance(first_item, list) and len(first_item) > 1:
                    offer_get_data = first_item[1]
                    algolia_data_map = offer_get_data.get('res', {}).get('algoliaDataMap', {})
                else:
                    algolia_data_map = {}
            else:
                algolia_data_map = {}

            self.logger.info(f"Found {len(algolia_data_map)} products in algoliaDataMap")
            return algolia_data_map

        except Exception as e:
            self.logger.error(f"Failed to extract algolia products: {e}")
            return {}

    def _filter_offer_products(self, algolia_products: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter products to only include those that are on offer"""
        offer_products = []

        for product_id, product_data in algolia_products.items():
            if self._is_product_on_offer(product_data):
                offer_products.append(product_data)

        self.logger.info(f"Filtered {len(offer_products)} offer products from {len(algolia_products)} total products")
        return offer_products

    def _is_product_on_offer(self, product_data: Dict[str, Any]) -> bool:
        """Check if a product is currently on offer"""
        # Check if product has 'offer' in categories
        categories = product_data.get('categories', [])
        if 'offer' in categories:
            return True

        # Check for permanent low price
        if product_data.get('permanentLowPrice', False):
            return True

        # Check for discount indicators in current price
        current_price = product_data.get('currentPrice', {})
        if current_price.get('strikePriceValue') is not None:
            return True

        return False

    def _create_product_from_algolia_data(self, product_data: Dict[str, Any]) -> Optional[Product]:
        """Create a Product object from ALDI's algolia product data"""
        try:
            # Extract basic product information
            product_id = str(product_data.get('objectID', ''))
            name = product_data.get('variantName', '').strip()

            if not product_id or not name:
                return None

            # Extract pricing information
            current_price, original_price, discount_type = self._extract_pricing_info(product_data)

            if not current_price or current_price <= 0:
                return None

            # Extract discount dates from promotion data
            discount_start_date, discount_end_date = self._extract_discount_dates(product_data)

            # Extract category information
            category = self._extract_category_from_algolia_data(product_data)

            # Extract unit information
            unit_amount = product_data.get('salesUnit', '1 stuk')

            # Extract brand information
            brand = product_data.get('brandName', '')

            # Extract image URL
            image_url = ''
            images = product_data.get('images', [])
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
                discount_start_date=discount_start_date,
                discount_end_date=discount_end_date,
                brand=brand,
                image_url=image_url
            )

        except Exception as e:
            self.logger.error(f"Failed to create product from algolia data: {e}")
            return None

    def _extract_pricing_info(self, product_data: Dict[str, Any]) -> tuple:
        """Extract current price, original price, and discount type from product data"""
        current_price = None
        original_price = None
        discount_type = None

        current_price_data = product_data.get('currentPrice', {})

        # Extract current price
        if 'priceValue' in current_price_data and current_price_data['priceValue'] is not None:
            try:
                current_price = float(current_price_data['priceValue'])
            except (ValueError, TypeError):
                current_price = None

        # Extract original price (strike price)
        if 'strikePriceValue' in current_price_data and current_price_data['strikePriceValue'] is not None:
            try:
                original_price = float(current_price_data['strikePriceValue'])
            except (ValueError, TypeError):
                original_price = None

        # Determine discount type
        if original_price and current_price and original_price > current_price:
            # Calculate discount percentage
            discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
            discount_type = f"{discount_percentage}% korting"
        elif current_price_data.get('reduction') and current_price_data['reduction'] != 'OP=OP':
            # Use the reduction text if available
            discount_type = current_price_data['reduction']
        elif product_data.get('permanentLowPrice', False):
            discount_type = "permanent low price"

        return current_price, original_price, discount_type

    def _extract_discount_dates(self, product_data: Dict[str, Any]) -> tuple:
        """Extract discount start and end dates from promotion data"""
        discount_start_date = None
        discount_end_date = None

        promotion_data = product_data.get('promotion', {})

        # Extract validFrom date
        if 'validFrom' in promotion_data and promotion_data['validFrom']:
            try:
                # Convert milliseconds timestamp to datetime
                timestamp_ms = int(promotion_data['validFrom'])
                discount_start_date = datetime.fromtimestamp(timestamp_ms / 1000)
            except (ValueError, TypeError, OSError):
                discount_start_date = None

        # Extract validUntil date
        if 'validUntil' in promotion_data and promotion_data['validUntil']:
            try:
                # Convert milliseconds timestamp to datetime
                timestamp_ms = int(promotion_data['validUntil'])
                discount_end_date = datetime.fromtimestamp(timestamp_ms / 1000)
            except (ValueError, TypeError, OSError):
                discount_end_date = None

        return discount_start_date, discount_end_date

    def _extract_category_from_algolia_data(self, product_data: Dict[str, Any]) -> str:
        """Extract category information from hierarchical categories"""
        hierarchical_categories = product_data.get('hierarchicalCategories', {})

        # Try to get category from lvl1 first, then lvl0
        lvl1_categories = hierarchical_categories.get('lvl1', [])
        if lvl1_categories and isinstance(lvl1_categories, list) and len(lvl1_categories) > 0:
            category_path = lvl1_categories[0]
            if ' > ' in category_path:
                return category_path.split(' > ')[-1].strip()
            return category_path.strip()

        lvl0_categories = hierarchical_categories.get('lvl0', [])
        if lvl0_categories and isinstance(lvl0_categories, list) and len(lvl0_categories) > 0:
            return lvl0_categories[0].strip()

        # Alternative category from categories array (including 'offer')
        categories = product_data.get('categories', [])
        if categories and isinstance(categories, list) and len(categories) > 0:
            # Use first category, even if it's 'offer'
            category = categories[0]
            if category and isinstance(category, str):
                category = category.strip()
                # Capitalize 'offer' to 'Offers' for consistency
                if category.lower() == 'offer':
                    return 'Offers'
                return category

        return "Offers"  # Default to Offers for offer products