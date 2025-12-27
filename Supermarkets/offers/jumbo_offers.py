"""
Jumbo Offer Scraper
Scrapes offers from https://www.jumbo.com/aanbiedingen/nu using promotions API
"""
import requests
import json
from typing import List, Optional, Dict, Any
from datetime import datetime

from base_scraper import BaseScraper
from database import Product


class JumboOfferScraper(BaseScraper):
    """Scraper for Jumbo offers using promotions GraphQL API"""

    BASE_URL = "https://www.jumbo.com"
    OFFERS_URL = "https://www.jumbo.com/aanbiedingen/nu"
    GRAPHQL_URL = "https://www.jumbo.com/api/graphql"
    
    # GraphQL operation constants
    PROMOTIONS_QUERY = '''
    {
        promotions {
            id
            title
            subtitle
            active
            group
            start {
                iso
            }
            end {
                iso
            }
            url
            tags {
                text
            }
        }
    }
    '''
    
    PROMOTION_DETAILS_QUERY = '''
    query promotion($id: String!, $referenceDate: String!) {
        promotion(id: $id) {
            id
            title
            subtitle
            active
            group
            start {
                iso
            }
            end {
                iso
            }
            tags {
                text
            }
            products {
                id: sku
                brand
                category
                subtitle: packSizeDisplay
                title
                image
                availability {
                    availability
                    isAvailable
                    label
                }
                link
                prices: price(referenceDate: $referenceDate) {
                    price
                    promoPrice
                    pricePerUnit {
                        price
                        unit
                    }
                }
                promotions {
                    group
                    id
                    title
                    tags {
                        text
                    }
                }
            }
        }
    }
    '''
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "JUMBO")
        self.headers = self._create_headers()
        self.cookies = self._create_cookies()

    def _create_headers(self) -> Dict[str, str]:
        """Create standardized headers for API requests"""
        return {
            'accept': '*/*',
            'accept-language': 'nl-NL,nl;q=0.9',
            'apollographql-client-name': 'JUMBO_WEB-promotion',
            'apollographql-client-version': 'master-v17.14.0-web',
            'content-type': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'x-source': 'JUMBO_WEB-promotion'
        }

    def _create_cookies(self) -> Dict[str, str]:
        """Create standardized cookies for API requests"""
        return {
            'STORE_LOCATION': '101',  # Default store
            'language': 'nl'
        }

    def scrape_products(self) -> List[Product]:
        """Main method to scrape offer products from Jumbo"""
        try:
            active_promotions = self._get_active_promotions()
            if not active_promotions:
                self.logger.warning("No active promotions found")
                return []

            self.logger.info(f"Found {len(active_promotions)} active promotions")
            
            products = self._extract_products_from_promotions(active_promotions)
            
            self.logger.info(f"Successfully scraped {len(products)} offer products from Jumbo")
            return products
            
        except Exception as e:
            self.logger.error(f"Failed to scrape Jumbo offers: {e}")
            return []

    def _get_active_promotions(self) -> List[Dict[str, Any]]:
        """Fetch main offer promotions from Jumbo API (weekly offers only)"""
        try:
            response = self._make_api_request(self.PROMOTIONS_QUERY)
            if not response:
                return []
                
            promotions = response.get('data', {}).get('promotions', [])
            
            # Filter for active weekly promotions (main offers page)
            main_offer_promotions = [
                p for p in promotions 
                if p.get('active', False) and p.get('group') == 'Week'
            ]
            
            self.logger.info(f"Filtered {len(main_offer_promotions)} weekly promotions from {len(promotions)} total")
            
            
            return main_offer_promotions
            
        except Exception as e:
            self.logger.error(f"Failed to fetch active promotions: {e}")
            return []

    def _extract_products_from_promotions(self, promotions: List[Dict[str, Any]]) -> List[Product]:
        """Extract products from all promotions"""
        products = []
        
        for promotion in promotions:
            if self._should_stop_processing(products):
                break
                
            promotion_products = self._get_promotion_products(promotion)
            
            for product_data in promotion_products:
                if self._should_stop_processing(products):
                    break
                    
                product = self._create_product_from_data(product_data, promotion)
                if product:
                    products.append(product)
        
        return products

    def _should_stop_processing(self, current_products: List[Product]) -> bool:
        """Check if we should stop processing based on product limit"""
        return bool(self.product_limit and len(current_products) >= self.product_limit)

    def _get_promotion_products(self, promotion: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch products for a specific promotion"""
        try:
            promotion_id = promotion.get('id')
            if not promotion_id:
                return []
                
            reference_date = self._get_current_reference_date()
            variables = {
                'id': promotion_id,
                'referenceDate': reference_date
            }
            
            response = self._make_api_request(self.PROMOTION_DETAILS_QUERY, variables)
            if not response:
                return []
                
            promotion_data = response.get('data', {}).get('promotion', {})
            products = promotion_data.get('products', [])
            
            self.logger.debug(f"Fetched {len(products)} products for promotion {promotion_id}")
            return products
            
        except Exception as e:
            self.logger.error(f"Failed to fetch products for promotion {promotion.get('id', 'unknown')}: {e}")
            return []

    def _make_api_request(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Make a GraphQL API request with error handling"""
        try:
            payload = {
                'query': query,
                'variables': variables or {}
            }
            
            if variables:
                payload['operationName'] = 'promotion'
            
            response = requests.post(
                self.GRAPHQL_URL,
                headers=self.headers,
                cookies=self.cookies,
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                self.logger.error(f"API returned status {response.status_code}")
                return None
                
            data = response.json()
            
            if 'errors' in data:
                self.logger.warning(f"GraphQL errors: {data['errors']}")
                return None
                
            return data
            
        except Exception as e:
            self.logger.error(f"API request failed: {e}")
            return None

    def _get_current_reference_date(self) -> str:
        """Get current datetime in the required ISO format"""
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def _create_product_from_data(self, product_data: Dict[str, Any], promotion: Optional[Dict[str, Any]] = None) -> Optional[Product]:
        """Create a Product object from Jumbo API data"""
        try:
            # Extract basic product information
            product_id = product_data.get('id')
            name = product_data.get('title')
            
            if not self._is_valid_product_data(product_id, name):
                return None
            
            # Extract and convert pricing (Jumbo stores prices in cents)
            pricing_info = self._extract_pricing_information(product_data)
            if not pricing_info['current_price']:
                return None
            
            # Extract promotion dates and discount information
            promotion_info = self._extract_promotion_information(product_data, promotion)
            
            # Extract other product details
            category = product_data.get('category', 'Offers')
            if not category:
                category = 'Offers'
            unit_amount = product_data.get('subtitle', '1 stuk') or '1 stuk'
            brand = product_data.get('brand', '')
            
            # Extract image URL
            image_url = product_data.get('image', '')
            
            return self._create_product(
                product_id=str(product_id),
                name=str(name),
                category=category,
                price=pricing_info['current_price'],
                unit_amount=unit_amount,
                original_price=pricing_info['original_price'],
                discount_type=promotion_info['discount_type'],
                discount_start_date=promotion_info['start_date'],
                discount_end_date=promotion_info['end_date'],
                brand=brand,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Failed to create product from data: {e}")
            return None

    def _is_valid_product_data(self, product_id: Any, name: Any) -> bool:
        """Validate that product has required basic information"""
        return bool(product_id and name)

    def _extract_pricing_information(self, product_data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Extract and convert pricing information from product data"""
        prices = product_data.get('prices', {})
        
        # Jumbo stores prices in cents, so we need to divide by 100
        current_price = self._convert_price_from_cents(prices.get('promoPrice') or prices.get('price'))
        original_price = None
        
        # If there's a promo price, the regular price becomes the original price
        if prices.get('promoPrice') and prices.get('price'):
            original_price = self._convert_price_from_cents(prices.get('price'))
        
        return {
            'current_price': current_price,
            'original_price': original_price
        }

    def _convert_price_from_cents(self, price_in_cents: Any) -> Optional[float]:
        """Convert price from cents to euros"""
        if not price_in_cents:
            return None
        
        try:
            return float(price_in_cents) / 100.0
        except (ValueError, TypeError):
            return None

    def _extract_promotion_information(self, product_data: Dict[str, Any], promotion: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract promotion dates and discount type"""
        promotion_info: Dict[str, Any] = {
            'discount_type': None,
            'start_date': None,
            'end_date': None
        }
        
        # Calculate discount percentage if we have both prices
        pricing = self._extract_pricing_information(product_data)
        if pricing['original_price'] and pricing['current_price']:
            discount_percentage = self._calculate_discount_percentage(
                pricing['original_price'], 
                pricing['current_price']
            )
            promotion_info['discount_type'] = f"{discount_percentage}% korting"
        
        # Extract dates from promotion data
        if promotion:
            promotion_dates = self._extract_promotion_dates(promotion)
            promotion_info.update(promotion_dates)
            
            # Use promotion tags if no discount type yet
            if not promotion_info['discount_type']:
                promotion_info['discount_type'] = self._extract_promotion_tags(promotion)
        
        # Fallback to product-level promotion tags
        if not promotion_info['discount_type']:
            promotion_info['discount_type'] = self._extract_product_promotion_tags(product_data)
        
        return promotion_info

    def _calculate_discount_percentage(self, original_price: float, current_price: float) -> float:
        """Calculate discount percentage"""
        if original_price > current_price:
            return round(((original_price - current_price) / original_price) * 100, 1)
        return 0.0

    def _extract_promotion_dates(self, promotion: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
        """Extract start and end dates from promotion data"""
        dates: Dict[str, Optional[datetime]] = {'start_date': None, 'end_date': None}
        
        start_data = promotion.get('start', {})
        if start_data and start_data.get('iso'):
            dates['start_date'] = self._parse_iso_datetime(start_data['iso'])
            
        end_data = promotion.get('end', {})
        if end_data and end_data.get('iso'):
            dates['end_date'] = self._parse_iso_datetime(end_data['iso'])
        
        return dates

    def _parse_iso_datetime(self, iso_string: str) -> Optional[datetime]:
        """Parse ISO datetime string"""
        try:
            return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None

    def _extract_promotion_tags(self, promotion: Dict[str, Any]) -> Optional[str]:
        """Extract promotion text from promotion tags"""
        tags = promotion.get('tags', [])
        if tags and isinstance(tags, list) and len(tags) > 0:
            return tags[0].get('text', '')
        return None

    def _extract_product_promotion_tags(self, product_data: Dict[str, Any]) -> Optional[str]:
        """Extract promotion text from product-level promotions"""
        promotions = product_data.get('promotions', [])
        if promotions:
            promo = promotions[0]
            tags = promo.get('tags', [])
            if tags:
                return tags[0].get('text', '')
        return None