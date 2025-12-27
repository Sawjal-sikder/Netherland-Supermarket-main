"""
Dekamarkt Offers Scraper
Extracts offers from Dekamarkt offers page using Nuxt.js data extraction
"""
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from bs4 import BeautifulSoup

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base_scraper import BaseScraper
from database import Product
from . import NuxtDataExtractor, DateParser, UnitExtractor, PriceValidator, DiscountCalculator


class DekamarktOfferScraper(BaseScraper):
    """Scraper for Dekamarkt offers page using Nuxt.js data extraction"""
    
    BASE_URL = "https://www.dekamarkt.nl"
    OFFERS_URL = "https://www.dekamarkt.nl/aanbiedingen"
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "DEKA")
        self._setup_headers()
        
    def _setup_headers(self):
        """Configure headers for Dekamarkt requests"""
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
    
    def scrape_products(self) -> List[Product]:
        """Main scraping method"""
        products: List[Product] = []
        
        self.logger.info(f"Starting Dekamarkt offers scraping from {self.OFFERS_URL}")
        
        try:
            # Get offers page
            response = self.session.get(self.OFFERS_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract Nuxt data
            nuxt_data = NuxtDataExtractor.extract_nuxt_data(soup)
            if not nuxt_data:
                self.logger.warning("No __NUXT_DATA__ found")
                return products
            
            self.logger.info("Successfully extracted Nuxt data")
            
            # Parse offers
            offers = self._parse_offers_from_nuxt(nuxt_data)
            self.logger.info(f"Found {len(offers)} offers")
            
            # Convert to products
            products = self._convert_offers_to_products(offers)
            
            # Apply product limit
            if self.product_limit is not None and len(products) > self.product_limit:
                products = products[:self.product_limit]
                self.logger.info(f"Applied limit: {len(products)} products")
            
            self.logger.info(f"Extracted {len(products)} offer products")
            
        except Exception as e:
            self.logger.error(f"Failed to scrape offers: {e}")
        
        return products
    
    def _parse_offers_from_nuxt(self, data: List[Any]) -> List[Dict[str, Any]]:
        """Parse offers from Nuxt data structure"""
        if not isinstance(data, list) or len(data) < 2:
            return []
        
        offers = []
        
        # Look for offer structures
        for i, item in enumerate(data):
            if isinstance(item, dict):
                if self._is_offer_item(item):
                    try:
                        offer = self._extract_offer(data, item)
                        if offer:
                            offers.append(offer)
                    except Exception as e:
                        self.logger.warning(f"Error parsing offer at index {i}: {e}")
        
        return offers
    
    def _is_offer_item(self, item: Dict[str, Any]) -> bool:
        """Check if item looks like an offer object"""
        # Look for typical offer fields in Dekamarkt structure
        offer_indicators = ['offerId', 'offerPrice', 'normalPrice', 'headerText', 'image', 'startDate', 'endDate']
        return any(key in item for key in offer_indicators)
    
    def _extract_offer(self, data: List[Any], item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract offer data from item"""
        offer = {}
        
        # Basic offer info
        offer['offer_id'] = NuxtDataExtractor.resolve_reference(data, item.get('offerId', ''))
        offer['header_text'] = NuxtDataExtractor.resolve_reference(data, item.get('headerText', ''))
        offer['image'] = NuxtDataExtractor.resolve_reference(data, item.get('image', ''))
        offer['offer_price'] = NuxtDataExtractor.resolve_reference(data, item.get('offerPrice', ''))
        offer['normal_price'] = NuxtDataExtractor.resolve_reference(data, item.get('normalPrice', ''))
        offer['discount_text'] = NuxtDataExtractor.resolve_reference(data, item.get('textPriceSign', ''))
        
        # Date extraction
        start_date = NuxtDataExtractor.resolve_reference(data, item.get('startDate', ''))
        end_date = NuxtDataExtractor.resolve_reference(data, item.get('endDate', ''))
        
        offer['start_date'] = DateParser.parse_date_string(start_date) if start_date else None
        offer['end_date'] = DateParser.parse_date_string(end_date) if end_date else None
        
        # Extract products if available
        products = []
        if 'products' in item:
            product_refs = NuxtDataExtractor.resolve_reference(data, item['products'])
            if isinstance(product_refs, list):
                for prod_ref in product_refs:
                    product_data = NuxtDataExtractor.resolve_reference(data, prod_ref)
                    if isinstance(product_data, dict):
                        product = self._extract_product_from_nuxt(data, product_data)
                        if product:
                            products.append(product)
        
        offer['products'] = products
        return offer
    
    def _extract_product_from_nuxt(self, data: List[Any], product_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract product data from Nuxt structure"""
        product = {}
        
        # Direct product fields
        product['product_id'] = NuxtDataExtractor.resolve_reference(data, product_data.get('productId', ''))
        product['offer_price'] = NuxtDataExtractor.resolve_reference(data, product_data.get('offerPrice', ''))
        product['normal_price'] = NuxtDataExtractor.resolve_reference(data, product_data.get('normalPrice', ''))
        
        # Product information from nested structure
        if 'productInformation' in product_data:
            info_ref = NuxtDataExtractor.resolve_reference(data, product_data['productInformation'])
            if isinstance(info_ref, dict):
                product['name'] = NuxtDataExtractor.resolve_reference(data, info_ref.get('headerText', ''))
                product['packaging'] = NuxtDataExtractor.resolve_reference(data, info_ref.get('packaging', ''))
                product['brand'] = NuxtDataExtractor.resolve_reference(data, info_ref.get('brand', ''))
                product['department'] = NuxtDataExtractor.resolve_reference(data, info_ref.get('department', ''))
        
        return product
    
    def _convert_offers_to_products(self, offers: List[Dict[str, Any]]) -> List[Product]:
        """Convert offers to Product objects"""
        products = []
        
        for offer in offers:
            # If offer has specific products, use them
            if offer.get('products'):
                for product_data in offer['products']:
                    product = self._create_product_from_data(offer, product_data)
                    if product:
                        products.append(product)
            else:
                # Use offer itself as product
                product = self._create_product_from_data(offer, {})
                if product:
                    products.append(product)
        
        return products
    
    def _create_product_from_data(self, offer: Dict[str, Any], product_data: Dict[str, Any]) -> Optional[Product]:
        """Create Product object from offer and product data"""
        try:
            # Get product details
            product_id = product_data.get('product_id') or offer.get('offer_id', '')
            name = product_data.get('name') or offer.get('header_text', '')
            packaging = product_data.get('packaging', '')
            
            if not all([product_id, name]):
                return None
            
            # Price validation
            current_price_str = product_data.get('offer_price') or offer.get('offer_price', '')
            original_price_str = product_data.get('normal_price') or offer.get('normal_price', '')
            
            current_price = PriceValidator.validate_price(current_price_str, product_id)
            if not current_price:
                return None
            
            original_price = PriceValidator.validate_price(original_price_str)
            
            # Calculate discount
            discount_type = DiscountCalculator.calculate_discount(current_price, original_price)
            
            # Extract unit amount
            unit_text = f"{name} {packaging}"
            unit_amount = UnitExtractor.extract_unit_amount(unit_text)
            
            # Determine category
            category = product_data.get('department') or offer.get('department') or "Offers"
            if category == "Aanbieding":
                category = "Offers"
            brand = product_data.get('brand', '')
            
            # Get image URL
            image_url = product_data.get('image') or offer.get('image', '')
            if image_url and not image_url.startswith('http'):
                image_url = self.BASE_URL + image_url if image_url.startswith('/') else ''
            
            # Get dates
            start_date = offer.get('start_date')
            end_date = offer.get('end_date')
            
            return self._create_product(
                product_id=str(product_id),
                name=name,
                category=category,
                price=current_price,
                unit_amount=unit_amount,
                original_price=original_price,
                discount_type=discount_type,
                brand=brand,
                discount_start_date=start_date,
                discount_end_date=end_date,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Failed to create product: {e}")
            return None
