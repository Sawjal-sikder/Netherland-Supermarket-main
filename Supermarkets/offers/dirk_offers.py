"""
Dirk Offer Scraper
Scrapes offers from https://www.dirk.nl/aanbiedingen using Nuxt.js data extraction
"""
import requests
from bs4 import BeautifulSoup
import logging
import json
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urljoin
from datetime import datetime
from collections import defaultdict

from base_scraper import BaseScraper
from database import Product, PriceCalculator
from . import NuxtDataExtractor, DateParser, UnitExtractor, PriceValidator, DiscountCalculator


class DirkOfferScraper(BaseScraper):
    """Scraper for Dirk.nl offer page - extracts offers from Nuxt.js JSON data"""

    BASE_URL = "https://www.dirk.nl"
    OFFERS_URL = "https://www.dirk.nl/aanbiedingen"
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "DIRK")
        self._setup_headers()
    
    def _setup_headers(self):
        """Configure headers for Dirk requests"""
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
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
        """Scrape products from offers page using Nuxt.js data extraction"""
        products: List[Product] = []
        
        self.logger.info(f"Starting offer scraping from {self.OFFERS_URL}")
        
        try:
            # Get offers page
            response = self.session.get(self.OFFERS_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract Nuxt data using shared utility
            nuxt_data = NuxtDataExtractor.extract_nuxt_data(soup)
            
            if not nuxt_data:
                self.logger.warning("No __NUXT_DATA__ found. Using fallback.")
                return self._fallback_scrape_products(soup)
            
            self.logger.info("Successfully extracted Nuxt data")
            
            # Parse offers
            offers = self._parse_offers_from_nuxt_data(nuxt_data)
            self.logger.info(f"Found {len(offers)} offers")
            
            # Convert to products
            products = self._flatten_offers_to_products(offers)
            
            # Apply limit
            if self.product_limit is not None and len(products) > self.product_limit:
                products = products[:self.product_limit]
                self.logger.info(f"Applied limit: {len(products)} products")
            
            self.logger.info(f"Extracted {len(products)} offer products")
            
        except Exception as e:
            self.logger.error(f"Failed to scrape offers: {e}")
        
        return products

    def _parse_offers_from_nuxt_data(self, json_data: List[Any]) -> List[Dict[str, Any]]:
        """
        Parse product offers from Nuxt.js hydration data structure.
        The data uses index references to minimize payload size.
        """
        if not isinstance(json_data, list) or len(json_data) < 2:
            return []
        
        data = json_data
        offers = []
        
        # Look for offer structures in the data
        for i, item in enumerate(data):
            if isinstance(item, dict):
                # Check if this looks like an offer object
                if 'offerId' in item and 'headerText' in item and 'offerPrice' in item:
                    try:
                        offer = self._extract_offer_data(data, item)
                        if offer:
                            offers.append(offer)
                    except Exception as e:
                        self.logger.warning(f"Error parsing offer at index {i}: {e}")
                        continue
        
        return offers

    def _extract_offer_data(self, data: List[Any], item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract offer data from a single offer item in the Nuxt data"""
        offer = {}
        
        # Extract basic offer information
        offer['offer_id'] = NuxtDataExtractor.resolve_reference(data, item.get('offerId'))
        offer['header_text'] = NuxtDataExtractor.resolve_reference(data, item.get('headerText'))
        offer['packaging'] = NuxtDataExtractor.resolve_reference(data, item.get('packaging'))
        offer['offer_price'] = NuxtDataExtractor.resolve_reference(data, item.get('offerPrice'))
        offer['normal_price'] = NuxtDataExtractor.resolve_reference(data, item.get('normalPrice'))
        offer['text_price_sign'] = NuxtDataExtractor.resolve_reference(data, item.get('textPriceSign'))
        offer['image'] = NuxtDataExtractor.resolve_reference(data, item.get('image'))
        
        # Extract dates
        if 'startDate' in item:
            offer['start_date'] = NuxtDataExtractor.resolve_reference(data, item['startDate'])
        if 'endDate' in item:
            offer['end_date'] = NuxtDataExtractor.resolve_reference(data, item['endDate'])
        if 'disclaimerStartDate' in item:
            offer['disclaimer_start_date'] = NuxtDataExtractor.resolve_reference(data, item['disclaimerStartDate'])
        if 'disclaimerEndDate' in item:
            offer['disclaimer_end_date'] = NuxtDataExtractor.resolve_reference(data, item['disclaimerEndDate'])
        
        # Extract products within this offer
        products = []
        if 'products' in item:
            product_refs = NuxtDataExtractor.resolve_reference(data, item['products'])
            if isinstance(product_refs, list):
                for prod_ref in product_refs:
                    product_data = NuxtDataExtractor.resolve_reference(data, prod_ref)
                    if isinstance(product_data, dict):
                        product = self._extract_product_from_nuxt_data(data, product_data)
                        if product:
                            products.append(product)
        
        offer['products'] = products
        return offer

    def _extract_product_from_nuxt_data(self, data: List[Any], product_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract product data from Nuxt data structure"""
        product = {}
        
        # Basic product info
        if 'productId' in product_data:
            product['product_id'] = NuxtDataExtractor.resolve_reference(data, product_data['productId'])
        if 'offerPrice' in product_data:
            product['offer_price'] = NuxtDataExtractor.resolve_reference(data, product_data['offerPrice'])
        if 'normalPrice' in product_data:
            product['normal_price'] = NuxtDataExtractor.resolve_reference(data, product_data['normalPrice'])
        
        # Get detailed product information
        if 'productInformation' in product_data:
            prod_info_ref = NuxtDataExtractor.resolve_reference(data, product_data['productInformation'])
            if isinstance(prod_info_ref, dict):
                if 'headerText' in prod_info_ref:
                    product['name'] = NuxtDataExtractor.resolve_reference(data, prod_info_ref['headerText'])
                if 'packaging' in prod_info_ref:
                    product['packaging'] = NuxtDataExtractor.resolve_reference(data, prod_info_ref['packaging'])
                if 'image' in prod_info_ref:
                    product['image'] = NuxtDataExtractor.resolve_reference(data, prod_info_ref['image'])
                if 'department' in prod_info_ref:
                    product['department'] = NuxtDataExtractor.resolve_reference(data, prod_info_ref['department'])
                if 'webgroup' in prod_info_ref:
                    product['webgroup'] = NuxtDataExtractor.resolve_reference(data, prod_info_ref['webgroup'])
                if 'brand' in prod_info_ref:
                    product['brand'] = NuxtDataExtractor.resolve_reference(data, prod_info_ref['brand'])
        
        # Get offer details
        if 'productOffer' in product_data:
            prod_offer_ref = NuxtDataExtractor.resolve_reference(data, product_data['productOffer'])
            if isinstance(prod_offer_ref, dict):
                if 'textPriceSign' in prod_offer_ref:
                    product['text_price_sign'] = NuxtDataExtractor.resolve_reference(data, prod_offer_ref['textPriceSign'])
        
        return product

    def _flatten_offers_to_products(self, offers: List[Dict[str, Any]]) -> List[Product]:
        """
        Flatten all products from all offers into a comprehensive list for database storage.
        """
        all_products = []
        
        for offer in offers:
            # Extract offer-level information
            offer_id = offer.get('offer_id', '')
            offer_header = offer.get('header_text', '')
            offer_packaging = offer.get('packaging', '')
            offer_price = offer.get('offer_price', '')
            offer_normal_price = offer.get('normal_price', '')
            offer_text_price_sign = offer.get('text_price_sign', '')
            offer_image = offer.get('image', '')
            offer_start_date = offer.get('start_date', '')
            offer_end_date = offer.get('end_date', '')
            disclaimer_start_date = offer.get('disclaimer_start_date', '')
            disclaimer_end_date = offer.get('disclaimer_end_date', '')
            
            # Parse dates using shared utility
            discount_start_date_obj = DateParser.parse_date_string(disclaimer_start_date)
            discount_end_date_obj = DateParser.parse_date_string(disclaimer_end_date)
            
            # If offer has specific products, extract them
            if offer.get('products'):
                for product in offer['products']:
                    product_obj = self._create_product_from_offer_data(
                        offer_id, offer_header, offer_packaging, product,
                        offer_price, offer_normal_price, offer_text_price_sign,
                        discount_start_date_obj, discount_end_date_obj
                    )
                    if product_obj:
                        all_products.append(product_obj)
            else:
                # If no specific products, treat the offer itself as a product
                product_obj = self._create_product_from_offer_data(
                    offer_id, offer_header, offer_packaging, {},
                    offer_price, offer_normal_price, offer_text_price_sign,
                    discount_start_date_obj, discount_end_date_obj
                )
                if product_obj:
                    all_products.append(product_obj)
        
        return all_products

    def _create_product_from_offer_data(self, offer_id: str, offer_header: str, offer_packaging: str,
                                      product: Dict[str, Any], offer_price: str, offer_normal_price: str,
                                      offer_text_price_sign: str, discount_start_date: Optional[datetime],
                                      discount_end_date: Optional[datetime]) -> Optional[Product]:
        """Create a Product object from offer data"""
        try:
            # Extract and validate product details
            product_details = self._extract_product_details(
                product, offer_id, offer_header, offer_packaging,
                offer_price, offer_normal_price
            )

            if not product_details:
                return None

            # Convert and validate prices
            price_info = self._convert_and_validate_prices(
                product_details['current_price'],
                product_details['normal_price'],
                product_details['product_id']
            )

            if not price_info:
                return None

            # Calculate discount information
            discount_info = self._calculate_discount_info(
                price_info['current_price'],
                price_info['original_price']
            )

            # Determine category
            category = self._determine_category(
                product_details['department']
            )

            # Build and return product object
            return self._build_product_object(
                product_details, price_info, discount_info, category,
                discount_start_date, discount_end_date
            )

        except Exception as e:
            self.logger.error(f"Failed to create product from offer data: {e}")
            return None

    def _extract_product_details(self, product: Dict[str, Any], offer_id: str,
                               offer_header: str, offer_packaging: str,
                               offer_price: str, offer_normal_price: str) -> Optional[Dict[str, Any]]:
        """Extract and validate basic product details"""
        product_id = product.get('product_id', offer_id)
        name = product.get('name', offer_header)
        packaging = product.get('packaging', offer_packaging)
        current_price = product.get('offer_price', offer_price)
        normal_price = product.get('normal_price', offer_normal_price)
        department = product.get('department', '')
        brand = product.get('brand', '')

        # Validate required fields
        if not all([name, current_price, product_id]):
            return None

        return {
            'product_id': product_id,
            'name': name,
            'packaging': packaging,
            'current_price': current_price,
            'normal_price': normal_price,
            'department': department,
            'brand': brand
        }

    def _convert_and_validate_prices(self, current_price_str: str,
                                   original_price_str: str, product_id: str) -> Optional[Dict[str, Optional[float]]]:
        """Convert price strings to floats using shared utility"""
        current_price = PriceValidator.validate_price(current_price_str, product_id)
        if not current_price:
            return None
        
        original_price = PriceValidator.validate_price(original_price_str)
        
        return {
            'current_price': current_price,
            'original_price': original_price
        }

    def _calculate_discount_info(self, current_price: float,
                               original_price: Optional[float]) -> Dict[str, Any]:
        """Calculate discount using shared utility"""
        discount_type = DiscountCalculator.calculate_discount(current_price, original_price)
        return {'discount_type': discount_type}

    def _determine_category(self, department: str) -> str:
        """Determine the product category"""
        return department if department else "Offers"

    def _build_product_object(self, product_details: Dict[str, Any],
                            price_info: Dict[str, Optional[float]], discount_info: Dict[str, Any],
                            category: str, discount_start_date: Optional[datetime],
                            discount_end_date: Optional[datetime]) -> Product:
        """Build the final Product object"""
        # Extract unit amount using shared utility
        unit_amount = UnitExtractor.extract_unit_amount(
            f"{product_details['name']} {product_details['packaging']}"
        )

        # Extract image URL
        image_url = product_details.get('image_url', '')

        return self._create_product(
            product_id=str(product_details['product_id']),
            name=product_details['name'],
            category=category,
            price=float(price_info['current_price']) if price_info['current_price'] is not None else 0.0,
            unit_amount=unit_amount,
            original_price=price_info['original_price'],
            discount_type=discount_info['discount_type'],
            brand=product_details['brand'],
            discount_start_date=discount_start_date,
            discount_end_date=discount_end_date,
            image_url=image_url
        )

    def _parse_date_string_to_datetime(self, date_string: str) -> Optional[datetime]:
        """Parse a date string into a datetime object with multiple format support"""
        if not date_string:
            return None

        # Clean the date string by removing timezone suffixes
        cleaned_date = date_string.split('+')[0].split('Z')[0]

        # Common date formats to try
        date_formats = [
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f'
        ]

        for date_format in date_formats:
            try:
                return datetime.strptime(cleaned_date, date_format)
            except ValueError:
                continue

        self.logger.warning(f"Unable to parse date string: {date_string}")
        return None

    def _fallback_scrape_products(self, soup: BeautifulSoup) -> List[Product]:
        """Fallback method using the original URL extraction approach"""
        self.logger.info("Using fallback scraping method")
        
        # Extract offer product URLs from the offers page
        offer_urls = self._extract_offer_urls(soup)
        self.logger.info(f"Found {len(offer_urls)} offer URLs")
        
        products = []
        
        if not offer_urls:
            return products
        
        # Respect optional product_limit if set
        urls_to_scrape = offer_urls
        if self.product_limit is not None:
            urls_to_scrape = offer_urls[:self.product_limit]
            self.logger.info(f"Applying product limit to URLs: {len(urls_to_scrape)}")
        
        # Simple sequential processing for fallback
        for i, url in enumerate(urls_to_scrape):
            try:
                response = self.session.get(url, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                product = self._parse_product_data(soup, url)
                if product:
                    products.append(product)
                
                if (i + 1) % 10 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(urls_to_scrape)} offer products")
                    
            except Exception as e:
                self.logger.error(f"Failed to scrape offer {url}: {e}")
        
        return products

    def _extract_offer_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract all offer product URLs from the offers page"""
        urls = []

        # Extract URLs from general links
        general_urls = self._extract_urls_from_general_links(soup)
        urls.extend(general_urls)

        # Extract URLs from offer containers
        container_urls = self._extract_urls_from_offer_containers(soup)
        urls.extend(container_urls)

        # Remove duplicates while preserving order
        return self._remove_duplicate_urls(urls)

    def _extract_urls_from_general_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract product URLs from all anchor tags"""
        urls = []
        offer_links = soup.find_all('a', href=True)

        for link in offer_links:
            url = self._extract_and_normalize_url(link.get('href'))
            if url and self._is_product_url(url):
                urls.append(url)

        return urls

    def _extract_urls_from_offer_containers(self, soup: BeautifulSoup) -> List[str]:
        """Extract product URLs from offer-specific containers"""
        urls = []
        offer_containers = soup.find_all(['div', 'article'],
                                       class_=re.compile(r'(offer|promo|aanbieding|product)', re.I))

        for container in offer_containers:
            container_links = container.find_all('a', href=True)
            for link in container_links:
                url = self._extract_and_normalize_url(link.get('href'))
                if url and self._is_product_url(url):
                    urls.append(url)

        return urls

    def _extract_and_normalize_url(self, href: Any) -> Optional[str]:
        """Extract and normalize a URL from href attribute"""
        if not href:
            return None

        # Convert to string if needed
        href_str = str(href) if href else ""

        # Convert relative URLs to absolute
        if href_str.startswith('/'):
            return urljoin(self.BASE_URL, href_str)

        return href_str

    def _is_product_url(self, url: str) -> bool:
        """Check if URL is a product page URL"""
        return '/producten/' in url and self.BASE_URL in url

    def _remove_duplicate_urls(self, urls: List[str]) -> List[str]:
        """Remove duplicate URLs while preserving order"""
        seen = set()
        unique_urls = []

        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        return unique_urls

    def _parse_product_data(self, soup: BeautifulSoup, url: str) -> Optional[Product]:
        """Parse product data from individual product page"""
        # This method is similar to the regular Dirk scraper but focuses on offers
        # Let's try JSON-LD first, then fallback to HTML parsing
        
        # Try to extract from JSON-LD structured data
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        if item.get('@type') == 'Product':
                            product = self._parse_from_json_ld(item, url, soup)
                            if product:
                                return product
                elif data.get('@type') == 'Product':
                    product = self._parse_from_json_ld(data, url, soup)
                    if product:
                        return product
            except (json.JSONDecodeError, KeyError) as e:
                continue
        
        # Fallback to HTML parsing
        return self._parse_from_html(soup, url)

    def _parse_from_json_ld(self, data: Dict[str, Any], url: str, soup: BeautifulSoup) -> Optional[Product]:
        """Parse product from JSON-LD structured data"""
        try:
            # Extract basic product information
            basic_info = self._extract_basic_product_info_from_json_ld(data, url)
            if not basic_info:
                return None

            # Extract pricing information
            pricing_info = self._extract_pricing_from_json_ld(data, soup)
            if not pricing_info:
                return None

            # Determine category
            category = self._extract_category_from_json_ld(data)

            # Extract unit amount
            unit_amount = self._extract_unit_amount_from_text(basic_info['name']) or "1 stuk"

            # Extract image URL from JSON-LD
            image_url = ''
            image_data = data.get('image', '')
            if image_data:
                if isinstance(image_data, str):
                    image_url = image_data
                elif isinstance(image_data, dict):
                    image_url = image_data.get('url', '')
                elif isinstance(image_data, list) and len(image_data) > 0:
                    first_image = image_data[0]
                    if isinstance(first_image, str):
                        image_url = first_image
                    elif isinstance(first_image, dict):
                        image_url = first_image.get('url', '')

            # Create product object
            return self._create_product(
                product_id=basic_info['product_id'],
                name=basic_info['name'],
                category=category,
                price=pricing_info['current_price'],
                unit_amount=unit_amount,
                original_price=pricing_info['original_price'],
                discount_type=pricing_info['discount_type'],
                image_url=image_url
            )

        except Exception as e:
            self.logger.error(f"Failed to parse JSON-LD for {url}: {e}")
            return None

    def _extract_basic_product_info_from_json_ld(self, data: Dict[str, Any], url: str) -> Optional[Dict[str, str]]:
        """Extract basic product information from JSON-LD data"""
        product_id = data.get('mpn', urlparse(url).path.split('/')[-1])
        name = data.get('name')

        if not name or not product_id:
            return None

        return {
            'product_id': str(product_id),
            'name': name
        }

    def _extract_pricing_from_json_ld(self, data: Dict[str, Any], soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract pricing information from JSON-LD data"""
        offer = data.get('offers', {})
        current_price = offer.get('Price') or offer.get('price')

        if not current_price:
            return None

        # Try to find original price from HTML
        original_price = self._find_original_price_in_html(soup)
        discount_type = None

        if original_price and original_price > current_price:
            discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
            discount_type = f"{discount_percentage}% korting"


        return {
            'current_price': float(current_price),
            'original_price': original_price,
            'discount_type': discount_type
        }

    def _find_original_price_in_html(self, soup: BeautifulSoup) -> Optional[float]:
        """Find original price from HTML elements"""
        regular_price_elem = soup.find('span', class_='regular-price')
        if not regular_price_elem:
            return None

        price_match = re.search(r'\d+\.\d+', regular_price_elem.text)
        if not price_match:
            return None

        try:
            return float(price_match.group())
        except ValueError:
            return None

    def _extract_category_from_json_ld(self, data: Dict[str, Any]) -> str:
        """Extract category from JSON-LD data"""
        category = data.get('category', 'Offers')
        if isinstance(category, list):
            return category[0] if category else 'Offers'
        return category if category else 'Offers'

    def _parse_from_html(self, soup: BeautifulSoup, url: str) -> Optional[Product]:
        """Parse product from HTML when JSON-LD is not available"""
        try:
            # Extract product name
            name = self._extract_product_name_from_html(soup)
            if not name:
                return None

            # Extract pricing information
            pricing_info = self._extract_pricing_from_html(soup)
            if not pricing_info:
                return None

            # Extract category
            category = self._extract_category_from_html(soup)

            # Extract unit amount
            unit_amount = self._extract_unit_amount_from_text(name) or "1 stuk"

            # Generate product ID from URL
            product_id = self._generate_product_id_from_url(url)

            # Extract image URL from HTML
            image_url = ''
            image_tag = soup.select_one('img.product-image, img[data-testid="product-image"], .product-image img')
            if image_tag:
                image_url = image_tag.get('src', '') or image_tag.get('data-src', '')

            return self._create_product(
                product_id=product_id,
                name=name,
                category=category,
                price=pricing_info['current_price'],
                unit_amount=unit_amount,
                original_price=pricing_info['original_price'],
                discount_type=pricing_info['discount_type'],
                image_url=image_url
            )

        except Exception as e:
            self.logger.error(f"Failed to parse HTML for {url}: {e}")
            return None

    def _extract_product_name_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product name from HTML using various selectors"""
        name_selectors = [
            'h1.product-title',
            'h1[data-testid="product-title"]',
            '.product-name h1',
            'h1'
        ]

        for selector in name_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)

        return None

    def _extract_pricing_from_html(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract pricing information from HTML"""
        # Extract current price
        current_price = self._find_price_by_selectors(soup, [
            '.price-current',
            '.product-price .price',
            '[data-testid="price-current"]',
            '.price'
        ])

        if not current_price:
            return None

        # Extract original price and calculate discount
        original_price = self._find_price_by_selectors(soup, [
            '.price-original',
            '.regular-price',
            '.old-price',
            '[data-testid="price-original"]'
        ])

        discount_type = None
        if original_price and original_price > current_price:
            discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
            discount_type = f"{discount_percentage}% korting"

        return {
            'current_price': current_price,
            'original_price': original_price,
            'discount_type': discount_type
        }

    def _find_price_by_selectors(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[float]:
        """Find price using a list of CSS selectors"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                price_text = element.get_text(strip=True)
                price_match = re.search(r'(\d+[.,]\d+)', price_text)
                if price_match:
                    try:
                        return float(price_match.group().replace(',', '.'))
                    except ValueError:
                        continue
        return None

    def _extract_category_from_html(self, soup: BeautifulSoup) -> str:
        """Extract category from HTML breadcrumb or category elements"""
        category_selectors = [
            '.breadcrumb a:last-child',
            '.product-category',
            '[data-testid="breadcrumb"] a:last-child'
        ]

        for selector in category_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)

        return 'Unknown'

    def _generate_product_id_from_url(self, url: str) -> str:
        """Generate product ID from URL path"""
        return urlparse(url).path.split('/')[-1]

    def _extract_unit_amount_from_text(self, text: str) -> str:
        """Extract unit amount from product name or description"""
        if not text:
            return "1 stuk"
        
        # Look for common unit patterns including "x" format (e.g., "24 x 300 ml")
        patterns = [
            r'(\d+\s*x\s*\d+(?:[.,]\d+)?\s*(?:cl|ml|l|g|kg))',  # "24 x 33cl", "6 x 330ml"
            r'(\d+(?:[.,]\d+)?\s*(?:kg|kilo|kilogram))',
            r'(\d+(?:[.,]\d+)?\s*(?:g|gram))',
            r'(\d+(?:[.,]\d+)?\s*(?:liter|l)\b)',  # Added word boundary for 'l'
            r'(\d+(?:[.,]\d+)?\s*(?:ml|milliliter))',
            r'(\d+(?:[.,]\d+)?\s*(?:cl|centiliter))',  # Added centiliter
            r'(\d+\s*(?:stuks?|st\.?|pieces?))',
            r'(\d+\s*(?:pack|pak))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                return match.group(1).replace(',', '.').strip()
        
        return "1 stuk"