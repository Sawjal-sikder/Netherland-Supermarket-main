"""
Hoogvliet Supermarket Scraper

This module implements a scraper for Hoogvliet supermarket using their two-step API approach:
1. Product Discovery: Query the Tweakwise navigation API to get product SKUs/IDs
2. Product Details: Use the SKUs to fetch detailed product information from Hoogvliet's internal API

API Endpoints:
- Product Discovery: https://navigator-group1.tweakwise.com/navigation/ed681b01
- Product Details: https://www.hoogvliet.com/INTERSHOP/web/WFS/org-webshop-Site/nl_NL/-/EUR/ProcessTWProducts-GetTWProductsBySkus
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base_scraper import BaseScraper
from database import DatabaseManager, Product, PriceCalculator, UnitType
from datetime import datetime
import time
import logging
import requests
import json
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode
import re


class HoogvlietScraper(BaseScraper):
    """Scraper for Hoogvliet supermarket using their two-step API approach"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, "hoogvliet")
        
        # API endpoints
        self.tweakwise_endpoint = "https://navigator-group1.tweakwise.com/navigation/ed681b01"
        self.hoogvliet_details_endpoint = "https://www.hoogvliet.com/INTERSHOP/web/WFS/org-webshop-Site/nl_NL/-/EUR/ProcessTWProducts-GetTWProductsBySkus"
        
        # Headers for Tweakwise API (Product Discovery)
        self.tweakwise_headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-length': '0',
            'origin': 'https://www.hoogvliet.com',
            'priority': 'u=1, i',
            'referer': 'https://www.hoogvliet.com/',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        }
        
        # Headers for Hoogvliet Details API
        self.hoogvliet_headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '0',
            'Origin': 'https://www.hoogvliet.com',
            'Referer': 'https://www.hoogvliet.com/INTERSHOP/web/WFS/org-webshop-Site/nl_NL/-/EUR/ViewTWParametricSearch-SimpleOfferSearch',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # Configuration
        self.page_size = 100  # higher page size for fewer requests
        self.batch_size = 16  # Products per batch for details (same as original curl)
        self.max_pages = 500  # Maximum pages to scrape (prevent infinite loops)
        self.request_delay = 1  # Delay between requests in seconds
        
        # Session management
        self.session_id = "5BFim558nWFUmvbvwWDNmIV2WDfoyXQM-GGI64IMana0wg=="  # Default from curl
        self.cookies = (
            'visid_incap_2265421=0s5tkGX/Qfi0uLQjBLmGbxwlk2gAAAAAPCk+5RwwzQgrEtugKbW13g=='
            ';sid=5BFim558nWFUmvbvwWDNmIV2WDfoyXQM-GGI64IMana0wg=='
            ';SecureSessionID-Qu0KAyhz_A4AAAE9ketGw_RR=2d10686dea1436e6d67f4d057fd0898244bb1c85bc0038ae4992f7323608886f'
            ';nlbi_2265421=wAlBQ/KH9hQScPnDFSnXtwAAAABxUlF3tTGVWUmOgorwpmVW'
            ';incap_ses_1688_2265421=5flgA0mNGFVpDURB7v1sF3Alk2gAAAAAEkGAL2c2GLbAktTtAvLUkQ=='
        )
    
    def scrape_products(self) -> List[Product]:
        """
        Main scraping method that implements the two-step process:
        1. Discover product SKUs and basic info using Tweakwise API
        2. Get detailed product information using Hoogvliet API and combine with Tweakwise data
        
        Returns:
            List of Product objects
        """
        self.logger.info("Starting Hoogvliet product scraping")
        
        # Step 1: Discover products with basic info
        all_products_basic = self._discover_all_product_skus()
        if not all_products_basic:
            self.logger.error("No products discovered")
            return []
        
        self.logger.info(f"Discovered {len(all_products_basic)} products with basic info")
        
        # Step 2: Get detailed product information and combine
        products = self._get_detailed_product_info_combined(all_products_basic)
        
        # Fallback: if details API fails, create products from Tweakwise data only
        if not products:
            self.logger.warning("Details API returned no products. Using Tweakwise data only.")
            products = self._create_products_from_tweakwise_only(all_products_basic)
        
        self.logger.info(f"Successfully scraped {len(products)} products from Hoogvliet")
        return products
    
    def _create_product_from_basic_info(self, basic_info: Dict[str, Any]) -> Optional[Product]:
        """
        Create a Product object from Tweakwise basic info only (fallback when details API fails)
        
        Args:
            basic_info: Basic product info from Tweakwise API
            
        Returns:
            Product object or None if creation fails
        """
        try:
            sku = (basic_info.get('sku') or '').strip()
            name = (basic_info.get('name') or '').strip()
            price = basic_info.get('price')
            brand = (basic_info.get('brand') or '').strip()
            
            if not sku or not name or price is None:
                return None
            
            try:
                price_float = float(price)
            except (ValueError, TypeError):
                return None
            
            if price_float <= 0:
                return None
            
            # Extract unit info from attributes if available
            attributes = basic_info.get('attributes', {})
            base_unit = attributes.get('BaseUnit', 'stuk').lower()
            ratio = attributes.get('RatioBasePackingUnit', '1')
            
            try:
                ratio_float = float(ratio)
            except (ValueError, TypeError):
                ratio_float = 1.0
            
            # Construct unit amount
            unit_amount = f"{int(ratio_float) if ratio_float == int(ratio_float) else ratio_float} {base_unit}".strip()
            
            # Prepend brand if available and not already present
            try:
                if brand:
                    if not name.lower().startswith(brand.lower()):
                        name = f"{brand} {name}"
            except Exception:
                pass

            # Extract image URL
            image_url = basic_info.get('image_url', '')
            
            # Create the product
            product = self._create_product(
                product_id=sku,
                name=name,
                category="Unknown",  # Category not available in basic info
                price=price_float,
                unit_amount=unit_amount,
                original_price=None,
                discount_type=None,
                brand=brand,
                discount_start_date=None,
                discount_end_date=None,
                image_url=image_url
            )
            
            return product
            
        except Exception as e:
            self.logger.error(f"Error creating product from basic info: {e}")
            return None
    def _create_products_from_tweakwise_only(self, products_basic: List[Dict[str, Any]]) -> List[Product]:
        """
        Create Product objects from Tweakwise basic info only (complete fallback)
        
        Args:
            products_basic: List of basic product info from Tweakwise
            
        Returns:
            List of Product objects
        """
        products = []
        for basic_info in products_basic:
            product = self._create_product_from_basic_info(basic_info)
            if product:
                products.append(product)
        return products
    
    def _discover_all_product_skus(self) -> List[Dict[str, Any]]:
        """
        Discover all product SKUs and their basic info by paginating through the Tweakwise API
        
        Returns:
            List of dictionaries containing SKU and basic product info
        """
        all_products = []
        seen_skus = set()
        page = 1
        
        self.logger.info("Starting product SKU discovery")
        
        while page <= self.max_pages:
            self.logger.info(f"Fetching product SKUs from page {page}")
            
            # Get products from this page
            page_data = self._get_product_ids(page=page, page_size=self.page_size)
            
            if not page_data:
                self.logger.warning(f"No data received from page {page}, stopping discovery")
                break
            
            # Extract SKUs and basic info from the page
            page_products = self._extract_products_from_tweakwise(page_data)
            
            if not page_products:
                self.logger.warning(f"No products found on page {page}, stopping discovery")
                break
            
            # Add new products to our collection
            new_products = []
            for product in page_products:
                sku = product.get('sku')
                if sku and sku not in seen_skus:
                    all_products.append(product)
                    seen_skus.add(sku)
                    new_products.append(product)
                    
                    # Check if we've reached our product limit during discovery
                    if self.product_limit is not None and len(all_products) >= self.product_limit:
                        self.logger.info(f"Reached product limit during discovery: {len(all_products)} products")
                        return all_products
            
            self.logger.info(f"Page {page}: Found {len(page_products)} products ({len(new_products)} new), Total: {len(all_products)}")
            
            # If we didn't get a full page, we've reached the end
            if len(page_products) < self.page_size:
                self.logger.info(f"Reached end of products at page {page}")
                break
            
            page += 1
            time.sleep(self.request_delay)
        
        return all_products
    
    def _get_product_ids(self, page: int = 1, page_size: int = 50, search_term: str = "") -> Dict[str, Any]:
        """
        Get product IDs from the Tweakwise navigation API
        
        Args:
            page: Page number to fetch (starts from 1)
            page_size: Number of products per page
            search_term: Search term to filter products
            
        Returns:
            Dictionary containing product IDs and metadata
        """
        # Build query parameters
        params = {
            'tn_q': search_term,  # Search query
            'tn_p': str(page),    # Page number
            'tn_ps': str(page_size),  # Page size
            'tn_sort': 'Relevantie',  # Sort by relevance
            'tn_profilekey': '',
            'tn_cid': '999999',   # Category ID (999999 = all products)
            'CatalogPermalink': 'producten',
            'CategoryPermalink': 'producten',
            'format': 'json',
            'tn_parameters': 'ae-productorrecipe=product'
        }
        
        # Construct URL with parameters
        url = f"{self.tweakwise_endpoint}?{urlencode(params)}"
        
        try:
            response = requests.post(url, headers=self.tweakwise_headers)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except requests.RequestException as e:
            self.logger.error(f"Request error fetching product IDs from page {page}: {e}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error for page {page}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error fetching product IDs from page {page}: {e}")
            return {}
    
    def _extract_products_from_tweakwise(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract product information from Tweakwise response
        
        Args:
            data: Tweakwise API response data
            
        Returns:
            List of dictionaries containing product info (sku, name, brand, etc.)
        """
        products = []
        
        if 'items' in data and isinstance(data['items'], list):
            for item in data['items']:
                if isinstance(item, dict) and 'itemno' in item:
                    product_info = {
                        'sku': str(item['itemno']),
                        'name': (item.get('title', '') or '').strip(),
                        'brand': (item.get('brand', '') or '').strip(),
                        'price': item.get('price'),
                        'image_url': item.get('image', ''),
                        'url': item.get('url', '')
                    }
                    
                    # Extract attributes
                    attributes = {}
                    if 'attributes' in item and isinstance(item['attributes'], list):
                        for attr in item['attributes']:
                            if isinstance(attr, dict) and 'name' in attr and 'values' in attr:
                                attr_name = attr['name']
                                attr_values = attr['values']
                                if attr_values and len(attr_values) > 0:
                                    attributes[attr_name] = attr_values[0]
                    
                    product_info['attributes'] = attributes
                    products.append(product_info)
        
        return products
    
    def _get_detailed_product_info_combined(self, products_basic: List[Dict[str, Any]]) -> List[Product]:
        """
        Get detailed product information for all products, combining Tweakwise basic info with Hoogvliet details
        
        Args:
            products_basic: List of basic product info from Tweakwise
            
        Returns:
            List of Product objects
        """
        products = []
        skus = [p['sku'] for p in products_basic]
        
        # Create a lookup map for basic product info
        basic_info_map = {p['sku']: p for p in products_basic}
        
        # Process SKUs in batches
        for i in range(0, len(skus), self.batch_size):
            batch_skus = skus[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(skus) + self.batch_size - 1) // self.batch_size
            
            self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_skus)} products)")
            
            # Get details for this batch
            batch_products = self._get_product_details_batch_combined(batch_skus, basic_info_map)
            
            if batch_products:
                products.extend(batch_products)
                self.logger.info(f"Batch {batch_num}: Added {len(batch_products)} products")
            else:
                self.logger.warning(f"Batch {batch_num}: No products received")
            
            # Add delay between batches
            if i + self.batch_size < len(skus):  # Don't delay after last batch
                time.sleep(self.request_delay)
        
        return products
    
    def _get_product_details_batch_combined(self, product_skus: List[str], basic_info_map: Dict[str, Dict[str, Any]]) -> List[Product]:
        """
        Get detailed product information for a batch of SKUs and combine with basic info from Tweakwise
        
        Args:
            product_skus: List of product SKUs/IDs to fetch details for
            basic_info_map: Dictionary mapping SKU to basic product info from Tweakwise
            
        Returns:
            List of Product objects
        """
        if not product_skus:
            return []
        
        # Join SKUs with commas
        products_param = ','.join(product_skus)
        
        # Build URL with products parameter
        url = f"{self.hoogvliet_details_endpoint};sid={self.session_id}?products={products_param}"
        
        # Headers with cookies
        headers_with_cookies = self.hoogvliet_headers.copy()
        headers_with_cookies['Cookie'] = self.cookies
        
        try:
            response = requests.post(url, headers=headers_with_cookies)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            if isinstance(data, list):
                products = []
                for product_data in data:
                    sku = str(product_data.get('sku', ''))
                    basic_info = basic_info_map.get(sku, {})
                    product = self._parse_product_data_combined(product_data, basic_info)
                    if product:
                        products.append(product)
                
                # Handle SKUs that weren't returned by details API (create from basic info only)
                returned_skus = {str(p.get('sku', '')) for p in data}
                missing_skus = set(product_skus) - returned_skus
                for missing_sku in missing_skus:
                    if missing_sku in basic_info_map:
                        self.logger.debug(f"Creating product from basic info only for SKU: {missing_sku}")
                        product = self._create_product_from_basic_info(basic_info_map[missing_sku])
                        if product:
                            products.append(product)
                
                return products
            else:
                self.logger.warning(f"Unexpected response format: {type(data)}")
                # Fallback: create products from basic info only
                return [self._create_product_from_basic_info(basic_info_map[sku]) 
                       for sku in product_skus if sku in basic_info_map]
                
        except requests.RequestException as e:
            self.logger.error(f"Request error fetching product details: {e}")
            # Fallback: create products from basic info only
            return [self._create_product_from_basic_info(basic_info_map[sku]) 
                   for sku in product_skus if sku in basic_info_map]
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error fetching product details: {e}")
            # Fallback: create products from basic info only
            return [self._create_product_from_basic_info(basic_info_map[sku]) 
                   for sku in product_skus if sku in basic_info_map]
        except Exception as e:
            self.logger.error(f"Unexpected error fetching product details: {e}")
            # Fallback: create products from basic info only
            return [self._create_product_from_basic_info(basic_info_map[sku]) 
                   for sku in product_skus if sku in basic_info_map]
    
    def _parse_product_data_combined(self, product_data: Dict[str, Any], basic_info: Dict[str, Any]) -> Optional[Product]:
        """
        Parse individual product data combining Hoogvliet details API response with Tweakwise basic info
        
        Args:
            product_data: Raw product data from Hoogvliet details API
            basic_info: Basic product info from Tweakwise API
            
        Returns:
            Product object or None if parsing fails
        """
        try:
            # Extract basic product information
            product_id = str(product_data.get('sku', ''))
            if not product_id:
                self.logger.warning("Product missing SKU, skipping")
                return None
            
            # Prefer a fuller product name when available: try details API first, then Tweakwise title
            basic_name = (basic_info.get('name') or '').strip()

            # Build candidate from details API fields if present
            details_name = ''
            product_info = product_data.get('productInformation') or {}
            if isinstance(product_info, dict):
                header = (product_info.get('headerText') or '').strip()
                sub = (product_info.get('subText') or '').strip()
                if header or sub:
                    details_name = ' '.join([p for p in (header, sub) if p]).strip()

            # Other possible detail-level name fields
            if not details_name:
                details_name = (product_data.get('productLabel') or '').strip()
            if not details_name:
                details_name = (product_data.get('name') or '').strip()

            # Choose the most descriptive, non-fallback name
            def _valid(n: str) -> bool:
                return bool(n) and not n.lower().startswith('product ')

            # Build list of candidate names (details first, then discovery/basic, then other detail fields)
            candidates = []
            for cand in (details_name, basic_name, product_data.get('productLabel') or '', product_data.get('name') or ''):
                if isinstance(cand, str) and _valid(cand):
                    candidates.append(cand.strip())

            if candidates:
                # Prefer candidate with most word tokens, then longer character length
                import re as _re
                candidates.sort(key=lambda s: (len(_re.findall(r"\w+", s)), len(s)), reverse=True)
                name = candidates[0]
            else:
                name = f"Product {product_id}"

            # Prepend brand if available and not already present
            try:
                if brand:
                    if not name.lower().startswith(brand.lower()):
                        name = f"{brand} {name}"
            except Exception:
                pass
            
            # Price information from details API
            current_price = product_data.get('listPrice')
            original_price = product_data.get('discountedPrice')  # Note: this might be backwards
            
            # Convert prices to float for comparison
            try:
                current_price = float(current_price) if current_price is not None else None
            except (ValueError, TypeError):
                current_price = None
                
            try:
                original_price = float(original_price) if original_price is not None else None
            except (ValueError, TypeError):
                original_price = None
            
            if not current_price or current_price <= 0:
                self.logger.warning(f"Product {product_id} has invalid price: {current_price}")
                return None
            
            # Handle discount pricing (Hoogvliet might have this backwards)
            if original_price and original_price != current_price:
                # If discountedPrice exists and is different, use listPrice as original and discountedPrice as current
                if original_price < current_price:
                    actual_current_price = original_price
                    actual_original_price = current_price
                else:
                    actual_current_price = current_price
                    actual_original_price = original_price
            else:
                actual_current_price = current_price
                actual_original_price = None
            
            # Category information from details API
            category = product_data.get('categoryHierarchy', '')
            if category and isinstance(category, str):
                if '/' in category:
                    # Take the most specific category (last part)
                    category = category.split('/')[-1].strip()
                else:
                    category = category.strip()
            
            # Default to Unknown if category is empty
            if not category:
                category = 'Unknown'
            
            # Brand information from Tweakwise basic info (more reliable)
            brand = (basic_info.get('brand') or '').strip()
            
            # Unit information from details API
            base_unit = product_data.get('baseUnit', 'stuk')
            ratio = product_data.get('ratioBasePackingUnit', 1)
            
            # Convert ratio to float for calculations
            try:
                ratio = float(ratio) if ratio is not None else 1.0
            except (ValueError, TypeError):
                ratio = 1.0
            
            # Construct unit amount string
            if base_unit in ['kg', 'liter', 'l']:
                unit_amount = f"{ratio} {base_unit}"
            elif base_unit in ['gram', 'g'] and ratio >= 1000:
                # Convert to kg for better comparison
                unit_amount = f"{ratio/1000} kg"
            elif base_unit in ['ml'] and ratio >= 1000:
                # Convert to liter for better comparison
                unit_amount = f"{ratio/1000} liter"
            else:
                unit_amount = f"{ratio} {base_unit}"
            
            # Stock information
            in_stock = product_data.get('inStock', True)
            if not in_stock:
                self.logger.debug(f"Product {product_id} is out of stock, still including")
            
            # Discount information
            discount_type = None
            discount_start_date = None
            discount_end_date = None
            
            if 'promotions' in product_data and product_data['promotions']:
                promotions = product_data['promotions']
                if isinstance(promotions, list) and promotions:
                    # Take the first promotion
                    promotion = promotions[0]
                    if isinstance(promotion, dict):
                        discount_type = promotion.get('description', 'Aanbieding')
                        
                        # Check for date fields in promotion (API may contain startDate, endDate, validFrom, validTo)
                        # Common date field names to check
                        start_date_fields = ['startDate', 'validFrom', 'start', 'fromDate', 'beginDate']
                        end_date_fields = ['endDate', 'validTo', 'end', 'toDate', 'untilDate']
                        
                        for field in start_date_fields:
                            if field in promotion and promotion[field]:
                                try:
                                    # Try to parse the date - could be ISO format or epoch timestamp
                                    date_value = promotion[field]
                                    if isinstance(date_value, str):
                                        discount_start_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                                    elif isinstance(date_value, (int, float)):
                                        discount_start_date = datetime.fromtimestamp(date_value)
                                    break  # Use first found date field
                                except (ValueError, TypeError) as e:
                                    self.logger.debug(f"Could not parse promotion start date from {field}: {e}")
                        
                        for field in end_date_fields:
                            if field in promotion and promotion[field]:
                                try:
                                    date_value = promotion[field]
                                    if isinstance(date_value, str):
                                        discount_end_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                                    elif isinstance(date_value, (int, float)):
                                        discount_end_date = datetime.fromtimestamp(date_value)
                                    break  # Use first found date field
                                except (ValueError, TypeError) as e:
                                    self.logger.debug(f"Could not parse promotion end date from {field}: {e}")
            
            # Extract image URL from basic info
            image_url = basic_info.get('image_url', '')
            
            # Create the product
            product = self._create_product(
                product_id=product_id,
                name=name,
                category=category,
                price=actual_current_price,
                unit_amount=unit_amount,
                original_price=actual_original_price,
                discount_type=discount_type,
                brand=brand,
                discount_start_date=discount_start_date,
                discount_end_date=discount_end_date,
                image_url=image_url
            )
            
            return product
            
        except Exception as e:
            self.logger.error(f"Error parsing combined product data: {e}", exc_info=True)
            return None
    
    def _normalize_unit_amount(self, base_unit: str, ratio: float) -> str:
        """
        Normalize unit amount to standard format
        
        Args:
            base_unit: Base unit from API (e.g., 'gram', 'liter', 'stuk')
            ratio: Ratio/amount from API
            
        Returns:
            Normalized unit amount string
        """
        # Convert common units to standard format
        unit_map = {
            'gram': 'g',
            'kilogram': 'kg',
            'liter': 'l',
            'litre': 'l',
            'milliliter': 'ml',
            'stuk': 'piece',
            'stuks': 'pieces'
        }
        
        normalized_unit = unit_map.get(base_unit.lower(), base_unit)
        
        # Format the amount
        if ratio == int(ratio):
            return f"{int(ratio)} {normalized_unit}"
        else:
            return f"{ratio} {normalized_unit}"


# Example usage and testing
if __name__ == "__main__":
    # This section can be used for testing the scraper independently
    import logging
    from database import get_db_config
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test the scraper
    db_config = get_db_config()
    db_manager = DatabaseManager(db_config)
    
    if db_manager.connect():
        scraper = HoogvlietScraper(db_manager)
        
        # Test with a small subset first
        scraper.max_pages = 2  # Limit for testing
        
        try:
            products = scraper.scrape_products()
            print(f"Successfully scraped {len(products)} products")
            
            # Show sample products
            for i, product in enumerate(products[:5]):
                print(f"\nProduct {i+1}:")
                print(f"  ID: {product.product_id}")
                print(f"  Name: {product.name}")
                print(f"  Category: {product.category_name}")
                print(f"  Price: €{product.price}")
                print(f"  Unit: {product.unit_amount}")
                print(f"  Price per {product.unit_type.value}: €{product.price_per_unit}")
                
        except Exception as e:
            print(f"Error running scraper: {e}")
        finally:
            db_manager.disconnect()
    else:
        print("Could not connect to database")
