"""
Simplified Dirk Scraper
Keeps original structure but focuses only on required data fields
"""
import requests
from bs4 import BeautifulSoup
import logging
import json
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urljoin
from datetime import datetime
import concurrent.futures

from base_scraper import BaseScraper
from database import Product, PriceCalculator


class DirkScraper(BaseScraper):
    """Simplified scraper for Dirk.nl supermarket - maintains original structure"""

    BASE_URL = "https://www.dirk.nl"
    SITEMAP_URL = "https://www.dirk.nl/products-sitemap.xml"
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "DIRK")
        self.max_workers = 8  # concurrent threads for product pages
        self._setup_headers()

    def _setup_headers(self):
        """Setup comprehensive browser-like headers to avoid 403 errors"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://www.dirk.nl/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Cache-Control': 'max-age=0',
        })

    def scrape_products(self) -> List[Product]:
        """Scrape products using original sitemap approach (now concurrent)."""
        products: List[Product] = []
        
        # Get sitemap URLs (same as original)
        sitemap_urls = self.fetch_sitemap()
        self.logger.info(f"Found {len(sitemap_urls)} URLs in sitemap")
        
        # Respect optional product_limit if set; otherwise scrape all
        urls_to_scrape = sitemap_urls
        if self.product_limit is not None:
            urls_to_scrape = sitemap_urls[: self.product_limit]
            self.logger.info(f"Applying product limit to URLs: {len(urls_to_scrape)}")
        
        if not urls_to_scrape:
            return products
        
        # Fetch product pages concurrently using per-thread sessions
        processed = 0
        total = len(urls_to_scrape)
        
        def _scrape_url(url: str) -> Optional[Product]:
            try:
                # Create a local session per thread to avoid sharing one session across threads
                local_session = requests.Session()
                # Copy default headers (e.g., User-Agent) from the base session
                try:
                    local_session.headers.update(self.session.headers)
                except Exception:
                    pass
                resp = local_session.get(url, timeout=20)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')
                return self._parse_product_data(soup, url)
            except Exception as e:
                self.logger.error(f"Failed to scrape {url}: {e}")
                return None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(_scrape_url, item['url']): item['url'] for item in urls_to_scrape}
            for future in concurrent.futures.as_completed(future_to_url):
                processed += 1
                try:
                    product = future.result()
                    if product:
                        products.append(product)
                except Exception as e:
                    url = future_to_url[future]
                    self.logger.error(f"Unhandled error scraping {url}: {e}")
                
                if processed % 10 == 0 or processed == total:
                    self.logger.info(f"Processed {processed}/{total} products")
        
        return products

    def fetch_sitemap(self) -> List[Dict[str, Any]]:
        """
        Fetch and parse the sitemap to get product URLs (same as original)
        
        Returns:
            List of dictionaries with url and last_modified
        """
        self.logger.info(f"Fetching sitemap from {self.SITEMAP_URL}")
        try:
            response = self.session.get(self.SITEMAP_URL, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            urls = []
            for url_tag in soup.find_all('url'):
                loc = url_tag.find('loc').text.strip()
                lastmod_tag = url_tag.find('lastmod')
                
                if lastmod_tag:
                    lastmod_str = lastmod_tag.text.strip()
                    # Parse timezone-aware datetime
                    try:
                        lastmod = datetime.fromisoformat(lastmod_str)
                    except ValueError:
                        self.logger.warning(f"Could not parse lastmod '{lastmod_str}' for url {loc}")
                        lastmod = None
                else:
                    lastmod = None

                urls.append({'url': loc, 'last_modified': lastmod})
            
            self.logger.info(f"Found {len(urls)} URLs in sitemap")
            return urls
        except Exception as e:
            self.logger.error(f"Failed to fetch sitemap: {e}")
            return []

    def scrape_product_page(self, url: str) -> Optional[Product]:
        """
        Scrape a single product page (same structure as original)
        
        Args:
            url: Product page URL
            
        Returns:
            Product object or None
        """
        self.logger.debug(f"Scraping product page: {url}")
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return self._parse_product_data(soup, url)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch product page {url}: {e}")
            return None

    def _parse_product_data(self, soup: BeautifulSoup, url: str) -> Optional[Product]:
        """
        Parse product data from BeautifulSoup object.
        Prioritizes JSON-LD structured data if available (same priority as original).
        
        Args:
            soup: BeautifulSoup object of the product page
            url: Product page URL
            
        Returns:
            Product object with only required data
        """
        try:
            # First, try to find JSON-LD structured data (same as original)
            json_ld_script = soup.find('script', type='application/ld+json')
            if json_ld_script:
                self.logger.debug(f"Found JSON-LD data for {url}")
                data = json.loads(json_ld_script.string)
                product = self._parse_from_json_ld(data, url, soup)
                
                if product:
                    return product
                    
            # Fallback to manual HTML parsing if JSON-LD is not found (same as original)
            self.logger.warning(f"No JSON-LD data found for {url}. Falling back to HTML parsing.")
            return self._parse_from_html(soup, url)

        except Exception as e:
            self.logger.error(f"Error parsing product data for {url}: {e}")
            return None

    def _parse_from_json_ld(self, data: Dict[str, Any], url: str, soup: BeautifulSoup) -> Optional[Product]:
        """
        Parse product data from a JSON-LD dictionary (same structure as original).
        Only extracts required data fields.
        
        Args:
            data: The dictionary parsed from the JSON-LD script.
            url: The product page URL.
            soup: BeautifulSoup object for additional HTML parsing
            
        Returns:
            Product object with required data only
        """
        try:
            # The main product data is usually the first item in the graph (same as original)
            product_info = next((item for item in data.get('@graph', []) if item.get('@type') == 'Product'), None)
            if not product_info:
                self.logger.warning(f"Could not find 'Product' type in JSON-LD @graph for {url}")
                return None

            # Extract required fields only
            product_id = product_info.get('mpn', urlparse(url).path.split('/')[-1])
            name = product_info.get('name')
            
            if not name or not product_id:
                return None

            # Offers (same extraction as original but fix the price field name)
            offer = product_info.get('offers', {})
            current_price = offer.get('Price') or offer.get('price')  # Try both Price and price
            if not current_price:
                return None

            # Check for original price in HTML (same approach as original)
            original_price = None
            discount_type = None
            regular_price_elem = soup.find('span', class_='regular-price')
            if regular_price_elem:
                price_match = re.search(r'\d+\.\d+', regular_price_elem.text)
                if price_match:
                    try:
                        original_price = float(price_match.group())
                        discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
                        discount_type = f"{discount_percentage}% korting"
                    except (ValueError, TypeError):
                        print(f"Error converting original price to float: {price_match.group()}")
                        original_price = None

            # Category from breadcrumb (same extraction as original but fix the structure)
            category = "Unknown"
            breadcrumb = next((item for item in data.get('@graph', []) if item.get('@type') == 'BreadcrumbList'), None)
            if breadcrumb and len(breadcrumb.get('itemListElement', [])) > 1:
                items = breadcrumb['itemListElement']
                # Get the second to last item (last is usually the product itself)
                if len(items) >= 2:
                    category_item = items[-2]
                    # The category name is nested in item.name
                    item_data = category_item.get('item', {})
                    category_name = item_data.get('name')
                    if category_name:
                        category = category_name

            # Unit amount from description, name, or HTML subtitle
            unit_amount = self._extract_unit_amount_from_text(
                product_info.get('description', '') + ' ' + name
            )
            
            # Try to get unit from HTML subtitle if not found in JSON-LD
            if unit_amount == "1 piece":
                subtitle_elem = soup.find('p', class_='subtitle')
                if subtitle_elem:
                    subtitle_text = subtitle_elem.get_text(strip=True)
                    unit_from_subtitle = self._extract_unit_amount_from_text(subtitle_text)
                    if unit_from_subtitle != "1 piece":
                        unit_amount = unit_from_subtitle

            # Brand
            brand = None
            if isinstance(product_info.get('brand'), dict):
                brand = product_info['brand'].get('name')
            
            # Extract image URL from JSON-LD
            image_url = ''
            image_data = product_info.get('image')
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

            return self._create_product(
                product_id=str(product_id),
                name=name,
                category=category,
                price=current_price,  # Already converted to float above with error handling
                unit_amount=unit_amount,
                original_price=original_price,
                discount_type=discount_type,
                brand=brand,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Failed to parse JSON-LD for {url}: {e}")
            return None

    def _parse_from_html(self, soup: BeautifulSoup, url: str) -> Optional[Product]:
        """
        Parse product data by scraping the HTML structure (same approach as original).
        Only extracts required data fields.
        
        Args:
            soup: BeautifulSoup object of the product page.
            url: The product page URL.
            
        Returns:
            Product object with required data only
        """
        try:
            # Extract product ID and slug from URL (same as original)
            path_parts = [p for p in urlparse(url).path.split('/') if p]
            product_id = path_parts[-1]
            
            # Product name (same selector as original)
            name_tag = soup.find('h1')
            name = name_tag.text.strip() if name_tag else None
            
            if not name or not product_id:
                return None

            # Price (same extraction logic as original)
            current_price = None
            original_price = None
            discount_type = None
            
            price_container = soup.find('div', class_='product-card__price-container')
            if price_container:
                price_text = price_container.text.strip()
                prices = re.findall(r'(\d+\.\d{2})', price_text)
                if len(prices) == 2:  # Sale price
                    try:
                        original_price = float(prices[0])
                        current_price = float(prices[1])
                        discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
                        discount_type = f"{discount_percentage}% korting"
                    except (ValueError, TypeError):
                        print(f"Error converting sale prices to float: {prices}")
                        return None
                elif len(prices) == 1:  # Regular price
                    try:
                        current_price = float(prices[0])
                    except (ValueError, TypeError):
                        print(f"Error converting regular price to float: {prices[0]}")
                        return None
            
            if not current_price:
                return None

            # Unit amount (same selector as original)
            unit_amount = "1 piece"  # Default
            package_size_tag = soup.find('span', class_='product-card__volume')
            if package_size_tag:
                unit_text = package_size_tag.text.strip()
                unit_amount = self._extract_unit_amount_from_text(unit_text)

            # Category (same extraction as original)
            category = "Unknown"
            breadcrumb = soup.find('div', class_='breadcrumb')
            if breadcrumb:
                category_items = breadcrumb.find_all('a')
                if len(category_items) > 1:
                    category = category_items[-2].text.strip()
            
            # Extract image URL from HTML
            image_url = ''
            image_tag = soup.find('img', class_='product-card__image')
            if image_tag:
                image_url = image_tag.get('src', '') or image_tag.get('data-src', '')

            return self._create_product(
                product_id=str(product_id),
                name=name,
                category=category,
                price=current_price,
                unit_amount=unit_amount,
                original_price=original_price,
                discount_type=discount_type,
                discount_start_date=None,  # Dirk doesn't have date fields in JSON-LD
                discount_end_date=None,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Failed to parse HTML for {url}: {e}")
            return None

    def _extract_unit_amount_from_text(self, text: str) -> str:
        """Extract unit amount from text (helper method)"""
        if not text:
            return "1 piece"
        
        # Look for common unit patterns including "x" format (e.g., "24 x 300 ml")
        patterns = [
            r'(\d+\s*x\s*\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))',  # "24 x 300 ml"
            r'(\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))',  # Standard units
        ]
        
        for pattern in patterns:
            unit_match = re.search(pattern, text.lower())
            if unit_match:
                return unit_match.group(1).replace(',', '.')
        
        return "1 piece"


# Test function
def test_dirk_scraper():
    """Test function for development"""
    from database import DatabaseManager, get_db_config
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Test with single product (same URL as original)
    test_url = "https://www.dirk.nl/boodschappen/dranken-sap-koffie-thee/bier/heineken-pilsener-krat/6"
    
    config = get_db_config()
    
    with DatabaseManager(config) as db:
        scraper = DirkScraper(db)
        product = scraper.scrape_product_page(test_url)
        
        if product:
            print(f"Product: {product.name}")
            print(f"Price: €{product.price}")
            print(f"Unit: {product.unit_amount}")
            print(f"Price per unit: €{product.price_per_unit}/{product.unit_type.value}")
            print(f"Category: {product.category_name}")
            print(f"Search tags: {product.search_tags}")
            if product.original_price:
                print(f"Original price: €{product.original_price}")
                print(f"Discount: {product.discount_type}")
        else:
            print("Failed to scrape product")


if __name__ == "__main__":
    test_dirk_scraper()
