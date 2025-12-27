"""
Simplified AH Scraper
Uses Selenium to get cookies, then requests for API calls
"""
import requests
from bs4 import BeautifulSoup
import logging
import json
import re
import time
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, parse_qs

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from base_scraper import BaseScraper
from database import Product, PriceCalculator


class AHScraper(BaseScraper):
    """Simplified scraper for AH.nl (Albert Heijn) supermarket"""

    BASE_URL = "https://www.ah.nl"
    SITEMAP_URL = "https://www.ah.nl/sitemaps/entities/products/categories.xml"
    API_BASE_URL = "https://www.ah.nl/zoeken/api/products/search"
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "AH")
        self.cookies_initialized = False
        self.driver = None

    def scrape_products(self) -> List[Product]:
        """Scrape products using AH API"""
        products = []
        
        # Initialize cookies first
        if not self._initialize_cookies():
            self.logger.error("Failed to initialize cookies")
            return products
        
        # Get categories from sitemap
        categories = self._fetch_categories()
        self.logger.info(f"Found {len(categories)} categories")
        
        # Limit for testing - remove in production
        categories = categories[:3]  # Remove this line for full scraping
        
        for category in categories:
            try:
                self.logger.info(f"Processing category: {category['name']}")
                category_products = self._scrape_category_products(category)
                products.extend(category_products)
                
                # Small delay between categories
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to scrape category {category.get('name', 'unknown')}: {e}")
                continue
        
        return products

    def _initialize_cookies(self) -> bool:
        """Initialize cookies using Selenium browser"""
        try:
            self.logger.info("Initializing cookies with Selenium")
            
            # Setup Chrome options
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Initialize driver
            try:
                # Use webdriver-manager to automatically get the correct ChromeDriver
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except WebDriverException as e:
                self.logger.error(f"Failed to initialize Chrome driver: {e}")
                self.logger.info("Make sure ChromeDriver is installed and in PATH")
                return False
            
            # Visit AH homepage
            self.driver.get(self.BASE_URL)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Try to handle cookie consent popup
            try:
                # Look for common cookie consent button texts
                cookie_buttons = [
                    "//button[contains(text(), 'Accepteren')]",
                    "//button[contains(text(), 'Accept')]",
                    "//button[contains(text(), 'Akkoord')]",
                    "//button[contains(@class, 'cookie')]",
                    "//button[contains(@id, 'cookie')]"
                ]
                
                for button_xpath in cookie_buttons:
                    try:
                        cookie_button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, button_xpath))
                        )
                        cookie_button.click()
                        self.logger.info("Clicked cookie consent button")
                        time.sleep(2)  # Wait for consent to be processed
                        break
                    except TimeoutException:
                        continue
                        
            except Exception as e:
                self.logger.warning(f"Could not handle cookie consent: {e}")
            
            # Visit products page to ensure full session
            self.driver.get(f"{self.BASE_URL}/producten")
            time.sleep(3)  # Wait for page to fully load
            
            # Extract cookies from Selenium and add to requests session
            selenium_cookies = self.driver.get_cookies()
            
            for cookie in selenium_cookies:
                self.session.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', '.ah.nl'),
                    path=cookie.get('path', '/'),
                    secure=cookie.get('secure', False)
                )
            
            
            
            self.logger.info(f"Transferred {len(selenium_cookies)} cookies from Selenium to requests")
                
            
            self.cookies_initialized = True
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize cookies with Selenium: {e}")
            return False
        finally:
            # Clean up Selenium driver
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None

    def _fetch_categories(self) -> List[Dict[str, Any]]:
        """Fetch categories from sitemap"""
        try:
            response = self.session.get(self.SITEMAP_URL, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            categories = []
            
            for url_tag in soup.find_all('url'):
                loc_tag = url_tag.find('loc')
                if not loc_tag:
                    continue
                
                category_url = loc_tag.text.strip()
                category_info = self._extract_category_info(category_url)
                
                if category_info:
                    categories.append(category_info)
            
            return categories
            
        except Exception as e:
            self.logger.error(f"Failed to fetch categories: {e}")
            return []

    def _extract_category_info(self, category_url: str) -> Optional[Dict[str, Any]]:
        """Extract category information and taxonomy ID"""
        try:
            # Get category name from URL
            path_parts = [p for p in urlparse(category_url).path.split('/') if p]
            if len(path_parts) < 2:
                return None
            
            category_slug = path_parts[-1]
            category_name = category_slug.replace('-', ' ').title()
            
            # Visit category page to get taxonomy ID
            taxonomy_id = self._get_taxonomy_id(category_url)
            if not taxonomy_id:
                return None
            
            return {
                'name': category_name,
                'slug': category_slug,
                'taxonomy_id': taxonomy_id,
                'url': category_url
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract category info from {category_url}: {e}")
            return None

    def _get_taxonomy_id(self, category_url: str) -> Optional[str]:
        """Extract taxonomy ID directly from URL path"""
        try:
            # Extract from URL path - AH category URLs have format: /producten/6401/groente-aardappelen
            path_parts = [p for p in urlparse(category_url).path.split('/') if p]
            
            # Check if we have the expected structure: ['producten', 'taxonomy_id', 'category_slug']
            if len(path_parts) >= 2 and path_parts[0] == 'producten' and path_parts[1].isdigit():
                taxonomy_id = path_parts[1]
                self.logger.debug(f"Found taxonomy ID {taxonomy_id} in URL: {category_url}")
                return taxonomy_id
            
            # If taxonomy ID is not in the expected format, skip this category
            self.logger.warning(f"No taxonomy ID found in URL path: {category_url}")
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to extract taxonomy ID from URL {category_url}: {e}")
            return None

    def _scrape_category_products(self, category: Dict[str, Any]) -> List[Product]:
        """Scrape all products from a category using API"""
        products = []
        page = 1
        page_size = 300  # Using the page size from the sample response
        self.session.headers.update({
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.8',
            'content-type': 'application/json',
        })
        
        while True:
            try:
                # Construct API URL
                api_url = (f"{self.API_BASE_URL}?page={page}&size={page_size}"
                          f"&taxonomySlug={category['slug']}&taxonomy={category['taxonomy_id']}")
                
                self.logger.info(f"Fetching page {page} for category {category['name']}")
                
                response = self.session.get(api_url, timeout=30)
                
                # Handle different response codes
                if response.status_code == 403:
                    self.logger.error(f"403 Forbidden - cookies may have expired or API access denied")
                    break
                elif response.status_code == 429:
                    self.logger.warning(f"Rate limited, waiting 5 seconds...")
                    time.sleep(5)
                    continue
                
                response.raise_for_status()
                
                # Try to parse JSON response
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    self.logger.error(f"Invalid JSON response for page {page}")
                    break
                
                # Extract products from cards structure - each card has a products array
                api_products = []
                cards = data.get('cards', [])
                
                self.logger.debug(f"Found {len(cards)} cards in response")
                
                for card in cards:
                    # Each card contains products array
                    card_products = card.get('products', [])
                    api_products.extend(card_products)
                    self.logger.debug(f"Card {card.get('id')} has {len(card_products)} products")
                
                if not api_products:
                    self.logger.info(f"No more products for category {category['name']}")
                    break
                
                self.logger.info(f"Processing {len(api_products)} products from page {page}")
                
                # Process products
                for api_product in api_products:
                    product = self._process_api_product(api_product, category)
                    if product:
                        products.append(product)
                
                # Check pagination info to determine if we should continue
                page_info = data.get('page', {})
                current_page = page_info.get('number', page)
                total_pages = page_info.get('totalPages', 1)
                total_elements = page_info.get('totalElements', 0)
                
                self.logger.info(f"Page {current_page} of {total_pages}, found {len(api_products)} products")
                
                # Break if we've reached the last page or no more products
                if current_page >= total_pages or len(api_products) == 0:
                    break
                
                page += 1
                time.sleep(1)  # Respectful delay between requests
                
            except Exception as e:
                self.logger.error(f"Failed to fetch page {page} for category {category['name']}: {e}")
                break
        
        self.logger.info(f"Scraped {len(products)} products from category {category['name']}")
        return products

    def _process_api_product(self, api_product: Dict[str, Any], category: Dict[str, Any]) -> Optional[Product]:
        """Process a single product from AH API response"""
        try:
            # Extract basic info
            product_id = str(api_product.get('id', ''))
            name = api_product.get('title', '').strip()
            
            if not product_id or not name:
                self.logger.debug(f"Skipping product with missing ID or name: {api_product}")
                return None
            
            # Price information - use the correct structure from API response
            price_info = api_product.get('price', {})
            current_price = price_info.get('now')
            
            if current_price is None:
                self.logger.debug(f"Skipping product {name} - no price information")
                return None
            
            # Unit size and unit information
            unit_size = price_info.get('unitSize', '1 piece')
            
            # Clean up unit size for better parsing
            if unit_size:
                # Handle Dutch unit formats
                unit_size = unit_size.replace('per stuk', '1 piece')
                unit_size = unit_size.replace('stuks', 'pieces')
                unit_size = unit_size.replace('stuk', 'piece')
                # Keep original if it looks like a quantity + unit
                if not re.search(r'\d+\s*[a-zA-Z]', unit_size):
                    unit_size = '1 piece'
            else:
                unit_size = '1 piece'
            

            # Brand information
            brand = api_product.get('brand', '').strip()
            
            # Category information - use product category if available, fallback to passed category
            product_category = api_product.get('category', category['name'])
            
            # Check for promotional/discount indicators
            property_icons = api_product.get('propertyIcons', [])
            
            # Look for discount indicators
            is_bonus = any(icon.get('name') == 'bonus' for icon in property_icons)
            is_price_favorite = any(icon.get('name') == 'prijsfavoriet' for icon in property_icons)
            is_biological = any(icon.get('name') == 'biologisch' for icon in property_icons)
            is_local = any(icon.get('name') == 'lokaal' for icon in property_icons)
            
            # For now, we don't have clear original price in the response
            # The API might have promotional pricing but it's not clearly exposed
            original_price = None
            discount_type = None
            
            if is_bonus:
                discount_type = "bonus"
            elif is_price_favorite:
                discount_type = "prijsfavoriet"
            
            # Additional product information
            available_online = api_product.get('availableOnline', True)
            orderable = api_product.get('orderable', True)
            
            # Nutritional information
            properties = api_product.get('properties', {})
            nutriscore = properties.get('nutriscore', '')
            
            # Product link for reference
            product_link = api_product.get('link', '')
            
            # Extract image URL from API response
            image_url = ''
            images = api_product.get('images', [])
            if images and isinstance(images, list) and len(images) > 0:
                # Get the first image URL
                first_image = images[0]
                if isinstance(first_image, dict):
                    image_url = first_image.get('url', '')
                elif isinstance(first_image, str):
                    image_url = first_image
            
            # Fallback to imageUrl field if available
            if not image_url:
                image_url = api_product.get('imageUrl', '') or api_product.get('image', '')
            
            # Log some key information for debugging
            self.logger.debug(f"Processing product: {name} - €{current_price} - {unit_size}")
            if brand:
                self.logger.debug(f"  Brand: {brand}")
            if discount_type:
                self.logger.debug(f"  Discount type: {discount_type}")
            
            # Add extra debugging for price calculation issues
            try:
                test_price_per_unit, test_unit_type = PriceCalculator.calculate_price_per_unit(float(current_price), unit_size)
                if test_price_per_unit <= 0:
                    self.logger.warning(f"Product {name} (ID: {product_id}) has invalid price per unit: {test_price_per_unit} per {test_unit_type} from price {current_price} and unit {unit_size}")
                    return None
            except Exception as calc_error:
                self.logger.error(f"Price calculation failed for product {name} (ID: {product_id}): {calc_error}")
                self.logger.debug(f"  Price: {current_price}, Unit: {unit_size}")
                return None
            
            return self._create_product(
                product_id=product_id,
                name=name,
                category=product_category,
                price=float(current_price),
                unit_amount=unit_size,
                original_price=float(original_price) if original_price else None,
                discount_type=discount_type,
                brand=brand,
                discount_start_date=None,  # AH doesn't provide date fields in API
                discount_end_date=None,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process API product {api_product.get('id', 'unknown')}: {e}")
            self.logger.debug(f"Problem product data: {api_product}")
            return None

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {e}")
        # Don't call super().cleanup() as base class doesn't have it


# Test function
def test_ah_scraper():
    """Test function for development"""
    from database import DatabaseManager, get_db_config
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    config = get_db_config()
    
    with DatabaseManager(config) as db:
        scraper = AHScraper(db)
        
        # Test cookie initialization
        if scraper._initialize_cookies():
            print("✓ Cookies initialized successfully")
        else:
            print("✗ Failed to initialize cookies")
            return
        
        # Test category fetching
        categories = scraper._fetch_categories()
        print(f"✓ Found {len(categories)} categories")
        
        if categories:
            # Test first category
            category = categories[0]
            print(f"Testing category: {category['name']}")
            
            products = scraper._scrape_category_products(category)
            print(f"✓ Found {len(products)} products in category")
            
            for product in products[:100]:
                
                print(f"Sample product: {product.name}")
                print(f"Price: €{product.price}")
                print(f"Unit: {product.unit_amount}")
                print(f"Price per unit: €{product.price_per_unit}/{product.unit_type}")
                print(f"Category: {product.category_name}")
                print(f"Search Tags: {product.search_tags}")
                print(product)
                print("")
                


if __name__ == "__main__":
    test_ah_scraper()
