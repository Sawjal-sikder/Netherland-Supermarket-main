"""
Plus Supermarket Scraper

This module implements a scraper for Plus supermarket using their API.
Plus uses a paginated API that returns 12 products per page, requiring
concurrent requests for efficient scraping of all 17,254+ products.
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
import concurrent.futures
from typing import List, Dict, Optional
import copy
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


class PlusScraper(BaseScraper):
    """Scraper for Plus supermarket using their API with concurrent requests"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, "plus")
        self.api_url = "https://www.plus.nl/screenservices/ECP_Composition_CW/ProductLists/PLP_Content/DataActionGetProductListAndCategoryInfo"
        self.products_url = "https://www.plus.nl/producten/"
        self.max_workers = 3  # Concurrent request workers
        self.page_size = 12  # Fixed by API
        self.cookies = {}
        self.headers = {}
        self.json_data_template = {}
        
    def _get_cookies_and_headers(self) -> bool:
        """Get fresh cookies by visiting the Plus website, keep fixed headers and CSRF token"""
        self.logger.info("Getting fresh cookies from Plus website...")
        
        try:
            # Setup Selenium driver
            driver = self._setup_selenium_driver()
            if not driver:
                self.logger.error("Failed to setup Selenium driver")
                return False
            
            try:
                # Visit the Plus products page
                self.logger.info("Visiting Plus products page...")
                driver.get(self.products_url)
                
                # Wait for page to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Give extra time for all cookies to be set
                time.sleep(3)
                
                # Extract cookies from Selenium
                selenium_cookies = driver.get_cookies()
                self.cookies = {}
                
                for cookie in selenium_cookies:
                    self.cookies[cookie['name']] = cookie['value']
                
                self.logger.info(f"Extracted {len(self.cookies)} cookies from website")
                
                # Use fixed headers with the CSRF token from notebook
                self.headers = {
                    'accept': 'application/json',
                    'accept-language': 'en-US,en;q=0.5',
                    'content-type': 'application/json; charset=UTF-8',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                    'x-csrftoken': 'T6C+9iB49TLra4jEsMeSckDMNhQ=',  # Fixed CSRF token from notebook
                }
                
                self.logger.info("Headers configured with fixed CSRF token")
                
                # Setup the JSON template
                if not self._setup_json_template():
                    self.logger.error("Failed to setup JSON template")
                    return False
                
                return True
                
            finally:
                driver.quit()
                
        except Exception as e:
            self.logger.error(f"Error getting cookies and headers: {e}")
            return False
    
    def _setup_selenium_driver(self) -> Optional[webdriver.Chrome]:
        """Setup Selenium Chrome driver with appropriate options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Use webdriver-manager to automatically get the correct ChromeDriver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return driver
            
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome driver: {e}")
            return None

    def _setup_json_template(self) -> bool:
        """Setup JSON data template with current date"""
        try:
            # Setup JSON data template with current date
            current_date = datetime.now()
            from_date = current_date.strftime('%Y-%m-%d')
            # Calculate end date (6 days from now)
            import calendar
            year, month = current_date.year, current_date.month
            last_day = calendar.monthrange(year, month)[1]
            end_day = min(current_date.day + 6, last_day)
            if current_date.day + 6 > last_day:
                # Move to next month
                if month == 12:
                    year += 1
                    month = 1
                else:
                    month += 1
                end_day = (current_date.day + 6) - last_day
            end_date = datetime(year, month, end_day)
            to_date = end_date.strftime('%Y-%m-%d')
            
            self.json_data_template = {
                'versionInfo': {
                    'moduleVersion': 'B0uImgq__reT+bwbX8Eg_Q',
                    'apiVersion': 'bYh0SIb+kuEKWPesnQKP1A',
                },
                'viewName': 'MainFlow.ProductListPage',
                'screenData': {
                    'variables': {
                        'AppliedFiltersList': {
                            'List': [],
                            'EmptyListItem': {
                                'Name': '',
                                'Quantity': '0',
                                'IsSelected': False,
                                'URL': '',
                            },
                        },
                        'LocalCategoryID': 0,
                        'LocalCategoryName': '',
                        'LocalCategoryParentId': 0,
                        'LocalCategoryTitle': '',
                        'IsLoadingMore': True,
                        'IsFirstDataFetched': True,
                        'ShowFilters': False,
                        'IsShowData': True,
                        'StoreNumber': 0,
                        'StoreChannel': '',
                        'CheckoutId': '',
                        'IsOrderEditMode': False,
                        'ProductList_All': {
                            'List': []
                        },
                        'PageNumber': 1,
                        'SelectedSort': '&tn_sort=Sorteeroptie%3A%20populariteit-zoekresultaten-ecop-v2',
                        'OrderEditId': '',
                        'IsListRendered': True,
                        'IsAlreadyFetch': True,
                        'IsPromotionBannersFetched': False,
                        'Period': {
                            'FromDate': from_date,
                            'ToDate': to_date,
                        },
                        'UserStoreId': '0',
                        'FilterExpandedList': {
                            'List': [False] * 9,
                        },
                        'ItemsInCart': {
                            'List': [],
                            'EmptyListItem': {
                                'LineItemId': '',
                                'SKU': '',
                                'MainCategory': {
                                    'Name': '',
                                    'Webkey': '',
                                    'OrderHint': '0',
                                },
                                'Quantity': 0,
                                'Name': '',
                                'Subtitle': '',
                                'Brand': '',
                                'Image': {
                                    'Label': '',
                                    'URL': '',
                                },
                                'ItemTypeAttributeId': '',
                                'DepositFee': '0',
                                'Slug': '',
                                'ChannelId': '',
                                'Promotion': {
                                    'BasedLabel': '',
                                    'Label': '',
                                    'StampURL': '',
                                    'NewPrice': '0',
                                    'IsFreeDelivery': False,
                                },
                                'IsNIX18': False,
                                'Price': '0',
                                'MaxOrderLimit': 0,
                                'QuantityOfFreeProducts': 0,
                            },
                        },
                        'HideDummy': False,
                        'OneWelcomeUserId': '',
                        '_oneWelcomeUserIdInDataFetchStatus': 1,
                        'CategorySlug': '',
                        '_categorySlugInDataFetchStatus': 1,
                        'SearchKeyword': '',
                        '_searchKeywordInDataFetchStatus': 1,
                        'IsDesktop': True,
                        '_isDesktopInDataFetchStatus': 1,
                        'IsSearch': False,
                        '_isSearchInDataFetchStatus': 1,
                        'URLPageNumber': 0,
                        '_uRLPageNumberInDataFetchStatus': 1,
                        'FilterQueryURL': '',
                        '_filterQueryURLInDataFetchStatus': 1,
                        'IsMobile': False,
                        '_isMobileInDataFetchStatus': 1,
                        'IsTablet': False,
                        '_isTabletInDataFetchStatus': 1,
                        'Monitoring_FlowTypeId': 3,
                        '_monitoring_FlowTypeIdInDataFetchStatus': 1,
                        'IsCustomerUnderAge': False,
                        '_isCustomerUnderAgeInDataFetchStatus': 1,
                    },
                },
            }
            
            self.logger.info("JSON template configured successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up JSON template: {e}")
            return False
            
            self.logger.info(f"Successfully set up {len(self.cookies)} cookies with fixed CSRF token")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup cookies and headers: {e}")
            return False
    
    def _fetch_page(self, page_number: int) -> List[Dict]:
        """Fetch a single page of products"""
        try:
            # Create request data for this page
            json_data = copy.deepcopy(self.json_data_template)
            json_data['screenData']['variables']['PageNumber'] = page_number
            
            # Make request using the exact same pattern as the notebook
            response = requests.post(
                self.api_url,
                cookies=self.cookies,
                headers=self.headers,
                json=json_data,
                timeout=30
            )
            
            self.logger.debug(f"Page {page_number} request status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'ProductList' in data['data']:
                    products = data['data']['ProductList']['List']
                    self.logger.debug(f"Page {page_number}: {len(products)} products")
                    return products
                else:
                    self.logger.warning(f"Page {page_number}: Unexpected response structure")
                    self.logger.debug(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    return []
            elif response.status_code == 403:
                self.logger.error(f"Page {page_number}: Access forbidden - check cookies and headers")
                return []
            else:
                self.logger.error(f"Page {page_number}: HTTP {response.status_code}")
                self.logger.debug(f"Response text: {response.text[:200]}...")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching page {page_number}: {e}")
            return []
    
    def _get_total_pages(self) -> int:
        """Get total number of pages available"""
        try:
            # Make a request to get pagination info
            json_data = copy.deepcopy(self.json_data_template)
            json_data['screenData']['variables']['PageNumber'] = 1
            
            response = requests.post(
                self.api_url,
                cookies=self.cookies,
                headers=self.headers,
                json=json_data,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    total_pages = data['data'].get('TotalPages', 0)
                    total_items = data['data'].get('TotalNumberItems', 0)
                    self.logger.info(f"Total pages: {total_pages}, Total items: {total_items}")
                    return total_pages
            else:
                self.logger.error(f"Failed to get pagination info: HTTP {response.status_code}")
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error getting total pages: {e}")
            return 0
    
    def _parse_product(self, product_data: Dict) -> Optional[Product]:
        """Parse a single product from API response"""
        try:
            # Extract product data from PLP_Str nested structure
            plp_data = product_data.get('PLP_Str', {})
            if not plp_data:
                return None
            
            # Extract basic product information
            product_id = plp_data.get('SKU', '')
            name = plp_data.get('Name', '').strip()
            brand = plp_data.get('Brand', '').strip()
            subtitle = plp_data.get('Product_Subtitle', '').strip()
            
            if not product_id or not name:
                return None
            
            # Combine name with brand and subtitle for full name
            full_name = name
            if brand and brand.lower() not in name.lower():
                full_name = f"{brand} {name}"
            if subtitle:
                full_name = f"{full_name} {subtitle}"
            
            # Extract category from Categories list
            category_name = "Unknown"
            categories = plp_data.get('Categories', {}).get('List', [])
            if categories:
                # Use the first (most general) category
                category_name = categories[0].get('Name', 'Unknown')
            
            # Extract price information
            original_price_str = plp_data.get('OriginalPrice', '0')
            new_price_str = plp_data.get('NewPrice', '0.0')
            
            try:
                price = float(original_price_str)
            except (ValueError, TypeError):
                price = 0.0
            
            if price <= 0:
                return None
            
            # Extract promotion information
            original_price = None
            discount_type = None
            promotion_label = plp_data.get('PromotionLabel', '')
            
            try:
                new_price = float(new_price_str)
                if new_price > 0 and new_price < price:
                    original_price = price
                    price = new_price
                    discount_type = promotion_label if promotion_label else 'Discount'
            except (ValueError, TypeError):
                pass
            
            # Calculate unit information from packaging info
            unit_amount = "1 stuk"
            unit_type = UnitType.PIECE
            price_per_unit = price
            
            # Try to extract unit information from Product_Subtitle
            packaging = plp_data.get('Packging', '')  # Note: API has typo 'Packging'
            if subtitle or packaging:
                unit_info = self._extract_unit_info(subtitle or packaging)
                if unit_info:
                    unit_amount, unit_type, price_per_unit = unit_info
                    if price_per_unit == 0:
                        price_per_unit = price  # Fallback
            
            # Create search tags
            search_tags = self._create_search_tags(full_name, brand, category_name)
            
            # Get image URL and availability
            image_url = plp_data.get('ImageURL', '')
            is_available = plp_data.get('IsAvailable', True)
            
            # Skip unavailable products
            if not is_available:
                return None
            
            return Product(
                product_id=product_id,
                name=full_name,
                category_name=category_name,
                price=price,
                unit_amount=unit_amount,
                price_per_unit=price_per_unit,
                unit_type=unit_type,
                supermarket_code=self.supermarket_code,
                search_tags=search_tags,
                original_price=original_price,
                discount_type=discount_type,
                discount_start_date=None,  # Plus doesn't provide date fields in API
                discount_end_date=None,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing product: {e}")
            return None
    
    def _extract_unit_info(self, subtitle: str) -> Optional[tuple]:
        """Extract unit information from product subtitle"""
        try:
            # Common patterns for Dutch unit information
            import re
            
            # Pattern for kg/gram
            kg_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(kg|kilo)', subtitle.lower())
            if kg_match:
                amount = float(kg_match.group(1).replace(',', '.'))
                return f"{amount} kg", UnitType.KG, 0  # price_per_unit will be calculated later
            
            # Pattern for grams
            g_match = re.search(r'(\d+)\s*g\b', subtitle.lower())
            if g_match:
                amount = int(g_match.group(1))
                return f"{amount} g", UnitType.GRAM, 0
            
            # Pattern for liters
            l_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(l|liter)', subtitle.lower())
            if l_match:
                amount = float(l_match.group(1).replace(',', '.'))
                return f"{amount} l", UnitType.LITER, 0
            
            # Pattern for ml
            ml_match = re.search(r'(\d+)\s*ml', subtitle.lower())
            if ml_match:
                amount = int(ml_match.group(1))
                return f"{amount} ml", UnitType.ML, 0
            
            # Pattern for pieces/stuks
            pieces_match = re.search(r'(\d+)\s*(stuk|stuks|st\.)', subtitle.lower())
            if pieces_match:
                amount = int(pieces_match.group(1))
                return f"{amount} stuks", UnitType.PIECE, 0
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting unit info from '{subtitle}': {e}")
            return None
    
    def _create_search_tags(self, name: str, brand: str, category: str) -> str:
        """Create search tags for the product"""
        tags = []
        
        if brand:
            tags.append(brand)
        if category and category != "Unknown":
            tags.append(category)
        
        # Add cleaned name words
        name_words = [word for word in name.lower().split() if len(word) > 2]
        tags.extend(name_words[:5])  # Limit to first 5 meaningful words
        
        return ", ".join(set(tags))
    
    def scrape_products(self) -> List[Product]:
        """Main scraping method using concurrent requests"""
        products = []
        
        # Get cookies and setup session
        if not self._get_cookies_and_headers():
            self.logger.error("Failed to get cookies and headers")
            return products
        
        # Get total pages
        total_pages = self._get_total_pages()
        if total_pages == 0:
            self.logger.error("Could not determine total pages")
            return products
        
        self.logger.info(f"Starting to scrape {total_pages} pages with {self.max_workers} workers")
        
        # Create list of all page numbers
        page_numbers = list(range(1, total_pages + 1))
        
        # Process pages in batches using concurrent requests
        batch_size = 50  # Process 50 pages at a time
        for i in range(0, len(page_numbers), batch_size):
            batch_pages = page_numbers[i:i + batch_size]
            
            self.logger.info(f"Processing batch {i//batch_size + 1}/{(len(page_numbers)-1)//batch_size + 1}: pages {batch_pages[0]}-{batch_pages[-1]}")
            
            # Use ThreadPoolExecutor for concurrent requests
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all pages in this batch
                future_to_page = {executor.submit(self._fetch_page, page_num): page_num 
                                for page_num in batch_pages}
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        page_products = future.result()
                        
                        # Parse each product
                        for product_data in page_products:
                            product = self._parse_product(product_data)
                            if product:
                                products.append(product)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing page {page_num}: {e}")
            
            # Add a small delay between batches to be respectful
            if i + batch_size < len(page_numbers):
                time.sleep(0.5)
            
            # Log progress
            self.logger.info(f"Scraped {len(products)} products so far")
        
        self.logger.info(f"Scraping completed: {len(products)} products total")
        return products


def main():
    """Test the Plus scraper"""
    # Database configuration (adjust as needed)
    db_config = {
        'host': 'localhost',
        'database': 'supermarket_db',
        'user': 'root',
        'password': 'password'
    }
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('plus_scraper.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Create database manager and scraper
        with DatabaseManager(db_config) as db_manager:
            scraper = PlusScraper(db_manager)
            
            # Run the scraper
            products_count = scraper.run()
            print(f"Successfully scraped {products_count} products from Plus")
            
    except Exception as e:
        logging.error(f"Scraping failed: {e}")


if __name__ == "__main__":
    main()
