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
        # Setup proper headers for API requests
        self._setup_headers()

    def _setup_headers(self):
        """Setup headers to mimic browser requests and avoid 403 errors"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://www.ah.nl/producten',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not-A.Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Priority': 'u=1, i',
        })

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
            
            # Setup Chrome options to better mimic real browser
            chrome_options = Options()
            
            # Auto-detect if we need headless mode (e.g., on servers without display)
            import os
            need_headless = os.environ.get('DISPLAY') is None or os.environ.get('AH_HEADLESS', 'false').lower() == 'true'
            
            if need_headless:
                self.logger.info("Running in HEADLESS mode (no display detected or AH_HEADLESS=true)")
                chrome_options.add_argument('--headless=new')  # Use new headless mode
            else:
                self.logger.warning("Running in NON-HEADLESS mode for better success rate. Browser window will be visible.")
            
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--start-maximized')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--dns-prefetch-disable')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            # Add prefs to appear more like regular browser
            chrome_options.add_experimental_option("prefs", {
                "profile.default_content_setting_values.notifications": 2,
                "credentials_enable_service": False,
                "profile.password_manager_enabled": False,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            # Initialize driver
            try:
                # Use webdriver-manager to automatically get the correct ChromeDriver
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                
                # Execute CDP command to hide webdriver property
                self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                    "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
                })
                
                # More comprehensive stealth script
                self.driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'nl']});
                    window.chrome = {runtime: {}};
                    Object.defineProperty(navigator, 'permissions', {
                        get: () => ({
                            query: () => Promise.resolve({state: 'granted'})
                        })
                    });
                """)
                
            except WebDriverException as e:
                self.logger.error(f"Failed to initialize Chrome driver: {e}")
                self.logger.info("Make sure ChromeDriver is installed and in PATH")
                return False
            
            # Visit AH homepage first
            self.logger.info("Visiting AH homepage...")
            self.driver.get(self.BASE_URL)
            
            # Wait for initial page load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Try to handle cookie consent popup with more attempts
            try:
                # Wait longer for popup to appear (OneTrust can be slow)
                time.sleep(5)
                
                # Try multiple strategies to find and click the consent button
                cookie_clicked = False
                
                # Strategy 1: Look for OneTrust cookie consent buttons (most common)
                onetrust_selectors = [
                    "//button[@id='onetrust-accept-btn-handler']",  # OneTrust accept all button
                    "//button[contains(@id, 'accept-recommended-btn')]",  # OneTrust recommended
                    "//button[@id='accept-all-cookies']",
                    "//button[contains(@class, 'onetrust-close-btn-handler')]",
                    "//button[contains(@class, 'cookie-accept-all')]",
                ]
                
                for selector in onetrust_selectors:
                    try:
                        cookie_button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, selector))
                        )
                        # Try to scroll to the element first
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", cookie_button)
                        time.sleep(1)
                        # Try regular click first
                        try:
                            cookie_button.click()
                        except:
                            # If regular click fails, try JavaScript click
                            self.driver.execute_script("arguments[0].click();", cookie_button)
                        
                        self.logger.info(f"✓ Clicked cookie consent button: {selector}")
                        time.sleep(3)  # Wait for cookies to be set
                        cookie_clicked = True
                        break
                    except TimeoutException:
                        continue
                    except Exception as e:
                        self.logger.debug(f"Failed to click {selector}: {e}")
                        continue
                
                # Strategy 2: Look for text-based buttons if OneTrust selectors don't work
                if not cookie_clicked:
                    text_selectors = [
                        "//button[contains(text(), 'Accepteren')]",
                        "//button[contains(text(), 'Akkoord')]",
                        "//button[contains(text(), 'Accept')]",
                        "//a[contains(text(), 'Accepteren')]",
                        "//a[contains(text(), 'Akkoord')]",
                    ]
                    
                    for selector in text_selectors:
                        try:
                            cookie_button = WebDriverWait(self.driver, 2).until(
                                EC.element_to_be_clickable((By.XPATH, selector))
                            )
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", cookie_button)
                            time.sleep(1)
                            try:
                                cookie_button.click()
                            except:
                                self.driver.execute_script("arguments[0].click();", cookie_button)
                            
                            self.logger.info(f"✓ Clicked cookie consent button: {selector}")
                            time.sleep(3)
                            cookie_clicked = True
                            break
                        except:
                            continue
                
                # Strategy 3: Try to find any button in a cookie banner
                if not cookie_clicked:
                    try:
                        # Look for common cookie banner containers
                        banner_selectors = [
                            "//div[@id='onetrust-banner-sdk']//button",
                            "//div[contains(@class, 'cookie-banner')]//button",
                            "//div[contains(@class, 'consent-banner')]//button",
                        ]
                        
                        for banner_selector in banner_selectors:
                            try:
                                buttons = self.driver.find_elements(By.XPATH, banner_selector)
                                if buttons:
                                    # Click the first button (usually "Accept All")
                                    button = buttons[0]
                                    self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                                    time.sleep(1)
                                    self.driver.execute_script("arguments[0].click();", button)
                                    self.logger.info(f"✓ Clicked cookie button in banner: {banner_selector}")
                                    time.sleep(3)
                                    cookie_clicked = True
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if not cookie_clicked:
                    self.logger.warning("⚠ Could not find or click cookie consent button")
                    self.logger.info("Attempting to set consent cookies manually...")
                    
                    # Manually set consent cookies if we can't click the button
                    try:
                        # Set OneTrust consent cookies manually
                        self.driver.add_cookie({
                            'name': 'OptanonAlertBoxClosed',
                            'value': '2025-12-27T09:00:00.000Z',
                            'domain': '.ah.nl',
                            'path': '/'
                        })
                        self.driver.add_cookie({
                            'name': 'OptanonConsent',
                            'value': 'isIABGlobal=false&datestamp=Fri+Dec+27+2025+09:00:00+GMT+0000&version=6.33.0&hosts=&consentId=&interactionCount=1&landingPath=NotLandingPage&groups=C0001:1,C0002:1,C0003:1,C0004:1&AwaitingReconsent=false',
                            'domain': '.ah.nl',
                            'path': '/'
                        })
                        self.logger.info("✓ Manually set consent cookies")
                    except Exception as e:
                        self.logger.warning(f"Could not set consent cookies manually: {e}")
                        
            except Exception as e:
                self.logger.warning(f"Error handling cookie consent: {e}")
            
            # Visit products page to ensure full session
            self.driver.get(f"{self.BASE_URL}/producten")
            time.sleep(8)  # Wait longer for page to fully load and JS to execute
            
            # Scroll page to trigger any lazy loading or tracking
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(3)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Wait for potential API calls to complete
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
            except:
                pass
            
            # Try to trigger the search API by opening network tab or making a search
            # This helps establish proper API session
            try:
                self.driver.execute_script("""
                    // Trigger any analytics or tracking that might be needed
                    if (window.dataLayer) {
                        console.log('DataLayer present');
                    }
                """)
            except:
                pass
            
            # Also visit sitemap page to ensure we have access to it
            try:
                self.logger.info("Visiting sitemap to ensure access...")
                self.driver.get(self.SITEMAP_URL)
                time.sleep(3)
                
                # Check if we can access it
                if "403" not in self.driver.page_source and "Forbidden" not in self.driver.page_source:
                    self.logger.info("✓ Sitemap is accessible")
                else:
                    self.logger.warning("⚠ Sitemap may be blocked (403 detected in page)")
            except Exception as e:
                self.logger.warning(f"Could not visit sitemap: {e}")
            
            # Extract cookies from Selenium and add to requests session
            selenium_cookies = self.driver.get_cookies()
            
            if not selenium_cookies:
                self.logger.warning("No cookies extracted from Selenium session!")
                self.logger.info("This may cause 403 errors. The site might be blocking automation.")
            
            # Clear existing cookies first
            self.session.cookies.clear()
            
            # Transfer all cookies including httpOnly ones
            cookie_count = 0
            for cookie in selenium_cookies:
                try:
                    # Set cookie with all available attributes
                    self.session.cookies.set(
                        cookie['name'], 
                        cookie['value'], 
                        domain=cookie.get('domain', '.ah.nl'),
                        path=cookie.get('path', '/'),
                        secure=cookie.get('secure', False)
                    )
                    cookie_count += 1
                except Exception as e:
                    self.logger.warning(f"Failed to transfer cookie {cookie.get('name')}: {e}")
            
            self.logger.info(f"Transferred {cookie_count} cookies from Selenium to requests")
            
            # Log all cookies for debugging
            if selenium_cookies:
                cookie_names = [c['name'] for c in selenium_cookies]
                self.logger.info(f"Cookie names: {', '.join(cookie_names)}")
            
            # Log key cookies for debugging
            important_cookies = ['anonymous-consents', 'OptanonConsent', 'OptanonAlertBoxClosed', 'ah-session', 'JSESSIONID']
            found_cookies = []
            missing_cookies = []
            for cookie_name in important_cookies:
                if cookie_name in self.session.cookies:
                    found_cookies.append(cookie_name)
                else:
                    missing_cookies.append(cookie_name)
            
            if found_cookies:
                self.logger.info(f"Found important cookies: {', '.join(found_cookies)}")
            if missing_cookies:
                self.logger.warning(f"Missing cookies: {', '.join(missing_cookies)}")
                
            
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
            # Update headers for sitemap request
            headers = self.session.headers.copy()
            headers.update({
                'Referer': f'{self.BASE_URL}/',
                'Accept': 'application/xml, text/xml, */*',
            })
            
            response = self.session.get(self.SITEMAP_URL, headers=headers, timeout=30)
            
            if response.status_code == 403:
                self.logger.warning("Sitemap returned 403, trying alternative category discovery...")
                return self._fetch_categories_alternative()
            
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
            self.logger.info("Trying alternative category discovery method...")
            return self._fetch_categories_alternative()

    def _fetch_categories_alternative(self) -> List[Dict[str, Any]]:
        """Alternative method to fetch categories using hardcoded list of main categories"""
        self.logger.info("Using hardcoded category list as fallback")
        
        # Common AH category taxonomy IDs and names (update these periodically)
        hardcoded_categories = [
            {'taxonomy_id': '6401', 'slug': 'groente-aardappelen', 'name': 'Groente Aardappelen'},
            {'taxonomy_id': '6402', 'slug': 'fruit-verse-sappen', 'name': 'Fruit Verse Sappen'},
            {'taxonomy_id': '6403', 'slug': 'vlees-kip-vis-vega', 'name': 'Vlees Kip Vis Vega'},
            {'taxonomy_id': '6404', 'slug': 'kaas-vleeswaren-delicatessen', 'name': 'Kaas Vleeswaren Delicatessen'},
            {'taxonomy_id': '6405', 'slug': 'zuivel-eieren', 'name': 'Zuivel Eieren'},
            {'taxonomy_id': '6406', 'slug': 'bakkerij-banket', 'name': 'Bakkerij Banket'},
            {'taxonomy_id': '6407', 'slug': 'diepvries', 'name': 'Diepvries'},
            {'taxonomy_id': '6408', 'slug': 'frisdrank-sappen-koffie-thee', 'name': 'Frisdrank Sappen Koffie Thee'},
            {'taxonomy_id': '6409', 'slug': 'pasta-rijst-wereldkeuken', 'name': 'Pasta Rijst Wereldkeuken'},
            {'taxonomy_id': '6410', 'slug': 'soepen-sauzen-kruiden', 'name': 'Soepen Sauzen Kruiden'},
            {'taxonomy_id': '6411', 'slug': 'snoep-koek-chips', 'name': 'Snoep Koek Chips'},
            {'taxonomy_id': '6412', 'slug': 'ontbijtgranen-beleg', 'name': 'Ontbijtgranen Beleg'},
            {'taxonomy_id': '6413', 'slug': 'baby-verzorging', 'name': 'Baby Verzorging'},
            {'taxonomy_id': '6414', 'slug': 'bewuste-voeding', 'name': 'Bewuste Voeding'},
        ]
        
        categories = []
        for cat in hardcoded_categories:
            categories.append({
                'taxonomy_id': cat['taxonomy_id'],
                'slug': cat['slug'],
                'name': cat['name'],
                'url': f"{self.BASE_URL}/producten/{cat['taxonomy_id']}/{cat['slug']}"
            })
        
        self.logger.info(f"Loaded {len(categories)} hardcoded categories")
        return categories

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
        
        # Update referer to the specific category page
        category_url = category.get('url', f"{self.BASE_URL}/producten")
        self.session.headers.update({
            'Referer': category_url,
        })
        
        # Remove Content-Type header for GET requests - browsers don't send it
        if 'Content-Type' in self.session.headers:
            del self.session.headers['Content-Type']
        
        while True:
            try:
                # Construct API URL
                api_url = (f"{self.API_BASE_URL}?page={page}&size={page_size}"
                          f"&taxonomySlug={category['slug']}&taxonomy={category['taxonomy_id']}")
                
                self.logger.info(f"Fetching page {page} for category {category['name']}")
                self.logger.debug(f"API URL: {api_url}")
                
                response = self.session.get(api_url, timeout=30)
                
                # Handle different response codes
                if response.status_code == 403:
                    self.logger.error(f"403 Forbidden - cookies may have expired or API access denied")
                    self.logger.error(f"Response headers: {dict(response.headers)}")
                    self.logger.error(f"Request URL: {api_url}")
                    
                    # Log current cookies
                    current_cookies = {k: v for k, v in self.session.cookies.items()}
                    self.logger.error(f"Current cookies: {list(current_cookies.keys())}")
                    
                    # Try to reinitialize cookies once per category
                    reinit_key = f'_reinit_attempted_{category["taxonomy_id"]}'
                    if not hasattr(self, reinit_key):
                        self.logger.info("Attempting to reinitialize cookies...")
                        setattr(self, reinit_key, True)
                        if self._initialize_cookies():
                            self.logger.info("Cookies reinitialized, retrying request...")
                            # Reset page to 1 after reinit
                            page = 1
                            continue
                    
                    self.logger.error("Skipping category due to persistent 403 errors")
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
