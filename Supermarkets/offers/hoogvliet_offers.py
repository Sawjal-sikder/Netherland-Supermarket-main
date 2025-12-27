"""
Hoogvliet Offers Scraper
Efficiently extracts offer products from Hoogvliet using their promotion API
"""

import requests
import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from base_scraper import BaseScraper
from database import Product
from .__init__ import DateParser, PriceValidator, DiscountCalculator


class HoogvlietOfferScraper(BaseScraper):
    """Optimized scraper for Hoogvliet promotional offers"""
    
    BASE_URL = "https://www.hoogvliet.com"
    OFFERS_API = f"{BASE_URL}/INTERSHOP/web/WFS/org-webshop-Site/nl_NL/-/EUR/ViewStandardCatalog-GetCategoriesForPromotionPage"
    
    def __init__(self, db_manager):
        super().__init__(db_manager, "HOOGVLIET")
        self._setup_session()
        
    def _setup_session(self) -> None:
        """Initialize session with proper headers and cookies"""
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Content-Length': '0',
            'Origin': 'https://www.hoogvliet.com',
            'Referer': 'https://www.hoogvliet.com/INTERSHOP/web/WFS/org-webshop-Site/nl_NL/-/EUR/ViewStandardCatalog-Browse?CategoryName=aanbiedingen&CatalogID=schappen',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Brave";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        })
        
        # Set session cookies from curl
        cookie_string = (
            'visid_incap_2265421=0s5tkGX/Qfi0uLQjBLmGbxwlk2gAAAAAQUIPAAAAAABg+27x8jfsHnM6SjzHQkRe; '
            'sid=qLlVwLiC3rJBwNA6ItAgwqOIlJ9UWjmRZEWhl6eRJt6DmQ==; '
            'pgid-org-webshop-Site=KRl889LzEzNSRpcOaUWVnNYE0000Tb72YwrT; '
            'SecureSessionID-Qu0KAyhz_A4AAAE9ketGw_RR=ebd65353ed4a1361dd2a6f880be7fc2133c49a63d1b28a0db476f1938c0fb76e; '
            'nlbi_2265421=iJUee/WGB1tCtAk9FSnXtwAAAAC44G4TFaFsoatD39sDxzAi; '
            'incap_ses_1687_2265421=sqrnL++vd0LQ7glC7W1pF2qKuGgAAAAA/vubqv9AvHhkBoyl6IWMCw=='
        )
        self._set_cookies(cookie_string)
        
    def _set_cookies(self, cookie_string: str) -> None:
        """Parse and set session cookies"""
        for cookie in cookie_string.split('; '):
            if '=' in cookie:
                name, value = cookie.split('=', 1)
                self.session.cookies.set(name.strip(), value.strip())
    
    def scrape_products(self) -> List[Product]:
        """Main scraping method for Hoogvliet offers"""
        self.logger.info("Starting Hoogvliet offers scraping")
        
        all_products = []
        page = 1
        max_pages = 50  # Reasonable upper limit to prevent infinite loops
        consecutive_empty_pages = 0
        max_consecutive_empty = 3  # Stop after 3 consecutive empty pages
        
        while page <= max_pages and consecutive_empty_pages < max_consecutive_empty:
            # Use appropriate page size based on page number (discovered from debugging)
            page_size = 10 if page == 1 else 2  # Page 1 works better with size 10, others with size 2
            
            # Use current promotion range (next 7 days starting from today)
            promotion_range = self._get_current_promotion_range()
            
            response_html = self._fetch_offers_page(page, page_size, promotion_range)
            if not response_html:
                consecutive_empty_pages += 1
                page += 1
                continue
                
            products = self._extract_products_from_html(response_html)
            if not products:
                consecutive_empty_pages += 1
                # Only try main offers page on first page if no products found
                if page == 1:
                    self.logger.info("No products on page 1, trying main offers page")
                    main_page_html = self._fetch_main_offers_page()
                    if main_page_html:
                        products = self._extract_products_from_html(main_page_html)
                        if products:
                            all_products.extend(products)
                            consecutive_empty_pages = 0
                page += 1
                continue
                
            # Found products, reset empty page counter
            consecutive_empty_pages = 0
            all_products.extend(products)
            self.logger.info(f"Page {page} (size {page_size}): Found {len(products)} offers (Total so far: {len(all_products)})")
            
            # Check if we've reached the limit AFTER logging total count
            if self.product_limit and len(all_products) >= self.product_limit:
                self.logger.info(f"Reached product limit of {self.product_limit}, stopping pagination")
                all_products = all_products[:self.product_limit]
                break
                
            page += 1
            
        self.logger.info(f"Total Hoogvliet offers found: {len(all_products)}")
        return all_products
    
    def _get_current_promotion_range(self) -> str:
        """Generate promotion range string for next 7 days starting from today"""
        today = datetime.now()
        end_date = today + timedelta(days=6)  # 7 days total (today + 6 more days)
        
        # Format like: "Aanbiedingen | 4 september - 10 september"
        month_names = {
            1: "januari", 2: "februari", 3: "maart", 4: "april", 5: "mei", 6: "juni",
            7: "juli", 8: "augustus", 9: "september", 10: "oktober", 11: "november", 12: "december"
        }
        
        start_month = month_names[today.month]
        end_month = month_names[end_date.month]
        
        range_str = f"Aanbiedingen | {today.day} {start_month} - {end_date.day} {end_month}"
        
        # URL encode the promotion range for API
        return range_str.replace(' ', '%2B').replace('|', '%257C')
    
    def _fetch_offers_page(self, page: int, page_size: int, promotion_range: str) -> Optional[str]:
        """Fetch offers HTML from API for specific page"""
        params = {
            'PageNumber': str(page),
            'PageSize': str(page_size),
            'LoadMoreProducts': '',
            'ListType': '',
            'PromotionRange': promotion_range,
            'TypeCode': '514'
        }
        
        try:
            response = self.session.post(self.OFFERS_API, params=params)
            response.raise_for_status()
            
            # Return the HTML content for parsing
            return response.text
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch offers page {page}: {e}")
            return None
    
    def _fetch_main_offers_page(self) -> Optional[str]:
        """Fetch the main offers page to get products directly"""
        main_offers_url = "https://www.hoogvliet.com/INTERSHOP/web/WFS/org-webshop-Site/nl_NL/-/EUR/ViewStandardCatalog-Browse?CategoryName=aanbiedingen&CatalogID=schappen"
        
        try:
            response = self.session.get(main_offers_url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch main offers page: {e}")
            return None
    
    def _extract_products_from_html(self, html_content: str) -> List[Product]:
        """Extract products from HTML response using BeautifulSoup"""
        products = []
        
        try:
            # Save a sample of the HTML for debugging
            if len(html_content) > 100:
                with open('hoogvliet_sample.html', 'w', encoding='utf-8') as f:
                    f.write(html_content[:5000])  # Save first 5000 characters
                self.logger.info(f"Saved HTML sample to hoogvliet_sample.html (first 5000 chars)")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find product items in the HTML - try different selectors
            product_items = soup.find_all('div', class_='product-list-item')
            self.logger.info(f"Found {len(product_items)} product items with class 'product-list-item'")
            
            # Try alternative selectors if no products found
            if not product_items:
                product_items = soup.find_all('div', class_=lambda x: x is not None and 'product' in str(x).lower())
                self.logger.info(f"Found {len(product_items)} items with 'product' in class name")
                
            if not product_items:
                # Look for any divs with tracking data
                product_items = soup.find_all('div', attrs={'data-track-click': True})
                self.logger.info(f"Found {len(product_items)} items with data-track-click attribute")
            
            for i, item in enumerate(product_items):
                product = self._parse_html_product_item(item)
                if product:
                    products.append(product)
                    self.logger.debug(f"Successfully parsed product {i+1}: {product.name}")
                else:
                    self.logger.debug(f"Failed to parse product {i+1}")
                    
        except Exception as e:
            self.logger.error(f"Error parsing HTML content: {e}")
            
        return products

    def _extract_date_from_tracking_data(self, track_data: dict) -> tuple[Optional[str], Optional[str]]:
        """Extract promotion start and end dates from tracking data."""
        try:
            # Look for date information in various tracking data fields
            for field in ['promotion_period', 'valid_until', 'discount_period', 'offer_dates']:
                if field in track_data and track_data[field]:
                    date_text = track_data[field]
                    return self._parse_date_range(date_text)
            
            # Check if there's a general date field
            if 'date' in track_data:
                date_text = track_data['date']
                return self._parse_date_range(date_text)
                
        except Exception as e:
            self.logger.warning(f"Error extracting dates from tracking data: {e}")
        
        return None, None

    def _parse_date_range(self, date_text: str) -> tuple[Optional[str], Optional[str]]:
        """Parse date range from text like '8 september - 14 september'."""
        try:
            from datetime import datetime
            
            # Dutch months mapping
            dutch_months = {
                'januari': 1, 'februari': 2, 'maart': 3, 'april': 4, 'mei': 5, 'juni': 6,
                'juli': 7, 'augustus': 8, 'september': 9, 'oktober': 10, 'november': 11, 'december': 12
            }
            
            # Look for pattern like "8 september - 14 september"
            import re
            pattern = r'(\d+)\s+(\w+)\s*-\s*(\d+)\s+(\w+)'
            match = re.search(pattern, date_text.lower())
            
            if match:
                start_day, start_month_name, end_day, end_month_name = match.groups()
                current_year = datetime.now().year
                
                start_month = dutch_months.get(start_month_name)
                end_month = dutch_months.get(end_month_name)
                
                if start_month and end_month:
                    start_date = datetime(current_year, start_month, int(start_day))
                    end_date = datetime(current_year, end_month, int(end_day))
                    
                    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
            
        except Exception as e:
            self.logger.warning(f"Error parsing date range '{date_text}': {e}")
        
        return None, None

    def _extract_dates_from_html(self, html_item) -> tuple[Optional[datetime], Optional[datetime]]:
        """Extract promotion dates from HTML content."""
        try:
            # Look for date text in the HTML
            date_text = ""
            
            # Check various elements that might contain date information
            date_elements = html_item.find_all(text=True)
            for text in date_elements:
                text_lower = str(text).lower().strip()
                # Look for text that contains month names and dates
                if any(month in text_lower for month in ['januari', 'februari', 'maart', 'april', 'mei', 'juni',
                                                        'juli', 'augustus', 'september', 'oktober', 'november', 'december']):
                    if '-' in text_lower and any(char.isdigit() for char in text_lower):
                        date_text = str(text).strip()
                        break
            
            if date_text:
                start_date_str, end_date_str = self._parse_date_range(date_text)
                if start_date_str and end_date_str:
                    try:
                        from datetime import datetime
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                        return start_date, end_date
                    except ValueError:
                        pass
                        
        except Exception as e:
            self.logger.debug(f"Error extracting dates from HTML: {e}")
        
        return None, None
    
    def _parse_html_product_item(self, item) -> Optional[Product]:
        """Parse individual product item from HTML"""
        try:
            # Extract tracking data which contains product information
            track_data = item.get('data-track-click')
            if track_data:
                try:
                    track_json = json.loads(track_data)
                    if 'products' in track_json and track_json['products']:
                        product_data = track_json['products'][0]
                        return self._create_product_from_track_data(product_data, item)
                except json.JSONDecodeError:
                    pass
            
            # Fallback: extract from HTML structure
            return self._create_product_from_html_structure(item)
            
        except Exception as e:
            self.logger.error(f"Error parsing product item: {e}")
            return None
    
    def _create_product_from_track_data(self, track_data: Dict[str, Any], html_item) -> Optional[Product]:
        """Create product from tracking data JSON"""
        try:
            product_id = str(track_data.get('id', '')).strip()
            name = str(track_data.get('name', '')).strip()
            price_str = str(track_data.get('price', '0.00')).strip()
            category = str(track_data.get('category', '')).strip()
            brand = str(track_data.get('brand', '')).strip()
            
            if not product_id or not name:
                return None
            
            # Parse price from tracking data
            price = None
            try:
                price = float(price_str)
            except (ValueError, TypeError):
                pass
                
            # If tracking price is 0 or invalid, try to extract from HTML
            if not price or price <= 0:
                price = self._extract_price_from_html(html_item)
                
            if not price or price <= 0:
                return None
            
            # Validate price
            if not PriceValidator.validate_price(str(price)):
                return None
            
            # Extract additional details from HTML
            original_price = self._extract_original_price_from_html(html_item)
            unit_amount = self._extract_unit_from_html(html_item)
            
            # Extract promotion dates from tracking data
            discount_start_date, discount_end_date = self._extract_date_from_tracking_data(track_data)
            
            # Convert string dates to datetime objects
            start_datetime = None
            end_datetime = None
            if discount_start_date:
                try:
                    from datetime import datetime
                    start_datetime = datetime.strptime(discount_start_date, '%Y-%m-%d')
                except ValueError:
                    pass
            if discount_end_date:
                try:
                    from datetime import datetime
                    end_datetime = datetime.strptime(discount_end_date, '%Y-%m-%d')
                except ValueError:
                    pass
            
            # Calculate discount if original price exists
            discount_percentage = None
            if original_price and original_price > price:
                discount_percentage = DiscountCalculator.calculate_discount(original_price, price)
            
            # Extract image URL from HTML
            image_url = ''
            img_tag = html_item.find('img')
            if img_tag:
                image_url = img_tag.get('src', '') or img_tag.get('data-src', '')
                if image_url and not image_url.startswith('http'):
                    image_url = self.BASE_URL + image_url if image_url.startswith('/') else ''
            
            # Clean category path
            if category and '/' in category:
                category = category.split('/')[-1].strip()
            if not category or category == "Aanbiedingen":
                category = "Offers"
                
            return self._create_product(
                product_id=product_id,
                name=name,
                category=category,
                price=price,
                unit_amount=unit_amount or "1 stuk",
                original_price=original_price,
                discount_type=f"{discount_percentage}% korting" if discount_percentage else "Aanbieding",
                brand=brand or "",
                discount_start_date=start_datetime,
                discount_end_date=end_datetime,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Error creating product from track data: {e}")
            return None
    
    def _create_product_from_html_structure(self, html_item) -> Optional[Product]:
        """Fallback: create product from HTML structure parsing"""
        try:
            # Extract product name from various selectors
            name_element = (
                html_item.find('h3') or 
                html_item.find('h4') or
                html_item.find(class_='product-name') or
                html_item.find(class_='product-title')
            )
            
            if not name_element:
                return None
                
            name = name_element.get_text(strip=True)
            if not name:
                return None
            
            # Extract price
            price_element = (
                html_item.find(class_='price') or
                html_item.find(class_='product-price') or
                html_item.find(class_='current-price')
            )
            
            if not price_element:
                return None
                
            price_text = price_element.get_text(strip=True)
            price = self._parse_price_from_text(price_text)
            
            if not price or price <= 0:
                return None
            
            # Generate a basic product ID from the name
            product_id = re.sub(r'[^a-zA-Z0-9]', '', name.lower())[:20]
            
            # Extract image URL
            image_url = ''
            img_tag = html_item.find('img')
            if img_tag:
                image_url = img_tag.get('src', '') or img_tag.get('data-src', '')
                if image_url and not image_url.startswith('http'):
                    image_url = self.BASE_URL + image_url if image_url.startswith('/') else ''
            
            # Try to extract dates from HTML content (look for date text)
            discount_start_date, discount_end_date = self._extract_dates_from_html(html_item)
            
            return self._create_product(
                product_id=product_id,
                name=name,
                category="Offers",
                price=price,
                unit_amount="1 stuk",
                original_price=None,
                discount_type="Aanbieding",
                brand="",
                discount_start_date=discount_start_date,
                discount_end_date=discount_end_date,
                image_url=image_url
            )
            
        except Exception as e:
            self.logger.error(f"Error creating product from HTML structure: {e}")
            return None
    
    def _extract_original_price_from_html(self, html_item) -> Optional[float]:
        """Extract original price from HTML if available"""
        try:
            # Look for crossed-out price or "was" price
            was_price_element = (
                html_item.find(class_='was-price') or
                html_item.find(class_='original-price') or
                html_item.find(class_='old-price') or
                html_item.find('del') or
                html_item.find('s')
            )
            
            if was_price_element:
                price_text = was_price_element.get_text(strip=True)
                return self._parse_price_from_text(price_text)
                
        except Exception:
            pass
            
        return None
    
    def _extract_price_from_html(self, html_item) -> Optional[float]:
        """Extract current price from HTML structure"""
        try:
            # Look for price elements in common selectors
            price_element = (
                html_item.find(class_='price') or
                html_item.find(class_='current-price') or
                html_item.find(class_='product-price') or
                html_item.find(class_='sale-price') or
                html_item.find('span', class_=re.compile('price')) or
                html_item.find('div', class_=re.compile('price'))
            )
            
            if price_element:
                price_text = price_element.get_text(strip=True)
                return self._parse_price_from_text(price_text)
                
        except Exception:
            pass
            
        return None
    
    def _extract_unit_from_html(self, html_item) -> Optional[str]:
        """Extract unit information from HTML if available"""
        try:
            # Look for unit information
            unit_element = (
                html_item.find(class_='unit') or
                html_item.find(class_='product-unit') or
                html_item.find(class_='quantity')
            )
            
            if unit_element:
                unit_text = unit_element.get_text(strip=True)
                if unit_text:
                    return unit_text
                    
        except Exception:
            pass
            
        return None
    
    def _parse_price_from_text(self, price_text: str) -> Optional[float]:
        """Parse price from text string"""
        try:
            # Remove currency symbols and clean up
            clean_price = re.sub(r'[€$£¥\s]', '', price_text)
            # Replace comma with dot for decimal separator
            clean_price = clean_price.replace(',', '.')
            return float(clean_price)
        except (ValueError, TypeError):
            return None
