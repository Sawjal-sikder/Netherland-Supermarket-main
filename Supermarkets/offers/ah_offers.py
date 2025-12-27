"""
AH Offer Scraper
Scrapes offers from https://www.ah.nl/bonus using GraphQL API
"""
import requests
import logging
import json
from datetime import datetime
from typing import List, Optional, Dict, Any

# Selenium imports for cookie initialization
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base_scraper import BaseScraper
from database import Product


class AHOfferScraper(BaseScraper):
    """Scraper for AH.nl bonus/offer page using GraphQL API"""

    BASE_URL = "https://www.ah.nl"
    GRAPHQL_URL = "https://www.ah.nl/gql"
    BONUS_URL = "https://www.ah.nl/bonus"

    def __init__(self, db_manager):
        super().__init__(db_manager, "AH")
        self.driver = None
        self._setup_session()

    def _setup_session(self):
        """Setup requests session with proper headers"""
        self.session.headers.update({
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.7',
            'client-name': 'ah-bonus',
            'client-version': '3.544.14',
            'content-type': 'application/json',
            'origin': self.BASE_URL,
            'referer': self.BONUS_URL,
            'sec-ch-ua': '"Not;A=Brand";v="99", "Brave";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        })

    def scrape_products(self) -> List[Product]:
        """Scrape products from AH bonus page using GraphQL API"""
        try:
            # Initialize cookies first
            if not self._initialize_cookies():
                self.logger.error("Failed to initialize cookies")
                return []

            # Fetch and process promotions
            promotions = self._fetch_bonus_promotions()
            return self._process_promotions(promotions)
        except Exception as e:
            self.logger.error(f"Failed to scrape AH offers: {e}")
            return []
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None

    def _initialize_cookies(self) -> bool:
        """Initialize cookies using Selenium browser"""
        try:
            self.logger.info("Initializing cookies with Selenium")

            # Setup Chrome options
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')

            # Initialize Chrome driver (Selenium will auto-download compatible version)
            self.driver = webdriver.Chrome(options=chrome_options)

            # Navigate to main page to get cookies
            self.driver.get(self.BASE_URL)

            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Navigate to offers page to ensure we have the right cookies
            self.driver.get(self.BONUS_URL)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Extract cookies and add them to our session
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])

            self.logger.info("Cookies initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize cookies: {e}")
            if self.driver:
                self.driver.quit()
                self.driver = None
            return False

    def _fetch_bonus_promotions(self) -> List[Dict[str, Any]]:
        """Fetch bonus promotions from AH GraphQL API"""
        # Calculate current week number and period
        from datetime import datetime, timedelta
        today = datetime.now()
        week_number = today.isocalendar()[1]

        # Get Monday of current week as period start
        period_start = today - timedelta(days=today.weekday())
        period_end = period_start + timedelta(days=6)

        payload = {
            "operationName": "bonusCategories",
            "variables": {
                "input": {
                    "weekNumber": week_number,
                    "periodStart": period_start.strftime("%Y-%m-%d"),
                    "periodEnd": period_end.strftime("%Y-%m-%d")
                }
            },
            "query": """
            query bonusCategories($input: PromotionSearchInput) {
              bonusCategories(filterSet: WEB_CATEGORIES, input: $input) {
                id
                title
                type
                promotions {
                  ...promotion
                  __typename
                }
                __typename
              }
            }

            fragment promotion on Promotion {
              id
              title
              subtitle
              category
              exampleText
              storeOnly
              productCount
              salesUnitSize
              webPath
              exceptionRule
              promotionType
              segmentType
              periodDescription
              periodStart
              periodEnd
              extraDescriptions
              activationStatus
              promotionLabels {
                topText
                centerText
                bottomText
                emphasis
                title
                variant
                __typename
              }
              images {
                url
                title
                width
                height
                __typename
              }
              price {
                label
                now {
                  amount
                  __typename
                }
                was {
                  amount
                  __typename
                }
                __typename
              }
              __typename
            }
            """
        }

        response = self.session.post(self.GRAPHQL_URL, json=payload)

        if response.status_code != 200:
            self.logger.error(f"GraphQL API returned {response.status_code}")
            return []

        data = response.json()
        promotions = []

        bonus_categories = data.get('data', {}).get('bonusCategories', [])
        if not isinstance(bonus_categories, list):
            self.logger.error("bonusCategories is not a list")
            return []

        for category in bonus_categories:
            if not isinstance(category, dict):
                self.logger.warning(f"Skipping invalid category: {type(category)}")
                continue

            category_promotions = category.get('promotions', [])
            if isinstance(category_promotions, list):
                promotions.extend(category_promotions)
            else:
                self.logger.warning(f"Invalid promotions data in category: {type(category_promotions)}")

        self.logger.info(f"Fetched {len(promotions)} promotions from AH")
        return promotions

    def _process_promotions(self, promotions: List[Dict[str, Any]]) -> List[Product]:
        """Process GraphQL promotions into Product objects"""
        products = []

        for i, promotion in enumerate(promotions):
            try:
                if not isinstance(promotion, dict):
                    self.logger.warning(f"Skipping invalid promotion at index {i}: {type(promotion)}")
                    continue

                product = self._create_product_from_promotion(promotion)
                if product:
                    products.append(product)
            except Exception as e:
                promotion_id = promotion.get('id', 'unknown') if isinstance(promotion, dict) else 'invalid'
                self.logger.error(f"Failed to process promotion {promotion_id}: {e}")
                continue

        # Apply product limit if specified
        if self.product_limit and len(products) > self.product_limit:
            products = products[:self.product_limit]

        self.logger.info(f"Processed {len(products)} products from promotions")
        return products

    def _create_product_from_promotion(self, promotion: Dict[str, Any]) -> Optional[Product]:
        """Create a Product object from GraphQL promotion data"""
        try:
            # Extract basic information
            product_id = str(promotion.get('id', ''))
            title = promotion.get('title', '')
            subtitle = promotion.get('subtitle', '')

            if not product_id or not title:
                return None

            # Use title + subtitle for full name
            full_name = title
            if subtitle:
                full_name += f" - {subtitle}"

            # Extract pricing information
            price_info = promotion.get('price', {})
            if not isinstance(price_info, dict) or price_info is None:
                self.logger.debug(f"Invalid price_info: {price_info}")
                return None

            current_price = None
            original_price = None

            if price_info and 'now' in price_info and price_info['now'] and isinstance(price_info['now'], dict):
                try:
                    current_price = float(price_info['now'].get('amount', 0))
                except (ValueError, TypeError):
                    current_price = None

            if price_info and 'was' in price_info and price_info['was'] and isinstance(price_info['was'], dict):
                try:
                    original_price = float(price_info['was'].get('amount', 0))
                except (ValueError, TypeError):
                    original_price = None

            # Skip if no current price
            if not current_price or current_price <= 0:
                return None

            # Calculate discount type
            discount_type = None
            if original_price and original_price > current_price:
                discount_percentage = round(((original_price - current_price) / original_price) * 100, 1)
                discount_type = f"{discount_percentage}% korting"

            # Extract discount period dates
            discount_start_date = None
            discount_end_date = None

            period_start_str = promotion.get('periodStart')
            period_end_str = promotion.get('periodEnd')

            if period_start_str:
                try:
                    # Parse ISO date string (e.g., "2025-08-25")
                    discount_start_date = datetime.fromisoformat(period_start_str.replace('Z', '+00:00'))
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Failed to parse periodStart '{period_start_str}': {e}")

            if period_end_str:
                try:
                    # Parse ISO date string (e.g., "2025-08-31")
                    discount_end_date = datetime.fromisoformat(period_end_str.replace('Z', '+00:00'))
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Failed to parse periodEnd '{period_end_str}': {e}")

            # Extract category
            category = promotion.get('category', 'Offers')
            if not isinstance(category, str) or not category:
                category = 'Offers'

            # Extract unit information
            unit_amount = promotion.get('salesUnitSize', '1 stuk')
            if not isinstance(unit_amount, str):
                unit_amount = '1 stuk'

            # Generate URL
            web_path = promotion.get('webPath', '')
            if web_path:
                url = f"{self.BASE_URL}{web_path}"
            else:
                url = f"{self.BASE_URL}/bonus"

            # Extract image URL
            image_url = ''
            images = promotion.get('images', [])
            if images and isinstance(images, list) and len(images) > 0:
                first_image = images[0]
                if isinstance(first_image, dict):
                    image_url = first_image.get('url', '')
                elif isinstance(first_image, str):
                    image_url = first_image
            
            # Fallback to other image fields if available
            if not image_url:
                image_url = promotion.get('imageUrl', '') or promotion.get('image', '')

            return self._create_product(
                product_id=product_id,
                name=full_name,
                category=category,
                price=current_price,
                unit_amount=unit_amount,
                original_price=original_price,
                discount_type=discount_type,
                discount_start_date=discount_start_date,
                discount_end_date=discount_end_date,
                image_url=image_url
            )

        except Exception as e:
            self.logger.error(f"Failed to create product from promotion: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None