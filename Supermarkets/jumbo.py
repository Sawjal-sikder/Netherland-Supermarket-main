"""
Jumbo Supermarket Scraper

This module implements a scraper for Jumbo supermarket using their GraphQL API.
Jumbo uses a modern GraphQL-based API that returns structured product data.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base_scraper import BaseScraper
from database import DatabaseManager, Product, PriceCalculator, UnitType
from datetime import datetime
import time
import logging


class JumboScraper(BaseScraper):
    """Scraper for Jumbo supermarket using GraphQL API"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, "jumbo")
        self.base_url = "https://www.jumbo.com/api"
        self.graphql_url = f"{self.base_url}/graphql"
        
        # Headers for API requests
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.6',
            'apollographql-client-name': 'JUMBO_WEB-search',
            'apollographql-client-version': 'master-v17.1.0-web',
            'content-type': 'application/json',
            'origin': 'https://www.jumbo.com',
            'priority': 'u=1, i',
            'referer': 'https://www.jumbo.com/producten/?offSet=24',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'x-source': 'JUMBO_WEB-search'
        }
        
        # Cookies for session (these may need periodic updates)
        self.cookies = {
            'country': 'NL',
            'AKA_A2': 'A',
            'akaas_as': '2147483647~rv=69~id=07661a79d369db894b41d018f17845bb~rn=',
            'SSLB': '1',
            'SSSC_2rnq': '1003.G7533186252926324986.1|88709.2772980:88734.2773386:89112.2783663:90721.2817433',
            'i18n_redirected': 'nl-NL',
            'fep_jodpuid': 'J706874165-1753956607',
            'fep_jodpsid': '101173757-1753958407118',
            'sid': 'agE-XNxqyMQpXLThKwFF779gYZhNvNfzGeGTZjva',
            'pgid-Jumbo-Grocery-Site': 'DYql6yV1CQ1SRpUuB1PyiAht0000fO5Hx7O8',
            'user-session': '89a91f70-6df6-11f0-8038-b18c11edbef5'
        }
        
        self.page_size = 200  # Optimal page size - API max limit appears to be 200
    
    def create_search_payload(self, offset=0, limit=None):
        """Create GraphQL payload for product search"""
        if limit is None:
            limit = self.page_size
            
        return {
            "operationName": "SearchProducts",
            "variables": {
                "input": {
                    "searchType": "category",
                    "searchTerms": "producten",
                    "friendlyUrl": f"?offSet={offset}&limit={limit}",
                    "offSet": offset,
                    "limit": limit,
                    "currentUrl": f"/producten/?offSet={offset}&limit={limit}",
                    "previousUrl": "",
                    "bloomreachCookieId": ""
                }
            },
            "query": """query SearchProducts($input: ProductSearchInput!) {
                searchProducts(input: $input) {
                    start
                    count
                    products {
                        id
                        brand
                        category
                        subtitle
                        title
                        image
                        inAssortment
                        availability {
                            availability
                            isAvailable
                            label
                            stockLimit
                            reason
                            availabilityNote
                            __typename
                        }
                        sponsored
                        auctionId
                        link
                        retailSet
                        prices: price {
                            price
                            promoPrice
                            pricePerUnit {
                                price
                                unit
                                __typename
                            }
                            __typename
                        }
                        quantityDetails {
                            maxAmount
                            minAmount
                            stepAmount
                            defaultAmount
                            __typename
                        }
                        primaryBadge: primaryProductBadges {
                            alt
                            image
                            __typename
                        }
                        secondaryBadges: secondaryProductBadges {
                            alt
                            image
                            __typename
                        }
                        customerAllergies {
                            short
                            __typename
                        }
                        promotions {
                            id
                            group
                            isKiesAndMix
                            image
                            tags {
                                text
                                inverse
                                __typename
                            }
                            start {
                                dayShort
                                date
                                monthShort
                                __typename
                            }
                            end {
                                dayShort
                                date
                                monthShort
                                __typename
                            }
                            attachments {
                                type
                                path
                                __typename
                            }
                            primaryBadge: primaryBadges {
                                alt
                                image
                                __typename
                            }
                            volumeDiscounts {
                                discount
                                volume
                                __typename
                            }
                            durationTexts {
                                shortTitle
                                __typename
                            }
                            maxPromotionQuantity
                            url
                            __typename
                        }
                        surcharges {
                            type
                            value {
                                amount
                                currency
                                __typename
                            }
                            __typename
                        }
                        characteristics {
                            freshness {
                                name
                                value
                                url
                                __typename
                            }
                            logo {
                                name
                                value
                                url
                                __typename
                            }
                            tags {
                                url
                                name
                                value
                                __typename
                            }
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
            }"""
        }
    
    def fetch_page(self, offset=0, limit=None):
        """Fetch a single page of products"""
        try:
            payload = self.create_search_payload(offset=offset, limit=limit)
            
            response = self.session.post(
                self.graphql_url,
                headers=self.headers,
                cookies=self.cookies,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data'] and 'searchProducts' in data['data']:
                    return data['data']['searchProducts']
                else:
                    print(f"Unexpected response structure or null data: {data}")
                    return None
            else:
                print(f"HTTP {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching page at offset {offset}: {e}")
            return None
    
    def parse_product(self, product_data):
        """Parse individual product data into Product object"""
        try:
            # Basic info
            product_id = product_data.get('id', '')
            name = product_data.get('title', '').strip()
            brand = product_data.get('brand', '').strip() if product_data.get('brand') else ''
            category = product_data.get('category', '').strip()
            # Default to "Unknown" if category is empty
            if not category:
                category = "Unknown"
            subtitle = product_data.get('subtitle')
            subtitle = subtitle.strip() if subtitle else ''
            
            # Skip products with empty title - these are invalid/unavailable products
            if not name:
                self.logger.warning(f"Skipping product {product_id}: empty title (subtitle: '{subtitle}', brand: '{brand}')")
                return None
            
            # Create full name with subtitle if available
            if subtitle and subtitle not in name:
                full_name = f"{name} {subtitle}".strip()
            else:
                full_name = name
            
            # Price information
            prices = product_data.get('prices', {})
            current_price = prices.get('price')
            promo_price = prices.get('promoPrice')
            
            # Convert prices from cents to euros with type safety
            try:
                price = float(current_price) / 100 if current_price is not None else None
            except (ValueError, TypeError):
                price = None
                
            original_price = price  # Default to current price
            
            # If there's a promo price, the promo price becomes current price
            # and the regular price becomes original price
            if promo_price is not None:
                try:
                    original_price = price
                    price = float(promo_price) / 100
                except (ValueError, TypeError):
                    # Keep original price if promo_price is invalid
                    pass
            
            # Price per unit
            price_per_unit_info = prices.get('pricePerUnit', {})
            price_per_unit = None
            unit_type = UnitType.PIECE
            unit_size = None
            
            if price_per_unit_info:
                unit_price = price_per_unit_info.get('price')
                unit = price_per_unit_info.get('unit', '').lower()
                if unit_price:
                    try:
                        # Convert unit price from cents to euros (except for pieces)
                        if unit != 'pieces':
                            price_per_unit = float(unit_price) / 100
                        else:
                            price_per_unit = float(unit_price)
                    except (ValueError, TypeError):
                        price_per_unit = None
                    
                    # Determine unit type
                    if unit in ['kg', 'kilogram']:
                        unit_type = UnitType.KG
                    elif unit in ['g', 'gram']:
                        unit_type = UnitType.GRAM
                    elif unit in ['l', 'liter', 'litre']:
                        unit_type = UnitType.LITER
                    elif unit in ['ml', 'milliliter']:
                        unit_type = UnitType.ML
                    else:
                        unit_type = UnitType.PIECE
            
            # Extract unit size from subtitle if available
            unit_amount = subtitle if subtitle else "1 stuk"
            
            # Availability
            availability_info = product_data.get('availability', {})
            in_stock = availability_info.get('isAvailable', False)
            in_assortment = product_data.get('inAssortment', False)
            availability = availability_info.get('availability', '')
            
            # Image and link
            image_url = product_data.get('image', '')
            product_link = product_data.get('link', '')
            if product_link and not product_link.startswith('http'):
                product_link = f"https://www.jumbo.com{product_link}"
            
            # Promotions
            promotions = product_data.get('promotions', [])
            on_sale = len(promotions) > 0 and promo_price is not None
            
            promotion_text = None
            discount_start_date = None
            discount_end_date = None
            
            if promotions:
                promo = promotions[0]  # Take first promotion
                tags = promo.get('tags', [])
                if tags:
                    promotion_text = tags[0].get('text', '')
                
                # Add duration if available
                duration = promo.get('durationTexts', {}).get('shortTitle', '')
                if duration:
                    promotion_text = f"{promotion_text} ({duration})" if promotion_text else duration
                
                # Extract promotion start and end dates
                start_info = promo.get('start', {})
                end_info = promo.get('end', {})
                
                if start_info and start_info.get('date'):
                    try:
                        # Date appears to be in DD format, need to construct full date
                        # We'll use the monthShort and current year for now
                        start_day = start_info.get('date')
                        start_month = start_info.get('monthShort')
                        
                        # Map Dutch month abbreviations to numbers
                        month_map = {
                            'jan': 1, 'feb': 2, 'mrt': 3, 'apr': 4, 'mei': 5, 'jun': 6,
                            'jul': 7, 'aug': 8, 'sep': 9, 'okt': 10, 'nov': 11, 'dec': 12
                        }
                        
                        if start_month and start_day and start_month.lower() in month_map:
                            current_year = datetime.now().year
                            discount_start_date = datetime(current_year, month_map[start_month.lower()], int(start_day))
                    except (ValueError, TypeError) as e:
                        self.logger.debug(f"Could not parse promotion start date: {e}")
                
                if end_info and end_info.get('date'):
                    try:
                        end_day = end_info.get('date')
                        end_month = end_info.get('monthShort')
                        
                        if end_month and end_day and end_month.lower() in month_map:
                            current_year = datetime.now().year
                            discount_end_date = datetime(current_year, month_map[end_month.lower()], int(end_day))
                    except (ValueError, TypeError) as e:
                        self.logger.debug(f"Could not parse promotion end date: {e}")
            
            # Badges and characteristics
            badges = []
            primary_badge = product_data.get('primaryBadge', [])
            secondary_badges = product_data.get('secondaryBadges', [])
            
            if primary_badge:
                for badge in primary_badge:
                    alt_text = badge.get('alt', '')
                    if alt_text:
                        badges.append(alt_text)
            
            if secondary_badges:
                for badge in secondary_badges:
                    alt_text = badge.get('alt', '')
                    if alt_text:
                        badges.append(alt_text)
            
            characteristics = product_data.get('characteristics', {})
            tags = characteristics.get('tags')
            if tags and isinstance(tags, dict) and 'value' in tags:
                badges.append(tags['value'])
            
            # Create search tags from available info
            search_tags = []
            if brand:
                search_tags.append(brand)
            if category:
                search_tags.append(category)
            if badges:
                search_tags.extend(badges)
            search_tags_str = ', '.join(search_tags) if search_tags else ''
            
            # Handle promotions for discount info
            discount_type = None
            if on_sale and promotion_text:
                discount_type = promotion_text
            
            # Validate price before creating product
            if price is None or price <= 0:
                self.logger.warning(f"Skipping product {product_id} ({full_name}): invalid price {price}")
                return None
            
            return Product(
                product_id=product_id,
                name=full_name,
                category_name=category,
                price=price,
                unit_amount=unit_amount,
                price_per_unit=price_per_unit or 0.0,
                unit_type=unit_type,
                supermarket_code=self.supermarket_code,
                search_tags=search_tags_str,
                original_price=original_price,
                discount_type=discount_type,
                discount_start_date=discount_start_date,
                discount_end_date=discount_end_date,
                image_url=image_url
            )
            
        except ValueError as ve:
            # Product validation failed (e.g., empty name, invalid price)
            self.logger.warning(f"Product validation failed for {product_data.get('id', 'unknown')}: {ve}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing product {product_data.get('id', 'unknown')}: {e}")
            self.logger.debug(f"Problem product data: {product_data}")
            return None
    
    def scrape_products(self):
        """Main method to scrape all products"""
        all_products = []
        offset = 0
        
        self.logger.info("Starting Jumbo product scraping...")
        
        while True:
            self.logger.info(f"Fetching products from offset {offset}")
            
            # Fetch page
            page_data = self.fetch_page(offset=offset)
            if not page_data:
                self.logger.error(f"Failed to fetch data at offset {offset}")
                break
            
            products = page_data.get('products', [])
            total_count = page_data.get('count', 0)
            
            self.logger.info(f"Received {len(products)} products (total available: {total_count})")
            
            if not products:
                self.logger.info("No more products found")
                break
            
            # Parse products
            parsed_count = 0
            skipped_count = 0
            for product_data in products:
                parsed_product = self.parse_product(product_data)
                if parsed_product:
                    all_products.append(parsed_product)
                    parsed_count += 1
                else:
                    skipped_count += 1
            
            self.logger.info(f"Parsed {parsed_count} products, skipped {skipped_count}. Total valid products: {len(all_products)}")
            
            # Check if we should continue
            offset += len(products)
            
            # Stop if we've reached the end
            if offset >= total_count:
                self.logger.info(f"Reached end of products (offset {offset} >= total {total_count})")
                break
            
            # Rate limiting to be respectful
            time.sleep(0.1)
        
        self.logger.info(f"Scraping completed. Total products: {len(all_products)}")
        return all_products


if __name__ == "__main__":
    # Test the scraper without database
    import requests
    
    # Create a mock database manager for testing
    class MockDBManager:
        pass
    
    # Create scraper with mock db manager
    scraper = JumboScraper(MockDBManager())
    
    print("=== Testing Jumbo Scraper ===")
    print("Fetching first page of products...")
    
    try:
        # Test fetching just one page with default page size
        page_data = scraper.fetch_page(offset=0)
        
        if page_data:
            products = page_data.get('products', [])
            total_count = page_data.get('count', 0)
            
            print(f"Successfully fetched {len(products)} products (total available: {total_count})")
            print()
            
            # Parse and display first few products
            for i, product_data in enumerate(products[:5], 1):
                parsed_product = scraper.parse_product(product_data)
                if parsed_product:
                    print(f"{i}. {parsed_product.name}")
                    print(f"   Category: {parsed_product.category_name}")
                    print(f"   Price: €{parsed_product.price:.2f}")
                    if parsed_product.original_price and parsed_product.original_price != parsed_product.price:
                        print(f"   Original Price: €{parsed_product.original_price:.2f}")
                    if parsed_product.price_per_unit and parsed_product.price_per_unit > 0:
                        print(f"   Price per unit: €{parsed_product.price_per_unit:.2f} per {parsed_product.unit_type.value}")
                    print(f"   Unit amount: {parsed_product.unit_amount}")
                    if parsed_product.discount_type:
                        print(f"   Discount: {parsed_product.discount_type}")
                    if parsed_product.search_tags:
                        print(f"   Tags: {parsed_product.search_tags}")
                    print()
                else:
                    print(f"{i}. Failed to parse product")
        else:
            print("Failed to fetch products")
            
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
    
    print("=== Test Complete ===")
