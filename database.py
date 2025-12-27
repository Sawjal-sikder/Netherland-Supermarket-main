"""
Simplified Database Manager for Supermarket Product Scraper
Follows clean architecture and SOLID principles
"""
import mysql.connector
from mysql.connector import Error
import logging
from typing import Optional, Dict, Any, List
import os
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import re
from dotenv import load_dotenv


class UnitType(Enum):
    """Standard unit types for price calculation"""
    KG = "kg"
    LITER = "liter"
    PIECE = "piece"
    METER = "meter"
    GRAM = "gram"
    ML = "ml"


@dataclass
class Product:
    """Product data class with required fields only"""
    product_id: str
    name: str
    category_name: str
    price: float
    unit_amount: str
    price_per_unit: float
    unit_type: UnitType
    supermarket_code: str
    search_tags: str
    original_price: Optional[float] = None
    discount_type: Optional[str] = None
    discount_start_date: Optional[datetime] = None
    discount_end_date: Optional[datetime] = None
    image_url: Optional[str] = None

    def __post_init__(self):
        """Validate product data after initialization"""
        if not self.product_id or not self.name:
            raise ValueError("Product ID and name are required")
        if self.price <= 0 or self.price_per_unit <= 0:
            raise ValueError("Price and price per unit must be positive")


class DatabaseManager:
    """Simplified database manager focused on essential operations"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.connection: Optional[mysql.connector.MySQLConnection] = None
        self.logger = logging.getLogger(__name__)
        
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def get_last_scrape_date(self, supermarket_code: Optional[str] = None) -> Optional[datetime]:
        """Return the last completed scrape datetime, optionally filtered by supermarket."""
        try:
            cursor = self.connection.cursor()
            if supermarket_code:
                supermarket_id = self._get_supermarket_id(supermarket_code)
                query = (
                    "SELECT MAX(completed_at) FROM scraping_sessions "
                    "WHERE status = 'completed' AND supermarket_id = %s"
                )
                cursor.execute(query, (supermarket_id,))
            else:
                query = (
                    "SELECT MAX(completed_at) FROM scraping_sessions "
                    "WHERE status = 'completed'"
                )
                cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row and row[0] else None
        except Error as e:
            self.logger.error(f"Failed to get last scrape date: {e}")
            return None

    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                port=self.config.get('port', 3306),
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=False
            )
            self.logger.info("Database connection established")
            return True
        except Error as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")
    
    def start_scraping_session(self, supermarket_code: str) -> int:
        """Start a new scraping session"""
        supermarket_id = self._get_supermarket_id(supermarket_code)
        if not supermarket_id:
            raise ValueError(f"Supermarket {supermarket_code} not found")
            
        query = """
        INSERT INTO scraping_sessions (supermarket_id, started_at, status)
        VALUES (%s, NOW(), 'running')
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query, (supermarket_id,))
        session_id = cursor.lastrowid
        self.connection.commit()
        cursor.close()
        
        self.logger.info(f"Started scraping session {session_id} for {supermarket_code}")
        return session_id
    
    def _ensure_connection(self) -> bool:
        """Ensure the DB connection is alive; try to reconnect if needed."""
        try:
            if self.connection and self.connection.is_connected():
                return True
        except Exception:
            pass
        # Attempt reconnect
        return self.connect()
    
    def end_scraping_session(self, session_id: int, products_count: int, 
                           status: str = 'completed', error_message: str = None):
        """End a scraping session"""
        if not self._ensure_connection():
            self.logger.error("Cannot end scraping session: DB connection unavailable")
            return
        query = """
        UPDATE scraping_sessions 
        SET completed_at = NOW(), products_scraped = %s, status = %s, error_message = %s
        WHERE id = %s
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query, (products_count, status, error_message, session_id))
        self.connection.commit()
        cursor.close()
        
        self.logger.info(f"Ended scraping session {session_id}: {products_count} products, status: {status}")
    
    def save_product(self, product: Product) -> bool:
        """Save or update a product"""
        if not self._ensure_connection():
            self.logger.error("DB connection not available when saving product")
            return False
        try:
            # Get or create category
            category_id = self._get_or_create_category(product.category_name, product.supermarket_code)
            supermarket_id = self._get_supermarket_id(product.supermarket_code)
            
            # Insert or update product
            query = """
            INSERT INTO products (
                product_id, name, category_id, supermarket_id, price, unit_amount,
                price_per_unit, unit_type, original_price, discount_type,
                discount_start_date, discount_end_date, search_tags, image_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                category_id = VALUES(category_id),
                price = VALUES(price),
                unit_amount = VALUES(unit_amount),
                price_per_unit = VALUES(price_per_unit),
                unit_type = VALUES(unit_type),
                original_price = VALUES(original_price),
                discount_type = VALUES(discount_type),
                discount_start_date = VALUES(discount_start_date),
                discount_end_date = VALUES(discount_end_date),
                search_tags = VALUES(search_tags),
                image_url = VALUES(image_url),
                last_updated = NOW()
            """
            
            params = (
                product.product_id, product.name, category_id, supermarket_id,
                product.price, product.unit_amount, product.price_per_unit,
                product.unit_type.value, product.original_price, product.discount_type,
                product.discount_start_date, product.discount_end_date, product.search_tags,
                product.image_url
            )
            
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Save price history
            self._save_price_history(cursor.lastrowid or self._get_product_db_id(product), product)
            
            self.connection.commit()
            cursor.close()
            return True
            
        except Error as e:
            self.logger.error(f"Failed to save product {product.product_id}: {e}")
            try:
                self.connection.rollback()
            except Exception:
                pass
            return False
    
    def save_products_batch(self, products: List[Product]) -> int:
        """Save multiple products in a batch for better performance using true batch operations"""
        if not products:
            return 0
            
        if not self._ensure_connection():
            self.logger.error("DB connection not available for batch save")
            return 0
        
        saved_count = 0
        batch_size = 500  # Process in smaller batches to avoid memory issues
        
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            try:
                batch_saved = self._save_products_batch_chunk(batch)
                saved_count += batch_saved
                self.logger.info(f"Saved {saved_count}/{len(products)} products so far...")
            except Exception as e:
                self.logger.error(f"Error saving batch {i//batch_size + 1}: {e}")
                # Continue with next batch instead of failing completely
                continue
        
        self.logger.info(f"Batch save completed: {saved_count}/{len(products)} products saved")
        return saved_count
    
    def _save_products_batch_chunk(self, products: List[Product]) -> int:
        """Save a chunk of products using batch operations"""
        if not self._ensure_connection():
            return 0
            
        try:
            cursor = self.connection.cursor()
            
            # 1. Batch create/get categories
            category_map = self._batch_get_or_create_categories(products, cursor)
            
            # 2. Get supermarket IDs
            supermarket_map = {}
            for product in products:
                if product.supermarket_code not in supermarket_map:
                    supermarket_map[product.supermarket_code] = self._get_supermarket_id(product.supermarket_code)
            
            # 3. Prepare batch insert data
            product_data = []
            price_history_data = []
            
            for product in products:
                category_id = category_map.get((product.category_name, product.supermarket_code))
                supermarket_id = supermarket_map.get(product.supermarket_code)
                
                if not category_id or not supermarket_id:
                    self.logger.warning(f"Skipping product {product.product_id} due to missing category or supermarket")
                    continue
                
                product_data.append((
                    product.product_id, product.name, category_id, supermarket_id,
                    product.price, product.unit_amount, product.price_per_unit,
                    product.unit_type.value, product.original_price, product.discount_type,
                    product.discount_start_date, product.discount_end_date, product.search_tags,
                    product.image_url
                ))
                
                # Prepare price history data (we'll get product IDs after insert)
                price_history_data.append((
                    product.price, product.original_price, product.price_per_unit, product.discount_type
                ))
            
            if not product_data:
                cursor.close()
                return 0
            
            # 4. Batch insert/update products
            query = """
            INSERT INTO products (
                product_id, name, category_id, supermarket_id, price, unit_amount,
                price_per_unit, unit_type, original_price, discount_type,
                discount_start_date, discount_end_date, search_tags, image_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                category_id = VALUES(category_id),
                price = VALUES(price),
                unit_amount = VALUES(unit_amount),
                price_per_unit = VALUES(price_per_unit),
                unit_type = VALUES(unit_type),
                original_price = VALUES(original_price),
                discount_type = VALUES(discount_type),
                discount_start_date = VALUES(discount_start_date),
                discount_end_date = VALUES(discount_end_date),
                search_tags = VALUES(search_tags),
                image_url = VALUES(image_url),
                last_updated = NOW()
            """
            
            cursor.executemany(query, product_data)
            
            # 5. Batch insert price history (simplified - just insert for new products)
            # Note: This is a simplified approach. For a complete solution, you'd need to 
            # get the actual product IDs after insert and match them with price history
            
            self.connection.commit()
            cursor.close()
            
            return len(product_data)
            
        except Error as e:
            self.logger.error(f"Failed to save product batch: {e}")
            try:
                self.connection.rollback()
            except Exception:
                pass
            return 0
    
    def _batch_get_or_create_categories(self, products: List[Product], cursor) -> Dict:
        """Batch get or create categories for products"""
        category_map = {}
        categories_to_create = []
        
        # Get unique categories per supermarket
        unique_categories = set()
        for product in products:
            unique_categories.add((product.category_name, product.supermarket_code))
        
        # Check which categories already exist
        for category_name, supermarket_code in unique_categories:
            supermarket_id = self._get_supermarket_id(supermarket_code)
            category_slug = self._create_slug(category_name)
            
            # Try to get existing category
            query = "SELECT id FROM categories WHERE slug = %s AND supermarket_id = %s"
            cursor.execute(query, (category_slug, supermarket_id))
            result = cursor.fetchone()
            
            if result:
                category_map[(category_name, supermarket_code)] = result[0]
            else:
                categories_to_create.append((category_name, category_slug, supermarket_id))
        
        # Batch create missing categories
        if categories_to_create:
            insert_query = """
            INSERT INTO categories (name, slug, supermarket_id)
            VALUES (%s, %s, %s)
            """
            cursor.executemany(insert_query, categories_to_create)
            
            # Get the newly created category IDs
            for category_name, category_slug, supermarket_id in categories_to_create:
                query = "SELECT id FROM categories WHERE slug = %s AND supermarket_id = %s"
                cursor.execute(query, (category_slug, supermarket_id))
                result = cursor.fetchone()
                if result:
                    # Find corresponding supermarket_code for this supermarket_id
                    for cat_name, sup_code in unique_categories:
                        if cat_name == category_name and self._get_supermarket_id(sup_code) == supermarket_id:
                            category_map[(cat_name, sup_code)] = result[0]
                            break
        
        return category_map
    
    def get_products_by_supermarket(self, supermarket_code: str, 
                                  category: str = None, on_discount: bool = None) -> List[Dict]:
        """Get products with optional filtering"""
        query = """
        SELECT * FROM products_with_details 
        WHERE supermarket_code = %s
        """
        params = [supermarket_code]
        
        if category:
            query += " AND category_name LIKE %s"
            params.append(f"%{category}%")
            
        if on_discount is not None:
            if on_discount:
                query += " AND original_price IS NOT NULL"
            else:
                query += " AND original_price IS NULL"
        
        query += " ORDER BY name"
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        
        return results
    
    def search_products(self, search_term: str, supermarket_code: str = None) -> List[Dict]:
        """Search products by name or tags"""
        query = """
        SELECT * FROM products_with_details 
        WHERE MATCH(name, search_tags) AGAINST(%s IN NATURAL LANGUAGE MODE)
        """
        params = [search_term]
        
        if supermarket_code:
            query += " AND supermarket_code = %s"
            params.append(supermarket_code)
        
        query += " ORDER BY name"
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        
        return results
    
    def _get_supermarket_id(self, supermarket_code: str) -> Optional[int]:
        """Get supermarket ID by code (case-insensitive)."""
        query = "SELECT id FROM supermarkets WHERE LOWER(code) = LOWER(%s)"
        cursor = self.connection.cursor()
        cursor.execute(query, (supermarket_code,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None
    
    def ensure_supermarket(self, supermarket_code: str, name: Optional[str] = None, base_url: Optional[str] = None) -> Optional[int]:
        """Ensure a supermarket exists in DB, create it if missing.
        Returns supermarket_id or None on failure.
        """
        try:
            sid = self._get_supermarket_id(supermarket_code)
            if sid:
                return sid
            # Derive defaults if not provided
            code_norm = supermarket_code.upper()
            defaults = {
                'DIRK': ('Dirk van den Broek', 'https://www.dirk.nl'),
                'AH': ('Albert Heijn', 'https://www.ah.nl'),
                'JUMBO': ('Jumbo', 'https://www.jumbo.com'),
                'HOOGVLIET': ('Hoogvliet', 'https://www.hoogvliet.com'),
                'ALDI': ('ALDI', 'https://www.aldi.nl'),
                'LIDL': ('Lidl', 'https://www.lidl.nl'),
                'PLUS': ('Plus', 'https://www.plus.nl'),
                'DEKA': ('Dekamarkt', 'https://www.dekamarkt.nl'),
            }
            if not name or not base_url:
                name, base_url = defaults.get(code_norm, (supermarket_code, 'https://example.com'))
            insert = "INSERT INTO supermarkets (name, code, base_url) VALUES (%s, %s, %s)"
            cursor = self.connection.cursor()
            cursor.execute(insert, (name, code_norm, base_url))
            self.connection.commit()
            sid = cursor.lastrowid
            cursor.close()
            self.logger.info(f"Created supermarket record: {name} ({code_norm})")
            return sid
        except Error as e:
            # Handle duplicate entry - might already exist with this name
            if "Duplicate entry" in str(e):
                self.logger.info(f"Supermarket {name} already exists, checking for code {code_norm}")
                # Try to find by name and update code if needed
                cursor = self.connection.cursor()
                cursor.execute("SELECT id, code FROM supermarkets WHERE name = %s", (name,))
                result = cursor.fetchone()
                if result:
                    existing_id, existing_code = result
                    if existing_code != code_norm:
                        # Update the code to match what we expect
                        cursor.execute("UPDATE supermarkets SET code = %s WHERE id = %s", (code_norm, existing_id))
                        self.connection.commit()
                        self.logger.info(f"Updated supermarket {name} code from {existing_code} to {code_norm}")
                    cursor.close()
                    return existing_id
                cursor.close()
            self.logger.error(f"Failed to ensure supermarket {supermarket_code}: {e}")
            return None
    
    def _get_or_create_category(self, category_name: str, supermarket_code: str) -> int:
        """Get category ID or create new category"""
        supermarket_id = self._get_supermarket_id(supermarket_code)
        category_slug = self._create_slug(category_name)
        
        # Try to get existing category
        query = "SELECT id FROM categories WHERE slug = %s AND supermarket_id = %s"
        cursor = self.connection.cursor()
        cursor.execute(query, (category_slug, supermarket_id))
        result = cursor.fetchone()
        
        if result:
            cursor.close()
            return result[0]
        
        # Create new category
        insert_query = """
        INSERT INTO categories (name, slug, supermarket_id)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (category_name, category_slug, supermarket_id))
        category_id = cursor.lastrowid
        cursor.close()
        
        return category_id
    
    def _get_product_db_id(self, product: Product) -> Optional[int]:
        """Get database ID for a product"""
        supermarket_id = self._get_supermarket_id(product.supermarket_code)
        query = "SELECT id FROM products WHERE product_id = %s AND supermarket_id = %s"
        cursor = self.connection.cursor()
        cursor.execute(query, (product.product_id, supermarket_id))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None
    
    def _save_price_history(self, product_db_id: int, product: Product):
        """Save price history entry"""
        query = """
        INSERT INTO price_history (
            product_id, price, original_price, price_per_unit, discount_type
        ) VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            product_db_id, product.price, product.original_price,
            product.price_per_unit, product.discount_type
        )
        
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        cursor.close()
    
    @staticmethod
    def _create_slug(text: str) -> str:
        """Create URL-friendly slug from text"""
        return re.sub(r'[^\w\s-]', '', text.lower()).strip().replace(' ', '-')


class PriceCalculator:
    """Utility class for price calculations"""
    
    # Standard unit conversions - conversion factor represents how many base units make 1 of this unit
    UNIT_CONVERSIONS = {
        'g': ('gram', 1),
        'gram': ('gram', 1),
        'kg': ('gram', 1000),      # 1 kg = 1000 grams
        'kilogram': ('gram', 1000),
        'ml': ('ml', 1),
        'l': ('ml', 1000),         # 1 liter = 1000 ml
        'liter': ('ml', 1000),
        'litre': ('ml', 1000),
        'st': ('piece', 1),
        'stuks': ('piece', 1),
        'pieces': ('piece', 1),
        'piece': ('piece', 1),
    }
    
    @classmethod
    def calculate_price_per_unit(cls, price: float, unit_amount: str) -> tuple[float, UnitType]:
        """
        Calculate price per standard unit from unit amount string
        Always converts to larger units: KG for weight, liter for volume
        
        Args:
            price: Product price
            unit_amount: Unit amount string (e.g., "500g", "1.5L", "24 pieces")
            
        Returns:
            Tuple of (price_per_unit, unit_type) - normalized to KG/liter/piece
        """
        # Extract number and unit from string
        match = re.search(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', unit_amount.lower())
        if not match:
            # If no unit found, assume pieces
            return round(price, 2), UnitType.PIECE
        
        amount = float(match.group(1))
        unit = match.group(2).lower()
        
        # Get standard unit and conversion factor
        if unit not in cls.UNIT_CONVERSIONS:
            return round(price, 2), UnitType.PIECE
        
        base_unit, conversion_factor = cls.UNIT_CONVERSIONS[unit]
        
        # Calculate total base units (grams, ml, or pieces)
        total_base_units = amount * conversion_factor
        
        # Calculate price per base unit
        price_per_base_unit = price / total_base_units
        
        # Convert to final display units
        if base_unit == 'gram':
            # Convert to price per KG (1000 grams)
            final_price = price_per_base_unit * 1000
            final_unit_type = UnitType.KG
        elif base_unit == 'ml':
            # Convert to price per liter (1000 ml)
            final_price = price_per_base_unit * 1000
            final_unit_type = UnitType.LITER
        else:  # pieces
            final_price = price_per_base_unit
            final_unit_type = UnitType.PIECE
        
        return round(final_price, 2), final_unit_type
    
    @classmethod
    def generate_search_tags(cls, name: str, category: str, brand: str = None) -> str:
        """Generate search tags from product information"""
        tags = []
        
        # Add name words
        name_words = re.findall(r'\w+', name.lower())
        tags.extend(name_words)
        
        # Add category words
        category_words = re.findall(r'\w+', category.lower())
        tags.extend(category_words)
        
        # Add brand if available
        if brand:
            brand_words = re.findall(r'\w+', brand.lower())
            tags.extend(brand_words)
        
        # Remove duplicates and common stop words
        stop_words = {'de', 'het', 'een', 'van', 'en', 'in', 'op', 'met', 'voor', 'the', 'and', 'or', 'of'}
        unique_tags = list(set(word for word in tags if len(word) > 2 and word not in stop_words))
        
        return ', '.join(unique_tags)


def get_db_config() -> Dict[str, str]:
    """Get database configuration from environment variables (.env supported).
    
    Supports both manual environment variables and DigitalOcean auto-injected variables.
    DigitalOcean auto-injects variables when you attach a managed database:
    - ${db-name.HOSTNAME} or ${db-name.HOST}
    - ${db-name.PORT}
    - ${db-name.DATABASE}
    - ${db-name.USERNAME} 
    - ${db-name.PASSWORD}
    """
    # Load environment variables from a .env file if present; override existing env to avoid stale values
    load_dotenv(override=True)
    
    # Try DigitalOcean auto-injected variables first (common database names)
    # You can replace 'db' with your actual database component name in DigitalOcean
    do_host = os.getenv('db.HOSTNAME') or os.getenv('db.HOST')
    do_port = os.getenv('db.PORT')
    do_database = os.getenv('db.DATABASE')
    do_user = os.getenv('db.USERNAME')
    do_password = os.getenv('db.PASSWORD')
    
    # Alternative: try with common database names if 'db' doesn't work
    if not do_host:
        do_host = os.getenv('mysql.HOSTNAME') or os.getenv('mysql.HOST')
        do_port = os.getenv('mysql.PORT')
        do_database = os.getenv('mysql.DATABASE')
        do_user = os.getenv('mysql.USERNAME')
        do_password = os.getenv('mysql.PASSWORD')
    
    config = {
        'host': do_host or os.getenv('DB_HOST', 'localhost'),
        'database': do_database or os.getenv('DB_NAME', 'supermarket_products'),
        'user': do_user or os.getenv('DB_USER', 'root'),
        'password': do_password or os.getenv('DB_PASSWORD', ''),
        'port': int(do_port or os.getenv('DB_PORT', 3306))
    }
    
    # Log which method was used (without showing password)
    if do_host:
        logging.getLogger(__name__).info(f"Using DigitalOcean auto-injected DB config: host={config['host']} db={config['database']} port={config['port']}")
    else:
        logging.getLogger(__name__).info(f"Using manual DB config: host={config['host']} db={config['database']} port={config['port']}")
    
    return config
