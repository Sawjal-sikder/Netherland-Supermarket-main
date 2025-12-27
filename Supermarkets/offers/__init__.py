"""
Offer-specific scrapers for Netherlands supermarkets

These scrapers focus on offer/promotion pages for daily scraping
to find new deals and discounts.
"""

import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from bs4 import BeautifulSoup


class NuxtDataExtractor:
    """Base class for extracting offer data from Nuxt.js __NUXT_DATA__ scripts"""
    
    @staticmethod
    def extract_nuxt_data(soup: BeautifulSoup) -> Optional[List[Any]]:
        """Extract and parse __NUXT_DATA__ script from BeautifulSoup object"""
        script = soup.find("script", {"id": "__NUXT_DATA__"})
        if not script:
            return None
        
        try:
            return json.loads(script.text)
        except json.JSONDecodeError:
            return None
    
    @staticmethod
    def resolve_reference(data: List[Any], value: Any) -> Any:
        """
        Resolve reference in Nuxt.js hydration data.
        If value is integer, it refers to index in data array.
        """
        if isinstance(value, int) and 0 <= value < len(data):
            return data[value]
        return value


class DateParser:
    """Utility class for parsing date strings"""
    
    @staticmethod
    def parse_date_string(date_string: str) -> Optional[datetime]:
        """Parse date string into datetime with multiple format support"""
        if not date_string:
            return None
        
        # Clean timezone suffixes
        cleaned = date_string.split('+')[0].split('Z')[0]
        
        formats = [
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        
        return None


class UnitExtractor:
    """Utility class for extracting unit amounts from text"""
    
    @staticmethod
    def extract_unit_amount(text: str) -> str:
        """Extract unit amount from product name or packaging text"""
        if not text:
            return "1 stuk"
        
        text_lower = text.lower()
        
        # Patterns for unit extraction
        patterns = [
            r'(\d+\s*x\s*\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))',
            r'(\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                return match.group(1).replace(",", ".")
        
        return "1 stuk"


class PriceValidator:
    """Utility class for validating and converting prices"""
    
    @staticmethod
    def validate_price(price_str: str, product_id: str = "") -> Optional[float]:
        """Convert price string to float and validate"""
        try:
            price = float(price_str) if price_str else 0.0
            return price if price > 0 else None
        except (ValueError, TypeError):
            return None


class DiscountCalculator:
    """Utility class for calculating discount information"""
    
    @staticmethod
    def calculate_discount(current_price: float, original_price: Optional[float]) -> Optional[str]:
        """Calculate discount percentage and return formatted string"""
        if not original_price or original_price <= current_price:
            return None
        
        discount_amount = original_price - current_price
        discount_percentage = (discount_amount / original_price) * 100
        return f"{discount_percentage:.1f}% korting"