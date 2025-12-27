"""
Generic Offer Scrapers for remaining supermarkets
These are simplified scrapers that focus on the basic structure
"""
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
from base_scraper import BaseScraper
from database import Product

# Import the Dekamarkt offers scraper
from .dekamarkt_offers import DekamarktOfferScraper

# Import the Hoogvliet offers scraper
from .hoogvliet_offers import HoogvlietOfferScraper

# Import the Lidl offers scraper
from .lidl_offers import LidlOfferScraper


# Import the proper PlusOffersScraper from plus_offers.py
from Supermarkets.offers.plus_offers import PlusOffersScraper as PlusAPIOfferScraper

class PlusOfferScraper(PlusAPIOfferScraper):
    """Scraper for Plus offers using the API-based implementation"""

    def __init__(self, db_manager):
        # Call the parent constructor from plus_offers.py
        super().__init__(db_manager)


# Use the optimized DekamarktOfferScraper from the dedicated module
# (No need to redefine here)

# Use the optimized HoogvlietOfferScraper from the dedicated module
# (No need to redefine here)