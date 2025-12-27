"""
Dekamarkt Scraper
Sitemap + per-product GraphQL approach (mirrors notebook implementation) and
adheres to existing project architecture (BaseScraper -> Product models).

Only required core fields are persisted (matching other scrapers):
- product_id
- name
- category_name (mapped from department if available)
- price (current price: offerPrice if present else normalPrice)
- unit_amount (parsed from packaging / name)
- price_per_unit + unit_type (calculated by PriceCalculator)
- supermarket_code ("DEKA")
- search_tags (auto-generated; includes brand & category)
- original_price (normalPrice when discounted)
- discount_type (textPriceSign or computed percentage)

Incremental behaviour:
If a previous successful scrape session exists (scraping_sessions.completed_at), only
products whose <lastmod> in the sitemap is newer are scraped.
Falling back to full scrape if no prior session.
"""
from __future__ import annotations

import logging
import re
from typing import List, Optional, Dict, Any
from datetime import datetime

import requests

from base_scraper import BaseScraper
from database import Product, PriceCalculator  # noqa: F401  (import for side effects / typing)


class DekamarktScraper(BaseScraper):
    """Scraper for Dekamarkt using sitemap discovery + detailed GraphQL per product."""

    SUPERMARKET_CODE = "DEKA"
    SITEMAP_URL = "https://www.dekamarkt.nl/products-sitemap.xml"
    GRAPHQL_ENDPOINT = "https://web-deka-gateway.detailresult.nl/graphql"
    STORE_ID = 283  # Default store (matches notebook)
    API_KEY = "6d3a42a3-6d93-4f98-838d-bcc0ab2307fd"

    # GraphQL payload template (single product)
    # Insert product_id and store_id using format() when sending
    PRODUCT_QUERY_TEMPLATE = (
        '{"query":"query { product(productId: %s) { productId headerText brand packaging '
        'isWeightProduct maxAmount department webgroup additionalDescription description '
        'declarations { contactInformation { contactName contactAdress } nutritionalInformation { '
        'standardPackagingUnit soldOrPrepared nutritionalValues { text value nutritionalSubValues { text value } } } '
        'storageInstructions cookingInstructions instructionsForUse ingredients allergiesInformation { text } } '
        'logos { position description image } images { image rankNumber mainImage } '
        'productAssortment(storeId: %s) { productId storeId normalPrice offerPrice isSingleUsePlastic productNumber '
        'startDate endDate productOffer { productId textPriceSign endDate startDate disclaimerStartDate disclaimerEndDate } '
        'productInformation { productId headerText subText packaging image isWeightProduct department webgroup brand '
        'logos { position description image } } } } }","variables":{}}'
    )

    def __init__(self, db_manager):
        super().__init__(db_manager, self.SUPERMARKET_CODE)
        # Headers for both sitemap and GraphQL requests
        self.session.headers.update({
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.5",
            "content-type": "application/json",
            "origin": "https://www.dekamarkt.nl",
            "referer": "https://www.dekamarkt.nl/",
            "api_key": self.API_KEY,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        })
        self.logger: logging.Logger

    # ------------------------------- PUBLIC API ---------------------------------
    def scrape_products(self) -> List[Product]:  # noqa: D401
        """Scrape Dekamarkt products via sitemap + GraphQL (core required fields only)."""
        products: List[Product] = []

        # 1. Fetch sitemap
        sitemap_items = self._fetch_sitemap()
        if not sitemap_items:
            self.logger.warning("No sitemap items discovered – aborting")
            return products
        self.logger.info(f"Discovered {len(sitemap_items)} products in sitemap")

        # 2. Incremental filtering based on last successful scrape session
        # The database stores the completed_at timestamp for each successful scrape.
        # We compare product lastmod dates (from sitemap) with this timestamp.
        # Only products with lastmod > last_scrape_date are processed.
        last_scrape = self.db_manager.get_last_scrape_date(self.supermarket_code)
        if last_scrape:
            filtered = [p for p in sitemap_items if p["last_modified"] and p["last_modified"] > last_scrape]
            self.logger.info(
                f"Last scrape at {last_scrape}. {len(filtered)} products updated since then (of {len(sitemap_items)})."
            )
            if filtered:
                sitemap_items = filtered
            else:
                self.logger.info("No updated products since last scrape – nothing to do.")
                return products
        else:
            self.logger.info("No prior scrape – performing full scrape")

        # 3. Apply optional product limit (set by BaseScraper.run())
        if self.product_limit is not None:
            sitemap_items = sitemap_items[: self.product_limit]
            self.logger.info(f"Applying product limit: {len(sitemap_items)} items")

        # 4. Iterate products sequentially (can be optimized with concurrency later)
        for idx, item in enumerate(sitemap_items, start=1):
            product_id = item["product_id"]
            try:
                data = self._fetch_product_graphql(product_id)
                if not data:
                    continue
                product = self._parse_product_data(data, item)
                if product:
                    products.append(product)
            except Exception as e:  # pragma: no cover - defensive
                self.logger.error(f"Error processing product {product_id}: {e}")

            if idx % 50 == 0 or idx == len(sitemap_items):
                self.logger.info(f"Progress: {idx}/{len(sitemap_items)} products")

        return products

    # ------------------------------ SITEMAP LOGIC -------------------------------
    def _fetch_sitemap(self) -> List[Dict[str, Any]]:
        """Fetch and parse sitemap -> list of {url, product_id, last_modified}."""
        try:
            # Use separate headers for sitemap request (no api_key needed)
            sitemap_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1"
            }
            resp = self.session.get(self.SITEMAP_URL, headers=sitemap_headers, timeout=30)
            resp.raise_for_status()
            text = resp.text
        except Exception as e:
            self.logger.error(f"Failed to download sitemap: {e}")
            return []

        # Lightweight regex parsing (HTML entities not expected). Provides speed over full XML parse.
        url_blocks = re.findall(r"<url>(.*?)</url>", text, flags=re.DOTALL)
        items: List[Dict[str, Any]] = []
        for block in url_blocks:
            loc_match = re.search(r"<loc>(.*?)</loc>", block)
            if not loc_match:
                continue
            url = loc_match.group(1).strip()
            if "/producten/" not in url:
                continue
            # Product ID is trailing number
            id_match = re.search(r"/(\d+)$", url)
            if not id_match:
                continue
            product_id = id_match.group(1)
            # lastmod (optional)
            lastmod_match = re.search(r"<lastmod>(.*?)</lastmod>", block)
            last_modified_dt: Optional[datetime] = None
            if lastmod_match:
                raw_lastmod = lastmod_match.group(1).strip()
                try:
                    # Remove timezone offset for naive comparison if needed
                    cleaned = re.sub(r"[+-]\d{2}:\d{2}$", "", raw_lastmod)
                    last_modified_dt = datetime.fromisoformat(cleaned)
                except Exception:  # pragma: no cover
                    self.logger.debug(f"Could not parse lastmod: {raw_lastmod}")
            items.append({
                "url": url,
                "product_id": product_id,
                "last_modified": last_modified_dt,
            })
        return items


    # ------------------------------ GRAPHQL LOGIC -------------------------------
    def _fetch_product_graphql(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Perform GraphQL POST request -> product dict or None."""
        payload = self.PRODUCT_QUERY_TEMPLATE % (product_id, self.STORE_ID)
        try:
            resp = self.session.post(self.GRAPHQL_ENDPOINT, data=payload, timeout=30)
            resp.raise_for_status()
            json_data = resp.json()
            if json_data.get("errors"):
                self.logger.debug(f"GraphQL errors for {product_id}: {json_data['errors']}")
                return None
            product = json_data.get("data", {}).get("product")
            if not product:
                return None
            return product
        except Exception as e:
            self.logger.debug(f"Product {product_id} fetch failed: {e}")
            return None

    # ------------------------------ PARSING LOGIC -------------------------------
    def _parse_product_data(self, data: Dict[str, Any], meta: Dict[str, Any]) -> Optional[Product]:
        """Transform GraphQL product dict -> Product domain object."""
        try:
            product_id = str(data.get("productId"))
            name = data.get("headerText") or data.get("description")
            if not product_id or not name:
                return None

            brand = data.get("brand")
            department = data.get("department") or data.get("webgroup") or "Unknown"

            # Pricing logic
            assortment = data.get("productAssortment") or {}
            normal_price = assortment.get("normalPrice")
            offer_price = assortment.get("offerPrice")
            offer = assortment.get("productOffer") or {}

            # Extract discount dates
            discount_start_date = None
            discount_end_date = None
            
            if offer_price and normal_price and offer_price < normal_price:
                price = offer_price
                original_price = normal_price
                discount_type = offer.get("textPriceSign") or self._compute_discount_label(normal_price, offer_price)
                
                # Extract discount dates from offer
                start_date_str = offer.get("startDate") or assortment.get("startDate")
                end_date_str = offer.get("endDate") or assortment.get("endDate")
                
                if start_date_str:
                    try:
                        discount_start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).date()
                    except Exception:
                        self.logger.debug(f"Could not parse start date: {start_date_str}")
                        
                if end_date_str:
                    try:
                        discount_end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).date()
                    except Exception:
                        self.logger.debug(f"Could not parse end date: {end_date_str}")
            else:
                price = normal_price or offer_price  # fallback if only one present
                original_price = None
                discount_type = None

            if not price:
                return None

            # Unit amount (from packaging or name)
            unit_amount = self._extract_unit_amount_from_text(
                (data.get("packaging") or "") + " " + name
            )
            
            # Extract image URL from GraphQL response
            image_url = ''
            # Try to get image from images array
            images = data.get('images', [])
            if images and isinstance(images, list):
                # Find main image or use first image
                main_image = next((img for img in images if img.get('mainImage')), None)
                if main_image:
                    image_url = main_image.get('image', '')
                elif images:
                    image_url = images[0].get('image', '')
            
            # Fallback to direct image field if no images array
            if not image_url:
                image_url = data.get('image', '')

            return self._create_product(
                product_id=product_id,
                name=name,
                category=department,
                price=float(price),
                unit_amount=unit_amount,
                original_price=float(original_price) if original_price else None,
                discount_type=discount_type,
                brand=brand,
                discount_start_date=discount_start_date,
                discount_end_date=discount_end_date,
                image_url=image_url
            )
        except Exception as e:  # pragma: no cover - defensive
            self.logger.error(f"Parse error for product {meta.get('product_id')}: {e}")
            return None

    @staticmethod
    def _compute_discount_label(original: float, current: float) -> str:
        try:
            pct = round(((original - current) / original) * 100, 1)
            return f"{pct}% korting"
        except Exception:  # pragma: no cover
            return "Aanbieding"

    # --------------------------- UNIT EXTRACTION LOGIC --------------------------
    def _extract_unit_amount_from_text(self, text: str) -> str:
        if not text:
            return "1 piece"
        patterns = [
            r"(\d+\s*x\s*\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))",
            r"(\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|st|stuks|pieces?))",
        ]
        lowered = text.lower()
        for pattern in patterns:
            m = re.search(pattern, lowered)
            if m:
                return m.group(1).replace(",", ".")
        return "1 piece"


# Optional local test (will not save to DB) -------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _DummyDB:
        def get_last_scrape_date(self, *_args, **_kwargs):
            return None
        def start_scraping_session(self, *_a, **_k):
            return 0
        def end_scraping_session(self, *a, **k):
            pass
        def save_products_batch(self, products):
            return len(products)

    scraper = DekamarktScraper(_DummyDB())
    scraper.product_limit = 3
    products = scraper.scrape_products()
    for p in products:
        print(p)
