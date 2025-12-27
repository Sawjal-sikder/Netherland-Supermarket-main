# Daily Offers Scraping

This repository now includes specialized offer scrapers that focus on daily deals and promotions from Dutch supermarkets.

## 🎯 Two Scraping Modes

### 1. Weekly Full Product Scraping (Existing)
- **Purpose**: Complete product catalog scraping
- **Frequency**: Weekly
- **Script**: `run_scrapers.py` or `main.py`
- **Target**: Product pages (all products)

### 2. Daily Offers Scraping (New)
- **Purpose**: Find new offers and promotions
- **Frequency**: Daily
- **Script**: `run_offers_scraper.py`
- **Target**: Offer/promotion pages

## 🏪 Supported Supermarkets for Offers

| Supermarket | Offer URL | Status |
|-------------|-----------|--------|
| Dirk | https://www.dirk.nl/aanbiedingen | ✅ Implemented |
| Albert Heijn | https://www.ah.nl/bonus | ✅ Implemented |
| Aldi | https://www.aldi.nl/aanbiedingen.html | ✅ Implemented |
| Jumbo | https://www.jumbo.com/aanbiedingen/nu | ✅ Implemented |
| Plus | https://www.plus.nl/aanbiedingen | ✅ Basic Implementation |
| Coop | https://www.coop.nl/aanbiedingen | ✅ Basic Implementation |
| Dekamarkt | https://www.dekamarkt.nl/aanbiedingen | 🔄 Structure Ready |
| Hoogvliet | Complex INTERSHOP URL | 🔄 Structure Ready |
| Lidl | https://www.lidl.nl/c/eten-en-drinken/s10068374 | 🔄 Structure Ready |

## 🚀 Usage

### Daily Offers Scraping

```bash
# Scrape offers from all supermarkets
python run_offers_scraper.py --supermarket all

# Scrape offers from specific supermarket
python run_offers_scraper.py --supermarket dirk

# Scrape offers from multiple supermarkets
python run_offers_scraper.py --multiple dirk ah aldi

# Limit products for testing
python run_offers_scraper.py --supermarket dirk --limit 10
```

### Weekly Full Product Scraping (Existing)

```bash
# Scrape all products from specific supermarket
python run_scrapers.py dirk

# Scrape with product limit
python run_scrapers.py dirk --product-limit 100
```

## 🏗️ Architecture

### Offer Scrapers Structure
```
Supermarkets/offers/
├── __init__.py
├── dirk_offers.py       # Dirk offer scraper
├── ah_offers.py         # AH offer scraper  
├── aldi_offers.py       # Aldi offer scraper
├── jumbo_offers.py      # Jumbo offer scraper
└── other_offers.py      # Plus, Coop, Dekamarkt, Hoogvliet, Lidl
```

### Key Design Principles

1. **Inheritance**: All offer scrapers inherit from `BaseScraper`
2. **Consistency**: Same data structure as regular product scrapers
3. **Focus**: Target only products with offers/discounts
4. **Efficiency**: Optimized for daily execution
5. **Scalability**: Easy to add new supermarkets

### Offer Detection Strategy

Each scraper implements offer detection using:
- **API Filters**: Using promotion/offer filters in API calls
- **Price Comparison**: Detecting original vs current price
- **Discount Indicators**: Looking for discount badges/labels
- **Category Filtering**: Focusing on offer categories

## 🔧 Configuration

The offer scrapers use the same database configuration as the main scrapers. Ensure your `.env` file contains:

```env
DB_HOST=your_host
DB_PORT=3306
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
```

## 📊 Data Collected

Offer scrapers collect the same data fields as regular scrapers but focus on products with:
- **Original Price**: Pre-discount price
- **Discount Type**: Type and percentage of discount
- **Promotion Period**: Start/end dates when available

## 🔄 Daily vs Weekly Workflow

### Recommended Daily Workflow
1. Run offer scrapers every morning: `python run_offers_scraper.py --supermarket all`
2. Focus on products with discounts and promotions
3. Quick execution for timely deal detection

### Recommended Weekly Workflow  
1. Run full product scrapers weekly: `python run_scrapers.py all`
2. Complete product catalog update
3. Comprehensive data for price tracking

## 📈 Benefits

- **Timely Deal Detection**: Find new offers within hours
- **Reduced Load**: Offer pages have fewer products than full catalogs
- **Better Performance**: Optimized for daily execution
- **Flexible Scheduling**: Different frequencies for different needs
- **Scalable Architecture**: Easy to extend to more supermarkets

## 🛠️ Development

To add a new supermarket's offer scraper:

1. Create a new class inheriting from `BaseScraper`
2. Implement `scrape_products()` method
3. Focus on offer/promotion pages
4. Add to `run_offers_scraper.py` orchestrator
5. Update documentation

Example:
```python
class NewSupermarketOfferScraper(BaseScraper):
    def __init__(self, db_manager):
        super().__init__(db_manager, "NEWSUPERMARKET")
        
    def scrape_products(self) -> List[Product]:
        # Implementation for offers scraping
        pass
```