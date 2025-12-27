# Netherlands Supermarket Scraper

A comprehensive Python scraping system for Netherlands supermarket websites. Built with clean architecture principles and focused on essential product data collection with support for both full product catalogs and daily offer tracking.

## 🎯 Features

- **Multi-Supermarket Support**: 9 major Dutch supermarkets (Dirk, AH, Aldi, Jumbo, Lidl, Plus, Coop, Dekamarkt, Hoogvliet)
- **Dual Scraping Modes**: Full product scraping (weekly) + Offer scraping (daily)
- **Essential Data Only**: Product name, category, price, unit amount, price per unit, supermarket, discounts, and search tags
- **Clean Architecture**: Object-oriented design with SOLID principles
- **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- **Database Integration**: MySQL storage with optimized schema
- **Easy Maintenance**: Well-documented, modular code structure
- **Flexible Scheduling**: Different frequencies for different scraping needs

## 🏪 Supported Supermarkets

| Supermarket | Product Scraping | Offer Scraping | Implementation Status |
|-------------|------------------|----------------|----------------------|
| Dirk van den Broek | ✅ Full | ✅ Offers | Fully Implemented |
| Albert Heijn | ✅ Full | ✅ Offers | Fully Implemented |
| Aldi | ✅ Full | ✅ Offers | Fully Implemented |
| Jumbo | ✅ Full | ✅ Offers | Fully Implemented |
| Lidl | ✅ Full | 🔄 Basic | Core Structure Ready |
| Plus | ✅ Full | ✅ Basic | Core Implementation |
| Coop | ❌ | ✅ Basic | Offers Only |
| Dekamarkt | ✅ Full | 🔄 Basic | Core Structure Ready |
| Hoogvliet | ✅ Full | 🔄 Basic | Core Structure Ready |

## 🔄 Two Scraping Modes

### 1. Weekly Full Product Scraping
- **Purpose**: Complete product catalog
- **Frequency**: Weekly
- **Command**: `python run_scrapers.py [supermarket]`
- **Target**: All products from main product pages

### 2. Daily Offers Scraping (NEW)
- **Purpose**: Find new deals and promotions  
- **Frequency**: Daily
- **Command**: `python run_offers_scraper.py --supermarket [name]`
- **Target**: Products on offer/promotion pages

## 🏗️ Architecture

### Core Components

```
├── database.py              # Database operations and data models
├── base_scraper.py          # Abstract base scraper class
├── run_scrapers.py          # Weekly full product scraping
├── run_offers_scraper.py    # Daily offers scraping (NEW)
├── config.py                # Configuration management
├── database_schema.sql      # Database schema
├── Supermarkets/            # Regular product scrapers
│   ├── dirk.py             # Dirk van den Broek scraper
│   ├── ah.py               # Albert Heijn scraper
│   ├── aldi.py             # Aldi scraper
│   ├── jumbo.py            # Jumbo scraper
│   └── ...                 # Other supermarket scrapers
└── Supermarkets/offers/     # Specialized offer scrapers (NEW)
    ├── dirk_offers.py      # Dirk offers scraper
    ├── ah_offers.py        # AH offers scraper
    ├── aldi_offers.py      # Aldi offers scraper
    ├── jumbo_offers.py     # Jumbo offers scraper
    └── other_offers.py     # Plus, Coop, Dekamarkt, etc.
```

### Design Patterns Used

- **Template Method Pattern**: Base scraper defines common workflow
- **Inheritance**: Offer scrapers extend regular scrapers
- **Context Manager**: Automatic database connection handling
- **Data Classes**: Type-safe product data structures
- **Factory Pattern**: Scraper initialization and configuration
- **Strategy Pattern**: Different scraping approaches per supermarket
- **Command Pattern**: CLI interface for different scraping modes

## 📊 Data Collected

For each product, the system collects:

- **Product Name**: Full product title
- **Category**: Product category classification
- **Price**: Current selling price
- **Unit Amount**: Package size/weight (e.g., "500g", "1 liter")
- **Price Per Unit**: Calculated price per unit (€/kg, €/liter, etc.)
- **Supermarket**: Store name
- **Discount Info**: Discount type and percentage (if applicable)
- **Search Tags**: Keywords for product discovery
- **Promotion Period**: Start/end dates for offers (when available)

## 🚀 Quick Start

### Daily Offers Scraping

```bash
# Scrape offers from all supermarkets
python run_offers_scraper.py --supermarket all

# Scrape offers from specific supermarket
python run_offers_scraper.py --supermarket dirk

# Scrape offers from multiple supermarkets
python run_offers_scraper.py --multiple dirk ah aldi

# Test with limited products
python run_offers_scraper.py --supermarket dirk --limit 10
```

### Weekly Full Product Scraping

```bash
# Scrape all products from specific supermarket
python run_scrapers.py dirk

# Scrape with product limit for testing
python run_scrapers.py dirk --product-limit 100
```

### 1. Prerequisites

- Python 3.8+
- MySQL Server
- Required Python packages (see requirements.txt)

### 2. Installation

```bash
# Clone repository
git clone <repository-url>
cd netherland-supermarket

# Install dependencies
pip install -r requirements.txt
```

### 3. Database Setup

```bash
# Create database and tables
mysql -u root -p < database_schema.sql
```

### 4. Configuration

```bash
# Create environment file
python config.py

# Edit .env with your database credentials
cp .env.example .env
# Edit .env file with your MySQL credentials
```

### 5. Run Scrapers

```bash
# Run all scrapers
python main_new.py

# Run specific scraper
python main_new.py dirk
python main_new.py ah

# Run multiple specific scrapers
python main_new.py dirk,ah
```

## 📋 Configuration

### Database Configuration

Edit `.env` file:

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=supermarket_products
DB_PORT=3306
```

### Scraping Configuration

Modify `config.py` for advanced settings:

```python
'scraping': {
    'request_timeout': 30,
    'retry_attempts': 3,
    'delay_between_requests': 1.0,
    'max_products_per_category': None,  # Limit for testing
    'log_level': 'INFO'
}
```

## 🛠️ Development

### Adding New Supermarket

1. Create new scraper class inheriting from `BaseScraper`:

```python
from base_scraper import BaseScraper

class NewSupermarketScraper(BaseScraper):
    def __init__(self, db_manager):
        super().__init__(db_manager, "NewSupermarket")
    
    def scrape_products(self) -> List[Product]:
        # Implement scraping logic
        pass
```

2. Add to `main_new.py` scraper initialization:

```python
('new_supermarket', NewSupermarketScraper, "New Supermarket")
```

### Database Schema

The simplified schema focuses on essential tables:

- `supermarkets`: Store information
- `categories`: Product categories
- `products`: Main product data
- `price_history`: Price tracking over time
- `scraping_sessions`: Scraping session logs

### Testing Individual Scrapers

Each scraper has a built-in test function:

```bash
# Test Dirk scraper
python dirk_new.py

# Test AH scraper
python ah_new.py
```

## 📈 Monitoring and Logs

### Log Files

Logs are stored in `logs/` directory with timestamp:
- `scraping_YYYYMMDD_HHMMSS.log`

### Database Monitoring

Check scraping sessions:

```sql
-- Recent scraping sessions
SELECT * FROM scraping_sessions ORDER BY created_at DESC LIMIT 10;

-- Products added today
SELECT COUNT(*) FROM products WHERE created_at >= CURDATE();

-- Price changes
SELECT * FROM price_history WHERE created_at >= CURDATE();
```

## 🔧 Troubleshooting

### Common Issues

**Database Connection Error**
```bash
# Check MySQL service
net start mysql

# Verify credentials in .env file
```

**Scraping Failures**
```bash
# Check logs for specific errors
tail -f logs/scraping_*.log

# Test individual scrapers
python dirk_new.py
```

**Missing Dependencies**
```bash
# Reinstall requirements
pip install -r requirements.txt --upgrade
```

### Performance Optimization

- Adjust `delay_between_requests` in config
- Set `max_products_per_category` for testing
- Use database indexes for faster queries
- Monitor memory usage during large scrapes

## 📚 API Reference

### Product Data Class

```python
@dataclass
class Product:
    product_id: str
    name: str
    category: str
    supermarket: str
    price: float
    unit_amount: str
    unit_type: UnitType
    price_per_unit: float
    discount_type: Optional[str] = None
    original_price: Optional[float] = None
    search_tags: str = ""
    image_url: Optional[str] = None
```

### Database Manager

```python
# Save product
db_manager.save_product(product)

# Create scraping session
session_id = db_manager.create_scraping_session()

# Update session results
db_manager.update_scraping_session(session_id, scraper, count, success)
```

## 🤝 Contributing

1. Follow existing code style and patterns
2. Add comprehensive error handling
3. Include logging for debugging
4. Update documentation for new features
5. Test thoroughly before committing

## 📄 License

This project is for educational and research purposes. Please respect the terms of service of the scraped websites.

## 🔄 Version History

### v2.0 (Current - Simplified)
- Complete architecture overhaul
- Focused on essential data only
- Clean OOP design with SOLID principles
- Improved error handling and logging
- Simplified database schema

### v1.0 (Legacy)
- Initial complex implementation
- Multiple features and data points
- More complex database schema
- Legacy files: `dirk.py`, `ah.py`, `main.py`

---

For questions or support, please check the logs first, then review the troubleshooting section above.
