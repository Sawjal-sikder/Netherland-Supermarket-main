# Docker Deployment Guide for DigitalOcean App Platform

This guide explains how to deploy the Netherlands Supermarket Scraper on DigitalOcean App Platform using a single Docker image with different run commands.

## Single Docker Image, Multiple Use Cases

The unified `Dockerfile` can run both:
1. **Full Product Scraping** - Complete product database updates
2. **Offers Only Scraping** - Daily promotional offers collection

## DigitalOcean App Platform Configuration

### 1. Full Product Scraping (Weekly/Monthly)

**Run Command:**
```bash
python run_scrapers.py all
```

**Alternative Commands:**
```bash
# Run specific supermarkets
python run_scrapers.py ah jumbo plus

# Run with specific limits
python run_scrapers.py all --limit 1000
```

### 2. Offers Only Scraping (Daily)

**Run Command:**
```bash
python run_offers_scraper.py --supermarket all
```

**Alternative Commands:**
```bash
# Run specific offers scrapers
python run_offers_scraper.py --supermarket ah,jumbo,plus

# Run with limits for testing
python run_offers_scraper.py --supermarket all --limit 10
```

## DigitalOcean App Platform Setup

### Method 1: Using App Platform Console

1. Create new App from GitHub repository
2. Choose "Docker" as the resource type
3. Set the **Run Command** based on your needs:
   - For offers: `python run_offers_scraper.py --supermarket all`
   - For full: `python run_scrapers.py all`
4. Configure environment variables (see Environment Variables section)

### Method 2: Using App Spec (app.yaml)

Create an `app.yaml` file:

```yaml
name: nl-supermarket-scraper
services:
- name: offers-scraper
  source_dir: /
  github:
    repo: your-username/Netherland-Supermarket
    branch: main
  dockerfile_path: Dockerfile
  run_command: python run_offers_scraper.py --supermarket all
  environment_slug: docker
  instance_count: 1
  instance_size_slug: basic-xxs
  envs:
  - key: DB_HOST
    value: your-db-host
  - key: DB_PORT
    value: "25060"
  - key: DB_NAME
    value: supermarket_products
  - key: DB_USER
    value: your-db-user
  - key: DB_PASSWORD
    value: your-db-password
    type: SECRET
  - key: SSLMODE
    value: REQUIRED
```

### Method 3: Multiple Services (Advanced)

You can run both scrapers as separate services:

```yaml
name: nl-supermarket-scraper
services:
- name: offers-daily
  dockerfile_path: Dockerfile
  run_command: python run_offers_scraper.py --supermarket all
  instance_count: 1
  instance_size_slug: basic-xxs
  
- name: full-weekly
  dockerfile_path: Dockerfile
  run_command: python run_scrapers.py all --limit 5000
  instance_count: 1
  instance_size_slug: basic-xs
```

## Environment Variables

Configure these in your DigitalOcean App Platform:

```bash
DB_HOST=your-database-host
DB_PORT=25060
DB_NAME=supermarket_products
DB_USER=your-db-username
DB_PASSWORD=your-db-password
SSLMODE=REQUIRED
```

## Resource Requirements

### Offers Scraping (Daily)
- **Instance Size:** basic-xxs ($5/month)
- **Memory:** 512 MB
- **CPU:** 0.5 vCPU
- **Runtime:** ~10-15 minutes
- **Data:** ~500-2000 offers per day

### Full Product Scraping (Weekly)
- **Instance Size:** basic-xs or basic-s ($12-25/month)
- **Memory:** 1 GB+
- **CPU:** 1 vCPU
- **Runtime:** 2-6 hours
- **Data:** 50,000+ products

## Scheduling

### Offers Scraping (Recommended: Daily)
- **Time:** Early morning (6-8 AM CET)
- **Frequency:** Once per day
- **Command:** `python run_offers_scraper.py --supermarket all`

### Full Product Scraping (Recommended: Weekly)
- **Time:** Weekend (low traffic)
- **Frequency:** Once per week
- **Command:** `python run_scrapers.py all`

## Manual Deployment Commands

```bash
# Build and test locally
docker build -t nl-supermarket .

# Test offers scraping
docker run --env-file .env nl-supermarket python run_offers_scraper.py --supermarket all --limit 5

# Test full scraping
docker run --env-file .env nl-supermarket python run_scrapers.py ah --limit 10

# Push to registry (if using private registry)
docker tag nl-supermarket your-registry/nl-supermarket:latest
docker push your-registry/nl-supermarket:latest
```

## Monitoring and Logs

- Monitor via DigitalOcean App Platform dashboard
- Check logs for scraping progress and errors
- Set up alerts for failed deployments
- Monitor database growth and performance

## Cost Optimization

1. **Use Jobs instead of Services** for scheduled scraping
2. **Scale down to basic-xxs** for offers-only
3. **Run full scraping weekly** instead of daily
4. **Use spot instances** if available
5. **Monitor resource usage** and adjust accordingly

## Troubleshooting

### Common Issues:
1. **Chrome/Selenium errors:** Ensure sufficient memory (1GB+)
2. **Database connection:** Verify environment variables
3. **Timeout errors:** Increase instance size for full scraping
4. **Rate limiting:** Add delays between requests

### Debug Commands:
```bash
# Test database connection
python -c "from database import Database; db = Database(); print('DB OK')"

# Test specific scraper
python run_offers_scraper.py --supermarket ah --limit 1

# Check Chrome installation
google-chrome --version
```
