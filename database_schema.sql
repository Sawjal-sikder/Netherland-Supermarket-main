-- Simplified Database Schema for Supermarket Product Scraper
-- Created: July 27, 2025

CREATE DATABASE IF NOT EXISTS supermarket_products 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE supermarket_products;

-- Supermarkets table
CREATE TABLE supermarkets (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    code VARCHAR(20) NOT NULL UNIQUE,
    base_url VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Categories table (simplified)
CREATE TABLE categories (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    supermarket_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (supermarket_id) REFERENCES supermarkets(id) ON DELETE CASCADE,
    UNIQUE KEY unique_category (supermarket_id, slug),
    INDEX idx_name (name)
);

-- Main products table (simplified and focused)
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    
    -- Core product information
    product_id VARCHAR(100) NOT NULL,
    name VARCHAR(500) NOT NULL,
    category_id INT NOT NULL,
    supermarket_id INT NOT NULL,
    
    -- Pricing (required fields)
    price DECIMAL(10,2) NOT NULL,
    unit_amount VARCHAR(100) NOT NULL, -- e.g., "500g", "1L", "24 pieces"
    price_per_unit DECIMAL(10,2) NOT NULL, -- calculated price per standard unit
    unit_type ENUM('kg', 'liter', 'piece', 'meter', 'gram', 'ml') NOT NULL,
    
    -- Discount information (nullable)
    original_price DECIMAL(10,2) NULL,
    discount_type VARCHAR(100) NULL, -- "30% discount", "1+1 free", "2 for €5"
    discount_start_date DATE NULL,
    discount_end_date DATE NULL,
    
    -- Search and categorization
    search_tags TEXT NOT NULL, -- comma-separated tags for search
    
    -- Product image
    image_url VARCHAR(500) NULL, -- product image URL
    
    -- Tracking
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints and indexes
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
    FOREIGN KEY (supermarket_id) REFERENCES supermarkets(id) ON DELETE CASCADE,
    UNIQUE KEY unique_product (supermarket_id, product_id),
    INDEX idx_name (name),
    INDEX idx_price (price),
    INDEX idx_price_per_unit (price_per_unit),
    INDEX idx_supermarket (supermarket_id),
    INDEX idx_category (category_id),
    INDEX idx_discount (discount_start_date, discount_end_date),
    FULLTEXT idx_search (name, search_tags)
);

-- Price history for tracking price changes
CREATE TABLE price_history (
    id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    original_price DECIMAL(10,2) NULL,
    price_per_unit DECIMAL(10,2) NOT NULL,
    discount_type VARCHAR(100) NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    INDEX idx_product_date (product_id, recorded_at)
);

-- Scraping sessions for tracking
CREATE TABLE scraping_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    supermarket_id INT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    products_scraped INT DEFAULT 0,
    status ENUM('running', 'completed', 'failed') DEFAULT 'running',
    error_message TEXT NULL,
    
    FOREIGN KEY (supermarket_id) REFERENCES supermarkets(id) ON DELETE CASCADE,
    INDEX idx_supermarket_date (supermarket_id, started_at)
);

-- Insert supermarkets
INSERT INTO supermarkets (name, code, base_url) VALUES
('Dirk van den Broek', 'DIRK', 'https://www.dirk.nl'),
('Albert Heijn', 'AH', 'https://www.ah.nl');

-- Create useful views
CREATE VIEW products_with_details AS
SELECT 
    p.id,
    p.product_id,
    p.name,
    s.name as supermarket_name,
    s.code as supermarket_code,
    c.name as category_name,
    p.price,
    p.unit_amount,
    p.price_per_unit,
    p.unit_type,
    p.original_price,
    p.discount_type,
    p.discount_start_date,
    p.discount_end_date,
    p.search_tags,
    CASE 
        WHEN p.original_price IS NOT NULL THEN 
            ROUND(((p.original_price - p.price) / p.original_price * 100), 2)
        ELSE NULL 
    END as discount_percentage,
    p.last_updated
FROM products p
JOIN supermarkets s ON p.supermarket_id = s.id
JOIN categories c ON p.category_id = c.id;

CREATE VIEW current_discounts AS
SELECT *
FROM products_with_details
WHERE original_price IS NOT NULL
AND (discount_end_date IS NULL OR discount_end_date >= CURDATE())
ORDER BY discount_percentage DESC;
