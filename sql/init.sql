-- ============================================
-- INITIALISATION BASE DE DONNÉES
-- ============================================

-- Table Suppliers
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    lead_time_days INTEGER DEFAULT 2,
    min_order_quantity INTEGER DEFAULT 24
);

-- Table Products
CREATE TABLE IF NOT EXISTS products (
    sku VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
    pack_size INTEGER NOT NULL DEFAULT 12,
    unit_cost DECIMAL(10,2)
);

-- Table Warehouses
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id VARCHAR(50) PRIMARY KEY,
    warehouse_name VARCHAR(255) NOT NULL
);

-- Table Rules
CREATE TABLE IF NOT EXISTS replenishment_rules (
    sku VARCHAR(50) REFERENCES products(sku),
    warehouse_id VARCHAR(50) REFERENCES warehouses(warehouse_id),
    safety_stock INTEGER DEFAULT 50,
    PRIMARY KEY(sku, warehouse_id)
);

-- Données de test
INSERT INTO suppliers VALUES 
('SUP01', 'Fresh Fruits Ltd', 1, 24),
('SUP02', 'Dairy Delights', 2, 36)
ON CONFLICT DO NOTHING;

INSERT INTO products VALUES 
('SKU001', 'Apples 1kg', 'SUP01', 12, 2.50),
('SKU002', 'Milk 1L', 'SUP02', 6, 1.20)
ON CONFLICT DO NOTHING;

INSERT INTO warehouses VALUES 
('WH01', 'Central Warehouse')
ON CONFLICT DO NOTHING;

INSERT INTO replenishment_rules VALUES 
('SKU001', 'WH01', 50),
('SKU002', 'WH01', 80)
ON CONFLICT DO NOTHING;
