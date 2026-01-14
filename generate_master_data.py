"""
Génération des données Master (PostgreSQL)
- Suppliers
- Products
- Replenishment Rules
"""

import psycopg2
from faker import Faker
import random

fake = Faker()

# Configuration PostgreSQL
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'metastore',
    'user': 'hive',
    'password': 'hive123'
}

def create_tables(conn):
    """Créer les tables PostgreSQL"""
    cursor = conn.cursor()
    
    # Table Suppliers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id VARCHAR(50) PRIMARY KEY,
            supplier_name VARCHAR(255) NOT NULL,
            lead_time_days INTEGER DEFAULT 2,
            min_order_quantity INTEGER DEFAULT 24,
            contact_email VARCHAR(255),
            active BOOLEAN DEFAULT TRUE
        )
    """)
    
    # Table Products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            sku VARCHAR(50) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
            pack_size INTEGER NOT NULL DEFAULT 12,
            unit_cost DECIMAL(10,2),
            category VARCHAR(100),
            perishable BOOLEAN DEFAULT FALSE
        )
    """)
    
    # Table Replenishment Rules
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS replenishment_rules (
            sku VARCHAR(50) PRIMARY KEY REFERENCES products(sku),
            safety_stock INTEGER NOT NULL DEFAULT 50,
            reorder_point INTEGER NOT NULL DEFAULT 100,
            max_stock_level INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    print("Tables créées")

def generate_suppliers(conn, num_suppliers=10):
    """Générer les fournisseurs"""
    cursor = conn.cursor()
    
    suppliers = []
    for i in range(1, num_suppliers + 1):
        supplier_id = f"SUP{i:03d}"
        supplier_name = fake.company()
        lead_time = random.randint(1, 5)
        moq = random.choice([12, 24, 36, 48])
        email = fake.company_email()
        
        suppliers.append((supplier_id, supplier_name, lead_time, moq, email))
    
    cursor.executemany("""
        INSERT INTO suppliers (supplier_id, supplier_name, lead_time_days, min_order_quantity, contact_email)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (supplier_id) DO NOTHING
    """, suppliers)
    
    conn.commit()
    print(f" {len(suppliers)} fournisseurs générés")
    return [s[0] for s in suppliers]

def generate_products(conn, supplier_ids, num_products=100):
    """Générer les produits"""
    cursor = conn.cursor()
    
    categories = ['Fruits', 'Légumes', 'Laitiers', 'Viandes', 'Poissons', 
                  'Épicerie', 'Boissons', 'Surgelés', 'Boulangerie', 'Hygiène']
    
    products = []
    for i in range(1, num_products + 1):
        sku = f"SKU{i:04d}"
        product_name = f"{fake.word().capitalize()} {fake.word()}"
        supplier_id = random.choice(supplier_ids)
        pack_size = random.choice([6, 12, 24, 48])
        unit_cost = round(random.uniform(0.5, 50.0), 2)
        category = random.choice(categories)
        perishable = category in ['Fruits', 'Légumes', 'Laitiers', 'Viandes', 'Poissons']
        
        products.append((sku, product_name, supplier_id, pack_size, unit_cost, category, perishable))
    
    cursor.executemany("""
        INSERT INTO products (sku, product_name, supplier_id, pack_size, unit_cost, category, perishable)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sku) DO NOTHING
    """, products)
    
    conn.commit()
    print(f" {len(products)} produits générés")
    return [p[0] for p in products]

def generate_replenishment_rules(conn, skus):
    """Générer les règles de réapprovisionnement"""
    cursor = conn.cursor()
    
    rules = []
    for sku in skus:
        safety_stock = random.randint(20, 100)
        reorder_point = safety_stock + random.randint(50, 150)
        max_stock = reorder_point + random.randint(200, 500)
        
        rules.append((sku, safety_stock, reorder_point, max_stock))
    
    cursor.executemany("""
        INSERT INTO replenishment_rules (sku, safety_stock, reorder_point, max_stock_level)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sku) DO NOTHING
    """, rules)
    
    conn.commit()
    print(f"{len(rules)} règles de réapprovisionnement générées")

def main():
    """Générer toutes les données master"""
    print("=== GÉNÉRATION DONNÉES MASTER ===\n")
    
    conn = psycopg2.connect(**PG_CONFIG)
    
    try:
        # Créer tables
        create_tables(conn)
        
        # Générer données
        supplier_ids = generate_suppliers(conn, num_suppliers=15)
        skus = generate_products(conn, supplier_ids, num_products=200)
        generate_replenishment_rules(conn, skus)
        
        # Vérification
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM suppliers")
        print(f"\n Total suppliers: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM products")
        print(f"Total products: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM replenishment_rules")
        print(f"Total rules: {cursor.fetchone()[0]}")
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()