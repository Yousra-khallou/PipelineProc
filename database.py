"""
Script de Réinitialisation Complète de la Base de Données
Supprime et recrée toutes les tables avec le bon schéma
"""

import psycopg2

# Configuration PostgreSQL
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'metastore',
    'user': 'hive',
    'password': 'hive123'
}

def reset_database():
    """Supprimer et recréer toutes les tables"""
    
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print(" SUPPRESSION DES TABLES EXISTANTES...\n")
    
    # Supprimer les tables dans le bon ordre (à cause des foreign keys)
    tables_to_drop = [
        'replenishment_rules',
        'products', 
        'suppliers'
    ]
    
    for table in tables_to_drop:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f" Table {table} supprimée")
        except Exception as e:
            print(f" Erreur suppression {table}: {e}")
    
    print("\n CRÉATION DES NOUVELLES TABLES...\n")
    
    # Table 1: Suppliers
    cursor.execute("""
        CREATE TABLE suppliers (
            supplier_id VARCHAR(50) PRIMARY KEY,
            supplier_name VARCHAR(255) NOT NULL,
            lead_time_days INTEGER DEFAULT 2,
            min_order_quantity INTEGER DEFAULT 24,
            contact_email VARCHAR(255),
            active BOOLEAN DEFAULT TRUE
        )
    """)
    print(" Table 'suppliers' créée")
    print("  Colonnes: supplier_id, supplier_name, lead_time_days, min_order_quantity, contact_email, active")
    
    # Table 2: Products
    cursor.execute("""
        CREATE TABLE products (
            sku VARCHAR(50) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
            pack_size INTEGER NOT NULL DEFAULT 12,
            unit_cost DECIMAL(10,2),
            category VARCHAR(100),
            perishable BOOLEAN DEFAULT FALSE
        )
    """)
    print(" Table 'products' créée")
    print("  Colonnes: sku, product_name, supplier_id, pack_size, unit_cost, category, perishable")
    
    # Table 3: Replenishment Rules
    cursor.execute("""
        CREATE TABLE replenishment_rules (
            sku VARCHAR(50) PRIMARY KEY REFERENCES products(sku),
            safety_stock INTEGER NOT NULL DEFAULT 50,
            reorder_point INTEGER NOT NULL DEFAULT 100,
            max_stock_level INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print(" Table 'replenishment_rules' créée")
    print("  Colonnes: sku, safety_stock, reorder_point, max_stock_level, last_updated")
    
    print("\n BASE DE DONNÉES RÉINITIALISÉE AVEC SUCCÈS!\n")
    
    # Vérifier les tables créées
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    print(f" Tables dans la base ({len(tables)}):")
    for table in tables:
        print(f"   - {table[0]}")
    
    conn.close()
    
    print("\n Vous pouvez maintenant exécuter:")
    print("   python generate_master_data.py")

if __name__ == '__main__':
    reset_database()