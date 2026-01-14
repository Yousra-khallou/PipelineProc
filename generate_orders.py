"""
Génération des fichiers de commandes quotidiennes (Orders)
Simule plusieurs points de vente (POS)
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2
from faker import Faker

fake = Faker()

# Configuration
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'metastore',
    'user': 'hive',
    'password': 'hive123'
}

NUM_STORES = 5  # Nombre de points de vente
OUTPUT_DIR = Path("data/orders")

def get_active_skus():
    """Récupérer les SKUs actifs depuis PostgreSQL"""
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT p.sku, p.product_name, p.category, p.perishable
        FROM products p
        JOIN suppliers s ON p.supplier_id = s.supplier_id
        WHERE s.active = TRUE
    """)
    
    skus = cursor.fetchall()
    conn.close()
    return skus

def generate_store_orders(store_id, date_str, skus):
    """Générer les commandes pour un magasin"""
    
    orders = []
    
    # Chaque magasin vend entre 30% et 70% des SKUs disponibles
    num_skus_to_sell = random.randint(int(len(skus) * 0.3), int(len(skus) * 0.7))
    selected_skus = random.sample(skus, num_skus_to_sell)
    
    for sku, product_name, category, perishable in selected_skus:
        # Quantités basées sur le type de produit
        if perishable:
            # Produits frais: ventes plus faibles mais plus fréquentes
            base_qty = random.randint(5, 30)
        else:
            # Produits secs: ventes plus élevées
            base_qty = random.randint(10, 80)
        
        # Variation aléatoire ±30%
        variation = random.uniform(0.7, 1.3)
        quantity = int(base_qty * variation)
        
        order = {
            "order_id": f"ORD-{store_id}-{date_str}-{fake.uuid4()[:8]}",
            "store_id": store_id,
            "sku": sku,
            "product_name": product_name,
            "quantity": quantity,
            "category": category,
            "timestamp": f"{date_str}T{random.randint(8, 22):02d}:{random.randint(0, 59):02d}:00"
        }
        
        orders.append(order)
    
    return orders

def generate_daily_orders(date=None):
    """Générer les commandes pour tous les magasins d'une journée"""
    
    if date is None:
        date = datetime.now()
    
    date_str = date.strftime('%Y-%m-%d')
    print(f"\n=== GÉNÉRATION COMMANDES DU {date_str} ===\n")
    
    # Créer répertoire de sortie
    date_dir = OUTPUT_DIR / date_str
    date_dir.mkdir(parents=True, exist_ok=True)
    
    # Récupérer SKUs
    skus = get_active_skus()
    print(f" {len(skus)} SKUs actifs récupérés")
    
    total_orders = 0
    total_units = 0
    
    # Générer pour chaque magasin
    for store_num in range(1, NUM_STORES + 1):
        store_id = f"STORE{store_num:03d}"
        
        orders = generate_store_orders(store_id, date_str, skus)
        
        # Écrire fichier JSON
        output_file = date_dir / f"orders_{store_id}_{date_str}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(orders, f, indent=2, ensure_ascii=False)
        
        store_units = sum(o['quantity'] for o in orders)
        total_orders += len(orders)
        total_units += store_units
        
        print(f" {store_id}: {len(orders)} commandes, {store_units} unités")
    
    print(f"\n Total: {total_orders} commandes, {total_units} unités")
    print(f" Fichiers dans: {date_dir}")

def generate_historical_orders(num_days=7):
    """Générer l'historique des commandes"""
    print(f"\n=== GÉNÉRATION HISTORIQUE ({num_days} jours) ===")
    
    for i in range(num_days):
        date = datetime.now() - timedelta(days=i)
        generate_daily_orders(date)

def main():
    """Point d'entrée"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Générer les commandes')
    parser.add_argument('--days', type=int, default=1, help='Nombre de jours à générer')
    parser.add_argument('--date', type=str, help='Date spécifique (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.date:
        date = datetime.strptime(args.date, '%Y-%m-%d')
        generate_daily_orders(date)
    elif args.days > 1:
        generate_historical_orders(args.days)
    else:
        generate_daily_orders()

if __name__ == '__main__':
    main()