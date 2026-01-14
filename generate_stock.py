"""
Génération des snapshots de stock quotidiens
Simule l'état des stocks en entrepôt
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2

# Configuration
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'metastore',
    'user': 'hive',
    'password': 'hive123'
}

NUM_WAREHOUSES = 2
OUTPUT_DIR = Path("data/stock")

def get_products_with_rules():
   
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            p.sku,
            p.product_name,
            p.perishable,
            r.safety_stock,
            r.reorder_point,
            r.max_stock_level
        FROM products p
        JOIN replenishment_rules r ON p.sku = r.sku
    """)
    
    products = cursor.fetchall()
    conn.close()
    return products

def generate_warehouse_stock(warehouse_id, date_str, products):
    """Générer le snapshot de stock pour un entrepôt"""
    
    stock_records = []
    
    for sku, product_name, perishable, safety_stock, reorder_point, max_stock in products:
        
        # Stock disponible: entre 0% et 150% du reorder_point
        # Avec plus de probabilité d'être bas pour déclencher réappro
        if random.random() < 0.3:
            # 30% de chance d'avoir stock faible
            available = random.randint(0, safety_stock)
        elif random.random() < 0.5:
            # 20% de chance d'être proche reorder_point
            available = random.randint(safety_stock, reorder_point)
        else:
            # 50% de chance d'avoir stock normal
            available = random.randint(reorder_point, int(max_stock * 0.8))
        
        # Stock réservé (commandes en cours)
        reserved = random.randint(0, int(available * 0.2)) if available > 0 else 0
        
        record = {
            "warehouse_id": warehouse_id,
            "sku": sku,
            "product_name": product_name,
            "available_stock": available,
            "reserved_stock": reserved,
            "safety_stock": safety_stock,
            "reorder_point": reorder_point,
            "snapshot_date": date_str,
            "snapshot_time": f"{date_str}T23:00:00"
        }
        
        stock_records.append(record)
    
    return stock_records

def generate_daily_stock(date=None):
    """Générer les snapshots de stock pour tous les entrepôts"""
    
    if date is None:
        date = datetime.now()
    
    date_str = date.strftime('%Y-%m-%d')
    print(f"\n=== GÉNÉRATION STOCK DU {date_str} ===\n")
    
    # Créer répertoire
    date_dir = OUTPUT_DIR / date_str
    date_dir.mkdir(parents=True, exist_ok=True)
    
    # Récupérer produits
    products = get_products_with_rules()
    print(f"{len(products)} produits avec règles")
    
    # Générer pour chaque entrepôt
    for wh_num in range(1, NUM_WAREHOUSES + 1):
        warehouse_id = f"WH{wh_num:02d}"
        
        stock_records = generate_warehouse_stock(warehouse_id, date_str, products)
        
        # Écrire fichier
        output_file = date_dir / f"stock_{warehouse_id}_{date_str}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(stock_records, f, indent=2, ensure_ascii=False)
        
        # Statistiques
        total_units = sum(r['available_stock'] for r in stock_records)
        low_stock = sum(1 for r in stock_records if r['available_stock'] < r['safety_stock'])
        
        print(f" {warehouse_id}: {len(stock_records)} SKUs")
        print(f"  Stock total: {total_units} unités")
        print(f"  Sous safety stock: {low_stock} SKUs ({low_stock/len(stock_records)*100:.1f}%)")
    
    print(f"\n Fichiers dans: {date_dir}")

def generate_historical_stock(num_days=7):
    """Générer l'historique des stocks"""
    print(f"\n=== GÉNÉRATION HISTORIQUE STOCK ({num_days} jours) ===")
    
    for i in range(num_days):
        date = datetime.now() - timedelta(days=i)
        generate_daily_stock(date)

def main():
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Générer les snapshots de stock')
    parser.add_argument('--days', type=int, default=1, help='Nombre de jours')
    parser.add_argument('--date', type=str, help='Date spécifique (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.date:
        date = datetime.strptime(args.date, '%Y-%m-%d')
        generate_daily_stock(date)
    elif args.days > 1:
        generate_historical_stock(args.days)
    else:
        generate_daily_stock()

if __name__ == '__main__':
    main()