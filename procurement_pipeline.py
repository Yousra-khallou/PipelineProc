import os
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

# Configuration Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProcurementPipeline:
    """Pipeline de traitement des commandes fournisseurs"""
    
    def __init__(self, processing_date=None):
        """Initialisation"""
        self.processing_date = processing_date or datetime.now().strftime('%Y-%m-%d')
        self.data_dir = Path('data')
        self.output_dir = Path('output')
        
        # Configuration PostgreSQL
        self.pg_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'metastore',
            'user': 'hive',
            'password': 'hive123'
        }
        
        logger.info(f"Pipeline initialisé pour {self.processing_date}")
    
    def hdfs_mkdir(self, path):
        """Créer répertoire HDFS"""
        cmd = f"docker exec namenode hdfs dfs -mkdir -p {path}"
        subprocess.run(cmd, shell=True, check=False)
    
    def hdfs_put(self, local_path, hdfs_path):
        """Upload fichier vers HDFS"""
        cmd = f"docker exec namenode hdfs dfs -put -f {local_path} {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Erreur upload HDFS: {result.stderr}")
            return False
        return True
    
    def hdfs_ls(self, path):
        """Lister fichiers HDFS"""
        cmd = f"docker exec namenode hdfs dfs -ls {path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout
    
    def step1_create_hdfs_structure(self):
        """Étape 1: Créer structure HDFS"""
        logger.info("=== ÉTAPE 1: STRUCTURE HDFS ===")
        
        directories = [
            f'/procurement/raw/orders/{self.processing_date}',
            f'/procurement/raw/stock/{self.processing_date}',
            f'/procurement/processed/{self.processing_date}',
            f'/procurement/output/{self.processing_date}',
            f'/procurement/logs/{self.processing_date}'
        ]
        
        for directory in directories:
            self.hdfs_mkdir(directory)
            logger.info(f" Créé: {directory}")
        
        return True
    
    def step2_ingest_orders(self):
        """Étape 2: Ingestion des commandes dans HDFS"""
        logger.info("=== ÉTAPE 2: INGESTION COMMANDES ===")
        
        orders_dir = self.data_dir / 'orders' / self.processing_date
        
        if not orders_dir.exists():
            logger.error(f"Répertoire commandes introuvable: {orders_dir}")
            return False
        
        hdfs_target = f'/procurement/raw/orders/{self.processing_date}'
        
        # Copier les fichiers locaux dans le conteneur puis HDFS
        for json_file in orders_dir.glob('*.json'):
            # Copier dans conteneur
            container_path = f'/tmp/{json_file.name}'
            cp_cmd = f"docker cp {json_file} namenode:{container_path}"
            subprocess.run(cp_cmd, shell=True, check=True)
            
            # Upload vers HDFS
            self.hdfs_put(container_path, f'{hdfs_target}/{json_file.name}')
            logger.info(f"Uploadé: {json_file.name}")
        
        # Vérifier
        files_list = self.hdfs_ls(hdfs_target)
        num_files = files_list.count('.json')
        logger.info(f" {num_files} fichiers dans HDFS")
        
        return True
    
    def step3_ingest_stock(self):
        """Étape 3: Ingestion des snapshots de stock"""
        logger.info("=== ÉTAPE 3: INGESTION STOCK ===")
        
        stock_dir = self.data_dir / 'stock' / self.processing_date
        
        if not stock_dir.exists():
            logger.error(f"Répertoire stock introuvable: {stock_dir}")
            return False
        
        hdfs_target = f'/procurement/raw/stock/{self.processing_date}'
        
        for json_file in stock_dir.glob('*.json'):
            container_path = f'/tmp/{json_file.name}'
            cp_cmd = f"docker cp {json_file} namenode:{container_path}"
            subprocess.run(cp_cmd, shell=True, check=True)
            
            self.hdfs_put(container_path, f'{hdfs_target}/{json_file.name}')
            logger.info(f" Uploadé: {json_file.name}")
        
        files_list = self.hdfs_ls(hdfs_target)
        num_files = files_list.count('.json')
        logger.info(f" {num_files} fichiers dans HDFS")
        
        return True
    
    def step4_aggregate_orders(self):
        """Étape 4: Agrégation des commandes"""
        logger.info("=== ÉTAPE 4: AGRÉGATION COMMANDES ===")
        
        # Lire tous les fichiers de commandes localement
        orders_dir = self.data_dir / 'orders' / self.processing_date
        all_orders = []
        
        for json_file in orders_dir.glob('*.json'):
            with open(json_file, 'r') as f:
                orders = json.load(f)
                all_orders.extend(orders)
        
        logger.info(f" {len(all_orders)} commandes chargées")
        
        # Agréger par SKU
        aggregated = {}
        for order in all_orders:
            sku = order['sku']
            qty = order['quantity']
            
            if sku not in aggregated:
                aggregated[sku] = {
                    'sku': sku,
                    'total_quantity': 0,
                    'num_orders': 0,
                    'num_stores': set()
                }
            
            aggregated[sku]['total_quantity'] += qty
            aggregated[sku]['num_orders'] += 1
            aggregated[sku]['num_stores'].add(order['store_id'])
        
        # Convertir sets en counts
        for sku in aggregated:
            aggregated[sku]['num_stores'] = len(aggregated[sku]['num_stores'])
        
        # Sauvegarder résultat
        output_file = self.output_dir / f'aggregated_orders_{self.processing_date}.json'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(list(aggregated.values()), f, indent=2)
        
        logger.info(f" {len(aggregated)} SKUs agrégés")
        logger.info(f" Total unités: {sum(a['total_quantity'] for a in aggregated.values())}")
        
        return aggregated
    
    def step5_calculate_net_demand(self, aggregated_orders):
        """Étape 5: Calcul du net demand"""
        logger.info("=== ÉTAPE 5: CALCUL NET DEMAND ===")
        
        # Charger stock
        stock_dir = self.data_dir / 'stock' / self.processing_date
        all_stock = {}
        
        for json_file in stock_dir.glob('*.json'):
            with open(json_file, 'r') as f:
                stock_records = json.load(f)
                for record in stock_records:
                    sku = record['sku']
                    # Prendre le premier warehouse (simplification)
                    if sku not in all_stock:
                        all_stock[sku] = record
        
        logger.info(f" {len(all_stock)} SKUs en stock")
        
        # Récupérer règles et infos produits depuis PostgreSQL
        conn = psycopg2.connect(**self.pg_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                p.sku,
                p.supplier_id,
                p.pack_size,
                r.safety_stock,
                s.min_order_quantity
            FROM products p
            JOIN replenishment_rules r ON p.sku = r.sku
            JOIN suppliers s ON p.supplier_id = s.supplier_id
        """)
        
        products_info = {row[0]: {
            'supplier_id': row[1],
            'pack_size': row[2],
            'safety_stock': row[3],
            'moq': row[4]
        } for row in cursor.fetchall()}
        
        conn.close()
        
        # Calculer net demand
        net_demands = []
        
        for sku, order_data in aggregated_orders.items():
            if sku not in all_stock or sku not in products_info:
                logger.warning(f"  SKU manquant: {sku}")
                continue
            
            stock = all_stock[sku]
            product = products_info[sku]
            
            aggregated_orders_qty = order_data['total_quantity']
            available = stock['available_stock']
            reserved = stock['reserved_stock']
            safety = product['safety_stock']
            pack_size = product['pack_size']
            moq = product['moq']
            
            # Formule: max(0, orders + safety - (available - reserved))
            preliminary = max(0, aggregated_orders_qty + safety - (available - reserved))
            
            # Arrondir au pack_size supérieur
            if preliminary > 0:
                num_packs = (preliminary + pack_size - 1) // pack_size
                rounded = num_packs * pack_size
                
                # Appliquer MOQ
                if rounded < moq:
                    rounded = moq
            else:
                rounded = 0
            
            net_demands.append({
                'sku': sku,
                'supplier_id': product['supplier_id'],
                'aggregated_orders': aggregated_orders_qty,
                'available_stock': available,
                'reserved_stock': reserved,
                'safety_stock': safety,
                'preliminary_demand': preliminary,
                'pack_size': pack_size,
                'rounded_demand': rounded,
                'moq': moq
            })
        
        # Sauvegarder
        output_file = self.output_dir / f'net_demand_{self.processing_date}.json'
        with open(output_file, 'w') as f:
            json.dump(net_demands, f, indent=2)
        
        skus_to_order = sum(1 for d in net_demands if d['rounded_demand'] > 0)
        total_units = sum(d['rounded_demand'] for d in net_demands)
        
        logger.info(f"{len(net_demands)} SKUs analysés")
        logger.info(f" {skus_to_order} SKUs à commander")
        logger.info(f" {total_units} unités totales à commander")
        
        return net_demands
    
    def step6_generate_supplier_orders(self, net_demands):
        """Étape 6: Génération des commandes fournisseurs"""
        logger.info("=== ÉTAPE 6: GÉNÉRATION COMMANDES FOURNISSEURS ===")
        
        # Grouper par fournisseur
        suppliers = {}
        for demand in net_demands:
            if demand['rounded_demand'] > 0:
                supplier_id = demand['supplier_id']
                if supplier_id not in suppliers:
                    suppliers[supplier_id] = []
                suppliers[supplier_id].append(demand)
        
        logger.info(f" {len(suppliers)} fournisseurs concernés")
        
        # Créer répertoire de sortie
        supplier_orders_dir = self.output_dir / 'supplier_orders' / self.processing_date
        supplier_orders_dir.mkdir(parents=True, exist_ok=True)
        
        # Générer fichier par fournisseur
        for supplier_id, items in suppliers.items():
            order = {
                'supplier_id': supplier_id,
                'order_date': self.processing_date,
                'generated_at': datetime.now().isoformat(),
                'total_items': len(items),
                'total_units': sum(item['rounded_demand'] for item in items),
                'items': [
                    {
                        'sku': item['sku'],
                        'quantity': item['rounded_demand'],
                        'pack_size': item['pack_size'],
                        'num_packs': item['rounded_demand'] // item['pack_size']
                    }
                    for item in items
                ]
            }
            
            # Sauvegarder
            filename = f'supplier_{supplier_id}_{self.processing_date}.json'
            output_file = supplier_orders_dir / filename
            
            with open(output_file, 'w') as f:
                json.dump(order, f, indent=2)
            
            logger.info(f" {supplier_id}: {len(items)} SKUs, {order['total_units']} unités")
            
            # Upload vers HDFS
            container_path = f'/tmp/{filename}'
            cp_cmd = f"docker cp {output_file} namenode:{container_path}"
            subprocess.run(cp_cmd, shell=True, check=True)
            
            hdfs_path = f'/procurement/output/{self.processing_date}/{filename}'
            self.hdfs_put(container_path, hdfs_path)
        
        logger.info(f" Commandes disponibles dans: {supplier_orders_dir}")
        
        return True
    
    def step7_detect_exceptions(self, aggregated_orders, net_demands):
        """Étape 7: Détection des exceptions"""
        logger.info("=== ÉTAPE 7: DÉTECTION EXCEPTIONS ===")
        
        exceptions = []
        
        # Exception 1: SKUs commandés sans stock
        stock_dir = self.data_dir / 'stock' / self.processing_date
        all_stock_skus = set()
        
        for json_file in stock_dir.glob('*.json'):
            with open(json_file, 'r') as f:
                stock_records = json.load(f)
                all_stock_skus.update(r['sku'] for r in stock_records)
        
        missing_stock = [sku for sku in aggregated_orders.keys() if sku not in all_stock_skus]
        if missing_stock:
            exceptions.append({
                'type': 'MISSING_STOCK_DATA',
                'severity': 'HIGH',
                'count': len(missing_stock),
                'skus': missing_stock[:10]
            })
            logger.warning(f"  {len(missing_stock)} SKUs sans données stock")
        
        # Exception 2: Demandes élevées
        high_demands = [
            {'sku': d['sku'], 'quantity': d['aggregated_orders']}
            for d in net_demands 
            if d['aggregated_orders'] > 500
        ]
        if high_demands:
            exceptions.append({
                'type': 'HIGH_DEMAND',
                'severity': 'MEDIUM',
                'count': len(high_demands),
                'details': high_demands
            })
            logger.warning(f"  {len(high_demands)} SKUs avec demande élevée")
        
        # Exception 3: Stock négatif
        negative_stock = []
        for json_file in (self.data_dir / 'stock' / self.processing_date).glob('*.json'):
            with open(json_file, 'r') as f:
                stock_records = json.load(f)
                negative = [
                    {'sku': r['sku'], 'stock': r['available_stock']}
                    for r in stock_records if r['available_stock'] < 0
                ]
                negative_stock.extend(negative)
        
        if negative_stock:
            exceptions.append({
                'type': 'NEGATIVE_STOCK',
                'severity': 'CRITICAL',
                'count': len(negative_stock),
                'details': negative_stock
            })
            logger.error(f" {len(negative_stock)} SKUs avec stock négatif")
        
        # Sauvegarder rapport
        if exceptions:
            report = {
                'date': self.processing_date,
                'timestamp': datetime.now().isoformat(),
                'total_exceptions': len(exceptions),
                'exceptions': exceptions
            }
            
            output_file = self.output_dir / f'exceptions_{self.processing_date}.json'
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f" Rapport d'exceptions: {len(exceptions)} types")
        else:
            logger.info(" Aucune exception détectée")
        
        return exceptions
    
    def run_pipeline(self):
        """Exécution complète du pipeline"""
        logger.info("=" * 70)
        logger.info(f"DÉMARRAGE PIPELINE - {self.processing_date}")
        logger.info("=" * 70)
        
        start_time = datetime.now()
        
        try:
            # Étape 1: Structure HDFS
            self.step1_create_hdfs_structure()
            
            # Étape 2: Ingestion commandes
            if not self.step2_ingest_orders():
                raise Exception("Échec ingestion commandes")
            
            # Étape 3: Ingestion stock
            if not self.step3_ingest_stock():
                raise Exception("Échec ingestion stock")
            
            # Étape 4: Agrégation
            aggregated = self.step4_aggregate_orders()
            
            # Étape 5: Net demand
            net_demands = self.step5_calculate_net_demand(aggregated)
            
            # Étape 6: Commandes fournisseurs
            self.step6_generate_supplier_orders(net_demands)
            
            # Étape 7: Exceptions
            self.step7_detect_exceptions(aggregated, net_demands)
            
            # Fin
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 70)
            logger.info(" PIPELINE TERMINÉ AVEC SUCCÈS")
            logger.info(f"  Durée: {duration:.2f} secondes")
            logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error("=" * 70)
            logger.error(f" PIPELINE ÉCHOUÉ: {e}")
            logger.error("=" * 70)
            raise

def main():
   
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline de Procurement')
    parser.add_argument('--date', type=str, help='Date de traitement (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    pipeline = ProcurementPipeline(processing_date=args.date)
    pipeline.run_pipeline()

if __name__ == '__main__':
    main()