"""
Planificateur Quotidien du Pipeline
Exécute automatiquement le pipeline tous les jours à 22:00
"""

import schedule
import time
import subprocess
import logging
from datetime import datetime

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_daily_pipeline():
    """Exécuter le pipeline quotidien"""
    logger.info("=" * 70)
    logger.info("DÉMARRAGE DU PIPELINE QUOTIDIEN")
    logger.info("=" * 70)
    
    try:
        # 1. Générer les commandes du jour
        logger.info("Étape 1/4: Génération des commandes...")
        result = subprocess.run(['python', 'generate_orders.py'], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Erreur génération commandes: {result.stderr}")
            return
        logger.info("Commandes générées avec succès")
        
        # 2. Générer les stocks du jour
        logger.info("Étape 2/4: Génération des stocks...")
        result = subprocess.run(['python', 'generate_stock.py'], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Erreur génération stocks: {result.stderr}")
            return
        logger.info("Stocks générés avec succès")
        
        # 3. Exécuter le pipeline
        logger.info("Étape 3/4: Exécution du pipeline...")
        result = subprocess.run(['python', 'procurement_pipeline.py'], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Erreur pipeline: {result.stderr}")
            return
        logger.info("Pipeline exécuté avec succès")
        
        # 4. Succès
        logger.info("=" * 70)
        logger.info("PIPELINE QUOTIDIEN TERMINÉ AVEC SUCCÈS")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")

def main():
    """Point d'entrée du planificateur"""
    logger.info("=" * 70)
    logger.info("PLANIFICATEUR DE PIPELINE DÉMARRÉ")
    logger.info("Heure d'exécution programmée: 22:00")
    logger.info("=" * 70)
    
    # Planifier l'exécution quotidienne à 22:00
    schedule.every().day.at("22:00").do(run_daily_pipeline)
    
    # Optionnel: exécution immédiate au démarrage (pour test)
    # run_daily_pipeline()
    
    # Boucle infinie
    while True:
        schedule.run_pending()
        time.sleep(60)  # Vérifier toutes les minutes

if __name__ == '__main__':
    # Installer d'abord: pip install schedule
    try:
        import schedule
    except ImportError:
        print("Installer schedule: pip install schedule")
        exit(1)
    
    main()