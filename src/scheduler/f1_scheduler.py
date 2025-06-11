import schedule
import time
import logging
from datetime import datetime
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('f1_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_f1_pipeline():
    """Complete F1 data pipeline"""
    start_time = datetime.now()
    logger.info("🏁 Starting F1 Weekly Pipeline")
    
    try:
        import sys
        import os
        
        # Add the project root to sys.path to prioritize local modules
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        src_path = os.path.join(project_root, 'src')
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        # Now import your local modules
        from crawler.f1_drivers import main as crawl_drivers
        from crawler.f1_teams import main as crawl_teams
        from crawler.f1_race import main as crawl_races
        from crawler.f1_fastest_laps import main as crawl_fastest_laps
        from transform.transform_data import main as transform_data
        from storage.bigquery_loader import main as load_to_bigquery
        
        # Run pipeline steps
        logger.info("🏎️ Crawling F1 data...")
        crawl_drivers()
        crawl_teams() 
        crawl_races()
        crawl_fastest_laps()
        
        logger.info("🔄 Transforming data...")
        transform_data()
        
        logger.info("📊 Loading to BigQuery...")
        load_to_bigquery()
        
        duration = datetime.now() - start_time
        logger.info(f"✅ Pipeline completed in {duration}")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise
    
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--run-now":
        logger.info("🚀 F1 Scheduler started")
        logger.info("📅 Schedule: Every Monday at 6:00 AM")
        run_f1_pipeline()

if __name__ == "__main__":
    main()