import schedule
import time
import logging
from datetime import datetime
import sys
import os
import asyncio

SRC_PATH = os.path.join(os.getcwd(), 'src')
sys.path.append(SRC_PATH)

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

def wait_for_all_background_tasks(max_wait_minutes=60, sleep_interval=30):
    """Wait for background tasks with timeout protection"""
    start_time = time.time()
    timeout = max_wait_minutes * 60
    
    while time.time() - start_time < timeout:  # ← Timeout protection
        try:
            loop = asyncio.get_running_loop()
            tasks = asyncio.all_tasks(loop)
            active_tasks = [task for task in tasks if not task.done()]
            
            if not active_tasks:
                logger.info("✅ No background tasks detected")
                return True
            
            elapsed = int(time.time() - start_time)
            logger.info(f"⏳ {len(active_tasks)} tasks running... ({elapsed}s/{timeout}s)")
            time.sleep(sleep_interval)
            
        except RuntimeError:
            logger.info("✅ No async event loop running")
            return True
    
    # Timeout reached - FAIL the pipeline
    logger.error(f"❌ TIMEOUT: Tasks didn't complete in {max_wait_minutes} minutes!")
    raise TimeoutError("Background tasks failed to complete")

def run_f1_pipeline():
    """Complete F1 data pipeline"""
    start_time = datetime.now()
    logger.info("🏁 Starting F1 Weekly Pipeline")
    
    try:
        from crawler.f1_drivers import main as crawl_drivers
        from crawler.f1_teams import main as crawl_teams
        from crawler.f1_race import main as crawl_races
        from crawler.f1_fastest_laps import main as crawl_fastest_laps
        from transform.transform_data import main as transform_data
        from storage.bigquery_loader import main as load_to_bigquery
        
        # Run pipeline steps with clear logging
        logger.info("=" * 60)
        logger.info("🏎️ PHASE 1: Crawling F1 Drivers...")
        crawl_drivers()
        wait_for_all_background_tasks(max_wait_minutes=15, sleep_interval=10)
        logger.info("✅ Drivers completed")
        
        logger.info("🏎️ PHASE 2: Crawling F1 Teams...")
        crawl_teams()
        wait_for_all_background_tasks(max_wait_minutes=15, sleep_interval=10)
        logger.info("✅ Teams completed")
        
        logger.info("🏎️ PHASE 3: Crawling F1 Races...")
        crawl_races()
        wait_for_all_background_tasks(max_wait_minutes=15, sleep_interval=30)
        logger.info("✅ Races completed")
        
        logger.info("🏎️ PHASE 4: Crawling F1 Fastest Laps...")
        crawl_fastest_laps()
        wait_for_all_background_tasks(max_wait_minutes=15, sleep_interval=5)
        logger.info("✅ Fastest Laps completed")
        
        logger.info("✅ ALL CRAWLING COMPLETED")
        
        logger.info("=" * 60)
        logger.info("🔄 PHASE 2: Transforming data...")
        transform_data()
        logger.info("✅ Data transformation completed")
        
        logger.info("📊 PHASE 3: Loading to BigQuery...")
        load_to_bigquery()
        logger.info("✅ BigQuery loading completed")
        
        duration = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"✅ Complete pipeline finished in {duration}")
        
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