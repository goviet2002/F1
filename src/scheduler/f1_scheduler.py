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

async def run_all_crawlers():
    """Run all crawlers concurrently"""
    
    from crawler.f1_drivers import scrape_driver_async
    from crawler.f1_teams import scrape_team_async
    from crawler.f1_race import scrape_race_async
    from crawler.f1_fastest_laps import scrape_fastest_laps_async
    
    scrape_results = await asyncio.gather(
        scrape_driver_async(),
        scrape_team_async(),
        scrape_race_async(),
        scrape_fastest_laps_async(),
        return_exceptions=True
    )
    
    return scrape_results

def run_f1_pipeline():
    """Complete F1 data pipeline"""
    start_time = datetime.now()
    logger.info("🏁 Starting F1 Weekly Pipeline")
    
    try:
        from transform.transform_data import main as transform_data
        from storage.bigquery_loader import main as load_to_bigquery
        
        # Run pipeline steps with clear logging
        logger.info("=" * 60)
        logger.info("🏎️ PHASE 1: Crawling F1 Data...")
        asyncio.run(run_all_crawlers())
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
        logger.info("📅 Schedule: Every Monday at 3:00 AM")
        run_f1_pipeline()

if __name__ == "__main__":
    main()