import json
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from storage.configuration import (
    GOOGLE_CLOUD_PROJECT_ID,
    BIGQUERY_DATASET_ID,
    DIMENSIONS_DIR,
    FACTS_DIR,
)

class BigQueryLoader:
    def __init__(self, project_id=None, dataset_id=BIGQUERY_DATASET_ID):
        """Initialize BigQuery client and dataset"""
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id or self.client.project
        
        # Create dataset if it doesn't exist
        self._create_dataset_if_not_exists()
    
    def _create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_ref = self.client.dataset(self.dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def load_json_to_table(self, json_file_path, table_name, write_disposition="WRITE_TRUNCATE"):
        """Load JSON file to BigQuery table"""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Check if file exists and has data
        if not os.path.exists(json_file_path):
            logger.warning(f"File not found: {json_file_path}")
            return False
            
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            logger.warning(f"No data to load for table {table_name}")
            return False
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition)
        )
        
        try:
            # Load data
            job = self.client.load_table_from_json(
                data, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            # Get table info
            table = self.client.get_table(table_id)
            logger.info(f"✅ Loaded {table.num_rows} rows to {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {str(e)}")
            return False
    
    def load_all_dimensions(self, dimensions_dir=DIMENSIONS_DIR):
        """Load all dimension tables"""
        success_count = 0
        total_count = 0
        
        dimension_files = [
            ("drivers.json", "drivers"),
            ("teams.json", "teams"), 
            ("races.json", "races"),
            ("sessions.json", "sessions"),
            ('countries.json', 'countries')
        ]
        
        for filename, table_name in dimension_files:
            file_path = os.path.join(dimensions_dir, filename)
            total_count += 1
            
            if self.load_json_to_table(file_path, table_name):
                success_count += 1
        
        logger.info(f"Dimensions loaded: {success_count}/{total_count}")
        return success_count == total_count
    
    def load_all_facts(self, facts_dir=FACTS_DIR):
        """Load all fact tables"""
        success_count = 0
        total_count = 0
        
        if not os.path.exists(facts_dir):
            logger.warning(f"Facts directory not found: {facts_dir}")
            return False
        
        # Load all JSON files in facts directory
        for filename in os.listdir(facts_dir):
            if filename.endswith('.json'):
                table_name = filename.replace('.json', '')
                file_path = os.path.join(facts_dir, filename)
                total_count += 1
                
                if self.load_json_to_table(file_path, table_name):
                    success_count += 1
        
        logger.info(f"Facts loaded: {success_count}/{total_count}")
        return success_count == total_count
    
    def load_all_data(self):
        """Load both dimensions and facts"""
        logger.info("Starting BigQuery data load...")
        
        # Load dimensions first (facts reference dimensions)
        dimensions_success = self.load_all_dimensions()
        
        # Load facts
        facts_success = self.load_all_facts()
        
        if dimensions_success and facts_success:
            logger.info("All data loaded successfully to BigQuery!")
            self._print_table_summary()
            return True
        else:
            logger.error("Some tables failed to load")
            return False
    
    def _print_table_summary(self):
        """Print summary of loaded tables"""
        logger.info("\nBigQuery Tables Summary:")
        
        # List all tables in dataset
        tables = self.client.list_tables(self.dataset_id)
        
        for table in tables:
            table_ref = self.client.get_table(table.reference)
            logger.info(f"{table.table_id}: {table_ref.num_rows:,} rows")

def main():
    print("🏎️  Loading F1 Data to BigQuery...")
    
    # Check if transformed data exists
    if not os.path.exists("data/transformed_data"):
        print("No transformed data found!")
        print("Run the transformation first: python src/transform/transform_data.py")
        return
    
    # Load to BigQuery
    loader = BigQueryLoader(project_id=GOOGLE_CLOUD_PROJECT_ID)
    success = loader.load_all_data()
    
    if success:
        print(f"\nSuccess! View your data at:")
        print(f"https://console.cloud.google.com/bigquery?project={GOOGLE_CLOUD_PROJECT_ID}")
        
        return True
    
    else:
        print("Some errors occurred during loading")
        return False

if __name__ == "__main__":
    main()