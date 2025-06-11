import os

# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'f1-projekt')
BIGQUERY_DATASET_ID = 'f1_analytics'
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Data paths - use environment variable or current working directory
PROJECT_ROOT = os.getenv('GITHUB_WORKSPACE', os.getcwd())
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
TRANSFORMED_DATA_DIR = os.path.join(DATA_DIR, "transformed_data")
DIMENSIONS_DIR = os.path.join(TRANSFORMED_DATA_DIR, "dimensions")
FACTS_DIR = os.path.join(TRANSFORMED_DATA_DIR, "facts")