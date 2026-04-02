import os

# AWS Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'

# S3 Bucket Configuration
S3_BUCKET = 'reddit-cs-career-analysis'
S3_RAW_DATA_PATH = 'raw-data/'
S3_PROCESSED_DATA_PATH = 'processed-data/'

# PySpark Configuration
SPARK_APP_NAME = 'Reddit-CS-Career-Analysis'