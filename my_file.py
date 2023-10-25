import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Extract user data from RDS database
db = DatabaseConnector()
db_creds = db.read_db_creds()
engine = db.init_db_engine()
extractor = DataExtractor('legacy_users')
df = extractor.read_rds_table(engine)
print("extracted user data")

# Clean user data
cleaner = DataCleaning(df)
cleaned_df = cleaner.clean_user_data()
print("cleaned user data")

# Upload cleaned data to RDS database
db.upload_to_db(cleaned_df, 'dim_users')
print("uploaded user data")

# Extract card data from PDF file
pdf_url = db_creds["pdf_url"]
extractor = DataExtractor('card_data')
card_df = extractor.retrieve_pdf_data(pdf_url)
print("extracted card data")

# Clean card data
cleaner = DataCleaning(card_df)
cleaned__card_df = cleaner.clean_card_data()
print("cleaned card data")

# Upload cleaned data to RDS database
db = DatabaseConnector()
db.upload_to_db(cleaned_df, 'dim_card_details')
print("uploaded card data")

# Extract store data from API
num_stores_endpoint = db_creds["num_stores_endpoint"]
store_endpoint = db_creds["store_endpoint"]
headers = {'x-api-key': db_creds["api_key"]}
extractor = DataExtractor('store_data')
stores_df = extractor.retrieve_stores_data(num_stores_endpoint, store_endpoint, headers)
print("extracted store data")

# Clean store data
cleaner = DataCleaning(stores_df)
cleaned_stores_df = cleaner.clean_store_data()
print("cleaned store data")

# Upload cleaned data to RDS database
db = DatabaseConnector()
db.upload_to_db(cleaned_stores_df, 'dim_stores')
print("uploaded store data")

# Extract product data from S3 bucket
s3_bucket = db_creds["s3_bucket"]
extractor = DataExtractor('product_data')
product_df = extractor.extract_from_s3(s3_bucket)
print("extracted product data")

# Clean product data
cleaner = DataCleaning(product_df)
cleaned_product_df = cleaner.clean_products_data()
print("cleaned product data")

# Upload cleaned data to RDS database
db = DatabaseConnector()
db.upload_to_db(cleaned_product_df, 'dim_products')
print("uploaded product data")