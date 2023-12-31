import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

### Init database connection
db = DatabaseConnector()
db_creds = db.read_db_creds()
engine = db.init_db_engine()

# Extract user data from RDS database
# extractor = DataExtractor('legacy_users')
# df = extractor.read_rds_table(engine)
# print("extracted user data")

# # Clean user data
# cleaner = DataCleaning(df)
# cleaned_df = cleaner.clean_user_data()
# print("cleaned user data")

# # Upload cleaned data to RDS database
# db.upload_to_db(cleaned_df, 'dim_users')
# print("uploaded user data")

### Extract and clean card data from PDF file
# pdf_url = db_creds["pdf_url"]
# extractor = DataExtractor('card_data')
# card_df = extractor.retrieve_pdf_data(pdf_url)
# print("extracted card data")
# cleaner = DataCleaning(card_df)
# clean_card_df = cleaner.clean_card_data()
# print("cleaned card data")
# db.upload_to_db(clean_card_df, 'dim_card_details')
# print("uploaded card data")

# # Extract store data from API
# num_stores_endpoint = db_creds["num_stores_endpoint"]
# store_endpoint = db_creds["store_endpoint"]
# headers = {'x-api-key': db_creds["api_key"]}
# extractor = DataExtractor('store_data')
# stores_df = extractor.retrieve_stores_data(num_stores_endpoint, store_endpoint, headers)
# print("extracted store data")

# # Clean store data
# cleaner = DataCleaning(stores_df)
# cleaned_stores_df = cleaner.clean_store_data()
# print("cleaned store data")

# # Upload cleaned data to RDS database
# db = DatabaseConnector()
# db.upload_to_db(cleaned_stores_df, 'dim_stores')
# print("uploaded store data")

# Extract product data from S3 bucket
# extractor = DataExtractor('product_data')
# product_df = extractor.extract_from_s3('s3_product_data_bucket')
# print("extracted product data")

# # # Clean product data
# cleaner = DataCleaning(product_df)
# clean_product_df = cleaner.convert_product_weights()
# clean_product_df = cleaner.clean_products_data()
# print("cleaned product data")
# print(clean_product_df.head())

# # # Upload cleaned data to RDS database
# db = DatabaseConnector()
# db.upload_to_db(clean_product_df, 'dim_products')
# print("uploaded product data")

### Retrieve and clean orders table
# table_names = db.list_db_tables(engine)
# extractor = DataExtractor('orders_table')
# orders_df = extractor.read_rds_table(engine)
# cleaner = DataCleaning(orders_df)
# clean_orders_df = cleaner.clean_orders_data()
# db.upload_to_db(clean_orders_df, 'orders_table')

### Retrieve and clean date events in s3 bucket
# extractor = DataExtractor('date_events')
# date_events_df = extractor.extract_from_s3('s3_date_events_bucket')
# print("extracted date events data")
# cleaner = DataCleaning(date_events_df)
# clean_date_events_df = cleaner.clean_date_events()
# print("cleaned date events data")
# db.upload_to_db(clean_date_events_df, 'dim_date_times')
# print("uploaded date events data")


