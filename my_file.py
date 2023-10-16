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

# Clean user data
cleaner = DataCleaning(df)
cleaned_df = cleaner.clean_user_data()
print("cleaned data")

# Upload cleaned data to RDS database
db.upload_to_db(cleaned_df, 'dim_users')


# # Extract card data from PDF file
# extractor = DataExtractor('card_data')
# df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# # Clean card data
# cleaner = DataCleaning(df)
# cleaned_df = cleaner.clean_card_data()

# # Upload cleaned data to RDS database
# db = DatabaseConnector()
# db.upload_to_db(cleaned_df, 'dim_card_details')