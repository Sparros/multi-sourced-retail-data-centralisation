import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Extract user data from RDS database
db = DatabaseConnector()
extractor = DataExtractor('user_data')
df = extractor.read_rds_table(db, 'user_data')

# Clean user data
cleaner = DataCleaning(df)
cleaned_df = cleaner.clean_user_data()

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