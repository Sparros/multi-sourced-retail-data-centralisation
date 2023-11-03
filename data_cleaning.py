import pandas as pd
import datetime
import uuid
pd.set_option('display.max_columns', None)  # Set to display all columns
pd.options.mode.chained_assignment = None  # default='warn'
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:

    def __init__(self, df):
        self.df = df
        self.card_df = df
        self.card_df = df
        self.products_df = df
        self.orders_df = df
        self.date_events_df = df
    
    def clean_user_data(self):
        # Create a copy of the DataFrame to ensure modifications are made to the original DataFrame
        self.df = self.df.copy()
        
        # Convert date columns to datetime format, incompatible dates coerced to NaT
        date_cols = ['date_of_birth', 'join_date']
        self.df[date_cols] = self.df[date_cols].apply(pd.to_datetime, errors='coerce')
        
        # Convert phone_number column to string format and remove non-numeric characters
        self.df['phone_number'] = self.df['phone_number'].astype(str).str.replace('[^0-9]', '')

        # Convert country_code column to string format and remove non-alphabetic characters
        self.df['country_code'] = self.df['country_code'].astype(str).str.replace('[^a-zA-Z]', '')
        
        # Drop rows with incorrect email format
        self.df = self.df[self.df['email_address'].str.contains('@')]

        # Drop rows with NULL or NaT values
        self.df.dropna(how='any', inplace=True)
        
        # Reset index
        self.df.reset_index(drop=True, inplace=True)
        
        return self.df
    
    def clean_card_data(self):
        # Drop rows with NULL values
        self.card_df.dropna(inplace=True)
        
        # Remove rows with invalid 'expiry_date'
        self.card_df = self.card_df[self.card_df['expiry_date'].apply(self.validate_expiry_date)]

        # Clean and format 'card_provider'
        self.card_df['card_provider'] = self.card_df['card_provider'].str.strip()

        # Remove rows with invalid card numbers using Luhn algorithm
        #self.df = self.df[self.df['card_number'].apply(self.luhn_algo)]

        # Reset index
        self.card_df.reset_index(drop=True, inplace=True)
        
        return self.card_df
    
    def luhn_algo(self, card_number):
        if len(card_number) < 13 or len(card_number) > 19:
            return False

        total = 0
        reverse = card_number[::-1]
        for i, digit in enumerate(reverse):
            if i % 2 == 1:
                double_digit = int(digit) * 2
                if double_digit > 9:
                    double_digit -= 9
                total += double_digit
            else:
                total += int(digit)

        return total % 10 == 0

    def validate_expiry_date(self, date_str):
        try:
            # Parse the 'expiry_date' string to extract the month and year
            date_parts = date_str.split('/')
            month = int(date_parts[0])
            year = int(date_parts[1])

            # Check if the month is between 1 and 12 and if the year is not in the past
            current_date = datetime.datetime.now()
            if 1 <= month <= 12 and year >= current_date.year % 100:
                return True
        except (ValueError, IndexError):
            pass

        return False
    
    def clean_store_data(self):
        # Drop rows with NULL or NaN values
        self.store_df.dropna(how='any', inplace=True)
        
        # Replace slashes and newline characters with a comma and space
        self.store_df['address'] = self.store_df['address'].str.replace(r'\n', ', ')
        
        # Reset index
        self.store_df.reset_index(drop=True, inplace=True)
        
        return self.store_df
    
    def convert_product_weights(self):  
        # Remove rows with None (unknown or invalid values)
        self.products_df = self.products_df.dropna(subset=['weight'])

        # Convert units to kg
        for index, row in self.products_df.iterrows():
            try:
                if 'g' in row['weight'] and 'kg' not in row['weight']:
                    if 'x' in row['weight']:
                        weight_parts = row['weight'].split('x')
                        self.products_df.at[index, 'weight'] = float(weight_parts[0]) * float(weight_parts[1].replace('g', '')) / 1000
                        continue
                    self.products_df.at[index, 'weight'] = float(row['weight'].replace('g', '')) / 1000
                elif 'ml' in row['weight']:
                    self.products_df.at[index, 'weight'] = float(row['weight'].replace('ml', '')) / 1000
                else:
                    self.products_df.at[index, 'weight'] = float(row['weight'].replace('kg', ''))
            except ValueError:
                self.products_df.at[index, 'weight'] = None
        
        # Remove rows with None (unknown or invalid values)
        self.products_df = self.products_df.dropna(subset=['weight'])

        # Convert the 'weight' column to a float data type
        self.products_df['weight'] = self.products_df['weight'].astype(float)
        
        return self.products_df
    
    def clean_products_data(self):
        # Drop rows with NULL or NaN values
        self.products_df.dropna(how='any', inplace=True)

        # Reset index
        self.products_df.reset_index(drop=True, inplace=True)
        
        return self.df
    
    def clean_orders_data(self):
        # Drop column 1, level_0, first_name, last_name
        self.orders_df.drop(columns=['1'], inplace=True)
        self.orders_df.drop(columns=['index'], inplace=True)
        self.orders_df.drop(columns=['first_name'], inplace=True)
        self.orders_df.drop(columns=['last_name'], inplace=True)

        # Reset the index
        self.orders_df.reset_index(drop=True, inplace=True)

        return self.orders_df

    def clean_date_events(self):
        # Drop rows with NULL or NaN values
        self.date_events_df.dropna(how='any', inplace=True)

        # Check UUID is valid
        self.date_events_df = self.date_events_df[self.date_events_df.apply(self.is_valid_uuid, axis=1, uuid_column_name='date_uuid')]        
        
        # Drop colum 'timestamp'
        self.date_events_df.drop(columns=['timestamp'], inplace=True)        
        
        # Reset index
        self.date_events_df.reset_index(drop=True, inplace=True)

        return self.date_events_df

    # Function to check if a value is a valid UUID
    def is_valid_uuid(self, row, uuid_column_name):
        try:
            uuid.UUID(row[uuid_column_name])
            return True
        except ValueError:
            return False
        
if __name__ == '__main__':
    # Init engine
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine()

    ### Extract user data from RDS database
    # extractor = DataExtractor('legacy_users')
    # df = extractor.read_rds_table(engine)
    #print('Data extracted from RDS database')

    # Clean the Data
    #print(df.head())
    #cleaner = DataCleaning(df)
    # cleaned_df = cleaner.clean_user_data()

    ### Extract + clean card data from PDF file
    # pdf_url = db_creds["pdf_url"]
    # pdf_extractor = DataExtractor('card_data')
    # card_df = pdf_extractor.retrieve_pdf_data(pdf_url)
    # print(card_df.head())
    # print(card_df.columns)
    # print(len(card_df))
    # cleaner = DataCleaning(card_df)
    # clean_card_df = cleaner.clean_card_data()
    # print('Card data cleaned')
    # print(clean_card_df.head())
    # print(len(clean_card_df))

    ### Extract + clean date events data from S3 bucket
    s3_extractor = DataExtractor('date_events')
    date_events_df = s3_extractor.extract_from_s3('s3_date_events_bucket')
    print(date_events_df.head())
    cleaner = DataCleaning(date_events_df)
    clean_date_events_df = cleaner.clean_date_events()
    print(clean_date_events_df.head())

    ### Extract + clean product data from S3 bucket
    # s3_extractor = DataExtractor('product_data')
    # product_df = s3_extractor.extract_from_s3('s3_product_data_bucket')
    # print(product_df.head())
    # cleaner = DataCleaning(product_df)
    # clean_product_df = cleaner.convert_product_weights()
    # clean_product_df = cleaner.clean_products_data()
    # print(clean_product_df.head())