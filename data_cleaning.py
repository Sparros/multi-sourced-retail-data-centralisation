import pandas as pd
import datetime
pd.set_option('display.max_columns', None)  # Set to display all columns
pd.options.mode.chained_assignment = None  # default='warn'
from data_extraction import DataExtractor

from database_utils import DatabaseConnector

class DataCleaning:

    def __init__(self, df):
        self.df = df
    
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
        self.df.dropna(inplace=True)
        
        # Remove rows with invalid 'expiry_date'
        self.df = self.df[self.df['expiry_date'].apply(self.validate_expiry_date)]

        # Clean and format 'card_provider'
        self.df['card_provider'] = self.df['card_provider'].str.strip()

        # Remove rows with invalid card numbers using Luhn algorithm
        #self.df = self.df[self.df['card_number'].apply(self.luhn_algo)]

        # Reset index
        self.df.reset_index(drop=True, inplace=True)
        
        return self.df
    
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
        self.df.dropna(how='any', inplace=True)
        
        # Remove leading/trailing white space from store_name and store_address columns
        self.df['store_name'] = self.df['store_name'].str.strip()
        self.df['store_address'] = self.df['store_address'].str.strip()
        
        # Convert store_number column to integer format
        self.df['store_number'] = self.df['store_number'].astype(int)
        
        # Reset index
        self.df.reset_index(drop=True, inplace=True)
        
        return self.df
    
    def convert_product_weights(self):
        # Remove excess characters from weight column
        self.df['weight'] = self.df['weight'].str.replace('[^0-9\.]', '')
        
        # Convert ml to g using 1:1 ratio
        self.df.loc[self.df['weight_unit'] == 'ml', 'weight'] = self.df['weight'][self.df['weight_unit'] == 'ml'].astype(float) * 1
        
        # Convert weight to kg
        self.df.loc[self.df['weight_unit'] == 'g', 'weight'] = self.df['weight'][self.df['weight_unit'] == 'g'].astype(float) / 1000
        
        # Drop weight_unit column
        self.df.drop('weight_unit', axis=1, inplace=True)
        
        return self.df
    
    def clean_products_data(self):
        # Drop rows with NULL or NaN values
        self.df.dropna(how='any', inplace=True)
        
        # Remove leading/trailing white space from product_name column
        self.df['product_name'] = self.df['product_name'].str.strip()
        
        # Remove excess characters from price column
        self.df['price'] = self.df['price'].str.replace('[^0-9\.]', '')
        
        # Convert price to float
        self.df['price'] = self.df['price'].astype(float)
        
        # Reset index
        self.df.reset_index(drop=True, inplace=True)
        
        return self.df

if __name__ == '__main__':
    # Init engine
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine()

    # Extract user data from RDS database
    # extractor = DataExtractor('legacy_users')
    # df = extractor.read_rds_table(engine)
    #print('Data extracted from RDS database')

    # Clean the Data
    #print(df.head())
    #cleaner = DataCleaning(df)
    # cleaned_df = cleaner.clean_user_data()

    # Extract card data from PDF file
    pdf_url = db_creds["pdf_url"]
    pdf_extractor = DataExtractor('card_data')
    card_df = pdf_extractor.retrieve_pdf_data(pdf_url)
    print(card_df.head())
    print(card_df.columns)
    print(len(card_df))
    cleaner = DataCleaning(card_df)
    clean_card_df = cleaner.clean_card_data()
    print('Card data cleaned')
    print(clean_card_df.head())
    print(len(clean_card_df))