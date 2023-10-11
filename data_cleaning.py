import pandas as pd
pd.set_option('display.max_columns', None)  # Set to display all columns
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
        
        # Convert date columns to datetime format
        date_cols = ['expiry_date']
        for col in date_cols:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        # Convert amount column to float format
        self.df['amount'] = pd.to_numeric(self.df['amount'], errors='coerce')
        
        # Drop rows with incorrect amount values
        self.df.dropna(subset=['amount'], inplace=True)
        self.df = self.df[self.df['amount'] > 0]
        
        # Drop rows with incorrect card type values
        self.df = self.df[self.df['card_type'].isin(['Visa', 'Mastercard', 'American Express'])]
        
        # Reset index
        self.df.reset_index(drop=True, inplace=True)
        
        return self.df
    
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
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine(db_creds)
    extractor = DataExtractor('legacy_users')
    df = extractor.read_rds_table(engine)

    #print(df.head())
    #print(df.isnull().sum())
    cleaner = DataCleaning(df)

    cleaned_df = cleaner.clean_user_data()
    print(cleaned_df.head())
    #print(cleaned_df.isnull().sum())