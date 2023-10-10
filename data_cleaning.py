import pandas as pd

class DataCleaning:

    def __init__(self, df):
        self.df = df
    
    def clean_user_data(self):
        # Drop rows with NULL values
        self.df.dropna(inplace=True)
        
        # Convert date columns to datetime format
        date_cols = ['date_of_birth', 'last_login']
        for col in date_cols:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        # Convert age column to integer format
        self.df['age'] = pd.to_numeric(self.df['age'], errors='coerce').astype('Int64')
        
        # Drop rows with incorrect age values
        self.df.dropna(subset=['age'], inplace=True)
        self.df = self.df[self.df['age'] > 0]
        
        # Drop rows with incorrect gender values
        self.df = self.df[self.df['gender'].isin(['M', 'F'])]
        
        # Drop rows with incorrect email format
        self.df = self.df[self.df['email'].str.contains('@')]
        
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