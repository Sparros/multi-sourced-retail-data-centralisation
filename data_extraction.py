import pandas as pd
import tabula
import requests
from sqlalchemy import text
from database_utils import DatabaseConnector

class DataExtractor:

    def __init__(self, table_name):
        self.table_name = table_name
    
    def read_data(self, engine):
        with engine.connect() as conn:
            query = f"SELECT * FROM {self.table_name}"
            result = conn.execute(text(query))
            data = result.fetchall()
        return data
    
    def read_rds_table(self, engine):
        with engine.connect() as conn:
            df = pd.read_sql_table(self.table_name, conn)
        return df
    
    '''
    Extract data from PDF using tabula-py
    Concatenate all DataFrames into a single DataFrame
    '''
    def retrieve_pdf_data(self, link):
        df_list = tabula.read_pdf(link, pages='all')
        df = pd.concat(df_list, ignore_index=True)
        return df
    
    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()['number_of_stores']
        else:
            return None
        
    def retrieve_stores_data(self, endpoint, headers):
        num_stores = self.list_number_of_stores(endpoint, headers)
        stores_df = pd.DataFrame(columns=['store_number', 'store_name', 'store_address'])
        for i in range(1, num_stores+1):
            store_endpoint = endpoint + f'/{i}'
            response = requests.get(store_endpoint, headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                store_number = store_data['store_number']
                store_name = store_data['store_name']
                store_address = store_data['store_address']
                stores_df = stores_df.append({'store_number': store_number, 'store_name': store_name, 'store_address': store_address}, ignore_index=True)
        return stores_df
    
    def extract_from_s3(self, s3_address):
        # Split S3 address into bucket and key
        s3_parts = s3_address.replace('s3://', '').split('/')
        bucket = s3_parts[0]
        key = '/'.join(s3_parts[1:])
        
        # Download file from S3
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        
        return df
    
if __name__ == '__main__':
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine(db_creds)
    extractor = DataExtractor('legacy_users')
    df = extractor.read_rds_table(engine)
    df_card = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(df.head())
    #print(df_card.head())

headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
