import pandas as pd
import tabula
import requests
import boto3
import urllib.parse
from botocore.exceptions import ClientError
from sqlalchemy import text
from database_utils import DatabaseConnector

class APIError(Exception):
    pass

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
        try:
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                return response.json()['number_stores']
            else:
                error_message = response.text
                raise APIError(f"API Error: Status code {response.status_code}, Message: {error_message}")
        except APIError as e:
            print(e)
        except Exception as e:
            print(f"An error occurred while fetching store data: {str(e)}")
        return None
        
    def store_data(self, store_endpoint, headers, store_number):
        store_url = store_endpoint.replace('{store_number}', str(store_number))
        try:
            response = requests.get(store_url, headers=headers)
            if response.status_code == 200:
                #print(response.json())
                return response.json()
            else:
                error_message = response.text
                raise APIError(f"API Error: Status code {response.status_code}, Message: {error_message}")
        except APIError as e:
            print(e)
        except Exception as e:
            print(f"An error occurred while fetching store data: {str(e)}")
        return None
    
    def retrieve_stores_data(self, num_stores_endpoint, store_endpoint, headers):
        num_stores = self.list_number_of_stores(num_stores_endpoint, headers)
        stores_list = []
        for i in range(1, num_stores):
            store_url = store_endpoint.replace('{store_number}', str(i))
            response = requests.get(store_url, headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                stores_list.append(store_data)
            else:
                error_message = response.text
                raise APIError(f"API Error: Status code {response.status_code}, Message: {error_message}")
        stores_df = pd.DataFrame(stores_list)
        return stores_df
    
    def extract_from_s3(self, s3_address_key):
        db = DatabaseConnector()
        db_creds = db.read_db_creds()
        s3 = boto3.client('s3', aws_access_key_id=db_creds["AWS_ACCESS_KEY_ID"], aws_secret_access_key=db_creds["AWS_SECRET_ACCESS_KEY"])
        
        # Retrieve the S3 address from your credentials based on the provided key
        s3_address = db_creds.get(s3_address_key)

        if not s3_address:
            print(f"S3 address for key '{s3_address_key}' not found in credentials.")
            return None

        # Parse the S3 address to extract the bucket and key
        if s3_address.startswith('s3://'):
            s3_parts = s3_address[5:].split('/')
            bucket = s3_parts[0]
            key = '/'.join(s3_parts[1:])
        else:
            parsed_url = urllib.parse.urlparse(s3_address)
            print(parsed_url)
            bucket = parsed_url.netloc.split('.')[0]
            key = parsed_url.path.lstrip('/')
            
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            if key.endswith('.json'):
                product_df = pd.read_json(response['Body'])
            else:
                product_df = pd.read_csv(response['Body'])
            return product_df
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"Error code: {error_code}, Error message: {error_message}")
            return None
    
if __name__ == '__main__':
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine()

    ### Extract card data from PDF file
    # pdf_url = db_creds["pdf_url"]
    # extractor = DataExtractor('legacy_users')
    # df = extractor.read_rds_table(engine)
    # df_card = extractor.retrieve_pdf_data(pdf_url)
    # print(df.head())
    
    ### Extract store data from API
    # num_stores_endpoint = db_creds["num_stores_endpoint"]
    # store_endpoint = db_creds["store_endpoint"]
    # headers = {'x-api-key': db_creds["api_key"]}
    # extractor = DataExtractor('store_data')
    # num_stores = extractor.list_number_of_stores(num_stores_endpoint, headers)
    # print(num_stores)
    # extractor.store_data(store_endpoint, headers, 20)
    # stores_df = extractor.retrieve_stores_data(num_stores_endpoint, store_endpoint, headers)
    # print(stores_df.head())

    ### Extract product data from S3 bucket
    extractor = DataExtractor('product_data')
    product_df = extractor.extract_from_s3('s3_date_events_bucket')
    print(product_df.head())
    

