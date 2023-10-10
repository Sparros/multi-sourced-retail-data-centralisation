import pandas as pd
import tabula
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
    
if __name__ == '__main__':
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine(db_creds)
    extractor = DataExtractor('legacy_users')
    df = extractor.read_rds_table(engine)
    df_card = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #print(df.head())
    print(df_card.head())
