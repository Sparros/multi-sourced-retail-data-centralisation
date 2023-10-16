import yaml
import sqlalchemy
from sqlalchemy import text, inspect

class DatabaseConnector:

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            db_creds = yaml.load(f, Loader=yaml.FullLoader)
        return db_creds
    
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        db_url = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = sqlalchemy.create_engine(db_url)
        return engine
    
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def init_local_db_engine(self):
        db_creds = self.read_db_creds()
        db_url = f"postgresql://{db_creds['lOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}"
        local_db_engine = sqlalchemy.create_engine(db_url)
        return local_db_engine

    def upload_to_db(self, df, table_name):
        local_db_engine = self.init_local_db_engine()
        with local_db_engine.connect() as conn:
            df.to_sql(table_name, con=conn, if_exists='replace', index=False)

if __name__ == '__main__':
    db = DatabaseConnector()
    db_creds = db.read_db_creds()
    engine = db.init_db_engine(db_creds)
    table_names = db.list_db_tables(engine)
    print(table_names)